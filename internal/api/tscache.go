// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/vkcom/statshouse/internal/chutil"
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/format"
	"go.uber.org/atomic"
)

const (
	maxEvictionSampleSize = 100
	invalidateFrom        = -48 * time.Hour
	invalidateLinger      = 15 * time.Second // try to work around ClickHouse table replication race
	tsSelectRowSize       = 20 + format.MaxTags*4 + format.MaxStringLen
	tsValueCount          = 7
)

type tsSelectRow struct {
	what tsWhat
	time int64
	tsTags
	tsValues
}

// all numeric tags are stored as int32 to save space
type tsTags struct {
	tag       [format.NewMaxTags]int64
	stag      [format.NewMaxTags]string
	shardNum  uint32
	stagCount int
}

type tsValues struct {
	min         float64
	max         float64
	sum         float64
	count       float64
	sumsquare   float64
	unique      data_model.ChUnique
	percentile  *data_model.TDigest
	mergeCount  int
	cardinality float64

	minHost    chutil.ArgMinInt32Float32
	maxHost    chutil.ArgMaxInt32Float32
	minHostStr chutil.ArgMinStringFloat32
	maxHostStr chutil.ArgMaxStringFloat32
}

type tsWhat [tsValueCount]data_model.DigestSelector

func (s tsWhat) len() int {
	var n int
	for s.specifiedAt(n) {
		n++
	}
	return n
}

func (s tsWhat) specifiedAt(n int) bool {
	return n < tsValueCount && s[n].What != data_model.DigestUnspecified
}

type tsCacheGroup struct {
	pointCaches map[string]map[int64]*tsCache // by version, step
}

func newTSCacheGroup(approxMaxSize int, lodTables map[string]map[int64]string, utcOffset int64, loader tsLoadFunc) *tsCacheGroup {
	g := &tsCacheGroup{
		pointCaches: map[string]map[int64]*tsCache{},
	}

	for version, tables := range lodTables {
		g.pointCaches[version] = map[int64]*tsCache{}
		for stepSec := range tables {
			now := time.Now()
			g.pointCaches[version][stepSec] = &tsCache{
				loader:            loader,
				approxMaxSize:     approxMaxSize,
				stepSec:           stepSec,
				utcOffset:         utcOffset,
				cache:             map[string]*tsEntry{},
				invalidatedAtNano: map[int64]int64{},
				lastDrop:          now,
				dropEvery:         data_model.CacheDropInterval(version),
			}
		}
	}

	return g
}

func (g *tsCacheGroup) changeMaxSize(newSize int) {
	for _, cs := range g.pointCaches {
		for _, c := range cs {
			c.changeMaxSize(newSize)
		}
	}
}

func (g *tsCacheGroup) Invalidate(lodLevel int64, times []int64) {
	g.pointCaches[Version2][lodLevel].invalidate(times)
	g.pointCaches[Version3][lodLevel].invalidate(times)
}

func (g *tsCacheGroup) Get(ctx context.Context, h *requestHandler, pq *queryBuilder, lod data_model.LOD, avoidCache bool) ([][]tsSelectRow, error) {
	x, err := lod.IndexOf(lod.ToSec)
	if err != nil {
		return nil, err
	}
	res := make([][]tsSelectRow, x)
	return g.pointCaches[lod.Version][lod.StepSec].get(ctx, h, pq, lod, avoidCache, res)
}

type tsCache struct {
	loader            tsLoadFunc
	size              int
	approxMaxSize     int
	stepSec           int64
	utcOffset         int64 // only used in maybeDropCache(); all external timestamps should be rounded with roundTime()
	cacheMu           sync.RWMutex
	cache             map[string]*tsEntry
	invalidatedAtNano map[int64]int64
	lastDrop          time.Time
	dropEvery         time.Duration
}

type tsLoadFunc func(ctx context.Context, h *requestHandler, pq *queryBuilder, lod data_model.LOD, ret [][]tsSelectRow, retStartIx int) (int, error)

type tsVersionedRows struct {
	rows         []tsSelectRow
	loadedAtNano int64
}

type tsEntry struct {
	// Place atomics first to ensure proper alignment, see https://pkg.go.dev/sync/atomic#pkg-note-BUG
	lru     atomic.Int64
	secRows map[int64]*tsVersionedRows
}

func (c *tsCache) changeMaxSize(newMaxSize int) {
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()
	c.approxMaxSize = newMaxSize
}

func (c *tsCache) maybeDropCache() {
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()

	if now := time.Now(); now.Sub(c.lastDrop) > c.dropEvery {
		fromSec := roundTime(now.Add(invalidateFrom).Unix(), c.stepSec, c.utcOffset)
		nowSec := now.Unix()
		for _, e := range c.cache {
			for t := fromSec; t <= nowSec; t += c.stepSec {
				cached, ok := e.secRows[t]
				if ok {
					c.size -= len(cached.rows)
					delete(e.secRows, t)
				}
			}
		}
		c.lastDrop = now
	}
}

func (c *tsCache) get(ctx context.Context, h *requestHandler, pq *queryBuilder, lod data_model.LOD, avoidCache bool, ret [][]tsSelectRow) ([][]tsSelectRow, error) {
	if c.dropEvery != 0 {
		c.maybeDropCache()
	}

	cachedRows := 0
	realLoadFrom := lod.FromSec
	realLoadTo := lod.ToSec
	key := pq.getOrBuildCacheKey()
	if !avoidCache {
		realLoadFrom, realLoadTo = c.loadCached(h, pq, key, lod.FromSec, lod.ToSec, ret, 0, lod.Location, &cachedRows)
		if realLoadFrom == realLoadTo {
			ChCacheRate(cachedRows, 0, pq.metricID(), lod.Table, "")
			return ret, nil
		}
	}

	loadAtNano := time.Now().UnixNano()
	loadLOD := data_model.LOD{FromSec: realLoadFrom, ToSec: realLoadTo, StepSec: c.stepSec, Table: lod.Table, HasPreKey: lod.HasPreKey, PreKeyOnly: lod.PreKeyOnly, Location: lod.Location, Version: lod.Version}
	startX, err := lod.IndexOf(realLoadFrom)
	if err != nil {
		return nil, err
	}
	chRows, err := c.loader(ctx, h, pq, loadLOD, ret, startX)
	if err != nil {
		return nil, err
	}

	ChCacheRate(cachedRows, chRows, pq.metricID(), lod.Table, "")

	// map string tags
	endX, err := lod.IndexOf(realLoadTo)
	if err != nil {
		return nil, err
	}
	if pq.metric != nil && len(pq.by) != 0 {
		for _, x := range pq.by {
			var tag format.MetricMetaTag
			if 0 <= x && x < len(pq.metric.Tags) {
				tag = pq.metric.Tags[x]
			}
			for i := startX; i < endX; i++ {
				for j := 0; j < len(ret[i]); j++ {
					if s := ret[i][j].stag[x]; s != "" {
						v, err := h.getRichTagValueID(&tag, h.version, s)
						if err == nil {
							ret[i][j].tag[x] = int64(v)
							ret[i][j].stag[x] = ""
						} else {
							ret[i][j].stagCount++
						}
					}
				}
			}
		}
	}

	if avoidCache {
		return ret, nil
	}

	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()

	for c.size >= c.approxMaxSize {
		c.size -= c.evictLocked()
	}

	e, ok := c.cache[key]
	if !ok {
		e = &tsEntry{secRows: map[int64]*tsVersionedRows{}}
		c.cache[key] = e
	}

	e.lru.Store(time.Now().UnixNano())
	for i, t := startX, realLoadFrom; i < endX; i++ {
		cached, ok := e.secRows[t]
		if !ok {
			cached = &tsVersionedRows{}
			e.secRows[t] = cached
		}
		if loadAtNano > cached.loadedAtNano {
			loadedRows := ret[i]
			c.size += len(loadedRows) - len(cached.rows)
			cached.loadedAtNano = loadAtNano
			cached.rows = loadedRows
		}
		t = data_model.StepForward(t, c.stepSec, lod.Location)
	}

	return ret, nil
}

func (c *tsCache) invalidate(times []int64) {
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()

	at := time.Now().UnixNano()
	for _, sec := range times {
		c.invalidatedAtNano[sec] = at
	}

	i := 0
	from := time.Now().Add(invalidateFrom).Unix()
	for sec := range c.invalidatedAtNano {
		if sec < from {
			delete(c.invalidatedAtNano, sec)
		}
		i++
		if i == maxEvictionSampleSize {
			break
		}
	}
}

func (c *tsCache) loadCached(h *requestHandler, pq *queryBuilder, key string, fromSec int64, toSec int64, ret [][]tsSelectRow, retStartIx int, location *time.Location, rows *int) (int64, int64) {
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()

	e, ok := c.cache[key]
	if !ok {
		return fromSec, toSec
	}

	e.lru.Store(time.Now().UnixNano())

	cacheStartX := retStartIx
	cacheFrom := fromSec
	cacheTo := fromSec
	for t, ix := fromSec, retStartIx; t < toSec; ix++ {
		nextStartFrom := data_model.StepForward(t, c.stepSec, location)
		cached, ok := e.secRows[t]
		if ok {
			ok = c.invalidatedAtNano[t]+int64(invalidateLinger) < cached.loadedAtNano
		}
		if ok {
			if t != cacheTo {
				cacheFrom = t
				cacheStartX = ix
			}
			cacheTo = nextStartFrom
		}
		t = nextStartFrom
	}

	if cacheFrom == cacheTo || fromSec < cacheFrom && cacheTo < toSec {
		return fromSec, toSec
	} else if fromSec == cacheFrom {
		if cacheTo == toSec {
			fromSec = 0
			toSec = 0
		} else {
			fromSec = cacheTo
		}
	} else { // cacheTo == toSec
		toSec = cacheFrom
	}

	var hit int
	for t, ix := cacheFrom, cacheStartX; t < cacheTo; ix++ {
		// map string tags
		cached := e.secRows[t]
		for i := 0; i < len(cached.rows); i++ {
			row := &cached.rows[i]
			for k := 0; k < len(row.stag) && row.stagCount != 0; k++ {
				if s := row.stag[k]; s != "" {
					var tag format.MetricMetaTag
					if 0 <= k && k < len(pq.metric.Tags) {
						tag = pq.metric.Tags[k]
					}
					if v, err := h.getRichTagValueID(&tag, h.version, s); err == nil {
						row.tag[k] = int64(v)
						row.stag[k] = ""
						row.stagCount--
					}
				}
			}
		}
		ret[ix] = make([]tsSelectRow, len(cached.rows))
		copy(ret[ix], cached.rows)
		*rows += len(cached.rows)
		hit++
		t = data_model.StepForward(t, c.stepSec, location)
	}

	reqCount := (toSec - fromSec) / c.stepSec
	h.Tracef("CACHE step %d, count %d, range [%d,%d), hit %d, miss [%d,%d), key %q", c.stepSec, reqCount, fromSec, toSec, hit, fromSec, toSec, key)

	return fromSec, toSec
}

func (c *tsCache) evictLocked() int {
	k := ""
	var v *tsEntry
	i := 0
	t := int64(math.MaxInt64)
	for key, e := range c.cache { // "power of N random choices" with map iteration providing randomness
		u := e.lru.Load()
		if u < t {
			k = key
			v = e
			t = u
		}
		i++
		if i == maxEvictionSampleSize {
			break
		}
	}
	n := 0
	for _, cached := range v.secRows {
		n += len(cached.rows)
	}
	delete(c.cache, k)
	return n
}

func (v *tsValues) merge(rhs tsValues) {
	if rhs.min < v.min {
		v.min = rhs.min
	}
	if v.max < rhs.max {
		v.max = rhs.max
	}
	v.sum += rhs.sum
	v.count += rhs.count
	v.sumsquare += rhs.sumsquare
	if v.mergeCount == 0 {
		// "unique" and "percentile" are holding references to memory
		// residing in read-only cache, therefore making a deep copy of them
		u := data_model.ChUnique{}
		u.Merge(v.unique)
		u.Merge(rhs.unique)
		v.unique = u
		if v.percentile != nil || rhs.percentile != nil {
			p := data_model.New()
			if v.percentile != nil {
				p.Merge(v.percentile)
			}
			if rhs.percentile != nil {
				p.Merge(rhs.percentile)
			}
			v.percentile = p
		} else {
			v.percentile = nil
		}
	} else {
		// already operating on cache memory copy, it's safe to modify current object
		v.unique.Merge(rhs.unique)
		if v.percentile == nil {
			v.percentile = rhs.percentile
		} else if rhs.percentile != nil {
			v.percentile.Merge(rhs.percentile)
		}
	}
	v.mergeCount++
	v.cardinality += rhs.cardinality
	v.minHost.Merge(rhs.minHost)
	v.maxHost.Merge(rhs.maxHost)
	v.minHostStr.Merge(rhs.minHostStr)
	v.maxHostStr.Merge(rhs.maxHostStr)
}
