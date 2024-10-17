// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"go.uber.org/atomic"

	"github.com/vkcom/statshouse-go"
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/vkgo/srvfunc"
)

const (
	maxEvictionSampleSize = 100
	invalidateFrom        = -48 * time.Hour
	invalidateLinger      = 15 * time.Second // try to work around ClickHouse table replication race
	tsSelectRowSize       = 20 + format.MaxTags*4 + format.MaxStringLen
)

type tsSelectRow struct {
	time    int64
	stepSec int64 // TODO - do not get using strange logic in clickhouse, set directly
	tsTags
	tsValues
}

// all numeric tags are stored as int32 to save space
type tsTags struct {
	tag      [format.MaxTags]int32
	stag     [format.MaxTags]string
	tagStr   stringFixed
	shardNum uint32
}

type tsValues struct {
	countNorm float64
	val       [7]float64
	host      [2]int32 // "min" at [0], "max" at [1]
}

type tsCacheGroup struct {
	pointCaches map[string]map[int64]*tsCache // by version, step
	shutdown    func()
}

func newTSCacheGroup(approxMaxSize int, lodTables map[string]map[int64]string, utcOffset int64, loader tsLoadFunc, dropEvery time.Duration) *tsCacheGroup {
	g := &tsCacheGroup{
		pointCaches: map[string]map[int64]*tsCache{},
	}

	for version, tables := range lodTables {
		drop := dropEvery
		if version == Version2 {
			drop = 0 // NB! WHY??
		}

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
				dropEvery:         drop,
				bytesAlloc: statshouse.GetMetricRef(format.BuiltinMetricAPICacheBytesAlloc, statshouse.Tags{
					1: srvfunc.HostnameForStatshouse(), 2: version, 3: strconv.FormatInt(stepSec, 10),
				}),
				bytesFreeStale: statshouse.GetMetricRef(format.BuiltinMetricAPICacheBytesFree, statshouse.Tags{
					1: srvfunc.HostnameForStatshouse(), 2: version, 3: strconv.FormatInt(stepSec, 10), 4: "1",
				}),
				bytesFreeLRU: statshouse.GetMetricRef(format.BuiltinMetricAPICacheBytesFree, statshouse.Tags{
					1: srvfunc.HostnameForStatshouse(), 2: version, 3: strconv.FormatInt(stepSec, 10), 4: "2",
				}),
				bytesFreeOverride: statshouse.GetMetricRef(format.BuiltinMetricAPICacheBytesFree, statshouse.Tags{
					1: srvfunc.HostnameForStatshouse(), 2: version, 3: strconv.FormatInt(stepSec, 10), 4: "3",
				}),
				bytesTotal: statshouse.GetMetricRef(format.BuiltinMetricAPICacheBytesTotal, statshouse.Tags{
					1: srvfunc.HostnameForStatshouse(), 2: version, 3: strconv.FormatInt(stepSec, 10),
				}),
				ageTotal: statshouse.GetMetricRef(format.BuiltinMetricAPICacheAgeTotal, statshouse.Tags{
					1: srvfunc.HostnameForStatshouse(), 2: version, 3: strconv.FormatInt(stepSec, 10),
				}),
				ageEvictStale: statshouse.GetMetricRef(format.BuiltinMetricAPICacheAgeEvict, statshouse.Tags{
					1: srvfunc.HostnameForStatshouse(), 2: version, 3: strconv.FormatInt(stepSec, 10), 4: "1",
				}),
				ageEvictLRU: statshouse.GetMetricRef(format.BuiltinMetricAPICacheAgeEvict, statshouse.Tags{
					1: srvfunc.HostnameForStatshouse(), 2: version, 3: strconv.FormatInt(stepSec, 10), 4: "2",
				}),
				ageEvictOverride: statshouse.GetMetricRef(format.BuiltinMetricAPICacheAgeEvict, statshouse.Tags{
					1: srvfunc.HostnameForStatshouse(), 2: version, 3: strconv.FormatInt(stepSec, 10), 4: "3",
				}),
			}
		}
	}

	var ctx context.Context
	ctx, g.shutdown = context.WithCancel(context.Background())
	go func() {
		timer := time.NewTimer(time.Hour)
		if !timer.Stop() {
			<-timer.C
		}
		for interval := time.Duration(15 * time.Second); ; {
			now := time.Now()
			timer.Reset(now.Truncate(interval).Add(interval).Sub(now))
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				g.reportStats()
			}
		}
	}()
	return g
}

func (g *tsCacheGroup) reportStats() {
	for _, v := range g.pointCaches {
		for _, v := range v {
			v.reportStats()
		}
	}
}

func (c *tsCache) reportStats() {
	var n int64
	c.cacheMu.RLock()
	defer c.cacheMu.RUnlock()
	for _, v := range c.cache {
		for _, v := range v.secRows {
			n += v.sizeInBytes()
			c.ageTotal.Value(v.ageInSeconds())
		}
	}
	if n != 0 {
		c.bytesTotal.Value(float64(n))
	}
}

func (c *tsVersionedRows) ageInSeconds() float64 {
	return time.Since(time.Unix(0, c.loadedAtNano)).Seconds()
}

func (c *tsVersionedRows) sizeInBytes() int64 {
	return int64(tsSelectRowSize * len(c.rows))
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
}

func (g *tsCacheGroup) Get(ctx context.Context, version string, key string, pq *preparedPointsQuery, lod data_model.LOD, avoidCache bool) ([][]tsSelectRow, error) {
	x, err := lod.IndexOf(lod.ToSec)
	if err != nil {
		return nil, err
	}
	res := make([][]tsSelectRow, x)
	switch pq.metricID {
	case format.BuiltinMetricIDGeneratorConstCounter:
		generateConstCounter(lod, res)
	case format.BuiltinMetricIDGeneratorSinCounter:
		generateSinCounter(lod, res)
	case format.BuiltinMetricIDGeneratorGapsCounter:
		generateGapsCounter(lod, res)
	default:
		return g.pointCaches[version][lod.StepSec].get(ctx, key, pq, lod, avoidCache, res)
	}
	return res, nil
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
	bytesAlloc        statshouse.MetricRef
	bytesFreeStale    statshouse.MetricRef
	bytesFreeLRU      statshouse.MetricRef
	bytesFreeOverride statshouse.MetricRef
	bytesTotal        statshouse.MetricRef
	ageTotal          statshouse.MetricRef
	ageEvictStale     statshouse.MetricRef
	ageEvictLRU       statshouse.MetricRef
	ageEvictOverride  statshouse.MetricRef
}

type tsLoadFunc func(ctx context.Context, pq *preparedPointsQuery, lod data_model.LOD, ret [][]tsSelectRow, retStartIx int) (int, error)

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
					c.ageEvictStale.Value(cached.ageInSeconds())
					c.bytesFreeStale.Value(float64(cached.sizeInBytes()))
					delete(e.secRows, t)
				}
			}
		}
		c.lastDrop = now
	}
}

func (c *tsCache) get(ctx context.Context, key string, pq *preparedPointsQuery, lod data_model.LOD, avoidCache bool, ret [][]tsSelectRow) ([][]tsSelectRow, error) {
	if c.dropEvery != 0 {
		c.maybeDropCache()
	}

	cachedRows := 0
	realLoadFrom := lod.FromSec
	realLoadTo := lod.ToSec
	if !avoidCache {
		realLoadFrom, realLoadTo = c.loadCached(ctx, key, lod.FromSec, lod.ToSec, ret, 0, lod.Location, &cachedRows)
		if realLoadFrom == 0 && realLoadTo == 0 {
			ChCacheRate(cachedRows, 0, pq.metricID, lod.Table, pq.kind.String())
			return ret, nil
		}
	}

	loadAtNano := time.Now().UnixNano()
	loadLOD := data_model.LOD{FromSec: realLoadFrom, ToSec: realLoadTo, StepSec: c.stepSec, Table: lod.Table, HasPreKey: lod.HasPreKey, PreKeyOnly: lod.PreKeyOnly, Location: lod.Location}
	chRows, err := c.loader(ctx, pq, loadLOD, ret, int((realLoadFrom-lod.FromSec)/c.stepSec))
	if err != nil {
		return nil, err
	}

	ChCacheRate(cachedRows, chRows, pq.metricID, lod.Table, pq.kind.String())

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
	i, err := lod.IndexOf(realLoadFrom)
	if err != nil {
		return nil, err
	}
	for t := realLoadFrom; t < realLoadTo; i++ {
		nextRealLoadFrom := data_model.StepForward(t, c.stepSec, lod.Location)
		cached, ok := e.secRows[t]
		if !ok {
			cached = &tsVersionedRows{}
			e.secRows[t] = cached
		}
		if loadAtNano > cached.loadedAtNano {
			loadedRows := ret[i]
			c.size += len(loadedRows) - len(cached.rows)
			if n := cached.sizeInBytes(); n != 0 {
				c.ageEvictOverride.Value(cached.ageInSeconds())
				c.bytesFreeOverride.Value(float64(cached.sizeInBytes()))
			}
			cached.loadedAtNano = loadAtNano
			cached.rows = loadedRows
			c.bytesAlloc.Value(float64(cached.sizeInBytes()))
		}

		t = nextRealLoadFrom
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

func (c *tsCache) loadCached(ctx context.Context, key string, fromSec int64, toSec int64, ret [][]tsSelectRow, retStartIx int, location *time.Location, rows *int) (int64, int64) {
	c.cacheMu.RLock()
	defer c.cacheMu.RUnlock()

	e, ok := c.cache[key]
	if !ok {
		return fromSec, toSec
	}

	e.lru.Store(time.Now().UnixNano())

	var loadFrom, loadTo int64
	var hit int
	for t, ix := fromSec, retStartIx; t < toSec; ix++ {
		nextStartFrom := data_model.StepForward(t, c.stepSec, location)
		cached, ok := e.secRows[t]
		if ok && cached.loadedAtNano >= c.invalidatedAtNano[t]+int64(invalidateLinger) {
			ret[ix] = make([]tsSelectRow, len(cached.rows))
			copy(ret[ix], cached.rows)
			*rows += len(cached.rows)
			hit++
		} else {
			if loadFrom == 0 {
				loadFrom = t
			}

			loadTo = nextStartFrom
		}

		t = nextStartFrom
	}

	if p, ok := ctx.Value(debugQueriesContextKey).(*[]string); ok {
		reqCount := (toSec - fromSec) / c.stepSec
		*p = append(*p, fmt.Sprintf("CACHE step %d, count %d, range [%d,%d), hit %d, miss [%d,%d), key %q", c.stepSec, reqCount, fromSec, toSec, hit, loadFrom, loadTo, key))
	}

	return loadFrom, loadTo
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
		c.ageEvictLRU.Value(cached.ageInSeconds())
		c.bytesFreeLRU.Value(float64(cached.sizeInBytes()))
	}
	delete(c.cache, k)
	return n
}
