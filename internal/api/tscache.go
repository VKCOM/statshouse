// Copyright 2022 V Kontakte LLC
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

	"go.uber.org/atomic"

	"github.com/vkcom/statshouse/internal/format"
)

const (
	maxEvictionSampleSize = 100
	invalidateFrom        = -48 * time.Hour
	invalidateLinger      = 15 * time.Second // try to work around ClickHouse table replication race
)

type tsSelectRow struct {
	time    int64
	stepSec int64 // TODO - do not get using strange logic in clickhouse, set directly
	tsTags
	tsValues
}

// all numeric tags are stored as int32 to save space
type tsTags struct {
	tag    [format.MaxTags]int32
	tagStr stringFixed
}

type tsValues struct {
	countNorm float64
	val       [7]float64
}

type tsCacheGroup struct {
	pointCaches map[string]map[int64]*tsCache
}

func newTSCacheGroup(approxMaxSize int, lodTables map[string]map[int64]string, utcOffset int64, loader tsLoadFunc, dropEvery time.Duration) *tsCacheGroup {
	g := &tsCacheGroup{
		pointCaches: map[string]map[int64]*tsCache{},
	}

	for version, tables := range lodTables {
		drop := dropEvery
		if version == Version2 {
			drop = 0
		}

		g.pointCaches[version] = map[int64]*tsCache{}
		for stepSec := range tables {
			g.pointCaches[version][stepSec] = newTSCache(approxMaxSize, stepSec, utcOffset, loader, drop)
		}
	}

	return g
}

func (g *tsCacheGroup) Invalidate(lodLevel int64, times []int64) {
	g.pointCaches[Version2][lodLevel].invalidate(times)
}

func (g *tsCacheGroup) Get(ctx context.Context, version string, key string, pq *preparedPointsQuery, lod lodInfo, avoidCache bool) ([][]tsSelectRow, error) {
	switch pq.metricID {
	case format.BuiltinMetricIDGeneratorConstCounter:
		return generateConstCounter(lod)
	case format.BuiltinMetricIDGeneratorSinCounter:
		return generateSinCounter(lod)
	case format.BuiltinMetricIDGeneratorGapsCounter:
		return generateGapsCounter(lod)
	default:
		return g.pointCaches[version][lod.stepSec].get(ctx, key, pq, lod, avoidCache)
	}
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

type tsLoadFunc func(ctx context.Context, pq *preparedPointsQuery, lod lodInfo, ret [][]tsSelectRow, retStartIx int) (int, error)

type tsVersionedRows struct {
	rows         []tsSelectRow
	loadedAtNano int64
}

type tsEntry struct {
	// Place atomics first to ensure proper alignment, see https://pkg.go.dev/sync/atomic#pkg-note-BUG
	lru     atomic.Int64
	secRows map[int64]*tsVersionedRows
}

func newTSCache(approxMaxSize int, stepSec int64, utcOffset int64, loader tsLoadFunc, dropEvery time.Duration) *tsCache {
	now := time.Now()
	return &tsCache{
		loader:            loader,
		approxMaxSize:     approxMaxSize,
		stepSec:           stepSec,
		utcOffset:         utcOffset,
		cache:             map[string]*tsEntry{},
		invalidatedAtNano: map[int64]int64{},
		lastDrop:          now,
		dropEvery:         dropEvery,
	}
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

func (c *tsCache) get(ctx context.Context, key string, pq *preparedPointsQuery, lod lodInfo, avoidCache bool) ([][]tsSelectRow, error) {
	if c.dropEvery != 0 {
		c.maybeDropCache()
	}

	ret := make([][]tsSelectRow, lod.getIndexForTimestamp(lod.toSec, 0))
	cachedRows := 0

	realLoadFrom := lod.fromSec
	realLoadTo := lod.toSec
	if !avoidCache {
		realLoadFrom, realLoadTo = c.loadCached(key, lod.fromSec, lod.toSec, ret, 0, lod.location, &cachedRows)
		if realLoadFrom == 0 && realLoadTo == 0 {
			ChCacheRate(cachedRows, 0, pq.metricID, lod.table, string(pq.kind))
			return ret, nil
		}
	}

	loadAtNano := time.Now().UnixNano()
	loadLOD := lodInfo{fromSec: realLoadFrom, toSec: realLoadTo, stepSec: c.stepSec, table: lod.table, hasPreKey: lod.hasPreKey, location: lod.location}
	chRows, err := c.loader(ctx, pq, loadLOD, ret, int((realLoadFrom-lod.fromSec)/c.stepSec))
	if err != nil {
		return nil, err
	}

	ChCacheRate(cachedRows, chRows, pq.metricID, lod.table, string(pq.kind))

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

	for t := realLoadFrom; t < realLoadTo; {
		var nextRealLoadFrom int64
		if c.stepSec == _1M {
			nextRealLoadFrom = time.Unix(t, 0).In(lod.location).AddDate(0, 1, 0).Unix()
		} else {
			nextRealLoadFrom = t + c.stepSec
		}

		cached, ok := e.secRows[t]
		if !ok {
			cached = &tsVersionedRows{}
			e.secRows[t] = cached
		}
		if loadAtNano > cached.loadedAtNano {
			loadedRows := ret[lod.getIndexForTimestamp(t, 0)]
			c.size += len(loadedRows) - len(cached.rows)
			cached.loadedAtNano = loadAtNano
			cached.rows = loadedRows
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

func (c *tsCache) loadCached(key string, fromSec int64, toSec int64, ret [][]tsSelectRow, retStartIx int, location *time.Location, rows *int) (int64, int64) {
	c.cacheMu.RLock()
	defer c.cacheMu.RUnlock()

	e, ok := c.cache[key]
	if !ok {
		return fromSec, toSec
	}

	e.lru.Store(time.Now().UnixNano())

	var loadFrom, loadTo int64
	for t, ix := fromSec, retStartIx; t < toSec; ix++ {
		var nextStartFrom int64
		if c.stepSec == _1M {
			nextStartFrom = time.Unix(t, 0).In(location).AddDate(0, 1, 0).Unix()
		} else {
			nextStartFrom = t + c.stepSec
		}

		cached, ok := e.secRows[t]
		if ok && cached.loadedAtNano >= c.invalidatedAtNano[t]+int64(invalidateLinger) {
			ret[ix] = cached.rows
			*rows += len(cached.rows)
		} else {
			if loadFrom == 0 {
				loadFrom = t
			}

			loadTo = nextStartFrom
		}

		t = nextStartFrom
	}

	return loadFrom, loadTo
}

func (c *tsCache) evictLocked() int {
	k := ""
	i := 0
	t := int64(math.MaxInt64)
	for key, e := range c.cache { // "power of N random choices" with map iteration providing randomness
		u := e.lru.Load()
		if u < t {
			k = key
			t = u
		}
		i++
		if i == maxEvictionSampleSize {
			break
		}
	}
	e := c.cache[k]
	n := 0
	for _, cached := range e.secRows {
		n += len(cached.rows)
	}
	delete(c.cache, k)
	return n
}
