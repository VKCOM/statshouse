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
)

const maxInternalEvictionSampleSize = 5

type pSelectRow struct {
	tsTags
	tsValues
}

type pointsCache struct {
	loader            pointsLoadFunc
	size              int
	approxMaxSize     int
	utcOffset         int64 // only used in maybeDropCache(); all external timestamps should be rounded with roundTime()
	cacheMu           sync.RWMutex
	cache             map[string]map[timeRange]*cacheEntry
	invalidatedAtNano *secondsCache
	now               func() time.Time
}

type pointsLoadFunc func(ctx context.Context, pq *preparedPointsQuery, pointsQuery pointQuery) ([]pSelectRow, error)

type timeRange struct {
	from int64
	to   int64
}

type cacheEntry struct {
	// Place atomics first to ensure proper alignment, see https://pkg.go.dev/sync/atomic#pkg-note-BUG
	lru          atomic.Int64
	loadedAtNano int64
	rows         []pSelectRow
}

func newPointsCache(approxMaxSize int, utcOffset int64, loader pointsLoadFunc, now func() time.Time) *pointsCache {
	return &pointsCache{
		loader:            loader,
		approxMaxSize:     approxMaxSize,
		utcOffset:         utcOffset,
		cache:             map[string]map[timeRange]*cacheEntry{},
		invalidatedAtNano: newSecondsCache(now),
		now:               now,
	}
}

func (c *pointsCache) get(ctx context.Context, key string, pq *preparedPointsQuery, pointQuery pointQuery, avoidCache bool) ([]pSelectRow, error) {
	if !avoidCache {
		rows, ok := c.loadCached(key, pointQuery.fromSec, pointQuery.toSec)
		if ok {
			return rows, nil
		}
	}
	loadedAtNano := c.now().UnixNano()
	rows, err := c.loader(ctx, pq, pointQuery)
	if err != nil {
		return rows, err
	}
	if avoidCache {
		return rows, nil
	}
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()
	if _, ok := c.cache[key]; !ok {
		c.cache[key] = map[timeRange]*cacheEntry{}
	}

	for c.size >= c.approxMaxSize {
		c.size -= c.evictLocked()
	}

	e := &cacheEntry{
		loadedAtNano: loadedAtNano,
		rows:         rows,
	}
	e.lru.Store(c.now().UnixNano())
	c.cache[key][timeRange{from: pointQuery.fromSec, to: pointQuery.toSec}] = e
	return rows, nil
}

func (c *pointsCache) loadCached(key string, from, to int64) ([]pSelectRow, bool) {
	c.cacheMu.RLock()
	defer c.cacheMu.RUnlock()
	trCache, ok := c.cache[key]
	if !ok {
		return nil, false
	}
	tr := timeRange{
		from: from,
		to:   to,
	}
	entry, ok := trCache[tr]
	if !ok {
		return nil, false
	}
	entry.lru.Store(c.now().UnixNano())
	cachedIsValid := c.invalidatedAtNano.checkInvalidationLocked(entry.loadedAtNano, from, to, c.utcOffset)
	return entry.rows, cachedIsValid
}

func (c *pointsCache) invalidate(times []int64) {
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()

	at := c.now().UnixNano()
	for _, sec := range times {
		c.invalidatedAtNano.updateTimeLocked(at, sec, c.utcOffset)
	}

	from := c.now().Add(invalidateFrom).Unix()
	c.invalidatedAtNano.invalidateLocked(from)
}

func (c *pointsCache) evictLocked() int {
	k := ""
	i := 0
	t := int64(math.MaxInt64)
	for key, caches := range c.cache { // "power of N random choices" with map iteration providing randomness
		j := 0
		for _, e := range caches {
			u := e.lru.Load()
			if u < t {
				k = key
				t = u
			}
			j++
			if j > maxInternalEvictionSampleSize {
				break
			}
		}
		i++
		if i == maxEvictionSampleSize {
			break
		}
	}
	if k == "" {
		return 0
	}
	e := c.cache[k]
	n := len(e)
	delete(c.cache, k)
	return n
}

type secondsCache struct {
	seconds [3]map[int64]int64
	now     func() time.Time
}

var steps = [3]int64{_1h, _1m, _1s}

func newSecondsCache(now func() time.Time) *secondsCache {
	cache := &secondsCache{now: now}
	for i := range steps {
		cache.seconds[i] = map[int64]int64{}
	}
	return cache
}

func (c *secondsCache) updateTimeLocked(invalidatedAtNano, sec int64, utcOffset int64) {
	for i, step := range steps {
		rounded := roundTime(sec, step, utcOffset)
		if last, ok := c.seconds[i][rounded]; !ok || (ok && invalidatedAtNano > last) {
			c.seconds[i][rounded] = invalidatedAtNano
		}
	}
}

func (c *secondsCache) invalidateLocked(from int64) {
	for i := range steps {
		j := 0
		for sec := range c.seconds[i] {
			if sec < from {
				delete(c.seconds[i], sec)
			}
			j++
			if j == maxEvictionSampleSize {
				break
			}
		}
	}

}

func (c *secondsCache) checkInvalidationLocked(loadAt, from, to int64, utcOffset int64) bool {
	now := c.now()
	cacheImmutable := now.Add(invalidateFrom)
	if time.Unix(from, 0).Before(cacheImmutable) {
		from = cacheImmutable.Unix()
	}
	if time.Unix(to, 0).Before(cacheImmutable) {
		return true
	}
	return c.checkInvalidationMapLocked(0, loadAt, from, to, utcOffset)

}

func (c *secondsCache) checkInvalidationMapLocked(ix int, loadAt, from, to int64, utcOffset int64) bool {
	if ix == len(steps) {
		return true
	}
	step := steps[ix]
	fromR := roundTime(from, step, utcOffset)
	toR := roundTime(to, step, utcOffset)
	if ix == len(steps)-1 {
		for i := from; i <= to; i += step {
			if invalidatedAtNano, ok := c.seconds[ix][i]; ok && loadAt <= invalidatedAtNano+int64(invalidateLinger) {
				return false
			}
		}
		return true
	}
	res := true

	fromNext := fromR + step
	if to < fromNext {
		fromNext = to
	}
	toPrev := toR
	if from > toPrev {
		toPrev = from
	}
	res = res && c.checkInvalidationMapLocked(ix+1, loadAt, from, fromNext, utcOffset)
	res = res && c.checkInvalidationMapLocked(ix+1, loadAt, toPrev, to, utcOffset)
	if !res {
		return false
	}

	for i := fromR + step; i < toR; i += step {
		if invalidatedAtNano, ok := c.seconds[ix][i]; ok && loadAt <= invalidatedAtNano+int64(invalidateLinger) {
			return false
		}
	}
	return true
}
