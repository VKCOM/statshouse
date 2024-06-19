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

	"github.com/vkcom/statshouse/internal/data_model"
	"go.uber.org/atomic"
)

type pSelectRow struct {
	tsTags
	tsValues
}

type pointsCache struct {
	loader            pointsLoadFunc
	size              int // len(cacheEntry.rows) + cacheEntry.rowsSize
	approxMaxSize     int
	cacheMu           sync.RWMutex
	cache             map[string]*cacheEntry
	invalidatedAtNano *invalidatedSecondsCache
	now               func() time.Time
}

type pointsLoadFunc func(ctx context.Context, pq *preparedPointsQuery, lod data_model.LOD) ([]pSelectRow, error)

type timeRange struct {
	from int64
	to   int64
}

type cacheEntry struct {
	// Place atomics first to ensure proper alignment, see https://pkg.go.dev/sync/atomic#pkg-note-BUG
	lru          atomic.Int64
	loadedAtNano int64
	rows         map[timeRange][]pSelectRow
	rowsSize     int
}

func newPointsCache(approxMaxSize int, utcOffset int64, loader pointsLoadFunc, now func() time.Time) *pointsCache {
	return &pointsCache{
		loader:            loader,
		approxMaxSize:     approxMaxSize,
		cache:             map[string]*cacheEntry{},
		invalidatedAtNano: newSecondsCache(utcOffset, now),
		now:               now,
	}
}

func (c *pointsCache) get(ctx context.Context, key string, pq *preparedPointsQuery, lod data_model.LOD, avoidCache bool) ([]pSelectRow, error) {
	if !avoidCache {
		rows, ok := c.loadCached(key, lod.FromSec, lod.ToSec)
		if ok {
			return rows, nil
		}
	}
	loadedAtNano := c.now().UnixNano()
	rows, err := c.loader(ctx, pq, lod)
	if err != nil {
		return rows, err
	}
	if avoidCache {
		return rows, nil
	}
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()

	for c.size+len(c.cache) >= c.approxMaxSize {
		c.size -= c.evictLocked()
	}

	e, ok := c.cache[key]
	if !ok {
		e = &cacheEntry{
			lru:          atomic.Int64{},
			loadedAtNano: 0,
			rows:         map[timeRange][]pSelectRow{},
		}
		c.cache[key] = e
	}

	e.lru.Store(c.now().UnixNano())
	e.loadedAtNano = loadedAtNano
	tr := timeRange{from: lod.FromSec, to: lod.ToSec}
	if _, ok := e.rows[tr]; !ok {
		c.size += 1 + len(rows)
	} else {
		c.size += len(rows)
	}
	e.rowsSize += len(rows)
	e.rows[tr] = rows
	return rows, nil
}

func (c *pointsCache) loadCached(key string, from, to int64) ([]pSelectRow, bool) {
	c.cacheMu.RLock()
	defer c.cacheMu.RUnlock()
	entry, ok := c.cache[key]
	if !ok {
		return nil, false
	}
	tr := timeRange{
		from: from,
		to:   to,
	}
	row, ok := entry.rows[tr]
	if !ok {
		return nil, false
	}
	entry.lru.Store(c.now().UnixNano())
	cachedIsValid := c.invalidatedAtNano.checkInvalidationLocked(entry.loadedAtNano, from, to)
	return row, cachedIsValid
}

func (c *pointsCache) invalidate(times []int64) {
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()

	at := c.now().UnixNano()
	for _, sec := range times {
		c.invalidatedAtNano.updateTimeLocked(at, sec)
	}

	from := c.now().Add(invalidateFrom).Unix()
	c.invalidatedAtNano.invalidateLocked(from)
}

func (c *pointsCache) evictLocked() int {
	k := ""
	i := 0
	t := int64(math.MaxInt64)
	for key, caches := range c.cache {
		u := caches.lru.Load()
		if u < t {
			k = key
			t = u
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
	n := e.rowsSize
	delete(c.cache, k)
	return n + len(e.rows)
}

type invalidatedSecondsCache struct {
	seconds   [3]map[int64]int64
	now       func() time.Time
	utcOffset int64
}

var steps = [3]int64{_1h, _1m, _1s}

func newSecondsCache(utcOffset int64, now func() time.Time) *invalidatedSecondsCache {
	cache := &invalidatedSecondsCache{now: now, utcOffset: utcOffset}
	for i := range steps {
		cache.seconds[i] = map[int64]int64{}
	}
	return cache
}

func (c *invalidatedSecondsCache) updateTimeLocked(invalidatedAtNano, sec int64) {
	for i, step := range steps {
		rounded := roundTime(sec, step, c.utcOffset)
		if last, ok := c.seconds[i][rounded]; !ok || (ok && invalidatedAtNano > last) {
			c.seconds[i][rounded] = invalidatedAtNano
		}
	}
}

func (c *invalidatedSecondsCache) invalidateLocked(from int64) {
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

func (c *invalidatedSecondsCache) checkInvalidationLocked(loadAt, from, to int64) bool {
	now := c.now()
	cacheImmutable := now.Add(invalidateFrom)
	if time.Unix(from, 0).Before(cacheImmutable) {
		from = cacheImmutable.Unix()
	}
	if time.Unix(to, 0).Before(cacheImmutable) {
		return true
	}
	return c.checkInvalidationMapLocked(0, loadAt, from, to)

}

func (c *invalidatedSecondsCache) checkInvalidationMapLocked(ix int, loadAt, from, to int64) bool {
	if ix == len(steps) {
		return true
	}
	step := steps[ix]
	fromR := roundTime(from, step, c.utcOffset)
	toR := roundTime(to, step, c.utcOffset)
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
	res = res && c.checkInvalidationMapLocked(ix+1, loadAt, from, fromNext)
	res = res && c.checkInvalidationMapLocked(ix+1, loadAt, toPrev, to)
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
