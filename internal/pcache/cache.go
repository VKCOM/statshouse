// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package pcache

import (
	"context"
	"encoding"
	"sync"
	"time"

	"golang.org/x/time/rate"
	"pgregory.net/rand"
)

// TODO: clean up old values from the disc cache
// TODO: implement "Optimal probabilistic cache stampede prevention" XFetch algorithm

const (
	maxEvictionSampleSize = 5
	runnersCount          = 2
)

type Callback func(r Result)

type entry struct {
	mu sync.Mutex

	result Result
	update time.Time

	lru time.Time

	isBeingLoaded    bool // either in workQueue or taken by updateCached()
	waitingClients   []chan Result
	waitingCallbacks []Callback
}

type Value interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}

type Result struct {
	Value Value
	Err   error
	TTL   time.Duration
}

type workQueueItem struct {
	key   string
	entry *entry
	extra interface{}
}

type LoaderFunc func(ctx context.Context, key string, extra interface{}) (Value, time.Duration, error)

type Cache struct {
	Loader                  LoaderFunc
	DiskCache               DiskCache // we do not cache errors on disk
	DiskCacheNamespace      string
	MaxMemCacheSize         int
	MaxDiskCacheSize        int // <= 0 means no limit
	SpreadCacheTTL          bool
	DefaultCacheTTL         time.Duration
	DefaultNegativeCacheTTL time.Duration
	LoadMinInterval         time.Duration
	LoadBurst               int
	Empty                   func() Value

	memCacheMu sync.RWMutex
	memCache   map[string]*entry
	limiter    *rate.Limiter

	runOnce   sync.Once
	workMu    sync.Mutex // No other locks taken when this lock is taken
	workQueue []workQueueItem
	workCond  *sync.Cond
	stopWG    sync.WaitGroup
	stop      bool
}

func (e Result) Found() bool {
	return e.Value != nil || e.Err != nil
}

func callClients(r Result, waitingClients []chan Result, waitingCallbacks []Callback) {
	for _, ch := range waitingClients {
		ch <- r
	}
	for _, ch := range waitingCallbacks {
		ch(r)
	}
}

func (e *entry) prepareCallClientsLocked() (Result, []chan Result, []Callback) {
	waitingClients := e.waitingClients
	waitingCallbacks := e.waitingCallbacks
	e.waitingClients = nil
	e.waitingCallbacks = nil
	// TODO - move between pairs of slices to avoid allocs
	return e.result, waitingClients, waitingCallbacks
}

func (c *Cache) startWorkLocked(key string, e *entry, extra interface{}) {
	if e.isBeingLoaded {
		return
	}
	e.isBeingLoaded = true
	c.workMu.Lock()
	c.workQueue = append(c.workQueue, workQueueItem{
		entry: e,
		extra: extra,
		key:   key,
	})
	c.workMu.Unlock()
	c.runRun()
	c.workCond.Signal()
}

func (c *Cache) DiskCacheEmpty() bool {
	if c.DiskCache == nil {
		return true
	}
	keys, _ := c.DiskCache.ListKeys(c.DiskCacheNamespace, 1, 0) // ignore errors
	return len(keys) == 0
}

func (c *Cache) SetBootstrapValue(now time.Time, key string, v Value, ttl time.Duration) error {
	b, err := v.MarshalBinary()
	if err != nil {
		return err
	}
	if ttl < 0 {
		return nil
	}
	if ttl == 0 {
		ttl = c.DefaultCacheTTL
	}
	if c.SpreadCacheTTL {
		// good enough for the job, but definitely can be made faster
		ttl = time.Duration((0.75 + 0.5*rand.Float64()) * float64(ttl)) // keep mean
	}
	c.memCacheMu.Lock()
	if c.memCache == nil {
		c.memCache = map[string]*entry{}
	}
	defer c.memCacheMu.Unlock()
	_, ok := c.memCache[key]
	if ok {
		return nil // if something started loading, skip it, invariants are complicated
	}
	e := &entry{
		result: Result{
			Value: v,
			Err:   nil,
			TTL:   ttl,
		},
		update: now,
		lru:    now,
	}
	c.memCache[key] = e
	if c.DiskCache == nil {
		return nil
	}
	return c.DiskCache.Set(c.DiskCacheNamespace, key, b, now, ttl)
}

func (c *Cache) runRun() {
	c.runOnce.Do(func() {
		c.workCond = sync.NewCond(&c.workMu)
		burst := c.LoadBurst
		if burst < 1 {
			burst = 1
		}
		c.limiter = rate.NewLimiter(rate.Every(c.LoadMinInterval), burst)
		c.stopWG.Add(runnersCount)
		for i := 0; i < runnersCount; i++ {
			go c.run()
		}
	})
}

func (c *Cache) Close() {
	c.runRun() // We ensure it is running first
	c.workMu.Lock()
	c.stop = true
	c.workMu.Unlock()
	c.workCond.Broadcast()
	c.stopWG.Wait()
}

func (c *Cache) run() {
	defer c.stopWG.Done()
	rng := rand.New()
	for {
		c.workMu.Lock()
		for len(c.workQueue) == 0 && !c.stop {
			c.workCond.Wait()
		}
		if c.stop {
			c.workMu.Unlock()
			return
		}
		e := c.workQueue[0]
		c.workQueue = c.workQueue[1:]
		c.workMu.Unlock()

		c.updateCached(rng, e)
	}
}

func (c *Cache) GetCachedString(now time.Time, key string) Result {
	c.memCacheMu.RLock()
	e, ok := c.memCache[key]
	c.memCacheMu.RUnlock()
	if !ok {
		return Result{}
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.result.TTL < 0 { // always load values with negative TTL
		return Result{}
	}

	e.lru = now
	stale := now.Sub(e.update) > e.result.TTL

	if stale {
		if e.result.Err != nil { // do not return stale errors
			return Result{}
		}
		c.startWorkLocked(key, e, nil)
		// Return only good stale results immediately
	}
	return e.result
}

func (c *Cache) GetCached(now time.Time, key []byte) Result {
	c.memCacheMu.RLock()
	e, ok := c.memCache[string(key)] // does not allocate
	c.memCacheMu.RUnlock()
	if !ok {
		return Result{}
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.result.TTL < 0 { // always load values with negative TTL
		return Result{}
	}

	e.lru = now
	stale := now.Sub(e.update) > e.result.TTL

	if stale {
		if e.result.Err != nil { // do not return stale errors
			return Result{}
		}
		c.startWorkLocked(string(key), e, nil)
		// Return only good stale results immediately
	}
	return e.result
}

func (c *Cache) getOrLoad(now time.Time, key string, extra interface{}, cb Callback) Result {
	r := c.GetCachedString(now, key)
	if r.Found() { // if stale value is returned, update is already triggered
		return r
	}
	c.memCacheMu.Lock()
	e, ok := c.memCache[key]
	if !ok {
		if c.memCache == nil {
			c.memCache = map[string]*entry{}
		}
		if len(c.memCache) >= c.MaxMemCacheSize {
			c.evictLocked()
			// We repeat, because we cannot evict those in work queue, so evictLocked() is NOP with some probability
			c.evictLocked()
		}
		e = &entry{}
		c.memCache[key] = e
	}
	e.mu.Lock()
	c.memCacheMu.Unlock()

	if e.result.Value != nil && e.result.TTL >= 0 { // only for good values
		e.mu.Unlock()
		return e.result
	}
	if cb != nil {
		e.waitingCallbacks = append(e.waitingCallbacks, cb)
		c.startWorkLocked(key, e, extra)
		e.mu.Unlock()
		return Result{} // Special case
	}
	ch := make(chan Result)
	e.waitingClients = append(e.waitingClients, ch)
	c.startWorkLocked(key, e, extra)
	e.mu.Unlock()

	result := <-ch
	return result
}

// Never returns empty result
func (c *Cache) GetOrLoad(now time.Time, key string, extra interface{}) Result {
	return c.getOrLoad(now, key, extra, nil)
}

// If empty result is returned, cb will be called in future
func (c *Cache) GetOrLoadCallback(now time.Time, key string, extra interface{}, cb Callback) Result {
	return c.getOrLoad(now, key, extra, cb)
}

// not perfect, takes big lock for duration of op, but we use it only when editing metrics (rare)
/* commented for now, because unused and we are unsure about correctness)
func (c *Cache) Remove(key string) error {
	c.memCacheMu.Lock()
	defer c.memCacheMu.Unlock()
	e, ok := c.memCache[key]
	if ok {
		e.mu.Lock()
		if e.isBeingLoaded {
			ch := make(chan Result)
			e.waitingClients = append(e.waitingClients, ch)
			e.mu.Unlock()
			<-ch
		} else {
			e.mu.Unlock()
		}
		// Here e is not being loaded and cannot start being loaded
		e.result.Value = nil
		e.result.Err = fmt.Errorf("value erased")
		e.result.TTL = 0
	}
	var err error
	if c.diskCache != nil {
		err = c.diskCache.Erase(c.DiskCacheNamespace, key)
	}
	return err
}
*/

func (c *Cache) adjustTTL(rng *rand.Rand, ttl time.Duration, err error) time.Duration {
	switch {
	case ttl != 0:
		break
	case err != nil:
		ttl = c.DefaultNegativeCacheTTL
	default:
		ttl = c.DefaultCacheTTL
	}
	if c.SpreadCacheTTL && ttl > 0 {
		// good enough for the job, but definitely can be made faster
		ttl = time.Duration((0.75 + 0.5*rng.Float64()) * float64(ttl)) // keep mean
	}
	return ttl
}

func (c *Cache) updateCached(rng *rand.Rand, item workQueueItem) {
	e := item.entry
	if c.DiskCache != nil {
		now := time.Now()
		b, update, ttl, err, found := c.DiskCache.Get(c.DiskCacheNamespace, item.key)
		if err == nil && found {
			v := c.Empty()
			err = v.UnmarshalBinary(b)
			if err == nil { // Set potentially stale info ASAP
				e.mu.Lock()
				e.result.Value = v
				e.result.Err = nil
				e.result.TTL = ttl
				e.update = update
				e.lru = now
				r, cl, ch := e.prepareCallClientsLocked()
				if now.Sub(e.update) <= ttl {
					e.isBeingLoaded = false
					e.mu.Unlock()
					callClients(r, cl, ch)
					return
				}
				// isBeingLoaded remains set, so clients are potentially to come before we finish updating
				e.mu.Unlock()
				callClients(r, cl, ch)
			}
		}
		if err != nil {
			c.logError(err)
		}
		// Just continue
	}
	time.Sleep(c.limiter.Reserve().Delay())

	v, ttl, err := c.Loader(context.Background(), item.key, item.extra)
	ttl = c.adjustTTL(rng, ttl, err)
	now := time.Now()

	if err == nil && c.DiskCache != nil && ttl > 0 { // negative TTL can be used to effectively disable caching
		b, err := v.MarshalBinary()
		if err == nil {
			err = c.DiskCache.Set(c.DiskCacheNamespace, item.key, b, now, ttl)
		}
		if err != nil {
			c.logError(err)
		}
		// If could not save to disk, no problem, keep in memory
	}

	e.mu.Lock()
	if v != nil || e.result.Value == nil || e.result.TTL < 0 { // Do not overwrite stale good info with error info
		e.result.Value = v
		e.result.Err = err
	}
	e.result.TTL = ttl
	e.update = now
	e.lru = now
	r, cl, ch := e.prepareCallClientsLocked()
	e.isBeingLoaded = false
	e.mu.Unlock()
	callClients(r, cl, ch)
}

func (c *Cache) evictLocked() {
	k := ""
	i := 0
	var t time.Time
	for key, e := range c.memCache { // "power of N random choices" with map iteration providing randomness
		e.mu.Lock()
		if (i == 0 || e.lru.Before(t)) && !e.isBeingLoaded {
			k = key
			t = e.lru
		}
		e.mu.Unlock()
		i++
		if i == maxEvictionSampleSize {
			break
		}
	}
	delete(c.memCache, k) // We assume deleting empty string is NOP
}

func (c *Cache) logError(err error) {
	// TODO - log here
}
