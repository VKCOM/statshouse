// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package pcache

import (
	"cmp"
	"fmt"
	"io"
	"os"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/vkgo/basictl"
	"pgregory.net/rand"
)

type MappingPair = tlstatshouse.Mapping

type cacheValue struct {
	value    int32
	accessTS uint32
	// we do not use counters, because they are easy to increment, but require code to gradually decrement
	// access time is good mechanism to remove elements if cache size is enough.
	// if cache does not fit all strings we need, we will evict random elements non-stop, so lose anyway
	// value -1 is used as a special value in file format to record deletion.
}

type cacheKeyValue struct {
	str string
	val cacheValue
}

// design goals
// 0. fixed memory size
// 1. compact representation in memory
// 2. some cheap mechanism to replace mappings by access time or hit count
// 3. simple and compact disk storage without fsync
// 4. minimally block getters while updating
//
// We do not incrementally update disk file, because this adds complexity.
// We save on agent shutdown, simply over writing all disk pages.
// If agent crashed, we are ready to start with stale or no mappings.

type MappingsCache struct {
	modifyMu  sync.Mutex      // allows consistency through modifier functions without taking mu.Lock
	itemCache []cacheKeyValue // protected by modifyMu

	mu      sync.RWMutex
	cache   map[string]cacheValue // no pointers in values for speed and GC efficiency
	sumSize int64
	sumTS   int64
	rnd     *rand.Rand

	// they rarely change, but still can change in runtime, and we must (slowly) adapt
	maxSize       atomic.Int64
	maxTTL        atomic.Int64 // older items will be unconditionally evicted
	accessTSGran  int          // increase it to reduce cost of updating access time
	testMode      bool         // for tests, check invariants which are not zero-cost
	deterministic bool         // for tests, makes structure more deterministic

	// statistics
	hits                 atomic.Int64
	misses               atomic.Int64
	timestampUpdates     atomic.Int64
	timestampUpdateSkips atomic.Int64
	evicts               atomic.Int64
	adds                 atomic.Int64

	// custom FS
	writeAt  func(offset int64, data []byte) error
	truncate func(offset int64) error
}

func elementSizeMem(s string) int64 {
	return int64(len(s)*5/4 + 32) // some simple approximation of actual cache element memory cost
}

func NewMappingsCache(maxSize int64, maxTTL int) *MappingsCache {
	c := &MappingsCache{
		cache:        map[string]cacheValue{},
		rnd:          rand.New(),
		accessTSGran: 1, // seconds
		writeAt:      func(offset int64, data []byte) error { return nil },
		truncate:     func(offset int64) error { return nil },
	}
	c.maxSize.Store(maxSize)
	c.maxTTL.Store(int64(maxTTL))
	return c
}

// fp, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0666) - recommended flags
// if fp nil, then cache works in memory-only mode
func LoadMappingsCacheFile(fp *os.File, maxSize int64, maxTTL int) (*MappingsCache, error) {
	c := NewMappingsCache(maxSize, maxTTL)
	w, t, r, fs := data_model.ChunkedStorageFile(fp)
	c.writeAt = w
	c.truncate = t
	err := c.load(fs, r)
	return c, err
}

func LoadMappingsCacheSlice(fp *[]byte, maxSize int64) *MappingsCache {
	c := NewMappingsCache(maxSize, 0)
	w, t, r, fs := data_model.ChunkedStorageSlice(fp)
	c.writeAt = w
	c.truncate = t
	_ = c.load(fs, r)
	return c
}

// Sensitive to latency
func (c *MappingsCache) GetValue(accessTS uint32, str string) (int32, bool) {
	c.mu.RLock()
	val, ok := c.cache[str]
	if !ok {
		c.mu.RUnlock()
		if c.testMode { // non-trivial cost, in production caller must track statistics
			c.misses.Add(1) // investigate cost of atomic++ here, move up if noticeable
		}
		return 0, false
	}
	if int64(val.accessTS) >= int64(accessTS) {
		c.mu.RUnlock()
		if c.testMode { // non-trivial cost, in production caller must track statistics
			c.hits.Add(1)
		}
		return val.value, true
	}
	c.mu.RUnlock()
	// code below runs ~once per element every accessTSGran
	if !c.mu.TryLock() {
		c.timestampUpdateSkips.Add(1)
		return val.value, true // there is cache update performing now, do not update access time at all
	}
	defer c.mu.Unlock()
	val, ok = c.cache[str]
	if !ok {
		if c.testMode { // non-trivial cost, in production caller must track statistics
			c.misses.Add(1)
		}
		return 0, false // simplify accounting by never adding back elements, only updating accessTS
	}
	accessTS += c.rnd.Uint32n(uint32(c.accessTSGran)) // spread updates across time, reducing cost of code below TryLock 2x, 8x, 64x, etc.
	c.addSumTSLocked(-int64(val.accessTS))
	val.accessTS = accessTS
	c.addSumTSLocked(int64(accessTS))
	c.cache[str] = val
	c.timestampUpdates.Add(1)
	return val.value, true
}

func (c *MappingsCache) GetValueBytes(accessTS uint32, str []byte) (int32, bool) {
	c.mu.RLock()
	val, ok := c.cache[string(str)]
	if !ok {
		c.mu.RUnlock()
		if c.testMode { // non-trivial cost, in production caller must track statistics
			c.misses.Add(1) // investigate cost of atomic++ here, move up if noticeable
		}
		return 0, false
	}
	if int64(val.accessTS) >= int64(accessTS) {
		c.mu.RUnlock()
		if c.testMode { // non-trivial cost, in production caller must track statistics
			c.hits.Add(1)
		}
		return val.value, true
	}
	c.mu.RUnlock()
	// code below runs ~once per element every accessTSGran
	if !c.mu.TryLock() {
		c.timestampUpdateSkips.Add(1)
		return val.value, true // there is cache update performing now, do not update access time at all
	}
	defer c.mu.Unlock()
	val, ok = c.cache[string(str)]
	if !ok {
		if c.testMode { // non-trivial cost, in production caller must track statistics
			c.misses.Add(1) // investigate cost of atomic++ here, move up if noticeable
		}
		return 0, false // simplify accounting by never adding back elements, only updating accessTS
	}
	accessTS += c.rnd.Uint32n(uint32(c.accessTSGran)) // spread updates across time, reducing cost of code below TryLock 2x, 8x, 64x, etc.
	c.addSumTSLocked(-int64(val.accessTS))
	val.accessTS = accessTS
	c.addSumTSLocked(int64(accessTS))
	c.cache[string(str)] = val
	c.timestampUpdates.Add(1)
	return val.value, true
}

func (c *MappingsCache) addSumTSLocked(a int64) {
	c.sumTS += a
	if c.sumTS < 0 {
		panic(fmt.Sprintf("sumTS negative %d after adding %d", c.sumTS, a))
	}
}

func (c *MappingsCache) addSumSizeLocked(a int64) {
	c.sumSize += a
	if c.sumSize < 0 {
		panic(fmt.Sprintf("sumSize negative %d after adding %d", c.sumSize, a))
	}
}

func (c *MappingsCache) expiredTTLLocked(itemAccessTS uint32, accessTS uint32, maxTTL int64) bool {
	return maxTTL > 0 && int64(itemAccessTS)+maxTTL < int64(accessTS)
}

func (c *MappingsCache) SetSizeTTL(maxSize int64, maxTTL int) {
	c.maxSize.Store(maxSize)
	c.maxTTL.Store(int64(maxTTL))
}

func (c *MappingsCache) Stats() (elements int, sumSize int64, averageTS float64, adds int64, evicts int64, timestampUpdates int64, timestampUpdateSkips int64) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.cache) != 0 {
		averageTS = float64(c.sumTS) / float64(len(c.cache))
	}
	return len(c.cache), c.sumSize, averageTS, c.adds.Swap(0), c.evicts.Swap(0), c.timestampUpdates.Swap(0), c.timestampUpdateSkips.Swap(0)
}

func (c *MappingsCache) debugPrint(nowUnix uint32, w io.Writer) {
	// uncomment for tests/experiments
	//c.mu.Lock()
	//updates := c.updates
	//c.mu.Unlock()
	//_, _ = fmt.Fprintf(w, "len: %d sumSize: %d avgTS-nowUnix: %f hits/misses/failed/updates %d/%d/%d/%d\n", len(c.cache), c.sumSize, c.AverageTS()-float64(nowUnix), c.hits.Load(), c.misses.Load(), c.timestampUpdateSkips.Load(), updates)
}

// call periodically to reduce cache size.
// beware that after restart access times will be far in the past,
// and we would need some fresh statistics to avoid evicting useful elements.
// for example, start removing by ttl after agent worked for 5 minutes
func (c *MappingsCache) RemoveByTTL(maxCount int, nowUnix uint32) {
	maxTTL := c.maxTTL.Load()
	c.modifyMu.Lock()
	defer c.modifyMu.Unlock()

	c.mu.RLock()

	items := c.itemCache[:0]
	var sumDiskSize int64
	visitedCount := 0
	for k, p := range c.cache {
		if visitedCount >= maxCount {
			break
		}
		visitedCount++
		if !c.expiredTTLLocked(p.accessTS, nowUnix, maxTTL) {
			continue
		}
		items = append(items, cacheKeyValue{str: k, val: p})
		sumDiskSize += elementSizeMem(k)
	}
	c.itemCache = items // save reallocated for the next time
	c.mu.RUnlock()      // but due to modifyMu.Lock we always successfully upgrade to c.mu.Lock with no updates between
	// d1 := time.Since(a)

	if len(items) == 0 {
		return // avoid Lock because it will make GetValues stuck for a bit
	} // getting Lock is expensive for concurrent GetValues

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, p := range items {
		c.removeItem(p.str, p.val.value, p.val.accessTS)
	}
}

// Not sensitive to latency, called from sending goroutine after sendBucket returns.
// There is also limit on len(pairs) as aggregators will not sent too many of them per sendBucket.
func (c *MappingsCache) AddValues(nowUnix uint32, pairs []MappingPair) {
	maxSize := c.maxSize.Load()
	maxTTL := c.maxTTL.Load()

	// a := time.Now() // uncomment to investigate how fast are separate stages

	c.modifyMu.Lock()
	defer c.modifyMu.Unlock()

	c.mu.RLock()

	var newItemsSizeMem int64
	var newItemsSizeDisk int64
	nextPos := 0
	for _, p := range pairs { // some elements could be added already, do not remove excess elements to fit them
		if _, ok := c.cache[p.Str]; ok {
			continue
		}
		if len(p.Str) == 0 || p.Value == 0 || p.Value == format.TagValueIDMappingFlood || p.Value == format.TagValueIDDoesNotExist {
			continue // Protect against known markers. Skip instead of panic for now.
		}
		newItemsSizeMem += elementSizeMem(p.Str)
		newItemsSizeDisk += elementSizeDisk(p.Str)
		pairs[nextPos] = p
		nextPos++
	}
	pairs = pairs[:nextPos]
	if len(pairs) == 0 { // common case, do not take write lock
		c.mu.RUnlock()
		return
	}
	// d0 := time.Since(a)

	if c.sumSize+newItemsSizeMem <= maxSize {
		c.mu.RUnlock() // but due to modifyMu.Lock we always successfully upgrade to c.mu.Lock with no updates between
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, p := range pairs {
			c.addItem(p.Str, p.Value, nowUnix)
		}
		return
	}
	removeSize := c.sumSize + newItemsSizeMem - maxSize
	if removeSize > newItemsSizeMem && removeSize > c.sumSize/1024 { // if cache settings changed to lower size
		// we only remove no more than 0.1% of cache per iteration
		removeSize = c.sumSize / 1024
	}
	items := c.itemCache[:0]
	var removeItemsSizeDisk int64
	var foundSize int64
	foundCount := 0
	for k, p := range c.cache {
		wasFoundSize := foundSize
		foundSize += elementSizeMem(k)
		removeItemsSizeDisk += elementSizeDisk(k)
		items = append(items, cacheKeyValue{str: k, val: p})
		if foundSize < removeSize {
			continue
		}
		if wasFoundSize < removeSize {
			foundCount = len(items) // remember how many items was barely enough to remove
		}
		if len(items) >= 2*foundCount { // find 2x items necessary to free up space
			break
		}
	}
	c.mu.RUnlock() // but due to modifyMu.Lock we always successfully upgrade to c.mu.Lock with no updates between
	// d1 := time.Since(a)

	slices.SortFunc(items, func(a, b cacheKeyValue) int {
		if v := cmp.Compare(a.val.accessTS, b.val.accessTS); v != 0 || !c.deterministic {
			return v
		}
		return cmp.Compare(a.str, b.str)
	})

	// d2 := time.Since(a)
	c.mu.Lock()
	defer c.mu.Unlock()
	// dels := 0
	for _, p := range items {
		if p.val.accessTS >= nowUnix {
			break // stop at the first element with accessTS better than elements we add, we will not add new elements instead to avoid thrashing/rotation
		}
		if !c.expiredTTLLocked(p.val.accessTS, nowUnix, maxTTL) && c.sumSize+newItemsSizeMem <= maxSize {
			// so we will remove all expired items unconditionally,
			// then remove not expired until enough space is free
			break
		}
		c.removeItem(p.str, p.val.value, p.val.accessTS)
		// dels++
	}
	c.itemCache = items
	// d3 := time.Since(a)
	// ins := 0
	for _, p := range pairs {
		size := elementSizeMem(p.Str)
		if c.sumSize+size > maxSize {
			break // we did not remove enough elements because they were fresh, do not add new elements then
		}
		c.addItem(p.Str, p.Value, nowUnix)
		// ins++
	}
	// d4 := time.Since(a)
	// fmt.Printf("addPairsLocked len(pairs) len(items) dels ins: %d %d %d %d %v %v %v %v %v\n", len(pairs), len(items), dels, ins, d0, d1-d0, d2-d1, d3-d2, d4-d3)
	// 1024 2048 1023 1024 104.589µs 54.722µs 459.022µs 74.654µs 93.05µs
	// So GetValues are stuck on RLock for 0.2ms per AddValues call.
	// We expect single AddValues call per shard, so conveyor will be stuck for <1% time for 32 shards
}

func (c *MappingsCache) addItem(k string, v int32, accessTS uint32) {
	if c.testMode {
		if _, ok := c.cache[k]; ok {
			panic("adding existing item")
		}
	}
	c.addSumSizeLocked(elementSizeMem(k))
	c.addSumTSLocked(int64(accessTS))
	c.cache[k] = cacheValue{value: v, accessTS: accessTS}
	c.adds.Add(1)
}

func (c *MappingsCache) removeItem(k string, v int32, accessTS uint32) {
	if c.testMode {
		ex, ok := c.cache[k]
		if !ok { // check existence for correct accounting
			panic("removing not existing item")
		}
		if ex.accessTS != accessTS || ex.value != v {
			panic(fmt.Sprintf("removing wrong item %d %d %d %d", ex.accessTS, accessTS, ex.value, v))
		}
	}
	size := elementSizeMem(k)
	c.addSumSizeLocked(-size)
	c.addSumTSLocked(-int64(accessTS))
	delete(c.cache, k)
	c.evicts.Add(1)
}

func elementSizeDisk(k string) int64 { // max, can be less if short string, requires no padding, etc.
	return 4 + int64(len(k)) + 3 + 4 + 4 // strlen, string, padding, value accessTS
}

func (c *MappingsCache) Save() error {
	// We exclude writers so that they do not block on Lock() while code below runs in RLock().
	// If we allow this, all new readers (GetValue) block on RLock(), effectively waiting for Save to finish.
	c.modifyMu.Lock()
	defer c.modifyMu.Unlock()

	c.mu.RLock()
	defer c.mu.RUnlock()

	saver := data_model.ChunkedStorageSaver{
		WriteAt:  c.writeAt,
		Truncate: c.truncate,
	}

	chunk := saver.StartWrite(data_model.ChunkedMagicMappings, 0)

	appendItem := func(k string, v int32, accessTS uint32) error {
		chunk = basictl.StringWrite(chunk, k)
		chunk = basictl.IntWrite(chunk, v)
		chunk = basictl.NatWrite(chunk, accessTS)
		var err error
		chunk, err = saver.FinishItem(chunk)
		return err
	}

	if c.deterministic { // for deterministic tests
		items := make([]cacheKeyValue, 0, len(c.cache))
		for k, p := range c.cache {
			items = append(items, cacheKeyValue{str: k, val: p})
		}
		slices.SortFunc(items, func(a, b cacheKeyValue) int {
			if v := cmp.Compare(a.val.accessTS, b.val.accessTS); v != 0 {
				return v
			}
			return cmp.Compare(a.str, b.str)
		})
		for _, p := range items {
			if err := appendItem(p.str, p.val.value, p.val.accessTS); err != nil {
				return err
			}
		}
	} else {
		for k, p := range c.cache {
			if err := appendItem(k, p.value, p.accessTS); err != nil {
				return err
			}
		}
	}
	return saver.FinishWrite(chunk)
}

// callers are expected to ignore error from this method
// if file tail is broken, it will be truncated on the next save
func (c *MappingsCache) load(fileSize int64, readAt func(b []byte, offset int64) error) error {
	c.modifyMu.Lock()
	defer c.modifyMu.Unlock()

	c.mu.Lock()
	defer c.mu.Unlock()

	loader := data_model.ChunkedStorageLoader{ReadAt: readAt}

	loader.StartRead(fileSize, data_model.ChunkedMagicMappings)
	for {
		chunk, _, err := loader.ReadNext()
		if err != nil {
			return err
		}
		if len(chunk) == 0 {
			break
		}
		var val cacheKeyValue
		for len(chunk) != 0 {
			var err error
			if chunk, err = basictl.StringRead(chunk, &val.str); err != nil {
				return err
			}
			if chunk, err = basictl.IntRead(chunk, &val.val.value); err != nil {
				return err
			}
			if chunk, err = basictl.NatRead(chunk, &val.val.accessTS); err != nil {
				return err
			}
			if _, ok := c.cache[val.str]; ok { // check existence for correct accounting
				continue
			}
			c.addSumSizeLocked(elementSizeMem(val.str))
			c.addSumTSLocked(int64(val.val.accessTS))
			c.cache[val.str] = val.val
		}
	}
	return nil
}
