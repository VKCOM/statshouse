package api

import (
	"context"
	"sync"
	"time"
	"unsafe"

	"github.com/vkcom/statshouse/internal/data_model"
	"go.uber.org/atomic"
	"golang.org/x/exp/slices"
)

var sizeofCache2Time = int(unsafe.Sizeof(int(0)))
var sizeofCache2Chunk = int(unsafe.Sizeof(cache2Chunk{}))
var sizeofCache2Map = int(unsafe.Sizeof(cache2Map{}))
var sizeofCache2DataCol = int(unsafe.Sizeof([]tsSelectRow(nil)))
var sizeofCache2DataRow = int(unsafe.Sizeof(tsSelectRow{}))

type cache2 struct {
	chunkCount atomic.Int32

	mu          sync.Mutex
	curSize     int64
	maxSize     int64
	trimRunning bool // when maxSize < curSize

	// readonly after init
	items     map[string]map[int64]*cache2Map // by version, step
	loc       *time.Location
	utcOffset int
}

type cache2Map struct {
	mu      sync.Mutex
	items   map[string]*cache2Series
	lru     cache2SeriesList
	lruNext *cache2Series // LRU iterator

	// readonly after init
	cache *cache2
}

type cache2SeriesList struct {
	head *cache2Series
}

type cache2Series struct {
	mu             sync.RWMutex
	time           []int // sorted
	chunks         []*cache2Chunk
	lastAccessedAt int

	// readonly after init
	key   string
	cache *cache2

	// protected by "cache2Map" mutex
	prev *cache2Series // LRU double <-
	next *cache2Series // -> linked list
}

type cache2Chunk struct {
	mu            sync.Mutex
	cache         *cache2 // nil if removed
	start, end    int     // [start, end)
	sizeofData    int
	data          cache2Data
	waiting       []cache2Waiting
	invalidatedAt int64
	loadStartedAt int64

	// protected by "cache2Series" mutex
	lastAccessedAt int
}

type cache2Waiting struct {
	data   cache2Data
	offset int
	c      chan<- error
}

type cache2SeriesLoader struct {
	series      *cache2Series
	gracePeriod time.Duration
	now         time.Time
	lod         *data_model.LOD
	chunks      []cache2ChunkLoader
	waitC       chan error
	waitN       int

	chunksAdded    int
	bytesAllocated int
}

type cache2ChunkLoader struct {
	src       *cache2Chunk
	dst       cache2Data
	dstOffset int
	load      bool
}

type cache2Data = [][]tsSelectRow

func newCache2(loc *time.Location, utcOffset int) *cache2 {
	res := &cache2{
		items:     make(map[string]map[int64]*cache2Map),
		loc:       loc,
		utcOffset: utcOffset,
	}
	for version, v := range data_model.LODTables {
		res.items[version] = make(map[int64]*cache2Map, len(v))
		for stepSec := range v {
			res.items[version][stepSec] = &cache2Map{
				lru:   newCache2SeriesList(),
				cache: res,
			}
		}
	}
	return res
}

func (c *cache2) Get(ctx context.Context, h *requestHandler, b *queryBuilder, lod data_model.LOD, avoidCache bool) (cache2Data, error) {
	n, err := lod.IndexOf(lod.ToSec)
	if err != nil {
		return nil, err
	}
	res := make(cache2Data, n)
	v, d := c.items[lod.Version][lod.StepSec].getOrCreateValue(b)
	if d != 0 {
		c.sizeAdd(d)
	}
	v.get(ctx, h, b, &lod, avoidCache, res)
	return res, err
}

func (c *cache2) setMaxSize(v int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.maxSize = v
	if c.maxSize != 0 && c.curSize > c.maxSize && !c.trimRunning {
		go c.trim()
		c.trimRunning = true
	}
}

func (c *cache2) size() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.curSize
}

func (c *cache2) sizeAdd(delta int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.curSize += int64(delta)
	if c.maxSize != 0 && c.curSize > c.maxSize && !c.trimRunning {
		go c.trim()
		c.trimRunning = true
	}
}

func (c *cache2) trim() {
	defer c.setTrimNotRunning()
	periodSec := []int{
		3600, // an hour
		1800, // 30 minutes
		1200, // 20 minutes
		600,  // 10 minutes
		300,  // 5 minutes
		60,   // a minute
	}
	for i := 0; i < len(periodSec); i++ {
		c.removeUnused(periodSec[i])
		if c.memoryUsageWithinLimit() {
			return
		}
	}
	const maxBackoffPeriodSec = 10
	backoffPeriodSec := 1
	for {
		// could not free up enough memory, backoff
		time.Sleep(time.Duration(backoffPeriodSec) * time.Second)
		if backoffPeriodSec != maxBackoffPeriodSec {
			backoffPeriodSec *= 2
			if backoffPeriodSec > maxBackoffPeriodSec {
				backoffPeriodSec = maxBackoffPeriodSec
			}
		}
		// and try again
		c.removeUnused(maxBackoffPeriodSec)
		if c.memoryUsageWithinLimit() {
			return
		}
	}
}

func (c *cache2) setTrimNotRunning() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.trimRunning = false
}

func (c *cache2) removeUnused(period int) {
	t := int(time.Now().Unix()) - period
	for _, m := range c.items {
		for _, v := range m {
			v.removeUnusedAfter(t)
		}
	}
}

func (c *cache2Map) removeUnusedAfter(t int) {
	v, n := c.lruIteratorStart()
	for i := 0; i < n && v != nil; i++ {
		var chunksRemoved int32
		var bytesFreed int
		if v.unusedAfter(t) {
			chunksRemoved, bytesFreed = c.remove(v)
		} else {
			chunksRemoved, bytesFreed = v.removeUnusedAfter(t)
		}
		if chunksRemoved != 0 {
			c.cache.chunkCount.Add(-chunksRemoved)
			c.cache.sizeAdd(-bytesFreed)
		}
		v = c.lruIteratorNext()
	}
}

func (c *cache2) memoryUsageWithinLimit() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.curSize <= c.maxSize
}

func (c *cache2) invalidate(stepSec int64, times []int64) {
	if len(times) == 0 {
		return
	}
	var s []int
	now := time.Now()
	nowUnixNano := now.UnixNano()
	start, d := c.chunkStartDuration(int(times[0]), int(stepSec))
	end := c.chunkEnd(start, d)
	s = append(s[:0], end)
	for i := 1; i < len(times); i++ {
		if t := int(times[i]); end <= t {
			end = c.chunkEnd(c.chunkStart(t, d), d)
			s = append(s, end)
		}
	}
	c.items[Version2][stepSec].invalidate(s, nowUnixNano)
	c.items[Version3][stepSec].invalidate(s, nowUnixNano)
}

func (c *cache2Map) getOrCreateValue(b *queryBuilder) (*cache2Series, int) {
	k := b.getOrBuildCacheKey()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.items == nil {
		c.items = make(map[string]*cache2Series)
	}
	var sizeDelta int
	var res *cache2Series
	if res = c.items[k]; res == nil {
		res = &cache2Series{
			key:   k,
			cache: c.cache,
		}
		c.items[k] = res
		sizeDelta = sizeofCache2Map + len(k) // good enough key length estimate
	} else {
		if res == c.lruNext {
			c.lruNext = c.lru.next(res)
		}
		c.lru.remove(res)
	}
	c.lru.add(res)
	return res, sizeDelta
}

func (c *cache2Map) remove(v *cache2Series) (chunksRemoved int32, bytesFreed int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items[v.key] = nil
	delete(c.items, v.key)
	c.lru.remove(v)
	return int32(len(v.chunks)), sizeofCache2Chunks(v.chunks)
}

func (c *cache2Map) invalidate(times []int, now int64) {
	for _, v := range c.items {
		v.invalidate(times, now)
	}
}

func (c *cache2Map) lruIteratorStart() (*cache2Series, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	res := c.lru.next(c.lru.head)
	if res == nil {
		return nil, 0
	}
	c.lruNext = c.lru.next(res)
	return res, len(c.items)
}

func (c *cache2Map) lruIteratorNext() *cache2Series {
	c.mu.Lock()
	defer c.mu.Unlock()
	res := c.lruNext
	if res == nil {
		return nil
	}
	c.lruNext = c.lru.next(res)
	return res
}

func (c *cache2Series) get(ctx context.Context, h *requestHandler, b *queryBuilder, lod *data_model.LOD, _ bool, data cache2Data) error {
	if len(data) == 0 {
		return nil
	}
	l := c.newSeriesLoader(h, lod, data)
	if l.chunksAdded != 0 {
		c.cache.chunkCount.Add(int32(l.chunksAdded))
		c.cache.sizeAdd(l.bytesAllocated)
	}
	for i := 0; i < len(l.chunks); {
		if !l.chunks[i].load {
			i++
			continue
		}
		j := i
		i++
		for i < len(l.chunks) && l.chunks[i].load && l.chunks[i-1].src.end == l.chunks[i].src.start {
			i++
		}
		go l.load(l.chunks[j:i], h, b)
	}
	for i := 0; i < l.waitN; i++ {
		select {
		case err := <-l.waitC:
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for i := 0; i < len(l.chunks); i++ {
		l.chunks[i].copyData()
	}
	return nil
}

func (c *cache2Series) invalidate(time []int, now int64) {
	if len(time) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.time) == 0 {
		return
	}
	if time[len(time)-1] < c.time[0] || c.time[len(c.time)-1] < time[0] {
		return
	}
	for i, j := 0, 0; i < len(time) && j < len(c.time); {
		for i < len(time) && time[i] < c.time[j] {
			i++
		}
		for j < len(c.time) && c.time[j] < time[i] {
			j++
		}
		for i < len(time) && j < len(c.time) && time[i] == c.time[j] {
			c.chunks[j].invalidate(now)
			i++
			j++
		}
	}
}

func (c *cache2Series) newSeriesLoader(h *requestHandler, lod *data_model.LOD, data cache2Data) cache2SeriesLoader {
	c.mu.Lock()
	defer c.mu.Unlock()
	var now = time.Now()
	var res = cache2SeriesLoader{
		series:      c,
		now:         now,
		gracePeriod: cache2InvalidDataGracePeriod(h, lod),
		lod:         lod,
		waitC:       make(chan error),
	}
	var time []int
	var t = int(lod.FromSec)
	var chunks []*cache2Chunk
	var i, _ = slices.BinarySearch(c.time, t)
	for len(data) != 0 {
		time = time[:0]
		chunks = chunks[:0]
		for len(data) != 0 && (i == len(c.chunks) || !c.chunks[i].contains(t)) {
			start, d := c.cache.chunkStartDuration(t, int(lod.StepSec))
			v := &cache2Chunk{
				cache: c.cache,
				start: start,
				end:   c.cache.chunkEnd(start, d),
			}
			data, t = res.addChunk(v, data, t)
			time = append(time, v.end)
			chunks = append(chunks, v)
		}
		if len(time) == 0 {
			data, t = res.addChunk(c.chunks[i], data, t)
			i++
		} else {
			c.time = append(append(c.time[:i], time...), c.time[i:]...)
			c.chunks = append(append(c.chunks[:i], chunks...), c.chunks[i:]...)
			res.chunksAdded += len(chunks)
			res.bytesAllocated += sizeofCache2Chunks(chunks)
			i += len(time)
		}
	}
	c.lastAccessedAt = int(now.Unix())
	return res
}

func (c *cache2Series) removeUnusedAfter(t int) (chunksRemoved int32, bytesFreed int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := 0; i < len(c.chunks); {
		for i < len(c.chunks) && c.chunks[i].lastAccessedAt >= t {
			i++
		}
		if i == len(c.chunks) {
			break
		}
		j := i + 1
		for j < len(c.chunks) && c.chunks[j].lastAccessedAt < t {
			j++
		}
		chunks := c.chunks[i:j]
		for _, v := range chunks {
			v.mu.Lock()
			v.data = nil  // help GC
			v.cache = nil // detach
			v.mu.Unlock()
		}
		chunksRemoved += int32(len(chunks))
		bytesFreed += sizeofCache2Chunks(chunks)
		k := i
		for m := j; m < len(c.chunks); m++ {
			c.time[k] = c.time[m]
			c.chunks[k] = c.chunks[m]
			k++
		}
		c.time = c.time[:k]
		c.chunks = c.chunks[:k]
		i = j
	}
	return chunksRemoved, bytesFreed
}

func (c *cache2Series) unusedAfter(t int) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lastAccessedAt <= t
}

func (l *cache2SeriesLoader) addChunk(c *cache2Chunk, data cache2Data, t int) (cache2Data, int) {
	var n int
	var await bool
	var startLoad bool
	var stepSec = int(l.lod.StepSec)
	var offset = (t - c.start) / stepSec
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.data == nil {
		startLoad = true
		await = true
	} else if c.invalidatedAt != 0 {
		startLoad = true
		if d := time.Duration(l.now.UnixNano() - c.invalidatedAt); d >= l.gracePeriod {
			await = true
		}
	}
	if await {
		n = cache2ChunkSize(stepSec) - offset
		if n > len(data) {
			n = len(data)
		}
		c.waiting = append(c.waiting, cache2Waiting{
			data:   data[:n],
			offset: offset,
			c:      l.waitC,
		})
		l.waitN++
	} else {
		n = len(c.data)
		if n > len(data) {
			n = len(data)
		}
	}
	v := cache2ChunkLoader{
		src:       c,
		dst:       data[:n],
		dstOffset: offset,
	}
	if startLoad && c.loadStartedAt == 0 {
		v.load = true
		c.loadStartedAt = l.now.UnixNano()
	}
	l.chunks = append(l.chunks, v)
	c.lastAccessedAt = int(l.now.Unix())
	return data[n:], c.end
}

func (c cache2ChunkLoader) copyData() {
	c.src.mu.Lock()
	defer c.src.mu.Unlock()
	if len(c.src.data) != 0 {
		copy(c.dst, c.src.data[c.dstOffset:])
	}
}

func (l cache2SeriesLoader) load(s []cache2ChunkLoader, h *requestHandler, b *queryBuilder) {
	var lod = data_model.LOD{
		FromSec:    int64(s[0].src.start),
		ToSec:      int64(s[len(s)-1].src.end),
		StepSec:    l.lod.StepSec,
		Version:    l.lod.Version,
		Table:      l.lod.Table,
		HasPreKey:  l.lod.HasPreKey,
		PreKeyOnly: l.lod.PreKeyOnly,
		Location:   l.lod.Location,
	}
	var n = cache2ChunkSize(int(lod.StepSec))
	var res = make(cache2Data, n*len(s))
	var _, err = loadPoints(context.Background(), h, b, lod, res, 0)
	var start, end = 0, n
	for _, c := range s {
		var sizeChange int
		var data cache2Data
		var waiting []cache2Waiting
		var chunk = c.src
		chunk.mu.Lock()
		chunk.waiting, waiting = waiting, chunk.waiting
		if err == nil {
			data = res[start:end]
			if chunk.cache != nil {
				c.dst = data
				if chunk.loadStartedAt < chunk.invalidatedAt {
					// invalidate while load
				} else {
					chunk.invalidatedAt = 0
				}
				newSize := sizeofCache2Data(data)
				sizeChange = newSize - chunk.sizeofData
				chunk.sizeofData = newSize
			}
		}
		chunk.loadStartedAt = 0
		chunk.mu.Unlock()
		for _, w := range waiting {
			if err == nil {
				copy(w.data, data[w.offset:])
				if chunk.cache != nil && sizeChange != 0 {
					chunk.cache.sizeAdd(sizeChange)
				}
			}
			w.c <- err
		}
		start = end
		end += n
	}
}

func (c *cache2Chunk) invalidate(at int64) {
	c.mu.Lock()
	c.invalidatedAt = at
	c.mu.Unlock()
}

func (c *cache2Chunk) contains(t int) bool {
	return c.start <= t && t < c.end
}

func newCache2SeriesList() cache2SeriesList {
	dummy := &cache2Series{} // simplifies list management
	dummy.prev = dummy
	dummy.next = dummy
	return cache2SeriesList{head: dummy}
}

func (l cache2SeriesList) add(v *cache2Series) {
	v.prev = l.head.prev
	v.next = l.head
	l.head.prev.next = v
	l.head.prev = v
}

func (l cache2SeriesList) remove(v *cache2Series) {
	v.prev.next = v.next
	v.next.prev = v.prev
	v.prev = nil
	v.next = nil
}

func (l cache2SeriesList) next(v *cache2Series) *cache2Series {
	if v.next == l.head {
		return nil
	}
	return v.next
}

func (l cache2SeriesList) len() (n int) {
	// linear time, for unit tests
	for v := l.head.next; ; v = v.next {
		if v == l.head {
			return n
		}
		n++
	}
}

func (c *cache2) chunkStartDuration(t, stepSec int) (int, int) {
	d := cache2ChunkDuration(int(stepSec))
	return c.chunkStart(t, d), d
}

func (c *cache2) chunkStart(t, d int) int {
	switch d {
	case 604800: // a week
		return ((t+c.utcOffset)/d)*d - c.utcOffset
	case 2678400: // 31 day, a month magic number
		t := time.Unix(int64(t), 0).In(c.loc)
		return int(time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, c.loc).UTC().Unix())
	default:
		return (int(t) / d) * d
	}
}

func (c *cache2) chunkEnd(t, d int) int {
	switch d {
	case 2678400: // 31 day, a month magic number
		return int(time.Unix(int64(t), 0).In(c.loc).AddDate(0, 1, 0).UTC().Unix())
	default:
		return t + d
	}
}

func sizeofCache2Chunks(s []*cache2Chunk) int {
	res := len(s) * (sizeofCache2Time + sizeofCache2Chunk) // account for "cache2Series" time slice
	for i := 0; i < len(s); i++ {
		res += s[i].sizeofData
	}
	return res
}

func sizeofCache2Data(data cache2Data) int {
	res := len(data) * sizeofCache2DataCol
	for i := 0; i < len(data); i++ {
		res += len(data[i]) * sizeofCache2DataRow
	}
	return res
}

func cache2ChunkSize(stepSec int) int {
	// NB! keep in sync with "tsCache2ChunkDuration"
	if stepSec < 60 {
		// max 120 points (two minutes) from second table
		return 120 / stepSec
	} else if stepSec < 3600 {
		// max 120 points (two hours) from minute table
		return 7200 / stepSec
	} else if stepSec == 3600 {
		// max 24 points (two days) from hour table
		return 24
	} else {
		return 1 // for week or month
	}
}

func cache2ChunkDuration(stepSec int) int {
	// NB! keep in sync with "tsCache2ChunkSize"
	if stepSec < 60 {
		return 120 // two minutes
	} else if stepSec < 3600 {
		return 7200 // two hours
	} else if stepSec == 3600 {
		return 86400 // a day
	} else {
		return stepSec
	}
}

func cache2InvalidDataGracePeriod(h *requestHandler, lod *data_model.LOD) time.Duration {
	if h.pickyUser() {
		return 0
	}
	return time.Duration(lod.StepSec) * time.Second
}

func cacheGet(ctx context.Context, h *requestHandler, pq *queryBuilder, lod data_model.LOD, avoidCache bool) ([][]tsSelectRow, error) {
	if h.CacheVersion2.Load() {
		return h.cache2.Get(ctx, h, pq, lod, avoidCache)
	} else {
		return h.cache.Get(ctx, h, pq, lod, avoidCache)
	}
}
