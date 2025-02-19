package api

import (
	"context"
	"sync"
	"time"
	"unsafe"

	"github.com/vkcom/statshouse-go"
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/vkgo/srvfunc"
	"golang.org/x/exp/slices"
)

const timeDay = 24 * time.Hour
const timeMonth = 31 * 24 * time.Hour

var sizeofCache2Time = int(unsafe.Sizeof(int(0)))
var sizeofCache2Chunk = int(unsafe.Sizeof(cache2Chunk{}))
var sizeofCache2Map = int(unsafe.Sizeof(cache2Map{}))
var sizeofCache2Series = int(unsafe.Sizeof(cache2Series{}))
var sizeofCache2DataCol = int(unsafe.Sizeof([]tsSelectRow(nil)))
var sizeofCache2DataRow = int(unsafe.Sizeof(tsSelectRow{}))

type cache2 struct {
	mu       sync.Mutex
	trimCond sync.Cond
	info     cache2RuntimeInfo
	limits   cache2Limits

	// readonly after init
	items     map[string]map[time.Duration]*cache2Map // by version, step
	handler   *Handler
	location  *time.Location
	utcOffset int64

	// debug log
	debugLogMu sync.Mutex
	debugLogS  [100]string
	debugLogX  int
}

type cache2RuntimeInfo struct {
	sizeInBytes        int
	chunkCount         int
	minChunkAccessTime int64
}

type cache2UpdateInfo struct {
	sizeInBytesDelta       int
	chunkCountDelta        int
	minChunkAccessTimeSeen int64
}

type cache2Limits struct {
	maxSizeInBytes int
	maxAge         time.Duration
}

type cache2Map struct {
	mu             sync.Mutex
	seriesM        map[string]*cache2Series
	seriesL        cache2SeriesList // "seriesM" values in a list
	trimIter       *cache2Series
	invalidateIter *cache2Series

	// readonly after init
	cache *cache2
}

type cache2SeriesList struct {
	head *cache2Series
}

type cache2Series struct {
	mu         sync.RWMutex
	time       []int64        // sorted
	chunks     []*cache2Chunk // same length and order as "time"
	accessTime int64

	// readonly after init
	key   string
	cache *cache2

	// protected by "cache2Map" mutex
	prev *cache2Series // LRU double <-
	next *cache2Series // -> linked list
}

type cache2Chunk struct {
	mu         sync.Mutex
	cache      *cache2 // nil if removed
	start, end int64   // [start, end)
	dataSize   int     // in bytes
	data       cache2Data
	waiting    []cache2Waiting

	invalidatedAt int64
	loadStartedAt int64
	loading       bool

	// protected by "cache2Series" mutex
	accessTime int64
}

type cache2Waiting struct {
	data   cache2Data
	offset int
	c      chan<- error
}

type cache2SeriesLoader struct {
	h                 *requestHandler
	b                 *queryBuilder
	lod               *data_model.LOD
	forceLoad         bool
	series            *cache2Series
	now               int64
	staleAcceptPeriod time.Duration
	chunks            []*cache2Chunk
	waitC             chan error
	waitN             int
}

type cache2Data = [][]tsSelectRow

func newCache2(h *Handler) *cache2 {
	res := &cache2{
		items:     make(map[string]map[time.Duration]*cache2Map),
		handler:   h,
		location:  h.location,
		utcOffset: h.utcOffset * int64(time.Second),
		info: cache2RuntimeInfo{
			minChunkAccessTime: time.Now().UnixNano(),
		},
	}
	res.trimCond = *sync.NewCond(&res.mu)
	for version, v := range data_model.LODTables {
		res.items[version] = make(map[time.Duration]*cache2Map, len(v))
		for stepSec := range v {
			res.items[version][time.Duration(stepSec)*time.Second] = &cache2Map{
				seriesM: make(map[string]*cache2Series),
				seriesL: newCache2SeriesList(),
				cache:   res,
			}
			res.info.sizeInBytes += sizeofCache2Map
		}
	}
	go res.trim()
	return res
}

func (c *cache2) Get(ctx context.Context, h *requestHandler, b *queryBuilder, lod data_model.LOD, avoidCache bool) (cache2Data, error) {
	n, err := lod.IndexOf(lod.ToSec)
	if err != nil {
		return nil, err
	}
	info := cache2UpdateInfo{}
	data := make(cache2Data, n)
	v := c.items[lod.Version][time.Duration(lod.StepSec)*time.Second].getOrCreateValue(b, &info)
	err = v.get(ctx, h, b, &lod, avoidCache, data, &info)
	c.updateRuntimeInfo(info)
	return data, err
}

func (c *cache2) setLimits(v cache2Limits) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.limits != v {
		c.limits = v
		c.debugPrintRuntimeInfoUnlocked("set limits")
		c.trimCond.Signal()
	}
}

func (c *cache2) updateRuntimeInfo(v cache2UpdateInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// chunk info
	c.info.chunkCount += v.chunkCountDelta
	if c.info.minChunkAccessTime < v.minChunkAccessTimeSeen {
		c.info.minChunkAccessTime = v.minChunkAccessTimeSeen
	}
	// memory usage
	c.info.sizeInBytes += v.sizeInBytesDelta
	if !c.memoryUsageWithinLimitUnlocked() {
		c.trimCond.Signal()
	}
}

func (c *cache2) runtimeInfo() cache2RuntimeInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.info
}

func (c *cache2) memoryUsageWithinLimit() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.memoryUsageWithinLimitUnlocked()
}

func (c *cache2) memoryUsageWithinLimitUnlocked() bool {
	return c.limits.maxSizeInBytes <= 0 || c.info.sizeInBytes <= c.limits.maxSizeInBytes
}

func (r cache2RuntimeInfo) age() time.Duration {
	return time.Since(time.Unix(0, r.minChunkAccessTime))
}

func (c *cache2) sendMetrics(client *statshouse.Client) {
	v := c.runtimeInfo()
	tags := statshouse.Tags{1: srvfunc.HostnameForStatshouse()}
	// TODO: replace with builtins
	client.Count("statshouse_api_cache_chunk_count", tags, float64(v.chunkCount))
	client.Value("statshouse_api_cache_size", tags, float64(v.sizeInBytes))
	client.Value("statshouse_api_cache_age", tags, v.age().Seconds())
}

func (c *cache2) reset() {
	c.debugPrintRuntimeInfo("reset start")
	res := cache2UpdateInfo{
		minChunkAccessTimeSeen: time.Now().UnixNano(),
	}
	for _, m := range c.items {
		for _, m := range m {
			m.reset(&res)
		}
	}
	c.updateRuntimeInfo(res)
	c.debugPrintRuntimeInfo("reset end")
}

func (c *cache2) trim() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for {
		if c.limits.maxAge > 0 && c.info.age() > c.limits.maxAge {
			maxAge := c.limits.maxAge
			c.mu.Unlock()
			c.debugPrint("trim aged")
			c.removeOlderThan(maxAge)
			c.mu.Lock()
		}
		if c.limits.maxSizeInBytes > 0 && c.info.sizeInBytes > c.limits.maxSizeInBytes {
			c.mu.Unlock()
			c.debugPrint("trim size")
			c.reduceMemoryUsage()
			if v := c.handler.CacheTrimBackoffPeriod.Load(); v > 0 {
				d := time.Duration(v) * time.Second
				c.debugPrintf("trim backoff for %s", d)
				time.Sleep(d)
			}
			c.mu.Lock()
		}
		t := time.AfterFunc(c.limits.maxAge-c.info.age(), c.trimCond.Signal)
		c.trimCond.Wait()
		t.Stop()
	}
}

func (c *cache2) reduceMemoryUsage() {
	s := []time.Duration{
		time.Hour,
		45 * time.Minute,
		30 * time.Minute,
		15 * time.Minute,
		time.Minute,
		45 * time.Second,
		30 * time.Second,
		15 * time.Second,
		time.Second,
	}
	i := 0
	for v := c.runtimeInfo().age(); i < len(s) && v <= s[i]; i++ {
		// pass
	}
	for ; i < len(s); i++ {
		c.removeOlderThan(s[i])
		if c.memoryUsageWithinLimit() {
			return
		}
	}
	for {
		// could not free up enough memory, backoff
		time.Sleep(time.Second)
		// and remove chunks not used while we were sleeping
		c.removeOlderThan(time.Second)
		if c.memoryUsageWithinLimit() {
			return
		}
	}
}

func (c *cache2) removeOlderThan(age time.Duration) int64 {
	c.debugPrintRuntimeInfof("remove older than %s", age)
	now := time.Now()
	res := cache2UpdateInfo{
		minChunkAccessTimeSeen: now.UnixNano(),
	}
	t := now.Add(-age).UnixNano()
	for _, m := range c.items {
		for _, m := range m {
			v := m.trimIteratorStart()
			for v != nil {
				if v.unusedAfter(t) {
					m.remove(v, &res)
				} else {
					v.removeUnusedAfter(t, &res)
				}
				v = m.trimIteratorNext()
			}
		}
	}
	c.updateRuntimeInfo(res)
	return res.minChunkAccessTimeSeen
}

func (c *cache2) invalidate(ts []int64, stepSec int64) {
	if len(ts) == 0 {
		return
	}
	var s []int64
	now := time.Now().UnixNano()
	step := time.Duration(stepSec) * time.Second
	start, d := c.chunkStartDuration(ts[0]*1e6, step)
	end := c.chunkEnd(start, d)
	s = append(s[:0], end)
	for i := 1; i < len(ts); i++ {
		if end <= ts[i] {
			end = c.chunkEnd(c.chunkStart(ts[i], d), d)
			s = append(s, end)
		}
	}
	c.items[Version2][step].invalidate(s, now)
	c.items[Version3][step].invalidate(s, now)
}

func (c *cache2Map) getOrCreateValue(b *queryBuilder, info *cache2UpdateInfo) *cache2Series {
	k := b.getOrBuildCacheKey()
	c.mu.Lock()
	defer c.mu.Unlock()
	if v := c.seriesM[k]; v != nil {
		return v
	}
	v := &cache2Series{
		key:   k,
		cache: c.cache,
	}
	c.seriesM[k] = v
	c.seriesL.add(v)
	info.sizeInBytesDelta += v.sizeInBytes()
	return v
}

func (c *cache2Map) reset(res *cache2UpdateInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, v := range c.seriesM {
		c.removeUnlocked(v, res)
	}
}

func (c *cache2Map) remove(v *cache2Series, res *cache2UpdateInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.removeUnlocked(v, res)
}

func (c *cache2Map) removeUnlocked(v *cache2Series, res *cache2UpdateInfo) {
	c.seriesM[v.key] = nil
	delete(c.seriesM, v.key)
	if c.trimIter == v {
		c.trimIter = c.seriesL.next(v)
	}
	if c.invalidateIter == v {
		c.invalidateIter = c.seriesL.next(v)
	}
	c.seriesL.remove(v)
	res.chunkCountDelta -= len(v.chunks)
	res.sizeInBytesDelta -= v.sizeInBytes()
}

func (c *cache2Series) sizeInBytes() int {
	return sizeofCache2Series +
		2*len(c.key) + // count both map key and value key
		sizeofCache2Chunks(c.chunks)
}

func (c *cache2Map) invalidate(ts []int64, now int64) {
	v := c.invalidateIteratorStart()
	for v != nil {
		v.invalidate(ts, now)
		v = c.invalidateIteratorNext()
	}
}

func (c *cache2Map) trimIteratorStart() *cache2Series {
	return c.iteratorStart(&c.trimIter)
}

func (c *cache2Map) trimIteratorNext() *cache2Series {
	return c.iteratorNext(&c.trimIter)
}

func (c *cache2Map) invalidateIteratorStart() *cache2Series {
	return c.iteratorStart(&c.invalidateIter)
}

func (c *cache2Map) invalidateIteratorNext() *cache2Series {
	return c.iteratorNext(&c.invalidateIter)
}

func (c *cache2Map) iteratorStart(iter **cache2Series) *cache2Series {
	c.mu.Lock()
	defer c.mu.Unlock()
	res := c.seriesL.next(c.seriesL.head)
	if res == nil {
		return nil
	}
	*iter = c.seriesL.next(res)
	return res
}

func (c *cache2Map) iteratorNext(iter **cache2Series) *cache2Series {
	c.mu.Lock()
	defer c.mu.Unlock()
	res := *iter
	if res == nil {
		return nil
	}
	*iter = c.seriesL.next(res)
	return res
}

func (c *cache2Series) get(ctx context.Context, h *requestHandler, b *queryBuilder, lod *data_model.LOD, forceLoad bool, data cache2Data, res *cache2UpdateInfo) error {
	if len(data) == 0 {
		return nil
	}
	l := cache2SeriesLoader{
		h:                 h,
		b:                 b,
		lod:               lod,
		forceLoad:         forceLoad,
		series:            c,
		now:               time.Now().UnixNano(),
		staleAcceptPeriod: cache2StaleAcceptPeriod(h),
	}
	l.loadSeries(c, lod, data, res)
	for i := 0; i < len(l.chunks); {
		j := i + 1
		for j < len(l.chunks) && l.chunks[j-1].end == l.chunks[j].start {
			j++
		}
		go l.loadChunk(l.chunks[i:j], h, b)
		i = j
	}
	if l.waitN == 0 {
		return nil
	}
	waitC := make(chan error, 1)
	go func() {
		var n int
		var err error
		for ; n < l.waitN && err == nil; n++ {
			err = <-l.waitC
		}
		for ; n < l.waitN; n++ {
			<-l.waitC
		}
		waitC <- err
	}()
	select {
	case err := <-waitC:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *cache2Series) invalidate(ts []int64, now int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.time) == 0 {
		return
	}
	if ts[len(ts)-1] < c.time[0] || c.time[len(c.time)-1] < ts[0] {
		return
	}
	for i, j := 0, 0; i < len(ts) && j < len(c.time); {
		for i < len(ts) && ts[i] < c.time[j] {
			i++
		}
		for j < len(c.time) && c.time[j] < ts[i] {
			j++
		}
		for i < len(ts) && j < len(c.time) && ts[i] == c.time[j] {
			c.chunks[j].invalidate(now)
			i++
			j++
		}
	}
}

func (res *cache2SeriesLoader) loadSeries(c *cache2Series, lod *data_model.LOD, data cache2Data, info *cache2UpdateInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.accessTime = res.now
	var ts []int64
	var chunks []*cache2Chunk
	t := lod.FromSec * int64(time.Second)
	i, _ := slices.BinarySearch(c.time, t)
	step := time.Duration(lod.StepSec) * time.Second
	for len(data) != 0 {
		ts = ts[:0]
		chunks = chunks[:0]
		for len(data) != 0 && (i == len(c.chunks) || !c.chunks[i].contains(t)) {
			start, d := c.cache.chunkStartDuration(t, step)
			v := &cache2Chunk{
				cache: c.cache,
				start: start,
				end:   c.cache.chunkEnd(start, d),
			}
			data, t = res.addChunk(v, data, t)
			ts = append(ts, v.end)
			chunks = append(chunks, v)
		}
		if len(ts) == 0 {
			data, t = res.addChunk(c.chunks[i], data, t)
			i++
		} else {
			c.time = append(append(c.time[:i], ts...), c.time[i:]...)
			c.chunks = append(append(c.chunks[:i], chunks...), c.chunks[i:]...)
			info.chunkCountDelta += len(chunks)
			info.sizeInBytesDelta += sizeofCache2Chunks(chunks)
			i += len(ts)
		}
	}
}

func (c *cache2Series) removeUnusedAfter(t int64, res *cache2UpdateInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := 0; i < len(c.chunks); {
		for i < len(c.chunks) && c.chunks[i].accessTime >= t {
			if res.minChunkAccessTimeSeen > c.chunks[i].accessTime {
				res.minChunkAccessTimeSeen = c.chunks[i].accessTime
			}
			i++
		}
		if i == len(c.chunks) {
			break
		}
		j := i + 1
		for j < len(c.chunks) && c.chunks[j].accessTime < t {
			j++
		}
		chunks := c.chunks[i:j]
		for _, v := range chunks {
			v.mu.Lock()
			v.data = nil  // help GC
			v.cache = nil // detach
			v.mu.Unlock()
		}
		res.chunkCountDelta -= len(chunks)
		res.sizeInBytesDelta -= sizeofCache2Chunks(chunks)
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
}

func (c *cache2Series) unusedAfter(t int64) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.accessTime <= t
}

func (l *cache2SeriesLoader) addChunk(c *cache2Chunk, data cache2Data, t int64) (cache2Data, int64) {
	var startLoad, await bool
	step := time.Duration(l.lod.StepSec) * time.Second
	offset := int(time.Duration(t-c.start) / step)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.data == nil || c.loadStartedAt < c.end || l.forceLoad {
		startLoad = true
		await = true
	} else if c.invalidatedAt != 0 {
		startLoad = true
		if time.Duration(l.now-c.invalidatedAt) >= l.staleAcceptPeriod {
			await = true
		}
	} else if c.loadStartedAt < (c.end + int64(invalidateLinger)) {
		startLoad = true
	}
	var n int
	if await {
		n = cache2ChunkSize(step) - offset
		if n > len(data) {
			n = len(data)
		}
		if l.waitC == nil {
			l.waitC = make(chan error)
		}
		c.waiting = append(c.waiting, cache2Waiting{
			data:   data[:n],
			offset: offset,
			c:      l.waitC,
		})
		l.waitN++
	} else {
		// map string tags and copy
		src := c.data[offset:]
		for i := 0; i < len(src) && i < len(data); i++ {
			for j := 0; j < len(src[i]); j++ {
				row := &src[i][j]
				for k := 0; k < len(row.stag) && row.stagCount != 0; k++ {
					if s := row.stag[k]; s != "" {
						var tag format.MetricMetaTag
						if 0 <= k && k < len(l.b.metric.Tags) {
							tag = l.b.metric.Tags[k]
						}
						if v, err := l.h.getRichTagValueID(&tag, l.h.version, s); err == nil {
							row.tag[k] = int64(v)
							row.stag[k] = ""
							row.stagCount--
						}
					}
				}
			}
			data[i] = make([]tsSelectRow, len(src[i]))
			copy(data[i], src[i])
			n++
		}
	}
	if startLoad && !c.loading {
		l.chunks = append(l.chunks, c)
		c.loading = true
		c.loadStartedAt = l.now
	}
	c.accessTime = l.now
	return data[n:], c.end
}

func (l cache2SeriesLoader) loadChunk(s []*cache2Chunk, h *requestHandler, b *queryBuilder) {
	lod := data_model.LOD{
		FromSec:    s[0].start / 1e9,
		ToSec:      s[len(s)-1].end / 1e9,
		StepSec:    l.lod.StepSec,
		Version:    l.lod.Version,
		Table:      l.lod.Table,
		HasPreKey:  l.lod.HasPreKey,
		PreKeyOnly: l.lod.PreKeyOnly,
		Location:   l.lod.Location,
	}
	n := cache2ChunkSize(time.Duration(lod.StepSec) * time.Second)
	res := make(cache2Data, n*len(s))
	_, err := loadPoints(context.Background(), h, b, lod, res, 0)
	if err == nil && b.metric != nil && len(b.by) != 0 {
		// map string tags
		for _, tagX := range b.by {
			var tag format.MetricMetaTag
			if 0 <= tagX && tagX < len(b.metric.Tags) {
				tag = b.metric.Tags[tagX]
			}
			for i := 0; i < len(res); i++ {
				for j := 0; j < len(res[i]); j++ {
					if s := res[i][j].stag[tagX]; s != "" {
						v, err := h.getRichTagValueID(&tag, h.version, s)
						if err == nil {
							res[i][j].tag[tagX] = int64(v)
							res[i][j].stag[tagX] = ""
						} else {
							res[i][j].stagCount++
						}
					}
				}
			}
		}
	}
	start, end := 0, n
	for _, c := range s {
		var data cache2Data
		var sizeInBytesDelta int
		var waiting []cache2Waiting
		c.mu.Lock()
		c.waiting, waiting = waiting, c.waiting
		if err == nil {
			data = res[start:end]
			if c.cache != nil {
				c.data = data
				if c.loadStartedAt < c.invalidatedAt {
					// invalidate while load
				} else {
					c.invalidatedAt = 0
				}
				newSize := sizeofCache2Data(data)
				sizeInBytesDelta = newSize - c.dataSize
				c.dataSize = newSize
			}
		}
		c.loading = false
		c.mu.Unlock()
		for _, w := range waiting {
			if err == nil {
				cache2DataCopy(w.data, data[w.offset:])
			}
			w.c <- err
		}
		if c.cache != nil && sizeInBytesDelta != 0 {
			c.cache.updateRuntimeInfo(cache2UpdateInfo{
				sizeInBytesDelta: sizeInBytesDelta,
			})
		}
		start = end
		end += n
	}
}

func (c *cache2Chunk) invalidate(now int64) {
	c.mu.Lock()
	c.invalidatedAt = now
	c.mu.Unlock()
}

func (c *cache2Chunk) contains(t int64) bool {
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

func (l cache2SeriesList) len() int {
	// linear time, for unit tests
	var n int
	for v := l.head.next; ; v = v.next {
		if v == l.head {
			return n
		}
		n++
	}
}

func (c *cache2) chunkStartDuration(t int64, step time.Duration) (int64, time.Duration) {
	d := cache2ChunkDuration(step)
	return c.chunkStart(t, d), d
}

func (c *cache2) chunkStart(t int64, d time.Duration) int64 {
	d64 := int64(d)
	if d < timeDay {
		return (t / d64) * d64
	} else if d < timeMonth {
		return ((t+c.utcOffset)/d64)*d64 - c.utcOffset
	} else {
		t := time.Unix(0, t).In(c.location)
		return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, c.location).UTC().UnixNano()
	}
}

func (c *cache2) chunkEnd(t int64, d time.Duration) int64 {
	switch d {
	case timeMonth:
		return time.Unix(0, t).In(c.location).AddDate(0, 1, 0).UTC().UnixNano()
	default:
		return t + int64(d)
	}
}

func sizeofCache2Chunks(s []*cache2Chunk) int {
	res := len(s) * (sizeofCache2Time + sizeofCache2Chunk) // account for "cache2Series" time slice
	for i := 0; i < len(s); i++ {
		res += s[i].dataSize
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

func cache2ChunkSize(step time.Duration) int {
	// NB! keep in sync with "tsCache2ChunkDuration"
	if step < time.Minute {
		// max 120 points (seconds, two minutes) from second table
		return int(2 * time.Minute / step)
	} else if step < time.Hour {
		// max 120 points (minutes, two hours) from minute table
		return int(2 * time.Hour / step)
	} else if step <= timeDay {
		// max 24 points (two days) from hour table
		return int(2 * timeDay / step)
	} else {
		return 1 // for week or month
	}
}

func cache2ChunkDuration(step time.Duration) time.Duration {
	// NB! keep in sync with "tsCache2ChunkSize"
	if step < time.Minute {
		return 2 * time.Minute
	} else if step < time.Hour {
		return 2 * time.Hour
	} else if step <= timeDay {
		return 2 * timeDay
	} else {
		return step
	}
}

func cache2StaleAcceptPeriod(h *requestHandler) time.Duration {
	if h.playRequest() {
		if v := h.CacheStaleAcceptPeriod.Load(); v > 0 {
			return time.Duration(v) * time.Second
		}
	}
	return 0
}

func cache2DataCopy(dst, src cache2Data) int {
	i := 0
	for ; i < len(dst) && i < len(src); i++ {
		dst[i] = make([]tsSelectRow, len(src[i]))
		copy(dst[i], src[i])
	}
	return i
}
