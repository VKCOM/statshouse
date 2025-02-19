package api

import (
	"context"
	"fmt"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/vkcom/statshouse-go"
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/vkgo/srvfunc"
)

const timeDay = 24 * time.Hour
const timeMonth = 31 * 24 * time.Hour

var errSeriesCacheRemovedBeforeBeingLocked = fmt.Errorf("series cache removed before being locked")

type cache2 struct {
	mu       sync.Mutex
	trimCond sync.Cond
	info     cache2RuntimeInfo
	limits   cache2Limits

	// readonly after init
	shards    map[string]map[time.Duration]*cache2Shard // by version, step
	handler   *Handler
	location  *time.Location
	utcOffset int64 // nanoseconds

	// debug log
	debugLogMu sync.Mutex
	debugLogS  [100]cache2DebugLogMessage
	debugLogX  int
}

type cache2Loader struct {
	handler           *requestHandler
	query             *queryBuilder
	lod               *data_model.LOD
	now               int64 // nanoseconds
	forceLoad         bool
	staleAcceptPeriod time.Duration
	chunks            []*cache2Chunk
	waitC             chan error
	waitN             int
}

type cache2Shard struct {
	mu             sync.Mutex
	bucketM        map[string]*cache2Bucket
	bucketL        cache2BucketList // "seriesM" values in a list
	trimIter       *cache2Bucket
	invalidateIter *cache2Bucket

	// readonly after init
	cache *cache2
}

type cache2BucketList struct {
	head *cache2Bucket
}

type cache2Bucket struct {
	mu             sync.Mutex
	cache          *cache2        // nil after being removed (detached)
	time           []int64        // sorted
	chunks         []*cache2Chunk // same length and order as "time"
	lastAccessTime int64          // nanoseconds
	playInterval   int            // seconds

	// readonly after init
	key string

	// double linked list, managed by "cache2Shard"
	prev *cache2Bucket
	next *cache2Bucket
}

type cache2BucketRuntimeInfo struct {
	lastAccessTime int64 // seconds
	playInterval   int   // seconds
	size           int
}

type cache2Chunk struct {
	mu         sync.Mutex
	cache      *cache2 // nil if removed
	start, end int64   // [start, end)
	dataSize   int     // in bytes
	data       cache2Data
	waiting    []cache2Waiting

	invalidatedAt int64 // nanoseconds
	loadStartedAt int64 // nanoseconds
	loading       bool

	// protected by "cache2Bucket" mutex
	lastAccessTime int64
}

type cache2Waiting struct {
	data   cache2Data
	offset int
	c      chan<- error
}

type cache2Limits struct {
	maxAge  time.Duration
	maxSize int
}

type cache2RuntimeInfo struct {
	minChunkAccessTime int64 // nanoseconds
	chunkCount         int
	size               int
}

type cache2UpdateInfo struct {
	sizeDelta              int
	chunkCountDelta        int
	minChunkAccessTimeSeen int64
}

type cache2Data = [][]tsSelectRow

func newCache2(h *Handler) *cache2 {
	res := &cache2{
		shards:    make(map[string]map[time.Duration]*cache2Shard),
		handler:   h,
		location:  h.location,
		utcOffset: h.utcOffset * int64(time.Second),
		info: cache2RuntimeInfo{
			minChunkAccessTime: time.Now().UnixNano(),
		},
	}
	res.trimCond = *sync.NewCond(&res.mu)
	for version, v := range data_model.LODTables {
		res.shards[version] = make(map[time.Duration]*cache2Shard, len(v))
		for stepSec := range v {
			res.shards[version][time.Duration(stepSec)*time.Second] = &cache2Shard{
				bucketM: make(map[string]*cache2Bucket),
				bucketL: newCache2BucketList(),
				cache:   res,
			}
			res.info.size += sizeofCache2Shard
		}
	}
	go res.trim()
	return res
}

func (c *cache2) Get(ctx context.Context, h *requestHandler, q *queryBuilder, lod data_model.LOD, forceLoad bool) (res cache2Data, err error) {
	shard := c.shards[lod.Version][time.Duration(lod.StepSec)*time.Second]
	for i := 0; i < 2; i++ {
		info := cache2UpdateInfo{}
		bucket := shard.getOrCreateBucket(q, &info)
		res, err = c.get(ctx, h, q, &lod, forceLoad, bucket, &info)
		if err != errSeriesCacheRemovedBeforeBeingLocked {
			c.updateRuntimeInfo(info)
			break
		}
		// try again
	}
	return res, err
}

func (c *cache2) get(ctx context.Context, h *requestHandler, q *queryBuilder, lod *data_model.LOD, forceLoad bool, b *cache2Bucket, info *cache2UpdateInfo) (cache2Data, error) {
	l := cache2Loader{
		handler:           h,
		query:             q,
		lod:               lod,
		forceLoad:         forceLoad,
		now:               time.Now().UnixNano(),
		staleAcceptPeriod: cache2StaleAcceptPeriod(h, q),
	}
	res, err := l.get(b, lod, info)
	if err != nil {
		return res, err
	}
	for i := 0; i < len(l.chunks); {
		j := i + 1
		for j < len(l.chunks) && l.chunks[j-1].end == l.chunks[j].start {
			j++
		}
		go c.loadChunk(lod, l.chunks[i:j], h, q)
		i = j
	}
	if l.waitN == 0 {
		return res, nil
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
	case err = <-waitC:
		return res, err
	case <-ctx.Done():
		return res, ctx.Err()
	}
}

func (c *cache2) loadChunk(l *data_model.LOD, s []*cache2Chunk, h *requestHandler, q *queryBuilder) {
	lod := data_model.LOD{
		FromSec:    s[0].start / 1e9,
		ToSec:      s[len(s)-1].end / 1e9,
		StepSec:    l.StepSec,
		Version:    l.Version,
		Table:      l.Table,
		HasPreKey:  l.HasPreKey,
		PreKeyOnly: l.PreKeyOnly,
		Location:   l.Location,
	}
	n := cache2ChunkLen(time.Duration(lod.StepSec) * time.Second)
	res := make(cache2Data, n*len(s))
	_, err := loadPoints(context.Background(), h, q, lod, res, 0)
	if err == nil && q.metric != nil && len(q.by) != 0 {
		// map string tags
		for _, tagX := range q.by {
			var tag format.MetricMetaTag
			if 0 <= tagX && tagX < len(q.metric.Tags) {
				tag = q.metric.Tags[tagX]
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
	var sizeDelta int
	for _, c := range s {
		var cache *cache2
		var data cache2Data
		var waiting []cache2Waiting
		c.mu.Lock()
		cache = c.cache
		c.waiting, waiting = waiting, c.waiting
		if err == nil {
			data = res[start:end]
			if cache != nil {
				c.data = data
				if c.loadStartedAt < c.invalidatedAt {
					// has been invalidated while loading
				} else {
					c.invalidatedAt = 0
				}
				newSize := sizeofCache2Data(data)
				sizeDelta += newSize - c.dataSize
				c.dataSize = newSize
			}
		}
		c.loading = false
		c.mu.Unlock()
		for i, w := range waiting {
			if err == nil {
				if cache != nil {
					cache2DataCopy(w.data, data[w.offset:])
				} else if i == len(waiting)-1 {
					// shallow copy last for detached chunk
					copy(w.data, data[w.offset:])
				}
			}
			w.c <- err
		}
		start = end
		end += n
	}
	if sizeDelta != 0 {
		c.updateRuntimeInfo(cache2UpdateInfo{
			sizeDelta: sizeDelta,
		})
	}
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

func (c *cache2) updateRuntimeInfo(info cache2UpdateInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.updateRuntimeInfoUnlocked(info)
}

func (c *cache2) updateRuntimeInfoUnlocked(info cache2UpdateInfo) {
	// chunk info
	c.info.chunkCount += info.chunkCountDelta
	if c.info.minChunkAccessTime < info.minChunkAccessTimeSeen {
		c.info.minChunkAccessTime = info.minChunkAccessTimeSeen
	}
	// memory usage
	c.info.size += info.sizeDelta
	if _, ok := c.memoryUsageWithinLimitUnlocked(); !ok {
		c.trimCond.Signal()
	}
}

func (c *cache2) runtimeInfo() cache2RuntimeInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.info
}

func (c *cache2) memoryUsageWithinLimitUnlocked() (int, bool) {
	return c.info.size, c.limits.maxSize <= 0 || c.info.size <= c.limits.maxSize
}

func (c *cache2) sendMetrics(client *statshouse.Client) {
	v := c.runtimeInfo()
	tags := statshouse.Tags{1: srvfunc.HostnameForStatshouse()}
	// TODO: replace with builtins
	client.Count("statshouse_api_cache_chunk_count", tags, float64(v.chunkCount))
	client.Value("statshouse_api_cache_size", tags, float64(v.size))
	client.Value("statshouse_api_cache_age", tags, v.age().Seconds())
}

func (c *cache2) reset() {
	c.debugPrintRuntimeInfo("reset start")
	info := cache2UpdateInfo{
		minChunkAccessTimeSeen: time.Now().UnixNano(),
	}
	for _, m := range c.shards {
		for _, m := range m {
			m.reset(&info)
		}
	}
	c.updateRuntimeInfo(info)
	c.debugPrintRuntimeInfo("reset end")
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
	c.shards[Version2][step].invalidate(s, now)
	c.shards[Version3][step].invalidate(s, now)
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

func (l *cache2Loader) get(b *cache2Bucket, lod *data_model.LOD, info *cache2UpdateInfo) (cache2Data, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.cache == nil { // detached
		return nil, errSeriesCacheRemovedBeforeBeingLocked
	}
	n, err := lod.IndexOf(lod.ToSec)
	if err != nil || n == 0 {
		return nil, err
	}
	res := make(cache2Data, n)
	b.lastAccessTime = l.now
	b.playInterval = l.query.play
	var ts []int64
	var chunks []*cache2Chunk
	t := lod.FromSec * int64(time.Second)
	i, _ := slices.BinarySearch(b.time, t)
	step := time.Duration(lod.StepSec) * time.Second
	for data := res; len(data) != 0; {
		ts = ts[:0]
		chunks = chunks[:0]
		for len(data) != 0 && (i == len(b.chunks) || !b.chunks[i].contains(t)) {
			start, d := b.cache.chunkStartDuration(t, step)
			v := &cache2Chunk{
				cache: b.cache,
				start: start,
				end:   b.cache.chunkEnd(start, d),
			}
			data, t = l.addChunk(v, data, t)
			ts = append(ts, v.end)
			chunks = append(chunks, v)
		}
		if len(ts) == 0 {
			data, t = l.addChunk(b.chunks[i], data, t)
			i++
		} else {
			b.time = append(append(b.time[:i], ts...), b.time[i:]...)
			b.chunks = append(append(b.chunks[:i], chunks...), b.chunks[i:]...)
			info.chunkCountDelta += len(chunks)
			info.sizeDelta += len(chunks) * sizeofCache2Chunk
			i += len(ts)
		}
	}
	return res, nil
}

func (l *cache2Loader) addChunk(c *cache2Chunk, data cache2Data, t int64) (cache2Data, int64) {
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
		n = cache2ChunkLen(step) - offset
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
						if 0 <= k && k < len(l.query.metric.Tags) {
							tag = l.query.metric.Tags[k]
						}
						if v, err := l.handler.getRichTagValueID(&tag, l.handler.version, s); err == nil {
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
	c.lastAccessTime = l.now
	return data[n:], c.end
}

func (s *cache2Shard) getOrCreateBucket(q *queryBuilder, info *cache2UpdateInfo) *cache2Bucket {
	k := q.getOrBuildCacheKey()
	s.mu.Lock()
	defer s.mu.Unlock()
	if res := s.bucketM[k]; res != nil {
		return res
	}
	res := &cache2Bucket{
		key:   k,
		cache: s.cache,
	}
	s.bucketM[k] = res
	s.bucketL.add(res)
	info.sizeDelta += sizeofCache2Bucket
	return res
}

func (s *cache2Shard) reset(info *cache2UpdateInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, v := range s.bucketM {
		s.removeBucketUnlocked(v, info)
	}
}

func (s *cache2Shard) removeBucket(b *cache2Bucket, info *cache2UpdateInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.removeBucketUnlocked(b, info)
}

func (s *cache2Shard) removeBucketUnlocked(b *cache2Bucket, info *cache2UpdateInfo) {
	s.bucketM[b.key] = nil
	delete(s.bucketM, b.key)
	if s.trimIter == b {
		s.trimIter = s.bucketL.next(b)
	}
	if s.invalidateIter == b {
		s.invalidateIter = s.bucketL.next(b)
	}
	s.bucketL.remove(b)
	b.clearAndDetach(info)
}

func (s *cache2Shard) invalidate(ts []int64, now int64) {
	v := s.invalidateIteratorStart()
	for v != nil {
		v.invalidate(ts, now)
		v = s.invalidateIteratorNext()
	}
}

func (s *cache2Shard) trimIteratorStart() *cache2Bucket {
	return s.iteratorStart(&s.trimIter)
}

func (s *cache2Shard) trimIteratorNext() *cache2Bucket {
	return s.iteratorNext(&s.trimIter)
}

func (s *cache2Shard) invalidateIteratorStart() *cache2Bucket {
	return s.iteratorStart(&s.invalidateIter)
}

func (s *cache2Shard) invalidateIteratorNext() *cache2Bucket {
	return s.iteratorNext(&s.invalidateIter)
}

func (s *cache2Shard) iteratorStart(iter **cache2Bucket) *cache2Bucket {
	s.mu.Lock()
	defer s.mu.Unlock()
	res := s.bucketL.next(s.bucketL.head)
	if res == nil {
		return nil
	}
	*iter = s.bucketL.next(res)
	return res
}

func (s *cache2Shard) iteratorNext(iter **cache2Bucket) *cache2Bucket {
	s.mu.Lock()
	defer s.mu.Unlock()
	res := *iter
	if res == nil {
		return nil
	}
	*iter = s.bucketL.next(res)
	return res
}

func newCache2BucketList() cache2BucketList {
	dummy := &cache2Bucket{} // simplifies list management
	dummy.prev = dummy
	dummy.next = dummy
	return cache2BucketList{head: dummy}
}

func (l cache2BucketList) add(v *cache2Bucket) {
	v.prev = l.head.prev
	v.next = l.head
	l.head.prev.next = v
	l.head.prev = v
}

func (l cache2BucketList) remove(v *cache2Bucket) {
	v.prev.next = v.next
	v.next.prev = v.prev
	v.prev = nil
	v.next = nil
}

func (l cache2BucketList) next(v *cache2Bucket) *cache2Bucket {
	if v.next == l.head {
		return nil
	}
	return v.next
}

func (l cache2BucketList) len() int {
	// linear time, for unit tests
	var n int
	for v := l.head.next; ; v = v.next {
		if v == l.head {
			return n
		}
		n++
	}
}

func (b *cache2Bucket) invalidate(ts []int64, now int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.time) == 0 {
		return
	}
	if ts[len(ts)-1] < b.time[0] || b.time[len(b.time)-1] < ts[0] {
		return
	}
	for i, j := 0, 0; i < len(ts) && j < len(b.time); {
		for i < len(ts) && ts[i] < b.time[j] {
			i++
		}
		for j < len(b.time) && b.time[j] < ts[i] {
			j++
		}
		for i < len(ts) && j < len(b.time) && ts[i] == b.time[j] {
			b.chunks[j].invalidate(now)
			i++
			j++
		}
	}
}

func (b *cache2Bucket) runtimeInfo() cache2BucketRuntimeInfo {
	b.mu.Lock()
	defer b.mu.Unlock()
	playInterval := b.playInterval
	if playInterval <= 0 {
		// no playing equvalent to playing with infinite period
		// simplifies bucket compare
		playInterval = math.MaxInt
	}
	return cache2BucketRuntimeInfo{
		lastAccessTime: b.lastAccessTime / 1e9,
		size:           sizeofCache2Chunks(b.chunks),
		playInterval:   playInterval,
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

func (r cache2RuntimeInfo) age() time.Duration {
	return time.Since(time.Unix(0, r.minChunkAccessTime))
}

func cache2ChunkLen(step time.Duration) int {
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

func cache2StaleAcceptPeriod(h *requestHandler, q *queryBuilder) time.Duration {
	if q.play == 1 {
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
