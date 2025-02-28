package api

import (
	"context"
	"math"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/vkcom/statshouse-go"
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/vkgo/srvfunc"
)

const timeDay = 24 * time.Hour
const timeMonth = 31 * 24 * time.Hour

type cache2 struct {
	mu        sync.Mutex
	trimCond  sync.Cond
	info      cache2RuntimeInfo                        // total
	infoM     map[string]map[string]*cache2RuntimeInfo // by step, user
	limits    cache2Limits
	shutdownF bool

	// readonly after init
	shards    map[time.Duration]*cache2Shard // by step
	handler   *Handler
	location  *time.Location
	utcOffset int64 // nanoseconds
	chunkSize int

	// debug log
	debugLogMu sync.Mutex
	debugLogS  [100]cache2DebugLogMessage
	debugLogX  int
}

type cache2Loader struct {
	handler           *requestHandler
	query             *queryBuilder
	cache             *cache2
	shard             *cache2Shard
	bucket            *cache2Bucket
	chunks            []*cache2Chunk
	lod               data_model.LOD
	waitC             chan error
	staleAcceptPeriod time.Duration
	timeNow           int64
	waitN             int
	mode              int
	forceLoad         bool
}

type cache2Shard struct {
	mu             sync.Mutex
	bucketM        map[string]*cache2Bucket
	bucketL        cache2BucketList // "seriesM" values in a list
	trimIter       *cache2Bucket
	invalidateIter *cache2Bucket

	// readonly after init
	step      time.Duration
	stepS     string
	chunkSize int
}

type cache2BucketList struct {
	head *cache2Bucket
}

type cache2Bucket struct {
	mu             sync.Mutex
	times          []int64        // sorted
	chunks         []*cache2Chunk // same length and order as "times"
	lastAccessTime int64          // nanoseconds
	playInterval   int            // seconds

	// readonly after init
	key       string // "cache2Shard" key
	fau       string // first access user
	chunkSize int

	// double linked list, managed by "cache2Shard"
	prev *cache2Bucket
	next *cache2Bucket

	attached bool
}

type cache2BucketRuntimeInfo struct {
	lastAccessTime int64 // seconds
	playInterval   int   // seconds
	size           int
}

type cache2Chunk struct {
	mu             sync.Mutex
	data           cache2Data
	waiting        []cache2Waiting
	start, end     int64 // [start, end)
	invalidatedAt  int64 // nanoseconds
	loadStartedAt  int64 // nanoseconds
	lastAccessTime int64 // protected by "cache2Bucket" mutex
	size           int   // of "data", in bytes
	hitCount       int
	attached       bool
	loading        bool
}

type cache2Waiting struct {
	dst       cache2Data
	dstC      chan<- error
	srcOffset int
}

type cache2Limits struct {
	maxAge      time.Duration
	maxSize     int
	maxSizeSoft int
}

type cache2RuntimeInfo struct {
	// waterlevel ["play mode" at 1]
	sumSizeS           [2]int
	sumChunkSizeS      [2]int
	sumChunkCountS     [2]int
	minChunkAccessTime int64 // nanoseconds

	// per second ["play mode" at 1]["miss" at 0, "hit" at 1]
	hitSizeS       [2][2]int
	hitChunkSizeS  [2][2]int
	hitChunkCountS [2][2]int
}

type cache2UpdateInfoM map[string]map[string]*cache2UpdateInfo // step, user

type cache2UpdateInfo struct {
	// waterlevel ["play mode" at 1]
	sumSizeS           [2]int
	sumChunkSizeS      [2]int
	sumChunkCountS     [2]int
	minChunkAccessTime int64

	// per second ["play mode" at 1]["miss" at 0, "hit" at 1]
	hitSizeS       [2][2]int
	hitChunkSizeS  [2][2]int
	hitChunkCountS [2][2]int
}

type cache2Data = [][]tsSelectRow

func newCache2(h *Handler, chunkSize int) *cache2 {
	c := &cache2{
		shards:    make(map[time.Duration]*cache2Shard),
		handler:   h,
		location:  h.location,
		utcOffset: h.utcOffset * int64(time.Second),
		info: cache2RuntimeInfo{
			minChunkAccessTime: time.Now().UnixNano(),
		},
		infoM:     make(map[string]map[string]*cache2RuntimeInfo),
		chunkSize: chunkSize,
	}
	c.trimCond = *sync.NewCond(&c.mu)
	c.shards = make(map[time.Duration]*cache2Shard, len(data_model.LODTables[Version3]))
	for stepSec := range data_model.LODTables[Version3] {
		step := time.Duration(stepSec) * time.Second
		c.shards[step] = &cache2Shard{
			bucketM:   make(map[string]*cache2Bucket),
			bucketL:   newCache2BucketList(),
			chunkSize: cache2ChunkSize(step, chunkSize),
			step:      step,
			stepS:     strconv.FormatInt(stepSec, 10),
		}
	}
	go c.trim()
	return c
}

func (c *cache2) Get(ctx context.Context, h *requestHandler, q *queryBuilder, lod data_model.LOD, forceLoad bool) (cache2Data, error) {
	n, err := lod.IndexOf(lod.ToSec)
	if err != nil || n == 0 {
		return nil, err
	}
	d := make(cache2Data, n)
	return d, c.newLoader(h, q, lod, forceLoad, d).run(ctx)
}

func (c *cache2) setLimits(v cache2Limits) {
	if v.maxSize <= 0 {
		// caching disabled
		v.maxSize = 0
		v.maxSizeSoft = 0
	} else if v.maxSizeSoft <= 0 || v.maxSize <= v.maxSizeSoft {
		// start trimming when 80% of hard limit reached
		v.maxSizeSoft = int(0.8 * float64(v.maxSize))
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.limits != v {
		c.limits = v
		c.debugPrintRuntimeInfoUnlocked("set limits")
		c.trimCond.Signal()
	}
}

func (c *cache2) updateRuntimeInfoM(infoM cache2UpdateInfoM) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for step, m := range infoM {
		for user, v := range m {
			c.updateRuntimeInfoUnlocked(step, user, v)
		}
	}
}

func (c *cache2) updateRuntimeInfo(step, user string, info *cache2UpdateInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.updateRuntimeInfoUnlocked(step, user, info)
}

func (c *cache2) updateRuntimeInfoUnlocked(step, user string, info *cache2UpdateInfo) {
	c.info.update(info)
	if 0 < c.limits.maxSize && c.limits.maxSizeSoft < c.info.size() {
		c.trimCond.Signal()
	}
	m := c.infoM[step]
	if m == nil {
		m = make(map[string]*cache2RuntimeInfo)
		c.infoM[step] = m
	}
	userGroup := getStatTokenName(user)
	if r := m[userGroup]; r != nil {
		r.update(info)
	} else {
		r := &cache2RuntimeInfo{
			minChunkAccessTime: time.Now().UnixNano(),
		}
		r.update(info)
		m[userGroup] = r
	}
}

func (c *cache2) runtimeInfo() cache2RuntimeInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.info
}

func (c *cache2) sendMetrics(client *statshouse.Client) {
	c.mu.Lock()
	defer c.mu.Unlock()
	tags := [2][2]statshouse.NamedTags{
		{ // default mode
			statshouse.NamedTags{
				{"1", srvfunc.HostnameForStatshouse()},
				{"2"},  // default mode
				{"3"},  // miss
				{"4"},  // step
				{"5"},  // user group
				{"_s"}, // user name
			},
			statshouse.NamedTags{
				{"1", srvfunc.HostnameForStatshouse()},
				{"2"},      // default mode
				{"3", "1"}, // hit
				{"4"},      // step
				{"5"},      // user group
				{"_s"},     // user name
			},
		},
		{ // play mode
			statshouse.NamedTags{
				{"1", srvfunc.HostnameForStatshouse()},
				{"2", "1"}, // play mode
				{"3"},      // miss
				{"4"},      // step
				{"5"},      // user group
				{"_s"},     // user name
			},
			statshouse.NamedTags{
				{"1", srvfunc.HostnameForStatshouse()},
				{"2", "1"}, // play mode
				{"3", "1"}, // hit
				{"4"},      // step
				{"5"},      // user group
				{"_s"},     // user name
			},
		},
	}
	// TODO: replace with builtins
	client.NamedValue("statshouse_api_cache_age", tags[0][0], c.info.age().Seconds())
	for i := 0; i < 2; i++ {
		client.NamedValue("statshouse_api_cache_sum_size", tags[i][0], float64(c.info.sumSizeS[i]))
		client.NamedCount("statshouse_api_cache_sum_chunks", tags[i][0], float64(c.info.sumChunkCountS[i]))
	}
	for step, m := range c.infoM {
		for user, info := range m {
			for i := 0; i < 2; i++ { // play mode at 1
				tags[i][0][3][1] = step
				tags[i][0][4][1] = getStatTokenName(user)
				tags[i][0][5][1] = user
				client.NamedValue("statshouse_api_cache_size", tags[i][0], float64(info.sumSizeS[i]))
				client.NamedValue("statshouse_api_cache_chunk_size", tags[i][0], float64(info.sumChunkSizeS[i]))
				client.NamedCount("statshouse_api_cache_chunk_count", tags[i][0], float64(info.sumChunkCountS[i]))
				for j := 0; j < 2; j++ { // hit at 1
					if j == 1 {
						tags[i][1][3] = tags[i][0][3]
						tags[i][1][4] = tags[i][0][4]
						tags[i][1][5] = tags[i][0][5]
					}
					client.NamedValue("statshouse_api_cache_access_size", tags[i][j], float64(info.hitSizeS[i][j]))
					client.NamedValue("statshouse_api_cache_access_chunk_size", tags[i][j], float64(info.hitChunkSizeS[i][j]))
					client.NamedCount("statshouse_api_cache_access_chunk_count", tags[i][j], float64(info.hitChunkCountS[i][j]))
				}
			}
			info.hitChunkSizeS = [2][2]int{}
			info.hitSizeS = [2][2]int{}
			info.hitChunkCountS = [2][2]int{}
			m[user] = info
		}
	}
}

func (c *cache2) shutdown() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.shutdownF = true
	c.trimCond.Signal()
}

func (c *cache2) reset() {
	c.debugPrintRuntimeInfo("reset start")
	infoM := make(cache2UpdateInfoM)
	timeNow := time.Now().UnixNano()
	for _, shard := range c.shards {
		shard.reset(infoM, timeNow)
	}
	c.updateRuntimeInfoM(infoM)
	c.debugPrintRuntimeInfo("reset end")
}

func (c *cache2) invalidate(times []int64, stepSec int64) {
	if len(times) == 0 {
		return
	}
	var s []int64
	now := time.Now().UnixNano()
	step := time.Duration(stepSec) * time.Second
	shard := c.shards[step]
	start := c.chunkStart(shard, times[0]*1e6)
	end := c.chunkEnd(shard, start)
	s = append(s[:0], end)
	for i := 1; i < len(times); i++ {
		if end <= times[i] {
			end = c.chunkEnd(shard, c.chunkStart(shard, times[i]))
			s = append(s, end)
		}
	}
	shard.invalidate(s, now)
}

func (c *cache2) chunkStart(s *cache2Shard, t int64) int64 {
	d64 := int64(s.step)
	if s.step < timeDay {
		return (t / d64) * d64
	} else if s.step < timeMonth {
		utcOffset := c.utcOffset
		return ((t+utcOffset)/d64)*d64 - utcOffset
	} else {
		l := c.location
		t := time.Unix(0, t).In(l)
		return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, l).UTC().UnixNano()
	}
}

func (c *cache2) chunkEnd(s *cache2Shard, t int64) int64 {
	switch s.step {
	case timeMonth:
		l := c.location
		return time.Unix(0, t).In(l).AddDate(0, 1, 0).UTC().UnixNano()
	default:
		return t + int64(s.chunkSize)*int64(s.step)
	}
}

func (c *cache2) newLoader(h *requestHandler, q *queryBuilder, lod data_model.LOD, forceLoad bool, data cache2Data) *cache2Loader {
	s := c.shards[time.Duration(lod.StepSec)*time.Second]
	l := &cache2Loader{
		handler:           h,
		query:             q,
		lod:               lod,
		cache:             c,
		shard:             s,
		bucket:            s.getOrCreateBucket(h, q, c),
		mode:              cache2BucketMode(q.play),
		timeNow:           time.Now().UnixNano(),
		staleAcceptPeriod: cache2StaleAcceptPeriod(h, q),
		forceLoad:         forceLoad,
	}
	l.init(data, lod.FromSec*int64(time.Second))
	return l
}

func (l *cache2Loader) init(d cache2Data, t int64) {
	cache, shard, bucket := l.cache, l.shard, l.bucket
	bucket.mu.Lock()
	defer bucket.mu.Unlock()
	info := cache2UpdateInfo{}
	if bucket.attached {
		defer cache.updateRuntimeInfo(shard.stepS, bucket.fau, &info)
		if mode := cache2BucketMode(bucket.playInterval); mode != l.mode {
			sizeDelta := sizeofCache2Chunks(bucket.chunks)
			chunkSizeDelta := len(bucket.chunks) * bucket.chunkSize
			chunkCountDelta := len(bucket.chunks)
			info.sumSizeS[mode] -= sizeDelta
			info.sumSizeS[l.mode] += sizeDelta
			info.sumChunkSizeS[mode] -= chunkSizeDelta
			info.sumChunkSizeS[l.mode] += chunkSizeDelta
			info.sumChunkCountS[l.mode] -= chunkCountDelta
			info.sumChunkCountS[l.mode] += chunkCountDelta
		}
		bucket.playInterval = l.query.play
		bucket.lastAccessTime = l.timeNow
	}
	var times []int64
	var chunks []*cache2Chunk
	i, _ := slices.BinarySearch(bucket.times, t)
	for len(d) != 0 {
		times = times[:0]
		chunks = chunks[:0]
		var newChunkCount int
		for len(d) != 0 && (i >= len(bucket.chunks) || !bucket.chunks[i].contains(t)) {
			start := cache.chunkStart(shard, t)
			chunk := &cache2Chunk{
				attached: bucket.attached,
				start:    start,
				end:      cache.chunkEnd(shard, start),
			}
			d, t = l.addChunk(d, t, chunk, &info)
			if bucket.attached {
				times = append(times, chunk.end)
				chunks = append(chunks, chunk)
			}
			newChunkCount++
		}
		if newChunkCount == 0 {
			d, t = l.addChunk(d, t, bucket.chunks[i], &info)
			i++
		} else if bucket.attached {
			bucket.times = append(append(bucket.times[:i], times...), bucket.times[i:]...)
			bucket.chunks = append(append(bucket.chunks[:i], chunks...), bucket.chunks[i:]...)
			info.sumChunkCountS[l.mode] += len(chunks)
			info.sumChunkSizeS[l.mode] += len(chunks) * bucket.chunkSize
			i += newChunkCount
		}
	}
}

func (l *cache2Loader) run(ctx context.Context) error {
	for i := 0; i < len(l.chunks); {
		j := i + 1
		for j < len(l.chunks) && l.chunks[j-1].end == l.chunks[j].start {
			j++
		}
		go l.loadChunks(i, j)
		i = j
	}
	return l.wait(ctx)
}

func (l *cache2Loader) addChunk(d cache2Data, t int64, c *cache2Chunk, info *cache2UpdateInfo) (cache2Data, int64) {
	bucket := l.bucket
	offset := int((t - c.start) / int64(l.shard.step))
	c.mu.Lock()
	defer c.mu.Unlock()
	var startLoad, await bool
	if c.data == nil || c.loadStartedAt < c.end || l.forceLoad {
		startLoad = true
		await = true
	} else if c.invalidatedAt != 0 {
		startLoad = true
		if time.Duration(l.timeNow-c.invalidatedAt) >= l.staleAcceptPeriod {
			await = true
		}
	} else if c.loadStartedAt < (c.end + int64(invalidateLinger)) {
		startLoad = true
	}
	var n int
	if await {
		// cache miss
		n = bucket.chunkSize - offset
		if n > len(d) {
			n = len(d)
		}
		if l.waitC == nil {
			l.waitC = make(chan error)
		}
		c.waiting = append(c.waiting, cache2Waiting{
			dst:       d[:n],
			dstC:      l.waitC,
			srcOffset: offset,
		})
		l.waitN++
	} else {
		// cache hit
		accessSize := 0
		src := c.data[offset:]
		// map string tags
		h, q := l.handler, l.query
		for i := 0; i < len(src) && i < len(d); i++ {
			for j := 0; j < len(src[i]); j++ {
				row := &src[i][j]
				for k := 0; k < len(row.stag) && row.stagCount != 0; k++ {
					if s := row.stag[k]; s != "" {
						var tag format.MetricMetaTag
						if 0 <= k && k < len(q.metric.Tags) {
							tag = q.metric.Tags[k]
						}
						if v, err := h.getRichTagValueID(&tag, h.version, s); err == nil {
							row.tag[k] = int64(v)
							row.stag[k] = ""
							row.stagCount--
						}
					}
				}
			}
			// copy
			accessSize += sizeofCache2DataCol + len(src[i])*sizeofCache2DataRow
			d[i] = make([]tsSelectRow, len(src[i]))
			copy(d[i], src[i])
			n++
		}
		// update runtime info and send metrics
		mode := 0
		tags := statshouse.NamedTags{
			{"1", srvfunc.HostnameForStatshouse()},
			{"2", "0"}, // mode
			{"4", l.shard.stepS},
			{"5", getStatTokenName(bucket.fau)},
			{"_s", bucket.fau},
		}
		if bucket.playInterval > 0 {
			mode = 1
			tags[2][1] = "1" // play mode
		}
		info.hitChunkSizeS[mode][1] += n
		info.hitSizeS[mode][1] += accessSize
		info.hitChunkCountS[mode][1]++
		c.hitCount++
		statshouse.NamedValue("statshouse_api_cache_chunk_hit", tags, float64(c.hitCount))
	}
	if startLoad && !c.loading {
		l.chunks = append(l.chunks, c)
		c.loading = true
		c.loadStartedAt = l.timeNow
	}
	c.lastAccessTime = l.timeNow
	return d[n:], c.end
}

func (l *cache2Loader) loadChunks(start, end int) {
	chunks := l.chunks[start:end]
	lod := data_model.LOD{
		FromSec:    chunks[0].start / 1e9,
		ToSec:      chunks[len(chunks)-1].end / 1e9,
		StepSec:    l.lod.StepSec,
		Version:    l.lod.Version,
		Table:      l.lod.Table,
		HasPreKey:  l.lod.HasPreKey,
		PreKeyOnly: l.lod.PreKeyOnly,
		Location:   l.lod.Location,
	}
	h, q := l.handler, l.query
	cache, shard, bucket := l.cache, l.shard, l.bucket
	res := make(cache2Data, bucket.chunkSize*len(chunks))
	_, err := loadPoints(context.Background(), h, q, lod, res, 0)
	if err == nil {
		// map string tags
		if q.metric != nil && len(q.by) != 0 {
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
	}
	cache.mu.Lock()
	shutdown, budget := cache.shutdownF, cache.limits.maxSize-cache.info.size()
	cache.mu.Unlock()
	start, end = 0, bucket.chunkSize
	info := cache2UpdateInfo{}
	mode := l.mode
	for _, chunk := range chunks {
		var data cache2Data
		var size int
		var attached bool
		if err == nil {
			data = res[start:end]
			size = sizeofCache2Data(data)
		}
		chunk.mu.Lock()
		sizeDelta := size - chunk.size
		var waiting []cache2Waiting
		chunk.waiting, waiting = waiting, chunk.waiting
		if shutdown || (0 < sizeDelta && budget < sizeDelta) {
			// release chunk memory
			chunk.data = nil
			info.sumSizeS[mode] -= chunk.size
			chunk.size = 0
		} else if err == nil && chunk.attached {
			attached = true
			budget -= sizeDelta
			// update chunk data
			chunk.data = data
			chunk.size = size
			if chunk.loadStartedAt < chunk.invalidatedAt {
				// chunk has been invalidated while loading
			} else {
				chunk.invalidatedAt = 0
			}
			info.sumSizeS[mode] += sizeDelta
		}
		chunk.loading = false
		chunk.mu.Unlock()
		for i, w := range waiting {
			if err == nil {
				missSize := 0
				dst, src := w.dst, data[w.srcOffset:]
				for j := 0; j < len(w.dst) && j < len(src); j++ {
					if attached || i < len(waiting)-1 {
						// deep copy
						dst[j] = make([]tsSelectRow, len(src[j]))
						copy(dst[j], src[j])
					} else {
						// shallow copy last for detached chunk
						dst[j] = src[j]
					}
					missSize += len(src[j]) * sizeofCache2DataRow
				}
				info.hitChunkSizeS[mode][0] += len(src)
				info.hitSizeS[mode][0] += missSize
				info.hitChunkCountS[mode][0]++
			}
			w.dstC <- err
		}
		start = end
		end += bucket.chunkSize
	}
	cache.updateRuntimeInfo(shard.stepS, bucket.fau, &info)
}

func (l *cache2Loader) wait(ctx context.Context) error {
	if l.waitN == 0 {
		return nil
	}
	c := make(chan error, 1)
	go func() {
		var n int
		var err error
		for ; n < l.waitN && err == nil; n++ {
			err = <-l.waitC
		}
		for ; n < l.waitN; n++ {
			<-l.waitC
		}
		c <- err
	}()
	select {
	case err := <-c:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *cache2Shard) getOrCreateBucket(h *requestHandler, q *queryBuilder, c *cache2) *cache2Bucket {
	k := q.getOrBuildCacheKey()
	s.mu.Lock()
	defer s.mu.Unlock()
	if res := s.bucketM[k]; res != nil {
		return res
	}
	b := &cache2Bucket{
		fau:          h.accessInfo.user,
		chunkSize:    s.chunkSize,
		playInterval: q.play,
	}
	c.mu.Lock() // NB! the only place with nested locking, keep an eye on it
	createAttached := !c.shutdownF && !h.cacheDisabled() && 0 < c.limits.maxSize && c.info.size() <= c.limits.maxSize
	c.mu.Unlock()
	if createAttached {
		b.key = k
		b.attached = true
		s.bucketM[k] = b
		s.bucketL.add(b)
	}
	return b
}

func (s *cache2Shard) reset(infoM cache2UpdateInfoM, timeNow int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, v := range s.bucketM {
		info := &cache2UpdateInfo{
			minChunkAccessTime: timeNow,
		}
		s.removeBucketUnlocked(v, info)
		infoM.add(s.stepS, v.fau, info)
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
	if len(b.times) == 0 {
		return
	}
	if ts[len(ts)-1] < b.times[0] || b.times[len(b.times)-1] < ts[0] {
		return
	}
	for i, j := 0, 0; i < len(ts) && j < len(b.times); {
		for i < len(ts) && ts[i] < b.times[j] {
			i++
		}
		for j < len(b.times) && b.times[j] < ts[i] {
			j++
		}
		for i < len(ts) && j < len(b.times) && ts[i] == b.times[j] {
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

func (b *cache2Bucket) mode() int {
	return cache2BucketMode(b.playInterval)
}

func (c *cache2Chunk) invalidate(now int64) {
	c.mu.Lock()
	c.invalidatedAt = now
	c.mu.Unlock()
}

func (c *cache2Chunk) contains(t int64) bool {
	return c.start <= t && t < c.end
}

func (r *cache2RuntimeInfo) age() time.Duration {
	return time.Since(time.Unix(0, r.minChunkAccessTime))
}

func (r *cache2RuntimeInfo) size() int {
	return r.sumSizeS[0] + r.sumSizeS[1]
}

func (r *cache2RuntimeInfo) update(info *cache2UpdateInfo) {
	r.sumChunkSizeS[0] += info.sumChunkSizeS[0]
	r.sumChunkSizeS[1] += info.sumChunkSizeS[1]
	r.sumSizeS[0] += info.sumSizeS[0]
	r.sumSizeS[1] += info.sumSizeS[1]
	r.sumChunkCountS[0] += info.sumChunkCountS[0]
	r.sumChunkCountS[1] += info.sumChunkCountS[1]
	if r.minChunkAccessTime < info.minChunkAccessTime {
		r.minChunkAccessTime = info.minChunkAccessTime
	}
	r.hitChunkSizeS[0][0] += info.hitChunkSizeS[0][0]
	r.hitChunkSizeS[0][1] += info.hitChunkSizeS[0][1]
	r.hitChunkSizeS[1][0] += info.hitChunkSizeS[1][0]
	r.hitChunkSizeS[1][1] += info.hitChunkSizeS[1][1]
	r.hitSizeS[0][0] += info.hitSizeS[0][0]
	r.hitSizeS[0][1] += info.hitSizeS[0][1]
	r.hitSizeS[1][0] += info.hitSizeS[1][0]
	r.hitSizeS[1][1] += info.hitSizeS[1][1]
	r.hitChunkCountS[0][0] += info.hitChunkCountS[0][0]
	r.hitChunkCountS[0][1] += info.hitChunkCountS[0][1]
	r.hitChunkCountS[1][0] += info.hitChunkCountS[1][0]
	r.hitChunkCountS[1][1] += info.hitChunkCountS[1][1]
}

func (m cache2UpdateInfoM) add(step, user string, newInfo *cache2UpdateInfo) {
	m2 := m[step]
	if m2 == nil {
		m2 = make(map[string]*cache2UpdateInfo)
		m[step] = m2
	}
	if v, ok := m2[user]; ok {
		v.sumSizeS[0] += newInfo.sumSizeS[0]
		v.sumSizeS[1] += newInfo.sumSizeS[1]
		v.sumChunkCountS[0] += newInfo.sumChunkCountS[0]
		v.sumChunkCountS[1] += newInfo.sumChunkCountS[1]
		if newInfo.minChunkAccessTime != 0 && v.minChunkAccessTime > newInfo.minChunkAccessTime {
			v.minChunkAccessTime = newInfo.minChunkAccessTime
		}
		m2[user] = v
	} else {
		m2[user] = newInfo
	}
}

func cache2ChunkSize(step time.Duration, chunkSize int) int {
	if chunkSize > 0 {
		return chunkSize
	}
	var d time.Duration
	if step < time.Minute {
		d = time.Minute // max 60 points from second table
	} else if step < time.Hour {
		d = time.Hour // max 60 points from minute table
	} else if step <= timeDay {
		d = timeDay // max 24 points from hour table
	} else {
		return 1 // max 1 point for week or month
	}
	return int(d / step)
}

func cache2StaleAcceptPeriod(h *requestHandler, q *queryBuilder) time.Duration {
	if q.play == 1 {
		if v := h.CacheStaleAcceptPeriod.Load(); v > 0 {
			return time.Duration(v) * time.Second
		}
	}
	return 0
}

func cache2BucketMode(playInterval int) int {
	if playInterval > 0 {
		return 1 // play mode
	}
	return 0
}
