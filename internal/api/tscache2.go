package api

import (
	"context"
	"math"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VKCOM/statshouse-go"
	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/format"
	"github.com/VKCOM/statshouse/internal/vkgo/srvfunc"
)

const timeDay = 24 * time.Hour
const timeMonth = 31 * 24 * time.Hour

type cache2 struct {
	waitN     atomic.Int64
	mu        sync.Mutex
	trimCond  *sync.Cond
	allocCond *sync.Cond
	info      cache2RuntimeInfo                        // total
	infoM     map[string]map[string]*cache2RuntimeInfo // by step, user
	limits    cache2Limits
	shutdownF bool
	shutdownG sync.WaitGroup

	// readonly after init
	shards    map[time.Duration]*cache2Shard // by step
	loader    tsLoadFunc
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
	handler *requestHandler
	query   *queryBuilder
	cache   *cache2
	shard   *cache2Shard
	bucket  *cache2Bucket
	lod     data_model.LOD
	data    cache2Data
	chunks  []cache2LoaderChunk
	waitC   chan error

	// in play mode with one second query interval stale data might be returned
	// not waiting for data load (we are going to return it next second)
	staleAcceptPeriod time.Duration

	// time range to query and current time
	timeStart int64
	timeEnd   int64
	timeNow   int64

	// data range to return
	loadStart int
	loadEnd   int

	mode      int // play mode indicator
	waitN     int // number of values to read from "waitC"
	forceLoad bool
}

type cache2LoaderChunk struct {
	chunk      *cache2Chunk
	chunkStart int
	chunkEnd   int
	loadStart  int // >= chunkStart
	loadEnd    int // <= chunkEnd

	load bool
	wait bool
}

type cache2Shard struct {
	mu             sync.Mutex
	bucketM        map[string]*cache2Bucket
	bucketL        cache2BucketList // "seriesM" values in a list
	trimIter       *cache2Bucket
	invalidateIter *cache2Bucket

	// readonly after init
	cache         *cache2
	stepS         string
	step          time.Duration
	chunkDuration time.Duration
	chunkSize     int
}

type cache2BucketList struct {
	head *cache2Bucket
}

type cache2Bucket struct {
	mu             sync.Mutex
	times          []int64        // sorted
	chunks         []*cache2Chunk // same length and order as "times"
	lastAccessTime int64          // nanoseconds
	playInterval   time.Duration

	// readonly after init
	key       string // "cache2Shard" key
	fau       string // first access user
	chunkSize int

	// double linked list, managed by "cache2Shard"
	prev *cache2Bucket
	next *cache2Bucket
}

type cache2BucketRuntimeInfo struct {
	idlePeriod   time.Duration
	playInterval time.Duration
	size         int
}

type cache2Chunk struct {
	mu             sync.Mutex
	data           cache2Data
	awaiters       []cache2Awaiter
	start, end     int64 // [start, end)
	invalidatedAt  int64 // nanoseconds
	loadStartedAt  int64 // nanoseconds
	lastAccessTime int64 // protected by "cache2Bucket" mutex
	size           int   // of "data", in bytes
	hitCount       int
	loading        int
	detached       bool
}

type cache2Awaiter struct {
	loaderChan  chan<- error
	loaderData  cache2Data
	loadStart   int
	loadEnd     int
	chunkOffset int
}

type cache2Limits struct {
	maxAge      time.Duration
	maxSize     int // memory hard limit
	maxSizeSoft int // memory soft limit
}

type cache2RuntimeInfo struct {
	// waterlevel ["play mode" at 1]
	sizeS              [2]int
	bucketCountS       [2]int
	chunkSizeS         [2]int
	chunkCountS        [2]int
	minChunkAccessTime int64 // nanoseconds

	// per second ["play mode" at 1]["miss" at 0, "hit" at 1]
	accessSizeS       [2][2]int
	accessChunkSizeS  [2][2]int
	accessChunkCountS [2][2]int
}

type cache2UpdateInfoM map[string]map[string]*cache2UpdateInfo // step, user

type cache2UpdateInfo struct {
	// waterlevel ["play mode" at 1]
	sizeS              [2]int
	bucketCountS       [2]int
	chunkSizeS         [2]int
	chunkCountS        [2]int
	minChunkAccessTime int64

	// per second ["play mode" at 1]["miss" at 0, "hit" at 1]
	hitSizeS       [2][2]int
	hitChunkSizeS  [2][2]int
	hitChunkCountS [2][2]int
}

type cache2Data = [][]tsSelectRow

func newCache2(h *Handler, chunkSize int, loader tsLoadFunc) *cache2 {
	c := &cache2{
		shards:    make(map[time.Duration]*cache2Shard),
		loader:    loader,
		handler:   h,
		location:  h.location,
		utcOffset: h.utcOffset * int64(time.Second), // nanoseconds from seconds
		info: cache2RuntimeInfo{
			minChunkAccessTime: time.Now().UnixNano(),
		},
		infoM:     make(map[string]map[string]*cache2RuntimeInfo),
		chunkSize: chunkSize,
	}
	c.trimCond = sync.NewCond(&c.mu)
	c.allocCond = sync.NewCond(&c.mu)
	c.shards = make(map[time.Duration]*cache2Shard, len(data_model.LODTables[Version3]))
	for stepSec := range data_model.LODTables[Version3] {
		step := time.Duration(stepSec) * time.Second
		size, duration := cache2ChunkSizeDuration(chunkSize, step)
		c.shards[step] = &cache2Shard{
			cache:         c,
			bucketM:       make(map[string]*cache2Bucket),
			bucketL:       newCache2BucketList(),
			step:          step,
			stepS:         strconv.FormatInt(stepSec, 10),
			chunkSize:     size,
			chunkDuration: duration,
		}
	}
	c.shutdownG.Add(1)
	go c.trim()
	return c
}

func (c *cache2) Get(ctx context.Context, h *requestHandler, q *queryBuilder, lod data_model.LOD, forceLoad bool) (res cache2Data, err error) {
	startCacheGet := time.Now()
	var lodSize int
	lodSize, err = lod.IndexOf(lod.ToSec)
	if err != nil || lodSize == 0 {
		return nil, err
	}
	defer func() {
		h.endpointStat.reportTiming("cache-total", time.Since(startCacheGet))
	}()
	shard := c.shards[time.Duration(lod.StepSec)*time.Second]
	if h.cacheDisabled() {
		c.tryNotExceedMemoryHardLimit()
		res = make(cache2Data, lodSize)
		_, err = c.loader(ctx, h, q, lod, res, 0)
		if err == nil {
			cache2MapStringTags(h, q, res)
			info := cache2UpdateInfo{}
			mode := cache2BucketMode(time.Duration(q.play) * time.Second)
			info.hitSizeS[mode][0] += sizeofCache2Data(res) // cache miss
			c.updateRuntimeInfo(shard.stepS, h.accessInfo.user, &info)
		}
	} else {
		res, err = c.newLoader(h, q, lod, lodSize, forceLoad, shard).run(ctx)
	}
	return res, err
}

func (c *cache2) tryNotExceedMemoryHardLimit() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for c.limits.maxSize != 0 && c.info.size() > c.limits.maxSize {
		c.allocCond.Wait()
	}
}

func (c *cache2) setLimits(v cache2Limits) {
	if v.maxSize <= 0 {
		// running without memory limit
		v.maxSize = 0
		v.maxSizeSoft = 0
	} else if v.maxSizeSoft <= 0 || v.maxSize <= v.maxSizeSoft {
		// start trimming when 80% of hard limit reached
		v.maxSizeSoft = int(0.8 * float64(v.maxSize))
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.shutdownF || c.limits == v {
		return
	}
	c.limits = v
	size := c.info.size()
	if c.limits.maxSizeSoft < size {
		c.trimCond.Signal()
	}
	if c.limits.maxSize == 0 || size <= c.limits.maxSize {
		c.allocCond.Broadcast()
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
	if c.limits.maxSize != 0 {
		size := c.info.size()
		if c.limits.maxSizeSoft < size {
			c.trimCond.Signal()
		}
		if size <= c.limits.maxSize {
			c.allocCond.Broadcast()
		}
	}
	m := c.infoM[step]
	if m == nil {
		m = make(map[string]*cache2RuntimeInfo)
		c.infoM[step] = m
	}
	r := m[user]
	if r == nil {
		r = &cache2RuntimeInfo{
			minChunkAccessTime: time.Now().UnixNano(),
		}
		m[user] = r
	}
	r.update(info)
}

func (c *cache2) sendMetrics(client *statshouse.Client) {
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
	c.mu.Lock()
	defer c.mu.Unlock()
	c.info.normalizeWaterLevel()
	// TODO: replace with builtins
	client.NamedValue("statshouse_api_cache_age", tags[0][0], c.info.age().Seconds())
	client.NamedCount("statshouse_api_cache_waiting", tags[0][0], float64(c.waitN.Load()))
	for i := 0; i < 2; i++ {
		client.NamedValue("statshouse_api_cache_sum_size", tags[i][0], float64(c.info.sizeS[i]))
		client.NamedCount("statshouse_api_cache_sum_bucket_count", tags[i][0], float64(c.info.bucketCountS[i]))
		client.NamedValue("statshouse_api_cache_sum_chunk_size", tags[i][0], float64(c.info.chunkSizeS[i]))
		client.NamedCount("statshouse_api_cache_sum_chunk_count", tags[i][0], float64(c.info.chunkCountS[i]))
	}
	for step, m := range c.infoM {
		for user, r := range m {
			r.normalizeWaterLevel()
			for i := 0; i < 2; i++ { // play mode at 1
				tags[i][0][3][1] = step
				tags[i][0][4][1] = getStatTokenName(user)
				tags[i][0][5][1] = user
				client.NamedValue("statshouse_api_cache_size", tags[i][0], float64(r.sizeS[i]))
				client.NamedCount("statshouse_api_cache_bucket_count", tags[i][0], float64(r.bucketCountS[i]))
				client.NamedValue("statshouse_api_cache_chunk_size", tags[i][0], float64(r.chunkSizeS[i]))
				client.NamedCount("statshouse_api_cache_chunk_count", tags[i][0], float64(r.chunkCountS[i]))
				for j := 0; j < 2; j++ { // hit at 1
					if j == 1 {
						tags[i][1][3] = tags[i][0][3]
						tags[i][1][4] = tags[i][0][4]
						tags[i][1][5] = tags[i][0][5]
					}
					client.NamedValue("statshouse_api_cache_access_size", tags[i][j], float64(r.accessSizeS[i][j]))
					client.NamedValue("statshouse_api_cache_access_chunk_size", tags[i][j], float64(r.accessChunkSizeS[i][j]))
					client.NamedCount("statshouse_api_cache_access_chunk_count", tags[i][j], float64(r.accessChunkCountS[i][j]))
				}
			}
			r.resetAccessInfo()
		}
	}
}

func (c *cache2) runtimeInfo() cache2RuntimeInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.info
}

func (c *cache2) shutdown() *sync.WaitGroup {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.shutdownF = true
	c.limits = cache2Limits{}
	c.trimCond.Signal()
	c.allocCond.Broadcast()
	return &c.shutdownG
}

func (c *cache2) reset() {
	infoM := make(cache2UpdateInfoM)
	defer c.updateRuntimeInfoM(infoM)
	timeNow := time.Now().UnixNano()
	for _, shard := range c.shards {
		shard.reset(infoM, timeNow)
	}
}

func (c *cache2) invalidate(times []int64, stepSec int64) {
	if len(times) == 0 {
		return
	}
	t := times[0] * int64(time.Second) // nanoseconds from seconds
	shard := c.shards[time.Duration(stepSec)*time.Second]
	start := c.chunkStart(shard, t)
	end := c.chunkEnd(shard, start)
	s := []int64{start}
	for i := 1; i < len(times); i++ {
		t = times[i] * int64(time.Second) // nanoseconds from seconds
		if end <= t {
			start = c.chunkStart(shard, t)
			end = c.chunkEnd(shard, start)
			s = append(s, start)
		}
	}
	shard.invalidate(s, time.Now().UnixNano())
}

func (c *cache2) chunkStart(shard *cache2Shard, t int64) int64 {
	d := int64(shard.chunkDuration)
	if shard.step <= time.Hour {
		return (t / d) * d
	} else if shard.step < timeMonth {
		utcOffset := c.utcOffset
		return ((t+utcOffset)/d)*d - utcOffset
	} else {
		l := c.location
		t := time.Unix(0, t).In(l)
		return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, l).UTC().UnixNano()
	}
}

func (c *cache2) chunkEnd(shard *cache2Shard, t int64) int64 {
	switch shard.step {
	case timeMonth:
		l := c.location
		for i := 0; i < shard.chunkSize; i++ {
			t = time.Unix(0, t).In(l).AddDate(0, 1, 0).UTC().UnixNano()
		}
		return t
	default:
		return t + int64(shard.chunkDuration)
	}
}

func (c *cache2) newLoader(h *requestHandler, q *queryBuilder, lod data_model.LOD, lodSize int, forceLoad bool, shard *cache2Shard) *cache2Loader {
	c.tryNotExceedMemoryHardLimit()
	lodStart := lod.FromSec * int64(time.Second)
	lodEnd := lod.ToSec * int64(time.Second)
	firstChunkStart := c.chunkStart(shard, lodStart)
	lastChunkEnd, chunkCount := firstChunkStart, 0
	for lastChunkEnd < lodEnd {
		lastChunkEnd = c.chunkEnd(shard, lastChunkEnd)
		chunkCount++
	}
	loadStart := int(time.Duration(lodStart-firstChunkStart) / shard.step)
	info := cache2UpdateInfo{}
	bucket := shard.getOrCreateLockedBucket(h, q, &info)
	defer c.updateRuntimeInfo(shard.stepS, bucket.fau, &info) // run with unlocked bucket
	defer bucket.mu.Unlock()                                  // bucket returned locked
	l := &cache2Loader{
		handler:           h,
		query:             q,
		cache:             c,
		shard:             shard,
		bucket:            bucket,
		lod:               lod,
		staleAcceptPeriod: cache2StaleAcceptPeriod(q),
		timeNow:           time.Now().UnixNano(),
		timeStart:         firstChunkStart,
		timeEnd:           lastChunkEnd,
		data:              make(cache2Data, chunkCount*shard.chunkSize),
		loadStart:         loadStart,
		loadEnd:           loadStart + lodSize,
		mode:              cache2BucketMode(time.Duration(q.play) * time.Second),
		forceLoad:         forceLoad,
	}
	l.init(&info)
	return l
}

func (c *cache2) bucketCount() int {
	n := 0
	for _, shard := range c.shards {
		n += shard.bucketCount()
	}
	return n
}

func (l *cache2Loader) init(info *cache2UpdateInfo) {
	// NB! bucket must be locked
	var ts []int64
	var cs []*cache2Chunk
	var chunks []cache2LoaderChunk
	defer func() {
		l.awaitCopyChunks(chunks, info)
	}()
	c, shard, b, t := l.cache, l.shard, l.bucket, l.timeStart
	i, _ := slices.BinarySearch(b.times, t)
	for pos := 0; t < l.timeEnd; {
		ts = ts[:0]
		cs = cs[:0]
		for t < l.timeEnd && (i >= len(b.times) || t != b.chunks[i].start) {
			chunk := &cache2Chunk{
				start: t,
				end:   c.chunkEnd(shard, t),
			}
			chunks = l.maybeAddChunk(chunks, chunk, pos, info)
			t = chunk.end
			pos += shard.chunkSize
			ts = append(ts, chunk.start)
			cs = append(cs, chunk)
		}
		if len(cs) == 0 {
			chunks = l.maybeAddChunk(chunks, b.chunks[i], pos, info)
			t = b.chunks[i].end
			pos += shard.chunkSize
		} else {
			b.times = slices.Insert(b.times, i, ts...)
			b.chunks = slices.Insert(b.chunks, i, cs...)
			info.chunkSizeS[l.mode] += len(cs) * b.chunkSize
			info.chunkCountS[l.mode] += len(cs)
			i += len(cs) - 1 // consider i++ below
		}
		i++
	}
	b.playInterval = time.Duration(l.query.play) * time.Second
	b.lastAccessTime = l.timeNow
}

func (l *cache2Loader) run(ctx context.Context) (cache2Data, error) {
	start := time.Now()
	defer func() {
		l.handler.endpointStat.reportTiming("cache-load-all-chunks", time.Since(start))
	}()
	if len(l.chunks) != 0 {
		if l.waitC == nil {
			l.waitC = make(chan error)
		}
		l.waitN++
		go l.loadChunks()
	}
	return l.data[l.loadStart:l.loadEnd], l.wait(ctx)
}

func (l *cache2Loader) maybeAddChunk(s []cache2LoaderChunk, c *cache2Chunk, pos int, info *cache2UpdateInfo) []cache2LoaderChunk {
	loadStart := max(pos, l.loadStart)
	chunkEnd := pos + l.shard.chunkSize
	loadEnd := min(l.loadEnd, chunkEnd)
	var load, wait bool
	c.mu.Lock()
	if c.data == nil || c.loadStartedAt < c.end || l.forceLoad {
		load = true
		wait = true
	} else if c.invalidatedAt != 0 {
		load = true
		if time.Duration(l.timeNow-c.invalidatedAt) >= l.staleAcceptPeriod {
			wait = true
		}
	} else if c.loadStartedAt < (c.end + int64(invalidateLinger)) {
		load = true
	}
	lc := cache2LoaderChunk{
		chunk:      c,
		chunkStart: pos,
		chunkEnd:   chunkEnd,
		loadStart:  loadStart,
		loadEnd:    loadEnd,
		load:       load,
		wait:       wait,
	}
	if lc.load && c.loading == 0 {
		defer c.mu.Unlock()
		if len(l.chunks) == 0 {
			s = l.awaitCopyChunks(s, info)
		} else {
			for i, v := range s {
				l.chunks = append(l.chunks, v)
				c := v.chunk
				c.loading++
				c.loadStartedAt = l.timeNow
				c.mu.Unlock()
				s[i].chunk = nil
			}
			s = s[:0]
		}
		l.chunks = append(l.chunks, lc)
		c.loading++
		c.loadStartedAt = l.timeNow
	} else {
		s = append(s, lc)
		// NB! chunk remains locked
	}
	c.lastAccessTime = l.timeNow
	return s
}

func (l *cache2Loader) awaitCopyChunks(s []cache2LoaderChunk, info *cache2UpdateInfo) []cache2LoaderChunk {
	for i, v := range s {
		if v.wait {
			v.await(l)
		} else {
			v.copy(l, info)
		}
		v.chunk.mu.Unlock()
		s[i].chunk = nil
	}
	return s[:0]
}

func (l *cache2Loader) loadChunks() {
	startLoadChunks := time.Now()
	defer func() {
		l.handler.endpointStat.reportTiming("cache-load-chunks", time.Since(startLoadChunks))
	}()
	chunks := l.chunks
	first := chunks[0]
	last := chunks[len(chunks)-1]
	lod := data_model.LOD{
		FromSec:     first.chunk.start / int64(time.Second), // nanoseconds from seconds
		ToSec:       last.chunk.end / int64(time.Second),    // nanoseconds from seconds
		StepSec:     l.lod.StepSec,
		Version:     l.lod.Version,
		Metric:      l.lod.Metric,
		NewSharding: l.lod.NewSharding,
		HasPreKey:   l.lod.HasPreKey,
		PreKeyOnly:  l.lod.PreKeyOnly,
		Location:    l.lod.Location,
	}
	h, q := l.handler, l.query
	c, b := l.cache, l.bucket
	data := l.data[first.chunkStart:last.chunkEnd]
	_, err := c.loader(context.Background(), h, q, lod, data, 0)
	if err == nil {
		startMapTags := time.Now()
		cache2MapStringTags(h, q, data)
		l.handler.endpointStat.reportTiming("cache-map-tags", time.Since(startMapTags))
	}
	l.waitC <- err
	startPostLoad := time.Now()
	defer func() {
		l.handler.endpointStat.reportTiming("cache-post-load", time.Since(startPostLoad))
	}()
	start, end := first.chunkStart, first.chunkEnd
	info := cache2UpdateInfo{}
	defer c.updateRuntimeInfo(l.shard.stepS, b.fau, &info)
	for _, v := range chunks {
		// calculate data size
		chunkSize := sizeofCache2Data(l.data[start:v.loadStart])
		dataMiss := l.data[v.loadStart:v.loadEnd]
		sizeMiss := sizeofCache2Data(dataMiss)
		chunkSize += sizeMiss
		chunkSize += sizeofCache2Data(l.data[v.loadEnd:end])
		// report cache miss
		info.hitSizeS[l.mode][0] += chunkSize
		info.hitChunkSizeS[l.mode][0] += v.loadEnd - v.loadStart
		info.hitChunkCountS[l.mode][0]++
		// update chunk
		chunk := v.chunk
		chunkData := l.data[start:end]
		var awaiters []cache2Awaiter
		chunk.mu.Lock()
		chunk.awaiters, awaiters = awaiters, chunk.awaiters
		attached := !chunk.detached
		if attached {
			if err == nil {
				if chunk.data == nil {
					chunk.data = make(cache2Data, b.chunkSize)
				}
				for i := 0; i < b.chunkSize; i++ {
					chunk.data[i] = append(chunk.data[i][:0], chunkData[i]...)
				}
				info.sizeS[l.mode] += chunkSize - chunk.size
				chunk.size = chunkSize
				if chunk.loadStartedAt < chunk.invalidatedAt {
					// chunk has been invalidated while loading
				} else {
					chunk.invalidatedAt = 0
				}
			}
			chunk.loading--
		}
		chunk.mu.Unlock()
		// copy data to awaiters
		for _, a := range awaiters {
			if err == nil {
				i := a.chunkOffset + a.loadStart
				j := a.chunkOffset
				for i < a.loadEnd {
					a.loaderData[i] = append(a.loaderData[i][:0], chunkData[j]...)
					i++
					j++
				}
				// report awaiter cache miss
				sizeMiss := sizeofCache2Data(a.loaderData[a.loadStart:a.loadEnd])
				info.hitSizeS[l.mode][0] += sizeMiss
				info.hitChunkSizeS[l.mode][0] += b.chunkSize
				info.hitChunkCountS[l.mode][0]++
			}
			a.loaderChan <- err
		}
		start = end
		end += b.chunkSize
	}
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
			l.cache.waitN.Add(-1)
		}
		for ; n < l.waitN; n++ {
			<-l.waitC
			l.cache.waitN.Add(-1)
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

func (v *cache2LoaderChunk) await(l *cache2Loader) {
	if l.waitC == nil {
		l.waitC = make(chan error)
	}
	chunk := v.chunk
	chunk.awaiters = append(chunk.awaiters, cache2Awaiter{
		loaderChan:  l.waitC,
		loaderData:  l.data,
		loadStart:   v.loadStart,
		loadEnd:     v.loadEnd,
		chunkOffset: v.loadStart - v.chunkStart,
	})
	l.waitN++
	l.cache.waitN.Add(1)
}

func (c *cache2LoaderChunk) copy(l *cache2Loader, info *cache2UpdateInfo) {
	// cache hit
	sizeHit := 0
	dataHit := l.data[c.loadStart:c.loadEnd]
	chunk := c.chunk
	chunkData := chunk.data[c.loadStart-c.chunkStart : c.loadEnd-c.chunkStart]
	// remap string tags
	h, q := l.handler, l.query
	for i := 0; i < len(chunkData); i++ {
		for j := 0; j < len(chunkData[i]); j++ {
			row := &chunkData[i][j]
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
		sizeHit += sizeofCache2DataCol + len(chunkData[i])*sizeofCache2DataRow
		dataHit[i] = make([]tsSelectRow, len(chunkData[i]))
		copy(dataHit[i], chunkData[i])
	}
	// update runtime info and send metrics
	bucket, mode := l.bucket, 0
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
	info.hitSizeS[mode][1] += sizeHit
	info.hitChunkSizeS[mode][1] += len(dataHit)
	info.hitChunkCountS[mode][1]++
	chunk.hitCount++
	statshouse.NamedValue("statshouse_api_cache_chunk_hit_count", tags, float64(chunk.hitCount))
}

func (shard *cache2Shard) getOrCreateLockedBucket(h *requestHandler, q *queryBuilder, info *cache2UpdateInfo) *cache2Bucket {
	key := q.getOrBuildCacheKey()
	shard.mu.Lock()
	defer shard.mu.Unlock()
	b := shard.bucketM[key]
	if b == nil {
		b = &cache2Bucket{
			key:          key,
			fau:          h.accessInfo.user,
			chunkSize:    shard.chunkSize,
			playInterval: time.Duration(q.play) * time.Second,
		}
		shard.bucketM[key] = b
		shard.bucketL.add(b)
		info.bucketCountS[b.mode()]++
	}
	// NB! don't forget to unblock on the calling side
	// bucket returned locked to not allow deletion while loader initialized
	b.mu.Lock()
	return b
}

func (shard *cache2Shard) reset(infoM cache2UpdateInfoM, timeNow int64) {
	shard.mu.Lock()
	defer shard.mu.Unlock()
	for _, v := range shard.bucketM {
		info := &cache2UpdateInfo{
			minChunkAccessTime: timeNow,
		}
		shard.removeBucketUnlocked(v, info)
		infoM.add(shard.stepS, v.fau, info)
	}
}

func (shard *cache2Shard) removeBucket(b *cache2Bucket, info *cache2UpdateInfo) {
	shard.mu.Lock()
	defer shard.mu.Unlock()
	shard.removeBucketUnlocked(b, info)
}

func (shard *cache2Shard) removeBucketUnlocked(b *cache2Bucket, info *cache2UpdateInfo) {
	// NB! shard must be locked
	b.mu.Lock()
	defer b.mu.Unlock()
	// remove from shard
	shard.bucketM[b.key] = nil
	delete(shard.bucketM, b.key)
	if shard.trimIter == b {
		shard.trimIter = shard.bucketL.next(b)
	}
	if shard.invalidateIter == b {
		shard.invalidateIter = shard.bucketL.next(b)
	}
	shard.bucketL.remove(b)
	// free bucket memory, mark as detached
	info.bucketCountS[b.mode()]--
	b.removeChunksNotUsedAfterUnlocked(math.MaxInt64, info)
	if len(b.chunks) != 0 {
		panic("len(b.chunks) != 0")
	}
	b.key = ""
	b.times = nil
	b.chunks = nil
}

func (shard *cache2Shard) invalidate(times []int64, timeNow int64) {
	b := shard.invalidateIteratorStart()
	for b != nil {
		b.invalidate(times, timeNow)
		b = shard.invalidateIteratorNext()
	}
}

func (shard *cache2Shard) trimIteratorStart() *cache2Bucket {
	return shard.iteratorStart(&shard.trimIter)
}

func (shard *cache2Shard) trimIteratorNext() *cache2Bucket {
	return shard.iteratorNext(&shard.trimIter)
}

func (shard *cache2Shard) invalidateIteratorStart() *cache2Bucket {
	return shard.iteratorStart(&shard.invalidateIter)
}

func (shard *cache2Shard) invalidateIteratorNext() *cache2Bucket {
	return shard.iteratorNext(&shard.invalidateIter)
}

func (shard *cache2Shard) iteratorStart(iter **cache2Bucket) *cache2Bucket {
	shard.mu.Lock()
	defer shard.mu.Unlock()
	res := shard.bucketL.next(shard.bucketL.head)
	if res == nil {
		return nil
	}
	*iter = shard.bucketL.next(res)
	return res
}

func (shard *cache2Shard) iteratorNext(iter **cache2Bucket) *cache2Bucket {
	shard.mu.Lock()
	defer shard.mu.Unlock()
	res := *iter
	if res == nil {
		return nil
	}
	*iter = shard.bucketL.next(res)
	return res
}

func (shard *cache2Shard) bucketCount() int {
	shard.mu.Lock()
	defer shard.mu.Unlock()
	return len(shard.bucketM)
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
	// linear time, for unit tests only
	var n int
	for v := l.head.next; ; v = v.next {
		if v == l.head {
			return n
		}
		n++
	}
}

func (b *cache2Bucket) invalidate(times []int64, timeNow int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.times) == 0 {
		return
	}
	if times[len(times)-1] < b.times[0] || b.times[len(b.times)-1] < times[0] {
		return
	}
	for i, j := 0, 0; i < len(times) && j < len(b.times); {
		for i < len(times) && times[i] < b.times[j] {
			i++
		}
		if i == len(times) {
			return
		}
		for j < len(b.times) && b.times[j] < times[i] {
			j++
		}
		for i < len(times) && j < len(b.times) && times[i] == b.times[j] {
			b.chunks[j].invalidate(timeNow)
			i++
			j++
		}
	}
}

func (b *cache2Bucket) runtimeInfo(timeNow int64) cache2BucketRuntimeInfo {
	b.mu.Lock()
	defer b.mu.Unlock()
	idlePeriod := time.Duration(timeNow - b.lastAccessTime)
	playInterval := b.playInterval
	if playInterval <= 0 || playInterval+5*time.Second < idlePeriod {
		// - being not accessed longer than play interval means not playing
		// - not playing is equvalent to playing with infinite period (simplifies bucket compare)
		playInterval = math.MaxInt
	}
	return cache2BucketRuntimeInfo{
		idlePeriod:   idlePeriod,
		size:         sizeofCache2Chunks(b.chunks),
		playInterval: playInterval,
	}
}

func (b *cache2Bucket) mode() int {
	return cache2BucketMode(b.playInterval)
}

func (c *cache2Chunk) invalidate(timeNow int64) {
	c.mu.Lock()
	c.invalidatedAt = timeNow
	c.mu.Unlock()
}

func (r *cache2RuntimeInfo) age() time.Duration {
	return time.Since(time.Unix(0, r.minChunkAccessTime))
}

func (r *cache2RuntimeInfo) size() int {
	return r.sizeS[0] + r.sizeS[1]
}

func (r *cache2RuntimeInfo) update(info *cache2UpdateInfo) {
	r.sizeS[0] += info.sizeS[0]
	r.sizeS[1] += info.sizeS[1]
	r.bucketCountS[0] += info.bucketCountS[0]
	r.bucketCountS[1] += info.bucketCountS[1]
	r.chunkSizeS[0] += info.chunkSizeS[0]
	r.chunkSizeS[1] += info.chunkSizeS[1]
	r.chunkCountS[0] += info.chunkCountS[0]
	r.chunkCountS[1] += info.chunkCountS[1]
	if r.minChunkAccessTime < info.minChunkAccessTime {
		r.minChunkAccessTime = info.minChunkAccessTime
	}
	r.accessSizeS[0][0] += info.hitSizeS[0][0]
	r.accessSizeS[0][1] += info.hitSizeS[0][1]
	r.accessSizeS[1][0] += info.hitSizeS[1][0]
	r.accessSizeS[1][1] += info.hitSizeS[1][1]
	r.accessChunkSizeS[0][0] += info.hitChunkSizeS[0][0]
	r.accessChunkSizeS[0][1] += info.hitChunkSizeS[0][1]
	r.accessChunkSizeS[1][0] += info.hitChunkSizeS[1][0]
	r.accessChunkSizeS[1][1] += info.hitChunkSizeS[1][1]
	r.accessChunkCountS[0][0] += info.hitChunkCountS[0][0]
	r.accessChunkCountS[0][1] += info.hitChunkCountS[0][1]
	r.accessChunkCountS[1][0] += info.hitChunkCountS[1][0]
	r.accessChunkCountS[1][1] += info.hitChunkCountS[1][1]
}

func (r *cache2RuntimeInfo) normalizeWaterLevel() {
	f := func(s *[2]int) {
		if s[0] < 0 {
			s[0], s[1] = 0, s[0]+s[1]
		} else if s[1] < 0 {
			s[0], s[1] = s[0]+s[1], 0
		}
	}
	f(&r.sizeS)
	f(&r.bucketCountS)
	f(&r.chunkSizeS)
	f(&r.chunkCountS)
}

func (r *cache2RuntimeInfo) resetAccessInfo() {
	r.accessSizeS = [2][2]int{}
	r.accessChunkSizeS = [2][2]int{}
	r.accessChunkCountS = [2][2]int{}
}

func (infoM cache2UpdateInfoM) add(step, user string, info *cache2UpdateInfo) {
	m := infoM[step]
	if m == nil {
		m = make(map[string]*cache2UpdateInfo)
		infoM[step] = m
	}
	if v := m[user]; v != nil {
		v.sizeS[0] += info.sizeS[0]
		v.sizeS[1] += info.sizeS[1]
		v.bucketCountS[0] += info.bucketCountS[0]
		v.bucketCountS[1] += info.bucketCountS[1]
		v.chunkSizeS[0] += info.chunkSizeS[0]
		v.chunkSizeS[1] += info.chunkSizeS[1]
		v.chunkCountS[0] += info.chunkCountS[0]
		v.chunkCountS[1] += info.chunkCountS[1]
		if info.minChunkAccessTime != 0 && v.minChunkAccessTime > info.minChunkAccessTime {
			v.minChunkAccessTime = info.minChunkAccessTime
		}
	} else {
		m[user] = info
	}
}

func cache2MapStringTags(h *requestHandler, q *queryBuilder, d cache2Data) {
	if q.metric == nil || len(q.by) == 0 {
		return
	}
	for _, tagX := range q.by {
		var tag format.MetricMetaTag
		if 0 <= tagX && tagX < len(q.metric.Tags) {
			tag = q.metric.Tags[tagX]
		}
		for i := 0; i < len(d); i++ {
			for j := 0; j < len(d[i]); j++ {
				if s := d[i][j].stag[tagX]; s != "" {
					v, err := h.getRichTagValueID(&tag, h.version, s)
					if err == nil {
						d[i][j].tag[tagX] = int64(v)
						d[i][j].stag[tagX] = ""
					} else {
						d[i][j].stagCount++
					}
				}
			}
		}
	}
}

func cache2ChunkSizeDuration(chunkSize int, step time.Duration) (int, time.Duration) {
	if chunkSize > 0 && step <= time.Hour {
		return chunkSize, time.Duration(chunkSize) * step
	}
	var d time.Duration
	if step < time.Minute {
		d = time.Minute // max 60 points from second table
	} else if step < time.Hour {
		d = time.Hour // max 60 points from minute table
	} else if step <= timeDay {
		d = timeDay // max 24 points from hour table
	} else {
		return 1, step // max 1 point for week or month
	}
	return int(d / step), d
}

func cache2StaleAcceptPeriod(q *queryBuilder) time.Duration {
	if q.play == 1 {
		return time.Second
	}
	return 0
}

func cache2BucketMode(playInterval time.Duration) int {
	if playInterval > 0 {
		return 1 // play mode
	}
	return 0
}
