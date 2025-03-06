package api

import (
	"context"
	"fmt"
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

var errOutOfRangeChunkOffset = fmt.Errorf("out of range chunk offset")

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
	attach            bool
}

type cache2Shard struct {
	mu             sync.Mutex
	bucketM        map[string]*cache2Bucket
	bucketL        cache2BucketList // "seriesM" values in a list
	trimIter       *cache2Bucket
	invalidateIter *cache2Bucket

	// readonly after init
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

	attached bool
}

type cache2BucketRuntimeInfo struct {
	idlePeriod   time.Duration
	playInterval time.Duration
	size         int
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
	sumBucketCountS    [2]int
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
	sumBucketCountS    [2]int
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
		utcOffset: h.utcOffset * int64(time.Second), // nanoseconds from seconds
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
		size, duration := cache2ChunkSizeDuration(chunkSize, step)
		c.shards[step] = &cache2Shard{
			bucketM:       make(map[string]*cache2Bucket),
			bucketL:       newCache2BucketList(),
			step:          step,
			stepS:         strconv.FormatInt(stepSec, 10),
			chunkSize:     size,
			chunkDuration: duration,
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
	if r := m[user]; r != nil {
		r.update(info)
	} else {
		r := &cache2RuntimeInfo{
			minChunkAccessTime: time.Now().UnixNano(),
		}
		r.update(info)
		m[user] = r
	}
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
		client.NamedCount("statshouse_api_cache_sum_bucket_count", tags[i][0], float64(c.info.sumBucketCountS[i]))
		client.NamedValue("statshouse_api_cache_sum_chunk_size", tags[i][0], float64(c.info.sumChunkSizeS[i]))
		client.NamedCount("statshouse_api_cache_sum_chunk_count", tags[i][0], float64(c.info.sumChunkCountS[i]))
	}
	for step, m := range c.infoM {
		for user, info := range m {
			for i := 0; i < 2; i++ { // play mode at 1
				tags[i][0][3][1] = step
				tags[i][0][4][1] = getStatTokenName(user)
				tags[i][0][5][1] = user
				client.NamedValue("statshouse_api_cache_size", tags[i][0], float64(info.sumSizeS[i]))
				client.NamedCount("statshouse_api_cache_bucket_count", tags[i][0], float64(info.sumBucketCountS[i]))
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
	infoM := make(cache2UpdateInfoM)
	timeNow := time.Now().UnixNano()
	for _, shard := range c.shards {
		shard.reset(infoM, timeNow)
	}
	c.updateRuntimeInfoM(infoM)
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
		return time.Unix(0, t).In(l).AddDate(0, 1, 0).UTC().UnixNano()
	default:
		return t + int64(shard.chunkDuration)
	}
}

func (c *cache2) newLoader(h *requestHandler, q *queryBuilder, lod data_model.LOD, forceLoad bool, data cache2Data) *cache2Loader {
	info := cache2UpdateInfo{}
	shard := c.shards[time.Duration(lod.StepSec)*time.Second]
	b, attach := shard.getOrCreateBucket(h, q, c, &info)
	l := &cache2Loader{
		handler:           h,
		query:             q,
		lod:               lod,
		cache:             c,
		shard:             shard,
		bucket:            b,
		mode:              cache2BucketMode(time.Duration(q.play) * time.Second),
		timeNow:           time.Now().UnixNano(),
		staleAcceptPeriod: cache2StaleAcceptPeriod(h, q),
		forceLoad:         forceLoad,
		attach:            attach,
	}
	l.init(data, lod.FromSec*int64(time.Second), &info)
	c.updateRuntimeInfo(shard.stepS, b.fau, &info)
	return l
}

func (c *cache2) bucketCount() int {
	res := 0
	for _, shard := range c.shards {
		res += shard.bucketCount()
	}
	return res
}

func (l *cache2Loader) init(d cache2Data, t int64, info *cache2UpdateInfo) {
	c, shard, b := l.cache, l.shard, l.bucket
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.attached {
		if mode := cache2BucketMode(b.playInterval); mode != l.mode {
			sizeDelta := sizeofCache2Chunks(b.chunks)
			chunkSizeDelta := len(b.chunks) * b.chunkSize
			chunkCountDelta := len(b.chunks)
			info.sumSizeS[mode] -= sizeDelta
			info.sumSizeS[l.mode] += sizeDelta
			info.sumBucketCountS[mode] -= 1
			info.sumBucketCountS[l.mode] += 1
			info.sumChunkSizeS[mode] -= chunkSizeDelta
			info.sumChunkSizeS[l.mode] += chunkSizeDelta
			info.sumChunkCountS[mode] -= chunkCountDelta
			info.sumChunkCountS[l.mode] += chunkCountDelta
		}
		b.playInterval = time.Duration(l.query.play) * time.Second
		b.lastAccessTime = l.timeNow
	}
	var times []int64
	var chunks []*cache2Chunk
	start := c.chunkStart(shard, t)
	i, _ := slices.BinarySearch(b.times, start)
	for len(d) != 0 {
		newChunkCount := 0
		chunks, times = chunks[:0], times[:0]
		for len(d) != 0 && (i >= len(b.times) || start != b.chunks[i].start) {
			chunk := &cache2Chunk{
				attached: b.attached,
				start:    start,
				end:      c.chunkEnd(shard, start),
			}
			d, t = l.addChunk(d, t, chunk, info)
			start = t
			newChunkCount++
			if l.attach {
				times = append(times, chunk.start)
				chunks = append(chunks, chunk)
			}
		}
		if newChunkCount == 0 {
			d, t = l.addChunk(d, t, b.chunks[i], info)
			start = t
			i++
		} else if l.attach {
			b.times = append(append(b.times[:i], times...), b.times[i:]...)
			b.chunks = append(append(b.chunks[:i], chunks...), b.chunks[i:]...)
			info.sumChunkSizeS[l.mode] += len(chunks) * b.chunkSize
			info.sumChunkCountS[l.mode] += len(chunks)
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
	b := l.bucket
	offset := int((t - c.start) / int64(l.shard.step))
	if offset < 0 || b.chunkSize <= offset {
		panic(errOutOfRangeChunkOffset)
	}
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
		n = b.chunkSize - offset
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
			{"5", getStatTokenName(b.fau)},
			{"_s", b.fau},
		}
		if b.playInterval > 0 {
			mode = 1
			tags[2][1] = "1" // play mode
		}
		info.hitSizeS[mode][1] += accessSize
		info.hitChunkSizeS[mode][1] += n
		info.hitChunkCountS[mode][1]++
		c.hitCount++
		statshouse.NamedValue("statshouse_api_cache_chunk_hit_count", tags, float64(c.hitCount))
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
		FromSec:    chunks[0].start / int64(time.Second),           // nanoseconds from seconds
		ToSec:      chunks[len(chunks)-1].end / int64(time.Second), // nanoseconds from seconds
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
			if chunk.size != 0 {
				info.sumSizeS[mode] -= chunk.size
				chunk.data = nil
				chunk.size = 0
			}
		} else if err == nil && chunk.attached {
			// update chunk data
			attached = true
			budget -= sizeDelta
			info.sumSizeS[mode] += sizeDelta
			chunk.data = data
			chunk.size = size
			if chunk.loadStartedAt < chunk.invalidatedAt {
				// chunk has been invalidated while loading
			} else {
				chunk.invalidatedAt = 0
			}
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
					missSize += sizeofCache2DataCol + len(src[j])*sizeofCache2DataRow
				}
				info.hitSizeS[mode][0] += missSize
				info.hitChunkSizeS[mode][0] += len(src)
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

func (shard *cache2Shard) getOrCreateBucket(h *requestHandler, q *queryBuilder, c *cache2, info *cache2UpdateInfo) (*cache2Bucket, bool) {
	c.mu.Lock()
	attach := !c.shutdownF && !h.cacheDisabled() && 0 < c.limits.maxSize && c.info.size() <= c.limits.maxSize
	c.mu.Unlock()
	k := q.getOrBuildCacheKey()
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if b := shard.bucketM[k]; b != nil {
		return b, attach
	}
	b := &cache2Bucket{
		fau:          h.accessInfo.user,
		chunkSize:    shard.chunkSize,
		playInterval: time.Duration(q.play) * time.Second,
	}
	if attach {
		b.key = k
		b.attached = true
		shard.bucketM[k] = b
		shard.bucketL.add(b)
		info.sumBucketCountS[b.mode()]++
	}
	return b, attach
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
	shard.bucketM[b.key] = nil
	delete(shard.bucketM, b.key)
	if shard.trimIter == b {
		shard.trimIter = shard.bucketL.next(b)
	}
	if shard.invalidateIter == b {
		shard.invalidateIter = shard.bucketL.next(b)
	}
	shard.bucketL.remove(b)
	info.sumBucketCountS[b.mode()]--
	b.clearAndDetach(info)
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
	// linear time, for unit tests
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
	return r.sumSizeS[0] + r.sumSizeS[1]
}

func (r *cache2RuntimeInfo) update(info *cache2UpdateInfo) {
	r.sumSizeS[0] += info.sumSizeS[0]
	r.sumSizeS[1] += info.sumSizeS[1]
	r.sumBucketCountS[0] += info.sumBucketCountS[0]
	r.sumBucketCountS[1] += info.sumBucketCountS[1]
	r.sumChunkSizeS[0] += info.sumChunkSizeS[0]
	r.sumChunkSizeS[1] += info.sumChunkSizeS[1]
	r.sumChunkCountS[0] += info.sumChunkCountS[0]
	r.sumChunkCountS[1] += info.sumChunkCountS[1]
	if r.minChunkAccessTime < info.minChunkAccessTime {
		r.minChunkAccessTime = info.minChunkAccessTime
	}
	r.hitSizeS[0][0] += info.hitSizeS[0][0]
	r.hitSizeS[0][1] += info.hitSizeS[0][1]
	r.hitSizeS[1][0] += info.hitSizeS[1][0]
	r.hitSizeS[1][1] += info.hitSizeS[1][1]
	r.hitChunkSizeS[0][0] += info.hitChunkSizeS[0][0]
	r.hitChunkSizeS[0][1] += info.hitChunkSizeS[0][1]
	r.hitChunkSizeS[1][0] += info.hitChunkSizeS[1][0]
	r.hitChunkSizeS[1][1] += info.hitChunkSizeS[1][1]
	r.hitChunkCountS[0][0] += info.hitChunkCountS[0][0]
	r.hitChunkCountS[0][1] += info.hitChunkCountS[0][1]
	r.hitChunkCountS[1][0] += info.hitChunkCountS[1][0]
	r.hitChunkCountS[1][1] += info.hitChunkCountS[1][1]
}

func (m cache2UpdateInfoM) add(step, user string, info *cache2UpdateInfo) {
	m2 := m[step]
	if m2 == nil {
		m2 = make(map[string]*cache2UpdateInfo)
		m[step] = m2
	}
	if v := m2[user]; v != nil {
		v.sumSizeS[0] += info.sumSizeS[0]
		v.sumSizeS[1] += info.sumSizeS[1]
		v.sumChunkSizeS[0] += info.sumChunkSizeS[0]
		v.sumChunkSizeS[1] += info.sumChunkSizeS[1]
		v.sumChunkCountS[0] += info.sumChunkCountS[0]
		v.sumChunkCountS[1] += info.sumChunkCountS[1]
		if info.minChunkAccessTime != 0 && v.minChunkAccessTime > info.minChunkAccessTime {
			v.minChunkAccessTime = info.minChunkAccessTime
		}
		m2[user] = v
	} else {
		m2[user] = info
	}
}

func cache2ChunkSizeDuration(chunkSize int, step time.Duration) (int, time.Duration) {
	if chunkSize > 0 {
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

func cache2StaleAcceptPeriod(h *requestHandler, q *queryBuilder) time.Duration {
	if q.play == 1 {
		if v := h.CacheStaleAcceptPeriod.Load(); v > 0 {
			return time.Duration(v) * time.Second
		}
	}
	return 0
}

func cache2BucketMode(playInterval time.Duration) int {
	if playInterval > 0 {
		return 1 // play mode
	}
	return 0
}
