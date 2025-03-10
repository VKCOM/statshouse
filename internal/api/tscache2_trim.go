package api

import (
	"cmp"
	"slices"
	"time"

	"github.com/vkcom/statshouse-go"
	"github.com/vkcom/statshouse/internal/vkgo/srvfunc"
)

type cache2Trim struct {
	cache *cache2
	heap  cache2TrimBucketHeap
}

type cache2TrimBucket struct {
	shard  *cache2Shard
	bucket *cache2Bucket
	info   cache2BucketRuntimeInfo
}

type cache2TrimBucketHeap []cache2TrimBucket

func (c *cache2) trim() {
	defer c.shutdownG.Done()
	t := cache2Trim{c, newCache2TrimBucketHeap()}
	c.mu.Lock()
	defer t.reduceMemoryUsage() // run with unlocked cache
	defer c.mu.Unlock()
	for !c.shutdownF {
		size := c.info.size()
		if 0 < size && 0 < c.limits.maxAge && c.limits.maxAge < c.info.age() {
			t.sendEvent(" 1", " 1", size)
			maxAge := c.limits.maxAge
			c.mu.Unlock()
			t.trimAged(maxAge)
			c.mu.Lock()
			t.sendEvent(" 2", " 1", c.info.size())
		}
		size = c.info.size()
		if c.limits.maxSizeSoft < size {
			t.sendEvent(" 1", " 2", size)
			c.mu.Unlock()
			v := t.reduceMemoryUsage()
			t.sendEvent(" 2", " 2", v)
			c.mu.Lock()
		}
		if c.shutdownF {
			break
		}
		size = c.info.size()
		if c.limits.maxSize == 0 || size <= c.limits.maxSize {
			var t *time.Timer
			if c.limits.maxAge > 0 {
				t = time.AfterFunc(c.limits.maxAge-c.info.age(), c.trimCond.Signal)
			}
			c.trimCond.Wait()
			if t != nil {
				t.Stop()
			}
		}
	}
}

func (t *cache2Trim) trimAged(maxAge time.Duration) {
	c := t.cache
	infoM := make(cache2UpdateInfoM)
	defer c.updateRuntimeInfoM(infoM)
	timeNow := time.Now()
	timeDOB := timeNow.Add(-maxAge).UnixNano()
	for _, shard := range c.shards {
		b := shard.trimIteratorStart()
		for b != nil {
			info := &cache2UpdateInfo{
				minChunkAccessTime: timeNow.UnixNano(),
			}
			if b.notUsedAfter(timeDOB) {
				shard.removeBucket(b, info)
			} else {
				b.removeChunksNotUsedAfter(timeDOB, info)
			}
			infoM.add(shard.stepS, b.fau, info)
			b = shard.trimIteratorNext()
		}
	}
}

func (t *cache2Trim) reduceMemoryUsage() int {
	c, h := t.cache, t.heap
	for i := 0; ; i++ {
		n := 0
		timeNow := time.Now().UnixNano()
		for _, shard := range c.shards {
			b := shard.trimIteratorStart()
			for b != nil {
				h = h.push(cache2TrimBucket{shard, b, b.runtimeInfo(timeNow)})
				b = shard.trimIteratorNext()
				n++
			}
		}
		c.debugPrintf("trim start #%d, buckets #%d/%d", i, n, c.bucketCount())
		if h.len() == 0 {
			t.heap = h
			return 0
		}
		for j := 1; h.len() != 0; j++ {
			v := h.min()
			info := cache2UpdateInfo{}
			v.shard.removeBucket(v.bucket, &info)
			// update runtime info and check if done
			c.mu.Lock()
			c.updateRuntimeInfoUnlocked(v.shard.stepS, v.bucket.fau, &info)
			size, maxSize := c.info.size(), c.limits.maxSizeSoft
			c.mu.Unlock()
			if size <= maxSize {
				c.debugPrintf("trim end   #%d, buckets #%d", i, j)
				t.heap = h.clear()
				return size
			}
			h = h.pop()
		}
	}
}

func (t *cache2Trim) sendEvent(event, reason string, sizeInBytes int) {
	statshouse.Value(
		"statshouse_api_cache_trim",
		statshouse.Tags{
			1: srvfunc.HostnameForStatshouse(),
			2: event,
			3: reason,
		},
		float64(sizeInBytes))
}

func (b *cache2Bucket) removeChunksNotUsedAfter(t int64, info *cache2UpdateInfo) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.removeChunksNotUsedAfterUnlocked(t, info)
}

func (b *cache2Bucket) removeChunksNotUsedAfterUnlocked(t int64, info *cache2UpdateInfo) {
	mode := b.mode()
	for i := 0; i < len(b.chunks); {
		for ; i < len(b.chunks); i++ {
			b.chunks[i].mu.Lock()
			if b.chunks[i].lastAccessTime < t {
				break // found not used, remains locked until removal
			}
			if info.minChunkAccessTime > b.chunks[i].lastAccessTime {
				info.minChunkAccessTime = b.chunks[i].lastAccessTime
			}
			b.chunks[i].mu.Unlock()
		}
		if i == len(b.chunks) {
			break // all remaining are in use
		}
		j := i + 1
		for j < len(b.chunks) {
			b.chunks[j].mu.Lock()
			if b.chunks[j].lastAccessTime >= t {
				b.chunks[j].mu.Unlock()
				break // found used
			}
			j++
		}
		for k := i; k < j; k++ {
			info.sizeS[mode] -= b.chunks[k].size
			b.chunks[k].size = 0
			b.chunks[k].data = nil      // free memory
			b.chunks[k].detached = true // detach
			b.chunks[k].mu.Unlock()
		}
		b.times = slices.Delete(b.times, i, j)
		b.chunks = slices.Delete(b.chunks, i, j)
		n := j - i
		info.chunkSizeS[mode] -= n * b.chunkSize
		info.chunkCountS[mode] -= n
		i = j
	}
}

func (b *cache2Bucket) notUsedAfter(t int64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.lastAccessTime <= t
}

func newCache2TrimBucketHeap() cache2TrimBucketHeap {
	return make(cache2TrimBucketHeap, 1) // dummy element, simplifies code
}

func (h cache2TrimBucketHeap) len() int {
	return len(h) - 1
}

func (h cache2TrimBucketHeap) min() cache2TrimBucket {
	return h[1]
}

func (h cache2TrimBucketHeap) push(b cache2TrimBucket) cache2TrimBucketHeap {
	h = append(h, b)
	// lift up
	j := len(h) - 1
	i := j / 2
	for j > 1 && !h.less(i, j) {
		h.swap(i, j)
		j = i
		i = j / 2
	}
	return h
}

func (h cache2TrimBucketHeap) pop() cache2TrimBucketHeap {
	h[1] = h[len(h)-1]
	h[len(h)-1] = cache2TrimBucket{}
	h = h[:len(h)-1]
	// push down
	for i, j := 1, 2; j < len(h); {
		if k := j + 1; k < len(h) && h.less(k, j) {
			j = k
		}
		if h.less(i, j) {
			break
		}
		h.swap(i, j)
		i = j
		j = i * 2
	}
	return h
}

func (h cache2TrimBucketHeap) clear() cache2TrimBucketHeap {
	for i := 1; i < len(h); i++ {
		h[i] = cache2TrimBucket{}
	}
	return h[:1]
}

func (h cache2TrimBucketHeap) less(i, j int) bool {
	l, r := &h[i].info, &h[j].info
	if v := cmp.Compare(l.playInterval, r.playInterval); v != 0 {
		return v > 0 // larger play period goes first
	}
	if v := cmp.Compare(l.idlePeriod, r.idlePeriod); v != 0 {
		return v > 0 // least recently used goes first
	}
	// larger one goes first
	return cmp.Compare(l.size, r.size) > 0
}

func (h cache2TrimBucketHeap) swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}
