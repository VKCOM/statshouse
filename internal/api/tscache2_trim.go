package api

import (
	"cmp"
	"math"
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
	tr := cache2Trim{c, newCache2TrimBucketHeap()}
	c.mu.Lock()
	defer c.mu.Unlock()
	for !c.shutdownF {
		trimAged := c.limits.maxAge > 0 && c.info.age() > c.limits.maxAge
		if trimAged {
			tr.sendEvent(" 1", " 1", c.info.size())
			maxAge := c.limits.maxAge
			c.mu.Unlock()
			c.debugPrint("trim aged")
			tr.trimAged(maxAge)
			c.mu.Lock()
			tr.sendEvent(" 2", " 1", c.info.size())
		}
		trimSize := c.limits.maxSize > 0 && c.info.size() > c.limits.maxSize
		if trimSize {
			tr.sendEvent(" 1", " 2", c.info.size())
			c.mu.Unlock()
			c.debugPrint("trim size")
			v := tr.reduceMemoryUsage()
			tr.sendEvent(" 2", " 2", v)
			c.mu.Lock()
		}
		wait := true
		if trimAged || trimSize {
			if p := c.handler.CacheTrimBackoffPeriod.Load(); p > 0 {
				d := time.Duration(p) * time.Second
				c.debugPrintf("trim backoff for %s", d)
				c.mu.Unlock()
				time.Sleep(d)
				wait = false
				c.mu.Lock()
			}
		}
		if wait {
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
	c.debugPrintRuntimeInfof("remove older than %s", maxAge)
	now := time.Now()
	infoM := make(cache2UpdateInfoM)
	timeNow := now.Add(-maxAge).UnixNano()
	for _, shard := range c.shards {
		bucket := shard.trimIteratorStart()
		for bucket != nil {
			info := &cache2UpdateInfo{
				minChunkAccessTime: now.UnixNano(),
			}
			if bucket.notUsedAfter(timeNow) {
				shard.removeBucket(bucket, info)
			} else {
				bucket.removeChunksNotUsedAfter(timeNow, info)
			}
			infoM.add(shard.stepS, bucket.fau, info)
			bucket = shard.trimIteratorNext()
		}
	}
	c.updateRuntimeInfoM(infoM)
}

func (t *cache2Trim) reduceMemoryUsage() int {
	c := t.cache
	h := t.heap
	for _, shard := range c.shards {
		bucket := shard.trimIteratorStart()
		for bucket != nil {
			h = h.push(cache2TrimBucket{shard, bucket, bucket.runtimeInfo()})
			bucket = shard.trimIteratorNext()
		}
	}
	for ; h.len() > 0; h = h.pop() {
		v := h.min()
		info := cache2UpdateInfo{}
		v.shard.removeBucket(v.bucket, &info)
		// update runtime info and check if done
		c.mu.Lock()
		c.updateRuntimeInfoUnlocked(v.shard.stepS, v.bucket.fau, &info)
		size, maxSize := c.info.size(), c.limits.maxSizeSoft
		c.mu.Unlock()
		if size <= maxSize {
			t.heap = h.clear()
			return size
		}
	}
	r := c.runtimeInfo()
	return r.size()
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

func (b *cache2Bucket) clearAndDetach(info *cache2UpdateInfo) {
	b.mu.Lock()
	defer b.mu.Unlock()
	// remove all chunks
	b.removeChunksNotUsedAfterUnlocked(math.MaxInt64, info)
	// free memory
	b.key = ""
	b.times = nil
	b.chunks = nil
	// detach
	b.attached = false
}

func (b *cache2Bucket) removeChunksNotUsedAfter(t int64, info *cache2UpdateInfo) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.removeChunksNotUsedAfterUnlocked(t, info)
}

func (b *cache2Bucket) removeChunksNotUsedAfterUnlocked(t int64, info *cache2UpdateInfo) {
	mode := b.mode()
	for i := 0; i < len(b.chunks); {
		for i < len(b.chunks) && b.chunks[i].lastAccessTime >= t {
			if info.minChunkAccessTime > b.chunks[i].lastAccessTime {
				info.minChunkAccessTime = b.chunks[i].lastAccessTime
			}
			i++
		}
		if i == len(b.chunks) {
			break
		}
		j := i + 1
		for j < len(b.chunks) && b.chunks[j].lastAccessTime < t {
			j++
		}
		chunks := b.chunks[i:j]
		for _, v := range chunks {
			v.mu.Lock()
			info.sumSizeS[mode] -= v.size
			v.size = 0
			v.data = nil       // free memory
			v.attached = false // detach
			v.mu.Unlock()
		}
		info.sumChunkCountS[mode] -= len(chunks)
		info.sumChunkSizeS[mode] -= len(chunks) * b.chunkSize
		k := i
		for m := j; m < len(b.chunks); m++ {
			b.times[k] = b.times[m]
			b.chunks[k] = b.chunks[m]
			k++
		}
		b.times = b.times[:k]
		b.chunks = b.chunks[:k]
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
	if v := cmp.Compare(l.lastAccessTime, r.lastAccessTime); v != 0 {
		return v < 0 // least recently used goes first
	}
	// larger one goes first
	return cmp.Compare(l.size, r.size) > 0
}

func (h cache2TrimBucketHeap) swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}
