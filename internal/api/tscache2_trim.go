package api

import (
	"cmp"
	"math"
	"time"

	"github.com/vkcom/statshouse-go"
	"github.com/vkcom/statshouse/internal/vkgo/srvfunc"
)

type cache2Trim struct {
	c *cache2
	h cache2TrimBucketHeap
}

type cache2TrimBucket struct {
	r cache2BucketRuntimeInfo
	m *cache2Shard
	v *cache2Bucket
}

type cache2TrimBucketHeap []cache2TrimBucket

func (c *cache2) trim() {
	tr := cache2Trim{c, newCache2TrimBucketHeap()}
	c.mu.Lock()
	defer c.mu.Unlock()
	for {
		trimAged := c.limits.maxAge > 0 && c.info.age() > c.limits.maxAge
		if trimAged {
			tr.sendEvent(" 1", " 1", c.info.size)
			maxAge := c.limits.maxAge
			c.mu.Unlock()
			c.debugPrint("trim aged")
			tr.trimAged(maxAge)
			c.mu.Lock()
			tr.sendEvent(" 2", " 1", c.info.size)
		}
		trimSize := c.limits.maxSize > 0 && c.info.size > c.limits.maxSize
		if trimSize {
			tr.sendEvent(" 1", " 2", c.info.size)
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
			t := time.AfterFunc(c.limits.maxAge-c.info.age(), c.trimCond.Signal)
			c.trimCond.Wait()
			t.Stop()
		}
	}
}

func (tr *cache2Trim) trimAged(maxAge time.Duration) {
	c := tr.c
	c.debugPrintRuntimeInfof("remove older than %s", maxAge)
	now := time.Now()
	infoM := make(cache2UpdateInfoM)
	t := now.Add(-maxAge).UnixNano()
	for _, m := range c.shards {
		for _, shard := range m {
			v := shard.trimIteratorStart()
			for v != nil {
				info := cache2UpdateInfo{
					minChunkAccessTimeSeen: now.UnixNano(),
				}
				if v.notUsedAfter(t) {
					shard.removeBucket(v, &info)
				} else {
					v.removeChunksNotUsedAfter(t, &info)
				}
				infoM.add(v.fau, info)
				v = shard.trimIteratorNext()
			}
		}
	}
	c.updateRuntimeInfoM(infoM)
}

func (tr *cache2Trim) reduceMemoryUsage() int {
	c := tr.c
	h := tr.h
	for _, m := range c.shards {
		for _, m := range m {
			v := m.trimIteratorStart()
			for v != nil {
				h = h.push(cache2TrimBucket{v.runtimeInfo(), m, v})
				v = m.trimIteratorNext()
			}
		}
	}
	for ; h.len() > 0; h = h.pop() {
		b := h.min()
		var info cache2UpdateInfo
		b.m.removeBucket(b.v, &info)
		// update runtime info and check if done
		c.mu.Lock()
		c.updateRuntimeInfoUnlocked(b.v.fau, info)
		size, ok := c.memoryUsageWithinLimitUnlocked()
		c.mu.Unlock()
		if ok {
			tr.h = h.clear()
			return size
		}
	}
	return c.runtimeInfo().size
}

func (tr *cache2Trim) sendEvent(event, reason string, sizeInBytes int) {
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
	b.removeChunksNotUsedAfterUnlocked(math.MaxInt64, info) // remove all chunks
	info.sizeDelta -= sizeofCache2Bucket
	b.time = nil
	b.chunks = nil
	b.cache = nil // now detached
}

func (b *cache2Bucket) removeChunksNotUsedAfter(t int64, info *cache2UpdateInfo) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.removeChunksNotUsedAfterUnlocked(t, info)
}

func (b *cache2Bucket) removeChunksNotUsedAfterUnlocked(t int64, info *cache2UpdateInfo) {
	for i := 0; i < len(b.chunks); {
		for i < len(b.chunks) && b.chunks[i].lastAccessTime >= t {
			if info.minChunkAccessTimeSeen > b.chunks[i].lastAccessTime {
				info.minChunkAccessTimeSeen = b.chunks[i].lastAccessTime
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
			v.data = nil  // help GC
			v.cache = nil // detach
			v.mu.Unlock()
		}
		info.chunkCountDelta -= len(chunks)
		info.sizeDelta -= sizeofCache2Chunks(chunks)
		info.lenDelta -= len(chunks) * b.chunkLen
		k := i
		for m := j; m < len(b.chunks); m++ {
			b.time[k] = b.time[m]
			b.chunks[k] = b.chunks[m]
			k++
		}
		b.time = b.time[:k]
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
	l, r := &h[i].r, &h[j].r
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
