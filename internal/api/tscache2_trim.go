package api

import (
	"math"
	"time"

	"github.com/vkcom/statshouse-go"
	"github.com/vkcom/statshouse/internal/vkgo/srvfunc"
	"pgregory.net/rand"
)

type cache2Trim struct {
	*cache2
	s []cache2TrimBucket
	r *rand.Rand
}

type cache2TrimBucket struct {
	m *cache2Shard
	v *cache2Bucket

	sizeInBytes int
}

func (c *cache2) trim() {
	tr := cache2Trim{
		cache2: c,
		r:      rand.New(),
	}
	tr.mu.Lock()
	defer tr.mu.Unlock()
	for {
		trimAged := tr.limits.maxAge > 0 && tr.info.age() > tr.limits.maxAge
		if trimAged {
			tr.sendEvent(" 1", " 1", tr.info.sizeInBytes)
			maxAge := tr.limits.maxAge
			tr.mu.Unlock()
			tr.debugPrint("trim aged")
			tr.trimAged(maxAge)
			tr.mu.Lock()
			tr.sendEvent(" 2", " 1", tr.info.sizeInBytes)
		}
		trimSize := tr.limits.maxSizeInBytes > 0 && tr.info.sizeInBytes > tr.limits.maxSizeInBytes
		if trimSize {
			tr.sendEvent(" 1", " 2", tr.info.sizeInBytes)
			tr.mu.Unlock()
			tr.debugPrint("trim size")
			v := tr.reduceMemoryUsage()
			tr.sendEvent(" 2", " 2", v)
			tr.mu.Lock()
		}
		wait := true
		if trimAged || trimSize {
			if p := tr.handler.CacheTrimBackoffPeriod.Load(); p > 0 {
				d := time.Duration(p) * time.Second
				tr.debugPrintf("trim backoff for %s", d)
				tr.mu.Unlock()
				time.Sleep(d)
				wait = false
				tr.mu.Lock()
			}
		}
		if wait {
			t := time.AfterFunc(tr.limits.maxAge-tr.info.age(), tr.trimCond.Signal)
			tr.trimCond.Wait()
			t.Stop()
		}
	}
}

func (tr *cache2Trim) trimAged(maxAge time.Duration) {
	tr.debugPrintRuntimeInfof("remove older than %s", maxAge)
	now := time.Now()
	info := cache2UpdateInfo{
		minChunkAccessTimeSeen: now.UnixNano(),
	}
	t := now.Add(-maxAge).UnixNano()
	for _, m := range tr.shards {
		for _, shard := range m {
			v := shard.trimIteratorStart()
			for v != nil {
				if v.notUsedAfter(t) {
					shard.removeBucket(v, &info)
				} else {
					v.removeChunksNotUsedAfter(t, &info)
				}
				v = shard.trimIteratorNext()
			}
		}
	}
	tr.updateRuntimeInfo(info)
}

func (tr *cache2Trim) reduceMemoryUsage() int {
	if v := tr.handler.CacheTrimAge.Load(); v > 0 {
		tr.trimAged(time.Duration(v) * time.Second)
		if size, ok := tr.memoryUsageWithinLimit(); ok {
			return size
		}
	}
	s := tr.s[:0]
	for _, m := range tr.shards {
		for _, m := range m {
			v := m.trimIteratorStart()
			for v != nil {
				s = append(s, cache2TrimBucket{
					m:           m,
					v:           v,
					sizeInBytes: v.sizeInBytes(),
				})
				v = m.trimIteratorNext()
			}
		}
	}
	r := tr.r
	for len(s) != 0 {
		// remove random
		i := r.Intn(len(s))
		if j := r.Intn(len(s)); s[i].sizeInBytes < s[j].sizeInBytes {
			i = j
		}
		var info cache2UpdateInfo
		s[i].m.removeBucket(s[i].v, &info)
		s[i] = s[len(s)-1]
		s[len(s)-1] = cache2TrimBucket{}
		s = s[:len(s)-1]
		// update runtime info and check if done
		tr.mu.Lock()
		tr.updateRuntimeInfoUnlocked(info)
		size, ok := tr.memoryUsageWithinLimitUnlocked()
		tr.mu.Unlock()
		if ok {
			tr.sendEvent(" 2", " 2", size)
			return size
		}
	}
	// set pointers to nil to prevent memory leak
	for i := 0; i < len(s); i++ {
		s[i] = cache2TrimBucket{}
	}
	tr.s = s
	return tr.runtimeInfo().sizeInBytes
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
		info.sizeInBytesDelta -= sizeofCache2Chunks(chunks)
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
