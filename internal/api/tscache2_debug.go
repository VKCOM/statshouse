package api

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/format"
)

type cache2DebugLogMessage struct {
	t time.Time
	s string
}

func DebugCacheLog(r *httpRequestHandler) {
	w := r.Response()
	if ok := r.accessInfo.insecureMode || r.accessInfo.bitAdmin; !ok {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	s := r.getCache2().debugLog()
	n := 0
	for ; n < len(s) && !s[n].t.IsZero(); n++ {
		// pass
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if n == 0 {
		w.Write([]byte("<empty>"))
		return
	}
	i, j := 0, n-1
	for k := 0; k < n && s[i].t.Compare(s[j].t) >= 0; k++ {
		i = (i + 1) % n
		j = (j + 1) % n
	}
	for k := 0; k < n; k++ {
		w.Write([]byte(s[i].t.Format("15:04:05.000 ")))
		w.Write([]byte(s[i].s))
		w.Write([]byte("\n"))
		i = (i + 1) % len(s)
	}
}

func DebugCacheReset(r *httpRequestHandler) {
	w := r.Response()
	if ok := r.accessInfo.insecureMode || r.accessInfo.bitAdmin; !ok {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	switch r.FormValue("v") {
	case "1":
		r.cache.reset()
		w.Write([]byte("Version 1 cache is now empty!"))
	case "2":
		r.getCache2().reset()
		w.Write([]byte("Version 2 cache is now empty!"))
	default:
		r.cache.reset()
		r.getCache2().reset()
		w.Write([]byte("All cache versions are now empty!"))
	}
}

func DebugCacheInfo(r *httpRequestHandler) {
	w := r.Response()
	if ok := r.accessInfo.insecureMode || r.accessInfo.bitAdmin; !ok {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Write([]byte("# runtime waterlevel\n"))
	cache := r.getCache2()
	info := cache.runtimeInfo()
	sum2 := func(s [2]int) int {
		return s[0] + s[1]
	}
	sum4 := func(s [2][2]int) int {
		return sum2(s[0]) + sum2(s[1])
	}
	w.Write([]byte(fmt.Sprintf("age\t%v\n", info.age())))
	w.Write([]byte(fmt.Sprintf("buckets\t%v\n", sum2(info.bucketCountS))))
	w.Write([]byte(fmt.Sprintf("chunks\t%v\n", sum2(info.chunkCountS))))
	w.Write([]byte(fmt.Sprintf("length\t%v\n", sum2(info.chunkSizeS))))
	w.Write([]byte(fmt.Sprintf("size\t%v\n", info.size())))
	w.Write([]byte("\n"))
	w.Write([]byte("# runtime access\n"))
	w.Write([]byte(fmt.Sprintf("chunks\t%v\n", sum4(info.accessChunkCountS))))
	w.Write([]byte(fmt.Sprintf("length\t%v\n", sum4(info.accessChunkSizeS))))
	w.Write([]byte(fmt.Sprintf("size\t%v\n", sum4(info.accessSizeS))))
	w.Write([]byte("\n"))
	sumChunkCount, sumSize := 0, 0
	for step, shard := range cache.shards {
		shard.mu.Lock()
		if len(shard.bucketM) != 0 {
			w.Write([]byte(fmt.Sprintf("# shard %v\n", step)))
			shardChunkCount, shardSize := 0, 0
			for _, b := range shard.bucketM {
				b.mu.Lock()
				chunkCount, size := len(b.chunks), sizeofCache2Chunks(b.chunks)
				shardChunkCount += chunkCount
				shardSize += size
				w.Write([]byte(fmt.Sprintf("%d\t%d\n", chunkCount, size)))
				b.mu.Unlock()
			}
			w.Write([]byte("--\n"))
			w.Write([]byte(fmt.Sprintf("%d\t%d\n", shardChunkCount, shardSize)))
			w.Write([]byte("\n"))
			sumChunkCount += shardChunkCount
			sumSize += shardSize
		}
		shard.mu.Unlock()
	}
	w.Write([]byte("# shard total\n"))
	w.Write([]byte(fmt.Sprintf("%d\t%d\n", sumChunkCount, sumSize)))
}

func DebugCacheCreateMetrics(r *httpRequestHandler) {
	w := r.Response()
	if ok := r.accessInfo.insecureMode || r.accessInfo.bitAdmin; !ok {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	tags := []format.MetricMetaTag{
		{Index: 0}, // environment
		{Index: 1, Description: "host"},
	}
	debugCacheCreateMetric(r, format.MetricMetaValue{
		Name: "statshouse_api_cache_age",
		Kind: format.MetricKindValue,
		Tags: tags,
	})
	debugCacheCreateMetric(r, format.MetricMetaValue{
		Name: "statshouse_api_cache_waiting",
		Kind: format.MetricKindCounter,
		Tags: tags,
	})
	debugCacheCreateMetric(r, format.MetricMetaValue{
		Name: "statshouse_api_cache_load_amplification",
		Kind: format.MetricKindValue,
		Tags: tags,
	})
	tags = append(tags, format.MetricMetaTag{
		Index:       2,
		Description: "mode",
		Raw:         true,
		RawKind:     "int",
		ValueComments: map[string]string{
			" 0": "default",
			" 1": "play",
		}})
	debugCacheCreateMetric(r, format.MetricMetaValue{
		Name: "statshouse_api_cache_sum_size",
		Kind: format.MetricKindValue,
		Tags: tags,
	})
	debugCacheCreateMetric(r, format.MetricMetaValue{
		Name: "statshouse_api_cache_sum_bucket_count",
		Kind: format.MetricKindCounter,
		Tags: tags,
	})
	debugCacheCreateMetric(r, format.MetricMetaValue{
		Name: "statshouse_api_cache_sum_chunk_size",
		Kind: format.MetricKindValue,
		Tags: tags,
	})
	debugCacheCreateMetric(r, format.MetricMetaValue{
		Name: "statshouse_api_cache_sum_chunk_count",
		Kind: format.MetricKindCounter,
		Tags: tags,
	})
	tags = append(tags,
		format.MetricMetaTag{
			Index:       3,
			Description: "result",
			Raw:         true,
			RawKind:     "int",
			ValueComments: map[string]string{
				" 0": "miss",
				" 1": "hit",
			}},
		format.MetricMetaTag{
			Index:       4,
			Description: "step",
			Raw:         true,
			RawKind:     "int",
		},
		format.MetricMetaTag{
			Index:       5,
			Description: "usergroup",
		},
	)
	debugCacheCreateMetric(r, format.MetricMetaValue{
		Name:                 "statshouse_api_cache_size",
		Kind:                 format.MetricKindValue,
		Tags:                 tags,
		StringTopDescription: "user",
	})
	debugCacheCreateMetric(r, format.MetricMetaValue{
		Name:                 "statshouse_api_cache_bucket_count",
		Kind:                 format.MetricKindCounter,
		Tags:                 tags,
		StringTopDescription: "user",
	})
	debugCacheCreateMetric(r, format.MetricMetaValue{
		Name:                 "statshouse_api_cache_chunk_size",
		Kind:                 format.MetricKindValue,
		Tags:                 tags,
		StringTopDescription: "user",
	})
	debugCacheCreateMetric(r, format.MetricMetaValue{
		Name:                 "statshouse_api_cache_chunk_count",
		Kind:                 format.MetricKindCounter,
		Tags:                 tags,
		StringTopDescription: "user",
	})
	debugCacheCreateMetric(r, format.MetricMetaValue{
		Name:                 "statshouse_api_cache_access_size",
		Kind:                 format.MetricKindValue,
		Tags:                 tags,
		StringTopDescription: "user",
	})
	debugCacheCreateMetric(r, format.MetricMetaValue{
		Name:                 "statshouse_api_cache_access_chunk_size",
		Kind:                 format.MetricKindValue,
		Tags:                 tags,
		StringTopDescription: "user",
	})
	debugCacheCreateMetric(r, format.MetricMetaValue{
		Name:                 "statshouse_api_cache_access_chunk_count",
		Kind:                 format.MetricKindCounter,
		Tags:                 tags,
		StringTopDescription: "user",
	})
	tags[3] = format.MetricMetaTag{}
	debugCacheCreateMetric(r, format.MetricMetaValue{
		Name:                 "statshouse_api_cache_chunk_hit_count",
		Kind:                 format.MetricKindValue,
		Tags:                 tags,
		StringTopDescription: "user",
	})
}

func debugCacheCreateMetric(r *httpRequestHandler, metric format.MetricMetaValue) {
	w := r.Response()
	w.Write([]byte(metric.Name + "\n"))
	if v := r.metricsStorage.GetMetaMetricByName(metric.Name); v != nil {
		metric.MetricID = v.MetricID
		metric.Version = v.Version
	}
	if _, err := r.handlePostMetric(context.Background(), accessInfo{insecureMode: true}, "", metric); err != nil {
		w.Write([]byte(err.Error()))
	} else {
		w.Write([]byte("OK"))
	}
	w.Write([]byte("\n\n"))
}

func cacheGet(ctx context.Context, h *requestHandler, pq *queryBuilder, lod data_model.LOD, avoidCache bool) ([][]tsSelectRow, error) {
	if h.CacheVersion.Load() == 2 {
		return h.getCache2().Get(ctx, h, pq, lod, avoidCache)
	} else {
		return h.cache.Get(ctx, h, pq, lod, avoidCache)
	}
}

func cacheInvalidate(h *Handler, times []int64, stepSec int64) {
	if h.CacheVersion.Load() == 2 {
		h.getCache2().invalidate(times, stepSec)
	} else {
		h.cache.Invalidate(stepSec, times)
	}
}

func (h *Handler) setCacheVersion(cacheVersion int32) {
	if prev := h.CacheVersion.Swap(cacheVersion); prev != cacheVersion {
		if cacheVersion == 2 {
			h.cache.reset()
		} else {
			h.getCache2().reset()
		}
	}
}

func (h *Handler) getCache2() *cache2 {
	h.cache2Mu.RLock()
	defer h.cache2Mu.RUnlock()
	return h.cache2
}

func (h *Handler) setCache2ChunkSize(v int) *cache2 {
	h.cache2Mu.Lock()
	defer h.cache2Mu.Unlock()
	if h.cache2.chunkSize != v {
		_ = h.cache2.shutdown() // do not wait
		h.cache2 = newCache2(h, v, loadPoints)
	}
	return h.cache2
}

func (c *cache2) debugPrint(s string) {
	c.debugLogMu.Lock()
	defer c.debugLogMu.Unlock()
	c.debugLogS[c.debugLogX] = cache2DebugLogMessage{t: time.Now(), s: s}
	c.debugLogX = (c.debugLogX + 1) % len(c.debugLogS)
}

func (c *cache2) debugPrintf(format string, a ...any) {
	c.debugPrint(fmt.Sprintf(format, a...))
}

func (c *cache2) debugLog() [100]cache2DebugLogMessage {
	c.debugLogMu.Lock()
	defer c.debugLogMu.Unlock()
	return c.debugLogS
}

func (g *tsCacheGroup) reset() {
	for _, v := range g.pointCaches {
		for _, c := range v {
			c.reset()
		}
	}
}

func (c *tsCache) reset() {
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()
	c.cache = map[string]*tsEntry{}
	c.invalidatedAtNano = map[int64]int64{}
}
