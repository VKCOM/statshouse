package api

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/vkcom/statshouse/internal/data_model"
)

func DebugCacheLog(r *httpRequestHandler) {
	w := r.Response()
	if ok := r.accessInfo.insecureMode || r.accessInfo.bitAdmin; !ok {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	s := r.cache2.debugLog()
	for i := 0; i < len(s) && s[i] != ""; i++ {
		w.Write([]byte(fmt.Sprint(s[i], "\n")))
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
		r.cache2.reset()
		w.Write([]byte("Version 2 cache is now empty!"))
	default:
		r.cache.reset()
		r.cache2.reset()
		w.Write([]byte("All cache versions are now empty!"))
	}
}

func cacheGet(ctx context.Context, h *requestHandler, pq *queryBuilder, lod data_model.LOD, avoidCache bool) ([][]tsSelectRow, error) {
	if h.CacheVersion2.Load() {
		return h.cache2.Get(ctx, h, pq, lod, avoidCache)
	} else {
		return h.cache.Get(ctx, h, pq, lod, avoidCache)
	}
}

func cacheInvalidate(h *Handler, ts []int64, stepSec int64) {
	if h.CacheVersion2.Load() {
		h.cache2.invalidate(ts, stepSec)
	} else {
		h.cache.Invalidate(stepSec, ts)
	}
}

func (h *Handler) setCacheVersion(useV2 bool) {
	if prev := h.CacheVersion2.Swap(useV2); prev != useV2 {
		if useV2 {
			h.cache.reset()
		} else {
			h.cache2.reset()
		}
	}
}

func (c *cache2) debugPrint(s string) {
	c.debugLogMu.Lock()
	defer c.debugLogMu.Unlock()
	c.debugLogS[c.debugLogX] = fmt.Sprint(time.Now().Format(time.RFC3339), " ", s)
	c.debugLogX = (c.debugLogX + 1) % len(c.debugLogS)
}

func (c *cache2) debugPrintf(f string, a ...any) {
	c.debugPrint(fmt.Sprintf(f, a...))
}

func (c *cache2) debugPrintRuntimeInfof(f string, a ...any) {
	c.debugPrintRuntimeInfo(fmt.Sprintf(f, a...))
}

func (c *cache2) debugPrintRuntimeInfo(s string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.debugPrintRuntimeInfoUnlocked(s)
}

func (c *cache2) debugPrintRuntimeInfoUnlocked(s string) {
	c.debugPrintf("%s size=%d (max=%d) age %s (max %s)", s, c.sizeInBytes, c.maxSizeInBytes, c.age(), c.maxAge)
}

func (c *cache2) debugLog() [100]string {
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
