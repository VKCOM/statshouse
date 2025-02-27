package api

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/vkcom/statshouse/internal/data_model"
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
	s := r.cache2.debugLog()
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
		r.cache2.reset()
		w.Write([]byte("Version 2 cache is now empty!"))
	default:
		r.cache.reset()
		r.cache2.reset()
		w.Write([]byte("All cache versions are now empty!"))
	}
}

func cacheGet(ctx context.Context, h *requestHandler, pq *queryBuilder, lod data_model.LOD, avoidCache bool) ([][]tsSelectRow, error) {
	if h.CacheVersion.Load() == 2 {
		return h.cache2.Get(ctx, h, pq, lod, avoidCache)
	} else {
		return h.cache.Get(ctx, h, pq, lod, avoidCache)
	}
}

func cacheInvalidate(h *Handler, ts []int64, stepSec int64) {
	if h.CacheVersion.Load() == 2 {
		h.cache2.invalidate(ts, stepSec)
	} else {
		h.cache.Invalidate(stepSec, ts)
	}
}

func (h *Handler) setCacheVersion(cacheVersion int32) {
	if prev := h.CacheVersion.Swap(cacheVersion); prev != cacheVersion {
		if cacheVersion == 2 {
			h.cache.reset()
		} else {
			h.cache2.reset()
		}
	}
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

func (c *cache2) debugPrintRuntimeInfof(f string, a ...any) {
	c.debugPrintRuntimeInfo(fmt.Sprintf(f, a...))
}

func (c *cache2) debugPrintRuntimeInfo(s string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.debugPrintRuntimeInfoUnlocked(s)
}

func (c *cache2) debugPrintRuntimeInfoUnlocked(s string) {
	c.debugPrint(fmt.Sprintf("%s, size=%d (max=%d), age %s (max %s)", s, c.info.size(), c.limits.maxSize, c.info.age(), c.limits.maxAge))
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
