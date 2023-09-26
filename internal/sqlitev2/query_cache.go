package sqlitev2

import (
	"log"
	"time"

	"github.com/vkcom/statshouse/internal/sqlite/sqlite0"
	"go.uber.org/multierr"
)

const cacheMaxSizeDefault = 3000
const evictSampleSize = 100
const evictSize = 10

type queryCache struct {
	queryCache   map[string]*cachedStmtInfo
	cacheMaxSize int
}

type cachedStmtInfo struct {
	key       string
	lastTouch int64
	stmt      *sqlite0.Stmt
}

func newQueryCache(cacheMaxSize int) *queryCache {
	if cacheMaxSize == 0 {
		cacheMaxSize = cacheMaxSizeDefault
	}
	return &queryCache{
		queryCache:   make(map[string]*cachedStmtInfo),
		cacheMaxSize: cacheMaxSize,
	}
}

func (cache *queryCache) closeTx() {
	for len(cache.queryCache) >= cache.cacheMaxSize {
		cache.evictCacheLocked()
	}
}

func (cache *queryCache) put(key string, stmt *sqlite0.Stmt) {
	cache.queryCache[key] = &cachedStmtInfo{key, time.Now().Unix(), stmt}
}

func (cache *queryCache) get(keyBytes []byte) (res *sqlite0.Stmt, ok bool) {
	var cachedStmt *cachedStmtInfo
	cachedStmt, ok = cache.queryCache[string(keyBytes)]
	if ok {
		cachedStmt.lastTouch = time.Now().Unix()
		return cachedStmt.stmt, ok
	}
	return nil, false
}

func (cache *queryCache) evictCacheLocked() {
	h := heap{}
	i := 0
	for _, v := range cache.queryCache {
		h.put(v)
		i++
		if i >= evictSampleSize {
			break
		}
	}
	for h.size > 0 {
		stmt := h.pop()
		err := stmt.stmt.Close()
		if err != nil {
			log.Println("failed to close SQLite statement: ", err.Error())
		}
		delete(cache.queryCache, stmt.key)
	}
}

func (cache *queryCache) close(err *error) {
	for _, stmt := range cache.queryCache {
		multierr.AppendInto(err, stmt.stmt.Close())
	}
}

func (cache *queryCache) size() int {
	return len(cache.queryCache)
}

const root = 1

type heap struct {
	heap [evictSize + 1]*cachedStmtInfo
	size int
}

func (h *heap) swap(i, j int) {
	h.heap[i], h.heap[j] = h.heap[j], h.heap[i]
}

func (h *heap) put(stmt *cachedStmtInfo) {
	if h.size == evictSize && stmt.lastTouch >= h.peek().lastTouch {
		return
	}
	if h.size == evictSize {
		h.pop()
	}
	putPos := h.size + 1
	h.heap[putPos] = stmt
	current := putPos
	h.size++
	for current != root {
		parent := current / 2
		if h.heap[parent].lastTouch < h.heap[current].lastTouch {
			h.swap(parent, current)
			current = parent
		} else {
			return
		}
	}
}

func (h *heap) peek() *cachedStmtInfo {
	return h.heap[root]
}

func (h *heap) pop() *cachedStmtInfo {
	res := h.heap[root]
	h.heap[root] = h.heap[h.size]
	h.heap[h.size] = nil
	h.size--
	current := root
	for {
		l := current * 2
		r := current*2 + 1
		if l > h.size {
			break
		}
		child := l
		if r <= h.size {
			if h.heap[r].lastTouch > h.heap[child].lastTouch {
				child = r
			}
		}
		if h.heap[child].lastTouch > h.heap[current].lastTouch {
			h.swap(child, current)
			current = child
		} else {
			break
		}
	}
	return res
}
