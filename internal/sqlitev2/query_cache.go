package sqlitev2

import (
	"log"
	"time"

	"github.com/vkcom/statshouse/internal/sqlite/sqlite0"
	"github.com/zeebo/xxh3"
	"go.uber.org/multierr"
)

/*
Этот кэш может стать проблемой в случаях когда много запросов, мы будем просто зря занимать память дял каждого sqlite соединения
Решение: пользователь явно выбирает что кешировать а что нет
*/
type queryCachev2 struct {
	queryCache   map[hash]int
	h            *minHeap
	cacheMaxSize int
	logger       *log.Logger
}

type hash struct {
	low  uint64
	high uint64
}

type cachedStmtInfo struct {
	key       hash
	lastTouch int64
	stmt      *sqlite0.Stmt
}

const cacheMaxSizeDefault = 3000

func newQueryCachev2(cacheMaxSize int, logger *log.Logger) *queryCachev2 {
	if cacheMaxSize == 0 {
		cacheMaxSize = cacheMaxSizeDefault
	}
	cache := &queryCachev2{
		queryCache:   make(map[hash]int),
		cacheMaxSize: cacheMaxSize,
		logger:       logger,
	}
	cache.h = newHeap(func(a, b hash, i, j int) {
		cache.queryCache[a] = i
		cache.queryCache[b] = j
	})
	return cache
}

func (cache *queryCachev2) closeTx() {
	if len(cache.queryCache) > cache.cacheMaxSize {
		cache.evictCacheLocked(len(cache.queryCache) - cache.cacheMaxSize)
	}
}

func (cache *queryCachev2) put(key hash, stmt *sqlite0.Stmt) {
	stmtInfo := &cachedStmtInfo{key: key, lastTouch: time.Now().Unix(), stmt: stmt}
	ix := cache.h.put(stmtInfo)
	cache.queryCache[key] = ix
}

func (cache *queryCachev2) get(key hash) (res *sqlite0.Stmt, ok bool) {
	ix, ok := cache.queryCache[key]
	if ok {
		cachedStmt := cache.h.get(ix)
		cachedStmt.lastTouch = time.Now().Unix()
		return cachedStmt.stmt, ok
	}
	return nil, false
}

func (cache *queryCachev2) evictCacheLocked(count int) {
	for i := 0; i < count; i++ {
		stmt := cache.h.pop()
		err := stmt.stmt.Close()
		if err != nil {
			cache.logger.Println("[error] failed to close cached stmt:", err.Error())
		}
		delete(cache.queryCache, stmt.key)
	}
}

func (cache *queryCachev2) close(err *error) {
	for _, ix := range cache.queryCache {
		multierr.AppendInto(err, cache.h.heap[ix].stmt.Close())
	}
}

func (cache *queryCachev2) size() int {
	return len(cache.queryCache)
}

func calcHashBytes(key []byte) hash {
	h := xxh3.Hash128(key)
	return hash{
		low:  h.Lo,
		high: h.Hi,
	}
}
