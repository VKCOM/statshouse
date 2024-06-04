package cache

import (
	"log"
	"time"

	"github.com/vkcom/statshouse/internal/sqlitev2/sqlite0"
	"github.com/zeebo/xxh3"
	"go.uber.org/multierr"
)

/*
Этот кэш может стать проблемой в случаях когда много запросов, мы будем просто зря занимать память дял каждого sqlite соединения
Решение: пользователь явно выбирает что кешировать а что нет
*/
type (
	QueryCache struct {
		// требуется делать Reset всех stmt's чтобы иметь возможность вне транзакции сделать чекпоинт
		txQueryCache map[QueryHash]struct{}
		queryCache   map[QueryHash]int
		h            *minHeap
		cacheMaxSize int
		logger       *log.Logger
	}

	QueryHash struct {
		Low  uint64
		High uint64
	}

	cachedStmtInfo struct {
		key       QueryHash
		lastTouch int64
		stmt      *sqlite0.Stmt
	}
)

var isCacheTest = false

const cacheMaxSizeDefault = 3000

func NewQueryCache(cacheMaxSize int, logger *log.Logger) *QueryCache {
	if cacheMaxSize == 0 {
		cacheMaxSize = cacheMaxSizeDefault
	}
	cache := &QueryCache{
		txQueryCache: map[QueryHash]struct{}{},
		queryCache:   make(map[QueryHash]int),
		cacheMaxSize: cacheMaxSize,
		logger:       logger,
	}
	cache.h = newHeap(func(a, b QueryHash, i, j int) {
		cache.queryCache[a] = i
		cache.queryCache[b] = j
	})
	return cache
}

func (cache *QueryCache) FinishTX() {
	for h := range cache.txQueryCache {
		i := cache.queryCache[h]
		stmt := cache.h.heap[i].stmt
		if stmt == nil && isCacheTest {
			continue
		}
		err := stmt.Reset()
		if err != nil {
			cache.logger.Println("[error] failed to reset stmt(probably bug)", err.Error())
		}
	}
	clear(cache.txQueryCache)
	if len(cache.queryCache) > cache.cacheMaxSize {
		cache.evictCacheLocked(len(cache.queryCache) - cache.cacheMaxSize)
	}
}

func (cache *QueryCache) Put(key QueryHash, t time.Time, stmt *sqlite0.Stmt) {
	stmtInfo := &cachedStmtInfo{key: key, lastTouch: t.Unix(), stmt: stmt}
	ix := cache.h.put(stmtInfo)
	cache.txQueryCache[key] = struct{}{}
	cache.queryCache[key] = ix
}

func (cache *QueryCache) Get(key QueryHash, t time.Time) (res *sqlite0.Stmt, ok bool) {
	ix, ok := cache.queryCache[key]
	if ok {
		cachedStmt := cache.h.getAndUpdate(ix, t.Unix())
		if _, ok := cache.txQueryCache[key]; ok {
			if cachedStmt.stmt == nil && isCacheTest {
				return nil, true
			}
			_ = cachedStmt.stmt.Reset()
		} else {
			cache.txQueryCache[key] = struct{}{}
		}
		return cachedStmt.stmt, ok
	}

	return nil, false
}

func (cache *QueryCache) evictCacheLocked(count int) {
	for i := 0; i < count && cache.size() > 0; i++ {
		stmt := cache.h.pop()
		delete(cache.queryCache, stmt.key)
		if stmt.stmt == nil && isCacheTest {
			continue
		}
		err := stmt.stmt.Close()
		if err != nil {
			cache.logger.Println("[error] failed to close cached stmt:", err.Error())
		}
	}
}

func (cache *QueryCache) Close(err *error) {
	for _, ix := range cache.queryCache {
		multierr.AppendInto(err, cache.h.heap[ix].stmt.Close())
	}
}

func (cache *QueryCache) size() int {
	return len(cache.queryCache)
}

func CalcHashBytes(key []byte) QueryHash {
	h := xxh3.Hash128(key)
	return QueryHash{
		Low:  h.Lo,
		High: h.Hi,
	}
}
