package cache

import (
	"cmp"
	"log"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
	"pgregory.net/rapid"
)

type cacheState struct {
	cache    *QueryCache
	pushed   []*cachedStmtInfo
	maxSize  int
	now      int64
	usedHash map[QueryHash]bool
}

func (s *cacheState) init(t *rapid.T) {
	s.maxSize = 5
	s.cache = NewQueryCache(s.maxSize, log.New(os.Stdout, "[sqlite-engine]", log.LstdFlags))
	s.usedHash = map[QueryHash]bool{}

}

func (s *cacheState) Put(r *rapid.T) {
	lo := rapid.Uint64().Draw(r, "lo")
	hi := rapid.Uint64().Draw(r, "hi")
	s.now++
	h, t := QueryHash{Low: lo, High: hi}, time.Unix(s.now, 0)
	if s.usedHash[h] {
		r.SkipNow()
		return
	}
	s.usedHash[h] = true
	s.cache.Put(h, t, nil)
	s.pushed = append(s.pushed, &cachedStmtInfo{
		key:       h,
		lastTouch: t.Unix(),
		stmt:      nil,
	})
	slices.SortFunc(s.pushed, func(a, b *cachedStmtInfo) int {
		return cmp.Compare(a.lastTouch, b.lastTouch)
	})
}

func checkIsExists(r *rapid.T, s *cacheState, stmt *cachedStmtInfo) {
	i, ok := s.cache.queryCache[stmt.key]
	actualStmt := s.cache.h.heap[i]
	require.True(r, ok)
	require.Equal(r, *stmt, *actualStmt)
}

func (s *cacheState) Get(r *rapid.T) {
	s.now++
	if s.cache.size() == 0 {
		r.SkipNow()
		return
	}
	i := rapid.IntRange(0, len(s.pushed)-1).Draw(r, "index_to_get")
	stmt := s.pushed[i]
	r.Log("key to get: ", stmt.key)
	t := time.Unix(s.now, 0)
	actualStmt, ok := s.cache.Get(stmt.key, t)
	stmt.lastTouch = t.Unix()
	slices.SortFunc(s.pushed, func(a, b *cachedStmtInfo) int {
		return cmp.Compare(a.lastTouch, b.lastTouch)
	})
	require.True(r, ok)
	require.Equal(r, stmt.stmt, actualStmt)
}

func (s *cacheState) CloseTx(r *rapid.T) {
	oldSizeExpected := len(s.pushed)
	require.Equal(r, oldSizeExpected, s.cache.size())
	newExpectedSize := oldSizeExpected
	howManyDelete := oldSizeExpected - s.maxSize
	if howManyDelete < 0 {
		howManyDelete = 0
	}
	if oldSizeExpected > s.maxSize {
		newExpectedSize = s.maxSize
	}
	s.cache.FinishTX()
	require.Equal(r, newExpectedSize, s.cache.size())
	for i := 0; i < howManyDelete; i++ {
		r.Log("DELETE", s.pushed[i].key)
	}
	s.pushed = s.pushed[howManyDelete:]
}

func (s *cacheState) Check(r *rapid.T) {
	for _, stmt := range s.pushed {
		checkIsExists(r, s, stmt)
	}
}

func TestCache(t *testing.T) {
	isCacheTest = true
	rapid.Check(t, func(t *rapid.T) {
		m := cacheState{}
		m.init(t)
		t.Repeat(rapid.StateMachineActions(&m))
	})
}
