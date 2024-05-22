package cache

import (
	"cmp"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
	"pgregory.net/rapid"
)

func Test_heap_put(t *testing.T) {
	h := newHeap(func(a, b QueryHash, i, j int) {})
	const n = 10000
	for i := 0; i <= n; i++ {
		h.put(&cachedStmtInfo{lastTouch: int64(i)})
	}
	for i := 0; i <= n; i++ {
		require.Greater(t, h.size, 0)
		require.Equal(t, int64(i), h.peek().lastTouch)
		require.Equal(t, int64(i), h.pop().lastTouch)
	}
	require.Equal(t, 0, h.size)
}

func Test_heap_put_random(t *testing.T) {
	h := newHeap(func(a, b QueryHash, i, j int) {})
	const n = 10000
	arr := []int{}
	for i := 0; i <= n; i++ {
		x := rand.Intn(50)
		arr = append(arr, x)
		h.put(&cachedStmtInfo{lastTouch: int64(x)})
	}
	sort.Ints(arr)
	for i := 0; i <= n; i++ {
		require.Greater(t, h.size, 0)
		require.Equal(t, int64(arr[i]), h.peek().lastTouch)
		require.Equal(t, int64(arr[i]), h.pop().lastTouch)
	}
	require.Equal(t, 0, h.size)
}

type heapState struct {
	heap   *minHeap
	pushed []*cachedStmtInfo
	vToI   map[QueryHash]int

	usedTS map[int64]bool
}

func (s *heapState) init(t *rapid.T) {
	s.vToI = map[QueryHash]int{}
	s.heap = newHeap(func(a, b QueryHash, i, j int) {
		s.vToI[a] = i
		s.vToI[b] = j
	})
	s.usedTS = map[int64]bool{}
	s.pushed = make([]*cachedStmtInfo, 0)
}
func (s *heapState) Push(r *rapid.T) {
	lo := rapid.Uint64().Draw(r, "lo")
	hi := rapid.Uint64().Draw(r, "hi")
	n := rapid.Int64().Draw(r, "push value")
	if s.usedTS[n] {
		r.SkipNow()
		return
	}
	s.usedTS[n] = true
	stmt := &cachedStmtInfo{key: QueryHash{
		Low:  lo,
		High: hi,
	}, lastTouch: n}
	if _, ok := s.vToI[stmt.key]; ok {
		r.SkipNow()
		return
	}
	s.pushed = append(s.pushed, stmt)
	ix := s.heap.put(stmt)
	s.vToI[stmt.key] = ix
	slices.SortFunc(s.pushed, func(a, b *cachedStmtInfo) int {
		return cmp.Compare(a.lastTouch, b.lastTouch)
	})
}

func (s *heapState) Extract(r *rapid.T) {
	if s.heap.size == 0 {
		r.SkipNow()
		return
	}
	i := rapid.IntRange(0, len(s.pushed)-1).Draw(r, "index_to_delete")
	elToDelete := s.pushed[i]
	ixToDelete := s.vToI[elToDelete.key]
	deleted := s.heap.extract(ixToDelete)
	delete(s.vToI, deleted.key)
	require.Equal(r, *elToDelete, *deleted)
	s.pushed = slices.Delete(s.pushed, i, i+1)
}

func (s *heapState) Pop(r *rapid.T) {
	if s.heap.size == 0 {
		r.SkipNow()
		return
	}
	rootExpected := s.pushed[0]
	s.pushed = s.pushed[1:]
	rootActual := s.heap.pop()
	delete(s.vToI, rootActual.key)
	require.Equal(r, *rootExpected, *rootActual)
}

func (s *heapState) Check(r *rapid.T) {
	require.Equal(r, len(s.pushed), s.heap.size)
	require.Equal(r, s.heap.size, len(s.vToI))
	for i := range s.heap.heap {
		if i >= root && i <= s.heap.size {
			require.NotNil(r, s.heap.heap[i])
		} else {
			require.Nil(r, s.heap.heap[i])
		}
	}
	for i := root; i <= s.heap.size; i++ {

	}
	if len(s.pushed) > 0 {
		last := s.pushed[0]
		root := s.heap.peek()
		require.Equal(r, 1, s.vToI[root.key])
		require.Equal(r, *last, *root)
	}
}

func TestHeap(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		m := heapState{}
		m.init(t)
		t.Repeat(rapid.StateMachineActions(&m))
	})
}
