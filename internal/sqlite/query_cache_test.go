package sqlite

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
	"pgregory.net/rand"
	"pgregory.net/rapid"
)

func Test_heap_put(t *testing.T) {
	h := heap{}
	const n = evictSize + 10000
	for i := 0; i <= n; i++ {
		h.put(&cachedStmtInfo{lastTouch: int64(i)})
	}
	for i := evictSize - 1; i >= 0; i-- {
		require.Greater(t, h.size, 0)
		require.Equal(t, int64(i), h.peek().lastTouch)
		require.Equal(t, int64(i), h.pop().lastTouch)
	}
	require.Equal(t, 0, h.size)
}

func Test_heap_put_random(t *testing.T) {
	h := heap{size: 0}
	const n = evictSize + 10000
	arr := []int{}
	for i := 0; i <= n; i++ {
		x := rand.Intn(50)
		arr = append(arr, x)
		h.put(&cachedStmtInfo{lastTouch: int64(x)})
	}
	sort.Ints(arr)
	for i := evictSize - 1; i >= 0; i-- {
		require.Greater(t, h.size, 0)
		require.Equal(t, int64(arr[i]), h.peek().lastTouch)
		require.Equal(t, int64(arr[i]), h.pop().lastTouch)
	}
	require.Equal(t, 0, h.size)
}

type heapState struct {
	heap   *heap
	pushed []int64
}

func (s *heapState) init(t *rapid.T) {
	s.heap = &heap{}
	s.pushed = make([]int64, 0, evictSize)
}
func (s *heapState) Push(r *rapid.T) {
	n := rapid.Int64().Draw(r, "push")
	s.pushed = append(s.pushed, n)
	s.heap.put(&cachedStmtInfo{lastTouch: n})
	slices.Sort(s.pushed)
	if len(s.pushed) > evictSize {
		s.pushed = s.pushed[:evictSize]
	}
}

func (s *heapState) Pop(r *rapid.T) {
	if s.heap.size == 0 {
		r.SkipNow()
	}
	last := s.pushed[len(s.pushed)-1]
	s.pushed = s.pushed[:len(s.pushed)-1]
	require.Equal(r, last, s.heap.pop().lastTouch)
}

func (s *heapState) Check(r *rapid.T) {
	require.Equal(r, len(s.pushed), s.heap.size)
	if len(s.pushed) > 0 {
		last := s.pushed[len(s.pushed)-1]
		require.Equal(r, last, s.heap.peek().lastTouch)
	}
}

func TestHeap(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		m := heapState{}
		m.init(t)
		t.Run(rapid.StateMachineActions(&m))
	})
}
