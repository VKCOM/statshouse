package heapmap

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
	h := NewHeap(cmp.Compare[int], func(a, b int, i, j int) {})
	const n = 10000
	for i := 0; i <= n; i++ {
		h.Put(i)
	}
	for i := 0; i <= n; i++ {
		require.Greater(t, h.size, 0)
		require.Equal(t, int(i), h.Peek())
		require.Equal(t, int(i), h.Pop())
	}
	require.Equal(t, 0, h.size)
}

func Test_heap_put_random(t *testing.T) {
	h := NewHeap(cmp.Compare[int], func(a, b int, i, j int) {})
	const n = 10000
	arr := []int{}
	for i := 0; i <= n; i++ {
		x := rand.Intn(50)
		arr = append(arr, x)
		h.Put(x)
	}
	sort.Ints(arr)
	for i := 0; i <= n; i++ {
		require.Greater(t, h.size, 0)
		require.Equal(t, int(arr[i]), h.Peek())
		require.Equal(t, int(arr[i]), h.Pop())
	}
	require.Equal(t, 0, h.size)
}

type heapState struct {
	heap *MinHeap[*event]

	pushed []*event
	vToI   map[int64]int

	usedTS map[int]bool
}

type event struct {
	id int64
	v  int
}

func (s *heapState) init(t *rapid.T) {
	s.vToI = map[int64]int{}
	s.heap = NewHeap(func(a, b *event) int {
		return cmp.Compare(a.v, b.v)
	}, func(a, b *event, i, j int) {
		s.vToI[a.id] = i
		s.vToI[b.id] = j
	})
	s.usedTS = map[int]bool{}
	s.pushed = make([]*event, 0)
}
func (s *heapState) Push(r *rapid.T) {
	id := rapid.Int64().Draw(r, "id")
	n := rapid.Int().Draw(r, "push value")
	if s.usedTS[n] {
		r.SkipNow()
		return
	}
	s.usedTS[n] = true
	e := &event{id: id, v: n}
	if _, ok := s.vToI[e.id]; ok {
		r.SkipNow()
		return
	}
	s.pushed = append(s.pushed, e)
	ix := s.heap.Put(e)
	s.vToI[e.id] = ix
	slices.SortFunc(s.pushed, func(a, b *event) int {
		return cmp.Compare(a.v, b.v)
	})
}

func (s *heapState) Extract(r *rapid.T) {
	if s.heap.size == 0 {
		r.SkipNow()
		return
	}
	i := rapid.IntRange(0, len(s.pushed)-1).Draw(r, "index_to_delete")
	elToDelete := s.pushed[i]
	ixToDelete := s.vToI[elToDelete.id]
	deleted := s.heap.Extract(ixToDelete)
	delete(s.vToI, deleted.id)
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
	rootActual := s.heap.Pop()
	delete(s.vToI, rootActual.id)
	require.Equal(r, *rootExpected, *rootActual)
}

func (s *heapState) Check(r *rapid.T) {
	require.Equal(r, len(s.pushed), s.heap.size)
	require.Equal(r, s.heap.size, len(s.vToI))
	for i := range s.heap.heap {
		if i >= root && i <= s.heap.size {
			require.NotNil(r, s.heap.heap[i])
		} // else {
		//	require.Nil(r, s.heap.heap[i])
		//}
	}
	for i := root; i <= s.heap.size; i++ {

	}
	if len(s.pushed) > 0 {
		last := s.pushed[0]
		root := s.heap.Peek()
		require.Equal(r, 1, s.vToI[root.id])
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
