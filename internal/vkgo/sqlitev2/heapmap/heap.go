package heapmap

type MinHeap[A any] struct {
	heap       []A
	compare    func(a, b A) int
	size       int
	swapParent func(a, b A, i, j int)
}

const root = 1

func NewHeap[A any](compare func(a, b A) int, swapParent func(a, b A, i, j int)) *MinHeap[A] {
	return &MinHeap[A]{
		heap:       make([]A, 8),
		size:       0,
		swapParent: swapParent,
		compare:    compare,
	}
}

func (h *MinHeap[A]) swap(i, j int) {
	iKey := h.heap[i]
	jKey := h.heap[j]
	h.heap[i], h.heap[j] = h.heap[j], h.heap[i]
	h.swapParent(iKey, jKey, j, i)
}

func (h *MinHeap[A]) Put(v A) int {
	putPos := h.size + 1
	if putPos >= len(h.heap) {
		heapCpy := make([]A, h.size*2)
		copy(heapCpy, h.heap)
		h.heap = heapCpy
	}
	h.heap[putPos] = v
	h.size++
	return h.siftUp(putPos)
}

func (h *MinHeap[A]) siftUp(current int) int {
	for current != root {
		parent := current / 2

		if h.compare(h.heap[parent], h.heap[current]) > 0 {
			h.swap(parent, current)
			current = parent
		} else {
			return current
		}
	}
	return current
}

func (h *MinHeap[A]) siftDown(current int) {
	for {
		l := current * 2
		r := current*2 + 1
		if l > h.size {
			break
		}
		child := l
		if r <= h.size {

			if h.compare(h.heap[r], h.heap[child]) < 0 {
				child = r
			}
		}

		if h.compare(h.heap[child], h.heap[current]) < 0 {
			h.swap(child, current)
			current = child
		} else {
			break
		}
	}
}

//func (h *MinHeap[A, K]) getAndUpdate(i int, nowUnix int64) A {
//	stmt := h.heap[i]
//	h.extract(i)
//	stmt.lastTouch = nowUnix
//	h.put(stmt)
//	return stmt
//}

func (h *MinHeap[A]) Extract(i int) A {
	res := h.heap[i]
	oldSize := h.size
	h.size--
	if oldSize != i {
		h.swap(i, oldSize)
		h.siftUp(i)
		h.siftDown(i)
	}
	//h.heap[oldSize] = nil
	return res
}

func (h *MinHeap[A]) Len() int {
	return h.size
}

func (h *MinHeap[A]) Peek() A {
	return h.heap[root]
}

func (h *MinHeap[A]) Pop() A {
	res := h.heap[root]
	h.swap(root, h.size)
	//h.heap[h.size] = nil
	h.size--
	current := root
	h.siftDown(current)
	return res
}
