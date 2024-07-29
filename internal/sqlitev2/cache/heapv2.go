package cache

type minHeap struct {
	heap       []*cachedStmtInfo
	size       int
	swapParent func(a, b QueryHash, i, j int)
}

const root = 1

func newHeap(swapParent func(a, b QueryHash, i, j int)) *minHeap {
	return &minHeap{
		heap:       make([]*cachedStmtInfo, 8),
		size:       0,
		swapParent: swapParent,
	}
}

func (h *minHeap) swap(i, j int) {
	iKey := h.heap[i].key
	jKey := h.heap[j].key
	h.heap[i], h.heap[j] = h.heap[j], h.heap[i]
	h.swapParent(iKey, jKey, j, i)
}

func (h *minHeap) put(stmt *cachedStmtInfo) int {
	putPos := h.size + 1
	if putPos >= len(h.heap) {
		heapCpy := make([]*cachedStmtInfo, h.size*2)
		copy(heapCpy, h.heap)
		h.heap = heapCpy
	}
	h.heap[putPos] = stmt
	h.size++
	return h.siftUp(putPos)
}

func (h *minHeap) siftUp(current int) int {
	for current != root {
		parent := current / 2
		if h.heap[parent].lastTouch > h.heap[current].lastTouch {
			h.swap(parent, current)
			current = parent
		} else {
			return current
		}
	}
	return current
}

func (h *minHeap) siftDown(current int) {
	for {
		l := current * 2
		r := current*2 + 1
		if l > h.size {
			break
		}
		child := l
		if r <= h.size {
			if h.heap[r].lastTouch < h.heap[child].lastTouch {
				child = r
			}
		}
		if h.heap[child].lastTouch < h.heap[current].lastTouch {
			h.swap(child, current)
			current = child
		} else {
			break
		}
	}
}
func (h *minHeap) getAndUpdate(i int, nowUnix int64) *cachedStmtInfo {
	stmt := h.heap[i]
	h.extract(i)
	stmt.lastTouch = nowUnix
	h.put(stmt)
	return stmt
}

func (h *minHeap) extract(i int) *cachedStmtInfo {
	res := h.heap[i]
	oldSize := h.size
	h.size--
	if oldSize != i {
		h.swap(i, oldSize)
		h.siftUp(i)
		h.siftDown(i)
	}
	h.heap[oldSize] = nil
	return res
}

func (h *minHeap) peek() *cachedStmtInfo {
	return h.heap[root]
}

func (h *minHeap) pop() *cachedStmtInfo {
	res := h.heap[root]
	h.swap(root, h.size)
	h.heap[h.size] = nil
	h.size--
	current := root
	h.siftDown(current)
	return res
}
