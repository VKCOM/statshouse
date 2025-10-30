package buffer

import (
	"fmt"
	"sync"
)

var UncommitedItemsFound = fmt.Errorf("you must commit all items")

type doubleIndex struct {
	i  int
	v1 bool
}

func (ind *doubleIndex) Swap() {
	ind.i = 0
	ind.v1 = !ind.v1
}

type DoubleRingBuffer[v any] struct {
	mu   sync.Mutex
	v1   []v
	v2   []v
	tail doubleIndex
	head doubleIndex
}

func NewDoubleRingBuffer[v any](startSize int) *DoubleRingBuffer[v] {
	return &DoubleRingBuffer[v]{
		v1:   make([]v, 0, startSize),
		v2:   make([]v, 0, startSize),
		tail: doubleIndex{0, false},
		head: doubleIndex{0, true},
	}
}

func (b *DoubleRingBuffer[v]) PushUncommited(item v) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.head.v1 {
		b.v1 = append(b.v1, item)
		return
	}
	b.v2 = append(b.v2, item)
}

func (b *DoubleRingBuffer[v]) ReadAndCommit(readF func(item v)) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.head.v1 {
		for ; b.head.i < len(b.v1); b.head.i++ {
			readF(b.v1[b.head.i])
		}
		return
	}
	for ; b.head.i < len(b.v2); b.head.i++ {
		readF(b.v2[b.head.i])
	}
}

func (b *DoubleRingBuffer[v]) PopF(f func(item v) bool) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.head.v1 && b.head.i < len(b.v1) ||
		!b.head.v1 && b.head.i < len(b.v2) {
		return UncommitedItemsFound
	}
	if b.tail.v1 && b.tail.i >= len(b.v1) {
		b.tail.Swap()
		b.head.Swap()
		b.v1 = b.v1[:0]
	} else if !b.tail.v1 && b.tail.i >= len(b.v2) {
		b.tail.Swap()
		b.head.Swap()
		b.v2 = b.v2[:0]
	}
	for b.tail.v1 && b.tail.i < len(b.v1) || !b.tail.v1 && b.tail.i < len(b.v2) {
		if !f(b.last()) {
			break
		}
		b.tail.i++
	}
	return nil
}

func (b *DoubleRingBuffer[v]) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.head.Swap()
	b.tail.Swap()
	b.v1 = b.v1[:0]
	b.v2 = b.v2[:0]
}

func (b *DoubleRingBuffer[v]) last() v {
	if b.tail.v1 {
		return b.v1[b.tail.i]
	}
	return b.v2[b.tail.i]
}
