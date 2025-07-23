package waitpool

import (
	"cmp"
	"context"
	"sync"

	"github.com/VKCOM/statshouse/internal/sqlitev2/heapmap"
)

type (
	WaitPool struct {
		mx              sync.Mutex
		lastKnownOffset int64
		events          *priorityQ
		id              int64
	}

	waitEvent struct {
		id     int64
		offset int64
		ch     chan struct{}
	}

	priorityQ struct {
		m map[int64]int
		h *heapmap.MinHeap[waitEvent]
	}
)

func NewPool() *WaitPool {
	return &WaitPool{
		events: newPriorityQ(),
	}
}

func newPriorityQ() *priorityQ {
	compare := func(a, b waitEvent) int {
		return cmp.Compare(a.offset, b.id)
	}
	m := map[int64]int{}
	return &priorityQ{
		h: heapmap.NewHeap[waitEvent](compare, func(a, b waitEvent, i, j int) {
			m[a.id] = i
			m[b.id] = j
		}),
		m: m,
	}
}

func (q *priorityQ) push(event waitEvent) {
	pos := q.h.Put(event)
	q.m[event.id] = pos
}
func (q *priorityQ) peek() waitEvent {
	return q.h.Peek()
}
func (q *priorityQ) pop() waitEvent {
	e := q.h.Pop()
	delete(q.m, e.id)
	return e

}
func (q *priorityQ) deleteIfExists(id int64) (waitEvent, bool) {
	if pos, ok := q.m[id]; ok {
		event := q.h.Extract(pos)
		delete(q.m, id)
		return event, true
	}
	return waitEvent{}, false

}

func (q *priorityQ) len() int {
	return q.h.Len()
}

func (p *WaitPool) Wait(ctx context.Context, offset int64) error {
	p.mx.Lock()
	if offset == 0 || offset <= p.lastKnownOffset {
		p.mx.Unlock()
		return nil
	}
	p.id++
	id := p.id
	ch := make(chan struct{})
	p.events.push(waitEvent{
		id:     id,
		offset: offset,
		ch:     ch,
	})
	p.mx.Unlock()
	select {
	case <-ctx.Done():
		p.mx.Lock()
		defer p.mx.Unlock()
		e, ok := p.events.deleteIfExists(id)
		if ok {
			close(e.ch)
		}
		return ctx.Err()
	case <-ch:
		return nil
	}

}

func (p *WaitPool) Notify(offset int64) {
	p.mx.Lock()
	defer p.mx.Unlock()
	p.lastKnownOffset = offset
	for p.events.len() > 0 {
		e := p.events.peek()
		if e.offset > offset {
			break
		}
		e = p.events.pop()
		close(e.ch)
	}
}
