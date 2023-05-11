// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"sync"
	"time"
)

const workerGCDuration = time.Second * 60

type worker struct {
	s      *Server
	ch     chan *HandlerContext
	gcTime time.Time
}

func (w *worker) run(wg *WaitGroup) {
	defer wg.Done()
	for hctx := range w.ch {
		w.s.handle(hctx)
		w.s.workerPool.Put(w)
	}
}

// LIFO, so last used goroutines tend to be used, greatly increasing speed
type workerPool struct {
	mu      sync.Mutex
	cond    sync.Cond
	closed  bool
	free    []*worker
	created int
	create  int

	beforeWait func()
}

func workerPoolNew(create int, beforeWait func()) *workerPool {
	t := &workerPool{
		create:     create,
		beforeWait: beforeWait,
	}
	t.cond.L = &t.mu
	return t
}

func (t *workerPool) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, w := range t.free {
		close(w.ch)
	}
	t.created -= len(t.free)
	t.free = t.free[:0]

	t.closed = true
	t.cond.Broadcast()
}

func (t *workerPool) Created() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.created
}

func (t *workerPool) Get(wg *WaitGroup) (*worker, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for !(t.closed || len(t.free) > 0 || t.created < t.create) {
		if t.beforeWait != nil {
			t.beforeWait()
		}
		t.cond.Wait()
	}

	if t.closed {
		return nil, false
	}

	if n := len(t.free) - 1; n >= 0 {
		v := t.free[n]
		t.free = t.free[:n]
		return v, true
	}

	t.created++
	wg.Add(1) // Must be here to avoid race in Close

	return nil, true
}

func (t *workerPool) gcLocked(now time.Time) {
	if len(t.free) != 0 && now.After(t.free[0].gcTime) { // we could change to for, but we want low latency of Get
		close(t.free[0].ch)
		t.created--
		t.free = t.free[1:] // slice will rotate and make rare allocations, this must not be a problem
	}
}

func (t *workerPool) GC(now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.gcLocked(now)
}

func (t *workerPool) Put(v *worker) {
	t.mu.Lock()
	if t.closed {
		close(v.ch)
		t.created--
		t.mu.Unlock()
		return
	}
	now := time.Now()
	t.gcLocked(now)
	v.gcTime = now.Add(workerGCDuration)

	t.free = append(t.free, v)
	t.mu.Unlock() // unlock without defer to try to reduce lock contention

	t.cond.Signal()
}
