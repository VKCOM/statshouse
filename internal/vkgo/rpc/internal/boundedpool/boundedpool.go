// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package boundedpool

import "sync"

// LIFO, so last used goroutines tend to be used, greatly increasing speed
type T struct {
	mu      sync.Mutex
	cond    sync.Cond
	closed  bool
	free    []any
	created int
	create  int

	beforeWait func()
}

func New(create int, beforeWait func()) *T {
	t := &T{
		create:     create,
		beforeWait: beforeWait,
	}
	t.cond.L = &t.mu
	return t
}

func (t *T) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.closed {
		t.cond.Broadcast()
	}
	t.closed = true

	return nil
}

func (t *T) Get() (any, bool) {
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

	return nil, true
}

func (t *T) Put(v any) {
	t.mu.Lock()
	t.free = append(t.free, v)
	t.mu.Unlock() // unlock without defer to try to reduce lock contention

	t.cond.Signal()
}
