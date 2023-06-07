// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"context"
	"sync"
)

// TODO - move to better place
// TODO - tests for WaitGroup

type WaitGroup struct {
	mu    sync.Mutex
	count int64
	ch    chan struct{} // is nil if count <= 0
}

func (w *WaitGroup) Add(delta int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.count += delta
	if w.count < 0 { // If this check is removes, WaitGroup will correctly wait for count < 0
		panic("WaitGroup counter is < 0")
	}
	if w.count <= 0 && w.ch != nil {
		close(w.ch)
		w.ch = nil
	}
}

func (w *WaitGroup) Done() {
	w.Add(-1)
}

func (w *WaitGroup) needsToWait() chan struct{} {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.count <= 0 {
		return nil // we create channel only if necessary
	}
	if w.ch == nil { // we are first to wait
		w.ch = make(chan struct{})
	}
	ch := w.ch
	return ch
}

func (w *WaitGroup) Wait(ctx context.Context) error {
	ch := w.needsToWait()
	if ch == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ch:
		return nil
	}
}

func (w *WaitGroup) WaitForever() {
	ch := w.needsToWait()
	if ch == nil {
		return
	}
	<-ch
}
