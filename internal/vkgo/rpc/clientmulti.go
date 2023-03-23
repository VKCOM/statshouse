// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// TODO: experiment with https://golang.org/pkg/reflect/#Select instead of reqFinished/reqClosed

var (
	errMultiClosed = errors.New("rpc: Multi closed")
)

type callState struct {
	pc   *peerClient
	cctx *callContext
}

// Invariants:
// - Start, Wait and Close hold the mutex from start to finish; WaitAny waits unlocked
// - after Close(), Multi is in a terminal do-nothing state
// - returning when context.Context is Done() does not change state
// - trying to Wait for a request that was not sent yet is an error (but WaitAny will hang)
// - each non-nop Wait/WaitAny decreases the size of `calls` by 1

type Multi struct {
	c           *Client
	mu          sync.Mutex
	reqFinished chan int64
	reqClosed   chan int64
	calls       map[int64]callState
	closed      bool
	closeCh     chan struct{}
}

// Multi must be followed with a call to Multi.Close to release request state resources
func (c *Client) Multi(n int) *Multi {
	return &Multi{
		c:           c,
		reqFinished: make(chan int64, n),
		reqClosed:   make(chan int64, n),
		calls:       make(map[int64]callState, n),
		closeCh:     make(chan struct{}),
	}
}

func (m *Multi) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.closed {
		m.closed = true
		close(m.closeCh)

		for _, cs := range m.calls {
			cs.cctx.stale.Store(true)
			m.teardownCallStateLocked(cs, false)
		}
	}
}

func (m *Multi) teardownCallStateLocked(cs callState, cctxPut bool) {
	delete(m.calls, cs.cctx.queryID)
	cs.pc.teardownCall(cs.cctx, cctxPut)
}

func (m *Multi) Start(ctx context.Context, network string, address string, req *Request) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return 0, errMultiClosed
	}

	pc, queryID, err := m.c.start(network, address, req)
	if err != nil {
		return 0, err
	}

	deadline, _ := ctx.Deadline()
	cctx, err := pc.setupCall(req, deadline, queryID, m.reqFinished, m.reqClosed)
	if err != nil {
		return 0, err
	}

	m.calls[queryID] = callState{
		pc:   pc,
		cctx: cctx,
	}

	return queryID, nil
}

func (m *Multi) Wait(ctx context.Context, queryID int64) (*Response, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil, errMultiClosed
	}

	cs, ok := m.calls[queryID]
	if !ok {
		return nil, fmt.Errorf("unknown query ID %v", queryID)
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-cs.cctx.closed:
		defer m.teardownCallStateLocked(cs, false) // defer because we are using cctx below
		switch {
		case cs.cctx.sent.Load():
			return nil, ErrClientConnClosedSideEffect
		case cs.cctx.failIfNoConnection:
			return nil, ErrClientConnClosedNoSideEffect
		default:
			return nil, ErrClientClosed
		}
	case r := <-cs.cctx.result:
		m.teardownCallStateLocked(cs, true) // don't defer because we don't use cctx anymore
		if r.ok == nil {
			return nil, r.err
		}
		return r.ok, nil
	}
}

func (m *Multi) WaitAny(ctx context.Context) (int64, *Response, error) {
	for {
		select {
		case <-ctx.Done():
			return 0, nil, ctx.Err()
		case <-m.closeCh:
			return 0, nil, errMultiClosed
		case queryID := <-m.reqClosed:
			m.mu.Lock()
			cs, ok := m.calls[queryID]
			if !ok {
				m.mu.Unlock()
				continue // we already did a Wait()
			}

			defer m.mu.Unlock()
			defer m.teardownCallStateLocked(cs, false)
			switch {
			case cs.cctx.sent.Load():
				return queryID, nil, ErrClientConnClosedSideEffect
			case cs.cctx.failIfNoConnection:
				return queryID, nil, ErrClientConnClosedNoSideEffect
			default:
				return queryID, nil, ErrClientClosed
			}
		case queryID := <-m.reqFinished:
			m.mu.Lock()
			cs, ok := m.calls[queryID]
			if !ok {
				m.mu.Unlock()
				continue // we already did a Wait()
			}

			r := <-cs.cctx.result
			m.teardownCallStateLocked(cs, true)
			m.mu.Unlock()
			if r.ok == nil {
				return queryID, nil, r.err
			}
			return queryID, r.ok, nil
		}
	}
}

// DoMulti is a convenient way of doing multiple RPCs at once. If you need more control, consider using Multi directly.
func (c *Client) DoMulti(
	ctx context.Context,
	addresses []NetAddr,
	prepareRequest func(addr NetAddr, req *Request) error,
	processResponse func(addr NetAddr, resp *Response, err error) error,
) error {
	m := c.Multi(len(addresses))
	defer m.Close()
	queryIDtoAddr := make(map[int64]int, len(addresses))

	for i, addr := range addresses {
		r := c.GetRequest()
		err := prepareRequest(addr, r)
		if err != nil {
			return fmt.Errorf("failed to prepare request for %v: %w", addr, err)
		}

		queryID, err := m.Start(ctx, addr.Network, addr.Address, r)
		if err != nil {
			return err
		}

		queryIDtoAddr[queryID] = i
	}

	blindAttributionStart := 0
	seen := make([]bool, len(addresses))

	for range addresses {
		queryID, resp, err := m.WaitAny(ctx)
		i := queryIDtoAddr[queryID]

		if queryID == 0 || blindAttributionStart > 0 {
			// We consider that multi is in the terminal error state at this point;
			// error is most likely to be ErrClientClosed, context.Canceled or context.DeadlineExceeded.
			// Attribute everything after this point to one of the addresses we have not seen yet.
			for j := blindAttributionStart; j < len(seen); j++ {
				if !seen[j] {
					i = j
					blindAttributionStart = j + 1
					break
				}
			}
		}

		seen[i] = true
		err = processResponse(addresses[i], resp, err)
		c.PutResponse(resp)
		if err != nil {
			return fmt.Errorf("failed to handle response from %v: %w", addresses[i], err)
		}
	}

	return nil
}
