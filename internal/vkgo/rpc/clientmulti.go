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
	"math"
	"sync"
)

// TODO: experiment with https://golang.org/pkg/reflect/#Select instead of reqFinished/reqClosed

const (
	MissingMultiRequestID = uint64(math.MaxUint64)

	multiStatusSent     = 0
	multiStatusErrored  = 1
	multiStatusReturned = 2
)

var (
	errMultiClosed     = errors.New("rpc: Multi closed")
	errMultiNoRequests = errors.New("rpc: no Multi requests to wait for")

	callStatePool sync.Pool
)

type callState struct {
	pc      *peerClient
	queryID int64
	cctx    *callContext
	status  int
}

func getCallState() *callState {
	v := callStatePool.Get()
	if v != nil {
		return v.(*callState)
	}
	return &callState{}
}

func putCallState(v *callState) {
	*v = callState{}
	callStatePool.Put(v)
}

type callFinished struct {
	multiRequestID uint64
	callResult
}

type callClosed struct {
	multiRequestID uint64
	clientClosing  bool
}

// Invariants:
// - there is no concurrent execution of Multi methods
// - `calls` map never shrinks
// - after Close(), Multi is in a terminal do-nothing state
// - returning when context.Context is Done() does not change state
// - trying to wait when there is nothing to wait for is an error
// - each non-nop wait() decreases the number of calls with status == multiStatusSent by 1

type Multi struct {
	c           *Client
	mu          sync.Mutex
	reqFinished chan callFinished
	reqClosed   chan callClosed
	calls       map[uint64]*callState
	closed      bool
}

// Multi must be followed with a call to Multi.Close to release request state resources
func (c *Client) Multi(n int) *Multi {
	return &Multi{
		c:           c,
		reqFinished: make(chan callFinished, n),
		reqClosed:   make(chan callClosed, n),
		calls:       make(map[uint64]*callState, n),
	}
}

func (m *Multi) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.closed {
		m.closed = true

		for _, cs := range m.calls {
			if cs.status != multiStatusReturned {
				cs.cctx.stale.Store(true)
			}
			if cs.status == multiStatusSent {
				cs.pc.teardownCall(cs.queryID, nil)
			}
			putCallState(cs)
		}
	}
}

func (m *Multi) Start(ctx context.Context, network string, address string, req *Request, id uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return errMultiClosed
	}

	if id == MissingMultiRequestID {
		return fmt.Errorf("invalid request ID: MissingMultiRequestID (0x%x)", MissingMultiRequestID)
	}

	if _, ok := m.calls[id]; ok {
		return fmt.Errorf("duplicate request ID %v", id)
	}

	pc, err := m.c.start(network, address, req)
	if err != nil {
		return err
	}

	deadline, _ := ctx.Deadline()
	cctx, queryID, err := pc.setupCall(req, deadline, id, m.reqFinished, m.reqClosed)
	if err != nil {
		return err
	}

	cs := getCallState()
	cs.pc = pc
	cs.queryID = queryID
	cs.cctx = cctx

	m.calls[id] = cs

	return nil
}

func (m *Multi) Wait(ctx context.Context, id uint64) (*Response, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil, errMultiClosed
	}

	cs, ok := m.calls[id]
	if !ok {
		return nil, fmt.Errorf("missing request ID %v", id)
	}

	if cs.status != multiStatusSent {
		return nil, fmt.Errorf("request with ID %v already returned", id)
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-cs.cctx.closed:
		cs.status = multiStatusErrored
		cs.pc.teardownCall(cs.queryID, nil)
		if cs.cctx.sent.Load() {
			return nil, ErrClientConnClosedSideEffect
		}
		if cs.cctx.failIfNoConnection {
			return nil, ErrClientConnClosedNoSideEffect
		}
		return nil, ErrClientClosed
	case r := <-cs.cctx.result:
		cs.status = multiStatusReturned
		cs.pc.teardownCall(cs.queryID, cs.cctx)
		if r.ok == nil {
			return nil, r.err
		}
		return r.ok, nil
	}
}

func (m *Multi) WaitAny(ctx context.Context) (uint64, *Response, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return MissingMultiRequestID, nil, errMultiClosed
	}

	hasStartedCalls := false
	for _, cs := range m.calls {
		if cs.status == multiStatusSent {
			hasStartedCalls = true
			break
		}
	}
	if !hasStartedCalls {
		return MissingMultiRequestID, nil, errMultiNoRequests
	}

	for {
		select {
		case <-ctx.Done():
			return MissingMultiRequestID, nil, ctx.Err()
		case c := <-m.reqClosed:
			cs := m.calls[c.multiRequestID]
			if cs == nil {
				panic(fmt.Errorf("got closed request with unknown ID %v", c.multiRequestID))
			}
			if cs.status != multiStatusSent {
				continue
			}

			cs.status = multiStatusErrored
			cs.pc.teardownCall(cs.queryID, nil)
			if c.clientClosing {
				return c.multiRequestID, nil, ErrClientClosed
			}
			return c.multiRequestID, nil, ErrClientConnClosedSideEffect
		case r := <-m.reqFinished:
			cs := m.calls[r.multiRequestID]
			if cs == nil {
				panic(fmt.Errorf("got response with unknown ID %v", r.multiRequestID))
			}
			if cs.status != multiStatusSent {
				continue
			}

			<-cs.cctx.result // make sure cctx can be returned to the pool
			cs.status = multiStatusReturned
			cs.pc.teardownCall(cs.queryID, cs.cctx)
			if r.ok == nil {
				return r.multiRequestID, nil, r.err
			}
			return r.multiRequestID, r.ok, nil
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

	for i, addr := range addresses {
		r := c.GetRequest()
		err := prepareRequest(addr, r)
		if err != nil {
			return fmt.Errorf("failed to prepare request for %v: %w", addr, err)
		}

		err = m.Start(ctx, addr.Network, addr.Address, r, uint64(i))
		if err != nil {
			return err
		}
	}

	blindAttributionStart := 0
	seen := make([]bool, len(addresses))

	for range addresses {
		i, resp, err := m.WaitAny(ctx)

		if i == MissingMultiRequestID || blindAttributionStart > 0 {
			// We consider that multi is in the terminal error state at this point;
			// error is most likely to be ErrClientClosed, context.Canceled or context.DeadlineExceeded.
			// Attribute everything after this point to one of the addresses we have not seen yet.
			for j := blindAttributionStart; j < len(seen); j++ {
				if !seen[j] {
					i = uint64(j)
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
