// Copyright 2025 V Kontakte LLC
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
	"time"

	"github.com/VKCOM/statshouse/internal/vkgo/semaphore"
)

var (
	errMultiClosed = errors.New("rpc: Multi closed")
)

// How to use:
// - each non-nop Wait/WaitAny decreases the size of `calls` by 1
// - trying to Wait/WaitAny for a request that was not sent yet is not a problem, Wait/WaitAny will simply wait until it is sent and received
// - returning from Wait/WaitAny when context.Context is Done() does not change state
// - after Close(), Multi is in a terminal do-nothing state

type Multi struct {
	c           *ClientImpl
	sem         *semaphore.Weighted
	mu          sync.Mutex
	multiResult chan callResult
	calls       map[int64]*clientConn
	results     map[int64]callResult
	closed      bool
	closeCh     chan struct{}
}

// Multi must be followed with a call to Multi.Close to release request state resources
func (c *ClientImpl) Multi(n int) *Multi {
	return &Multi{
		c:           c,
		sem:         semaphore.NewWeighted(int64(n)),
		multiResult: make(chan callResult, n),
		calls:       make(map[int64]*clientConn, n),
		results:     make(map[int64]callResult, n),
		closeCh:     make(chan struct{}),
	}
}

func (m *Multi) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return
	}
	m.closed = true
	close(m.closeCh)

	for queryID, pc := range m.calls {
		cctx := pc.cancelCall(queryID)
		if cctx != nil {
			// exclusive ownership of cctx by this function
			m.multiResult <- callResult{resp: cctx, err: errMultiClosed}
		}
	}
	for queryID := range m.results {
		delete(m.results, queryID)
	}
	// some responses may be in channel here
	// no receivers will block because there is enough space in channel for all responses
}

func (m *Multi) Client() Client {
	return m.c
}

func (m *Multi) teardownCallStateLocked(queryID int64) {
	delete(m.calls, queryID)
	m.sem.Release(1)
}

func (m *Multi) Start(ctx context.Context, network string, address string, req *Request) error {
	req.startTime = time.Now()
	if err := m.sem.Acquire(ctx, 1); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		m.sem.Release(1)
		return errMultiClosed
	}

	queryID := req.QueryID() // must not access req after setupCall
	cctx := m.c.getResponse(req)
	cctx.result = m.multiResult // does not touch cctx.singleResult
	pc, err := m.c.setupCall(ctx, NetAddr{network, address}, req, cctx)
	if err != nil {
		m.sem.Release(1)
		return err
	}

	m.calls[queryID] = pc

	return nil
}

func (m *Multi) waitHasResult(queryID int64) (*Response, error, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, errMultiClosed, true
	}

	if res, ok := m.results[queryID]; ok {
		delete(m.results, queryID)
		return res.resp, res.err, true
	}

	if _, ok := m.calls[queryID]; !ok {
		return nil, fmt.Errorf("unknown query ID %v", queryID), true
	}
	return nil, nil, false
}

func (m *Multi) Wait(ctx context.Context, queryID int64) (*Response, error) {
	if resp, err, ok := m.waitHasResult(queryID); ok {
		return resp, err
	}
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case finishedCall := <-m.multiResult: // got ownership of cctx
			qID := finishedCall.resp.queryID
			m.mu.Lock()
			m.teardownCallStateLocked(qID)
			if qID == queryID {
				m.mu.Unlock()
				return finishedCall.resp, finishedCall.err
			}
			m.results[qID] = finishedCall
			m.mu.Unlock()
			continue
		case <-m.closeCh:
			return nil, errMultiClosed
		}
	}
}

func (m *Multi) waitAnyHasResult() (int64, *Response, error, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return 0, nil, errMultiClosed, true
	}

	for queryID, finishedCall := range m.results {
		delete(m.results, queryID)
		return queryID, finishedCall.resp, finishedCall.err, true
	}

	return 0, nil, nil, false
}

func (m *Multi) WaitAny(ctx context.Context) (int64, *Response, error) {
	if queryID, resp, err, ok := m.waitAnyHasResult(); ok {
		return queryID, resp, err
	}
	select {
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	case finishedCall := <-m.multiResult:
		qID := finishedCall.resp.queryID
		m.mu.Lock()
		m.teardownCallStateLocked(qID)
		m.mu.Unlock()
		return qID, finishedCall.resp, finishedCall.err
	case <-m.closeCh:
		return 0, nil, errMultiClosed
	}
}

// DoMulti is a convenient way of doing multiple RPCs at once. If you need more control, consider using Multi directly.
func (c *ClientImpl) DoMulti(
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
		queryID := r.QueryID()

		err = m.Start(ctx, addr.Network, addr.Address, r)
		if err != nil {
			return err
		}

		queryIDtoAddr[queryID] = i
	}

	var blindErrors []error
	for range addresses {
		queryID, resp, err := m.WaitAny(ctx)
		i, ok := queryIDtoAddr[queryID]
		if !ok {
			// some errors like timeout cannot be attributed to particular queryID, so we assign them to random addresses
			blindErrors = append(blindErrors, err)
			continue
		}
		delete(queryIDtoAddr, queryID)
		err = processResponse(addresses[i], resp, err)
		c.PutResponse(resp)
		if err != nil {
			return fmt.Errorf("failed to handle response from %v: %w", addresses[i], err)
		}
	}

	for _, i := range queryIDtoAddr {
		err := processResponse(addresses[i], nil, blindErrors[0])
		blindErrors = blindErrors[1:]
		if err != nil {
			return fmt.Errorf("failed to handle response from %v: %w", addresses[i], err)
		}
	}

	return nil
}
