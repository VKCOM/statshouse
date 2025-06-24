// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"context"
	"fmt"
	"sync"
)

type hijackedResponse struct {
	canceller HijackResponseCanceller
	hctx      *HandlerContext
}

// Motivation - we want zero allocations, so we cannot use lambda
type HijackResponseCanceller interface {
	CancelHijack(hctx *HandlerContext)
}

type serverConnCommon struct {
	server *Server

	debugName string // connection description for debug

	closeCtx       context.Context
	cancelCloseCtx context.CancelCauseFunc

	closedFlag  bool // cannot push responses if write half is already closed
	readFINFlag bool // writer continues until canGracefullyShutdown and empty queue

	mu                sync.Mutex
	longpollResponses map[int64]hijackedResponse

	releaseFun    func(hctx *HandlerContext) // reuse, signal inflight status, etc.
	pushUnlockFun func(hctx *HandlerContext) // actually send response, called with Locked mu and must Unlock
}

func (sc *serverConnCommon) HijackResponse(hctx *HandlerContext, canceller HijackResponseCanceller) error {
	// if hctx.noResult - we cannot return any other error from here because caller already updated its state. TODO - cancel immediately
	hctx.releaseRequest(sc.server)
	sc.makeLongpollResponse(hctx, canceller)
	return errHijackResponse
}

func (sc *serverConnCommon) AccountResponseMem(hctx *HandlerContext, respBodySizeEstimate int) error {
	respTaken, err := sc.server.accountResponseMem(sc.closeCtx, hctx.respTaken, respBodySizeEstimate, false)
	hctx.respTaken = respTaken
	return err
}

func (sc *serverConnCommon) RareLog(format string, args ...any) {
	sc.server.rareLog(&sc.server.lastOtherLog, format, args...)
}

func (sc *serverConnCommon) SendHijackedResponse(hctx *HandlerContext, err error) {
	if debugPrint {
		fmt.Printf("longpollResponses send %d\n", hctx.queryID)
	}
	sc.pushResponse(hctx, err, true)
}

func (sc *serverConnCommon) handle(ctx context.Context, hctx *HandlerContext) {
	err := sc.server.callHandler(ctx, hctx)
	sc.pushResponse(hctx, err, false)
}

func (sc *serverConnCommon) pushResponse(hctx *HandlerContext, err error, isLongpoll bool) {
	if !isLongpoll { // already released for longpoll
		hctx.releaseRequest(sc.server)
	}
	hctx.PrepareResponse(err)
	if !hctx.noResult { // do not spend time for accounting, will release anyway couple lines below
		hctx.respTaken, _ = sc.server.accountResponseMem(sc.closeCtx, hctx.respTaken, cap(hctx.Response), true)
	}
	if sc.server.opts.ResponseHook != nil {
		// we'd like to avoid calling handler for cancelled response,
		// but we do not want to do it under lock in push and cannot do it after lock due to potential race
		sc.server.opts.ResponseHook(hctx, err)
	}
	sc.mu.Lock()
	if isLongpoll {
		resp, ok := sc.longpollResponses[hctx.queryID]
		if !ok {
			if debugTrace {
				sc.server.addTrace(fmt.Sprintf("push (longpoll, NOP) %p", hctx))
			}
			sc.mu.Unlock()
			return // already released
		}
		sc.server.protocolStats[resp.hctx.protocolID].longPollsWaiting.Add(-1)
		delete(sc.longpollResponses, resp.hctx.queryID)
	}
	if sc.closedFlag || hctx.noResult {
		if debugTrace {
			sc.server.addTrace(fmt.Sprintf("push (closedFlag | noResult) %p", hctx))
		}
		sc.mu.Unlock()
		sc.releaseFun(hctx)
		return
	}
	if debugTrace {
		sc.server.addTrace(fmt.Sprintf("push %p", hctx))
	}
	sc.pushUnlockFun(hctx)
}

func (sc *serverConnCommon) makeLongpollResponse(hctx *HandlerContext, canceller HijackResponseCanceller) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	queryID := hctx.queryID

	if _, ok := sc.longpollResponses[queryID]; ok {
		sc.server.rareLog(&sc.server.lastHijackWarningLog, "[rpc.Server] - invariant violation, hijack response queryID collision")
		return
	}
	sc.server.protocolStats[hctx.protocolID].longPollsWaiting.Add(1)
	sc.longpollResponses[queryID] = hijackedResponse{canceller: canceller, hctx: hctx}
	if debugTrace {
		sc.server.addTrace(fmt.Sprintf("makeLongpollResponse %p %d", hctx, hctx.queryID))
	}
	if debugPrint {
		fmt.Printf("longpollResponses added %p %d\n", hctx, queryID)
	}
}

func (sc *serverConnCommon) cancelLongpollResponse(queryID int64) {
	sc.mu.Lock()
	resp, ok := sc.longpollResponses[queryID]
	if !ok {
		if debugTrace {
			sc.server.addTrace(fmt.Sprintf("cancelLongpollResponse (NOP) %d", queryID))
		}
		sc.mu.Unlock()
		return // Already sent/cancelled
	}
	if debugPrint {
		fmt.Printf("longpollResponses cancel %d\n", queryID)
	}
	sc.server.protocolStats[resp.hctx.protocolID].longPollsWaiting.Add(-1)
	delete(sc.longpollResponses, queryID)
	if debugTrace {
		sc.server.addTrace(fmt.Sprintf("cancelLongpollResponse %p %d", resp.hctx, queryID))
	}
	sc.mu.Unlock()
	resp.canceller.CancelHijack(resp.hctx)
	if sc.server.opts.ResponseHook != nil {
		sc.server.opts.ResponseHook(resp.hctx, errCancelHijack)
	}
	sc.releaseFun(resp.hctx)
}

func (sc *serverConnCommon) cancelAllLongpollResponses(setReadFINFlag bool) {
	sc.mu.Lock()
	longpollResponses := sc.longpollResponses
	sc.longpollResponses = nil // If makeLongpollResponse is called, we'll panic. But this must be impossible if SyncHandler follows protocol
	if debugTrace {
		sc.server.addTrace("cancelAllLongpollResponses")
	}
	if setReadFINFlag {
		sc.readFINFlag = true
	}
	sc.mu.Unlock()
	for _, resp := range longpollResponses {
		if debugPrint {
			fmt.Printf("longpollResponses cancel (all) %d\n", resp.hctx.queryID)
		}
		if debugTrace {
			sc.server.addTrace(fmt.Sprintf("cancelAllLongpollResponse %d", resp.hctx.queryID))
		}
		sc.server.protocolStats[resp.hctx.protocolID].longPollsWaiting.Add(-1)
		resp.canceller.CancelHijack(resp.hctx)
		sc.pushResponse(resp.hctx, errCancelHijack, false)
		// isLongpoll false because it would look it up in sc.longpollResponses and incorrectly consider hctx released and do nothing
	}
}
