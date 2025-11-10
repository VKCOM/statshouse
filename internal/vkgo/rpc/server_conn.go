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
	"time"
)

var ErrAlreadyCanceled = fmt.Errorf("longpoll already have been canceled")
var errWriteEmptyResponseUnimplemented = &Error{
	Code:        TlErrorTimeout,
	Description: "write empty response is unimplemented",
}

// Motivation - we want zero allocations, so we cannot use lambda
type LongpollCanceller interface {
	CancelLongpoll(lh LongpollHandle)
	WriteEmptyResponse(lh LongpollHandle, resp *HandlerContext) error
}

// Fields in this struct are public, so the user could create his own unique handles for tests
type LongpollHandle struct {
	QueryID    int64
	CommonConn HandlerContextConnection
	Deadline   int64 // Unix timestamp
}

// When you are ready to reply to longpoll, call this function to abtain hctx.
// if this function returns (nil, false) you must do nothing.
// otherwise it returns (!nil, true) and you should write response to hctx.Response
// and send it with hctx.SendLongpollResponse
func (lh *LongpollHandle) FinishLongpoll() (*HandlerContext, bool) {
	// We do not want our users to try doing something with err.
	// And we want explicit warning that hctx will sometimes (rarely) be nil.
	// So we ignore error from commonConn.GetResponse() here.
	hctx, _ := lh.CommonConn.GetResponse(*lh)
	return hctx, hctx != nil
}

type hijackedResponse struct {
	// sorted for minimal structure size
	handle                 LongpollHandle
	requestTime            time.Time     // Longpoll handles are stored in the btree now, so we have to know the request time to be able to delete
	timeout                time.Duration // 0 means infinite, for this, both client and server must have infinite timeout
	actorID                int64
	requestFunctionName    string // Experimental. Generated handlers fill this during request processing.
	traceIDStr             string // allocated after extra parsing
	requestExtraFieldsmask uint32 // defensive copy
	reqTag                 uint32 // actual request can be wrapped 0 or more times within reqHeader
	protocolTransportID    byte   // copy from connection, we use it many times, virtual call is costly
	bodyFormatTL2          bool
	noResult               bool // defensive copy
	canceller              LongpollCanceller
}

type serverConnCommon struct {
	server *Server

	debugName string // connection description for debug

	closeCtx       context.Context
	cancelCloseCtx context.CancelCauseFunc

	closedFlag  bool // cannot push responses if write half is already closed
	readFINFlag bool // writer continues until canGracefullyShutdown and empty queue

	// This is pretty easy to get a data race/race condition here, so we use simple technique here
	// whoever deletes the response wins the race, so if GetResponse deletes the response then the user's
	// response will be sended
	// if SendEmptyResponse deletes the response, then the empty response will be sended
	// and finally, if cancelLongpollResponse deletes the response, then nothing is going to be sended
	mu                sync.Mutex
	longpollResponses map[int64]hijackedResponse

	acquireHctx   func() *HandlerContext
	releaseFun    func(hctx *HandlerContext) // reuse, signal inflight status, etc.
	pushUnlockFun func(hctx *HandlerContext) // actually send response, called with Locked mu and must Unlock
}

// This method allows users to set custom timeouts for long polls from a handler.
// Although this method exists, client code must respect timeouts from rpc.RequestExtra
// and shouldn't use this method.
// The method is going to be deleted once Persic migrates to default RPC timeouts instead of
// the custom timeout in request's tl body.
func (sc *serverConnCommon) StartLongpollWithTimeoutDeprecated(
	hctx *HandlerContext,
	canceller LongpollCanceller,
	timeout time.Duration,
) (LongpollHandle, error) {
	hctx.timeout = timeout
	return sc.StartLongpoll(hctx, canceller)
}

func (sc *serverConnCommon) StartLongpoll(hctx *HandlerContext, canceller LongpollCanceller) (LongpollHandle, error) {
	resp := sc.toHijackedResponse(hctx)
	resp.canceller = canceller

	sc.releaseFun(hctx)
	sc.makeLongpollResponse(resp)
	return resp.handle, errHijackResponse
}

func (sc *serverConnCommon) AccountResponseMem(hctx *HandlerContext, respBodySizeEstimate int) error {
	respTaken, err := sc.server.accountResponseMem(sc.closeCtx, hctx.respTaken, respBodySizeEstimate, false)
	hctx.respTaken = respTaken
	return err
}

func (sc *serverConnCommon) RareLog(format string, args ...any) {
	sc.server.rareLog(&sc.server.lastOtherLog, format, args...)
}

func (sc *serverConnCommon) SendLongpollResponse(hctx *HandlerContext, err error) {
	if debugPrint {
		fmt.Printf("longpollResponses send %d\n", hctx.queryID)
	}
	sc.pushResponse(hctx, err, true)
}

func (sc *serverConnCommon) toHijackedResponse(hctx *HandlerContext) hijackedResponse {
	timeout := hctx.timeout
	// Don't forget to check that timeout is big enough
	if timeout != 0 && (timeout <= sc.server.opts.MinimumLongpollTimeout) {
		timeout = sc.server.opts.MinimumLongpollTimeout
	}

	deadline := hctx.RequestTime().
		Add(timeout).
		Add(-sc.server.opts.MinimumLongpollTimeout / 5).UnixNano()

	return hijackedResponse{
		handle: LongpollHandle{
			CommonConn: hctx.commonConn,
			QueryID:    hctx.queryID,
			Deadline:   deadline,
		},
		requestTime:            hctx.RequestTime(),
		timeout:                timeout,
		actorID:                hctx.actorID,
		requestFunctionName:    hctx.RequestFunctionName,
		traceIDStr:             hctx.traceIDStr,
		requestExtraFieldsmask: hctx.requestExtraFieldsmask,
		reqTag:                 hctx.reqTag,
		protocolTransportID:    hctx.protocolTransportID,
		bodyFormatTL2:          hctx.bodyFormatTL2,
		noResult:               hctx.noResult,
	}
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

func (sc *serverConnCommon) makeLongpollResponse(resp hijackedResponse) {
	sc.mu.Lock()
	queryID := resp.handle.QueryID

	if _, ok := sc.longpollResponses[queryID]; ok {
		sc.server.rareLog(&sc.server.lastHijackWarningLog, "[rpc.Server] - invariant violation, hijack response queryID collision")
		sc.mu.Unlock()
		return
	}
	sc.server.protocolStats[resp.protocolTransportID].longPollsWaiting.Add(1)
	sc.longpollResponses[queryID] = resp
	sc.mu.Unlock()

	// It is important to perform AddLongpoll without holding sc.mu.
	// Although this leads to inconsistency between sc.longpollResponses
	// and longpollTree, it avoids a deadlock. The deadlock scenario is as
	// follows: makeLongpollResponse calls AddLongpoll, but
	// longpollTree.updatesCh is full. This causes makeLongpollResponse
	// to block on a write to updatesCh.
	// Meanwhile, the check loop is trying to lock sc.mu in SendEmptyResponse.
	// This inconsistency is not a problem because SendEmptyRequests
	// ignores longpolls that have already been answered or canceled.
	// Zero means infinite timeout, useful for tcp
	if resp.timeout != 0 {
		sc.server.longpollTree.AddLongpoll(resp.handle)
	}
	if debugTrace {
		sc.server.addTrace(fmt.Sprintf("makeLongpollResponse %v %d", resp, resp.handle.QueryID))
	}
	if debugPrint {
		fmt.Printf("longpollResponses added %v %d\n", resp, queryID)
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
	sc.server.protocolStats[resp.protocolTransportID].longPollsWaiting.Add(-1)
	delete(sc.longpollResponses, queryID)
	sc.server.longpollTree.DeleteLongpoll(resp.handle)
	if debugTrace {
		sc.server.addTrace(fmt.Sprintf("cancelLongpollResponse %v %d", resp, queryID))
	}
	sc.mu.Unlock()
	resp.canceller.CancelLongpoll(resp.handle)
	// if sc.server.opts.ResponseHook != nil {
	// 	sc.server.opts.ResponseHook(resp.lctx, errCancelHijack) // TODO: что тут делать в случае с лонгполлом?
	// }
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
			fmt.Printf("longpollResponses cancel (all) %d\n", resp.handle.QueryID)
		}
		if debugTrace {
			sc.server.addTrace(fmt.Sprintf("cancelAllLongpollResponse %d", resp.handle.QueryID))
		}
		sc.server.protocolStats[resp.protocolTransportID].longPollsWaiting.Add(-1)
		sc.server.longpollTree.DeleteLongpoll(resp.handle)
		resp.canceller.CancelLongpoll(resp.handle)
		// This happens only in shutdown, so we have to tell the client that his longpoll
		// was cancelled
		sc.pushResponse(sc.acquireHctx(), errCancelHijack, false)
		// isLongpoll false because it would look it up in sc.longpollResponses and incorrectly consider hctx released and do nothing
	}
}

func (sc *serverConnCommon) GetResponse(lh LongpollHandle) (*HandlerContext, error) {
	sc.mu.Lock()
	if resp, exist := sc.longpollResponses[lh.QueryID]; exist {
		// Race is over, user's handler wins
		delete(sc.longpollResponses, lh.QueryID)
		sc.mu.Unlock() // acquire might block

		hctx := sc.acquireHctx().fillFromHijackedResponse(resp)
		if err := sc.server.acquireHCtxResponse(sc.closeCtx, hctx); err != nil {
			return nil, err
		}

		return hctx, nil
	}
	sc.mu.Unlock()

	return nil, ErrAlreadyCanceled
}

func (sc *serverConnCommon) SendEmptyResponse(lh LongpollHandle) {
	sc.mu.Lock()
	resp, exist := sc.longpollResponses[lh.QueryID]
	if !exist {
		sc.mu.Unlock()
		// smb else won the race

		return
	}

	// Empty response won the race, acquire hctx
	delete(sc.longpollResponses, lh.QueryID)
	sc.mu.Unlock()

	hctx := sc.acquireHctx().fillFromHijackedResponse(resp)
	if err := sc.server.acquireHCtxResponse(sc.closeCtx, hctx); err != nil {
		sc.server.rareLog(&sc.server.lastOtherLog, "can't acquire bytes for response in empty req: %s", err.Error())
		return
	}

	err := resp.canceller.WriteEmptyResponse(
		lh,
		hctx,
	)

	var sendErr error
	if err == nil && len(hctx.Response) == 0 {
		sendErr = errWriteEmptyResponseUnimplemented
	}
	hctx.SendLongpollResponse(sendErr)
}
