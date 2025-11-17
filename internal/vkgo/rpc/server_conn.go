// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"context"
	"fmt"
	"net"
	"sync"
)

// Motivation - we want zero allocations, so we cannot use lambda
type LongpollCanceller interface {
	CancelLongpoll(lh LongpollHandle)
	// should cancel longpoll and
	// either write empty (NOP) response, or return ErrLongpollNoEmptyResponse
	WriteEmptyResponse(lh LongpollHandle, resp *HandlerContext) error
}

// Fields in this struct are public, so the user could create his own unique handles for tests
type LongpollHandle struct {
	QueryID    int64
	CommonConn HandlerContextConnection
}

// When you are ready to reply to longpoll, call this function to abtain hctx.
// if this function returns (nil, false) you must do nothing.
// otherwise it returns (!nil, true) and you should write response to hctx.Response
// and send it with hctx.SendLongpollResponse
func (lh *LongpollHandle) FinishLongpoll() (*HandlerContext, bool) {
	// We do not want our users to try doing something with err.
	// And we want explicit warning that hctx will sometimes (rarely) be nil.
	// So we ignore error from commonConn.GetResponse() here.
	hctx, _ := lh.CommonConn.FinishLongpoll(*lh)
	return hctx, hctx != nil
}

type longpollHctx struct {
	canceller LongpollCanceller
	deadline  int64 // calculated from requestTime and timeout. int64 so total order property is obvious (unlike time.Time)

	handlerContextFields
}

type serverConnCommon struct {
	server *Server

	debugName string // connection description for debug

	closeCtx       context.Context
	cancelCloseCtx context.CancelCauseFunc

	// This is pretty easy to get a data race/race condition here, so we use simple technique here
	// whoever deletes the response wins the race, so if GetResponse deletes the response then the user's
	// response will be sent
	// if SendEmptyResponse deletes the response, then the empty response will be sended
	// and finally, if cancelLongpollResponse deletes the response, then nothing is going to be sended
	mu               sync.Mutex
	longpolls        map[int64]longpollHctx
	connectionStatus int
}

func (sc *serverConnCommon) setDebugName(protocol string, remote net.Addr, local net.Addr) {
	if sc.server.opts.DebugRPC {
		sc.debugName = fmt.Sprintf("%s(%s%d %s->%s)", protocol, sc.server.debugNameForTests, sc.server.nextConnID.Add(1), remote, local)
	} else {
		sc.debugName = fmt.Sprintf("%s(%s->%s)", protocol, remote, local)
	}
}
func (sc *serverConnCommon) AccountResponseMem(hctx *HandlerContext, respBodySizeEstimate int) (err error) {
	hctx.respTaken, err = sc.server.accountResponseMem(sc.closeCtx, hctx.respTaken, respBodySizeEstimate, false)
	return err
}

func (sc *serverConnCommon) makeLongpollResponse(lh LongpollHandle, resp longpollHctx) error {
	sc.mu.Lock()
	if sc.connectionStatus >= serverStatusShutdown { // we must not start longpolls in this state
		sc.mu.Unlock()
		return errGracefulShutdown
	}
	if _, ok := sc.longpolls[lh.QueryID]; ok {
		sc.mu.Unlock()
		sc.server.rareLog(&sc.server.lastLongpollWarningLog, "rpc: %s invariant violation, longpoll response queryID %d collision", sc.debugName, lh.QueryID)
		return errLongpollQueryIDCollision
	}
	sc.longpolls[lh.QueryID] = resp
	sc.mu.Unlock()

	sc.server.protocolStats[resp.protocolTransportID].longPollsWaiting.Add(1)

	// It is important to perform AddLongpoll without holding sc.mu.
	// Although this leads to inconsistency between sc.longpolls
	// and longpollTree, it avoids a deadlock. The deadlock scenario is as
	// follows: makeLongpollResponse calls AddLongpoll, but
	// longpollTree.updatesCh is full. This causes makeLongpollResponse
	// to block on write to updatesCh.
	// Meanwhile, the check loop is trying to lock sc.mu in SendEmptyResponse.
	// This inconsistency is not a problem because SendEmptyRequests
	// ignores longpolls that have already been answered or canceled.
	// Zero means infinite timeout, useful for tcp
	if resp.deadline != 0 {
		sc.server.longpollTree.AddLongpoll(lh, resp.deadline)
	}
	if sc.server.opts.DebugRPC {
		sc.server.opts.Logf("rpc_debug: %s StartLongpoll queryID=%d, timeout=%v, deadline=%v", sc.debugName, lh.QueryID, resp.timeout, resp.deadline)
	}
	return nil
}

func (sc *serverConnCommon) finishLongpoll(queryID int64, protocol int) (longpollHctx, error) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if resp, exist := sc.longpolls[queryID]; exist {
		// Race is over, user's handler wins
		delete(sc.longpolls, queryID)
		sc.server.protocolStats[protocolTCP].longPollsWaiting.Add(-1)
		return resp, nil
	}
	if sc.server.opts.DebugRPC {
		sc.server.opts.Logf("rpc_debug: %s FinishLongpoll already cancelled queryID=%d", sc.debugName, queryID)
	}
	return longpollHctx{}, errAlreadyCanceled
}
