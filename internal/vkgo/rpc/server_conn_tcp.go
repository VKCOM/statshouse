// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"net"
	"sync"
	"unsafe"

	"github.com/VKCOM/statshouse/internal/vkgo/rpc/internal/gen/tl"
)

type serverConnTCP struct {
	serverConnCommon

	listenAddr net.Addr

	inFlight int

	closeWaitCond sync.Cond

	conn         *PacketConn
	writeQ       []*HandlerContext // never contains noResult responses
	writeQCond   sync.Cond
	writeBuiltin bool
	writeLetsFin bool

	errHandler ErrHandlerFunc
}

var _ HandlerContextConnection = &serverConnTCP{}

func (sc *serverConnTCP) ListenAddr() net.Addr      { return sc.listenAddr }
func (sc *serverConnTCP) LocalAddr() net.Addr       { return sc.conn.LocalAddr() }
func (sc *serverConnTCP) RemoteAddr() net.Addr      { return sc.conn.RemoteAddr() }
func (sc *serverConnTCP) KeyID() [4]byte            { return sc.conn.KeyID() }
func (sc *serverConnTCP) ProtocolVersion() uint32   { return sc.conn.ProtocolVersion() }
func (sc *serverConnTCP) ProtocolTransportID() byte { return protocolTCP }
func (sc *serverConnTCP) ConnectionID() uintptr {
	return uintptr(unsafe.Pointer(sc))
}

func (sc *serverConnTCP) StartLongpoll(hctx *HandlerContext, canceller LongpollCanceller) (LongpollHandle, error) {
	lh, resp := sc.server.toLongpollContext(hctx, canceller)
	if err := sc.makeLongpollResponse(lh, resp); err != nil {
		return LongpollHandle{}, err
	}
	hctx.longpollStarted = true // do not send response
	return lh, nil
}

func (sc *serverConnTCP) finishLongpoll2(lh LongpollHandle) (*HandlerContext, LongpollCanceller, error) {
	resp, err := sc.finishLongpoll(lh.QueryID, protocolTCP)
	if err != nil {
		return nil, nil, err
	}
	if resp.deadline != 0 {
		sc.server.longpollTree.DeleteLongpoll(lh, resp.deadline)
	}
	hctx := sc.acquireHandlerCtx()
	hctx.queryID = lh.QueryID
	hctx.commonConn = lh.CommonConn
	hctx.handlerContextFields = resp.handlerContextFields
	if err = sc.server.acquireHCtxResponse(sc.closeCtx, hctx); err != nil {
		sc.releaseHandlerCtx(hctx)
		if sc.server.opts.DebugRPC {
			sc.server.opts.Logf("rpc_debug: %s FinishLongpoll connection closed queryID=%d", sc.debugName, lh.QueryID)
		}
		return nil, nil, err
	}
	return hctx, resp.canceller, nil
}

func (sc *serverConnTCP) DebugName() string {
	return sc.debugName
}

func (sc *serverConnTCP) FinishLongpoll(lh LongpollHandle) (*HandlerContext, error) {
	hctx, _, err := sc.finishLongpoll2(lh)
	if err == nil {
		if sc.server.opts.DebugRPC {
			sc.server.opts.Logf("rpc_debug: %s FinishLongpoll success queryID=%d", sc.debugName, lh.QueryID)
		}
	}
	return hctx, err
}

func (sc *serverConnTCP) CancelLongpoll(queryID int64) (LongpollCanceller, int64) {
	resp, err := sc.finishLongpoll(queryID, protocolTCP)
	if err != nil {
		return nil, 0
	}
	return resp.canceller, resp.deadline
}

func (sc *serverConnTCP) SendEmptyResponse(lh LongpollHandle) {
	hctx, canceller, err := sc.finishLongpoll2(lh)
	if err != nil {
		return
	}
	err = canceller.WriteEmptyResponse(lh, hctx)
	if err == nil && len(hctx.Response) == 0 {
		err = ErrLongpollNoEmptyResponse // TODO - make public, let clients return from their methods
	}
	if sc.server.opts.DebugRPC {
		sc.server.opts.Logf("rpc_debug: %s WriteEmptyResponse queryID=%d err: %v", sc.debugName, lh.QueryID, err)
	}
	sc.SendLongpollResponse(hctx, err)
}

func (sc *serverConnTCP) SetWriteBuiltin() {
	sc.mu.Lock()
	sc.writeBuiltin = true
	sc.mu.Unlock()
	sc.writeQCond.Signal()
}

func (sc *serverConnTCP) shutdown() {
	sc.mu.Lock()
	if sc.connectionStatus >= serverStatusShutdown {
		sc.mu.Unlock()
		return
	}
	sc.connectionStatus = serverStatusShutdown
	if sc.server.opts.DebugRPC {
		sc.server.opts.Logf("rpc_debug: %s Shutdown", sc.debugName)
	}

	queryIDs := make([]int64, 0, 8) // will use stack if len(sc.longpolls) <= 8
	for queryID := range sc.longpolls {
		queryIDs = append(queryIDs, queryID)
	}
	sc.mu.Unlock()
	sc.writeQCond.Signal()

	for _, queryID := range queryIDs {
		lh := LongpollHandle{QueryID: queryID, CommonConn: sc}
		sc.SendEmptyResponse(lh)
	}
}

func (sc *serverConnTCP) close(cause error) {
	sc.shutdown()
	sc.mu.Lock()
	if sc.connectionStatus >= serverStatusStopped {
		sc.mu.Unlock()
		return
	}
	if cause != nil && sc.errHandler != nil {
		sc.errHandler(cause)
	}
	sc.connectionStatus = serverStatusStopped
	writeQ := sc.writeQ
	sc.writeQ = nil
	sc.inFlight -= len(writeQ)
	sc.mu.Unlock()
	if sc.server.opts.DebugRPC {
		sc.server.opts.Logf("rpc_debug: %s Close", sc.debugName)
	}

	sc.cancelCloseCtx(cause)

	_ = sc.conn.Close()

	for _, hctx := range writeQ {
		sc.server.releaseHandlerCtx(hctx)
	}

	sc.closeWaitCond.Signal()

	sc.writeQCond.Signal()
}

func (sc *serverConnTCP) waitClosed() {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	for sc.inFlight != 0 {
		sc.closeWaitCond.Wait()
	}
	if len(sc.writeQ) != 0 {
		sc.server.opts.Logf("rpc: %s connection write queue length (%d) invariant violated", sc.debugName, len(sc.writeQ))
	}
}

func writeResponseUnlocked(conn *PacketConn, hctx *HandlerContext) error {
	resp := hctx.Response
	extraStart := hctx.extraStart

	if err := conn.WritePacketHeaderUnlocked(tl.RpcReqResultHeader{}.TLTag(), len(resp), DefaultPacketTimeout); err != nil {
		return err
	}
	// we serialize Extra after Body, so we have to twist spacetime a bit
	if err := conn.WritePacketBodyUnlocked(resp[extraStart:]); err != nil {
		return err
	}
	if err := conn.WritePacketBodyUnlocked(resp[:extraStart]); err != nil {
		return err
	}
	conn.WritePacketTrailerUnlocked()
	return nil
}

func (sc *serverConnTCP) acquireHandlerCtx() *HandlerContext {
	sc.mu.Lock()
	sc.inFlight++
	inFlight := sc.inFlight
	sc.mu.Unlock()
	if sc.server.opts.DebugRPC {
		sc.server.opts.Logf("rpc_debug: %s acquireHandlerCtx inFlight=%d", sc.debugName, inFlight)
	}

	return sc.server.acquireHandlerCtx(sc, protocolTCP)
}

func (sc *serverConnTCP) releaseHandlerCtx(hctx *HandlerContext) {
	sc.mu.Lock()
	sc.inFlight--
	inFlight := sc.inFlight
	if sc.connectionStatus >= serverStatusStopped && sc.inFlight == 0 {
		sc.closeWaitCond.Signal()
	}
	sc.mu.Unlock()
	if sc.server.opts.DebugRPC {
		sc.server.opts.Logf("rpc_debug: %s releaseHandlerCtx queryID=%d inFlight=%d", sc.debugName, hctx.queryID, inFlight)
	}
	sc.server.releaseHandlerCtx(hctx)
}

func (sc *serverConnTCP) SendLongpollResponse(hctx *HandlerContext, err error) {
	if hctx.longpollStarted || hctx.noResult {
		if hctx.noResult && err != nil {
			sc.server.rareLog(&sc.server.lastOtherLog, "rpc: failed to handle no_result query: #%v tag: #%08x method: %q error: %v", hctx.queryID, hctx.reqTag, hctx.requestFunctionName, err)
		}
		if sc.server.opts.DebugRPC {
			if hctx.longpollStarted {
				sc.server.opts.Logf("rpc_debug: %s SendLongpollResponse (longpollStarted) queryID=%d", sc.debugName, hctx.queryID)
			} else {
				sc.server.opts.Logf("rpc_debug: %s SendLongpollResponse (noResult) queryID=%d", sc.debugName, hctx.queryID)
			}
		}
		sc.releaseHandlerCtx(hctx)
		return
	}
	hctx.releaseRequest(sc.server)
	hctx.PrepareResponse(err)
	hctx.respTaken, _ = sc.server.accountResponseMem(sc.closeCtx, hctx.respTaken, cap(hctx.Response), true)
	if sc.server.opts.ResponseHook != nil {
		// we'd like to avoid calling handler for cancelled response,
		// but we do not want to do it under lock in push and cannot do it after lock due to potential race
		sc.server.opts.ResponseHook(hctx, err)
	}
	if hctx.extraStart == 0 {
		// Handler should return ErrNoHandler if it does not know how to return response
		sc.server.rareLog(&sc.server.lastOtherLog, "rpc: handler returned empty response with no error query #%v tag #%08x (%s)", hctx.queryID, hctx.reqTag, hctx.requestFunctionName)
	}

	sc.mu.Lock()
	if sc.connectionStatus >= serverStatusStopped {
		sc.mu.Unlock()
		if sc.server.opts.DebugRPC {
			sc.server.opts.Logf("rpc_debug: %s SendLongpollResponse (closed) queryID=%d", sc.debugName, hctx.queryID)
		}
		sc.releaseHandlerCtx(hctx)
		return
	}
	wasLen := len(sc.writeQ)
	sc.writeQ = append(sc.writeQ, hctx)
	queryID := hctx.queryID // after unlock, hctx could be released by goSend
	sc.mu.Unlock()
	if wasLen == 0 {
		sc.writeQCond.Signal()
	}
	if sc.server.opts.DebugRPC {
		sc.server.opts.Logf("rpc_debug: %s SendLongpollResponse (push) queryID=%d", sc.debugName, queryID)
	}
}
