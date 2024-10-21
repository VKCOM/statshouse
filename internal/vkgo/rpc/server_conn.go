// Copyright 2024 V Kontakte LLC
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
	"time"

	"github.com/vkcom/statshouse/internal/vkgo/rpc/internal/gen/tl"
)

type serverConn struct {
	closeCtx       context.Context
	cancelCloseCtx context.CancelCauseFunc

	server      *Server
	listenAddr  net.Addr
	maxInflight int

	mu   sync.Mutex
	cond sync.Cond

	closeWaitCond sync.Cond

	longpollResponses map[int64]hijackedResponse

	hctxPool     []*HandlerContext // TODO - move to Server?
	hctxCreated  int
	readFINFlag  bool // writer continues until canGracefullyShutdown and empty queue
	closedFlag   bool
	conn         *PacketConn
	writeQ       []*HandlerContext // never contains noResult responses
	writeQCond   sync.Cond
	writeBuiltin bool
	writeLetsFin bool

	errHandler ErrHandlerFunc
}

var _ HandlerContextConnection = &serverConn{}

type hijackedResponse struct {
	canceller HijackResponseCanceller
	hctx      *HandlerContext
}

// Motivation - we want zero allocations, so we cannot use lambda
type HijackResponseCanceller interface {
	CancelHijack(hctx *HandlerContext)
}

func (sc *serverConn) HijackResponse(hctx *HandlerContext, canceller HijackResponseCanceller) error {
	// if hctx.noResult - we cannot return any other error from here because caller already updated its state. TODO - cancel immediately
	sc.releaseRequest(hctx)
	sc.makeLongpollResponse(hctx, canceller)
	return errHijackResponse
}

func (sc *serverConn) SendHijackedResponse(hctx *HandlerContext, err error) {
	if debugPrint {
		fmt.Printf("longpollResponses send %d\n", hctx.queryID)
	}
	sc.pushResponse(hctx, err, true)
}

func (sc *serverConn) AccountResponseMem(hctx *HandlerContext, respBodySizeEstimate int) error {
	respTaken, err := sc.server.accountResponseMem(sc.closeCtx, hctx.respTaken, respBodySizeEstimate, false)
	hctx.respTaken = respTaken
	return err
}

func (sc *serverConn) RareLog(format string, args ...any) {
	sc.server.rareLog(&sc.server.lastOtherLog, format, args...)
}

func (sc *serverConn) push(hctx *HandlerContext, isLongpoll bool) {
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
		sc.server.statLongPollsWaiting.Dec()
		delete(sc.longpollResponses, resp.hctx.queryID)
	}
	if sc.closedFlag || hctx.noResult {
		if debugTrace {
			sc.server.addTrace(fmt.Sprintf("push (closedFlag | noResult) %p", hctx))
		}
		closedFlag := sc.closedFlag
		sc.mu.Unlock()
		sc.releaseHandlerCtx(hctx)
		if closedFlag {
			sc.server.rareLog(&sc.server.lastPushToClosedLog, "attempt to push response to closed connection to %v", sc.conn.remoteAddr)
		}
		return
	}
	if debugTrace {
		sc.server.addTrace(fmt.Sprintf("push %p", hctx))
	}
	wasLen := len(sc.writeQ)
	sc.writeQ = append(sc.writeQ, hctx)
	sc.mu.Unlock()

	if wasLen == 0 {
		sc.writeQCond.Signal()
	}
}

func (sc *serverConn) sendLetsFin() {
	if !sc.conn.FlagCancelReq() {
		// We do not close here, because connections with declared cancel support can also freeze/not respond to protocol
		return
	}
	sc.mu.Lock()
	if sc.closedFlag || sc.readFINFlag {
		sc.mu.Unlock()
		return
	}
	sc.writeLetsFin = true
	sc.mu.Unlock()
	sc.writeQCond.Signal()
}

func (sc *serverConn) SetReadFIN() {
	sc.mu.Lock()
	sc.readFINFlag = true
	sc.mu.Unlock()
	sc.writeQCond.Signal()
}

func (sc *serverConn) SetWriteBuiltin() {
	sc.mu.Lock()
	sc.writeBuiltin = true
	sc.mu.Unlock()
	sc.writeQCond.Signal()
}

func (sc *serverConn) close(cause error) {
	sc.mu.Lock()
	if sc.closedFlag {
		sc.mu.Unlock()
		return
	}
	if cause != nil && sc.errHandler != nil {
		sc.errHandler(cause)
	}
	sc.closedFlag = true
	writeQ := sc.writeQ
	sc.writeQ = nil
	sc.hctxCreated -= len(writeQ) // we do not want to take locks to return contexts to pool. We simply release memory below
	sc.server.statRequestsCurrent.Add(-int64(len(writeQ)))
	if debugTrace {
		sc.server.addTrace(fmt.Sprintf("Close %d", len(writeQ)))
	}
	sc.mu.Unlock()

	sc.cancelCloseCtx(cause)

	_ = sc.conn.Close()

	for _, hctx := range writeQ {
		sc.releaseRequest(hctx)
		sc.releaseResponse(hctx)
	}

	sc.cond.Broadcast()    // wake up everyone who waits on !sc.closedFlag
	sc.writeQCond.Signal() // or writeQ
}

func (sc *serverConn) WaitClosed() error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	for !sc.canGracefullyShutdown() {
		sc.closeWaitCond.Wait()
	}
	if len(sc.writeQ) != 0 {
		sc.server.opts.Logf("rpc: connection write queue length (%d) invariant violated", len(sc.writeQ))
	}
	return nil
}

func writeResponseUnlocked(conn *PacketConn, hctx *HandlerContext) error {
	return hctx.ServerRequest.writeReponseUnlocked(conn)
}

func (hctx *ServerRequest) writeReponseUnlocked(conn *PacketConn) error {
	resp := hctx.Response
	extraStart := hctx.extraStart

	if err := conn.writePacketHeaderUnlocked(tl.RpcReqResultHeader{}.TLTag(), len(resp), DefaultPacketTimeout); err != nil {
		return err
	}
	// we serialize Extra after Body, so we have to twist spacetime a bit
	if err := conn.writePacketBodyUnlocked(resp[extraStart:]); err != nil {
		return err
	}
	if err := conn.writePacketBodyUnlocked(resp[:extraStart]); err != nil {
		return err
	}
	conn.writePacketTrailerUnlocked()
	return nil
}

// do not take into account long poll responses, as 1) they are not valuable 2) waiting for them can be... long
func (sc *serverConn) canGracefullyShutdown() bool {
	return sc.hctxCreated == len(sc.hctxPool)+len(sc.longpollResponses)
}

func (req *ServerRequest) WriteReponseAndFlush(conn *PacketConn, err error, rareLog func(format string, args ...any)) error {
	req.extraStart = len(req.Response)
	err = req.prepareResponseBody(err, rareLog)
	if err != nil {
		return err
	}
	conn.writeMu.Lock()
	defer conn.writeMu.Unlock()
	err = req.writeReponseUnlocked(conn)
	if err != nil {
		return err
	}
	conn.FlushUnlocked()
	return nil
}

func (req *ServerRequest) ForwardAndFlush(conn *PacketConn, tip uint32, timeout time.Duration) error {
	switch tip {
	case tl.RpcCancelReq{}.TLTag(), tl.RpcClientWantsFin{}.TLTag():
		conn.writeMu.Lock()
		defer conn.writeMu.Unlock()
		err := writeCustomPacketUnlocked(conn, tip, req.Request, timeout)
		if err != nil {
			return err
		}
		return conn.FlushUnlocked()
	case tl.RpcInvokeReqHeader{}.TLTag():
		fwd := Request{
			Body:    req.Request,
			Extra:   req.RequestExtra,
			queryID: req.QueryID,
		}
		if err := preparePacket(&fwd); err != nil {
			return err
		}
		conn.writeMu.Lock()
		defer conn.writeMu.Unlock()
		if err := writeRequestUnlocked(conn, &fwd, timeout); err != nil {
			return err
		}
		return conn.FlushUnlocked()
	default:
		return fmt.Errorf("unknown packet type 0x%x", tip)
	}
}

func (sc *serverConn) acquireHandlerCtx(tip uint32, options *ServerOptions) (*HandlerContext, bool) {
	sc.mu.Lock()
	for !(sc.closedFlag || len(sc.hctxPool) > 0 || sc.hctxCreated < sc.maxInflight) {
		sc.server.rareLog(&sc.server.lastHctxWaitLog, "rpc: waiting to acquire handler context; consider increasing Server.MaxInflightPackets")
		sc.cond.Wait()
	}
	if sc.closedFlag {
		sc.mu.Unlock()
		return nil, false
	}

	if n := len(sc.hctxPool) - 1; n >= 0 {
		hctx := sc.hctxPool[n]
		sc.hctxPool = sc.hctxPool[:n]
		if debugTrace {
			sc.server.addTrace(fmt.Sprintf("acquireHandlerCtx (pool) %p %x", hctx, tip))
		}
		sc.mu.Unlock()
		return hctx, true
	}

	sc.hctxCreated++
	hctx := &HandlerContext{}
	if debugTrace {
		sc.server.addTrace(fmt.Sprintf("acquireHandlerCtx (new) %p %x", hctx, tip))
	}
	sc.mu.Unlock()

	hctx.commonConn = sc
	hctx.listenAddr = sc.listenAddr
	hctx.localAddr = sc.conn.conn.LocalAddr()
	hctx.remoteAddr = sc.conn.conn.RemoteAddr()
	hctx.keyID = sc.conn.keyID
	hctx.protocolVersion = sc.conn.ProtocolVersion()
	hctx.protocolTransport = "TCP"
	return hctx, true
}

func (sc *serverConn) releaseHandlerCtx(hctx *HandlerContext) {
	sc.releaseRequest(hctx)
	sc.releaseResponse(hctx)
	hctx.reset()

	sc.mu.Lock()
	if debugTrace {
		sc.server.addTrace(fmt.Sprintf("releaseHandlerCtx %p", hctx))
	}
	wakeupAcquireHandlerCtx := len(sc.hctxPool) == 0
	sc.hctxPool = append(sc.hctxPool, hctx)
	wasReadFINFlag := sc.readFINFlag
	wakeupWaitClosed := sc.canGracefullyShutdown()
	sc.mu.Unlock() // unlock without defer to try to reduce lock contention

	if wakeupAcquireHandlerCtx {
		sc.cond.Signal()
	}
	if wakeupWaitClosed {
		sc.closeWaitCond.Signal()
	}
	if wasReadFINFlag { // wake up writer for condition (sc.readFINFlag && sc.hctxCreated == len(sc.hctxPool)
		sc.writeQCond.Signal()
	}

	sc.server.statRequestsCurrent.Dec()
}

func (sc *serverConn) releaseRequest(hctx *HandlerContext) {
	sc.server.releaseRequestBuf(hctx.reqTaken, hctx.request)
	hctx.reqTaken = 0
	hctx.request = nil
	hctx.Request = nil
}

func (sc *serverConn) releaseResponse(hctx *HandlerContext) {
	if hctx.response != nil {
		// if Response was reallocated and became too big, we will reuse original slice we got from pool
		// otherwise, we will move reallocated slice into slice in heap
		if cap(hctx.Response) <= sc.server.opts.ResponseBufSize {
			*hctx.response = hctx.Response[:0]
		}
	}
	sc.server.releaseResponseBuf(hctx.respTaken, hctx.response)
	hctx.respTaken = 0
	hctx.response = nil
	hctx.Response = nil
}

func (sc *serverConn) makeLongpollResponse(hctx *HandlerContext, canceller HijackResponseCanceller) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	queryID := hctx.queryID

	if _, ok := sc.longpollResponses[queryID]; ok {
		sc.server.rareLog(&sc.server.lastHijackWarningLog, "[rpc.Server] - invariant violation, hijack response queryID collision")
		return
	}
	sc.server.statLongPollsWaiting.Inc()
	sc.longpollResponses[queryID] = hijackedResponse{canceller: canceller, hctx: hctx}
	if debugTrace {
		sc.server.addTrace(fmt.Sprintf("makeLongpollResponse %p %d", hctx, hctx.queryID))
	}
	if debugPrint {
		fmt.Printf("longpollResponses added %p %d\n", hctx, queryID)
	}
}

func (sc *serverConn) cancelLongpollResponse(queryID int64) {
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
	sc.server.statLongPollsWaiting.Dec()
	delete(sc.longpollResponses, queryID)
	if debugTrace {
		sc.server.addTrace(fmt.Sprintf("cancelLongpollResponse %p %d", resp.hctx, queryID))
	}
	sc.mu.Unlock()
	resp.canceller.CancelHijack(resp.hctx)
	if sc.server.opts.ResponseHook != nil {
		sc.server.opts.ResponseHook(resp.hctx, ErrCancelHijack)
	}
	sc.releaseHandlerCtx(resp.hctx)
}

func (sc *serverConn) cancelAllLongpollResponses() {
	sc.mu.Lock()
	longpollResponses := sc.longpollResponses
	sc.longpollResponses = nil // If makeLongpollResponse is called, we'll panic. But this must be impossible if SyncHandler follows protocol
	sc.server.statLongPollsWaiting.Sub(int64(len(longpollResponses)))
	if debugTrace {
		sc.server.addTrace("cancelAllLongpollResponses")
	}
	sc.mu.Unlock()
	for _, resp := range longpollResponses {
		if debugPrint {
			fmt.Printf("longpollResponses cancel (all) %d\n", resp.hctx.queryID)
		}
		if debugTrace {
			sc.server.addTrace(fmt.Sprintf("cancelAllLongpollResponse %d", resp.hctx.queryID))
		}
		resp.canceller.CancelHijack(resp.hctx)
		if sc.server.opts.ResponseHook != nil {
			sc.server.opts.ResponseHook(resp.hctx, ErrCancelHijack)
		}
		sc.releaseHandlerCtx(resp.hctx)
	}
}
