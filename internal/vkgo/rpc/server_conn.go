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

	"github.com/vkcom/statshouse/internal/vkgo/rpc/internal/gen/tl"
)

type serverConn struct {
	closeCtx       context.Context
	cancelCloseCtx context.CancelFunc

	server      *Server
	listenAddr  net.Addr
	maxInflight int

	mu   sync.Mutex
	cond sync.Cond

	closeWaitCond sync.Cond

	longpollResponses map[int64]hijackedResponse

	userData any // single common instance for handlers called from receiveLoopImpl
	// We swap userData back here from contexts, because often sync handler and normal handlers set different
	// parts of user data. If we disallow mixing them, they will use less memory

	hctxPool     []*HandlerContext // TODO - move to Server?
	hctxCreated  int
	readFINFlag  bool // reader quit, writer continues until hctxCreated == len(hctxPool) and empty queue
	closedFlag   bool
	conn         *PacketConn
	writeQ       []*HandlerContext // never contains noResult responses
	writeQCond   sync.Cond
	writeBuiltin bool
	writeLetsFin bool
}

type hijackedResponse struct {
	canceller HijackResponseCanceller
	hctx      *HandlerContext
}

// Motivation - we want zero allocations, so we cannot use lambda
type HijackResponseCanceller interface {
	CancelHijack(hctx *HandlerContext)
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
		delete(sc.longpollResponses, resp.hctx.queryID)
	}
	if sc.closedFlag || hctx.noResult {
		if debugTrace {
			sc.server.addTrace(fmt.Sprintf("push (closedFlag | noResult) %p", hctx))
		}
		sc.mu.Unlock()
		hctx.serverConn.releaseHandlerCtx(hctx)
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
		return // TODO - close connection here?
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

func (sc *serverConn) flush() error {
	err := sc.conn.FlushUnlocked()
	if err != nil {
		if !sc.closed() && !commonConnCloseError(err) {
			sc.server.rareLog(&sc.server.lastOtherLog, "rpc: error flushing packet to %v, disconnecting: %v", sc.conn.remoteAddr, err)
		}
	}
	return err
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

func (sc *serverConn) Close() error {
	sc.mu.Lock()
	if sc.closedFlag {
		sc.mu.Unlock()
		return nil
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

	sc.cancelCloseCtx()

	_ = sc.conn.Close()

	for _, hctx := range writeQ {
		hctx.releaseRequest()
		hctx.releaseResponse()
	}

	sc.cond.Broadcast()    // wake up everyone who waits on !sc.closedFlag
	sc.writeQCond.Signal() // or writeQ
	return nil
}

func (sc *serverConn) WaitClosed() error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	for sc.hctxCreated != len(sc.hctxPool) {
		sc.closeWaitCond.Wait()
	}
	if len(sc.writeQ) != 0 {
		sc.server.opts.Logf("rpc: connection write queue length (%d) invariant violated", len(sc.writeQ))
	}
	return nil
}

func (sc *serverConn) closed() bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.closedFlag
}

func writeResponseUnlocked(conn *PacketConn, hctx *HandlerContext) error {
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

func (sc *serverConn) acquireHandlerCtx(tip uint32, stateInit func() ServerHookState) (*HandlerContext, bool) {
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

	hctx.serverConn = sc
	hctx.listenAddr = sc.listenAddr
	hctx.localAddr = sc.conn.conn.LocalAddr()
	hctx.remoteAddr = sc.conn.conn.RemoteAddr()
	hctx.keyID = sc.conn.keyID
	hctx.protocolVersion = sc.conn.ProtocolVersion()
	hctx.hooksState = stateInit()

	return hctx, true
}

func (sc *serverConn) releaseHandlerCtx(hctx *HandlerContext) {
	hctx.releaseRequest()
	hctx.releaseResponse()
	hctx.reset()

	sc.mu.Lock()
	if debugTrace {
		sc.server.addTrace(fmt.Sprintf("releaseHandlerCtx %p", hctx))
	}
	wakeupAcquireHandlerCtx := len(sc.hctxPool) == 0
	sc.hctxPool = append(sc.hctxPool, hctx)
	wasReadFINFlag := sc.readFINFlag
	wakeupWaitClosed := sc.hctxCreated == len(sc.hctxPool)
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

func (sc *serverConn) makeLongpollResponse(hctx *HandlerContext, canceller HijackResponseCanceller) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	queryID := hctx.queryID

	if _, ok := sc.longpollResponses[queryID]; ok {
		sc.server.rareLog(&sc.server.lastHijackWarningLog, "[rpc.Server] - invariant violation, hijack response queryID collision")
		return
	}
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
	delete(sc.longpollResponses, queryID)
	if debugTrace {
		sc.server.addTrace(fmt.Sprintf("cancelLongpollResponse %p %d", resp.hctx, queryID))
	}
	sc.mu.Unlock()
	resp.canceller.CancelHijack(resp.hctx)
	resp.hctx.serverConn.releaseHandlerCtx(resp.hctx)
}

func (sc *serverConn) cancelAllLongpollResponses() {
	sc.mu.Lock()
	longpollResponses := sc.longpollResponses
	sc.longpollResponses = nil // If makeLongpollResponse is called, we'll panic. But this must be impossible if SyncHandler follows protocol
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
		resp.hctx.serverConn.releaseHandlerCtx(resp.hctx)
	}
}
