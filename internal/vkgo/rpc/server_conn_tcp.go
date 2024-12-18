// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"fmt"
	"net"
	"sync"

	"github.com/vkcom/statshouse/internal/vkgo/rpc/internal/gen/tl"
)

type serverConnTCP struct {
	serverConnCommon

	listenAddr net.Addr

	inFlight     int
	maxInflight  int
	inflightCond sync.Cond

	closeWaitCond sync.Cond

	conn         *PacketConn
	writeQ       []*HandlerContext // never contains noResult responses
	writeQCond   sync.Cond
	writeBuiltin bool
	writeLetsFin bool

	errHandler ErrHandlerFunc
}

var _ HandlerContextConnection = &serverConnTCP{}

func (sc *serverConnTCP) pushUnlock(hctx *HandlerContext) {
	wasLen := len(sc.writeQ)
	sc.writeQ = append(sc.writeQ, hctx)
	sc.mu.Unlock()
	if wasLen == 0 {
		sc.writeQCond.Signal()
	}
}

func (sc *serverConnTCP) sendLetsFin() {
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

func (sc *serverConnTCP) SetReadFIN() {
	sc.cancelAllLongpollResponses(true) // we always cancel from receiving goroutine.
	sc.writeQCond.Signal()
}

func (sc *serverConnTCP) SetWriteBuiltin() {
	sc.mu.Lock()
	sc.writeBuiltin = true
	sc.mu.Unlock()
	sc.writeQCond.Signal()
}

func (sc *serverConnTCP) close(cause error) {
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
	sc.inFlight -= len(writeQ)
	if debugTrace {
		sc.server.addTrace(fmt.Sprintf("Close %d", len(writeQ)))
	}
	sc.mu.Unlock()

	sc.cancelCloseCtx(cause)

	_ = sc.conn.Close()

	for _, hctx := range writeQ {
		sc.server.releaseHandlerCtx(hctx)
	}

	sc.inflightCond.Signal()
	sc.writeQCond.Signal()
}

func (sc *serverConnTCP) WaitClosed() error {
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
func (sc *serverConnTCP) canGracefullyShutdown() bool {
	return sc.inFlight == 0
}

func (sc *serverConnTCP) acquireHandlerCtx() (*HandlerContext, bool) {
	sc.mu.Lock()
	for !(sc.closedFlag || sc.inFlight < sc.maxInflight) {
		sc.server.rareLog(&sc.server.lastHctxWaitLog, "rpc: waiting to acquire handler context; consider increasing Server.MaxInflightPackets")
		sc.inflightCond.Wait()
	}
	if sc.closedFlag {
		sc.mu.Unlock()
		return nil, false
	}
	sc.inFlight++
	sc.mu.Unlock()

	hctx := sc.server.acquireHandlerCtx(protocolTCP)

	hctx.commonConn = sc
	hctx.listenAddr = sc.listenAddr
	hctx.localAddr = sc.conn.conn.LocalAddr()
	hctx.remoteAddr = sc.conn.conn.RemoteAddr()
	hctx.keyID = sc.conn.keyID
	hctx.protocolVersion = sc.conn.ProtocolVersion()
	return hctx, true
}

func (sc *serverConnTCP) releaseHandlerCtx(hctx *HandlerContext) {
	sc.server.releaseHandlerCtx(hctx)

	sc.mu.Lock()
	if debugTrace {
		sc.server.addTrace(fmt.Sprintf("releaseHandlerCtx %p", hctx))
	}
	wakeupAcquireHandlerCtx := sc.inFlight >= sc.maxInflight
	sc.inFlight--
	wasReadFINFlag := sc.readFINFlag
	wakeupWaitClosed := sc.canGracefullyShutdown()
	sc.mu.Unlock() // unlock without defer to try to reduce lock contention

	if wakeupAcquireHandlerCtx {
		sc.inflightCond.Signal()
	}
	if wakeupWaitClosed {
		sc.closeWaitCond.Signal()
	}
	if wasReadFINFlag { // wake up writer for condition (sc.readFINFlag && sc.hctxCreated == len(sc.hctxPool)
		sc.writeQCond.Signal()
	}
}
