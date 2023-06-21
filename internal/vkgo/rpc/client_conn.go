// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/vkcom/statshouse/internal/vkgo/basictl"
	"github.com/vkcom/statshouse/internal/vkgo/srvfunc"
)

type writeReq struct {
	// We have 2 request kinds in writeQ
	// 1. cctx != nil, req != nil, customPacketType == 0
	// 2. cctx == nil, req == nil, customPacketType != 0
	cctx             *callContext
	customPacketType uint32
	customPacketData int64
	req              *Request
	deadline         time.Time
}

type clientConn struct {
	client *Client

	address NetAddr

	writeQ     []writeReq
	writeQCond sync.Cond
	writeFin   bool

	mu                 sync.Mutex
	calls              map[int64]*callContext
	conn               *PacketConn
	waitingToReconnect bool

	closeCC chan<- struct{} // makes select case in goConnect always selectable. Set to nil after close to prevent double close
}

func (pc *clientConn) close() error {
	pc.dropClientConn(true)
	return nil
}

// if multiResult is used for many requests, it must contain enough space so that no receiver is blocked
func (pc *clientConn) setupCallLocked(req *Request, deadline time.Time, multiResult chan *callContext) (*callContext, error) {

	cctx := pc.getCallContext()
	cctx.queryID = req.QueryID()
	if multiResult != nil {
		cctx.result = multiResult // overrides single-result channel
	}
	cctx.failIfNoConnection = req.Extra.FailIfNoConnection

	if pc.closeCC == nil {
		putCallContext(cctx)
		return nil, ErrClientClosed
	}

	if req.Extra.FailIfNoConnection && pc.waitingToReconnect {
		putCallContext(cctx)
		return nil, ErrClientConnClosedNoSideEffect
	}
	if debugPrint {
		fmt.Printf("%v setupCallLocked for client %p pc %p\n", time.Now(), pc.client, pc)
	}

	pc.calls[cctx.queryID] = cctx

	pc.writeQ = append(pc.writeQ, writeReq{cctx: cctx, req: req, deadline: deadline})

	// Here cctx is owned by both writeQ and calls map

	pc.client.opts.Hooks.Request.BeforeSend(cctx.hooksState, req)

	return cctx, nil
}

func (pc *clientConn) cancelCall(cctx *callContext, unblockWaitersError error) {
	if pc.cancelCallImpl(cctx) {
		if unblockWaitersError != nil {
			cctx.err = unblockWaitersError
			cctx.result <- cctx // cctx owned by channel
		} else {
			putCallContext(cctx)
		}
	}
}

func (pc *clientConn) cancelCallImpl(cctx *callContext) (shouldRelease bool) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	queryID := cctx.queryID
	_, ok := pc.calls[queryID]
	if !ok {
		return false
	}
	delete(pc.calls, queryID)
	if !cctx.sent {
		cctx.stale = true // exclusive ownership of cctx by writeQ now, will be released
		return false
	}
	if pc.conn != nil {
		pc.writeQ = append(pc.writeQ, writeReq{customPacketType: packetTypeRPCCancelReq, customPacketData: queryID})
		pc.writeQCond.Signal() // Better move out of the lock, for now it is good enough, as cancels are rare
	}
	// exclusive ownership of cctx by this function
	return true
}

func (pc *clientConn) finishCall(queryID int64) *callContext {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	cctx, ok := pc.calls[queryID]
	if !ok {
		return nil
	}
	delete(pc.calls, queryID)
	return cctx
}

func (pc *clientConn) massCancelRequestsLocked() {
	pc.writeFin = false
	nQueued := 0
	now := time.Now()
	for _, wr := range pc.writeQ {
		if wr.customPacketType != 0 { // Must not send old ping pongs to new connection
			continue
		}
		// here wr.cctx != nil && wr.req != nil
		if wr.cctx.stale {
			pc.putStaleRequest(wr)
			continue
		}
		if wr.cctx.failIfNoConnection || (!wr.deadline.IsZero() && now.After(wr.deadline)) {
			wr.cctx.failIfNoConnection = true // so next loop below notices
			pc.client.putRequest(wr.req)
			continue
		}
		if pc.closeCC == nil { // && !wr.cctx.stale due to if above
			pc.client.putRequest(wr.req)
			continue
		}
		pc.writeQ[nQueued] = wr
		nQueued++
	}
	for i := nQueued; i < len(pc.writeQ); i++ {
		pc.writeQ[i] = writeReq{}
	}
	pc.writeQ = pc.writeQ[:nQueued]

	// unblock all possible calls
	for queryID, cctx := range pc.calls {
		if cctx.sent {
			cctx.err = ErrClientConnClosedSideEffect
		} else if cctx.failIfNoConnection || pc.closeCC == nil {
			cctx.err = ErrClientConnClosedNoSideEffect
		} else {
			continue
		}
		cctx.result <- cctx
		delete(pc.calls, queryID)
	}
}

func (pc *clientConn) continueRunning(previousGoodHandshake bool) bool {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	pc.massCancelRequestsLocked()

	if !previousGoodHandshake {
		pc.waitingToReconnect = true
	}
	return pc.closeCC != nil && len(pc.calls) != 0
}

func (pc *clientConn) putStaleRequest(wr writeReq) {
	putCallContext(wr.cctx)
	pc.client.putRequest(wr.req)
}

func (pc *clientConn) setClientConn(conn *PacketConn) bool {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if pc.closeCC == nil {
		return false
	}

	pc.conn = conn
	pc.waitingToReconnect = false

	return true
}

func (pc *clientConn) dropClientConn(stopConnecting bool) {
	pc.mu.Lock()
	if pc.closeCC == nil {
		pc.mu.Unlock()
		return
	}
	if stopConnecting {
		close(pc.closeCC)
		pc.closeCC = nil
	}
	conn := pc.conn
	pc.conn = nil
	pc.mu.Unlock()

	if conn != nil {
		_ = conn.Close()
		pc.writeQCond.Signal()
	}
}

func (pc *clientConn) goConnect(closeCC <-chan struct{}) {
	defer pc.client.wg.Done()
	reconnectTimer := time.NewTimer(maxReconnectDelay)
	reconnectTimer.Stop()
	defer reconnectTimer.Stop()

	var reconnectDelay time.Duration
	for {
		if reconnectDelay > 0 {
			reconnectTimer.Reset(reconnectDelay)
			select {
			case <-reconnectTimer.C:
				break
			case <-closeCC: // wait on copy of nc.closeCC
				break
			}
		}
		pc.mu.Lock()
		if pc.closeCC == nil {
			if debugPrint {
				fmt.Printf("%v closeCC == nil for pc %p", time.Now(), pc)
			}
			pc.massCancelRequestsLocked()
			pc.mu.Unlock()
			break
		}
		pc.mu.Unlock()
		goodHandshake := pc.run()
		if !pc.continueRunning(goodHandshake) {
			if pc.client.removeConnection(pc) {
				if debugPrint {
					fmt.Printf("%v !continueRunning for pc %p\n", time.Now(), pc)
				}
				return
			}
			// otherwise calls were added exactly while we were taking locks for connection
		}
		// waitingToReconnect is true here if !goodHandshake
		switch {
		case goodHandshake:
			reconnectDelay = 0
		case reconnectDelay < minReconnectDelay:
			reconnectDelay = minReconnectDelay
		default:
			reconnectDelay *= 2
			if reconnectDelay > maxReconnectDelay {
				reconnectDelay = maxReconnectDelay
			}
		}
		pc.client.opts.Logf("rpc: reconnecting to %v in %v", pc.address, reconnectDelay)
	}
}

func (pc *clientConn) run() (goodHandshake bool) {
	address := srvfunc.MaybeResolveHost(pc.address.Network, pc.address.Address)
	nc, err := net.DialTimeout(pc.address.Network, address, DefaultHandshakeStepTimeout)
	if err != nil {
		pc.client.opts.Logf("rpc: failed to start new peer connection with %v: %v", pc.address, err)
		return false
	}

	conn := NewPacketConn(nc, pc.client.opts.ConnReadBufSize, pc.client.opts.ConnWriteBufSize, DefaultConnTimeoutAccuracy)

	err = conn.HandshakeClient(pc.client.opts.CryptoKey, pc.client.opts.TrustedSubnetGroups, pc.client.opts.ForceEncryption, uniqueStartTime(), 0, DefaultHandshakeStepTimeout)
	if err != nil {
		_ = conn.Close()
		pc.client.opts.Logf("rpc: failed to establish new peer connection with %v: %v", pc.address, err)
		return false
	}

	if !pc.setClientConn(conn) {
		_ = conn.Close()
		return false
	}

	var wg sync.WaitGroup
	pongCC := make(chan int64, 1) // must have at least 1 buffer space

	wg.Add(2)
	go pc.pingLoop(conn, &wg, pongCC, pc.client.opts.PongTimeout)
	go func() {
		defer wg.Done()
		if sendErr := pc.sendLoop(conn); sendErr != nil {
			pc.dropClientConn(false) // only in case of error, not when sent FIN or responded to close
		}
	}()
	pc.receiveLoop(conn, pongCC)
	close(pongCC) // reader can no more send to pongCC, so safe close will trigger pinging goroutine exit
	wg.Wait()

	return true
}

func (pc *clientConn) pingLoop(conn *PacketConn, wg *sync.WaitGroup, pongCC <-chan int64, pongTimeout time.Duration) {
	defer wg.Done()
	defer pc.dropClientConn(false)
	pingTimer := time.NewTimer(clientPingInterval)
	defer pingTimer.Stop()

	pongTimeoutTimer := time.NewTimer(pongTimeout)
	pongTimeoutTimer.Stop()
	defer pongTimeoutTimer.Stop()

	pingID := int64(0)
	for {
		select {
		case <-pingTimer.C:
			pingID++
			pc.writeQPush(writeReq{customPacketType: PacketTypeRPCPing, customPacketData: pingID})
			pongTimeoutTimer.Reset(pongTimeout)
		case pongID, ok := <-pongCC:
			if !ok {
				return
			}
			if pongID != pingID {
				pc.client.opts.Logf("rpc: got pong(%v) in response to ping(%v) from %v, disconnecting", pongID, pingID, conn.remoteAddr)
				return
			}
			if !pongTimeoutTimer.Stop() {
				<-pongTimeoutTimer.C
			}
			pingTimer.Reset(clientPingInterval)
		case <-pongTimeoutTimer.C:
			pc.client.opts.Logf("rpc: did not receive pong from %v in %v, disconnecting", conn.remoteAddr, pongTimeout)
			return
		}
	}
}

func (pc *clientConn) sendLoop(conn *PacketConn) error {
	if debugPrint {
		fmt.Printf("%v sendLoop %p conn %p start\n", time.Now(), pc.client, pc)
	}
	var buf []writeReq
	writeFin := false
	for {
		writeFin, buf = pc.writeQAcquire(buf[:0])
		if !writeFin && len(buf) == 0 {
			return nil
		}

		for _, wr := range buf {
			if wr.customPacketType != 0 {
				err := writeCustomPacketUnlocked(conn, wr.customPacketType, wr.customPacketData, DefaultClientPongTimeout)
				if err != nil {
					if !commonConnCloseError(err) {
						pc.client.opts.Logf("rpc: failed to send ping/pong to %v, disconnecting: %v", conn.remoteAddr, err)
					}
					return err
				}
			} else { // here wr.cctx != nil && wr.req != nil
				err := writeRequestUnlocked(conn, wr.req, wr.deadline, maxPacketRWTime)
				pc.client.putRequest(wr.req)
				if err != nil {
					if !commonConnCloseError(err) {
						pc.client.opts.Logf("rpc: failed to send packet to %v, disconnecting: %v", conn.remoteAddr, err)
					}
					return err
				}
			}
		}

		if writeFin || pc.writeQFlush() { // before writeFIN we must flush regardless of policy set by writeQFlush
			if err := conn.writeFlushUnlocked(); err != nil {
				if !commonConnCloseError(err) {
					pc.client.opts.Logf("rpc: failed to flush packets to %v, disconnecting: %v", conn.remoteAddr, err)
				}
				return err
			}
		}
		if writeFin {
			if debugPrint {
				fmt.Printf("%v client %p conn %p writes FIN\n", time.Now(), pc.client, pc)
			}
			if err := conn.ShutdownWrite(); err != nil {
				if !commonConnCloseError(err) {
					pc.client.opts.Logf("rpc: failed to flush packets to %v, disconnecting: %v", conn.remoteAddr, err)
				}
				return err
			}
			return nil
		}
	}
}

func (pc *clientConn) receiveLoop(conn *PacketConn, pong chan<- int64) {
	defer pc.dropClientConn(false)

	for {
		pkt := pc.client.getResponse() // we use Response as a scratch space for incoming (even non-response) packet here
		var typ uint32
		var err error
		typ, pkt.Body, err = conn.ReadPacket(pkt.body, maxIdleDuration+maxPacketRWTime)
		pkt.body = pkt.Body[:0] // prepare for reuse immediately
		if err != nil {
			pc.client.PutResponse(pkt)
			if !commonConnCloseError(err) {
				pc.client.opts.Logf("rpc: error reading packet from %v, disconnecting: %v", conn.remoteAddr, err)
			}
			return
		}
		pkt.responseType = typ
		put, err := pc.handlePacket(pkt, pong)
		if put {
			pc.client.PutResponse(pkt)
		}
		if err != nil {
			pc.client.opts.Logf("rpc: failed to handle packet from %v, disconnecting: %v", conn.remoteAddr, err)
			return
		}
	}
}

func (pc *clientConn) handlePacket(pkt *Response, pongCC chan<- int64) (put bool, _ error) {
	switch pkt.responseType {
	case PacketTypeRPCPing:
		var pongID int64
		req, err := basictl.LongRead(pkt.Body, &pongID)
		if err != nil {
			return true, fmt.Errorf("error reading ping: %w", err)
		}
		if len(req) != 0 {
			return true, fmt.Errorf("excess %d bytes in ping packet", len(req))
		}
		pc.writeQPush(writeReq{customPacketType: PacketTypeRPCPong, customPacketData: pongID})
		return true, nil
	case PacketTypeRPCPong:
		var pongID int64
		req, err := basictl.LongRead(pkt.Body, &pongID)
		if err != nil {
			return true, fmt.Errorf("error reading pong: %w", err)
		}
		if len(req) != 0 {
			return true, fmt.Errorf("excess %d bytes in pong packet", len(req))
		}
		pongCC <- pongID
		return true, nil
	case packetTypeRPCServerWantsFin:
		if debugPrint {
			fmt.Printf("%v client %p conn %p read Let's FIN\n", time.Now(), pc.client, pc)
		}
		pc.mu.Lock()
		pc.writeFin = true
		pc.mu.Unlock()
		pc.writeQCond.Signal()
		// goWrite will take last bunch of requests plus fin from writeQ, write them and quit.
		// all requests put into writeQ after writer quits will wait there until the next reconnect
		return true, nil
	case packetTypeRPCReqResult, packetTypeRPCReqError:
		var queryID int64
		var err error
		pkt.Body, err = basictl.LongRead(pkt.Body, &queryID)
		if err != nil {
			return true, fmt.Errorf("failed to read response query ID: %w", err)
		}
		return pc.handleResponse(queryID, pkt, pkt.responseType == packetTypeRPCReqError), nil
	default:
		return true, fmt.Errorf("unexpected packet type 0x%x", pkt.responseType)
	}
}

func (pc *clientConn) handleResponse(queryID int64, resp *Response, toplevelError bool) (put bool) {
	cctx := pc.finishCall(queryID)
	if cctx == nil {
		// we expect that cctx can be nil because of teardownCall after context was done (and not because server decided to send garbage)
		return true // already cancelled or served
	}

	cctx.err = cctx.parseResponse(resp, toplevelError, pc.client.opts.Logf)

	put = cctx.resp == nil // we do not own cctx after <-

	pc.client.opts.Hooks.Request.AfterReceive(cctx.hooksState, cctx.resp, cctx.err)

	cctx.result <- cctx
	return
}

func writeCustomPacketUnlocked(conn *PacketConn, packetType uint32, pingPong int64, writeTimeout time.Duration) error {
	if err := conn.startWritePacketUnlocked(packetType, writeTimeout); err != nil {
		return err
	}
	conn.headerWriteBuf = basictl.LongWrite(conn.headerWriteBuf, pingPong)
	crc, err := conn.writePacketHeaderUnlocked(0)
	if err != nil {
		return err
	}
	// body is empty
	return conn.writePacketTrailerUnlocked(crc, 0)
}

func writeRequestUnlocked(conn *PacketConn, req *Request, deadline time.Time, writeTimeout time.Duration) (err error) {
	if !deadline.IsZero() && !req.Extra.IsSetCustomTimeoutMs() {
		// As close to actual writing as possible, due to time.Until
		// If negative already, so be it.
		req.Extra.SetCustomTimeoutMs(int32(time.Until(deadline).Milliseconds()))
	}

	if err = conn.startWritePacketUnlocked(packetTypeRPCInvokeReq, writeTimeout); err != nil {
		return err
	}
	headerBuf := basictl.LongWrite(conn.headerWriteBuf, req.queryID) // move to local var, then back for speed
	switch {
	case req.ActorID != 0 && req.Extra.Flags != 0:
		headerBuf = basictl.NatWrite(headerBuf, destActorFlagsTag)
		headerBuf = basictl.LongWrite(headerBuf, int64(req.ActorID))
		if headerBuf, err = req.Extra.Write(headerBuf); err != nil {
			return fmt.Errorf("failed to write extra: %w", err)
		}
	case req.Extra.Flags != 0:
		headerBuf = basictl.NatWrite(headerBuf, destFlagsTag)
		if headerBuf, err = req.Extra.Write(headerBuf); err != nil {
			return fmt.Errorf("failed to write extra: %w", err)
		}
	case req.ActorID != 0:
		headerBuf = basictl.NatWrite(headerBuf, destActorTag)
		headerBuf = basictl.LongWrite(headerBuf, int64(req.ActorID))
	}
	conn.headerWriteBuf = headerBuf
	return conn.writeSimplePacketUnlocked(req.Body)
}

func (pc *clientConn) writeQPush(wr writeReq) {
	pc.mu.Lock()
	pc.writeQ = append(pc.writeQ, wr)
	pc.mu.Unlock()

	pc.writeQCond.Signal() // signal without holding the mutex to reduce contention
}

func (pc *clientConn) writeQAcquire(q []writeReq) (writeFin bool, _ []writeReq) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	for !(pc.conn == nil || len(pc.writeQ) != 0 || pc.writeFin) {
		pc.writeQCond.Wait()
	}

	if pc.conn == nil {
		return false, nil
	}

	ret := q
	for i, wr := range pc.writeQ {
		if wr.cctx != nil {
			if wr.cctx.stale {
				pc.putStaleRequest(wr) // avoid sending stale requests
				pc.writeQ[i] = writeReq{}
				continue
			}
			wr.cctx.sent = true // point of potential side effect
		}
		pc.writeQ[i] = writeReq{}
		ret = append(ret, wr)
	}
	pc.writeQ = pc.writeQ[:0]

	return pc.writeFin, ret // We do not clear writeFin, because it must be cleared on reconnect anyway
}

func (pc *clientConn) writeQFlush() bool {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	return pc.conn == nil || len(pc.writeQ) == 0
}
