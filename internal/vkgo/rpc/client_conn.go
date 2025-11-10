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
	"time"

	"pgregory.net/rand"

	"github.com/VKCOM/statshouse/internal/vkgo/rpc/internal/gen/tl"
	"github.com/VKCOM/statshouse/internal/vkgo/srvfunc"
)

type writeRespCancel struct {
	// We must have 2 request kinds ordered, that's why we need a queue for them
	// 1. cctx != nil, not a cancel
	// 2. cctx == nil, cancel cancelQueryID
	cctx          *Response
	cancelQueryID int64
}

type writeReqCancel struct {
	// We must have 2 request kinds ordered, that's why we need a queue for them
	// 1. req != nil, not a cancel
	// 2. req == nil, cancel cancelQueryID
	req           *Request
	cancelQueryID int64
}

type clientConn struct {
	client *ClientImpl

	address NetAddr

	mu         sync.Mutex
	writeQCond sync.Cond
	// initially cctx is added both to calls and writeQ.
	// when cctx is taken from writeQ, cctx.req is set to nil, indicating that request is going to be sent/
	// request is then sent and freed.
	// if cctx is cancelled, and cctx.req is nil, then request is not in writeQ and can be deleted from calls,
	// but if cctx.req is not nil, cctx is somewhere in writeQ and we, instead of searching for it with O(n),
	// set cctx.stale = true. If writeQ encounters such stale cctx, it frees it.
	// So, ownership rules table is:
	// if cctx.req == nil: cctx is owned by calls
	// else if cctx.stale: cctx is owned by writeQ
	// otherwise, cctx is shared by calls and writeQ
	calls               map[int64]*Response
	writeQ              []writeRespCancel // writeRespCancel.cctx.req is to be sent
	inFlight            int               // # of not cancelled calls in flight. During shutdown, wait for this to become 0 before closing connection to server.
	isShutdown          bool
	writeClientWantsFin bool
	writeBuiltin        bool

	conn               *PacketConn
	waitingToReconnect bool

	closeCC chan<- struct{} // closeCC == nil if client.close() was called.

	resetReconnectDelayC chan<- struct{}
}

func (pc *clientConn) close() {
	// first, prevent reconnect by setting closeCC to nil
	pc.mu.Lock()
	if pc.closeCC != nil {
		close(pc.closeCC)
		pc.closeCC = nil
	}
	pc.mu.Unlock()
	pc.writeQCond.Signal() // pc.closeCC is a flag
	pc.dropClientConn()
}

func (pc *clientConn) shutdown() {
	pc.mu.Lock()
	if pc.conn == nil || pc.isShutdown { // already closing or in shutdown
		pc.mu.Unlock()
		return
	}
	pc.isShutdown = true
	if pc.inFlight != 0 {
		if debugPrint {
			fmt.Printf("%v client %p conn %p shutdown inFlight(%d) != 0\n", time.Now(), pc.client, pc, pc.inFlight)
		}
		pc.writeClientWantsFin = true
		pc.mu.Unlock()
		pc.writeQCond.Signal()
		return
	}
	if debugPrint {
		fmt.Printf("%v client %p conn %p shutdown inFlight == 0\n", time.Now(), pc.client, pc)
	}
	conn := pc.conn
	pc.conn = nil
	pc.mu.Unlock()
	pc.writeQCond.Signal()
	_ = conn.Close() // not under lock
}

// if multiResult is used for many requests, it must contain enough space so that no receiver is blocked
func (pc *clientConn) setupCallLocked(req *Request, deadline time.Time, multiResult chan *Response, cb ClientCallback, userData any) (*Response, error) {
	cctx := pc.client.getResponse()
	cctx.queryID = req.QueryID()
	if multiResult != nil {
		cctx.result = multiResult // overrides single-result channel
	}
	cctx.cb = cb
	cctx.req = req
	cctx.deadline = deadline
	cctx.userData = userData
	cctx.failIfNoConnection = req.FailIfNoConnection
	cctx.bodyFormatTL2 = req.BodyFormatTL2
	cctx.readonly = req.ReadOnly
	cctx.hookState, req.hookState = req.hookState, cctx.hookState // transfer ownership of "dirty" hook state to cctx

	if pc.closeCC == nil {
		return cctx, ErrClientClosed
	}

	if req.FailIfNoConnection && pc.waitingToReconnect {
		return cctx, ErrClientConnClosedNoSideEffect
	}
	// Prints usually too much info for analysis. Uncomment for specific case debugging.
	// if debugPrint {
	//	fmt.Printf("%v setupCallLocked for client %p pc %p\n", time.Now(), pc.client, pc)
	// }

	pc.calls[cctx.queryID] = cctx

	pc.writeQ = append(pc.writeQ, writeRespCancel{cctx: cctx})

	// Here cctx is owned by both writeQ and calls map

	return cctx, nil
}

func (pc *clientConn) cancelCall(queryID int64, deliverError error) (cancelled bool) {
	cctx, closeNow := pc.cancelCallImpl(queryID)
	if closeNow != nil { // not under lock
		_ = closeNow.Close()
	}
	if cctx != nil {
		// exclusive ownership of cctx by this function
		if deliverError != nil {
			cctx.err = deliverError
			cctx.deliverResult(pc.client)
		} else {
			pc.client.PutResponse(cctx)
		}
	}
	return cctx != nil
}

func (pc *clientConn) cancelCallImpl(queryID int64) (shouldReleaseCctx *Response, closeNow *PacketConn) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	cctx, ok := pc.calls[queryID]
	if !ok {
		return nil, nil
	}
	delete(pc.calls, queryID)
	if cctx.req != nil { // was not sent, residing somewhere in writeQ
		cctx.stale = true // exclusive ownership of cctx by writeQ now, will be released
		return nil, nil
	}
	// was sent
	pc.inFlight--
	if pc.inFlight < 0 {
		panic("rpc.Client invariant violation: pc.inFlight < 0")
	}
	if pc.conn == nil {
		return cctx, nil
	}
	if pc.isShutdown && pc.inFlight == 0 {
		if debugPrint {
			fmt.Printf("%v client %p conn %p cancelCall inFlight == 0\n", time.Now(), pc.client, pc)
		}
		conn := pc.conn
		pc.conn = nil
		pc.writeQCond.Signal()
		return cctx, conn
	}
	if pc.conn.FlagCancelReq() {
		pc.writeQ = append(pc.writeQ, writeRespCancel{cancelQueryID: queryID})
		pc.writeQCond.Signal()
	}
	return cctx, nil
}

func (pc *clientConn) finishCall(queryID int64) (_ *Response, closeNow *PacketConn) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	cctx, ok := pc.calls[queryID]
	if !ok {
		return nil, nil
	}
	if cctx.req != nil { // was not sent, residing somewhere in writeQ
		// In finish call, this is possible if server sends response with a queryID before request was sent by client.
		// We should treat it as a NOP
		return nil, nil
	}
	// was sent
	delete(pc.calls, queryID)
	pc.inFlight--
	if pc.inFlight < 0 {
		panic("rpc.Client invariant violation: pc.inFlight < 0")
	}
	if pc.conn == nil {
		return cctx, nil
	}
	if pc.isShutdown && pc.inFlight == 0 {
		if debugPrint {
			fmt.Printf("%v client %p conn %p finishCall inFlight == 0\n", time.Now(), pc.client, pc)
		}
		conn := pc.conn
		pc.conn = nil
		pc.writeQCond.Signal()
		return cctx, conn
	}
	return cctx, nil
}

func (pc *clientConn) massCancelRequestsLocked() (finishedCalls []*Response) {
	pc.writeBuiltin = false
	pc.isShutdown = false
	pc.writeClientWantsFin = false
	now := time.Now()
	for i, wr := range pc.writeQ {
		if wr.cctx != nil && wr.cctx.stale { // not in calls, so we are owner
			pc.client.PutResponse(wr.cctx)
		}
		pc.writeQ[i] = writeRespCancel{} // free memory
	}
	pc.writeQ = pc.writeQ[:0]

	// unblock all possible calls
	for queryID, cctx := range pc.calls {
		if cctx.req == nil { // was sent
			pc.inFlight--
			if pc.inFlight < 0 {
				panic("rpc.Client invariant violation: pc.inFlight < 0")
			}
			cctx.err = ErrClientConnClosedSideEffect
		} else if cctx.failIfNoConnection || pc.closeCC == nil {
			cctx.err = ErrClientConnClosedNoSideEffect
		} else if !cctx.deadline.IsZero() && now.After(cctx.deadline) {
			cctx.err = context.DeadlineExceeded
		} else {
			pc.writeQ = append(pc.writeQ, writeRespCancel{cctx: cctx})
			continue
		}
		delete(pc.calls, queryID)
		if cctx.cb == nil { // code moved from deliverResult to avoid allocation for common case of not using callbacks
			cctx.result <- cctx
		} else {
			// we do some allocations here, but after disconnect we allocate anyway
			// we cannot call callbacks under any lock, otherwise deadlock
			finishedCalls = append(finishedCalls, cctx)
		}
	}
	return
}

func (pc *clientConn) continueRunning(previousGoodHandshake bool) bool {
	finishedCalls, result := pc.continueRunningImpl(previousGoodHandshake)
	for _, cctx := range finishedCalls {
		cctx.deliverResult(pc.client)
	}
	return result
}

func (pc *clientConn) continueRunningImpl(previousGoodHandshake bool) (finishedCalls []*Response, _ bool) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	finishedCalls = pc.massCancelRequestsLocked()

	if !previousGoodHandshake {
		pc.waitingToReconnect = true
	}
	return finishedCalls, pc.closeCC != nil && len(pc.calls) != 0
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

func (pc *clientConn) dropClientConn() {
	pc.mu.Lock()
	conn := pc.conn
	pc.conn = nil
	pc.mu.Unlock()

	if conn != nil {
		_ = conn.Close()
		pc.writeQCond.Signal()
	}
}

func (pc *clientConn) goConnect(closeCC <-chan struct{}, resetReconnectDelayC <-chan struct{}) {
	defer pc.client.wg.Release(1)
	reconnectTimer := time.NewTimer(0)
	if !reconnectTimer.Stop() {
		<-reconnectTimer.C
	}
	defer reconnectTimer.Stop()

	var reconnectDelay time.Duration
	for {
		if reconnectDelay > 0 {
			// "Full jitter" from https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
			reconnectDelayJitter := time.Duration(rand.Uint64n(uint64(reconnectDelay)))
			pc.client.opts.Logf("rpc: reconnecting to %v in %v", pc.address, reconnectDelayJitter)
			reconnectTimer.Reset(reconnectDelayJitter)
			select {
			case <-reconnectTimer.C:
				break
			case <-resetReconnectDelayC:
				if !reconnectTimer.Stop() {
					<-reconnectTimer.C
				}
				reconnectDelay = minReconnectDelay
				continue
			case <-closeCC: // wait on copy of nc.closeCC
				break
			}
		}
		pc.mu.Lock()
		if pc.closeCC == nil {
			if debugPrint {
				fmt.Printf("%v closeCC == nil for pc %p", time.Now(), pc)
			}
			finishedCalls := pc.massCancelRequestsLocked()
			pc.mu.Unlock()
			for _, cctx := range finishedCalls {
				cctx.deliverResult(pc.client)
			}
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
		case reconnectDelay == 0:
			reconnectDelay = minReconnectDelay
		default:
			reconnectDelay *= 2
			if reconnectDelay > pc.client.opts.MaxReconnectDelay {
				reconnectDelay = pc.client.opts.MaxReconnectDelay
			}
		}
	}
}

func (pc *clientConn) run() (goodHandshake bool) {
	address := srvfunc.MaybeResolveAddr(pc.address.Network, pc.address.Address)
	nc, err := net.DialTimeout(pc.address.Network, address, pc.client.opts.PacketTimeout)
	if err != nil {
		pc.client.opts.Logf("rpc: failed to start new peer connection with %v: %v", pc.address, err)
		return false
	}

	conn := NewPacketConn(nc, pc.client.opts.ConnReadBufSize, pc.client.opts.ConnWriteBufSize)

	err = conn.HandshakeClient(pc.client.opts.CryptoKey, pc.client.opts.TrustedSubnetGroups, pc.client.opts.ForceEncryption, uniqueStartTime(), 0, pc.client.opts.PacketTimeout, pc.client.opts.ProtocolVersion)
	if err != nil {
		_ = conn.Close()
		pc.client.opts.Logf("rpc: failed to establish new peer connection with %v: %v", pc.address, err)
		return false
	}

	if !pc.setClientConn(conn) {
		_ = conn.Close()
		return false
	}

	wg := make(chan error)

	go func() {
		sendErr := pc.sendLoop(conn)
		pc.dropClientConn()
		wg <- sendErr
	}()
	pc.receiveLoop(conn)
	pc.dropClientConn()
	<-wg

	return true
}

func (pc *clientConn) sendLoop(conn *PacketConn) error {
	if debugPrint {
		fmt.Printf("%v sendLoop %p conn %p start\n", time.Now(), pc.client, pc)
	}
	var writeQ []writeReqCancel
	customBody := make([]byte, 16) // most likely will allocate on stack
	sent := false                  // true if there is data to flush
	pc.mu.Lock()
	for {
		if pc.conn == nil { // should stop
			pc.mu.Unlock()
			return nil
		}
		unlockCondition := sent || pc.writeClientWantsFin || pc.writeBuiltin
		isShutdown := pc.isShutdown
		if !isShutdown { // otherwise we will spin in this loop forever, as we do not clear pc.writeQ during shutdown
			unlockCondition = unlockCondition || len(pc.writeQ) != 0
		}
		if !unlockCondition {
			pc.writeQCond.Wait()
			continue
		}
		writeClientWantsFin := pc.writeClientWantsFin
		pc.writeClientWantsFin = false
		writeBuiltin := pc.writeBuiltin
		pc.writeBuiltin = false
		writeQ = writeQ[:0]
		if !isShutdown { // do not send requests or cancels if we are in shut down
			writeQ = pc.moveRequestsToSendLocked(writeQ)
		}
		pc.mu.Unlock()
		sentNow := false
		if writeBuiltin {
			sentNow = true
			err := conn.WritePacketBuiltinNoFlushUnlocked(pc.client.opts.PacketTimeout)
			if err != nil {
				if !isShutdown && !commonConnCloseError(err) {
					pc.client.opts.Logf("rpc: failed to send ping/pong to %s, disconnecting: %v", conn.RemoteAddr(), err)
				}
				return err
			}
		}
		for _, wr := range writeQ {
			sentNow = true
			if wr.req != nil {
				err := writeRequestUnlocked(conn, wr.req, pc.client.opts.PacketTimeout)
				pc.client.putRequest(wr.req)
				if err != nil {
					if !isShutdown && !commonConnCloseError(err) {
						pc.client.opts.Logf("rpc: failed to send packet to %s, disconnecting: %v", conn.RemoteAddr(), err)
					}
					return err
				}
			} else {
				ret := tl.RpcCancelReq{QueryId: wr.cancelQueryID}
				customBody = ret.Write(customBody[:0])
				err := conn.WritePacketNoFlushUnlocked(tl.RpcCancelReq{}.TLTag(), customBody, pc.client.opts.PacketTimeout)
				if err != nil {
					if !isShutdown && !commonConnCloseError(err) {
						pc.client.opts.Logf("rpc: failed to send custom packet to %s, disconnecting: %v", conn.RemoteAddr(), err)
					}
					return err
				}
			}
		}
		if writeClientWantsFin {
			sentNow = true
			if debugPrint {
				fmt.Printf("%v client %p conn %p writes FIN\n", time.Now(), pc.client, pc)
			}
			err := conn.WritePacketNoFlushUnlocked(tl.RpcClientWantsFin{}.TLTag(), nil, pc.client.opts.PacketTimeout)
			if err != nil {
				if !isShutdown && !commonConnCloseError(err) {
					pc.client.opts.Logf("rpc: failed to send custom packet to %s, disconnecting: %v", conn.RemoteAddr(), err)
				}
				return err
			}
		}
		if sent && !sentNow {
			if err := conn.FlushUnlocked(); err != nil {
				if !isShutdown && !commonConnCloseError(err) {
					pc.client.opts.Logf("rpc: failed to flush packets to %s, disconnecting: %v", conn.RemoteAddr(), err)
				}
				return err
			}
		}
		sent = sentNow
		pc.mu.Lock()
	}
}

func (pc *clientConn) receiveLoop(conn *PacketConn) {
	respReuseData := pc.client.getResponseData()
	defer func() { pc.client.putResponseData(respReuseData) }()
	for {
		respBody := *respReuseData
		var responseType uint32

		for {
			var isBuiltin bool
			var err error
			responseType, respBody, isBuiltin, _, err = conn.ReadPacketUnlocked(respBody, pc.client.opts.PacketTimeout)
			if len(respBody) < maxGoAllocSizeClass { // Prepare for reuse immediately
				*respReuseData = respBody[:0]
			}
			if err != nil {
				if !commonConnCloseError(err) {
					pc.client.opts.Logf("rpc: error reading packet from %s, disconnecting: %v", conn.RemoteAddr(), err)
				}
				return
			}
			if !isBuiltin {
				break
			}
			pc.mu.Lock()
			pc.writeBuiltin = true
			pc.mu.Unlock()
			pc.writeQCond.Signal()
		}
		resp, err := pc.handlePacket(responseType, respReuseData, respBody)
		if err != nil { // resp is always nil here
			pc.client.opts.Logf("rpc: failed to handle packet from %s, disconnecting: %v", conn.RemoteAddr(), err)
			return
		}
		if resp != nil {
			// resp now owns respReuseData, we need to break alias before callback for panic safety
			respReuseData = pc.client.getResponseData()
			if resp.hookState != nil {
				resp.hookState.AfterReceive(resp, resp.err)
			}
			resp.deliverResult(pc.client)
		}
	}
}

func (pc *clientConn) handlePacket(responseType uint32, respReuseData *[]byte, respBody []byte) (resp *Response, _ error) {
	switch responseType {
	case tl.RpcServerWantsFin{}.TLTag():
		if debugPrint {
			fmt.Printf("%v client %p conn %p read Let's FIN\n", time.Now(), pc.client, pc)
		}
		pc.shutdown()
		return nil, nil
	case tl.RpcReqResultError{}.TLTag(): // old style, should not be sent by modern servers
		var reqResultError tl.RpcReqResultError
		var err error
		if respBody, err = reqResultError.Read(respBody); err != nil {
			return nil, fmt.Errorf("failed to read RpcReqResultError: %w", err)
		}
		cctx, closeNow := pc.finishCall(reqResultError.QueryId)
		if closeNow != nil { // not under lock
			_ = closeNow.Close()
		}
		if cctx == nil {
			// we expect that cctx can be nil because of teardownCall after context was done (and not because server decided to send garbage)
			return nil, nil // already cancelled or served
		}
		cctx.body = respReuseData
		cctx.Body = respBody
		cctx.err = &Error{Code: reqResultError.ErrorCode, Description: reqResultError.Error}
		return cctx, nil
	case tl.RpcReqResultHeader{}.TLTag():
		var header tl.RpcReqResultHeader
		var err error
		if respBody, err = header.Read(respBody); err != nil {
			return nil, fmt.Errorf("failed to read RpcReqResultHeader: %w", err)
		}
		cctx, closeNow := pc.finishCall(header.QueryId)
		if closeNow != nil { // not under lock
			_ = closeNow.Close()
		}
		if cctx == nil {
			// we expect that cctx can be nil because of teardownCall after context was done (and not because server decided to send garbage)
			return nil, nil // already cancelled or served
		}
		cctx.body = respReuseData
		cctx.Body, cctx.err = parseResponseExtra(&cctx.Extra, respBody)
		return cctx, nil
	default:
		pc.client.opts.Logf("rpc: unknown packet type 0x%x", responseType)
		return nil, nil
	}
}

func writeRequestUnlocked(conn *PacketConn, req *Request, writeTimeout time.Duration) error {
	if err := conn.WritePacketHeaderUnlocked(tl.RpcInvokeReqHeader{}.TLTag(), len(req.Body), writeTimeout); err != nil {
		return err
	}
	// we serialize Extra after Body, so we have to twist spacetime a bit
	if err := conn.WritePacketBodyUnlocked(req.Body[req.extraStart:]); err != nil {
		return err
	}
	if err := conn.WritePacketBodyUnlocked(req.Body[:req.extraStart]); err != nil {
		return err
	}
	conn.WritePacketTrailerUnlocked()
	return nil
}

func writeRequest(conn *PacketConn, req *Request, writeTimeout time.Duration) error {
	// we serialize Extra after Body, so we have to twist spacetime a bit
	body := req.Body[req.extraStart:]
	body2 := req.Body[:req.extraStart]
	return conn.WritePacket2(tl.RpcInvokeReqHeader{}.TLTag(), body, body2, writeTimeout)
}

func (pc *clientConn) moveRequestsToSendLocked(writeQ []writeReqCancel) []writeReqCancel {
	for i, wr := range pc.writeQ {
		if wr.cctx == nil {
			writeQ = append(writeQ, writeReqCancel{cancelQueryID: wr.cancelQueryID})
		} else if wr.cctx.stale {
			pc.client.PutResponse(wr.cctx) // free cancelled requests, we own
		} else {
			if wr.cctx.req == nil {
				panic("rpc.Client invariant violation: double sent")
			}
			pc.inFlight++
			writeQ = append(writeQ, writeReqCancel{req: wr.cctx.req})
			wr.cctx.req = nil // point of potential side effect
			// TODO - we want to move side effect just before WritePacket, but this would require locking there
		}
		pc.writeQ[i] = writeRespCancel{} // free memory
	}
	pc.writeQ = pc.writeQ[:0]
	return writeQ
}
