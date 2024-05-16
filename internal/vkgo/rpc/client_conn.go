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
	"time"

	"pgregory.net/rand"

	"github.com/vkcom/statshouse/internal/vkgo/rpc/internal/gen/tl"
	"github.com/vkcom/statshouse/internal/vkgo/srvfunc"
)

type writeReq struct {
	// We have 2 request kinds in writeQ
	// 1. cctx != nil, req != nil, not a cancel
	// 2. cctx == nil, req == nil, cancel cancelQueryID
	cctx          *callContext
	cancelQueryID int64
	req           *Request
	deadline      time.Time
}

type clientConn struct {
	client *Client

	address NetAddr

	writeQ       []writeReq
	writeQCond   sync.Cond
	writeFin     bool
	writeBuiltin bool

	mu                 sync.Mutex
	calls              map[int64]*callContext
	conn               *PacketConn
	waitingToReconnect bool

	closeCC chan<- struct{} // makes select case in goConnect always selectable. Set to nil after close to prevent double close

	resetReconnectDelayC chan<- struct{}
}

func (pc *clientConn) close() error {
	pc.dropClientConn(true)
	return nil
}

// if multiResult is used for many requests, it must contain enough space so that no receiver is blocked
func (pc *clientConn) setupCallLocked(req *Request, deadline time.Time, multiResult chan *callContext, cb ClientCallback, userData any) (*callContext, error) {
	cctx := pc.client.getCallContext()
	cctx.queryID = req.QueryID()
	if multiResult != nil {
		cctx.result = multiResult // overrides single-result channel
	}
	cctx.cb = cb
	cctx.userData = userData
	cctx.failIfNoConnection = req.Extra.FailIfNoConnection
	cctx.readonly = req.ReadOnly
	cctx.hookState, req.hookState = req.hookState, cctx.hookState // transfer ownership of "dirty" hook state to cctx

	if pc.closeCC == nil {
		pc.client.putCallContext(cctx)
		return nil, ErrClientClosed
	}

	if req.Extra.FailIfNoConnection && pc.waitingToReconnect {
		pc.client.putCallContext(cctx)
		return nil, ErrClientConnClosedNoSideEffect
	}
	if debugPrint {
		fmt.Printf("%v setupCallLocked for client %p pc %p\n", time.Now(), pc.client, pc)
	}

	pc.calls[cctx.queryID] = cctx

	pc.writeQ = append(pc.writeQ, writeReq{cctx: cctx, req: req, deadline: deadline})

	// Here cctx is owned by both writeQ and calls map

	if cctx.hookState != nil {
		cctx.hookState.BeforeSend(req)
	}

	return cctx, nil
}

func (pc *clientConn) cancelCall(queryID int64, deliverError error) (cancelled bool) {
	cctx, shouldSignal := pc.cancelCallImpl(queryID)
	if cctx != nil {
		// exclusive ownership of cctx by this function
		if deliverError != nil {
			cctx.err = deliverError
			cctx.deliverResult(pc.client)
		} else {
			pc.client.putCallContext(cctx)
		}
	}
	if shouldSignal { // write tl.RpcCancelReq, Signal is outside lock for efficiency
		pc.writeQCond.Signal()
	}
	return cctx != nil
}

func (pc *clientConn) cancelCallImpl(queryID int64) (shouldReleaseCctx *callContext, shouldSignal bool) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	cctx, ok := pc.calls[queryID]
	if !ok {
		return nil, false
	}
	delete(pc.calls, queryID)
	if !cctx.sent {
		cctx.stale = true // exclusive ownership of cctx by writeQ now, will be released
		return nil, false
	}
	if pc.conn != nil && pc.conn.FlagCancelReq() {
		pc.writeQ = append(pc.writeQ, writeReq{cancelQueryID: queryID})
		return cctx, true
	}
	return cctx, false
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

func (pc *clientConn) massCancelRequestsLocked() (finishedCalls []*callContext) {
	pc.writeFin = false
	nQueued := 0
	now := time.Now()
	for _, wr := range pc.writeQ {
		if wr.cctx == nil { // Must not send old cancels to new connection
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

func (pc *clientConn) continueRunningImpl(previousGoodHandshake bool) (finishedCalls []*callContext, _ bool) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	finishedCalls = pc.massCancelRequestsLocked()

	if !previousGoodHandshake {
		pc.waitingToReconnect = true
	}
	return finishedCalls, pc.closeCC != nil && len(pc.calls) != 0
}

func (pc *clientConn) putStaleRequest(wr writeReq) {
	pc.client.putCallContext(wr.cctx)
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

func (pc *clientConn) goConnect(closeCC <-chan struct{}, resetReconnectDelayC <-chan struct{}) {
	defer pc.client.wg.Done()
	reconnectTimer := time.NewTimer(maxReconnectDelay)
	reconnectTimer.Stop()
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
			if reconnectDelay > maxReconnectDelay {
				reconnectDelay = maxReconnectDelay
			}
		}
	}
}

func (pc *clientConn) run() (goodHandshake bool) {
	address := srvfunc.MaybeResolveAddr(pc.address.Network, pc.address.Address)
	nc, err := net.DialTimeout(pc.address.Network, address, DefaultHandshakeStepTimeout)
	if err != nil {
		pc.client.opts.Logf("rpc: failed to start new peer connection with %v: %v", pc.address, err)
		return false
	}

	conn := NewPacketConn(nc, pc.client.opts.ConnReadBufSize, pc.client.opts.ConnWriteBufSize, DefaultConnTimeoutAccuracy)

	err = conn.HandshakeClient(pc.client.opts.CryptoKey, pc.client.opts.TrustedSubnetGroups, pc.client.opts.ForceEncryption, uniqueStartTime(), 0, DefaultHandshakeStepTimeout, pc.client.opts.ProtocolVersion)
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

	wg.Add(1)
	go func() {
		defer wg.Done()
		if sendErr := pc.sendLoop(conn); sendErr != nil {
			pc.dropClientConn(false) // only in case of error, not when sent FIN or responded to close
		}
	}()
	pc.receiveLoop(conn)
	wg.Wait()

	return true
}

func (pc *clientConn) sendLoop(conn *PacketConn) error {
	if debugPrint {
		fmt.Printf("%v sendLoop %p conn %p start\n", time.Now(), pc.client, pc)
	}
	var buf []writeReq
	writeFin := false
	writeBuiltin := false
	var customBody []byte
	for {
		writeFin, writeBuiltin, buf = pc.writeQAcquire(buf[:0])
		if !writeFin && !writeBuiltin && len(buf) == 0 {
			return nil
		}
		if writeBuiltin {
			err := conn.WritePacketBuiltinNoFlushUnlocked(pc.client.opts.PacketTimeout)
			if err != nil {
				if !commonConnCloseError(err) {
					pc.client.opts.Logf("rpc: failed to send ping/pong to %v, disconnecting: %v", conn.remoteAddr, err)
				}
				return err
			}
		}
		for _, wr := range buf {
			if wr.cctx == nil {
				ret := tl.RpcCancelReq{QueryId: wr.cancelQueryID}
				customBody = ret.Write(customBody[:0])
				err := writeCustomPacketUnlocked(conn, tl.RpcCancelReq{}.TLTag(), customBody, pc.client.opts.PacketTimeout)
				if err != nil {
					if !commonConnCloseError(err) {
						pc.client.opts.Logf("rpc: failed to send custom packet to %v, disconnecting: %v", conn.remoteAddr, err)
					}
					return err
				}
			} else { // here wr.cctx != nil && wr.req != nil
				err := writeRequestUnlocked(conn, wr.req, pc.client.opts.PacketTimeout)
				pc.client.putRequest(wr.req)
				if err != nil {
					if !commonConnCloseError(err) {
						pc.client.opts.Logf("rpc: failed to send packet to %v, disconnecting: %v", conn.remoteAddr, err)
					}
					return err
				}
			}
		}

		// TODO - use sent flag instead, like in rpc.Server
		if pc.writeQFlush() { // before writeFIN we must flush regardless of policy set by writeQFlush
			if err := conn.FlushUnlocked(); err != nil {
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

func (pc *clientConn) receiveLoop(conn *PacketConn) {
	defer pc.dropClientConn(false)

	for {
		resp := pc.client.getResponse() // we use Response as a scratch space for incoming (even non-response) packet here
		var typ uint32
		var isBuiltin bool
		var err error

		for {
			typ, resp.Body, isBuiltin, _, err = conn.ReadPacketUnlocked(resp.body, pc.client.opts.PacketTimeout)
			resp.body = resp.Body[:0] // prepare for reuse immediately
			if err != nil {
				pc.client.PutResponse(resp)
				if !commonConnCloseError(err) {
					pc.client.opts.Logf("rpc: error reading packet from %v, disconnecting: %v", conn.remoteAddr, err)
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
		resp.responseType = typ
		put, err := pc.handlePacket(resp)
		if put {
			pc.client.PutResponse(resp)
		}
		if err != nil {
			pc.client.opts.Logf("rpc: failed to handle packet from %v, disconnecting: %v", conn.remoteAddr, err)
			return
		}
	}
}

func (pc *clientConn) handlePacket(resp *Response) (put bool, _ error) {
	switch resp.responseType {
	case tl.RpcServerWantsFin{}.TLTag():
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
	case tl.RpcReqResultError{}.TLTag():
		var reqResultError tl.RpcReqResultError
		var err error
		if resp.Body, err = reqResultError.Read(resp.Body); err != nil {
			return true, fmt.Errorf("failed to read RpcReqResultError: %w", err)
		}
		err = Error{Code: reqResultError.ErrorCode, Description: reqResultError.Error}
		return pc.handleResponse(reqResultError.QueryId, resp, err), nil
	case tl.RpcReqResultHeader{}.TLTag():
		var header tl.RpcReqResultHeader
		var err error
		if resp.Body, err = header.Read(resp.Body); err != nil {
			return true, fmt.Errorf("failed to read RpcReqResultHeader: %w", err)
		}
		err = parseResponseExtra(resp, pc.client.opts.Logf)
		return pc.handleResponse(header.QueryId, resp, err), nil
	default:
		pc.client.opts.Logf("unknown packet type 0x%x", resp.responseType)
		return true, nil
	}
}

func (pc *clientConn) handleResponse(queryID int64, resp *Response, rpcErr error) (put bool) {
	cctx := pc.finishCall(queryID)
	if cctx == nil {
		// we expect that cctx can be nil because of teardownCall after context was done (and not because server decided to send garbage)
		return true // already cancelled or served
	}

	cctx.err = rpcErr
	cctx.resp = resp

	if cctx.hookState != nil {
		cctx.hookState.AfterReceive(cctx.resp, cctx.err)
	}

	cctx.deliverResult(pc.client)
	return false
}

func writeCustomPacketUnlocked(conn *PacketConn, packetType uint32, customBody []byte, writeTimeout time.Duration) error {
	if err := conn.writePacketHeaderUnlocked(packetType, len(customBody), writeTimeout); err != nil {
		return err
	}
	if err := conn.writePacketBodyUnlocked(customBody); err != nil {
		return err
	}
	conn.writePacketTrailerUnlocked()
	return nil
}

func writeRequestUnlocked(conn *PacketConn, req *Request, writeTimeout time.Duration) error {
	if err := conn.writePacketHeaderUnlocked(tl.RpcInvokeReqHeader{}.TLTag(), len(req.Body), writeTimeout); err != nil {
		return err
	}
	// we serialize Extra after Body, so we have to twist spacetime a bit
	if err := conn.writePacketBodyUnlocked(req.Body[req.extraStart:]); err != nil {
		return err
	}
	if err := conn.writePacketBodyUnlocked(req.Body[:req.extraStart]); err != nil {
		return err
	}
	conn.writePacketTrailerUnlocked()
	return nil
}

func (pc *clientConn) writeQAcquire(q []writeReq) (writeFin bool, writeBuiltin bool, _ []writeReq) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	for {
		if !(pc.conn == nil || len(pc.writeQ) != 0 || pc.writeFin || pc.writeBuiltin) {
			pc.writeQCond.Wait()
			continue
		}

		if pc.conn == nil {
			return false, false, nil
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
				// TODO - we want to move side effect just before WritePacket, but this would require
			}
			pc.writeQ[i] = writeReq{}
			ret = append(ret, wr)
		}
		pc.writeQ = pc.writeQ[:0]
		writeBuiltin := pc.writeBuiltin
		pc.writeBuiltin = false
		if pc.writeFin || writeBuiltin || len(ret) != 0 {
			return pc.writeFin, writeBuiltin, ret // We do not clear writeFin, because it must be cleared on reconnect anyway
		}
		// All requests were stale, wait for more requests
	}
}

func (pc *clientConn) writeQFlush() bool {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	return pc.conn == nil || len(pc.writeQ) == 0
}
