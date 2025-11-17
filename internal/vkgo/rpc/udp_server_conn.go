// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"encoding/binary"
	"log"
	"net"
	"unsafe"

	"github.com/VKCOM/statshouse/internal/vkgo/basictl"
	"github.com/VKCOM/statshouse/internal/vkgo/rpc/internal/gen/tl"
	"github.com/VKCOM/statshouse/internal/vkgo/rpc/internal/gen/tlnet"
	"github.com/VKCOM/statshouse/internal/vkgo/rpc/udp"
)

type UdpServerConn struct {
	serverConnCommon

	conn *udp.Connection
}

func (sc *UdpServerConn) ListenAddr() net.Addr      { return sc.conn.ListenAddr() }
func (sc *UdpServerConn) LocalAddr() net.Addr       { return sc.conn.LocalAddr() }
func (sc *UdpServerConn) RemoteAddr() net.Addr      { return sc.conn.RemoteAddr() }
func (sc *UdpServerConn) KeyID() [4]byte            { return sc.conn.KeyID() }
func (sc *UdpServerConn) ProtocolVersion() uint32   { return 0 }
func (sc *UdpServerConn) ProtocolTransportID() byte { return protocolUDP }
func (sc *UdpServerConn) ConnectionID() uintptr {
	return uintptr(unsafe.Pointer(sc))
}

func (sc *UdpServerConn) StartLongpoll(hctx *HandlerContext, canceller LongpollCanceller) (LongpollHandle, error) {
	lh, resp := sc.server.toLongpollContext(hctx, canceller)
	if err := sc.makeLongpollResponse(lh, resp); err != nil {
		return LongpollHandle{}, err
	}
	hctx.longpollStarted = true // do not send response
	return lh, nil
}

func (sc *UdpServerConn) finishLongpoll2(lh LongpollHandle) (*HandlerContext, LongpollCanceller, error) {
	resp, err := sc.finishLongpoll(lh.QueryID, protocolUDP)
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

func (sc *UdpServerConn) DebugName() string {
	return sc.debugName
}

func (sc *UdpServerConn) FinishLongpoll(lh LongpollHandle) (*HandlerContext, error) {
	hctx, _, err := sc.finishLongpoll2(lh)
	if err == nil {
		if sc.server.opts.DebugRPC {
			sc.server.opts.Logf("rpc_debug: %s FinishLongpoll success queryID=%d", sc.debugName, lh.QueryID)
		}
	}
	return hctx, err
}

func (sc *UdpServerConn) CancelLongpoll(queryID int64) (LongpollCanceller, int64) {
	resp, err := sc.finishLongpoll(queryID, protocolUDP)
	if err != nil {
		return nil, 0
	}
	return resp.canceller, resp.deadline
}

func (sc *UdpServerConn) SendEmptyResponse(lh LongpollHandle) {
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

func (sc *UdpServerConn) acquireHandlerCtx() *HandlerContext {
	if sc.server.opts.DebugRPC {
		sc.server.opts.Logf("rpc_debug: %s acquireHandlerCtx", sc.debugName)
	}
	return sc.server.acquireHandlerCtx(sc, protocolUDP)
}

func (sc *UdpServerConn) releaseHandlerCtx(hctx *HandlerContext) {
	if sc.server.opts.DebugRPC {
		sc.server.opts.Logf("rpc_debug: %s releaseHandlerCtx queryID=%d", sc.debugName, hctx.queryID)
	}
	sc.server.releaseHandlerCtx(hctx)
}

func (sc *UdpServerConn) cancelAllLongpollResponses() {
	// TODO - do this under correct Lock at correct time
	/*
		sc.mu.Lock()
		longpolls := sc.longpolls
		sc.longpolls = nil // If makeLongpollResponse is called, we'll panic. But this must be impossible if SyncHandler follows protocol
		sc.mu.Unlock()
		for _, resp := range longpolls {
			if debugPrint {
				fmt.Printf("longpolls cancel (all) %d\n", resp.handle.QueryID)
			}
			if debugTrace {
				sc.server.addTrace(fmt.Sprintf("cancelAllLongpollResponse %d", resp.handle.QueryID))
			}
			sc.server.protocolStats[resp.protocolTransportID].longPollsWaiting.Add(-1)
			sc.server.longpollTree.DeleteLongpoll(resp.handle, resp.deadline)
			resp.canceller.CancelLongpoll(resp.handle)
			// This happens only in shutdown, so we have to tell the client that his longpoll
			// was cancelled
			hctx := sc.acquireHctx().fillFromHijackedResponse(resp)
			sc.pushResponse(hctx, errCancelHijack)
			// isLongpoll false because it would look it up in sc.longpolls and incorrectly consider hctx released and do nothing
		}
	*/
}

func (sc *UdpServerConn) SendLongpollResponse(hctx *HandlerContext, err error) {
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
	sc.mu.Unlock()
	if sc.server.opts.DebugRPC {
		sc.server.opts.Logf("rpc_debug: %s SendLongpollResponse (push) queryID=%d", sc.debugName, hctx.queryID)
	}
	sc.pushUnlock(hctx)
}

func (sc *UdpServerConn) pushUnlock(hctx *HandlerContext) {
	// TODO implement and use Transport::SendCircularMessage method
	fullResponseSize := len(hctx.Response) + 4                           // tagSize
	responseMessage := sc.server.allocateRequestBufUDP(fullResponseSize) // TODO - better idea
	*responseMessage = (*responseMessage)[:0]
	*responseMessage = basictl.NatWrite(*responseMessage, tl.RpcReqResultHeader{}.TLTag())
	*responseMessage = append(*responseMessage, hctx.Response[hctx.extraStart:]...)
	*responseMessage = append(*responseMessage, hctx.Response[:hctx.extraStart]...)

	pong := len(hctx.Response) >= 4 && binary.LittleEndian.Uint32(hctx.Response) == tlnet.Pid{}.TLTag()

	var err error
	if pong {
		err = sc.conn.SendUnreliableMessage(responseMessage)
	} else {
		err = sc.conn.SendMessage(responseMessage)
	}

	if pong && sc.serverConnCommon.server.opts.DebugUdpRPC >= 1 {
		log.Printf("udp pong sent")
	}
	if sc.serverConnCommon.server.opts.DebugUdpRPC >= 2 {
		log.Printf("udp rpc response sent")
	}
	sc.releaseHandlerCtx(hctx)

	if err != nil {
		log.Printf("%+v", err)
	}
}
