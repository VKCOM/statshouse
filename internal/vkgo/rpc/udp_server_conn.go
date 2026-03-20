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

	conn       *udp.Connection
	errHandler ErrHandlerFunc
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

// hctx==nil, canceller==nil - do nothing, response already sent
// hctx!=nil, canceller!=nil - normal, call canceller.SendEmptyReponse
// hctx==nil, canceller!=nil - server shutdown, call canceller.Cancel
func (sc *UdpServerConn) finishLongpoll2(lh LongpollHandle) (*HandlerContext, LongpollCanceller) {
	resp := sc.finishLongpoll(lh, protocolUDP)
	if resp.canceller == nil {
		return nil, nil
	}
	hctx := sc.acquireHandlerCtx()
	hctx.queryID = lh.QueryID
	hctx.commonConn = lh.CommonConn
	hctx.handlerContextFields = resp.handlerContextFields
	if err := sc.server.acquireHCtxResponse(sc.closeCtx, hctx); err != nil {
		sc.releaseHandlerCtx(hctx)
		if sc.server.opts.DebugRPC {
			sc.server.opts.Logf("rpc_debug: %s FinishLongpoll connection closed queryID=%d", sc.debugName, lh.QueryID)
		}
		return nil, resp.canceller
	}
	return hctx, resp.canceller
}

func (sc *UdpServerConn) DebugName() string {
	return sc.debugName
}

func (sc *UdpServerConn) FinishLongpoll(lh LongpollHandle) (*HandlerContext, error) {
	hctx, _ := sc.finishLongpoll2(lh)
	// called by user, so we should not call CancelLongpoll
	if sc.server.opts.DebugRPC {
		if hctx != nil {
			sc.server.opts.Logf("rpc_debug: %s FinishLongpoll success queryID=%d", sc.debugName, lh.QueryID)
		} else {
			sc.server.opts.Logf("rpc_debug: %s FinishLongpoll empty queryID=%d", sc.debugName, lh.QueryID)
		}
	}
	return hctx, nil
}

func (sc *UdpServerConn) CancelLongpoll(queryID int64) (LongpollCanceller, int64) {
	lh := LongpollHandle{QueryID: queryID, CommonConn: sc}
	resp := sc.finishLongpoll(lh, protocolUDP)
	return resp.canceller, resp.deadline
}

func (sc *UdpServerConn) SendEmptyResponse(lh LongpollHandle) {
	hctx, canceller := sc.finishLongpoll2(lh)
	if canceller == nil {
		return
	}
	if hctx == nil {
		canceller.CancelLongpoll(lh)
		return
	}
	err := canceller.WriteEmptyResponse(lh, hctx)
	if err == nil && len(hctx.Response) == 0 {
		err = ErrLongpollNoEmptyResponse
	}
	if sc.server.opts.DebugRPC {
		sc.server.opts.Logf("rpc_debug: %s WriteEmptyResponse queryID=%d err: %v", sc.debugName, lh.QueryID, err)
	}
	sc.SendResponse(hctx, err)
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

func (sc *UdpServerConn) SendResponse(hctx *HandlerContext, err error) {
	if hctx.longpollStarted || hctx.noResult {
		if hctx.noResult && err != nil {
			sc.server.rareLog(&sc.server.lastOtherLog, "rpc: failed to handle no_result query: #%v tag: #%08x method: %q error: %v", hctx.queryID, hctx.reqTag, hctx.requestFunctionName, err)
		}
		if sc.server.opts.DebugRPC {
			if hctx.longpollStarted {
				sc.server.opts.Logf("rpc_debug: %s SendResponse (longpollStarted) queryID=%d", sc.debugName, hctx.queryID)
			} else {
				sc.server.opts.Logf("rpc_debug: %s SendResponse (noResult) queryID=%d", sc.debugName, hctx.queryID)
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
			sc.server.opts.Logf("rpc_debug: %s SendResponse (closed) queryID=%d", sc.debugName, hctx.queryID)
		}
		sc.releaseHandlerCtx(hctx)
		return
	}
	sc.mu.Unlock()

	queryID := hctx.queryID
	sc.sendResponseImpl(hctx)
	sc.releaseHandlerCtx(hctx)

	if sc.server.opts.DebugRPC {
		sc.server.opts.Logf("rpc_debug: %s SendResponse (push) queryID=%d", sc.debugName, queryID)
	}
}

func (sc *UdpServerConn) sendResponseImpl(hctx *HandlerContext) {
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

	if err != nil {
		log.Printf("%+v", err)
	}
}
