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

func (sc *UdpServerConn) pushUnlock(hctx *HandlerContext) {
	sc.mu.Unlock()
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
	sc.server.releaseHandlerCtx(hctx)

	if err != nil {
		log.Printf("%+v", err)
	}
}

func (sc *UdpServerConn) acquireHandlerCtx() *HandlerContext {
	return sc.server.acquireHandlerCtx(sc, protocolUDP)
}
