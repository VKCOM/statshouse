// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"log"

	"github.com/VKCOM/statshouse/internal/vkgo/basictl"
	"github.com/VKCOM/statshouse/internal/vkgo/rpc/internal/gen/tl"
	"github.com/VKCOM/statshouse/internal/vkgo/rpc/internal/gen/tlnet"
	"github.com/VKCOM/statshouse/internal/vkgo/rpc/udp"
)

type UdpServerConn struct {
	serverConnCommon

	conn *udp.Connection
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

	_, err := basictl.NatReadExactTag(hctx.Response, tlnet.Pid{}.TLTag())
	pong := err == nil

	if pong {
		err = sc.conn.SendUnreliableMessage(responseMessage)
	} else {
		err = sc.conn.SendMessage(responseMessage)
	}

	if sc.serverConnCommon.server.opts.DebugUdpRPC >= 2 {
		log.Printf("udp rpc response sent")
		if pong {
			log.Printf("udp pong sent")
		}
	}
	sc.server.releaseHandlerCtx(hctx)

	if err != nil {
		log.Printf("%+v", err)
	}
}
