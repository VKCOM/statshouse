// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"fmt"
	"sync"
	"time"

	"github.com/VKCOM/statshouse/internal/vkgo/algo"
	"github.com/VKCOM/statshouse/internal/vkgo/basictl"
	"github.com/VKCOM/statshouse/internal/vkgo/rpc/internal/gen/tl"
	"github.com/VKCOM/statshouse/internal/vkgo/rpc/udp"
)

type udpClientConn struct {
	client *udpClient

	addresses map[NetAddr]struct{}

	mu        sync.Mutex
	calls     map[int64]*Response
	totalSent int // # of send calls. Wait for this to become 0 before closing connection to server
	conn      *udp.Connection
}

// if multiResult is used for many requests, it must contain enough space so that no receiver is blocked
func (pc *udpClientConn) setupCallLocked(req *Request, cctx *Response) error {
	cctx.req = nil // we take ownership of request, freeing it below
	pc.totalSent++

	/* in UDP we don't have pc.closeCC: we either got pc from client.conns and has added call to it, or didn't get pc and created new connect
	// TODO understand why no pc.closeCC in UDP
	if pc.closeCC == nil {
		return cctx, ErrClientClosed
	}
	*/

	/* in UDP there is no such reconnect as in tcp
	if req.FailIfNoConnection && pc.waitingToReconnect {
		return cctx, ErrClientConnClosedNoSideEffect
	}
	*/
	if debugPrint {
		fmt.Printf("%v setupCallLocked for client %p pc %p\n", time.Now(), pc.client, pc)
	}

	pc.calls[cctx.queryID] = cctx

	var message = pc.client.allocateMessage(0) // not easy to avoid copy, so we simply pay for it

	*message = basictl.NatWrite(*message, tl.RpcInvokeReqHeader{}.TLTag())
	headerTagSize := len(*message)
	*message = algo.ResizeSlice(*message, headerTagSize+len(req.Body))
	*message = (*message)[:headerTagSize]
	*message = append(*message, req.Body[req.extraStart:]...)
	*message = append(*message, req.Body[:req.extraStart]...)

	pc.client.client.putRequest(req)

	// TODO pass deadline to SendMessage()
	return pc.conn.SendMessage(message) // TODO conn.SendCircularMessage
}

func (pc *udpClientConn) cancelCall(queryID int64) (shouldReleaseCctx *Response) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	cctx, ok := pc.calls[queryID]
	if !ok {
		return nil
	}
	delete(pc.calls, queryID)
	if cctx.req != nil { // was not sent, residing somewhere in writeQ
		return nil
	}
	// was sent
	pc.totalSent--
	if pc.totalSent < 0 {
		panic("rpc.Client invariant violation: pc.inFlight < 0")
	}
	if pc.conn != nil {
		var message = pc.client.allocateMessage(12)
		ret := tl.RpcCancelReq{QueryId: queryID}
		*message = (*message)[:0]
		*message = ret.Write(*message)
		err := pc.conn.SendMessage(message)
		if err != nil {
			panic(err) // cancel message is always 12 byte sized, so code must be unreachable
		}
		return cctx
	}
	return cctx
}

func (pc *udpClientConn) handle(message *[]byte, canSave bool) {
	//log.Print("%v   2) udpClient got response\n", time.Now().String())
	if !canSave {
		newMsg := pc.client.allocateMessage(len(*message))
		copy(*newMsg, *message)
		message = newMsg
	}

	respBody := *message
	var err error
	var responseType uint32
	respBody, err = basictl.NatRead(respBody, &responseType)
	if err != nil {
		panic(err)
		// TODO
	}
	_, finishedCallback, _, err := pc.handlePacket(responseType, message, respBody)
	//if bodyOwned {
	// TODO - move canSave check into handlePacket where boy is assigned to cctx
	//}
	if err != nil { // resp is always nil here
		pc.client.client.opts.Logf("rpc: failed to handle packet from ---, disconnecting: %v", err)
		return
	}
	if finishedCallback.resp != nil {
		finishedCallback.resp.cb(pc.client.client, finishedCallback.resp, finishedCallback.err)
	}
	//TODO - readTime
	//if recordLatency && pc.client.client.opts.receiveLatencyHandler != nil {
	//	latency := time.Since(readTime)
	//	pc.client.client.opts.receiveLatencyHandler(latency)
	//}
}

func (pc *udpClientConn) finishCall(queryID int64, reuseBody *[]byte, body []byte, extra *ResponseExtra, err error) (bodyOwned bool, finishedCallback callResult, recordLatency bool) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	cctx, ok := pc.calls[queryID]
	if !ok {
		return false, callResult{}, false
	}
	//if cctx.req != nil { // was not sent, residing somewhere in writeQ
	// In finish call, this is possible if server sends response with a queryID before request was sent by client.
	// We treat it as a normal cancel
	//}
	// was sent
	delete(pc.calls, queryID)

	recordLatency = cctx.actorID == requestLatencyDebugActorId
	cctx.reuseBody = reuseBody
	cctx.Body = body
	cctx.Extra = *extra
	if cctx.result != nil {
		cctx.result <- callResult{resp: cctx, err: err} // must deliver under lock
	} else {
		finishedCallback = callResult{resp: cctx, err: err}
	}
	bodyOwned = true

	return
}

func (pc *udpClientConn) handlePacket(responseType uint32, respReuseData *[]byte, respBody []byte) (bodyOwned bool, finishedCallback callResult, recordLatency bool, err error) {
	switch responseType {
	case tl.RpcServerWantsFin{}.TLTag():
		// Must not arrive via UDP, we ignore it for now
		if debugPrint {
			fmt.Printf("%v client %p conn %p read Let's FIN\n", time.Now(), pc.client, pc)
		}
		return false, callResult{}, false, nil
	case tl.RpcReqResultError{}.TLTag(): // old style, should not be sent by modern servers
		var reqResultError tl.RpcReqResultError
		var err error
		if respBody, err = reqResultError.Read(respBody); err != nil {
			return false, callResult{}, false, fmt.Errorf("failed to read RpcReqResultError: %w", err)
		}
		err = &Error{Code: reqResultError.ErrorCode, Description: reqResultError.Error}
		var extra ResponseExtra
		bodyOwned, finishedCallback, recordLatency = pc.finishCall(reqResultError.QueryId, respReuseData, respBody, &extra, err)
		return bodyOwned, finishedCallback, recordLatency, nil
	case tl.RpcReqResultHeader{}.TLTag():
		var header tl.RpcReqResultHeader
		var err error
		if respBody, err = header.Read(respBody); err != nil {
			return false, callResult{}, false, fmt.Errorf("failed to read RpcReqResultHeader: %w", err)
		}
		var extra ResponseExtra
		respBody, err = parseResponseExtra(&extra, respBody)

		bodyOwned, finishedCallback, recordLatency = pc.finishCall(header.QueryId, respReuseData, respBody, &extra, err)
		return bodyOwned, finishedCallback, recordLatency, nil
	default:
		pc.client.client.opts.Logf("rpc: unknown packet type 0x%x", responseType)
		return false, callResult{}, false, nil
	}
}
