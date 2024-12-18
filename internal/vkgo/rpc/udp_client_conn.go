// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"fmt"
	"sync"
	"time"

	"github.com/vkcom/statshouse/internal/vkgo/algo"
	"github.com/vkcom/statshouse/internal/vkgo/basictl"
	"github.com/vkcom/statshouse/internal/vkgo/rpc/internal/gen/tl"
	"github.com/vkcom/statshouse/internal/vkgo/rpc/udp"
)

type udpClientConn struct {
	client *udpClient

	addresses map[NetAddr]struct{}

	mu    sync.Mutex
	calls map[int64]*Response
	conn  *udp.Connection
}

// if multiResult is used for many requests, it must contain enough space so that no receiver is blocked
func (pc *udpClientConn) setupCallLocked(req *Request, deadline time.Time, multiResult chan *Response, cb ClientCallback, userData any) (*Response, error) {
	cctx := pc.client.client.getResponse()
	cctx.queryID = req.QueryID()
	if multiResult != nil {
		cctx.result = multiResult // overrides single-result channel
	}
	cctx.cb = cb
	cctx.userData = userData
	cctx.failIfNoConnection = req.FailIfNoConnection
	cctx.readonly = req.ReadOnly
	cctx.hookState, req.hookState = req.hookState, cctx.hookState // transfer ownership of "dirty" hook state to cctx

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
	err := pc.conn.SendMessage(message) // TODO conn.SendCircularMessage
	if err != nil {
		return nil, err
	}

	if cctx.hookState != nil {
		cctx.hookState.BeforeSend(req)
	}

	return cctx, nil
}

func (pc *udpClientConn) cancelCall(queryID int64, deliverError error) (cancelled bool) {
	cctx := pc.cancelCallImpl(queryID)
	if cctx != nil {
		// exclusive ownership of cctx by this function
		if deliverError != nil {
			cctx.err = deliverError
			cctx.deliverResult(pc.client.client)
		} else {
			pc.client.client.PutResponse(cctx)
		}
	}
	return cctx != nil
}

func (pc *udpClientConn) cancelCallImpl(queryID int64) (shouldReleaseCctx *Response) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	cctx, ok := pc.calls[queryID]
	if !ok {
		return nil
	}
	delete(pc.calls, queryID)
	if !cctx.sent {
		cctx.stale = true // exclusive ownership of cctx by writeQ now, will be released
		return nil
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
	resp, err := pc.handlePacket(responseType, message, respBody)
	if resp != nil {
		/* TODO
		if resp.hookState != nil {
			resp.hookState.AfterReceive(resp, resp.err)
		}
		*/
		resp.deliverResult(nil)
	}
	if err != nil { // resp is always nil here
		pc.client.client.opts.Logf("rpc: failed to handle packet from ---, disconnecting: %v", err)
		return
	}
}

func (pc *udpClientConn) finishCall(queryID int64) *Response {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	resp, ok := pc.calls[queryID]
	if !ok {
		return nil
	}
	delete(pc.calls, queryID)
	return resp
}

func (pc *udpClientConn) handlePacket(responseType uint32, respReuseData *[]byte, respBody []byte) (resp *Response, _ error) {
	switch responseType {
	case tl.RpcServerWantsFin{}.TLTag():
		pc.client.client.opts.Logf("rpc: TODO handle RpcServerWantsFin")
		/*if debugPrint {
			fmt.Printf("%v client %p conn %p read Let's FIN\n", time.Now(), pc.client, pc)
		}
		pc.mu.Lock()
		pc.writeFin = true
		pc.mu.Unlock()
		pc.writeQCond.Signal()*/
		// goWrite will take last bunch of requests plus fin from writeQ, write them and quit.
		// all requests put into writeQ after writer quits will wait there until the next reconnect
		return nil, nil
	case tl.RpcReqResultError{}.TLTag():
		var reqResultError tl.RpcReqResultError
		var err error
		if respBody, err = reqResultError.Read(respBody); err != nil {
			return nil, fmt.Errorf("failed to read RpcReqResultError: %w", err)
		}
		cctx := pc.finishCall(reqResultError.QueryId)
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
		cctx := pc.finishCall(header.QueryId)
		if cctx == nil {
			// we expect that cctx can be nil because of teardownCall after context was done (and not because server decided to send garbage)
			return nil, nil // already cancelled or served
		}
		cctx.body = respReuseData
		cctx.Body, cctx.err = parseResponseExtra(&cctx.Extra, respBody)
		return cctx, nil
	default:
		pc.client.client.opts.Logf("rpc: unknown packet type 0x%x", responseType)
		return nil, nil
	}
}
