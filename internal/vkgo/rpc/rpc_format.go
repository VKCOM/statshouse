// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/VKCOM/statshouse/internal/vkgo/basictl"
	"github.com/VKCOM/statshouse/internal/vkgo/rpc/internal/gen/tl"
)

// serialisation of RPC packets is here
// 1. client writes request
// 2. server reads request
// 3. server writes response
// 2. client reads response

func preparePacket(req *Request) error {
	headerBuf := req.Body // move to local var, then back for speed
	req.extraStart = len(headerBuf)
	reqHeader := tl.RpcInvokeReqHeader{QueryId: req.queryID}
	headerBuf = reqHeader.Write(headerBuf)
	switch {
	case req.ActorID != 0 && req.Extra.Flags != 0:
		// extra := tl.RpcDestActorFlags{ActorId: req.ActorID, Extra: req.Extra}
		// headerBuf = extra.WriteBoxed(headerBuf)
		// we optimize copy of large extra here by writing code above manually
		headerBuf = basictl.NatWrite(headerBuf, tl.RpcDestActorFlags{}.TLTag())
		headerBuf = basictl.LongWrite(headerBuf, req.ActorID)
		headerBuf = req.Extra.Write(headerBuf)
	case req.Extra.Flags != 0:
		// extra := tl.RpcDestFlags{Extra: req.Extra}
		// headerBuf = extra.WriteBoxed(headerBuf)
		// we optimize copy of large extra here by writing code above manually
		headerBuf = basictl.NatWrite(headerBuf, tl.RpcDestFlags{}.TLTag())
		headerBuf = req.Extra.Write(headerBuf)
	case req.ActorID != 0:
		extra := tl.RpcDestActor{ActorId: req.ActorID}
		headerBuf = extra.WriteBoxed(headerBuf)
	}
	if req.BodyFormatTL2 {
		headerBuf = basictl.NatWrite(headerBuf, tl.RpcTL2Marker{}.TLTag())
	}
	if err := validBodyLen(len(headerBuf)); err != nil { // exact
		return err
	}
	req.Body = headerBuf
	return nil
}

// This method is temporarily public, do not use directly
func (hctx *HandlerContext) ParseInvokeReq(opts *ServerOptions) (err error) {
	var reqHeader tl.RpcInvokeReqHeader
	if hctx.Request, err = reqHeader.Read(hctx.Request); err != nil {
		return fmt.Errorf("failed to read request query ID: %w", err)
	}
	hctx.queryID = reqHeader.QueryId

	var afterTag []byte
	actorIDSet := 0
	extraSet := 0
	tl2MarkerSet := 0
	tl2MarkerNotLast := false
loop:
	for {
		var tag uint32
		if afterTag, err = basictl.NatRead(hctx.Request, &tag); err != nil {
			return fmt.Errorf("failed to read tag: %w", err)
		}
		switch tag {
		case tl.RpcDestActor{}.TLTag():
			if hctx.bodyFormatTL2 {
				tl2MarkerNotLast = true
			}
			var extra tl.RpcDestActor
			if hctx.Request, err = extra.Read(afterTag); err != nil {
				return fmt.Errorf("failed to read rpcDestActor: %w", err)
			}
			hctx.actorID = extra.ActorId
			actorIDSet++
		case tl.RpcDestFlags{}.TLTag():
			if hctx.bodyFormatTL2 {
				tl2MarkerNotLast = true
			}
			// var extra tl.RpcDestFlags
			// if hctx.Request, err = extra.Read(afterTag); err != nil {
			// here we optimize copy of large extra
			if hctx.Request, err = hctx.RequestExtra.Read(afterTag); err != nil {
				return fmt.Errorf("failed to read request rpcDestFlags: %w", err)
			}
			extraSet++
		case tl.RpcDestActorFlags{}.TLTag():
			if hctx.bodyFormatTL2 {
				tl2MarkerNotLast = true
			}
			// var extra tl.RpcDestActorFlags
			// if hctx.Request, err = extra.Read(afterTag); err != nil {
			// here we optimize copy of large extra
			if afterTag, err = basictl.LongRead(afterTag, &hctx.actorID); err != nil {
				return fmt.Errorf("failed to read rpcDestActorFlags: %w", err)
			}
			if hctx.Request, err = hctx.RequestExtra.Read(afterTag); err != nil {
				return fmt.Errorf("failed to read rpcDestActorFlags: %w", err)
			}
			actorIDSet++
			extraSet++
		case tl.RpcTL2Marker{}.TLTag():
			hctx.bodyFormatTL2 = true
			hctx.Request = afterTag
			tl2MarkerSet++
		default:
			hctx.reqTag = tag
			break loop
		}
	}
	if actorIDSet > 1 || extraSet > 1 {
		return fmt.Errorf("rpc: ActorID or RequestExtra set more than once (%d and %d) for request tag #%08d; please report to infrastructure team", actorIDSet, extraSet, hctx.reqTag)
	}
	if tl2MarkerSet > 1 {
		return fmt.Errorf("rpc: TL2 marker set more than once %d for request tag #%08d; please report to infrastructure team", tl2MarkerSet, hctx.reqTag)
	}
	if tl2MarkerNotLast {
		return fmt.Errorf("rpc: TL2 marker is not last wrapper for request tag #%08d; please report to infrastructure team", hctx.reqTag)
	}

	hctx.fillInvokeReqInternals(opts)
	return nil
}

func (hctx *HandlerContext) fillInvokeReqInternals(opts *ServerOptions) {
	hctx.noResult = hctx.RequestExtra.IsSetNoResult()
	hctx.requestExtraFieldsmask = hctx.RequestExtra.Flags
	hctx.traceIDStr = TraceIDToString(hctx.RequestExtra.TraceContext.TraceId)
	// We calculate timeout before handler can change hctx.RequestExtra
	if hctx.RequestExtra.CustomTimeoutMs > 0 { // implies hctx.RequestExtra.IsSetCustomTimeoutMs()
		// CustomTimeoutMs <= 0 means forever/maximum. TODO - harmonize condition with C engines
		customTimeout := time.Duration(hctx.RequestExtra.CustomTimeoutMs)*time.Millisecond - opts.ResponseTimeoutAdjust
		if customTimeout < time.Millisecond { // TODO - adjustable minimum timeout
			customTimeout = time.Millisecond
		}
		hctx.timeout = customTimeout
	} else {
		hctx.timeout = opts.DefaultResponseTimeout
	}
}

func (hctx *HandlerContext) prepareResponseBody(err error) error {
	resp := hctx.Response
	if err != nil {
		respErr := Error{}
		var respErr2 *Error
		switch {
		case err == ErrNoHandler: // this case is only to include reqTag into description
			respErr.Code = TlErrorNoHandler
			respErr.Description = fmt.Sprintf("RPC handler for #%08x not found", hctx.reqTag)
		case errors.As(err, &respErr2):
			respErr = *respErr2    // OK, forward the error as-is
			if respErr.Code == 0 { // important for rpc2.invokeReq, where Code != 0 is condition for error
				respErr.Code = TlErrorUnknown
			}
		case errors.Is(err, context.DeadlineExceeded):
			respErr.Code = TlErrorTimeout
			respErr.Description = fmt.Sprintf("%s (server-adjusted request timeout was %v)", err.Error(), hctx.timeout)
		default:
			respErr.Code = TlErrorUnknown
			respErr.Description = err.Error()
		}

		if hctx.noResult {
			hctx.commonConn.RareLog("rpc: failed to handle no_result query: #%v tag: #%08x method: %q error_code: %d error_text: %s", hctx.queryID, hctx.reqTag, hctx.RequestFunctionName, respErr.Code, respErr.Description)
			return nil
		}

		resp = resp[:0]
		// vkext compatibility hack instead of
		// packetTypeRPCReqError in packet header
		ret := tl.RpcReqResultError{
			QueryId:   hctx.queryID,
			ErrorCode: respErr.Code,
			Error:     respErr.Description,
		}
		// TODO - always reply with ReqError after we are sure vkext and kphp parse this correctly.
		// ret := tl.ReqError{
		//	ErrorCode: respErr.Code,
		//	Error:     respErr.Description,
		// }
		resp = ret.WriteBoxed(resp)
	}
	if hctx.noResult { // We do not care what is in Response, might be any trash
		return nil
	}
	if len(resp) == 0 {
		// Handler should return ErrNoHandler if it does not know how to return response
		hctx.commonConn.RareLog("rpc: handler returned empty response with no error query #%v tag #%08x (%s)", hctx.queryID, hctx.reqTag, hctx.RequestFunctionName)
	}
	hctx.extraStart = len(resp)
	rest := tl.RpcReqResultHeader{QueryId: hctx.queryID}
	resp = rest.Write(resp)
	hctx.ResponseExtra.Flags &= hctx.requestExtraFieldsmask // return only fields they understand
	if hctx.ResponseExtra.Flags != 0 {
		// extra := tl.ReqResultHeader{Extra: hctx.ResponseExtra}
		// resp = extra.WriteBoxed(resp)
		// we optimize copy of large extra here by writing code above manually
		resp = basictl.NatWrite(resp, tl.ReqResultHeader{}.TLTag())
		resp = hctx.ResponseExtra.Write(resp)
	}
	hctx.Response = resp
	return validBodyLen(len(resp))
}

func parseResponseExtra(extra *ResponseExtra, respBody []byte) (_ []byte, err error) {
	var tag uint32
	var afterTag []byte
	extraSet := 0
	for {
		if afterTag, err = basictl.NatRead(respBody, &tag); err != nil {
			return respBody, err
		}
		if (tag != tl.ReqResultHeader{}.TLTag()) {
			break
		}
		if respBody, err = extra.Read(afterTag); err != nil {
			return respBody, err
		}
		extraSet++
	}
	if extraSet > 1 {
		return respBody, fmt.Errorf("rpc: ResultExtra set more than once (%d) for result tag #%08d; please report to infrastructure team", extraSet, tag)
	}

	switch tag {
	case tl.ReqError{}.TLTag():
		var rpcErr tl.ReqError
		if respBody, err = rpcErr.Read(afterTag); err != nil {
			return respBody, err
		}
		return respBody, &Error{Code: rpcErr.ErrorCode, Description: rpcErr.Error}
	case tl.RpcReqResultError{}.TLTag(): // old style, should not be sent by modern servers
		var rpcErr tl.RpcReqResultError // ignore query ID
		if respBody, err = rpcErr.Read(afterTag); err != nil {
			return respBody, err
		}
		return respBody, &Error{Code: rpcErr.ErrorCode, Description: rpcErr.Error}
	case tl.RpcReqResultErrorWrapped{}.TLTag(): // old style, should not be sent by modern servers
		var rpcErr tl.RpcReqResultErrorWrapped
		if respBody, err = rpcErr.Read(afterTag); err != nil {
			return respBody, err
		}
		return respBody, &Error{Code: rpcErr.ErrorCode, Description: rpcErr.Error}
	}
	return respBody, nil
}
