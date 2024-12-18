// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/vkcom/statshouse/internal/vkgo/basictl"
	"github.com/vkcom/statshouse/internal/vkgo/rpc/internal/gen/tl"
)

// commonality between UDP and TCP servers requried for HandlerContext
type HandlerContextConnection interface {
	HijackResponse(hctx *HandlerContext, canceller HijackResponseCanceller) error
	SendHijackedResponse(hctx *HandlerContext, err error)
	AccountResponseMem(hctx *HandlerContext, respBodySizeEstimate int) error
	RareLog(format string, args ...any)
}

// HandlerContext must not be used outside the handler, except in HijackResponse (longpoll) protocol
type HandlerContext struct {
	ActorID     int64
	QueryID     int64
	RequestTime time.Time
	listenAddr  net.Addr
	localAddr   net.Addr
	remoteAddr  net.Addr

	keyID           [4]byte // encryption key prefix
	protocolID      int
	protocolVersion uint32

	request  *[]byte // pointer for reuse. Holds allocated slice which will be put into sync pool, has always len 0
	Request  []byte
	response *[]byte // pointer for reuse. Holds allocated slice which will be put into sync pool, has always len 0
	Response []byte

	extraStart int // We serialize extra after body into Body, then write into reversed order

	RequestExtra           RequestExtra  // every proxy adds bits it needs to client extra, sends it to server, then clears all bits in response so client can interpret all bits
	ResponseExtra          ResponseExtra // everything we set here will be sent if client requested it (bit of RequestExtra.flags set)
	requestExtraFieldsmask uint32        // defensive copy

	// UserData allows caching common state between different requests.
	UserData any

	RequestFunctionName string // Experimental. Generated handlers fill this during request processing.

	commonConn HandlerContextConnection
	reqTag     uint32 // actual request can be wrapped 0 or more times within reqHeader
	reqTaken   int
	respTaken  int
	noResult   bool  // defensive copy
	queryID    int64 // defensive copy

	timeout time.Duration // 0 means infinite, for this, both client and server must have infinite timeout
}

type handlerContextKey struct{}

// rpc.HandlerContext must never be used outside of the handler
func GetHandlerContext(ctx context.Context) *HandlerContext {
	hctx, _ := ctx.Value(handlerContextKey{}).(*HandlerContext)
	return hctx
}

func (hctx *HandlerContext) WithContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, handlerContextKey{}, hctx)
}

func (hctx *HandlerContext) RequestTag() uint32        { return hctx.reqTag } // First 4 bytes of request available even after request is freed
func (hctx *HandlerContext) KeyID() [4]byte            { return hctx.keyID }
func (hctx *HandlerContext) ProtocolVersion() uint32   { return hctx.protocolVersion }
func (hctx *HandlerContext) ProtocolTransport() string { return protocolName(hctx.protocolID) }
func (hctx *HandlerContext) ListenAddr() net.Addr      { return hctx.listenAddr }
func (hctx *HandlerContext) LocalAddr() net.Addr       { return hctx.localAddr }
func (hctx *HandlerContext) RemoteAddr() net.Addr      { return hctx.remoteAddr }

// for implementing servers, also for tests with server mock ups
func (hctx *HandlerContext) ResetTo(
	commonConn HandlerContextConnection, listenAddr net.Addr, localAddr net.Addr, remoteAddr net.Addr,
	keyID [4]byte, protocolVersion uint32, protocolID int) {
	hctx.commonConn = commonConn
	hctx.listenAddr = listenAddr
	hctx.localAddr = localAddr
	hctx.remoteAddr = remoteAddr
	hctx.keyID = keyID
	hctx.protocolVersion = protocolVersion
	hctx.protocolID = protocolID
}

func (hctx *HandlerContext) reset() {
	// We do not preserve map in ResponseExtra because strings will not be reused anyway
	// Also we do not reuse it because client could possibly save slice/pointer
	*hctx = HandlerContext{
		UserData: hctx.UserData,
	}
}

func (hctx *HandlerContext) AccountResponseMem(respBodySizeEstimate int) error {
	return hctx.commonConn.AccountResponseMem(hctx, respBodySizeEstimate)
}

// HijackResponse releases Request bytes (and UserData) for reuse, so must be called only after Request processing is complete
// You must return result of this call from your handler
// After Hijack you are responsible for (whatever happens first)
// 1) forget about hctx if canceller method is called (so you must update your data structures strictly after call HijackResponse finished)
// 2) if 1) did not yet happen, can at any moment in the future call SendHijackedResponse
func (hctx *HandlerContext) HijackResponse(canceller HijackResponseCanceller) error {
	return hctx.commonConn.HijackResponse(hctx, canceller)
}

// Be careful, it's responsibility of the caller to synchronize SendHijackedResponse and CancelHijack
func (hctx *HandlerContext) SendHijackedResponse(err error) {
	hctx.commonConn.SendHijackedResponse(hctx, err)
}

// This method is temporarily public, do not use directly
func (hctx *HandlerContext) ParseInvokeReq(opts *ServerOptions) (err error) {
	var reqHeader tl.RpcInvokeReqHeader
	if hctx.Request, err = reqHeader.Read(hctx.Request); err != nil {
		return fmt.Errorf("failed to read request query ID: %w", err)
	}
	hctx.queryID = reqHeader.QueryId
	hctx.QueryID = reqHeader.QueryId

	var afterTag []byte
	actorIDSet := 0
	extraSet := 0
loop:
	for {
		var tag uint32
		if afterTag, err = basictl.NatRead(hctx.Request, &tag); err != nil {
			return fmt.Errorf("failed to read tag: %w", err)
		}
		switch tag {
		case tl.RpcDestActor{}.TLTag():
			var extra tl.RpcDestActor
			if hctx.Request, err = extra.Read(afterTag); err != nil {
				return fmt.Errorf("failed to read rpcDestActor: %w", err)
			}
			hctx.ActorID = extra.ActorId
			actorIDSet++
		case tl.RpcDestFlags{}.TLTag():
			// var extra tl.RpcDestFlags
			// if hctx.Request, err = extra.Read(afterTag); err != nil {
			// here we optimize copy of large extra
			if hctx.Request, err = hctx.RequestExtra.Read(afterTag); err != nil {
				return fmt.Errorf("failed to read request rpcDestFlags: %w", err)
			}
			extraSet++
		case tl.RpcDestActorFlags{}.TLTag():
			// var extra tl.RpcDestActorFlags
			// if hctx.Request, err = extra.Read(afterTag); err != nil {
			// here we optimize copy of large extra
			if afterTag, err = basictl.LongRead(afterTag, &hctx.ActorID); err != nil {
				return fmt.Errorf("failed to read rpcDestActorFlags: %w", err)
			}
			if hctx.Request, err = hctx.RequestExtra.Read(afterTag); err != nil {
				return fmt.Errorf("failed to read rpcDestActorFlags: %w", err)
			}
			actorIDSet++
			extraSet++
		default:
			hctx.reqTag = tag
			break loop
		}
	}
	if actorIDSet > 1 || extraSet > 1 {
		return fmt.Errorf("rpc: ActorID or RequestExtra set more than once (%d and %d) for request tag #%08d; please report to infrastructure team", actorIDSet, extraSet, hctx.reqTag)
	}

	hctx.noResult = hctx.RequestExtra.IsSetNoResult()
	hctx.requestExtraFieldsmask = hctx.RequestExtra.Flags

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
	return nil
}

// We serialize extra after body into Body, then write into reversed order
// so full response is concatentation of hctx.Reponse[extraStart:], then hctx.Reponse[:extraStart]
func (hctx *HandlerContext) PrepareResponse(err error) (extraStart int) {
	if err = hctx.prepareResponseBody(err); err == nil {
		return hctx.extraStart
	}
	// Too large packet. Very rare.
	hctx.Response = hctx.Response[:0]
	if err = hctx.prepareResponseBody(err); err == nil {
		return hctx.extraStart
	}
	// err we passed above should be small, something is very wrong here
	panic("PrepareResponse with too large error is itself too large")
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
			respErr = *respErr2 // OK, forward the error as-is
		case errors.Is(err, context.DeadlineExceeded):
			respErr.Code = TlErrorTimeout
			respErr.Description = fmt.Sprintf("%s (server-adjusted request timeout was %v)", err.Error(), hctx.timeout)
		default:
			respErr.Code = TlErrorUnknown
			respErr.Description = err.Error()
		}

		if hctx.noResult {
			hctx.commonConn.RareLog("rpc: failed to handle no_result query #%v to 0x%x: %s", hctx.queryID, hctx.reqTag, respErr.Error())
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
		hctx.commonConn.RareLog("rpc: handler returned empty response with no error query #%v to 0x%x", hctx.queryID, hctx.reqTag)
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

func (hctx *HandlerContext) releaseRequest(s *Server) {
	s.releaseRequestBuf(hctx.reqTaken, hctx.request)
	hctx.reqTaken = 0
	hctx.request = nil
	hctx.Request = nil
}

func (hctx *HandlerContext) releaseResponse(s *Server) {
	if hctx.response != nil {
		// if Response was reallocated and became too big, we will reuse original slice we got from pool
		// otherwise, we will move reallocated slice into slice in heap
		if cap(hctx.Response) <= s.opts.ResponseBufSize {
			*hctx.response = hctx.Response
		}
	}
	s.releaseResponseBuf(hctx.respTaken, hctx.response)
	hctx.respTaken = 0
	hctx.response = nil
	hctx.Response = nil
}
