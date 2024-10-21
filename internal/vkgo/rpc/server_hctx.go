// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"context"
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

type ServerRequest struct {
	ActorID     int64
	QueryID     int64
	RequestTime time.Time
	Request     []byte
	Response    []byte

	extraStart int // We serialize extra after body into Body, then write into reversed order

	RequestExtra           RequestExtra  // every proxy adds bits it needs to client extra, sends it to server, then clears all bits in response so client can interpret all bits
	ResponseExtra          ResponseExtra // everything we set here will be sent if client requested it (bit of RequestExtra.flags set)
	requestExtraFieldsmask uint32        // defensive copy

	reqTag   uint32 // actual request can be wrapped 0 or more times within reqHeader
	noResult bool   // defensive copy

	timeout time.Duration // 0 means infinite, for this, both client and server must have infinite timeout
}

// HandlerContext must not be used outside the handler, except in HijackResponse (longpoll) protocol
type HandlerContext struct {
	ServerRequest
	listenAddr net.Addr
	localAddr  net.Addr
	remoteAddr net.Addr

	keyID             [4]byte // encryption key prefix
	protocolTransport string
	protocolVersion   uint32

	request  *[]byte // pointer for reuse. Holds allocated slice which will be put into sync pool, has always len 0
	response *[]byte // pointer for reuse. Holds allocated slice which will be put into sync pool, has always len 0

	// UserData allows caching common state between different requests.
	UserData any

	RequestFunctionName string // Experimental. Generated handlers fill this during request processing.

	commonConn HandlerContextConnection
	reqTaken   int
	respTaken  int
	queryID    int64 // defensive copy
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
func (hctx *HandlerContext) ProtocolTransport() string { return hctx.protocolTransport }
func (hctx *HandlerContext) ListenAddr() net.Addr      { return hctx.listenAddr }
func (hctx *HandlerContext) LocalAddr() net.Addr       { return hctx.localAddr }
func (hctx *HandlerContext) RemoteAddr() net.Addr      { return hctx.remoteAddr }

// for implementing servers, also for tests with server mock ups
func (hctx *HandlerContext) ResetTo(
	commonConn HandlerContextConnection, listenAddr net.Addr, localAddr net.Addr, remoteAddr net.Addr,
	keyID [4]byte, protocolVersion uint32, protocolTransport string,
	options *ServerOptions) {
	hctx.commonConn = commonConn
	hctx.listenAddr = listenAddr
	hctx.localAddr = localAddr
	hctx.remoteAddr = remoteAddr
	hctx.keyID = keyID
	hctx.protocolVersion = protocolVersion
	hctx.protocolTransport = protocolTransport
}

func (hctx *HandlerContext) reset() {
	// We do not preserve map in ResponseExtra because strings will not be reused anyway
	// Also we do not reuse because client could possibly save slice/pointer
	*hctx = HandlerContext{
		UserData: hctx.UserData,

		// HandlerContext is bound to the connection, so these are always valid
		commonConn:        hctx.commonConn,
		listenAddr:        hctx.listenAddr,
		localAddr:         hctx.localAddr,
		remoteAddr:        hctx.remoteAddr,
		keyID:             hctx.keyID,
		protocolTransport: hctx.protocolTransport,
		protocolVersion:   hctx.protocolVersion,
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
	if err = hctx.ServerRequest.ParseInvokeReq(opts); err != nil {
		return err
	}
	hctx.queryID = hctx.QueryID
	return nil
}

func (hctx *ServerRequest) ParseInvokeReq(opts *ServerOptions) (err error) {
	var reqHeader tl.RpcInvokeReqHeader
	if hctx.Request, err = reqHeader.Read(hctx.Request); err != nil {
		return fmt.Errorf("failed to read request query ID: %w", err)
	}
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

func (hctx *ServerRequest) RequestTag() uint32 { return hctx.reqTag }
