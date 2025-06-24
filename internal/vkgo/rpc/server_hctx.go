// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"context"
	"net"
	"time"
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
	actorID     int64
	queryID     int64
	RequestTime time.Time
	listenAddr  net.Addr
	localAddr   net.Addr
	remoteAddr  net.Addr

	keyID           [4]byte // encryption key prefix
	protocolVersion uint32
	protocolID      int

	request  *[]byte // pointer for reuse. Holds allocated slice which will be put into sync pool, has always len 0
	Request  []byte
	response *[]byte // pointer for reuse. Holds allocated slice which will be put into sync pool, has always len 0
	Response []byte

	extraStart int // We serialize extra after body into Body, then write into reversed order

	RequestExtra           RequestExtra  // every proxy adds bits it needs to client extra, sends it to server, then clears all bits in response so client can interpret all bits
	ResponseExtra          ResponseExtra // everything we set here will be sent if client requested it (bit of RequestExtra.flags set)
	requestExtraFieldsmask uint32        // defensive copy
	traceIDStr             string        // allocated after extra parsing
	bodyFormatTL2          bool

	// UserData allows caching common state between different requests.
	UserData any

	RequestFunctionName string // Experimental. Generated handlers fill this during request processing.

	commonConn HandlerContextConnection
	reqTaken   int
	respTaken  int
	reqTag     uint32 // actual request can be wrapped 0 or more times within reqHeader
	noResult   bool   // defensive copy

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

func (hctx *HandlerContext) ActorID() int64            { return hctx.actorID }
func (hctx *HandlerContext) QueryID() int64            { return hctx.queryID }
func (hctx *HandlerContext) TraceIDStr() string        { return hctx.traceIDStr }
func (hctx *HandlerContext) RequestTag() uint32        { return hctx.reqTag } // First 4 bytes of request available even after request is freed
func (hctx *HandlerContext) KeyID() [4]byte            { return hctx.keyID }
func (hctx *HandlerContext) ProtocolVersion() uint32   { return hctx.protocolVersion }
func (hctx *HandlerContext) ProtocolTransport() string { return protocolName(hctx.protocolID) }
func (hctx *HandlerContext) ListenAddr() net.Addr      { return hctx.listenAddr }
func (hctx *HandlerContext) LocalAddr() net.Addr       { return hctx.localAddr }
func (hctx *HandlerContext) RemoteAddr() net.Addr      { return hctx.remoteAddr }
func (hctx *HandlerContext) BodyFormatTL2() bool       { return hctx.bodyFormatTL2 }

// for implementing servers, also for tests with server mock ups
func (hctx *HandlerContext) ResetTo(
	commonConn HandlerContextConnection, listenAddr net.Addr, localAddr net.Addr, remoteAddr net.Addr,
	keyID [4]byte, protocolVersion uint32, protocolID int, queryID int64) {
	hctx.commonConn = commonConn
	hctx.listenAddr = listenAddr
	hctx.localAddr = localAddr
	hctx.remoteAddr = remoteAddr
	hctx.keyID = keyID
	hctx.protocolVersion = protocolVersion
	hctx.protocolID = protocolID
	hctx.queryID = queryID
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

// We serialize extra after body into Body, then write into reversed order
// so full response is concatenation of hctx.Reponse[extraStart:], then hctx.Reponse[:extraStart]
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
