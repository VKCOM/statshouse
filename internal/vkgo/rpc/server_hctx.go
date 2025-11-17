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
	StartLongpoll(hctx *HandlerContext, canceller LongpollCanceller) (LongpollHandle, error)
	CancelLongpoll(queryID int64) (_ LongpollCanceller, deadline int64)
	FinishLongpoll(LongpollHandle) (*HandlerContext, error)
	DebugName() string
	SendLongpollResponse(hctx *HandlerContext, err error)
	SendEmptyResponse(lh LongpollHandle) // Prefer only one request instead of a batch here, because it's difficult to aggregate batches to different connections
	AccountResponseMem(hctx *HandlerContext, respBodySizeEstimate int) error
	ListenAddr() net.Addr
	LocalAddr() net.Addr
	RemoteAddr() net.Addr
	KeyID() [4]byte
	ProtocolVersion() uint32
	ProtocolTransportID() byte
	ConnectionID() uintptr
}

// typical Sizeofs: HandlerContext=536 LongpollHandle=24 longpollHctx=112

// common for longpoll and normal hctx
type handlerContextFields struct {
	// sorted for minimal structure size
	actorID             int64
	timeout             time.Duration // 0 means infinite, for this, both client and server must have infinite timeout
	requestTime         time.Time     // Longpoll handles are stored in the btree now, so we have to know the request time to be able to delete
	traceIDStr          string        // allocated after extra parsing
	requestFunctionName string        // Experimental. Generated handlers fill this during request processing.

	requestExtraFieldsmask uint32 // defensive copy
	reqTag                 uint32 // actual request can be wrapped 0 or more times within reqHeader
	protocolTransportID    byte   // copy from connection, we use it many times, virtual call is costly
	bodyFormatTL2          bool
	noResult               bool // defensive copy
	longpollStarted        bool // here to save 8 bytes in HandlerContext
}

// HandlerContext must not be used outside the handler, except after FinishLongpoll call, when it must be sent immediately
type HandlerContext struct {
	queryID    int64
	commonConn HandlerContextConnection

	request  *[]byte // pointer for reuse. Holds allocated slice which will be put into sync pool, has always len 0
	Request  []byte
	response *[]byte // pointer for reuse. Holds allocated slice which will be put into sync pool, has always len 0
	Response []byte

	extraStart int // We serialize extra after body into Body, then write into reversed order

	RequestExtra  RequestExtra  // every proxy adds bits it needs to client extra, sends it to server, then clears all bits in response so client can interpret all bits
	ResponseExtra ResponseExtra // everything we set here will be sent if client requested it (bit of RequestExtra.flags set)

	// UserData allows caching common state between different requests.
	UserData any

	reqTaken  int
	respTaken int

	handlerContextFields
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

func (hctx *HandlerContext) ActorID() int64              { return hctx.actorID }
func (hctx *HandlerContext) QueryID() int64              { return hctx.queryID }
func (hctx *HandlerContext) TraceIDStr() string          { return hctx.traceIDStr }
func (hctx *HandlerContext) RequestTag() uint32          { return hctx.reqTag } // First 4 bytes of request available even after request is freed
func (hctx *HandlerContext) KeyID() [4]byte              { return hctx.commonConn.KeyID() }
func (hctx *HandlerContext) ProtocolVersion() uint32     { return hctx.commonConn.ProtocolVersion() }
func (hctx *HandlerContext) ProtocolTransport() string   { return protocolName(hctx.protocolTransportID) }
func (hctx *HandlerContext) ListenAddr() net.Addr        { return hctx.commonConn.ListenAddr() }
func (hctx *HandlerContext) LocalAddr() net.Addr         { return hctx.commonConn.LocalAddr() }
func (hctx *HandlerContext) RemoteAddr() net.Addr        { return hctx.commonConn.RemoteAddr() }
func (hctx *HandlerContext) BodyFormatTL2() bool         { return hctx.bodyFormatTL2 }
func (hctx *HandlerContext) RequestTime() time.Time      { return hctx.requestTime }
func (hctx *HandlerContext) RequestFunctionName() string { return hctx.requestFunctionName }

// Used by middleware (tlgen) to avoid serializing response returned by handler.
func (hctx *HandlerContext) LongpollStarted() bool { return hctx.longpollStarted }

func (hctx *HandlerContext) SetRequestFunctionName(name string) { hctx.requestFunctionName = name }

// for implementing servers, also for tests with server mock ups
func (hctx *HandlerContext) ResetTo(
	commonConn HandlerContextConnection, queryID int64) {
	hctx.commonConn = commonConn
	hctx.protocolTransportID = commonConn.ProtocolTransportID()
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

// StartLongpoll releases Request bytes (and UserData) for reuse, so must be called only after Request processing is complete
// if this method returns err != nil, connection or server is in shutdown, long poll was not started, handle is empty,
// and you must not add it to your data structure. You can either return this error from your handler, or you can
// create empty response and return nil error, then empty response iwll be sent.
//
// After starting longpoll you are responsible for (whatever happens first)
// 1) forget about hctx if canceller method is called (so you must update your data structures strictly after call StartLongpoll finished)
// 2) if 1) did not yet happen, can at any moment in the future call FinishLongpoll(), and then SendLongpollResponse()
func (hctx *HandlerContext) StartLongpoll(canceller LongpollCanceller) (LongpollHandle, error) {
	return hctx.commonConn.StartLongpoll(hctx, canceller)
}

// This method allows users to set custom timeouts for long polls from a handler.
// Although this method exists, client code must respect timeouts from rpc.RequestExtra
// and shouldn't use this method.
// The method is going to be deleted once Persic migrates to default RPC timeouts instead of
// the custom timeout in request's tl body.
func (hctx *HandlerContext) StartLongpollWithTimeoutDeprecated(
	canceller LongpollCanceller,
	timeout time.Duration,
) (LongpollHandle, error) {
	hctx.timeout = timeout
	return hctx.commonConn.StartLongpoll(hctx, canceller)
}

// Be careful, it's responsibility of the caller to synchronize SendLongpollResponse and CancelLongpoll
func (hctx *HandlerContext) SendLongpollResponse(err error) {
	hctx.commonConn.SendLongpollResponse(hctx, err)
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
