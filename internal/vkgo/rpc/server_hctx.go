// Copyright 2022 V Kontakte LLC
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
)

// HandlerContext must not be used outside the handler
type HandlerContext struct {
	ActorID     uint64
	QueryID     int64
	RequestTime time.Time
	listenAddr  net.Addr
	localAddr   net.Addr
	remoteAddr  net.Addr
	keyID       [4]byte

	request  *[]byte // pointer for reuse. Holds allocated slice which will be put into sync pool, has always len 0
	Request  []byte
	response *[]byte // pointer for reuse. Holds allocated slice which will be put into sync pool, has always len 0
	Response []byte

	RequestExtra           InvokeReqExtra // every proxy adds bits it needs to client extra, sends it to server, then clears all bits in response so client can interpret all bits
	ResponseExtra          ReqResultExtra // everything we set here will be sent if client requested it (bit of RequestExtra.flags set)
	requestExtraFieldsmask uint32         // defensive copy

	// UserData allows caching common state between different requests.
	UserData any

	hooksState any

	serverConn     *serverConn
	reqHeader      packetHeader
	reqType        uint32 // actual request can be wrapped 0 or more times within reqHeader
	reqTaken       int
	respTaken      int
	respPacketType uint32
	noResult       bool  // defensive copy
	queryID        int64 // defensive copy
	defaultTimeout time.Duration
	timeoutAdjust  time.Duration
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

func (hctx *HandlerContext) KeyID() [4]byte       { return hctx.keyID }
func (hctx *HandlerContext) ListenAddr() net.Addr { return hctx.remoteAddr }
func (hctx *HandlerContext) LocalAddr() net.Addr  { return hctx.localAddr }
func (hctx *HandlerContext) RemoteAddr() net.Addr { return hctx.remoteAddr }

func (hctx *HandlerContext) timeout() time.Duration {
	timeout := hctx.defaultTimeout
	if hctx.RequestExtra.IsSetCustomTimeoutMs() {
		curTimeout := time.Duration(hctx.RequestExtra.CustomTimeoutMs) * time.Millisecond
		if timeout == hctx.defaultTimeout || timeout > curTimeout {
			timeout = curTimeout
		}
	}
	if timeout > hctx.timeoutAdjust {
		timeout -= hctx.timeoutAdjust
	}
	return timeout
}

func (hctx *HandlerContext) deadline() time.Time {
	timeout := hctx.timeout()
	if timeout != 0 {
		return hctx.RequestTime.Add(timeout)
	}
	return time.Time{}
}

func (hctx *HandlerContext) releaseRequest() {
	hctx.serverConn.server.releaseRequestBuf(hctx.reqTaken, hctx.request)
	hctx.reqTaken = 0
	hctx.request = nil
	hctx.Request = nil
}

func (hctx *HandlerContext) releaseResponse() {
	if hctx.response != nil {
		// if Response was reallocated and became too big, we will reuse original slice we got from pool
		// otherwise, we will move reallocated slice into slice in heap
		if cap(hctx.Response) <= hctx.serverConn.server.opts.ResponseBufSize {
			*hctx.response = hctx.Response[:0]
		}
	}
	hctx.serverConn.server.releaseResponseBuf(hctx.respTaken, hctx.response)
	hctx.respTaken = 0
	hctx.response = nil
	hctx.Response = nil
}

func (hctx *HandlerContext) reset() {
	// We do not preserve map in ResponseExtra because strings will not be reused anyway
	*hctx = HandlerContext{
		UserData: hctx.UserData,

		// HandlerContext is bound to the connection, so these are always valid
		serverConn: hctx.serverConn,
		listenAddr: hctx.listenAddr,
		localAddr:  hctx.localAddr,
		remoteAddr: hctx.remoteAddr,
		keyID:      hctx.keyID,
		hooksState: hctx.hooksState,
	}
}

func (hctx *HandlerContext) AccountResponseMem(respBodySizeEstimate int) error {
	var err error
	hctx.respTaken, err = hctx.serverConn.server.accountResponseMem(hctx.serverConn.closeCtx, hctx.respTaken, respBodySizeEstimate, false)
	return err
}

// HijackResponse releases Request bytes for reuse, so must be called only after Request processing is complete
// You must return result of this call from your handler
// After Hijack you are responsible for (whatever happens first)
// 1) forget about hctx if canceller method is called (so you must update your data structures strictly after call HijackResponse finished)
// 2) if 1) did not yet happen, can at any moment in the future call SendHijackedResponse
func (hctx *HandlerContext) HijackResponse(canceller HijackResponseCanceller) error {
	hctx.releaseRequest()
	hctx.serverConn.makeLongpollResponse(hctx, canceller)
	return errHijackResponse
}

func (hctx *HandlerContext) SendHijackedResponse(err error) {
	if debugPrint {
		fmt.Printf("longpollResponses send %d\n", hctx.queryID)
	}
	hctx.prepareResponse(err)
	hctx.serverConn.server.pushResponse(hctx, true)
}

func (hctx *HandlerContext) parseInvokeReq(s *Server) (err error) {
	if hctx.Request, err = basictl.LongRead(hctx.Request, &hctx.queryID); err != nil {
		return fmt.Errorf("failed to read request query ID: %w", err)
	}
	hctx.QueryID = hctx.queryID

	var tag uint32
	var afterTag []byte
	actorIDSet := 0
	extraSet := 0
	for {
		if afterTag, err = basictl.NatRead(hctx.Request, &tag); err != nil {
			return fmt.Errorf("failed to read tag: %w", err)
		}
		if tag != destActorFlagsTag && tag != destFlagsTag && tag != destActorTag {
			break
		}
		hctx.Request = afterTag
		if tag == destActorFlagsTag || tag == destActorTag {
			var actorID int64
			if hctx.Request, err = basictl.LongRead(hctx.Request, &actorID); err != nil {
				return fmt.Errorf("failed to read actor ID: %w", err)
			}
			if actorIDSet == 0 {
				hctx.ActorID = uint64(actorID)
			}
			actorIDSet++
		}
		if tag == destActorFlagsTag || tag == destFlagsTag {
			var extra InvokeReqExtra // do not reuse because client could possibly save slice/pointer
			if hctx.Request, err = extra.Read(hctx.Request); err != nil {
				return fmt.Errorf("failed to read request extra: %w", err)
			}
			if extraSet == 0 {
				hctx.RequestExtra = extra
			}
			extraSet++
		}
	}
	if actorIDSet > 1 || extraSet > 1 {
		s.rareLog(&s.lastDuplicateExtraLog, "rpc: ActorID or RequestExtra set more than once (%d and %d) for request tag #%08d; please report to infrastructure team", actorIDSet, extraSet, tag)
	}

	hctx.reqType = tag
	hctx.respPacketType = packetTypeRPCReqResult

	hctx.noResult = hctx.RequestExtra.IsSetNoResult()
	hctx.requestExtraFieldsmask = hctx.RequestExtra.flags
	return nil
}
