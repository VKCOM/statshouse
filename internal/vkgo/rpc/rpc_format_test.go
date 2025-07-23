// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"encoding/hex"
	"testing"

	"github.com/VKCOM/statshouse/internal/vkgo/rpc/internal/gen/tl"
)

func testRPCRequestRoundTrip(t *testing.T, req *Request, mustBeBody string) {
	if err := preparePacket(req); err != nil {
		t.Error(err)
	}
	body := append(append([]byte(nil), req.Body[req.extraStart:]...), req.Body[:req.extraStart]...)
	req.Body = req.Body[:req.extraStart]
	if hex.EncodeToString(body) != mustBeBody {
		t.Fatalf("not equal body %x", body)
	}

	opts := &ServerOptions{}
	hctx := &HandlerContext{
		Request: body,
	}
	if err := hctx.ParseInvokeReq(opts); err != nil {
		t.Error(err)
	}
	if hctx.QueryID() != req.QueryID() {
		t.Fatalf("not equal query id")
	}
	if hctx.ActorID() != req.ActorID {
		t.Fatalf("not equal actor id")
	}
	if hctx.RequestExtra.String() != req.Extra.String() {
		t.Fatalf("not equal extra")
	}
}

func testRPCResponseRoundTrip(t *testing.T, hctx *HandlerContext, sendErr error, mustBeBody string) {
	hctx.ResponseExtra.SetBinlogPos(555) // test that extra fields not delivered
	hctx.ResponseExtra.SetViewNumber(777)
	if err := hctx.prepareResponseBody(sendErr); err != nil {
		t.Error(err)
	}
	body := append(append([]byte(nil), hctx.Response[hctx.extraStart:]...), hctx.Response[:hctx.extraStart]...)
	hctx.Response = hctx.Response[:hctx.extraStart]
	if hex.EncodeToString(body) != mustBeBody {
		t.Fatalf("not equal body %x", body)
	}

	var header tl.RpcReqResultHeader
	var err error
	if body, err = header.Read(body); err != nil {
		t.Error(err)
	}

	var extra ResponseExtra
	if _, err = parseResponseExtra(&extra, body); err != nil && err.Error() != sendErr.Error() {
		t.Error(err)
	}
	if hctx.QueryID() != header.QueryId {
		t.Fatalf("not equal query id")
	}
	if a, b := hctx.ResponseExtra.String(), extra.String(); a != b {
		t.Fatalf("not equal extra %s\n%s", a, b)
	}
}

// Simple regression test
func TestRPCFormat(t *testing.T) {
	req := &Request{
		Body:         []byte{0xaa, 0xbb, 0xcc, 0xdd},
		FunctionName: "memcache.Get",
		queryID:      222,
	}
	testRPCRequestRoundTrip(t, req, "de00000000000000aabbccdd")
	testRPCRequestRoundTrip(t, req, "de00000000000000aabbccdd")
	req.ActorID = 111
	testRPCRequestRoundTrip(t, req, "de00000000000000bdaa68756f00000000000000aabbccdd")
	req.ActorID = 0
	req.Extra.SetCustomTimeoutMs(255)
	req.Extra.SetReturnViewNumber(true)
	requestExtraFieldsmask := req.Extra.Flags
	testRPCRequestRoundTrip(t, req, "de000000000000005e0352e300008008ff000000aabbccdd")
	req.ActorID = 111
	testRPCRequestRoundTrip(t, req, "de00000000000000f7aca5f06f0000000000000000008008ff000000aabbccdd")

	hctx := &HandlerContext{
		queryID:                222,
		Response:               []byte{0xa1, 0xb2, 0xc3, 0xd4},
		requestExtraFieldsmask: requestExtraFieldsmask,
	}
	testRPCResponseRoundTrip(t, hctx, nil, "de00000000000000e14cc88c0000000800000000000000000903000000000000a1b2c3d4")
	testRPCResponseRoundTrip(t, hctx, &Error{Code: 444, Description: "bad"}, "de00000000000000e14cc88c0000000800000000000000000903000000000000f532e47ade00000000000000bc01000003626164")
}
