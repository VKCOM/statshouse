// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"context"
	"encoding/binary"
	"net"
	"reflect"
	"strconv"
	"sync"
	"testing"

	"pgregory.net/rand"
	"pgregory.net/rapid"

	"github.com/VKCOM/statshouse/internal/vkgo/basictl"
	"github.com/VKCOM/statshouse/internal/vkgo/rpc/internal/gen/tl"
)

const (
	testRequestType   = uint32(0x12345678)
	testRequestMethod = "test.request"
)

var testCryptoKeys = []string{"test-crypto-key-crypto-key-crypto-key", "2-crypto-key-crypto-key-crypto-key", "1test-crypto-key-crypto-key-crypto-key"}

func handler(_ context.Context, hctx *HandlerContext) (err error) {
	bodyCopy := string(hctx.Request)
	hctx.Request, err = basictl.NatReadExactTag(hctx.Request, testRequestType)
	if err != nil {
		return err
	}

	var n uint32
	if hctx.bodyFormatTL2 { // pretend we have variation in format
		var sz int
		hctx.Request, err = basictl.TL2ReadSize(hctx.Request, &sz)
		n = uint32(sz)
	} else {
		hctx.Request, err = basictl.NatRead(hctx.Request, &n)
	}
	if err != nil {
		return err
	}

	if n%7 == 0 {
		hctx.ResponseExtra.SetBinlogPos(int64(n) * 100)
	}

	if n%2 != 0 {
		rpcErr := &Error{
			Code:        int32(n),
			Description: strconv.Itoa(int(n)),
		}
		switch (n / 2) % 3 {
		case 0:
			if n/6%2 == 0 {
				return rpcErr // will serialize by default
			}
			// serialize manually to check parsing correctness
			hctx.Response = basictl.NatWrite(hctx.Response, tl.ReqError{}.TLTag())
			hctx.Response = basictl.IntWrite(hctx.Response, rpcErr.Code)
			hctx.Response = basictl.StringWrite(hctx.Response, rpcErr.Description)
			return nil
		case 1:
			// serialize manually to check parsing correctness
			hctx.Response = basictl.NatWrite(hctx.Response, tl.RpcReqResultErrorWrapped{}.TLTag())
			hctx.Response = basictl.IntWrite(hctx.Response, rpcErr.Code)
			hctx.Response = basictl.StringWrite(hctx.Response, rpcErr.Description)
			return nil
		default:
			// serialize manually to check parsing correctness
			hctx.Response = basictl.NatWrite(hctx.Response, tl.RpcReqResultError{}.TLTag())
			hctx.Response = basictl.LongWrite(hctx.Response, 0) // unused
			hctx.Response = basictl.IntWrite(hctx.Response, rpcErr.Code)
			hctx.Response = basictl.StringWrite(hctx.Response, rpcErr.Description)
			return nil
		}
	}
	hctx.Response = append(hctx.Response, bodyCopy...)
	return nil
}

func prepareTestRequest(req *Request) string {
	req.FunctionName = testRequestMethod
	j := rand.Int63()
	if j%2 == 0 {
		req.ActorID = j
	}
	if (j/2)%2 == 0 {
		req.Extra.SetIntForward(j)
	}
	n := uint32(req.QueryID())

	if (j/4)%2 == 0 {
		req.BodyFormatTL2 = true
	}
	req.Body = binary.LittleEndian.AppendUint32(req.Body, testRequestType)
	if req.BodyFormatTL2 { // pretend we have variation in format
		req.Body = basictl.TL2WriteSize(req.Body, int(n))
	} else {
		req.Body = binary.LittleEndian.AppendUint32(req.Body, n)
	}
	req.Body = append(req.Body, "body"...)
	return string(req.Body)
}

func checkTestResponse(t *rapid.T, resp *Response, err error, bodyCopy string) {
	n := uint32(resp.QueryID())
	if n%2 != 0 {
		refErr := &Error{
			Code:        int32(n),
			Description: strconv.Itoa(int(n)),
		}

		if !reflect.DeepEqual(err, refErr) {
			t.Errorf("got error %q instead of %q", err, refErr)
		}
	} else if resp == nil || bodyCopy != string(resp.Body) {
		t.Errorf("sent %q, got back %v (%v)", bodyCopy, resp, err)
	}
}

func dorequest(t *rapid.T, c Client, addr string) {
	req := c.GetRequest()
	bodyCopy := prepareTestRequest(req)
	resp, err := c.Do(context.Background(), "tcp4", addr, req)
	defer c.PutResponse(resp)
	checkTestResponse(t, resp, err, bodyCopy)
}

func TestRPCRoundtrip(t *testing.T) {
	t.Parallel()

	// this is not really a property-based test, since it is not deterministic
	// however, biased integer generators from rapid are very convenient
	rapid.Check(t, testRPCRoundtrip)
}

func genClient(t *rapid.T) Client {
	return NewClient(
		ClientWithProtocolVersion(LatestProtocolVersion),
		ClientWithForceEncryption(rapid.Bool().Draw(t, "forceEncryption")),
		ClientWithCryptoKey(testCryptoKeys[rapid.IntRange(0, 2).Draw(t, "cryptoKeyIndex")]),
		ClientWithConnReadBufSize(rapid.IntRange(0, 64).Draw(t, "connReadBufSize")),
		ClientWithConnWriteBufSize(rapid.IntRange(0, 64).Draw(t, "connWriteBufSize")),
	)
}

func genServer(t *rapid.T) *Server {
	return NewServer(
		ServerWithHandler(handler),
		ServerWithCryptoKeys(testCryptoKeys),
		ServerWithMaxConns(rapid.IntRange(0, 3).Draw(t, "maxConns")),
		ServerWithMaxWorkers(rapid.IntRange(-1, 3).Draw(t, "maxWorkers")),
		ServerWithConnReadBufSize(rapid.IntRange(0, 64).Draw(t, "connReadBufSize")),
		ServerWithConnWriteBufSize(rapid.IntRange(0, 64).Draw(t, "connWriteBufSize")),
		ServerWithRequestBufSize(rapid.IntRange(512, 1024).Draw(t, "requestBufSize")),
		ServerWithResponseBufSize(rapid.IntRange(512, 1024).Draw(t, "responseBufSize")),
	)
}

func testRPCRoundtrip(t *rapid.T) {
	ln, err := net.Listen("tcp4", "127.0.0.1:")
	if err != nil {
		t.Fatal(err)
	}

	clients := rapid.SliceOf(rapid.Custom(genClient)).Draw(t, "clients")
	numRequests := rapid.IntRange(1, 10).Draw(t, "numRequests")

	s := genServer(t)

	serverErr := make(chan error, 1)
	var wg sync.WaitGroup
	go func() {
		serverErr <- s.Serve(ln)
	}()

	for _, c := range clients {
		wg.Add(1)
		go func(c Client) {
			defer wg.Done()

			var cwg sync.WaitGroup
			for j := 0; j < numRequests; j++ {
				cwg.Add(1)
				go func(j int) {
					defer cwg.Done()

					dorequest(t, c, ln.Addr().String())
				}(j)
			}

			cwg.Wait()

			err := c.Close()
			if err != nil {
				t.Errorf("failed to close client: %v", err)
			}
		}(c)
	}
	// s.Shutdown()
	// ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	// defer cancel()
	// _ = s.CloseWait(ctx)

	wg.Wait()

	err = s.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = <-serverErr
	if err != nil {
		t.Fatal(err)
	}
}
