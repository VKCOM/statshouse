// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"bytes"
	"context"
	"encoding/binary"
	"net"
	"reflect"
	"strconv"
	"sync"
	"testing"

	"pgregory.net/rand"
	"pgregory.net/rapid"

	"github.com/vkcom/statshouse/internal/vkgo/basictl"
	"github.com/vkcom/statshouse/internal/vkgo/rpc/internal/gen/tl"
)

const (
	requestType = uint32(0x12345678)
)

var testCryptoKeys = []string{"test", "2", "1test"}

func handler(_ context.Context, hctx *HandlerContext) (err error) {
	hctx.Request, err = basictl.NatReadExactTag(hctx.Request, requestType)
	if err != nil {
		return err
	}

	n, b := binary.Varint(hctx.Request)
	if b <= 0 {
		return err
	}

	if n%7 == 0 {
		hctx.ResponseExtra.SetBinlogPos(n * 100)
	}

	if n%2 != 0 {
		rpcErr := &Error{
			Code:        int32(n),
			Description: strconv.Itoa(int(n)),
		}
		switch (n / 2) % 3 {
		case 0:
			return rpcErr // will serialize as tl.ReqError
		case 1:
			// serialize manually to check parsing correctness
			hctx.Response = basictl.NatWrite(hctx.Response, tl.RpcReqResultErrorWrapped{}.TLTag())
			hctx.Response = basictl.IntWrite(hctx.Response, rpcErr.Code)
			hctx.Response = basictl.StringWrite(hctx.Response, rpcErr.Description)
			return nil
		default:
			// serialize manually to check parsing correctness
			hctx.Response = basictl.NatWrite(hctx.Response, tl.RpcReqResultError{}.TLTag())
			hctx.Response = basictl.LongWrite(hctx.Response, n) // unused
			hctx.Response = basictl.IntWrite(hctx.Response, rpcErr.Code)
			hctx.Response = basictl.StringWrite(hctx.Response, rpcErr.Description)
			return nil
		}
	}

	buf := make([]byte, binary.MaxVarintLen64)
	buf = buf[:binary.PutVarint(buf, n)]
	hctx.Response = append(hctx.Response, bytes.Repeat(buf, 4)...)
	return nil
}

func dorequest(t *rapid.T, c *Client, addr string) {
	j := rand.Int()
	req := c.GetRequest()
	req.ActorID = int64(j % 2)
	if j%3 == 0 {
		req.Extra.SetIntForward(int64(j))
	}

	buf := make([]byte, binary.MaxVarintLen64)
	n := rand.Int31()
	buf = buf[:binary.PutVarint(buf, int64(n))]
	buf = append(make([]byte, 4), bytes.Repeat(buf, 4)...) // 4 bytes zero request type + hacky way to make sure request size is divisible by 4
	binary.LittleEndian.PutUint32(buf, requestType)
	req.Body = append(req.Body, buf...)

	resp, err := c.Do(context.Background(), "tcp4", addr, req)
	defer c.PutResponse(resp)

	if n%2 != 0 {
		refErr := &Error{
			Code:        n,
			Description: strconv.Itoa(int(n)),
		}

		if !reflect.DeepEqual(err, refErr) {
			t.Errorf("got error %q instead of %q", err, refErr)
		}
	} else if resp == nil || !bytes.Equal(buf[4:], resp.Body) {
		t.Errorf("sent %q, got back %v (%v)", buf, resp, err)
	}
}

func TestRPCRoundtrip(t *testing.T) {
	t.Parallel()

	// this is not really a property-based test, since it is not deterministic
	// however, biased integer generators from rapid are very convenient
	rapid.Check(t, testRPCRoundtrip)
}

func genClient(t *rapid.T) *Client {
	return NewClient(
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
		ServerWithMaxInflightPackets(rapid.IntRange(0, 3).Draw(t, "maxInflight")),
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
		go func(c *Client) {
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
