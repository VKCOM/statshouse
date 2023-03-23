// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"reflect"
	"strconv"
	"sync"
	"testing"

	"pgregory.net/rand"
	"pgregory.net/rapid"

	"github.com/vkcom/statshouse/internal/vkgo/basictl"
)

const (
	listenAddr  = "127.0.0.1:" // automatic port selection
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
		var extra ReqResultExtra
		extra.SetBinlogPos(n * 100)
		// w := &bytes.Buffer{}
		// if err = extra.writeToBytesBuffer(w); err != nil {
		//	return fmt.Errorf("error writing extra: %w", err)
		// }
		// hctx.Response = basictl.NatWrite(hctx.Response, reqResultHeaderTag)
		// hctx.Response = basictl.NatWrite(hctx.Response, extra.flags)
		// hctx.Response = append(hctx.Response, w.Bytes()...)

		hctx.Response = basictl.NatWrite(hctx.Response, reqResultHeaderTag)
		hctx.Response, err = extra.Write(hctx.Response)
		if err != nil {
			return fmt.Errorf("error writing extra: %w", err)
		}
	}

	if n%2 != 0 {
		rpcErr := Error{
			Code:        int32(n),
			Description: strconv.Itoa(int(n)),
		}
		switch (n / 2) % 3 {
		case 0:
			return rpcErr
		case 1:
			hctx.Response = basictl.NatWrite(hctx.Response, reqResultErrorWrappedTag)
			hctx.Response = basictl.IntWrite(hctx.Response, rpcErr.Code)
			hctx.Response = basictl.StringWriteTruncated(hctx.Response, rpcErr.Description)
			return nil
		default:
			hctx.Response = basictl.NatWrite(hctx.Response, reqResultErrorTag)
			hctx.Response = basictl.LongWrite(hctx.Response, n) // unused
			hctx.Response = basictl.IntWrite(hctx.Response, rpcErr.Code)
			hctx.Response = basictl.StringWriteTruncated(hctx.Response, rpcErr.Description)
			return nil
		}
	}

	buf := make([]byte, binary.MaxVarintLen64)
	buf = buf[:binary.PutVarint(buf, n)]
	hctx.Response = append(hctx.Response, bytes.Repeat(buf, 4)...)
	return nil
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

func testRPCRoundtrip(t *rapid.T) {
	ln, err := net.Listen("tcp4", listenAddr)
	if err != nil {
		t.Fatal(err)
	}

	clients := rapid.SliceOf(rapid.Custom(genClient)).Draw(t, "clients")
	numRequests := rapid.IntRange(1, 10).Draw(t, "numRequests")

	s := NewServer(
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

	serverErr := make(chan error)
	go func() {
		serverErr <- s.Serve(ln)
	}()

	var wg sync.WaitGroup
	for _, c := range clients {
		wg.Add(1)
		go func(c *Client) {
			defer wg.Done()

			var cwg sync.WaitGroup
			for j := 0; j < numRequests; j++ {
				cwg.Add(1)
				go func(j int) {
					defer cwg.Done()

					req := c.GetRequest()
					req.ActorID = uint64(j % 2)
					if j%3 == 0 {
						req.Extra.SetIntForward(int64(j))
					}

					buf := make([]byte, binary.MaxVarintLen64)
					n := rand.New().Int31()
					buf = buf[:binary.PutVarint(buf, int64(n))]
					buf = append(make([]byte, 4), bytes.Repeat(buf, 4)...) // 4 bytes zero request type + hacky way to make sure request size is divisible by 4
					binary.LittleEndian.PutUint32(buf, requestType)
					req.Body = append(req.Body, buf...)

					resp, err := c.Do(context.Background(), "tcp4", ln.Addr().String(), req)
					if resp != nil {
						defer c.PutResponse(resp)
					}

					if n%2 != 0 {
						refErr := Error{
							Code:        n,
							Description: strconv.Itoa(int(n)),
						}

						if !reflect.DeepEqual(err, refErr) {
							t.Errorf("got error %q instead of %q", err, refErr)
						}
					} else if resp == nil || !bytes.Equal(buf[4:], resp.Body) {
						t.Errorf("sent %q, got back %v (%v)", buf, resp, err)
					}
				}(j)
			}

			cwg.Wait()

			err := c.Close()
			if err != nil {
				t.Errorf("failed to close client: %v", err)
			}
		}(c)
	}

	wg.Wait()

	err = s.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = <-serverErr
	if err != ErrServerClosed {
		t.Fatal(err)
	}
}
