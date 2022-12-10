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
	"net"
	"reflect"
	"strconv"
	"sync"
	"testing"

	"pgregory.net/rand"
	"pgregory.net/rapid"
)

func TestRPCMultiRoundtrip(t *testing.T) {
	t.Parallel()

	// this is not really a property-based test, since it is not deterministic
	// however, biased integer generators from rapid are very convenient
	rapid.Check(t, testRPCMultiRoundtrip)
}

func testRPCMultiRoundtrip(t *rapid.T) {
	ln, err := net.Listen("tcp4", listenAddr)
	if err != nil {
		t.Fatal(err)
	}

	clients := rapid.SliceOf(rapid.Custom(genClient)).Draw(t, "clients")
	numRequests := rapid.IntRange(1, 10).Draw(t, "numRequests")

	s := &Server{
		Handler:            handler,
		CryptoKeys:         testCryptoKeys,
		MaxConns:           rapid.IntRange(0, 3).Draw(t, "maxConns"),
		MaxWorkers:         rapid.IntRange(-1, 3).Draw(t, "maxWorkers"),
		MaxInflightPackets: rapid.IntRange(0, 3).Draw(t, "maxInflight"),
		ConnReadBufSize:    rapid.IntRange(0, 64).Draw(t, "connReadBufSize"),
		ConnWriteBufSize:   rapid.IntRange(0, 64).Draw(t, "connWriteBufSize"),
		RequestBufSize:     rapid.IntRange(512, 1024).Draw(t, "requestBufSize"),
		ResponseBufSize:    rapid.IntRange(512, 1024).Draw(t, "responseBufSize"),
	}

	serverErr := make(chan error)
	go func() {
		serverErr <- s.Serve(ln)
	}()

	var wg sync.WaitGroup
	for _, c := range clients {
		wg.Add(1)
		go func(c *Client) {
			defer wg.Done()

			m := c.Multi(numRequests)
			defer m.Close()

			requestIDs := map[int]uint64{}
			requestBufs := map[int][]byte{}

			rng := rand.New()
			for j := 0; j < numRequests; j++ {
				req := c.GetRequest()
				req.ActorID = uint64(j % 2)
				if j%3 == 0 {
					req.Extra.SetIntForward(int64(j))
				}

				buf := make([]byte, binary.MaxVarintLen64)
				n := (int64(j) << 32) | int64(rng.Int31())
				buf = buf[:binary.PutVarint(buf, n)]
				buf = append(make([]byte, 4), bytes.Repeat(buf, 4)...) // 4 bytes zero request type + hacky way to make sure request size is divisible by 4
				binary.LittleEndian.PutUint32(buf, requestType)
				req.Body = append(req.Body, buf...)

				err := m.Start(context.Background(), "tcp4", ln.Addr().String(), req, uint64(n))
				if err != nil {
					t.Errorf("failed to start request %v: %v", j, err)
				}

				requestIDs[j] = uint64(n)
				requestBufs[j] = buf
			}

			for k := 0; k < numRequests; k++ {
				var id uint64
				var resp *Response
				var err error
				if k%2 == 0 {
					for _, rID := range requestIDs {
						id = rID // get the first request ID from the map
						break
					}
					resp, err = m.Wait(context.Background(), id)
				} else {
					id, resp, err = m.WaitAny(context.Background())
				}

				j := int(id >> 32)
				buf := requestBufs[j]
				delete(requestIDs, j)

				n := int64(id)
				if n%2 != 0 {
					refErr := Error{
						Code:        int32(n),
						Description: strconv.Itoa(int(n)),
					}

					if !reflect.DeepEqual(err, refErr) {
						t.Errorf("got error %q instead of %q", err, refErr)
					}
				} else if resp == nil || !bytes.Equal(buf[4:], resp.Body) {
					t.Errorf("sent %q, got back %v (%v)", buf, resp, err)
				}

				if resp != nil {
					c.PutResponse(resp)
				}
			}

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
