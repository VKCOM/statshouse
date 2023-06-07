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

	"pgregory.net/rapid"
)

func TestRPCMultiRoundtrip(t *testing.T) {
	t.Parallel()

	// this is not really a property-based test, since it is not deterministic
	// however, biased integer generators from rapid are very convenient
	rapid.Check(t, testRPCMultiRoundtrip)
}

func testRPCMultiRoundtrip(t *rapid.T) {
	ln, err := net.Listen("tcp4", "127.0.0.1:")
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

			m := c.Multi(numRequests)
			defer m.Close()

			queryIDs := map[int64]struct{}{}
			queryIDToRequestBuf := map[int64][]byte{}

			for j := 0; j < numRequests; j++ {
				req := c.GetRequest()
				req.ActorID = uint64(j % 2)
				if j%3 == 0 {
					req.Extra.SetIntForward(int64(j))
				}

				buf := make([]byte, binary.MaxVarintLen64)
				queryID := req.QueryID()
				buf = buf[:binary.PutVarint(buf, queryID)]
				buf = append(make([]byte, 4), bytes.Repeat(buf, 4)...) // 4 bytes zero request type + hacky way to make sure request size is divisible by 4
				binary.LittleEndian.PutUint32(buf, requestType)
				req.Body = append(req.Body, buf...)

				err := m.Start(context.Background(), "tcp4", ln.Addr().String(), req)
				if err != nil {
					t.Errorf("failed to start request %v: %v", j, err)
				}

				queryIDs[queryID] = struct{}{}
				queryIDToRequestBuf[queryID] = buf
			}

			for k := 0; k < numRequests; k++ {
				var queryID int64
				var resp *Response
				var err error
				if k%2 == 0 {
					for qID := range queryIDs {
						queryID = qID // get the first request ID from the map
						break
					}
					resp, err = m.Wait(context.Background(), queryID)
				} else {
					queryID, resp, err = m.WaitAny(context.Background())
				}

				buf := queryIDToRequestBuf[queryID]
				delete(queryIDToRequestBuf, queryID)
				delete(queryIDs, queryID)

				if queryID%2 != 0 {
					refErr := Error{
						Code:        int32(queryID),
						Description: strconv.Itoa(int(queryID)),
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
