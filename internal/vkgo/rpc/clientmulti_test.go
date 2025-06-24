// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"context"
	"net"
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
		go func(c Client) {
			defer wg.Done()

			m := c.Multi(numRequests)
			defer m.Close()

			queryIDs := map[int64]struct{}{}
			queryIDToBodyCopy := map[int64]string{}

			for j := 0; j < numRequests; j++ {
				req := c.GetRequest()
				queryID := req.QueryID()
				bodyCopy := prepareTestRequest(req)

				err := m.Start(context.Background(), "tcp4", ln.Addr().String(), req)
				if err != nil {
					t.Errorf("failed to start request %v: %v", j, err)
				}

				queryIDs[queryID] = struct{}{}
				queryIDToBodyCopy[queryID] = bodyCopy
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

				bodyCopy := queryIDToBodyCopy[queryID]
				delete(queryIDToBodyCopy, queryID)
				delete(queryIDs, queryID)
				checkTestResponse(t, resp, err, bodyCopy)
				c.PutResponse(resp)
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
	if err != nil {
		t.Fatal(err)
	}
}
