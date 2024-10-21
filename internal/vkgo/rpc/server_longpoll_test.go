// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"pgregory.net/rand"
	"pgregory.net/rapid"

	"github.com/vkcom/statshouse/internal/vkgo/basictl"
)

func TestLongpollServer(t *testing.T) {
	t.Parallel()

	// this is not really a property-based test, since it is not deterministic
	// however, biased integer generators from rapid are very convenient
	rapid.Check(t, testLongpollServer)
}

func testLongpollServer(t *rapid.T) {
	if debugPrint {
		fmt.Printf("---- TestLongpollServer\n")
	}
	ln, err := net.Listen("tcp4", "127.0.0.1:")
	if err != nil {
		t.Fatal(err)
	}

	// var clients []*Client
	clients := rapid.SliceOf(rapid.Custom(genClient)).Draw(t, "clients")
	if len(clients) == 0 {
		clients = append(clients, genClient(t))
	}
	clients = clients[:1]
	numRequests := rapid.IntRange(1, 10).Draw(t, "numRequests")

	ts := shutdownTestServer{clients: map[*HandlerContext]struct{}{}}
	s := NewServer(
		ServerWithSyncHandler(ts.testShutdownHandler),
		ServerWithMaxInflightPackets(numRequests+1), // for ping/cancel packet
		ServerWithCryptoKeys(testCryptoKeys),
		ServerWithMaxConns(len(clients)), //  rapid.IntRange(0, 3).Draw(t, "maxConns")
		ServerWithMaxWorkers(rapid.IntRange(-1, 3).Draw(t, "maxWorkers")),
		ServerWithConnReadBufSize(rapid.IntRange(0, 64).Draw(t, "connReadBufSize")),
		ServerWithConnWriteBufSize(rapid.IntRange(0, 64).Draw(t, "connWriteBufSize")),
		ServerWithRequestBufSize(rapid.IntRange(512, 1024).Draw(t, "requestBufSize")),
		ServerWithResponseBufSize(rapid.IntRange(512, 1024).Draw(t, "responseBufSize")),
	)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		if err := s.Serve(ln); err != nil {
			t.Fatal(err)
		}
		wg.Done()
	}()

	var sendWG sync.WaitGroup
	var receiveWG sync.WaitGroup
	var cancelMu sync.Mutex
	var cancelFuncs []func()

	sendWG.Add(len(clients) * numRequests)
	receiveWG.Add(len(clients) * numRequests)
	for _, c := range clients {
		go func(c *Client) {
			for j := 0; j < numRequests; j++ {
				go func() {
					n := rand.New().Int31()
					req := c.GetRequest()
					req.FailIfNoConnection = true
					req.Body = basictl.NatWrite(req.Body, requestType)
					req.Body = basictl.IntWrite(req.Body, n)

					ctx := context.Background()
					if n%2 == 0 {
						ctx2, cancel := context.WithCancel(context.Background())
						ctx = ctx2
						cancelMu.Lock()
						cancelFuncs = append(cancelFuncs, cancel)
						cancelMu.Unlock()
					}
					sendWG.Done()
					resp, _ := c.Do(ctx, "tcp4", ln.Addr().String(), req)
					defer c.PutResponse(resp)
					receiveWG.Done()
				}()
			}
		}(c)
	}
	time.Sleep(10 * time.Millisecond) // bad
	sendWG.Wait()                     // everything sent
	if debugPrint {
		fmt.Printf("everything sent\n")
	}
	for _, c := range cancelFuncs {
		c()
	}
	ts.sendSomeResponses()
	time.Sleep(20 * time.Millisecond) // bad
	s.Shutdown()
	receiveWG.Wait()
	if debugPrint {
		fmt.Printf("everything received\n")
	}
	for _, c := range clients {
		_ = c.Close()
	}
	wg.Wait()

	err = s.Close()
	if err != nil {
		t.Fatal(err)
	}
	if len(ts.clients) != 0 {
		t.Fatal("long poll contexts did not clear in server")
	}
}
