// Copyright 2024 V Kontakte LLC
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
	"time"

	"pgregory.net/rand"
	"pgregory.net/rapid"

	"github.com/vkcom/statshouse/internal/vkgo/basictl"
)

type shutdownTestServer struct {
	mu      sync.Mutex
	clients map[*HandlerContext]struct{}
}

func (s *shutdownTestServer) CancelHijack(hctx *HandlerContext) {
	s.mu.Lock()
	delete(s.clients, hctx)
	s.mu.Unlock()
}

func (s *shutdownTestServer) sendSomeResponses() {
	s.mu.Lock()
	counter := 0
	for hctxToSend := range s.clients {
		delete(s.clients, hctxToSend)
		hctxToSend.SendHijackedResponse(nil)
		counter++
		if counter >= len(s.clients) {
			break // approx. half sent
		}
	}
	s.mu.Unlock()
}

func (s *shutdownTestServer) testShutdownHandler(_ context.Context, hctx *HandlerContext) (err error) {
	if hctx.Request, err = basictl.NatReadExactTag(hctx.Request, requestType); err != nil {
		return err
	}
	var n int32
	if hctx.Request, err = basictl.IntRead(hctx.Request, &n); err != nil {
		return err
	}

	hctx.Response = basictl.IntWrite(hctx.Response, n)
	if (n/2)%2 == 0 {
		s.mu.Lock()
		s.clients[hctx] = struct{}{}
		s.mu.Unlock()
		return hctx.HijackResponse(s)
	}
	return nil
}

func TestShutdownClient(t *testing.T) {
	t.Parallel()

	// this is not really a property-based test, since it is not deterministic
	// however, biased integer generators from rapid are very convenient
	rapid.Check(t, testShutdownClient)
}

func testShutdownClient(t *rapid.T) {
	ln, err := net.Listen("tcp4", "127.0.0.1:")
	if err != nil {
		t.Fatal(err)
	}

	clients := rapid.SliceOf(rapid.Custom(genClient)).Draw(t, "clients")
	if len(clients) == 0 {
		clients = append(clients, genClient(t))
	}
	clients = clients[:1]
	numRequests := rapid.IntRange(1, 10).Draw(t, "numRequests")

	ts := shutdownTestServer{clients: map[*HandlerContext]struct{}{}}
	s := NewServer(
		ServerWithSyncHandler(ts.testShutdownHandler),
		ServerWithCryptoKeys(testCryptoKeys),
		ServerWithMaxConns(rapid.IntRange(0, 3).Draw(t, "maxConns")),
		ServerWithMaxWorkers(rapid.IntRange(-1, 3).Draw(t, "maxWorkers")),
		ServerWithMaxInflightPackets(numRequests+1), // some requests will long poll, so ping/cancel packet must fit
		ServerWithConnReadBufSize(rapid.IntRange(0, 64).Draw(t, "connReadBufSize")),
		ServerWithConnWriteBufSize(rapid.IntRange(0, 64).Draw(t, "connWriteBufSize")),
		ServerWithRequestBufSize(rapid.IntRange(512, 1024).Draw(t, "requestBufSize")),
		ServerWithResponseBufSize(rapid.IntRange(512, 1024).Draw(t, "responseBufSize")),
	)

	serverErrChan := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		serverErr := s.Serve(ln)
		serverErrChan <- serverErr
		wg.Done()
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

					n := rand.New().Int31()
					req := c.GetRequest()
					req.Body = basictl.NatWrite(req.Body, requestType)
					req.Body = basictl.IntWrite(req.Body, n)

					resp, _ := c.Do(context.Background(), "tcp4", ln.Addr().String(), req)
					defer c.PutResponse(resp)
				}(j)
			}

			cwg.Wait()
		}(c)
	}

	time.Sleep(10 * time.Millisecond) // bad
	ts.sendSomeResponses()
	time.Sleep(10 * time.Millisecond) // bad
	for _, c := range clients {
		_ = c.Close()
	}
	time.Sleep(10 * time.Millisecond) // bad
	s.Shutdown()
	wg.Wait()

	err = s.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = <-serverErrChan
	if err != nil {
		t.Fatal(err)
	}
	if len(ts.clients) != 0 {
		t.Fatal("long poll contexts did not clear in server")
	}
}
