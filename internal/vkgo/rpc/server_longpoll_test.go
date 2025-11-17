// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"pgregory.net/rand"
	"pgregory.net/rapid"

	"github.com/VKCOM/statshouse/internal/vkgo/basictl"
)

const emptyBody = "empty response"

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
	numRequests := rapid.IntRange(1, 10).Draw(t, "numRequests")

	ts := shutdownTestServer{clients: map[LongpollHandle]int32{}}
	s := NewServer(
		ServerWithSyncHandler(ts.testShutdownHandler),
		ServerWithCryptoKeys(testCryptoKeys),
		ServerWithDebugRPC(true),
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
		go func(c Client) {
			for j := 0; j < numRequests; j++ {
				go func() {
					n := rand.New().Int31()
					req := c.GetRequest()
					req.FailIfNoConnection = true
					req.Body = basictl.NatWrite(req.Body, testRequestType)
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

type longpollTestServer struct {
	mu                    sync.Mutex
	handleToId            map[LongpollHandle]int
	idToHandle            map[int]LongpollHandle
	cancellationsCount    int
	emptyResponsesCount   int
	handlerCallback       func()
	cancellationCallback  func()
	emptyResponseCallback func()
	customTimeout         time.Duration
}

type longpollTestServerOption func(lts *longpollTestServer)

func withCustomTimeout(timeout time.Duration) longpollTestServerOption {
	return func(lts *longpollTestServer) {
		lts.customTimeout = timeout
	}
}

func newLongpollTestServer(opts ...longpollTestServerOption) *longpollTestServer {
	lts := &longpollTestServer{
		handleToId: map[LongpollHandle]int{},
		idToHandle: map[int]LongpollHandle{},
	}

	for _, opt := range opts {
		opt(lts)
	}

	return lts
}

func (s *longpollTestServer) CancelLongpoll(lh LongpollHandle) {
	s.mu.Lock()
	id, exists := s.handleToId[lh]
	if !exists {
		s.mu.Unlock()
		return
	}
	s.cancellationsCount++
	delete(s.handleToId, lh)
	delete(s.idToHandle, id)
	s.mu.Unlock()
	if s.cancellationCallback != nil {
		s.cancellationCallback()
	}
}

func (s *longpollTestServer) WriteEmptyResponse(lh LongpollHandle, resp *HandlerContext) error {
	s.mu.Lock()
	id, exists := s.handleToId[lh]
	if !exists {
		s.mu.Unlock()
		return ErrLongpollNoEmptyResponse
	}
	s.emptyResponsesCount++
	delete(s.handleToId, lh)
	delete(s.idToHandle, id)
	s.mu.Unlock()

	resp.Response = basictl.StringWrite(resp.Response, emptyBody)
	if s.emptyResponseCallback != nil {
		s.emptyResponseCallback()
	}
	return nil
}

func (s *longpollTestServer) WakeUpLongpoll(id int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	lh, exists := s.idToHandle[id]
	if !exists {
		return
	}

	delete(s.idToHandle, id)
	delete(s.handleToId, lh)

	hctx, _ := lh.FinishLongpoll()
	if hctx == nil {
		return
	}

	// Don't store int int tl here, because then I actually need maybe instead of string
	hctx.Response = basictl.StringWrite(hctx.Response, strconv.FormatInt(int64(id), 10))
	hctx.SendLongpollResponse(nil)
}

func (s *longpollTestServer) Handler(_ context.Context, hctx *HandlerContext) (err error) {
	if s.handlerCallback != nil {
		defer s.handlerCallback()
	}

	if hctx.Request, err = basictl.NatReadExactTag(hctx.Request, testRequestType); err != nil {
		return err
	}
	var id int32
	if hctx.Request, err = basictl.IntRead(hctx.Request, &id); err != nil {
		return err
	}

	var lh LongpollHandle
	if s.customTimeout != 0 {
		lh, err = hctx.StartLongpollWithTimeoutDeprecated(s, s.customTimeout)
	} else {
		lh, err = hctx.StartLongpoll(s)
	}
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.handleToId[lh] = int(id)
	s.idToHandle[int(id)] = lh

	return nil
}

func (s *longpollTestServer) CancellationsCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.cancellationsCount
}

func (s *longpollTestServer) EmptyResponsesCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.emptyResponsesCount
}

func TestLongpollTimeout(t *testing.T) {
	t.Run("empty body is sent after timeout", func(t *testing.T) {
		ts := newLongpollTestServer()
		s := NewServer(
			ServerWithSyncHandler(ts.Handler),
			ServerWithCryptoKeys(testCryptoKeys),
			ServerWithDebugRPC(true),
			ServerWithMinimumLongpollTimeout(500*time.Millisecond),
		)

		ln, err := net.Listen("tcp4", "127.0.0.1:")
		if err != nil {
			t.Fatal(err)
		}

		errCh := make(chan error)
		go func() {
			errCh <- s.Serve(ln)
		}()

		c := NewClient()
		req := c.GetRequest()
		req.FailIfNoConnection = true
		req.Body = basictl.NatWrite(req.Body, testRequestType)

		n := rand.New().Int31()
		req.Body = basictl.IntWrite(req.Body, n)

		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()

		resp, err := c.Do(ctx, "tcp4", ln.Addr().String(), req)
		if err != nil {
			t.Fatalf("unexpected err in client do: %s", err.Error())
		}

		var body string
		if _, err := basictl.StringRead(resp.Body, &body); err != nil {
			t.Fatalf("unexpected err in string read: %s", err.Error())
		}

		if body != emptyBody {
			t.Fatalf("unexpected body in response: %s", body)
		}

		s.Shutdown()
		if err := s.Close(); err != nil {
			t.Fatalf("unexpected err in close: %s", err.Error())
		}

		err = <-errCh
		if err != nil {
			t.Fatalf("unexpected error in serve: %s", err.Error())
		}

		if ts.cancellationsCount != 0 {
			t.Fatalf("unexpected cancel in sever: %d", ts.cancellationsCount)
		}
	})

	t.Run("cancel during waiting", func(t *testing.T) {
		handleChan := make(chan struct{}, 1)
		cancelChan := make(chan struct{}, 1)
		ts := newLongpollTestServer()
		ts.handlerCallback = func() {
			handleChan <- struct{}{}
		}
		ts.cancellationCallback = func() {
			cancelChan <- struct{}{}
		}

		s := NewServer(
			ServerWithSyncHandler(ts.Handler),
			ServerWithCryptoKeys(testCryptoKeys),
			ServerWithDebugRPC(true),
		)

		ln, err := net.Listen("tcp4", "127.0.0.1:")
		if err != nil {
			t.Fatal(err)
		}

		errCh := make(chan error)
		go func() {
			errCh <- s.Serve(ln)
		}()

		c := NewClient()
		req := c.GetRequest()
		req.FailIfNoConnection = true
		req.Body = basictl.NatWrite(req.Body, testRequestType)

		n := rand.New().Int31()
		req.Body = basictl.IntWrite(req.Body, n)

		clientErrCh := make(chan error)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Hour)
		go func() {
			_, err = c.Do(ctx, "tcp4", ln.Addr().String(), req)
			clientErrCh <- err
		}()
		// Make sure that request was received by the server
		<-handleChan
		cancel()
		if err := <-clientErrCh; err != context.Canceled {
			t.Fatalf("unexpected err in client do: %s", err.Error())
		}
		// Make sure that cancel was called by the server
		<-cancelChan
		if err := s.Close(); err != nil {
			t.Fatalf("unexpected err in close: %s", err.Error())
		}

		err = <-errCh
		if err != nil {
			t.Fatalf("unexpected error in serve: %s", err.Error())
		}

		if ts.CancellationsCount() != 1 {
			t.Fatalf("expected exactly one cancel: %d", ts.CancellationsCount())
		}
		if s.longpollTree.Size() != 0 {
			t.Fatalf("longpoll tree expected to be empty")
		}
	})

	t.Run("shutdown during waiting", func(t *testing.T) {
		handleChan := make(chan struct{}, 1)
		emptyResponseChan := make(chan struct{}, 1)
		ts := newLongpollTestServer()
		ts.handlerCallback = func() {
			handleChan <- struct{}{}
		}
		ts.emptyResponseCallback = func() {
			emptyResponseChan <- struct{}{}
		}

		s := NewServer(
			ServerWithSyncHandler(ts.Handler),
			ServerWithCryptoKeys(testCryptoKeys),
			ServerWithDebugRPC(true),
		)

		ln, err := net.Listen("tcp4", "127.0.0.1:")
		if err != nil {
			t.Fatal(err)
		}

		errCh := make(chan error)
		go func() {
			errCh <- s.Serve(ln)
		}()

		c := NewClient()
		req := c.GetRequest()
		req.FailIfNoConnection = true
		req.Body = basictl.NatWrite(req.Body, testRequestType)

		n := rand.New().Int31()
		req.Body = basictl.IntWrite(req.Body, n)

		clientErrCh := make(chan error)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Hour)
		defer cancel()
		go func() {
			_, err = c.Do(ctx, "tcp4", ln.Addr().String(), req)
			clientErrCh <- err
		}()
		// Make sure that request was received by the server
		<-handleChan
		s.Shutdown()
		if err := <-clientErrCh; err != nil {
			t.Fatalf("unexpected err in client do: %v", err)
		}
		// Make sure that cancel was called by the server
		<-emptyResponseChan
		if err := s.Close(); err != nil {
			t.Fatalf("unexpected err in close: %v", err)
		}

		err = <-errCh
		if err != nil {
			t.Fatalf("unexpected error in serve: %s", err.Error())
		}

		if ts.EmptyResponsesCount() != 1 {
			t.Fatalf("expected exactly one empty response: %d", ts.EmptyResponsesCount())
		}
		if s.longpollTree.Size() != 0 {
			t.Fatalf("longpoll tree expected to be empty")
		}
	})

	t.Run(
		"StartLongpollWithTimeoutDeprecated rewrites rpc.Request extra timeout",
		func(t *testing.T) {
			ts := newLongpollTestServer(withCustomTimeout(time.Millisecond))
			s := NewServer(
				ServerWithSyncHandler(ts.Handler),
				ServerWithCryptoKeys(testCryptoKeys),
				ServerWithDebugRPC(true),
			)

			ln, err := net.Listen("tcp4", "127.0.0.1:")
			if err != nil {
				t.Fatal(err)
			}

			errCh := make(chan error)
			go func() {
				errCh <- s.Serve(ln)
			}()
			defer s.Shutdown()

			c := NewClient()
			req := c.GetRequest()
			req.FailIfNoConnection = true
			req.Body = basictl.NatWrite(req.Body, testRequestType)

			n := rand.New().Int31()
			req.Body = basictl.IntWrite(req.Body, n)

			// Set really big timeout here
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Hour)
			defer cancel()

			// If custom timeout works then this req will take 1 ms
			resp, err := c.Do(ctx, "tcp4", ln.Addr().String(), req)
			if err != nil {
				t.Fatal(err)
			}

			var body string
			if _, err := basictl.StringRead(resp.Body, &body); err != nil {
				t.Fatalf("unexpected err in string read: %s", err.Error())
			}

			if body != emptyBody {
				t.Fatalf("unexpected body in response: %s", body)
			}

			s.Shutdown()
			if err := <-errCh; err != nil {
				t.Fatalf("serve returned an error: %s", err.Error())
			}
		},
	)
}

type client struct {
	id      int32
	ctx     context.Context
	cancel  context.CancelFunc
	readyCh chan struct {
		response *Response
		err      error
	}
	deadline time.Time
}

type longpollServerStateMachine struct {
	mode           string   // udp or tcp
	clients        []client // waiting longpolls
	server         *longpollTestServer
	rpcServer      *Server
	rpcServerErr   chan error
	addr           string
	emptyResponses int
	nextId         int32
}

func NewLongpollServerStateMachine(t *rapid.T) *longpollServerStateMachine {
	mode := "tcp4"
	// TODO: там отдельный метод для udp
	// if rapid.Bool().Draw(t, "udp") {
	// 	mode = "udp"
	// }

	longpollServer := newLongpollTestServer()
	s := NewServer(
		ServerWithSyncHandler(longpollServer.Handler),
		ServerWithCryptoKeys(testCryptoKeys),
		// ServerWithDebugRPC(true),
	)
	ln, err := net.Listen(mode, "127.0.0.1:")
	if err != nil {
		t.Fatal(err)
	}

	rpcServerErr := make(chan error)
	go func() {
		rpcServerErr <- s.Serve(ln)
	}()

	return &longpollServerStateMachine{
		mode:         mode,
		server:       longpollServer,
		rpcServer:    s,
		rpcServerErr: rpcServerErr,
		addr:         ln.Addr().String(),
	}
}

func (lssm *longpollServerStateMachine) SendLongpoll(t *rapid.T) {
	var opts []ClientOptionsFunc
	if lssm.mode == "udp" {
		opts = append(opts, ClientWithExperimentalLocalUDPAddress("127.0.0.1:"))
	}

	c := NewClient(opts...)
	req := c.GetRequest()
	req.FailIfNoConnection = true

	req.Body = basictl.NatWrite(req.Body, testRequestType)

	id := lssm.nextId
	lssm.nextId++

	req.Body = basictl.IntWrite(req.Body, id)

	timeout := rapid.IntRange(100, 200).Draw(t, "requestTimeout")
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	readyCh := make(chan struct {
		response *Response
		err      error
	}, 1)

	lssm.clients = append(lssm.clients, client{
		id:       id,
		ctx:      ctx,
		cancel:   cancel,
		readyCh:  readyCh,
		deadline: time.Now().Add(time.Duration(timeout) * time.Millisecond),
	})

	go func() {
		resp, err := c.Do(ctx, lssm.mode, lssm.addr, req)
		readyCh <- struct {
			response *Response
			err      error
		}{
			response: resp,
			err:      err,
		}
	}()
}

// func (lssm *longpollServerStateMachine) CancelLongpoll(t *rapid.T) {

// }

// func (lssm *longpollServerStateMachine) WaitEmptyResponse(t *rapid.T) {

// }

func (lssm *longpollServerStateMachine) WakeUpLongpoll(t *rapid.T) {
	if len(lssm.clients) == 0 {
		return
	}

	// wake up first client
	client := lssm.clients[0]
	lssm.clients = lssm.clients[1:]
	lssm.server.WakeUpLongpoll(int(client.id))

	respInfo := <-client.readyCh
	if respInfo.err != nil {
		t.Fatalf("unexpected err in wake up: %s", respInfo.err.Error())
	}

	var body string
	if _, err := basictl.StringRead(respInfo.response.Body, &body); err != nil {
		t.Fatalf("unexpected err in string read: %s", err.Error())
	}

	if body == emptyBody {
		lssm.emptyResponses++
		return
	}

	parsedId, err := strconv.Atoi(body)
	if err != nil {
		t.Fatalf("can't parse id from body: %s", err.Error())
	}

	if parsedId != int(client.id) {
		t.Fatalf("unexpected id in resp: %d", parsedId)
	}
}

func (lssm *longpollServerStateMachine) Check(t *rapid.T) {

}

// func TestLongpollProertyBased(t *testing.T) {
// 	t.Parallel()

// Now there is a race condition between the moment when longpoll arrives
// and the moment when the longpoll is woken up
// so this test isn't working, should working once the go version is >= 1.24
// because https://pkg.go.dev/testing/synctest#Test could be used
// rapid.Check(t, func(t *rapid.T) {
// 	sm := NewLongpollServerStateMachine(t)
// 	t.Repeat(rapid.StateMachineActions(sm))
// 	sm.rpcServer.Close()
// 	<-sm.rpcServerErr
// 	// TODO: в конце попритить пустые запросы и проверить, что нету тех кому ответ не пришел
// })
// }
