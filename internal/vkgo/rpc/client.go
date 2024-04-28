// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"go.uber.org/atomic"
	"pgregory.net/rand"
)

const (
	DefaultClientConnReadBufSize  = maxGoAllocSizeClass
	DefaultClientConnWriteBufSize = maxGoAllocSizeClass

	minReconnectDelay = 200 * time.Millisecond // same as TCP_RTO_MIN
	maxReconnectDelay = 10 * time.Second

	debugPrint = false // turn on during testing using custom scheduler
	debugTrace = false // turn on during rapid testing to have trace stored in Server
)

var (
	ErrClientClosed                 = errors.New("rpc: Client closed")
	ErrClientConnClosedSideEffect   = errors.New("rpc: client connection closed after request sent")
	ErrClientConnClosedNoSideEffect = errors.New("rpc: client connection closed (or connect failed) before request sent")

	ErrClientDropRequest = errors.New("rpc hook: drop request")
)

type Request struct {
	Body    []byte
	ActorID int64
	Extra   InvokeReqExtra

	extraStart int // We serialize extra after body into Body, then write into reversed order

	FunctionName string // Experimental. Generated calls fill this during request serialization.
	ReadOnly     bool   // no side effects, can be retried by client

	queryID   int64       // unique per client, assigned by client
	hookState ClientHooks // can be nil
}

// QueryID is always non-zero (guaranteed by [Client.GetRequest]).
func (req *Request) QueryID() int64 {
	return req.queryID
}

func (req *Request) HookState() ClientHooks {
	return req.hookState
}

type Response struct {
	body  []byte // slice for reuse, always len 0
	Body  []byte
	Extra ReqResultExtra

	responseType uint32
}

// lifecycle of connections:
// when connection is first needed, it is added to conns, and connect goroutine is started
// requests are added to both connection writeQ and calls.
// once successful connection is made, requests will be sent from writeQ and marked as sent
// if request is cancelled, it will be removed from calls and marked as stale
// when response is received, it will be looked up in calls and either processed or NOP, if already cancelled or processed
// if disconnect happens, all stale, sent, expired and failIfNoConnect requests are removed from writeQ and calls
// if no calls remain, connection will be terminated and removed from conns

// Details on graceful shutdown:
// When client receives LetsFIN packet from server, client will write pending requests and write FIN, then goWriter will exit
// All new requests will be added to writeQ and conns as usually, but not sent.
// Then client will receive responses as usual. If all responses are received, goReader will also exit, and normal
// reconnect will happen.

type Client struct {
	// Place atomics first to ensure proper alignment, see https://pkg.go.dev/sync/atomic#pkg-note-BUG
	lastQueryID atomic.Int64 // use an atomic counter instead of pure random to guarantee no ID reuse

	opts ClientOptions

	mu     sync.RWMutex
	closed bool
	conns  map[NetAddr]*clientConn

	requestPool  sync.Pool
	responsePool sync.Pool
	callCtxPool  sync.Pool

	wg sync.WaitGroup
}

func NewClient(options ...ClientOptionsFunc) *Client {
	opts := ClientOptions{
		Logf:             log.Printf,
		ConnReadBufSize:  DefaultClientConnReadBufSize,
		ConnWriteBufSize: DefaultClientConnWriteBufSize,
		PacketTimeout:    DefaultPacketTimeout,
		Hooks:            func() ClientHooks { return nil },
		ProtocolVersion:  DefaultProtocolVersion,
	}
	for _, opt := range options {
		opt(&opts)
	}

	for _, err := range opts.trustedSubnetGroupsParseErrors {
		opts.Logf("[rpc] failed to parse server trusted subnet %q, ignoring", err)
	}

	c := &Client{
		opts:  opts,
		conns: map[NetAddr]*clientConn{},
	}
	c.lastQueryID.Store(rand.Int63() / 2) // We like positive query IDs. Dividing by 2 makes wrapping very rare

	return c
}

type ClientHooks interface {
	Reset()
	BeforeSend(req *Request)
	AfterReceive(resp *Response, err error)
	NeedToDropRequest(ctx context.Context, address NetAddr, req *Request) bool
}

// Client is used by other components of your app.
// So, you must first CloseWait() all other components, then Close() client.
// Client will have 0 outstanding requests if everything is done correctly, so can simply close all sackets
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true // no new connections possible
	c.mu.Unlock()

	for na, pc := range c.conns { // Exclusive ownership here, due to c.closed check in setupCall, removeConnection
		_ = pc.close()
		delete(c.conns, na)
	}

	c.wg.Wait()
	return nil
}

func (c *Client) Logf(format string, args ...any) {
	c.opts.Logf(format, args...)
}

// Do supports only "tcp4" and "unix" networks
func (c *Client) Do(ctx context.Context, network string, address string, req *Request) (*Response, error) {
	pc, cctx, err := c.setupCall(ctx, NetAddr{network, address}, req, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		pc.cancelCall(cctx, nil) // do not unblock, reuse normally
		return nil, ctx.Err()
	case r := <-cctx.result: // got ownership of cctx
		defer c.putCallContext(cctx)
		return r.resp, r.err
	}
}

// Experimental API, can change any moment
type ClientCallback func(queryID int64, resp *Response, err error, userData any)

// Either error is returned immediately, or ClientCallback will be called in the future.
// There is no cancellation logic, you must remember QueryID and compare ir for now to avoid inconsistency
func (c *Client) DoCallback(ctx context.Context, network string, address string, req *Request, cb ClientCallback, userData any) error {
	_, _, err := c.setupCall(ctx, NetAddr{network, address}, req, nil, cb, userData)
	return err
}

// Starts if it needs to
// We must setupCall inside client lock, otherwise connection might decide to quit before we can setup call
func (c *Client) setupCall(ctx context.Context, address NetAddr, req *Request, multiResult chan *callContext, cb ClientCallback, userData any) (*clientConn, *callContext, error) {
	if req.hookState != nil && req.hookState.NeedToDropRequest(ctx, address, req) {
		return nil, nil, ErrClientDropRequest
	}

	if req.Extra.IsSetNoResult() {
		// We consider it antipattern
		return nil, nil, fmt.Errorf("sending no_result requests is not supported")
	}

	deadline, _ := ctx.Deadline()

	if err := preparePacket(req, deadline); err != nil {
		return nil, nil, err
	}

	c.mu.RLock()
	// ------ to test RACE detector, replace lines below
	if c.closed {
		c.mu.RUnlock()
		return nil, nil, ErrClientClosed
	}
	pc := c.conns[address]
	// ------ with
	// pc := c.conns[address]
	// closed := c.closed
	// if closed {
	//	c.mu.RUnlock()
	//	return nil, nil, ErrClientClosed
	// }
	// ------
	if pc != nil {
		pc.mu.Lock()
		c.mu.RUnlock() // Do not hold while working with pc
		cctx, err := pc.setupCallLocked(req, deadline, multiResult, cb, userData)
		pc.mu.Unlock()
		pc.writeQCond.Signal() // signal without holding the mutex to reduce contention
		return pc, cctx, err
	}
	c.mu.RUnlock()

	if address.Network != "tcp4" && address.Network != "tcp6" && address.Network != "tcp" && address.Network != "unix" { // optimization: check only if not found in c.conns
		return nil, nil, fmt.Errorf("unsupported network type %q", address.Network)
	}

	c.mu.Lock()
	defer c.mu.Unlock() // for simplicity, this is not a fastpath

	if c.closed {
		return nil, nil, ErrClientClosed
	}

	pc = c.conns[address]
	if pc == nil {
		closeCC := make(chan struct{})
		resetReconnectDelayC := make(chan struct{}, 1)
		pc = &clientConn{
			client:               c,
			address:              address,
			calls:                map[int64]*callContext{},
			closeCC:              closeCC,
			resetReconnectDelayC: resetReconnectDelayC,
		}
		pc.writeQCond.L = &pc.mu

		c.conns[address] = pc
		if debugPrint {
			fmt.Printf("%v goConnect for client %p pc %p\n", time.Now(), c, pc)
		}
		c.wg.Add(1)
		go pc.goConnect(closeCC, resetReconnectDelayC)
	}
	pc.mu.Lock()
	cctx, err := pc.setupCallLocked(req, deadline, multiResult, cb, userData)
	pc.mu.Unlock()
	pc.writeQCond.Signal() // signal without holding the mutex to reduce contention
	return pc, cctx, err
}

// ResetReconnectDelay resets timer before the next reconnect attempt.
// If connect goroutine is already waiting - ResetReconnectDelay forces it to wait again with minimal delay
func (c *Client) ResetReconnectDelay(address NetAddr) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	conn := c.conns[address]
	if conn == nil {
		return
	}

	select {
	case conn.resetReconnectDelayC <- struct{}{}:
	default:
	}
}

func (c *Client) removeConnection(pc *clientConn) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return true
	}
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if pc.closeCC == nil { // pc already removed from pc.client.conns
		return true
	}
	if len(pc.calls) == 0 {
		delete(pc.client.conns, pc.address) // will be added/connected again if needed next time
		return true
	}
	return false
}

func (c *Client) getLoad(address NetAddr) int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	pc := c.conns[address]
	if pc == nil {
		return 0
	}

	pc.mu.Lock()
	defer pc.mu.Unlock()

	return len(pc.calls)
}

func (c *Client) GetRequest() *Request {
	req := c.getRequest()
	req.queryID = c.lastQueryID.Inc()
	for req.queryID == 0 { // so users can user QueryID as a flag
		req.queryID = c.lastQueryID.Inc()
	}
	return req
}

func (c *Client) getRequest() *Request {
	v := c.requestPool.Get()
	if v != nil {
		return v.(*Request)
	}
	return &Request{hookState: c.opts.Hooks()}
}

func (c *Client) putRequest(req *Request) {
	if req.hookState != nil {
		req.hookState.Reset()
	}
	*req = Request{Body: req.Body[:0], hookState: req.hookState}

	c.requestPool.Put(req)
}

func (c *Client) getResponse() *Response {
	v := c.responsePool.Get()
	if v != nil {
		resp := v.(*Response)
		return resp
	}
	return &Response{}
}

func (c *Client) PutResponse(resp *Response) {
	if resp == nil {
		// We sometimes return both error and response, so user can inspect Extra, if parsed successfully
		return
	}

	*resp = Response{
		body: resp.body,
	}
	c.responsePool.Put(resp)
}

func (pc *clientConn) getCallContext() *callContext {
	v := pc.client.callCtxPool.Get()
	if v != nil {
		return v.(*callContext)
	}
	cctx := &callContext{
		singleResult: make(chan *callContext, 1),
		hookState:    pc.client.opts.Hooks(),
	}
	cctx.result = cctx.singleResult
	return cctx
}

func (c *Client) putCallContext(cctx *callContext) {
	if cctx.hookState != nil {
		cctx.hookState.Reset()
	}
	*cctx = callContext{
		singleResult: cctx.singleResult,
		result:       cctx.singleResult,
		hookState:    cctx.hookState,
	}

	c.callCtxPool.Put(cctx)
}
