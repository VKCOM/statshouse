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
	"math"
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
	Body               []byte
	ActorID            int64
	Extra              RequestExtra
	FailIfNoConnection bool

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
	body  *[]byte // slice for reuse, always len 0
	Body  []byte  // rest of body after parsing various extras
	Extra ResponseExtra

	// We use Response as a call context to reduce # of allocations. Fields below represent call context.
	queryID            int64
	failIfNoConnection bool // set in setupCall call and never changes. Allows to quickly try multiple servers without waiting timeout
	readonly           bool // set in setupCall call and never changes. TODO - implement logic

	sent  bool // Request was sent.
	stale bool // Request was cancelled before sending. Instead of removing from the write queue which is O(N), we set the flag  and check before sending

	singleResult chan *Response // channel for single caller is reused here
	result       chan *Response // can point to singleResult or multiResult if used by MultiClient

	cb       ClientCallback // callback-style API, if set result channels are unused
	userData any

	err error // if set, we have error response, sometimes we have Body and Extra with it. May point to rpcErr.

	hookState ClientHooks // can be nil
}

func (resp *Response) QueryID() int64 { return resp.queryID }
func (resp *Response) UserData() any  { return resp.userData }

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
	// use an atomic counter instead of pure random to guarantee no ID reuse
	// queryID should be per connection, but in our API user fills Request before connection is established/known, so we use per client
	lastQueryID atomic.Uint64

	opts ClientOptions

	mu     sync.RWMutex
	closed bool
	conns  map[NetAddr]*clientConn

	requestPool      sync.Pool
	responseDataPool sync.Pool
	responsePool     sync.Pool

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
	c.lastQueryID.Store(rand.Uint64())

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

// Do supports only "tcp", "tcp4", "tcp6" and "unix" networks
func (c *Client) Do(ctx context.Context, network string, address string, req *Request) (*Response, error) {
	pc, cctx, err := c.setupCall(ctx, NetAddr{network, address}, req, nil, nil, nil)
	if err != nil { // got ownership of cctx, if not nil
		return cctx, err
	}
	select {
	case <-ctx.Done():
		_ = pc.cancelCall(cctx.queryID, nil) // do not unblock, reuse normally
		return nil, ctx.Err()
	case r := <-cctx.result: // got ownership of cctx
		return cctx, r.err
	}
}

// Experimental API, can change any moment. For high-performance clients, like ingress proxy
type ClientCallback func(client *Client, resp *Response, err error)

type CallbackContext struct {
	pc      *clientConn
	queryID int64
}

func (cc CallbackContext) QueryID() int64 { return cc.queryID } // so client will not have to remember queryID separately

// Either error is returned immediately, or ClientCallback will be called in the future.
// We add explicit userData, because for many users it will avoid allocation of lambda during capture of userData in cb
func (c *Client) DoCallback(ctx context.Context, network string, address string, req *Request, cb ClientCallback, userData any) (CallbackContext, error) {
	queryID := req.QueryID() // must not access cctx or req after setupCall, because callback can be already called and cctx reused
	pc, _, err := c.setupCall(ctx, NetAddr{network, address}, req, nil, cb, userData)
	return CallbackContext{
		pc:      pc,
		queryID: queryID,
	}, err
}

// Callback will never be called twice, but you should be ready for callback even after you call Cancel.
// This is because callback could be in the process of calling.
// The best idea is to remember queryID (which is unique per Client) of call in your per call data structure and compare in callback.
// if cancelled == true, call was cancelled before callback scheduled for calling/called
// if cancelled == false, call result was already being delivered/delivered
func (c *Client) CancelDoCallback(cc CallbackContext) (cancelled bool) {
	return cc.pc.cancelCall(cc.queryID, nil)
}

// Starts if it needs to
// We must setupCall inside client lock, otherwise connection might decide to quit before we can setup call
func (c *Client) setupCall(ctx context.Context, address NetAddr, req *Request, multiResult chan *Response, cb ClientCallback, userData any) (*clientConn, *Response, error) {
	if req.hookState != nil && req.hookState.NeedToDropRequest(ctx, address, req) {
		return nil, nil, ErrClientDropRequest
	}

	if req.Extra.IsSetNoResult() {
		// We consider it antipattern. TODO - implement)
		return nil, nil, fmt.Errorf("sending no_result requests is not supported")
	}

	deadline, err := c.fillRequestTimeout(ctx, req)
	if err != nil {
		return nil, nil, err
	}
	if err := preparePacket(req); err != nil {
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
			calls:                map[int64]*Response{},
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

func (c *Client) fillRequestTimeout(ctx context.Context, req *Request) (time.Time, error) {
	UpdateExtraTimeout(&req.Extra, c.opts.DefaultTimeout)
	if !req.Extra.IsSetCustomTimeoutMs() && req.Extra.CustomTimeoutMs != 0 { // protect against programmer's mistake
		return time.Time{}, fmt.Errorf("rpc: custom timeout should be set with extra.SetCustomTimeoutMs function, otherwise custom timeout is ignored")
	}
	if req.Extra.CustomTimeoutMs < 0 { // TODO - change TL scheme to have unsigned type
		return time.Time{}, fmt.Errorf("rpc: extra.CustomTimeoutMs (%d) should not be negative", req.Extra.CustomTimeoutMs)
	}
	if deadline, ok := ctx.Deadline(); ok {
		to := time.Until(deadline)
		if to <= 0 { // local timeout, <= is the only correct comparison
			return time.Time{}, context.DeadlineExceeded
		}
		if req.Extra.CustomTimeoutMs == 0 || to < time.Millisecond*time.Duration(req.Extra.CustomTimeoutMs) {
			// there is no point to set timeout larger than set in context, as client will fail locally before server returns response
			req.Extra.ClearCustomTimeoutMs()
			UpdateExtraTimeout(&req.Extra, to)
			return deadline, nil
		}
	}
	if req.Extra.CustomTimeoutMs == 0 { //  infinite
		req.Extra.ClearCustomTimeoutMs() // normalize by not sending infinite timeout
		return time.Time{}, nil
	}
	return time.Now().Add(time.Millisecond * time.Duration(req.Extra.CustomTimeoutMs)), nil
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
	for {
		req.queryID = int64(c.lastQueryID.Inc() & math.MaxInt64)
		// We like positive query IDs, but not 0, so users can use QueryID as a flag
		if req.queryID != 0 {
			break
		}
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

func (c *Client) getResponseData() *[]byte {
	v := c.responseDataPool.Get()
	if v != nil {
		resp := v.(*[]byte)
		return resp
	}
	var result []byte
	return &result
}

func (c *Client) putResponseData(resp *[]byte) {
	if resp != nil {
		c.responseDataPool.Put(resp)
	}
}

func (c *Client) getResponse() *Response {
	v := c.responsePool.Get()
	if v != nil {
		return v.(*Response)
	}
	cctx := &Response{
		singleResult: make(chan *Response, 1),
		hookState:    c.opts.Hooks(),
	}
	cctx.result = cctx.singleResult
	return cctx
}

func (c *Client) PutResponse(cctx *Response) {
	if cctx == nil {
		// We sometimes return both error and response, so user can inspect Extra, if parsed successfully
		return
	}
	if cctx.hookState != nil {
		cctx.hookState.Reset()
	}
	c.putResponseData(cctx.body)
	*cctx = Response{
		singleResult: cctx.singleResult,
		result:       cctx.singleResult,
		hookState:    cctx.hookState,
	}

	c.responsePool.Put(cctx)
}

// naive rounding can round tiny to infinite
func roundTimeoutToInt32(timeout time.Duration) int32 {
	if timeout <= 0 {
		return 0 // infinite
	}
	timeoutMilli := timeout.Milliseconds()
	if timeoutMilli > math.MaxInt32 {
		return 0 // large timeouts round to infinite
	}
	if timeoutMilli < 1 { // do not round small timeouts to infinite
		return 1
	}
	return int32(timeoutMilli)
}

// We have several hierarchical timeouts, we update from high to lower priority.
// So if high priority timeout is set (even to 0 (infinite)), subsequent calls do nothing.
func UpdateExtraTimeout(extra *RequestExtra, timeout time.Duration) {
	if extra.IsSetCustomTimeoutMs() {
		return
	}
	if t := roundTimeoutToInt32(timeout); t > 0 {
		extra.SetCustomTimeoutMs(t)
	}
}
