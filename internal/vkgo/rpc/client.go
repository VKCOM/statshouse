// Copyright 2022 V Kontakte LLC
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
	"net"
	"sync"
	"time"

	"go.uber.org/atomic"
	"go.uber.org/multierr"
	"pgregory.net/rand"

	"github.com/vkcom/statshouse/internal/vkgo/basictl"
	"github.com/vkcom/statshouse/internal/vkgo/srvfunc"
)

// TODO: graceful shutdown

const (
	DefaultClientConnReadBufSize  = maxGoAllocSizeClass
	DefaultClientConnWriteBufSize = maxGoAllocSizeClass

	minReconnectDelay = 200 * time.Millisecond // same as TCP_RTO_MIN
	maxReconnectDelay = 5 * time.Second
)

var (
	ErrClientClosed                 = errors.New("rpc: Client closed")
	ErrClientConnClosedSideEffect   = errors.New("rpc: client connection closed after request sent")
	ErrClientConnClosedNoSideEffect = errors.New("rpc: client connection closed (or connect failed) before request sent")

	callCtxPool sync.Pool
)

type Request struct {
	Body     []byte
	ActorID  uint64
	Extra    InvokeReqExtra
	HookArgs any
}

func (req *Request) reset(resetHookArgs func(any)) {
	resetHookArgs(req.HookArgs)
	*req = Request{Body: req.Body[:0], HookArgs: req.HookArgs}
}

type Response struct {
	body  []byte // slice for reuse, always len 0
	Body  []byte
	Extra ReqResultExtra

	responseType uint32
}

func (resp *Response) reset() {
	resp.Body = nil
	resp.Extra = ReqResultExtra{}
	resp.responseType = 0
}

type callContext struct {
	// Place atomics first to ensure proper alignment, see https://pkg.go.dev/sync/atomic#pkg-note-BUG
	sent  atomic.Bool
	stale atomic.Bool

	failIfNoConnection bool // experimental, set in setupCall call and never changes

	hooksState any

	result    chan callResult
	closeOnce sync.Once
	closed    chan struct{} // single channel to signal closing both client and connection: this way we have 1 less select case in do()

	queryID       int64
	multiFinished chan int64
	multiClosed   chan int64
}

func newCallContext(initHookState func() any) *callContext {
	ret := &callContext{
		result:     make(chan callResult, 1),
		closed:     make(chan struct{}),
		hooksState: initHookState(),
	}
	return ret
}

func (cctx *callContext) reset() {
	cctx.sent.Store(false)
	cctx.stale.Store(false)
	cctx.failIfNoConnection = false
	cctx.queryID = 0
	cctx.multiFinished = nil
	cctx.multiClosed = nil
}

// Not reused when closed
func (cctx *callContext) close() {
	cctx.closeOnce.Do(func() {
		if cctx.multiClosed != nil {
			cctx.multiClosed <- cctx.queryID
		}
		close(cctx.closed) // synchronization point; control is transferred
	})
}

type writeReq struct {
	cctx               *callContext
	pingPongPacketType uint32 // 0 (if not ping-pong), packetTypeRPCPing, packetTypeRPCPong
	pingPongID         int64
	req                *Request
	deadline           time.Time
}

type callResult struct {
	ok  *Response
	err Error
}

type Client struct {
	// Place atomics first to ensure proper alignment, see https://pkg.go.dev/sync/atomic#pkg-note-BUG
	lastQueryID atomic.Int64 // use an atomic counter instead of pure random to guarantee no ID reuse
	logf        LoggerFunc   // defaults to log.Printf; set to NoopLogf to disable all logging

	forceEncryption  bool
	cryptoKey        string
	connReadBufSize  int
	connWriteBufSize int
	pongTimeout      time.Duration // defaults to rpc.DefaultClientPongTimeout

	trustedSubnetGroups [][]*net.IPNet

	mu      sync.RWMutex
	clients map[NetAddr]*peerClient

	closeOnce sync.Once
	closeErr  error
	closed    bool

	requestPool  sync.Pool
	responsePool sync.Pool

	hooks ClientHooks
}

type ClientOptions struct {
	Logf                LoggerFunc
	Hooks               ClientHooks
	TrustedSubnetGroups [][]*net.IPNet
	ForceEncryption     bool
	CryptoKey           string
	ConnReadBufSize     int
	ConnWriteBufSize    int
	PongTimeout         time.Duration

	trustedSubnetGroupsParseErrors []error
}

type ClientOptionsFunc func(*ClientOptions)

func ClientWithLogf(f LoggerFunc) ClientOptionsFunc {
	return func(o *ClientOptions) {
		o.Logf = f
	}
}

func ClientWithHooks(hooks ClientHooks) ClientOptionsFunc {
	return func(o *ClientOptions) {
		o.Hooks = hooks
	}
}

func ClientWithTrustedSubnetGroups(groups [][]string) ClientOptionsFunc {
	return func(o *ClientOptions) {
		gs, errs := ParseTrustedSubnets(groups)
		o.TrustedSubnetGroups = gs
		o.trustedSubnetGroupsParseErrors = errs
	}
}

func ClientWithForceEncryption(force bool) ClientOptionsFunc {
	return func(o *ClientOptions) {
		o.ForceEncryption = force
	}
}

func ClientWithCryptoKey(key string) ClientOptionsFunc {
	return func(o *ClientOptions) {
		o.CryptoKey = key
	}
}

func ClientWithConnReadBufSize(size int) ClientOptionsFunc {
	return func(o *ClientOptions) {
		if size > 0 {
			o.ConnReadBufSize = size
		}
	}
}

func ClientWithConnWriteBufSize(size int) ClientOptionsFunc {
	return func(o *ClientOptions) {
		if size > 0 {
			o.ConnWriteBufSize = size
		}
	}
}

func ClientWithPongTimeout(timeout time.Duration) ClientOptionsFunc {
	return func(o *ClientOptions) {
		if timeout > 0 {
			o.PongTimeout = timeout
		}
	}
}

func NewClient(options ...ClientOptionsFunc) *Client {
	opts := &ClientOptions{
		Logf:             log.Printf,
		ConnReadBufSize:  DefaultClientConnReadBufSize,
		ConnWriteBufSize: DefaultClientConnWriteBufSize,
		PongTimeout:      DefaultClientPongTimeout,
		Hooks: ClientHooks{
			InitState:  func() any { return nil },
			ResetState: func(state any) {},
			Request: RequestHooks{
				InitArguments:  func() any { return 0 },
				ResetArguments: func(state any) {},
				BeforeSend:     func(state any, req *Request) {},
				AfterReceive:   func(state any, resp *Response, err error) {},
			},
		},
	}
	for _, opt := range options {
		opt(opts)
	}

	for _, err := range opts.trustedSubnetGroupsParseErrors {
		opts.Logf("[rpc] failed to parse server trusted subnet %q, ignoring", err)
	}

	c := &Client{
		logf:                opts.Logf,
		hooks:               opts.Hooks,
		connReadBufSize:     opts.ConnReadBufSize,
		connWriteBufSize:    opts.ConnWriteBufSize,
		pongTimeout:         opts.PongTimeout,
		cryptoKey:           opts.CryptoKey,
		forceEncryption:     opts.ForceEncryption,
		trustedSubnetGroups: opts.TrustedSubnetGroups,
		clients:             map[NetAddr]*peerClient{},
	}
	c.lastQueryID.Store(int64(rand.Uint64()))

	return c
}

type ClientHooks struct {
	InitState  func() any
	ResetState func(state any)
	Request    RequestHooks
}

type RequestHooks struct {
	InitArguments  func() any
	ResetArguments func(args any)

	BeforeSend   func(state any, req *Request)
	AfterReceive func(state any, resp *Response, err error)
}

func (c *Client) Close() error {
	c.closeOnce.Do(c.doClose)
	return c.closeErr
}

func (c *Client) doClose() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, pc := range c.clients {
		err := pc.Close()
		multierr.AppendInto(&c.closeErr, err)
	}

	c.closed = true
}

func (c *Client) start(network string, address string, req *Request) (*peerClient, int64, error) {
	if network != "tcp4" && network != "unix" {
		return nil, 0, fmt.Errorf("unsupported network type %q", network)
	}

	if req.Extra.IsSetNoResult() {
		return nil, 0, fmt.Errorf("sending no_result requests is not supported")
	}

	err := validPacketBodyLen(len(req.Body))
	if err != nil {
		return nil, 0, err
	}

	na := NetAddr{network, address}
	pc, ok := c.getPeerClient(na)
	if !ok {
		return nil, 0, ErrClientClosed
	}

	return pc, c.lastQueryID.Inc(), nil
}

func (c *Client) Logf(format string, args ...interface{}) {
	c.logf(format, args...)
}

// Do supports only "tcp4" and "unix" networks
func (c *Client) Do(ctx context.Context, network string, address string, req *Request) (*Response, error) {
	pc, queryID, err := c.start(network, address, req)
	if err != nil {
		return nil, err
	}

	return pc.do(ctx, req, queryID)
}

func (c *Client) getPeerClient(address NetAddr) (*peerClient, bool) {
	pc, ok := c.getPeerClientFast(address)
	if !ok {
		return nil, false
	}
	if pc != nil {
		return pc, true
	}

	return c.getPeerClientSlow(address)
}

func (c *Client) getPeerClientFast(address NetAddr) (*peerClient, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.clients[address], !c.closed
}

func (c *Client) getPeerClientSlow(address NetAddr) (*peerClient, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, false
	}

	pc := c.clients[address]
	if pc == nil {
		pc = &peerClient{
			client:          c,
			logf:            c.logf,
			address:         address,
			forceEncryption: c.forceEncryption,
			cryptoKey:       c.cryptoKey,
			calls:           map[int64]*callContext{},
		}
		pc.writeQCond.L = &pc.mu

		c.clients[address] = pc
	}

	return pc, true
}

func (c *Client) getLoad(address NetAddr) int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	pc := c.clients[address]
	if pc == nil {
		return 0
	}

	pc.mu.Lock()
	defer pc.mu.Unlock()

	return len(pc.calls)
}

type peerClient struct {
	client *Client

	logf            LoggerFunc
	address         NetAddr
	forceEncryption bool
	cryptoKey       string

	writeQ     []writeReq
	writeQCond sync.Cond

	mu                 sync.Mutex
	calls              map[int64]*callContext
	conn               *clientConn
	running            bool
	waitingToReconnect bool // defined only when running

	closeOnce sync.Once
	closeErr  error
	closed    bool
}

func (pc *peerClient) Close() error {
	pc.closeOnce.Do(pc.doClose)
	return pc.closeErr
}

func (pc *peerClient) doClose() {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	for _, cctx := range pc.calls {
		cctx.sent.Store(false) // avoid do() returning ErrClientConnClosedSideEffect instead of ErrClientClosed
		cctx.close()
	}

	if pc.conn != nil {
		err := pc.conn.Close()
		multierr.AppendInto(&pc.closeErr, err)
		pc.conn = nil
		pc.writeQCond.Broadcast() // unblock sendLoop()
	}

	pc.closed = true
}

func (pc *peerClient) do(ctx context.Context, req *Request, queryID int64) (*Response, error) {
	var cctxPut bool
	deadline, _ := ctx.Deadline()

	cctx, err := pc.setupCall(req, deadline, queryID, nil, nil)
	if err != nil {
		return nil, err
	}
	defer func() { pc.teardownCall(cctx, cctxPut) }()

	select {
	case <-ctx.Done():
		cctx.stale.Store(true)
		return nil, ctx.Err()
	case <-cctx.closed:
		switch {
		case cctx.sent.Load():
			return nil, ErrClientConnClosedSideEffect
		case cctx.failIfNoConnection:
			return nil, ErrClientConnClosedNoSideEffect
		default:
			return nil, ErrClientClosed
		}
	case r := <-cctx.result:
		cctxPut = true
		if r.ok == nil {
			return nil, r.err
		}
		return r.ok, nil
	}
}

func (pc *peerClient) setupCall(req *Request, deadline time.Time, queryID int64, multiFinished chan int64, multiClosed chan int64) (*callContext, error) {
	cctx := getCallContext(pc.client.hooks.InitState)
	cctx.queryID = queryID
	cctx.multiFinished = multiFinished
	cctx.multiClosed = multiClosed
	cctx.failIfNoConnection = req.Extra.FailIfNoConnection

	err := pc.setupCallLocked(cctx, req, deadline)
	if err != nil {
		putCallContext(cctx)
		return nil, err
	}

	pc.writeQCond.Signal() // signal without holding the mutex to reduce contention

	return cctx, nil
}

func (pc *peerClient) setupCallLocked(cctx *callContext, req *Request, deadline time.Time) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if pc.closed {
		return ErrClientClosed
	}

	if pc.running {
		if req.Extra.FailIfNoConnection && pc.waitingToReconnect {
			return ErrClientConnClosedNoSideEffect
		}
	} else {
		pc.running = true
		pc.waitingToReconnect = false
		go pc.runLoop()
	}

	pc.calls[cctx.queryID] = cctx

	pc.writeQ = append(pc.writeQ, writeReq{
		cctx:     cctx,
		req:      req,
		deadline: deadline,
	})

	pc.client.hooks.Request.BeforeSend(cctx.hooksState, req)

	return nil
}

func (pc *peerClient) teardownCall(cctx *callContext, cctxPut bool) {
	pc.mu.Lock()
	delete(pc.calls, cctx.queryID)
	pc.mu.Unlock()

	// no need to hold mutex here, since we are the sole owners of cctx
	if cctxPut {
		select {
		case <-cctx.closed:
		default:
			putCallContext(cctx)
		}
	}
}

func (pc *peerClient) findCall(queryID int64) (*callContext, bool) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if pc.closed {
		return nil, false
	}

	return pc.calls[queryID], true
}

func (pc *peerClient) continueRunning(didConnect bool) bool {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	// free all possible resources in writeQ
	nQueued := 0
	for _, wr := range pc.writeQ {
		if wr.cctx != nil && wr.cctx.stale.Load() {
			pc.putStaleRequest(wr)
		} else {
			pc.writeQ[nQueued] = wr
			nQueued++
		}
	}
	for i := nQueued; i < len(pc.writeQ); i++ {
		pc.writeQ[i] = writeReq{}
	}
	pc.writeQ = pc.writeQ[:nQueued]

	// unblock all possible calls
	allSent := true
	for _, cctx := range pc.calls {
		if cctx.sent.Load() || cctx.failIfNoConnection {
			cctx.close()
		} else {
			allSent = false
		}
	}

	if pc.closed || allSent {
		pc.running = false
		return false
	}

	if !didConnect {
		pc.waitingToReconnect = true
	}
	return true
}

func (pc *peerClient) putStaleRequest(wr writeReq) {
	putCallContext(wr.cctx)
	pc.client.putRequest(wr.req)
}

func (pc *peerClient) setClientConn(cc *clientConn) bool {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if pc.closed {
		return false
	}

	pc.conn = cc
	pc.waitingToReconnect = false

	return true
}

func (pc *peerClient) dropClientConn(cc *clientConn) {
	pc.mu.Lock()
	notify := pc.conn != nil
	pc.conn = nil
	pc.mu.Unlock()

	_ = cc.Close()

	if notify {
		pc.writeQCond.Broadcast()
	}
}

func (pc *peerClient) runLoop() {
	var reconnectDelay time.Duration
	for {
		didConnect := pc.run()
		if !pc.continueRunning(didConnect) {
			return
		}
		// waitingToReconnect is true here if !didConnect

		switch {
		case didConnect:
			reconnectDelay = 0
		case reconnectDelay < minReconnectDelay:
			reconnectDelay = minReconnectDelay
		default:
			reconnectDelay *= 2
			if reconnectDelay > maxReconnectDelay {
				reconnectDelay = maxReconnectDelay
			}
		}
		pc.logf("rpc: reconnecting to %v in %v", pc.address, reconnectDelay)
		time.Sleep(reconnectDelay)
	}
}

func (pc *peerClient) run() bool {
	address := srvfunc.MaybeResolveHost(pc.address.Network, pc.address.Address)
	nc, err := net.DialTimeout(pc.address.Network, address, DefaultHandshakeStepTimeout)
	if err != nil {
		pc.logf("rpc: failed to start new peer connection with %v: %v", pc.address, err)
		return false
	}

	c := NewPacketConn(nc, pc.client.connReadBufSize, pc.client.connWriteBufSize, DefaultConnTimeoutAccuracy)
	defer func() { _ = c.Close() }()

	err = c.HandshakeClient(pc.cryptoKey, pc.client.trustedSubnetGroups, pc.forceEncryption, uniqueStartTime(), 0, DefaultHandshakeStepTimeout)
	if err != nil {
		pc.logf("rpc: failed to establish new peer connection with %v: %v", pc.address, err)
		return false
	}

	cc := &clientConn{
		client: pc.client,
		conn:   c,
		close:  make(chan struct{}),
	}
	if !pc.setClientConn(cc) {
		return false
	}

	var wg sync.WaitGroup
	pong := make(chan int64, 1)

	wg.Add(3)
	go pc.pingLoop(cc, &wg, pong, pc.client.pongTimeout)
	go pc.sendLoop(cc, &wg)
	go pc.receiveLoop(cc, &wg, pong)
	wg.Wait()

	return true
}

func (pc *peerClient) pingLoop(cc *clientConn, wg *sync.WaitGroup, pong <-chan int64, pongTimeout time.Duration) {
	defer wg.Done()
	defer pc.dropClientConn(cc)

	pingTimer := time.NewTimer(clientPingInterval)
	defer pingTimer.Stop()

	pongTimeoutTimer := time.NewTimer(pongTimeout)
	pongTimeoutTimer.Stop()
	defer pongTimeoutTimer.Stop()

	pingID := int64(0)
	for {
		select {
		case <-cc.close:
			return
		case <-pingTimer.C:
			pingID++
			pc.writeQPush(writeReq{pingPongPacketType: packetTypeRPCPing, pingPongID: pingID})
			pongTimeoutTimer.Reset(pongTimeout)
		case pongID := <-pong:
			if pongID != pingID {
				pc.logf("rpc: got pong(%v) in response to ping(%v) from %v, disconnecting", pongID, pingID, cc.conn.remoteAddr)
				return
			}
			if !pongTimeoutTimer.Stop() {
				<-pongTimeoutTimer.C
			}
			pingTimer.Reset(clientPingInterval)
		case <-pongTimeoutTimer.C:
			pc.logf("rpc: did not receive pong from %v in %v, disconnecting", cc.conn.remoteAddr, DefaultClientPongTimeout)
			return
		}
	}
}

func (pc *peerClient) sendLoop(cc *clientConn, wg *sync.WaitGroup) {
	defer wg.Done()
	defer pc.dropClientConn(cc)

	var buf []writeReq
	for {
		buf = pc.writeQAcquire(buf[:0])
		if len(buf) == 0 {
			return
		}

		for _, wr := range buf {
			switch {
			case wr.cctx != nil && wr.cctx.stale.Load():
				pc.putStaleRequest(wr) // avoid sending stale requests
			case wr.pingPongPacketType != 0:
				err := cc.writePingPongUnlocked(wr.pingPongPacketType, wr.pingPongID, DefaultClientPongTimeout)
				if err != nil {
					if !cc.closed() {
						pc.logf("rpc: failed to send ping/pong to %v, disconnecting: %v", cc.conn.remoteAddr, err)
					}
					return
				}
			default:
				err := cc.writeRequestUnlocked(wr.cctx.queryID, wr.req, wr.deadline, maxPacketRWTime)
				if err != nil {
					if !cc.closed() {
						pc.logf("rpc: failed to send packet to %v, disconnecting: %v", cc.conn.remoteAddr, err)
					}
					return
				}
			}
		}

		if pc.writeQFlush() {
			err := cc.conn.writeFlushUnlocked()
			if err != nil {
				if !cc.closed() {
					pc.logf("rpc: failed to flush packets to %v, disconnecting: %v", cc.conn.remoteAddr, err)
				}
				return
			}
		}
	}
}

func (pc *peerClient) receiveLoop(cc *clientConn, wg *sync.WaitGroup, pong chan<- int64) {
	defer wg.Done()
	defer pc.dropClientConn(cc)

	for {
		pkt := pc.client.getResponse() // we use Response as a scratch space for incoming (even non-response) packet here
		var typ uint32
		var err error
		typ, pkt.Body, err = cc.conn.ReadPacket(pkt.body, maxIdleDuration+maxPacketRWTime)
		pkt.body = pkt.Body[:0] // prepare for reuse immediately
		if err != nil {
			pc.client.PutResponse(pkt)
			if !cc.closed() {
				pc.logf("rpc: error reading packet from %v, disconnecting: %v", cc.conn.remoteAddr, err)
			}
			return
		}
		pkt.responseType = typ
		err = pc.handlePacket(pkt, pong)
		if err != nil {
			pc.logf("rpc: failed to handle packet from %v, disconnecting: %v", cc.conn.remoteAddr, err)
			return
		}
	}
}

func (pc *peerClient) handlePacket(pkt *Response, pong chan<- int64) error {
	put := true
	defer func() {
		if put {
			pc.client.PutResponse(pkt)
		}
	}()

	switch pkt.responseType {
	case packetTypeRPCPing:
		var pongID int64
		req, err := basictl.LongRead(pkt.Body, &pongID)
		if err != nil {
			return fmt.Errorf("error reading ping: %w", err)
		}
		if len(req) != 0 {
			return fmt.Errorf("excess %d bytes in ping packet", len(req))
		}
		pc.writeQPush(writeReq{pingPongPacketType: packetTypeRPCPong, pingPongID: pongID})
		return nil
	case packetTypeRPCPong:
		var pongID int64
		req, err := basictl.LongRead(pkt.Body, &pongID)
		if err != nil {
			return fmt.Errorf("error reading pong: %w", err)
		}
		if len(req) != 0 {
			return fmt.Errorf("excess %d bytes in pong packet", len(req))
		}
		pong <- pongID
		return nil
	case packetTypeRPCReqResult, packetTypeRPCReqError:
		var queryID int64
		var err error
		pkt.Body, err = basictl.LongRead(pkt.Body, &queryID)
		if err != nil {
			return fmt.Errorf("failed to read response query ID: %w", err)
		}
		put, err = pc.handleResponse(queryID, pkt, pkt.responseType == packetTypeRPCReqError)
		if err != nil {
			return fmt.Errorf("failed to handle RPC response: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("unexpected packet type 0x%x", pkt.responseType)
	}
}

func (pc *peerClient) handleResponse(queryID int64, resp *Response, toplevelError bool) (put bool, err error) {
	put = true
	cctx, ok := pc.findCall(queryID)
	if cctx == nil || !ok {
		// we expect that cctx can be nil because of teardownCall after context was done (and not because server decided to send garbage)
		return
	}

	var r callResult
	defer func() {
		hookErr := err
		if err == nil {
			hookErr = r.err
		}
		pc.client.hooks.Request.AfterReceive(cctx.hooksState, r.ok, hookErr)

		if err == nil {
			if cctx.multiFinished != nil {
				cctx.multiFinished <- cctx.queryID
			}
			cctx.result <- r // synchronization point; control is transferred
		}
	}()

	if toplevelError {
		if resp.Body, err = basictl.IntRead(resp.Body, &r.err.Code); err != nil {
			return
		}
		if resp.Body, err = basictl.StringRead(resp.Body, &r.err.Description); err != nil {
			return
		}
	} else {
		var tag uint32
		var afterTag []byte
		extraSet := 0
		for {
			if afterTag, err = basictl.NatRead(resp.Body, &tag); err != nil {
				return
			}
			if tag != reqResultHeaderTag {
				break
			}
			var extra ReqResultExtra
			if resp.Body, err = extra.Read(afterTag); err != nil {
				return
			}
			if extraSet == 0 {
				resp.Extra = extra
			}
			extraSet++
		}
		if extraSet > 1 {
			pc.logf("rpc: ResultExtra set more than once (%d) for result tag #%08d; please report to infrastructure team", extraSet, tag)
		}

		if tag == reqResultErrorTag {
			var unused int64 // excess query_id erroneously saved by incorrect serialization of RpcReqResult object tree
			if afterTag, err = basictl.LongRead(afterTag, &unused); err != nil {
				return
			}
		}
		if tag == reqResultErrorTag || tag == reqResultErrorWrappedTag {
			if resp.Body, err = basictl.IntRead(afterTag, &r.err.Code); err != nil {
				return
			}
			if resp.Body, err = basictl.StringRead(resp.Body, &r.err.Description); err != nil {
				return
			}
		} else {
			r.ok = resp
		}
	}

	put = r.ok == nil
	return
}

type clientConn struct {
	client *Client
	conn   *PacketConn

	closeOnce sync.Once
	closeErr  error
	close     chan struct{}
}

func (cc *clientConn) Close() error {
	cc.closeOnce.Do(cc.doClose)
	return cc.closeErr
}

func (cc *clientConn) doClose() {
	close(cc.close)
	cc.closeErr = cc.conn.Close()
}

func (cc *clientConn) closed() bool {
	select {
	case <-cc.close:
		return true
	default:
		return false
	}
}

func (cc *clientConn) writePingPongUnlocked(packetType uint32, pingPong int64, writeTimeout time.Duration) error {
	if err := cc.conn.startWritePacketUnlocked(packetType, writeTimeout); err != nil {
		return err
	}
	cc.conn.headerWriteBuf = basictl.LongWrite(cc.conn.headerWriteBuf, pingPong)
	crc, err := cc.conn.writePacketHeaderUnlocked(0)
	if err != nil {
		return err
	}
	// body is empty
	return cc.conn.writePacketTrailerUnlocked(crc, 0)
}

func (cc *clientConn) writeRequestUnlocked(queryID int64, req *Request, deadline time.Time, writeTimeout time.Duration) (err error) {
	defer cc.client.putRequest(req)

	if !deadline.IsZero() && !req.Extra.IsSetCustomTimeoutMs() {
		req.Extra.SetCustomTimeoutMs(int32(time.Until(deadline).Milliseconds()))
	}

	if err = cc.conn.startWritePacketUnlocked(packetTypeRPCInvokeReq, writeTimeout); err != nil {
		return err
	}
	headerBuf := basictl.LongWrite(cc.conn.headerWriteBuf, queryID) // move to local var, then back for speed
	switch {
	case req.ActorID != 0 && req.Extra.flags != 0:
		headerBuf = basictl.NatWrite(headerBuf, destActorFlagsTag)
		headerBuf = basictl.LongWrite(headerBuf, int64(req.ActorID))
		if headerBuf, err = req.Extra.Write(headerBuf); err != nil {
			return fmt.Errorf("failed to write extra: %w", err)
		}
	case req.Extra.flags != 0:
		headerBuf = basictl.NatWrite(headerBuf, destFlagsTag)
		if headerBuf, err = req.Extra.Write(headerBuf); err != nil {
			return fmt.Errorf("failed to write extra: %w", err)
		}
	case req.ActorID != 0:
		headerBuf = basictl.NatWrite(headerBuf, destActorTag)
		headerBuf = basictl.LongWrite(headerBuf, int64(req.ActorID))
	}
	cc.conn.headerWriteBuf = headerBuf
	return cc.conn.writeSimplePacketUnlocked(req.Body)
}

func (c *Client) GetRequest() *Request {
	v := c.requestPool.Get()
	if v != nil {
		return v.(*Request)
	}
	return &Request{HookArgs: c.hooks.Request.InitArguments()}
}

func (c *Client) putRequest(req *Request) {
	req.reset(c.hooks.Request.ResetArguments)
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
		// be forgiving and do not blow up everything in the rare case of RPC error
		return
	}

	resp.reset()
	c.responsePool.Put(resp)
}

func getCallContext(initState func() any) *callContext {
	v := callCtxPool.Get()
	if v != nil {
		return v.(*callContext)
	}
	return newCallContext(initState)
}

func putCallContext(cctx *callContext) {
	cctx.reset()
	callCtxPool.Put(cctx)
}

func (pc *peerClient) writeQPush(wr writeReq) {
	pc.mu.Lock()
	pc.writeQ = append(pc.writeQ, wr)
	pc.mu.Unlock()

	pc.writeQCond.Signal() // signal without holding the mutex to reduce contention
}

func (pc *peerClient) writeQAcquire(q []writeReq) []writeReq {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	for pc.conn != nil && len(pc.writeQ) == 0 {
		pc.writeQCond.Wait()
	}

	if pc.conn == nil {
		return nil
	}

	ret := q
	for i, wr := range pc.writeQ {
		if wr.cctx != nil {
			wr.cctx.sent.Store(true)
		}
		pc.writeQ[i] = writeReq{}
		ret = append(ret, wr)
	}
	pc.writeQ = pc.writeQ[:0]

	return ret
}

func (pc *peerClient) writeQFlush() bool {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	return pc.conn == nil || len(pc.writeQ) == 0
}
