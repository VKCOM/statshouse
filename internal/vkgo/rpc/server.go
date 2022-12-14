// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"go.uber.org/atomic"
	"go.uber.org/multierr"
	"golang.org/x/net/netutil"
	"golang.org/x/sys/unix"

	"github.com/vkcom/statshouse/internal/vkgo/basictl"
	"github.com/vkcom/statshouse/internal/vkgo/rpc/internal/boundedpool"
	"github.com/vkcom/statshouse/internal/vkgo/semaphore"
	"github.com/vkcom/statshouse/internal/vkgo/srvfunc"
)

// TODO: reap free workers periodically
// TODO: graceful shutdown
// TODO: explain safety rules: no ctx/buffer use outside of the handler
// TODO: experiment with github.com/mailru/easygo/netpoll or github.com/xtaci/gaio

const (
	DefaultMaxWorkers             = 1024
	DefaultMaxConns               = 131072 // note, this number of connections will require 10+ GB of memory
	DefaultMaxInflightPackets     = 256
	DefaultRequestMemoryLimit     = 256 * 1024 * 1024
	DefaultResponseMemoryLimit    = 2048 * 1024 * 1024
	DefaultServerConnReadBufSize  = maxGoAllocSizeClass
	DefaultServerConnWriteBufSize = maxGoAllocSizeClass
	DefaultServerRequestBufSize   = 4096                // TODO: should be at least bytes.MinRead for now
	DefaultServerResponseBufSize  = maxGoAllocSizeClass // TODO: should be at least bytes.MinRead for now
	DefaultResponseMemEstimate    = 1024 * 1024         // we likely over-account unknown response length before the handler has finished

	minAcceptDelay  = 5 * time.Millisecond
	maxAcceptDelay  = 1 * time.Second
	rpsCalcInterval = 5 * time.Second
	rareLogInterval = 1 * time.Second

	maxGoAllocSizeClass = 32768
	tracebackBufSize    = 65536
)

var (
	ErrServerClosed   = errors.New("rpc: Server closed")
	ErrNoHandler      = &Error{Code: tlErrorNoHandler, Description: "rpc: no handler"} // Never wrap this error
	errHijackResponse = errors.New("rpc: user of Server is now responsible for sending the response")

	statCPUInfo = srvfunc.MakeCPUInfo()
)

type (
	HandlerFunc          func(ctx context.Context, hctx *HandlerContext) error
	StatsHandlerFunc     func(map[string]string)
	VerbosityHandlerFunc func(int) error

	LoggerFunc func(format string, args ...interface{})
)

func ChainHandler(ff ...HandlerFunc) HandlerFunc {
	return func(ctx context.Context, hctx *HandlerContext) error {
		for _, f := range ff {
			if err := f(ctx, hctx); err != ErrNoHandler {
				return err
			}
		}
		return ErrNoHandler
	}
}

// HandlerContext must not be used outside the handler
type HandlerContext struct {
	ActorID     []uint64
	QueryID     int64
	RequestTime time.Time
	listenAddr  net.Addr
	localAddr   net.Addr
	remoteAddr  net.Addr
	keyID       [4]byte

	request  *[]byte // pointer for reuse. Holds allocated slice which will be put into sync pool, has always len 0
	Request  []byte
	response *[]byte // pointer for reuse. Holds allocated slice which will be put into sync pool, has always len 0
	Response []byte

	RequestExtra           []InvokeReqExtra // every proxy added its own extra. Clients must take into account RequestExtra[0] (last one added)
	ResponseExtra          ReqResultExtra   // everything we set here will be sent if client requested it (bit of RequestExtra[0].flags set)
	requestExtraFieldsmask uint32           // defensive copy

	// UserData allows caching common state between different requests.
	UserData interface{}

	serverConn     *serverConn
	reqHeader      packetHeader
	reqType        uint32 // actual request can be wrapped 0 or more times within reqHeader
	respTaken      int
	respPacketType uint32
	noResult       bool  // defensive copy
	queryID        int64 // defensive copy
	defaultTimeout time.Duration
	timeoutAdjust  time.Duration
}

type handlerContextKey struct{}

// rpc.HandlerContext must never be used outside of the handler
func GetHandlerContext(ctx context.Context) *HandlerContext {
	hctx, _ := ctx.Value(handlerContextKey{}).(*HandlerContext)
	return hctx
}

func (hctx *HandlerContext) WithContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, handlerContextKey{}, hctx)
}

func (hctx *HandlerContext) KeyID() [4]byte       { return hctx.keyID }
func (hctx *HandlerContext) ListenAddr() net.Addr { return hctx.remoteAddr }
func (hctx *HandlerContext) LocalAddr() net.Addr  { return hctx.localAddr }
func (hctx *HandlerContext) RemoteAddr() net.Addr { return hctx.remoteAddr }

func (hctx *HandlerContext) timeout() time.Duration {
	timeout := hctx.defaultTimeout
	for _, extra := range hctx.RequestExtra {
		if extra.IsSetCustomTimeoutMs() {
			curTimeout := time.Duration(extra.CustomTimeoutMs) * time.Millisecond
			if timeout == hctx.defaultTimeout || timeout > curTimeout {
				timeout = curTimeout
			}
		}
	}
	if timeout > hctx.timeoutAdjust {
		timeout -= hctx.timeoutAdjust
	}
	return timeout
}

func (hctx *HandlerContext) deadline() time.Time {
	timeout := hctx.timeout()
	if timeout != 0 {
		return hctx.RequestTime.Add(timeout)
	}
	return time.Time{}
}

func (hctx *HandlerContext) releaseRequest() {
	hctx.serverConn.server.releaseRequestBuf(int(hctx.reqHeader.length), hctx.request)
	hctx.request = nil
	hctx.Request = nil
}

func (hctx *HandlerContext) releaseResponse() {
	if hctx.response != nil {
		// if Response was reallocated and became too big, we will reuse original slice we got from pool
		// otherwise, we will move reallocated slice into slice in heap
		if cap(hctx.Response) <= hctx.serverConn.server.responseBufSize() {
			*hctx.response = hctx.Response[:0]
		}
	}
	hctx.serverConn.server.releaseResponseBuf(hctx.respTaken, hctx.response)
	hctx.response = nil
	hctx.Response = nil
}

func (hctx *HandlerContext) reset() {
	// We do not preserve map in ResponseExtra because strings will not be reused anyway
	*hctx = HandlerContext{
		ActorID:      hctx.ActorID[:0],
		RequestExtra: hctx.RequestExtra[:0],

		UserData: hctx.UserData,

		// HandlerContext is bound to the connection, so these are always valid
		serverConn: hctx.serverConn,
		listenAddr: hctx.listenAddr,
		localAddr:  hctx.localAddr,
		remoteAddr: hctx.remoteAddr,
		keyID:      hctx.keyID,
	}
}

func (hctx *HandlerContext) AccountResponseMem(respBodySizeEstimate int) error {
	var err error
	hctx.respTaken, err = hctx.serverConn.server.accountResponseMem(hctx.serverConn.closeCtx, hctx.respTaken, respBodySizeEstimate, false)
	return err
}

// HijackResponse releases Request bytes for reuse, so must be called only after Request processing is complete
func (hctx *HandlerContext) HijackResponse() error {
	hctx.releaseRequest()
	return errHijackResponse
}

func (hctx *HandlerContext) SendHijackedResponse(err error) {
	hctx.prepareResponse(err)
	hctx.serverConn.server.pushResponse(hctx)
}

type Server struct {
	// Place atomics first to ensure proper alignment, see https://pkg.go.dev/sync/atomic#pkg-note-BUG
	statConnectionsTotal   atomic.Int64
	statConnectionsCurrent atomic.Int64
	statRequestsTotal      atomic.Int64
	statRequestsCurrent    atomic.Int64
	statRPS                atomic.Int64
	statRequestMemory      atomic.Int64
	statResponseMemory     atomic.Int64
	statOnce               sync.Once
	statHostname           string

	Handler          HandlerFunc
	StatsHandler     StatsHandlerFunc
	VerbosityHandler VerbosityHandlerFunc
	Version          string
	Logf             LoggerFunc // defaults to log.Printf; set to NoopLogf to disable all logging
	loggerOnce       sync.Once

	TransportHijackHandler func(conn *PacketConn) // Experimental, server handles connection to this function if FlagHijackTransport client flag set

	TrustedSubnetGroups    [][]string
	ForceEncryption        bool
	CryptoKeys             []string
	MaxConns               int           // defaults to DefaultMaxConns
	MaxWorkers             int           // defaults to DefaultMaxWorkers; negative values disable worker pool completely
	MaxInflightPackets     int           // defaults to DefaultMaxInflightPackets
	RequestMemoryLimit     int           // defaults to DefaultRequestMemoryLimit
	ResponseMemoryLimit    int           // defaults to DefaultResponseMemoryLimit
	ConnReadBufSize        int           // defaults to DefaultServerConnReadBufSize
	ConnWriteBufSize       int           // defaults to DefaultServerConnWriteBufSize
	RequestBufSize         int           // defaults to DefaultServerRequestBufSize
	ResponseBufSize        int           // defaults to DefaultServerResponseBufSize
	ResponseMemEstimate    int           // defaults to DefaultResponseMemEstimate; must be greater than ResponseBufSize
	DefaultResponseTimeout time.Duration // defaults to no timeout
	ResponseTimeoutAdjust  time.Duration
	DisableContextTimeout  bool
	DisableTCPReuseAddr    bool
	LogCommonNetworkErrors bool

	mu             sync.Mutex
	closed         bool
	listeners      map[*net.Listener]struct{}
	conns          map[*serverConn]struct{}
	workers        []*worker
	workerPoolOnce sync.Once
	workerPool     *boundedpool.T
	goroutineGroup sync.WaitGroup

	semOnce        sync.Once
	closeCtx       context.Context
	cancelCloseCtx context.CancelFunc
	reqMemSem      *semaphore.Weighted
	respMemSem     *semaphore.Weighted
	reqBufPool     sync.Pool
	respBufPool    sync.Pool

	startTimeOnce       sync.Once
	startTime           int32
	trustedSubnetsOnce  sync.Once
	trustedSubnetGroups [][]*net.IPNet

	closeOnce sync.Once
	closeErr  error

	rareLogMu          sync.Mutex
	lastReqMemWaitLog  time.Time
	lastRespMemWaitLog time.Time
	lastHctxWaitLog    time.Time
	lastWorkerWaitLog  time.Time
}

// Close stops server from accepting new requests, closes all connections and waits for all goroutines to exit.
func (s *Server) Close() error {
	s.initLog()
	s.initSem()
	s.initStats() // ensure WaitGroup.Add happens before Wait
	s.initWorkerPool()

	s.closeOnce.Do(s.doClose)
	s.goroutineGroup.Wait()

	return s.closeErr
}

func (s *Server) doClose() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.cancelCloseCtx()

	if s.workerPool != nil {
		err := s.workerPool.Close()
		multierr.AppendInto(&s.closeErr, err)
	}

	for ln := range s.listeners {
		err := (*ln).Close()
		multierr.AppendInto(&s.closeErr, err)
	}

	for sc := range s.conns {
		err := sc.Close()
		multierr.AppendInto(&s.closeErr, err)
	}

	for _, w := range s.workers {
		close(w.ch)
	}

	s.closed = true
}

func (s *Server) shuttingDown() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.closed
}

func (s *Server) maxConns() int {
	if s.MaxConns > 0 {
		return s.MaxConns
	}
	return DefaultMaxConns
}

func (s *Server) maxWorkers() int {
	switch {
	case s.MaxWorkers > 0:
		return s.MaxWorkers
	case s.MaxWorkers == -1:
		return 0
	default:
		return DefaultMaxWorkers
	}
}

func (s *Server) maxInflightPackets() int {
	if s.MaxInflightPackets > 0 {
		return s.MaxInflightPackets
	}
	return DefaultMaxInflightPackets
}

func (s *Server) requestMemoryLimit() int {
	if s.RequestMemoryLimit > maxPacketLen {
		return s.RequestMemoryLimit
	}
	return DefaultRequestMemoryLimit
}

func (s *Server) responseMemoryLimit() int {
	if s.ResponseMemoryLimit > maxPacketLen {
		return s.ResponseMemoryLimit
	}
	return DefaultResponseMemoryLimit
}

func (s *Server) connReadBufSize() int {
	if s.ConnReadBufSize > 0 {
		return s.ConnReadBufSize
	}
	return DefaultServerConnReadBufSize
}

func (s *Server) connWriteBufSize() int {
	if s.ConnWriteBufSize > 0 {
		return s.ConnWriteBufSize
	}
	return DefaultServerConnWriteBufSize
}

func (s *Server) requestBufSize() int {
	if s.RequestBufSize > 0 {
		return s.RequestBufSize
	}
	return DefaultServerRequestBufSize
}

func (s *Server) responseBufSize() int {
	if s.ResponseBufSize > 0 {
		return s.ResponseBufSize
	}
	return DefaultServerResponseBufSize
}

func (s *Server) responseMemEstimate() int {
	switch {
	case s.ResponseMemEstimate > 0:
		return s.ResponseMemEstimate
	case s.ResponseMemEstimate == -1:
		return 0
	default:
		return DefaultResponseMemEstimate
	}
}

func (s *Server) defaultResponseTimeout() time.Duration {
	if s.DefaultResponseTimeout > 0 {
		return s.DefaultResponseTimeout
	}
	return 0
}

func (s *Server) responseTimeoutAdjust() time.Duration {
	if s.ResponseTimeoutAdjust > 0 {
		return s.ResponseTimeoutAdjust
	}
	return 0
}

func (s *Server) requestBufTake(reqBodySize int) int {
	return max(reqBodySize, s.requestBufSize()) // we consider that most buffers in the pool will be stretched up to max size
}

func (s *Server) responseBufTake(respBodySizeEstimate int) int {
	return max(respBodySizeEstimate, s.responseBufSize()) // we consider that most buffers in the pool will be stretched up to max size
}

func (s *Server) acquireRequestBuf(ctx context.Context, reqBodySize int) (*[]byte, error) {
	take := s.requestBufTake(reqBodySize)
	ok := s.reqMemSem.TryAcquire(int64(take))
	if !ok {
		s.rareLog(&s.lastReqMemWaitLog,
			"rpc: waiting to acquire request memory (want %d, mem %.3f, limit %.3f, %d conns, %d reqs); consider increasing Server.RequestMemoryLimit",
			take,
			float64(s.statRequestMemory.Load())/1024/1024,
			float64(s.requestMemoryLimit())/1024/1024,
			s.statConnectionsCurrent.Load(),
			s.statRequestsCurrent.Load(),
		)
		err := s.reqMemSem.Acquire(ctx, int64(take))
		if err != nil {
			return nil, err
		}
	}
	s.statRequestMemory.Add(int64(take))

	if take > s.requestBufSize() {
		return nil, nil // large requests will not go back to pool, so fall back to GC
	}

	v := s.reqBufPool.Get()
	if v != nil {
		return v.(*[]byte), nil
	}
	var b []byte // allocate heap slice, which will be put into pool in releaseRequest
	return &b, nil
}

func (s *Server) releaseRequestBuf(reqBodySize int, buf *[]byte) {
	taken := s.requestBufTake(reqBodySize)
	s.reqMemSem.Release(int64(taken))
	s.statRequestMemory.Sub(int64(taken))

	if buf != nil {
		s.reqBufPool.Put(buf)
	}
}

func (s *Server) acquireResponseBuf(ctx context.Context) (*[]byte, int, error) {
	take := s.responseBufTake(s.responseMemEstimate())
	ok := s.respMemSem.TryAcquire(int64(take))
	if !ok {
		s.rareLog(&s.lastRespMemWaitLog,
			"rpc: waiting to acquire response memory (want %d, mem %.3f, limit %.3f, %d conns, %d reqs); consider increasing Server.ResponseMemoryLimit or lowering Server.ResponseMemEstimate",
			take,
			float64(s.statResponseMemory.Load())/1024/1024,
			float64(s.responseMemoryLimit())/1024/1024,
			s.statConnectionsCurrent.Load(),
			s.statRequestsCurrent.Load(),
		)
		err := s.respMemSem.Acquire(ctx, int64(take))
		if err != nil {
			return nil, 0, err
		}
	}
	s.statResponseMemory.Add(int64(take))

	// we do not know if handler will write large or small response, so we always give small response from pool
	// if handler will write large response, we will release in releaseResponseBuf, as with requests

	// we hope (but can not guarantee) that buffer will go back to pool
	v := s.respBufPool.Get()
	if v != nil {
		return v.(*[]byte), take, nil
	}
	var b []byte // allocate heap slice, which will be put into pool in releaseResponse
	return &b, take, nil
}

func (s *Server) accountResponseMem(ctx context.Context, taken int, respBodySizeEstimate int, force bool) (int, error) {
	need := s.responseBufTake(respBodySizeEstimate)
	if need > taken {
		want := int64(need - taken)
		var ok bool
		if force {
			s.respMemSem.ForceAcquire(want)
			ok = true
		} else {
			ok = s.respMemSem.TryAcquire(want)
		}
		if !ok {
			s.rareLog(&s.lastRespMemWaitLog,
				"rpc: waiting to acquire response memory (want %d, mem %.3f, limit %.3f, %d conns, %d reqs); consider increasing Server.ResponseMemoryLimit",
				want,
				float64(s.statResponseMemory.Load())/1024/1024,
				float64(s.responseMemoryLimit())/1024/1024,
				s.statConnectionsCurrent.Load(),
				s.statRequestsCurrent.Load(),
			)
			err := s.respMemSem.Acquire(ctx, want)
			if err != nil {
				return taken, err
			}
		}
		s.statResponseMemory.Add(want)
	} else {
		dontNeed := int64(taken - need)
		s.respMemSem.Release(dontNeed)
		s.statResponseMemory.Sub(dontNeed)
	}
	return need, nil
}

func (s *Server) releaseResponseBuf(taken int, buf *[]byte) { // buf is never nil
	s.respMemSem.Release(int64(taken))
	s.statResponseMemory.Sub(int64(taken))

	if buf != nil {
		s.respBufPool.Put(buf) // we always reuse heap-allocated slice
	}
}

func (s *Server) initLog() {
	s.loggerOnce.Do(func() {
		if s.Logf == nil {
			s.Logf = log.Printf
		}
	})
}

func (s *Server) initSem() {
	s.semOnce.Do(func() {
		s.closeCtx, s.cancelCloseCtx = context.WithCancel(context.Background())
		s.reqMemSem = semaphore.NewWeighted(int64(s.requestMemoryLimit()))
		s.respMemSem = semaphore.NewWeighted(int64(s.responseMemoryLimit()))
	})
}

func (s *Server) initStartTime() {
	s.startTimeOnce.Do(func() {
		s.startTime = uniqueStartTime()
	})
}

func (s *Server) initWorkerPool() {
	s.initLog()
	s.workerPoolOnce.Do(func() {
		s.workerPool = boundedpool.New(s.maxWorkers(), func() {
			s.rareLog(&s.lastWorkerWaitLog, "rpc: waiting to acquire worker; consider increasing Server.MaxWorkers")
		})
	})
}

func (s *Server) initStats() {
	s.initSem() // for s.closeCtx

	s.statOnce.Do(func() {
		host, _ := os.Hostname()
		s.statHostname = host

		s.goroutineGroup.Add(1)
		go s.rpsCalcLoop(&s.goroutineGroup)
	})
}

func (s *Server) rpsCalcLoop(wg *sync.WaitGroup) {
	defer wg.Done()
	tick := time.NewTicker(rpsCalcInterval)
	defer tick.Stop()
	prev := s.statRequestsTotal.Load()
	for {
		select {
		case <-tick.C:
			cur := s.statRequestsTotal.Load()
			s.statRPS.Store((cur - prev) / int64(rpsCalcInterval/time.Second))
			prev = cur
		case <-s.closeCtx.Done():
			return
		}
	}
}

func (s *Server) initTrustedSubnets() {
	s.trustedSubnetsOnce.Do(func() {
		gs, errs := ParseTrustedSubnets(s.TrustedSubnetGroups)
		s.trustedSubnetGroups = gs
		for _, err := range errs {
			s.Logf("[rpc] failed to parse server trusted subnet %q, ignoring", err)
		}
	})
}

// ListenAndServe supports only "tcp4" and "unix" networks
func (s *Server) ListenAndServe(network string, address string) error {
	if network != "tcp4" && network != "unix" {
		return fmt.Errorf("unsupported network type %q", network)
	}

	var lc net.ListenConfig
	if !s.DisableTCPReuseAddr {
		lc.Control = controlSetTCPReuseAddr
	}

	ln, err := lc.Listen(context.Background(), network, address)
	if err != nil {
		return err
	}

	return s.Serve(ln)
}

func (s *Server) Serve(ln net.Listener) error {
	if s.requestBufSize() < bytes.MinRead {
		return fmt.Errorf("Server.RequestBufSize should be at least %v", bytes.MinRead)
	}
	if s.responseBufSize() < bytes.MinRead {
		return fmt.Errorf("Server.ResponseBufSize should be at least %v", bytes.MinRead)
	}

	s.initLog()
	s.initStartTime()
	s.initSem()
	s.initStats()
	s.initWorkerPool()
	s.initTrustedSubnets()

	ln = &closeOnceListener{Listener: netutil.LimitListener(ln, s.maxConns())}
	defer func() { _ = ln.Close() }()

	if !s.trackListener(&ln, true) {
		return ErrServerClosed
	}
	defer s.trackListener(&ln, false)

	var acceptDelay time.Duration
	for {
		nc, err := ln.Accept()
		if err != nil {
			if s.shuttingDown() {
				return ErrServerClosed
			}

			//lint:ignore SA1019 "FIXME: to ne.Timeout()"
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				if acceptDelay == 0 {
					acceptDelay = minAcceptDelay
				} else {
					acceptDelay *= 2
				}
				if acceptDelay > maxAcceptDelay {
					acceptDelay = maxAcceptDelay
				}
				s.Logf("rpc: Accept error: %v; retrying in %v", err, acceptDelay)
				time.Sleep(acceptDelay)
				continue
			}
			return err
		}
		conn := NewPacketConn(nc, s.connReadBufSize(), s.connWriteBufSize(), DefaultConnTimeoutAccuracy)

		s.statConnectionsTotal.Inc()
		s.statConnectionsCurrent.Inc()

		s.goroutineGroup.Add(1)
		go s.goHandshake(conn, ln.Addr(), &s.goroutineGroup)
	}
}

func (s *Server) goHandshake(conn *PacketConn, lnAddr net.Addr, wg *sync.WaitGroup) {
	magicHead, flags, err := conn.HandshakeServer(s.CryptoKeys, s.trustedSubnetGroups, s.ForceEncryption, s.startTime, DefaultHandshakeStepTimeout)
	if err != nil {
		switch string(magicHead) {
		case memcachedStatsReqRN, memcachedStatsReqN, memcachedGetStatsReq:
			s.respondWithMemcachedStats(conn)
		case memcachedVersionReq:
			s.respondWithMemcachedVersion(conn)
		default:
			if !commonConnCloseError(err) || s.LogCommonNetworkErrors {
				s.Logf("rpc: failed to handshake with %v, disconnecting: %v, magic head: %+q", conn.remoteAddr, err, magicHead)
			}
		}
		s.statConnectionsCurrent.Dec()
		wg.Done()
		_ = conn.Close()
		return
	}
	if flags&FlagHijackTransport == FlagHijackTransport && s.TransportHijackHandler != nil {
		// if flags match, but no TransportHijackHandler, we presume this is valid combination and proceed as usual
		s.statConnectionsCurrent.Dec()
		wg.Done()
		// at this point goroutine holds no resources, handler is free to use this goroutine for as long as it wishes
		s.TransportHijackHandler(conn)
		return
	}
	defer wg.Done()

	closeCtx, cancelCloseCtx := context.WithCancel(s.closeCtx)

	n := s.maxInflightPackets()
	sc := &serverConn{
		closeCtx:       closeCtx,
		cancelCloseCtx: cancelCloseCtx,
		server:         s,
		listenAddr:     lnAddr,
		hctxPool: boundedpool.New(n, func() {
			s.rareLog(&s.lastHctxWaitLog, "rpc: waiting to acquire handler context; consider increasing Server.MaxInflightPackets")
		}),
		conn:   conn,
		writeQ: newSWriteQ(n),
		close:  make(chan struct{}),
	}

	if !s.trackConn(sc) {
		_ = sc.Close()
		return
	}

	wg.Add(1)
	go s.sendLoop(sc, wg)
	s.receiveLoop(sc)
}

func (s *Server) rareLog(last *time.Time, format string, args ...interface{}) {
	now := time.Now()

	s.rareLogMu.Lock()
	defer s.rareLogMu.Unlock()

	if now.Sub(*last) > rareLogInterval {
		*last = now
		s.Logf(format, args...)
	}
}

type serverConn struct {
	closeOnce      sync.Once
	closeErr       error
	closeCtx       context.Context
	cancelCloseCtx context.CancelFunc

	server     *Server
	listenAddr net.Addr
	hctxPool   *boundedpool.T
	conn       *PacketConn
	writeQ     *sWriteQ
	close      chan struct{}
}

func (sc *serverConn) Close() error {
	sc.closeOnce.Do(sc.doClose)
	return sc.closeErr
}

func (sc *serverConn) doClose() {
	sc.server.statConnectionsCurrent.Dec()

	sc.cancelCloseCtx()

	err := sc.hctxPool.Close()
	multierr.AppendInto(&sc.closeErr, err)

	err = sc.conn.Close()
	multierr.AppendInto(&sc.closeErr, err)

	sc.writeQ.close()
	close(sc.close)
}

func (sc *serverConn) closed() bool {
	select {
	case <-sc.close:
		return true
	default:
		return false
	}
}

func (sc *serverConn) writeResponseUnlocked(hctx *HandlerContext, timeout time.Duration) (err error) {
	if err := sc.conn.startWritePacketUnlocked(hctx.respPacketType, timeout); err != nil {
		return err
	}
	if hctx.respPacketType == packetTypeRPCReqResult || hctx.respPacketType == packetTypeRPCReqError {
		sc.conn.headerWriteBuf = basictl.LongWrite(sc.conn.headerWriteBuf, hctx.queryID)
		hctx.ResponseExtra.flags &= hctx.requestExtraFieldsmask // return only fields they understand
		if hctx.respPacketType == packetTypeRPCReqResult && hctx.ResponseExtra.flags != 0 {
			sc.conn.headerWriteBuf = basictl.NatWrite(sc.conn.headerWriteBuf, reqResultHeaderTag)

			if sc.conn.headerWriteBuf, err = hctx.ResponseExtra.Write(sc.conn.headerWriteBuf); err != nil {
				return err // should be no errors during writing, though
			}
		}
	}
	return sc.conn.writeSimplePacketUnlocked(hctx.Response)
}

func (sc *serverConn) acquireHandlerCtx() (*HandlerContext, bool) {
	v, ok := sc.hctxPool.Get()
	if !ok {
		return nil, false
	}
	if v != nil {
		return v.(*HandlerContext), true
	}

	hctx := &HandlerContext{}
	hctx.serverConn = sc
	hctx.listenAddr = sc.listenAddr
	hctx.localAddr = sc.conn.conn.LocalAddr()
	hctx.remoteAddr = sc.conn.conn.RemoteAddr()
	hctx.keyID = sc.conn.keyID

	return hctx, true
}

func (sc *serverConn) releaseHandlerCtx(hctx *HandlerContext) {
	hctx.reset()
	sc.hctxPool.Put(hctx)
}

func (s *Server) acquireWorker() (*worker, bool) {
	if s.maxWorkers() == 0 {
		return nil, true
	}

	v, ok := s.workerPool.Get()
	if !ok {
		return nil, false
	}
	if v != nil {
		return v.(*worker), true
	}

	w := &worker{
		s:  s,
		ch: make(chan *HandlerContext, 1),
	}
	if !s.trackWorker(w) {
		return nil, false
	}

	s.goroutineGroup.Add(1)
	go w.run(&s.goroutineGroup)

	return w, true
}

func (s *Server) releaseWorker(w *worker) {
	s.workerPool.Put(w)
}

func (s *Server) receiveLoop(sc *serverConn) {
	defer s.dropConn(sc)

	var hctx *HandlerContext
	defer func() {
		if hctx != nil {
			hctx.releaseRequest()
			hctx.releaseResponse()
		}
	}()

	for {
		// read header first, before acquiring handler context,
		// to be able to disconnect event when all handler contexts are taken
		var header packetHeader
		head, err := sc.conn.readPacketHeaderUnlocked(&header, maxIdleDuration)
		if err != nil {
			if (len(head) > 0 && !sc.closed() && !commonConnCloseError(err)) || s.LogCommonNetworkErrors {
				s.Logf("rpc: error reading packet header from %v, disconnecting: %v, head: %+q", sc.conn.remoteAddr, err, head)
			}
			return
		}
		requestTime := time.Now()

		var ok bool
		hctx, ok = sc.acquireHandlerCtx()
		if !ok {
			return
		}
		hctx.RequestTime = requestTime
		hctx.defaultTimeout = s.defaultResponseTimeout()
		hctx.timeoutAdjust = s.responseTimeoutAdjust()
		hctx.reqHeader = header

		hctx.request, err = s.acquireRequestBuf(sc.closeCtx, int(hctx.reqHeader.length))
		if err != nil {
			return
		}
		if hctx.request != nil {
			hctx.Request = *hctx.request
		}
		hctx.Request, err = sc.conn.readPacketBodyUnlocked(&hctx.reqHeader, hctx.Request, true, maxPacketRWTime)
		if hctx.request != nil {
			*hctx.request = hctx.Request[:0] // prepare for reuse immediately
		}
		if err != nil {
			if !sc.closed() && (!commonConnCloseError(err) || s.LogCommonNetworkErrors) {
				s.Logf("rpc: error reading packet body from %v, disconnecting: %v", sc.conn.remoteAddr, err)
			}
			return
		}

		resp, respTaken, err := s.acquireResponseBuf(sc.closeCtx)
		if err != nil {
			return
		}
		hctx.response = resp
		hctx.Response = *hctx.response
		hctx.respTaken = respTaken

		w, ok := s.acquireWorker()
		if !ok {
			return
		}

		s.statRequestsTotal.Inc()
		s.statRequestsCurrent.Inc()

		if w != nil {
			w.tryPushWork(hctx)
		} else {
			s.handle(hctx)
		}
		hctx = nil // this loop does not own hctx anymore
	}
}

func (s *Server) sendLoop(sc *serverConn, wg *sync.WaitGroup) {
	defer wg.Done()
	defer s.dropConn(sc)

	releaseFrom := 0
	buf := make([]*HandlerContext, 0, s.maxInflightPackets())
	defer func() {
		for _, hctx := range buf[releaseFrom:] {
			hctx.releaseResponse()
		}
	}()

	for {
		releaseFrom = 0
		buf = sc.writeQ.acquire(buf[:0])
		if len(buf) == 0 {
			return
		}

		sent := false
		for _, hctx := range buf {
			if !hctx.noResult {
				err := sc.writeResponseUnlocked(hctx, maxPacketRWTime)
				if err != nil {
					if !sc.closed() && (!commonConnCloseError(err) || s.LogCommonNetworkErrors) {
						s.Logf("rpc: error writing packet 0x%x#0x%x to %v, disconnecting: %v", hctx.respPacketType, hctx.reqType, sc.conn.remoteAddr, err)
					}
					return
				}
				sent = true
			}

			hctx.releaseResponse()
			sc.releaseHandlerCtx(hctx)
			releaseFrom++
		}

		if sent && sc.writeQ.empty() {
			err := sc.conn.writeFlushUnlocked()
			if err != nil {
				if !sc.closed() && (!commonConnCloseError(err) || s.LogCommonNetworkErrors) {
					s.Logf("rpc: error flushing packet to %v, disconnecting: %v", sc.conn.remoteAddr, err)
				}
				return
			}
		}
	}
}

func (s *Server) handle(hctx *HandlerContext) {
	err := s.doHandle(hctx.serverConn.closeCtx, hctx)
	if err == errHijackResponse {
		// User is now responsible for calling hctx.SendHijackedResponse
		return
	}
	hctx.releaseRequest()
	hctx.prepareResponse(err)
	s.pushResponse(hctx)
}

func (s *Server) pushResponse(hctx *HandlerContext) {
	hctx.respTaken, _ = s.accountResponseMem(hctx.serverConn.closeCtx, hctx.respTaken, cap(hctx.Response), true)
	hctx.serverConn.writeQ.push(hctx)
	s.statRequestsCurrent.Dec()
}

func (s *Server) doHandle(ctx context.Context, hctx *HandlerContext) (err error) {
	switch hctx.reqHeader.tip {
	case packetTypeRPCPing:
		// This logic is not necessary, uncomment, if it is needed some day
		// var pingID int64
		// req, err := basictl.LongRead(hctx.Request, &pingID)
		// if err != nil {
		//	return fmt.Errorf("error reading ping: %w", err)
		// }
		// if len(req) != 0 {
		//	return fmt.Errorf("excess %d bytes in pong packet", len(req))
		// }
		hctx.respPacketType = packetTypeRPCPong
		hctx.Response = append(hctx.Response, hctx.Request...)
		return nil
	case packetTypeRPCInvokeReq:
		if hctx.Request, err = basictl.LongRead(hctx.Request, &hctx.queryID); err != nil {
			return fmt.Errorf("failed to read request query ID: %w", err)
		}
		hctx.QueryID = hctx.queryID

		var tag uint32
		var afterTag []byte
		for {
			if afterTag, err = basictl.NatRead(hctx.Request, &tag); err != nil {
				return fmt.Errorf("failed to read tag: %w", err)
			}
			if tag != destActorFlagsTag && tag != destFlagsTag && tag != destActorTag {
				break
			}
			hctx.Request = afterTag
			if tag == destActorFlagsTag || tag == destActorTag {
				var actorID int64
				if hctx.Request, err = basictl.LongRead(hctx.Request, &actorID); err != nil {
					return fmt.Errorf("failed to read actor ID: %w", err)
				}
				hctx.ActorID = append(hctx.ActorID, uint64(actorID))
			}
			if tag == destActorFlagsTag || tag == destFlagsTag {
				var extra InvokeReqExtra // do not reuse because client could possibly save slice/pointer
				if hctx.Request, err = extra.Read(hctx.Request); err != nil {
					return fmt.Errorf("failed to read request extra: %w", err)
				}
				hctx.RequestExtra = append(hctx.RequestExtra, extra)
			}
		}

		hctx.reqType = tag
		hctx.respPacketType = packetTypeRPCReqResult
		if len(hctx.RequestExtra) > 0 {
			hctx.noResult = hctx.RequestExtra[0].IsSetNoResult()
			hctx.requestExtraFieldsmask = hctx.RequestExtra[0].flags
		}
		return s.callHandler(ctx, hctx)
	default:
		return fmt.Errorf("unexpected packet type 0x%x", hctx.reqHeader.tip)
	}
}

func (hctx *HandlerContext) prepareResponse(err error) {
	if err == nil {
		if len(hctx.Response) == 0 {
			// Handler should return ErrNoHandler if it does not know how to return response
			hctx.serverConn.server.Logf("rpc: handler returned empty response with no error query #%v to 0x%x", hctx.queryID, hctx.reqType)
		}
		return
	}
	respErr := Error{}
	switch {
	case err == ErrNoHandler: // this case is only to include reqType into description
		respErr.Code = tlErrorNoHandler
		respErr.Description = fmt.Sprintf("RPC handler for #%08x not found", hctx.reqType)
	case errors.As(err, &respErr):
		// OK, forward the error as-is
	case errors.Is(err, context.DeadlineExceeded):
		respErr.Code = tlErrorTimeout
		respErr.Description = fmt.Sprintf("%s (request timeout was %v)", err.Error(), hctx.timeout())
	default:
		respErr.Code = tlErrorUnknown
		respErr.Description = err.Error()
	}

	if hctx.noResult {
		hctx.serverConn.server.Logf("rpc: failed to handle no_result query #%v to 0x%x: %s", hctx.queryID, hctx.reqType, respErr.Error())
		return
	}

	resp := hctx.Response[:0]
	resp = basictl.NatWrite(resp, reqResultErrorTag) // vkext compatibility hack instead of
	resp = basictl.LongWrite(resp, hctx.queryID)     // packetTypeRPCReqError in packet header
	resp = basictl.IntWrite(resp, respErr.Code)
	hctx.Response = basictl.StringWriteTruncated(resp, respErr.Description)
}

func (s *Server) callHandler(ctx context.Context, hctx *HandlerContext) (err error) {
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, tracebackBufSize)
			buf = buf[:runtime.Stack(buf, false)]
			s.Logf("rpc: panic serving %v: %v\n%s", hctx.remoteAddr.String(), r, buf)
			err = &Error{Code: tlErrorInternal, Description: fmt.Sprintf("rpc: HandlerFunc panic: %v serving %v", r, hctx.remoteAddr.String())}
		}
	}()

	switch hctx.reqType {
	case enginePIDTag:
		return s.handleEnginePID(hctx)
	case engineStatTag:
		return s.handleEngineStat(hctx)
	case engineVersionTag:
		return s.handleEngineVersion(hctx)
	case engineSetVerbosityTag:
		return s.handleEngineSetVerbosity(hctx)
	case goPProfTag:
		return s.handleGoPProf(hctx)
	default:
		deadline := hctx.deadline()
		if !deadline.IsZero() {
			dt := time.Since(deadline)
			if dt >= 0 {
				return Error{
					Code:        tlErrorTimeout,
					Description: fmt.Sprintf("RPC query timeout (%v after deadline)", dt),
				}
			}

			if !s.DisableContextTimeout {
				var cancel context.CancelFunc
				ctx, cancel = context.WithDeadline(ctx, deadline)
				defer cancel()
			}
		}

		if s.Handler == nil {
			return ErrNoHandler
		}

		return s.Handler(ctx, hctx)
	}
}

func (s *Server) trackWorker(w *worker) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return false
	}
	s.workers = append(s.workers, w)

	return true
}

func (s *Server) trackListener(ln *net.Listener, add bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if add {
		if s.closed {
			return false
		}
		if s.listeners == nil {
			s.listeners = map[*net.Listener]struct{}{}
		}
		s.listeners[ln] = struct{}{}
	} else {
		delete(s.listeners, ln)
	}

	return true
}

func (s *Server) trackConn(sc *serverConn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return false
	}
	if s.conns == nil {
		s.conns = map[*serverConn]struct{}{}
	}
	s.conns[sc] = struct{}{}

	return true
}

func (s *Server) dropConn(sc *serverConn) {
	s.mu.Lock()
	delete(s.conns, sc)
	s.mu.Unlock()

	_ = sc.Close()
}

type worker struct {
	s  *Server
	ch chan *HandlerContext
}

func (w *worker) run(wg *sync.WaitGroup) {
	defer wg.Done()
	for hctx := range w.ch {
		w.s.handle(hctx)
		w.s.releaseWorker(w)
	}
}

// not using separate "close" channel in worker.run (signaling exit by closing shared worker.ch) allows avoiding costly select
func (w *worker) tryPushWork(hctx *HandlerContext) {
	defer func() {
		_ = recover() // ignore writes to closed channel
	}()

	w.ch <- hctx
}

type sWriteQ struct {
	mu     sync.Mutex
	cond   sync.Cond
	closed bool
	q      []*HandlerContext
}

func newSWriteQ(n int) *sWriteQ {
	wq := &sWriteQ{
		q: make([]*HandlerContext, 0, n),
	}
	wq.cond.L = &wq.mu
	return wq
}

func (wq *sWriteQ) close() {
	wq.mu.Lock()
	defer wq.mu.Unlock()

	if !wq.closed {
		wq.closed = true
		for _, hctx := range wq.q {
			hctx.releaseResponse()
		}
		wq.cond.Broadcast()
	}
}

func (wq *sWriteQ) push(hctx *HandlerContext) {
	wq.mu.Lock()
	if wq.closed {
		hctx.releaseResponse()
	} else {
		wq.q = append(wq.q, hctx)
	}
	wq.mu.Unlock() // unlock without defer to try to reduce lock contention

	wq.cond.Signal()
}

func (wq *sWriteQ) acquire(q []*HandlerContext) []*HandlerContext {
	wq.mu.Lock()
	defer wq.mu.Unlock()

	for !wq.closed && len(wq.q) == 0 {
		wq.cond.Wait()
	}

	if wq.closed {
		return nil
	}

	q = append(q, wq.q...)
	wq.q = wq.q[:0]

	return q
}

func (wq *sWriteQ) empty() bool {
	wq.mu.Lock()
	defer wq.mu.Unlock()

	return len(wq.q) == 0
}

func commonConnCloseError(err error) bool {
	s := err.Error()
	return strings.HasSuffix(s, "EOF") ||
		strings.HasSuffix(s, "broken pipe") ||
		strings.HasSuffix(s, "reset by peer") ||
		strings.HasSuffix(s, "use of closed network connection")
}

func controlSetTCPReuseAddr(_ /*network*/ string, _ /*address*/ string, c syscall.RawConn) error {
	var opErr error
	err := c.Control(func(fd uintptr) {
		// this is a no-op for Unix sockets
		opErr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEADDR, 1)
	})
	if err != nil {
		return err
	}
	return opErr
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func IsHijackedResponse(err error) bool {
	return err == errHijackResponse
}
