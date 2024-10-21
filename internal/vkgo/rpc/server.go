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
	"io"
	"log"
	"net"
	"os"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"go.uber.org/atomic"

	"golang.org/x/sys/unix"

	"github.com/vkcom/statshouse/internal/vkgo/basictl"
	"github.com/vkcom/statshouse/internal/vkgo/rpc/internal/gen/constants"
	"github.com/vkcom/statshouse/internal/vkgo/rpc/internal/gen/tl"
	"github.com/vkcom/statshouse/internal/vkgo/semaphore"
	"github.com/vkcom/statshouse/internal/vkgo/srvfunc"
)

// TODO: explain safety rules: no ctx/buffer use outside of the handler
// TODO: experiment with github.com/mailru/easygo/netpoll or github.com/xtaci/gaio

// Server properties
// Done
// * Fast (1+ million RPS single core without workers for small requests)
// * Limited Requests/Responses Memory
// * Limited Workers
// * Limited TCP connections
// * Limited Timeouts for all operations
// * FastHandler called directly before sending to worker
// * client FIN - all requests are processed and sent before connection close
// * graceful shutdown - all
// * rpcCancelReq - call HijackContextClosed
// * close connection - call HijackContextClosed

const (
	DefaultMaxWorkers             = 1024
	DefaultMaxConns               = 131072 // note, this number of connections will require 10+ GB of memory
	DefaultMaxInflightPackets     = 256
	DefaultRequestMemoryLimit     = 256 * 1024 * 1024
	DefaultResponseMemoryLimit    = 2048*1024*1024 - 1
	DefaultServerConnReadBufSize  = maxGoAllocSizeClass
	DefaultServerConnWriteBufSize = maxGoAllocSizeClass
	DefaultServerRequestBufSize   = 4096 // we expect shorter requests on average
	DefaultServerResponseBufSize  = maxGoAllocSizeClass
	DefaultResponseMemEstimate    = 1024 * 1024 // we likely over-account unknown response length before the handler has finished

	minAcceptDelay  = 5 * time.Millisecond
	maxAcceptDelay  = 1 * time.Second
	rpsCalcInterval = 5 * time.Second
	rareLogInterval = 1 * time.Second

	maxGoAllocSizeClass = 32768 // from here: https://go.dev/src/runtime/mksizeclasses.go
	tracebackBufSize    = 65536
)

var (
	errServerClosed   = errors.New("rpc: server closed")
	ErrNoHandler      = &Error{Code: TlErrorNoHandler, Description: "rpc: no handler"} // Never wrap this error
	errHijackResponse = errors.New("rpc: user of server is now responsible for sending the response")
	ErrCancelHijack   = &Error{Code: TlErrorTimeout, Description: "rpc: longpoll cancelled"} // passed to response hook. Decided to add separate code later.

	statCPUInfo = srvfunc.MakeCPUInfo() // TODO - remove global
)

type (
	HandlerFunc          func(ctx context.Context, hctx *HandlerContext) error
	StatsHandlerFunc     func(map[string]string)
	VerbosityHandlerFunc func(int) error
	LoggerFunc           func(format string, args ...any)
	ErrHandlerFunc       func(err error)
	RequestHookFunc      func(hctx *HandlerContext)
	ResponseHookFunc     func(hctx *HandlerContext, err error)
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

func Listen(network, address string, disableTCPReuseAddr bool) (net.Listener, error) {
	if network != "tcp4" && network != "tcp6" && network != "tcp" && network != "unix" {
		return nil, fmt.Errorf("unsupported network type %q", network)
	}

	var lc net.ListenConfig
	if !disableTCPReuseAddr {
		lc.Control = controlSetTCPReuseAddr
	}

	return lc.Listen(context.Background(), network, address)
}

func NewServer(options ...ServerOptionsFunc) *Server {
	opts := ServerOptions{
		Logf:                   log.Printf,
		MaxConns:               DefaultMaxConns,
		MaxWorkers:             DefaultMaxWorkers,
		MaxInflightPackets:     DefaultMaxInflightPackets,
		RequestMemoryLimit:     DefaultRequestMemoryLimit,
		ResponseMemoryLimit:    DefaultResponseMemoryLimit,
		ConnReadBufSize:        DefaultServerConnReadBufSize,
		ConnWriteBufSize:       DefaultServerConnWriteBufSize,
		RequestBufSize:         DefaultServerRequestBufSize,
		ResponseBufSize:        DefaultServerResponseBufSize,
		ResponseMemEstimate:    DefaultResponseMemEstimate,
		DefaultResponseTimeout: 0,
		ResponseTimeoutAdjust:  0,
		StatsHandler:           func(m map[string]string) {},
		Handler:                func(ctx context.Context, hctx *HandlerContext) error { return ErrNoHandler },
		RecoverPanics:          true,
	}
	for _, option := range options {
		option(&opts)
	}

	host, _ := os.Hostname()

	s := &Server{
		serverStatus: serverStatusInitial,
		opts:         opts,
		conns:        map[*serverConn]struct{}{},
		reqMemSem:    semaphore.NewWeighted(int64(opts.RequestMemoryLimit)),
		respMemSem:   semaphore.NewWeighted(int64(opts.ResponseMemoryLimit)),
		connSem:      semaphore.NewWeighted(int64(opts.MaxConns)),
		startTime:    uniqueStartTime(),
		statHostname: host,
	}

	s.workerPool = workerPoolNew(s.opts.MaxWorkers, func() {
		if s.opts.MaxWorkers == DefaultMaxWorkers { // print only if user did not change default value
			s.rareLog(&s.lastWorkerWaitLog, "rpc: waiting to acquire worker; consider increasing Server.MaxWorkers")
		}
	})

	s.closeCtx, s.cancelCloseCtx = context.WithCancel(context.Background())

	s.workersGroup.Add(1)
	go s.rpsCalcLoop(&s.workersGroup)

	return s
}

const (
	serverStatusInitial  = 0 // ordering is important, we use >/< comparisons with statues
	serverStatusStarted  = 1 // after first call to Serve.
	serverStatusShutdown = 2 // after first call to Shutdown(). New Serve() calls will quit immediately. Main context is cancelled, Responses are still being sent.
	serverStatusStopped  = 3 // after first call to Wait. Everything is torn down, except if some handlers did not return
)

type Server struct {
	// Place atomics first to ensure proper alignment, see https://pkg.go.dev/sync/atomic#pkg-note-BUG
	statConnectionsTotal   atomic.Int64
	statConnectionsCurrent atomic.Int64
	statRequestsTotal      atomic.Int64
	statRequestsCurrent    atomic.Int64
	statRPS                atomic.Int64
	statHostname           string
	statLongPollsWaiting   atomic.Int64

	opts ServerOptions

	mu           sync.Mutex
	serverStatus int
	listeners    []net.Listener // will close after
	conns        map[*serverConn]struct{}

	workerPool   *workerPool
	workersGroup sync.WaitGroup

	closeCtx       context.Context
	cancelCloseCtx context.CancelFunc
	connSem        *semaphore.Weighted
	reqMemSem      *semaphore.Weighted
	respMemSem     *semaphore.Weighted
	reqBufPool     sync.Pool
	respBufPool    sync.Pool

	startTime uint32

	rareLogMu            sync.Mutex
	lastReqMemWaitLog    time.Time
	lastRespMemWaitLog   time.Time
	lastHctxWaitLog      time.Time
	lastWorkerWaitLog    time.Time
	lastHijackWarningLog time.Time
	lastPacketTypeLog    time.Time
	lastReadErrorLog     time.Time
	lastPushToClosedLog  time.Time
	lastOtherLog         time.Time // TODO - may be split this into different error classes

	tracingMu  sync.Mutex
	tracingLog []string
}

func (s *Server) addTrace(str string) {
	if !debugTrace {
		return
	}
	s.tracingMu.Lock()
	defer s.tracingMu.Unlock()
	s.tracingLog = append(s.tracingLog, str)
}

// some users want to delay registering handler after server is created
func (s *Server) RegisterHandlerFunc(h HandlerFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.serverStatus >= serverStatusStarted {
		panic("cannot register handler after first Serve() or Shutdown() call")
	}
	s.opts.Handler = h
}

// some users want to delay registering handler after server is created
func (s *Server) RegisterStatsHandlerFunc(h StatsHandlerFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.serverStatus >= serverStatusStarted {
		panic("cannot register stats handler after first Serve() or Shutdown() call")
	}
	s.opts.StatsHandler = h
}

// some users want to delay registering handler after server is created
func (s *Server) RegisterVerbosityHandlerFunc(h VerbosityHandlerFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.serverStatus >= serverStatusStarted {
		panic("cannot register verbosity handler after first Serve() or Shutdown() call")
	}
	s.opts.VerbosityHandler = h
}

// Server stops accepting new clients, sends USER LEVEL FINs, continues to respond to pending requests
// Can be called as many times as needed. Does not wait.
func (s *Server) Shutdown() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.serverStatus >= serverStatusShutdown {
		return
	}
	s.serverStatus = serverStatusShutdown

	for _, ln := range s.listeners {
		_ = ln.Close() // We do not care
	}
	s.listeners = s.listeners[:0]

	for sc := range s.conns {
		sc.sendLetsFin()
	}
}

// Waits for all requests to be handled and responses sent
// After this function Close() must return immediately, but we are still afraid to call it here as it wait for more objects
func (s *Server) CloseWait(ctx context.Context) error {
	s.Shutdown()
	// After shutdown, if clients follow protocol, all connections will soon be closed
	// we can acquire whole semaphore only if all connections finished
	err := s.connSem.Acquire(ctx, int64(s.opts.MaxConns))
	if err == nil {
		s.connSem.Release(int64(s.opts.MaxConns)) // we support multiple calls to Close/CloseWait
	}
	return err
}

func (s *Server) Close() error {
	s.Shutdown()
	s.mu.Lock()
	if s.serverStatus >= serverStatusStopped {
		s.mu.Unlock()
		return nil
	}
	s.serverStatus = serverStatusStopped

	cause := fmt.Errorf("server Close called")

	for sc := range s.conns {
		sc.close(cause)
	}

	s.mu.Unlock()

	s.cancelCloseCtx()

	// any worker got before Close will have channel not closed, so work will be sent there and executed
	// after that Put will return false and worker will quit
	s.workerPool.Close()

	// we can acquire whole semaphore only if all connections finished
	_ = s.connSem.Acquire(context.Background(), int64(s.opts.MaxConns))
	s.connSem.Release(int64(s.opts.MaxConns)) // we support multiple calls to Close/CloseWait

	s.workersGroup.Wait()

	if len(s.conns) != 0 {
		s.opts.Logf("rpc: tracking of connection invariant violated after close wait - %d connections", len(s.conns))
	}
	if cur, _ := s.reqMemSem.Observe(); cur != 0 {
		s.opts.Logf("rpc: tracking of request memory invariant violated after close wait - %d bytes", cur)
	}
	if cur, _ := s.respMemSem.Observe(); cur != 0 {
		s.opts.Logf("rpc: tracking of request memory invariant violated after close wait - %d bytes", cur)
	}
	if cur := s.statRequestsCurrent.Load(); cur != 0 {
		s.opts.Logf("rpc: tracking of current requests invariant violated after close wait - %d requests", cur)
	}
	return nil
}

func (s *Server) requestBufTake(reqBodySize int) int {
	return max(reqBodySize, s.opts.RequestBufSize) // we consider that most buffers in the pool will be stretched up to max size
}

func (s *Server) responseBufTake(respBodySizeEstimate int) int {
	return max(respBodySizeEstimate, s.opts.ResponseBufSize) // we consider that most buffers in the pool will be stretched up to max size
}

func (s *Server) acquireRequestBuf(ctx context.Context, reqBodySize int) (*[]byte, int, error) {
	take := s.requestBufTake(reqBodySize)
	ok := s.reqMemSem.TryAcquire(int64(take))
	if !ok {
		cur, size := s.reqMemSem.Observe()
		s.rareLog(&s.lastReqMemWaitLog,
			"rpc: waiting to acquire request memory (want %s, mem %s, limit %s, %d conns, %d reqs); consider increasing Server.RequestMemoryLimit",
			humanByteCountIEC(int64(take)),
			humanByteCountIEC(cur),
			humanByteCountIEC(size),
			s.statConnectionsCurrent.Load(),
			s.statRequestsCurrent.Load(),
		)
		err := s.reqMemSem.Acquire(ctx, int64(take))
		if err != nil {
			return nil, 0, err
		}
	}

	if take > s.opts.RequestBufSize {
		return nil, take, nil // large requests will not go back to pool, so fall back to GC
	}

	v := s.reqBufPool.Get()
	if v != nil {
		return v.(*[]byte), take, nil
	}
	var b []byte // allocate heap slice, which will be put into pool in releaseRequest
	return &b, take, nil
}

func (s *Server) releaseRequestBuf(taken int, buf *[]byte) {
	if taken != 0 {
		s.reqMemSem.Release(int64(taken))
	}

	if buf != nil {
		s.reqBufPool.Put(buf)
	}
}

func (s *Server) acquireResponseBuf(ctx context.Context) (*[]byte, int, error) {
	take := s.responseBufTake(s.opts.ResponseMemEstimate)
	ok := s.respMemSem.TryAcquire(int64(take))
	if !ok {
		cur, size := s.respMemSem.Observe()
		s.rareLog(&s.lastRespMemWaitLog,
			"rpc: waiting to acquire response memory (want %s, mem %s, limit %s, %d conns, %d reqs); consider increasing Server.ResponseMemoryLimit or lowering Server.ResponseMemEstimate",
			humanByteCountIEC(int64(take)),
			humanByteCountIEC(cur),
			humanByteCountIEC(size),
			s.statConnectionsCurrent.Load(),
			s.statRequestsCurrent.Load(),
		)
		err := s.respMemSem.Acquire(ctx, int64(take))
		if err != nil {
			return nil, 0, err
		}
	}

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
	if need == taken {
		return need, nil
	}
	if need < taken {
		dontNeed := int64(taken - need)
		s.respMemSem.Release(dontNeed)
		return need, nil
	}
	want := int64(need - taken)
	if force {
		s.respMemSem.ForceAcquire(want)
		return need, nil
	}
	if !s.respMemSem.TryAcquire(want) {
		cur, size := s.respMemSem.Observe()
		s.rareLog(&s.lastRespMemWaitLog,
			"rpc: waiting to acquire response memory (want %s, mem %s, limit %s, %d conns, %d reqs); consider increasing Server.ResponseMemoryLimit",
			humanByteCountIEC(want),
			humanByteCountIEC(cur),
			humanByteCountIEC(size),
			s.statConnectionsCurrent.Load(),
			s.statRequestsCurrent.Load(),
		)
		err := s.respMemSem.Acquire(ctx, want)
		if err != nil {
			return taken, err
		}
	}
	return need, nil
}

func (s *Server) releaseResponseBuf(taken int, buf *[]byte) { // buf is never nil
	if taken != 0 {
		s.respMemSem.Release(int64(taken))
	}

	if buf != nil {
		s.respBufPool.Put(buf) // we always reuse heap-allocated slice
	}
}

// ListenAndServe supports only "tcp4" and "unix" networks
func (s *Server) ListenAndServe(network string, address string) error {
	ln, err := Listen(network, address, s.opts.DisableTCPReuseAddr)
	if err != nil {
		return err
	}
	return s.Serve(ln)
}

func (s *Server) Serve(ln net.Listener) error {
	_, _ = statCPUInfo.GetSelfCpuUsage()
	s.mu.Lock()
	if s.serverStatus > serverStatusStarted {
		s.mu.Unlock()
		// probably some goroutine was delayed to call Serve, and user already called Shutdown(). No problem, simply quit
		_ = ln.Close() // usually server closes all listeners passed to Serve(), let's make no exceptions
		return nil
	}
	s.serverStatus = serverStatusStarted
	s.listeners = append(s.listeners, ln)
	s.mu.Unlock()

	if err := s.connSem.Acquire(s.closeCtx, 1); err != nil {
		return nil
	}
	var acceptDelay time.Duration
	for {
		// at this point we own connSem once
		nc, err := ln.Accept()
		s.mu.Lock()
		if s.serverStatus >= serverStatusShutdown {
			s.mu.Unlock()
			if err == nil { // We could be waiting on Lock above with accepted connection when server was Shutdown()
				_ = nc.Close()
			}
			s.connSem.Release(1)
			return nil
		}
		s.mu.Unlock()
		if err != nil {
			if s.opts.AcceptErrHandler != nil {
				s.opts.AcceptErrHandler(err)
			}
			//lint:ignore SA1019 "FIXME: to ne.Timeout()"
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				// In practice error can happen when system is out of descriptors.
				// Closing some connections allows to accept again.
				// TODO - check if golang considers such error Temporary
				if acceptDelay == 0 {
					acceptDelay = minAcceptDelay
				} else {
					acceptDelay *= 2
				}
				if acceptDelay > maxAcceptDelay {
					acceptDelay = maxAcceptDelay
				}
				s.rareLog(&s.lastOtherLog, "rpc: Accept error: %v; retrying in %v", err, acceptDelay)
				time.Sleep(acceptDelay)
				continue
			}
			s.connSem.Release(1)
			return err
		}
		conn := NewPacketConn(nc, s.opts.ConnReadBufSize, s.opts.ConnWriteBufSize)

		s.statConnectionsTotal.Inc()
		s.statConnectionsCurrent.Inc()

		go s.goHandshake(conn, ln.Addr())                        // will release connSem
		if err := s.connSem.Acquire(s.closeCtx, 1); err != nil { // we need to acquire again to Accept()
			return nil
		}
	}
}

func (s *Server) goHandshake(conn *PacketConn, lnAddr net.Addr) {
	defer s.connSem.Release(1)
	defer s.statConnectionsCurrent.Dec() // we have the same value in connSema, but reading from here is faster

	magicHead, flags, err := conn.HandshakeServer(s.opts.cryptoKeys, s.opts.TrustedSubnetGroups, s.opts.ForceEncryption, s.startTime, DefaultPacketTimeout)
	if err != nil {
		switch string(magicHead) {
		case memcachedStatsReqRN, memcachedStatsReqN, memcachedGetStatsReq:
			s.respondWithMemcachedStats(conn)
		case memcachedVersionReq:
			s.respondWithMemcachedVersion(conn)
		default:
			if len(magicHead) != 0 && s.opts.SocketHijackHandler != nil {
				_ = conn.setReadTimeoutUnlocked(0)
				_ = conn.setWriteTimeoutUnlocked(0)
				// We do not close connection, ownership is moved to SocketHijackHandler
				s.opts.SocketHijackHandler(&HijackConnection{Magic: append(magicHead, conn.r.buf[conn.r.begin:conn.r.end]...), Conn: conn.conn})
				return
			}
			// We have some admin scripts which test port by connecting and then disconnecting, looks like port probing, but OK
			// If you want to make logging unconditional, please ask Petr Mikushin @petr8822 first.
			if len(magicHead) != 0 {
				s.rareLog(&s.lastOtherLog, "rpc: failed to handshake with %v, peer sent(hex) %x, disconnecting: %v", conn.remoteAddr, magicHead, err)
			}
			// } else { s.rareLog(&s.lastOtherLog, "rpc: failed to handshake with %v, disconnecting: %v", conn.remoteAddr, err) }
		}
		if s.opts.ConnErrHandler != nil {
			s.opts.ConnErrHandler(err)
		}
		_ = conn.Close()
		return
	}
	// if flags match, but no TransportHijackHandler, we presume this is valid combination and proceed as usual
	// if we are wrong, client gets normal error response from rpc Server
	if flags&FlagP2PHijack != 0 && s.opts.TransportHijackHandler != nil {
		// at this point goroutine holds no resources, handler is free to use this goroutine for as long as it wishes
		// server can be in shutdown state, we have no ordering between TransportHijackHandler and server status yet
		s.opts.TransportHijackHandler(conn)
		return
	}

	closeCtx, cancelCloseCtx := context.WithCancelCause(s.closeCtx)

	sc := &serverConn{
		closeCtx:          closeCtx,
		cancelCloseCtx:    cancelCloseCtx,
		server:            s,
		listenAddr:        lnAddr,
		maxInflight:       s.opts.MaxInflightPackets,
		conn:              conn,
		writeQ:            make([]*HandlerContext, 0, s.opts.maxInflightPacketsPreAlloc()),
		longpollResponses: map[int64]hijackedResponse{},
		errHandler:        s.opts.ConnErrHandler,
	}
	sc.cond.L = &sc.mu
	sc.writeQCond.L = &sc.mu
	sc.closeWaitCond.L = &sc.mu

	if !s.trackConn(sc) {
		// server is shutting down
		_ = conn.Close()
		return
	}
	if s.opts.DebugRPC {
		s.opts.Logf("rpc: %s->%s Handshake ", sc.conn.remoteAddr, sc.conn.localAddr)
	}

	readErrCC := make(chan error, 1)
	go s.receiveLoop(sc, readErrCC)
	writeErr := s.sendLoop(sc)
	if debugPrint {
		fmt.Printf("%v server %p conn %p sendLoop quit\n", time.Now(), s, sc)
	}
	sc.close(writeErr)     // after writer quit, there is no point to continue connection operation
	readErr := <-readErrCC // wait for reader

	if s.opts.DebugRPC {
		s.opts.Logf("rpc: %s->%s Disconnect with readErr=%v writeErr=%v", sc.conn.remoteAddr, sc.conn.localAddr, readErr, writeErr)
	}

	_ = sc.WaitClosed()

	s.dropConn(sc)
}

func (s *Server) rareLog(last *time.Time, format string, args ...any) {
	now := time.Now()

	s.rareLogMu.Lock()
	defer s.rareLogMu.Unlock()

	if now.Sub(*last) > rareLogInterval {
		*last = now
		s.opts.Logf(format, args...)
	}
}

func (s *Server) acquireWorker() *worker {
	v, ok := s.workerPool.Get(&s.workersGroup)
	if !ok { // closed
		return nil
	}
	if v != nil {
		return v
	}

	w := &worker{
		workerPool: s.workerPool,
		ch:         make(chan workerWork, 1),
	}

	go w.run(&s.workersGroup)

	return w
}

func (s *Server) receiveLoop(sc *serverConn, readErrCC chan<- error) {
	hctxToRelease, err := s.receiveLoopImpl(sc)
	if hctxToRelease != nil {
		sc.releaseHandlerCtx(hctxToRelease)
	}
	if err != nil {
		sc.close(err)
	}
	sc.cancelAllLongpollResponses() // we always cancel from receiving goroutine.
	readErrCC <- err
}

func (s *Server) receiveLoopImpl(sc *serverConn) (*HandlerContext, error) {
	readFIN := false
	for {
		// read header first, before acquiring handler context,
		// to be able to disconnect event when all handler contexts are taken
		var header packetHeader
		head, isBuiltin, _, err := sc.conn.readPacketHeaderUnlocked(&header, DefaultPacketTimeout*11/10)
		// motivation for slightly increasing timeout is so that client and server will not send pings to each other, client will do it first
		if err != nil {
			if len(head) == 0 && (err == io.EOF || err == io.ErrUnexpectedEOF) { // legacy client behavior sending FIN, TODO - remove in January 2025
				if debugPrint {
					fmt.Printf("%v server %p conn %p reader received FIN\n", time.Now(), s, sc)
				}
				sc.SetReadFIN()
				return nil, nil // clean shutdown, finish writing then close
			}
			if len(head) != 0 {
				// We complain only if partially read header.
				s.rareLog(&s.lastReadErrorLog, "rpc: error reading packet header from %v, disconnecting: %v", sc.conn.remoteAddr, err)
			}
			// Also, returning error closes send loop, which will complain if there are responses not sent
			return nil, err
		}
		if isBuiltin {
			sc.SetWriteBuiltin()
			continue
		}
		if readFIN { // only built-in (ping-pongs) after user space FIN
			return nil, fmt.Errorf("rpc: client %v sent request length %d type 0x%x after ClientWantsFIN, shutdown protocol invariant violated", sc.conn.remoteAddr, header.length, header.tip)
		}
		// TODO - read packet body for tl.RpcCancelReq, tl.RpcClientWantsFin without waiting on semaphores
		requestTime := time.Now()

		hctx, ok := sc.acquireHandlerCtx(header.tip, &s.opts)
		if !ok {
			return nil, errServerClosed
		}
		s.statRequestsCurrent.Inc()

		hctx.RequestTime = requestTime

		req, reqTaken, err := s.acquireRequestBuf(sc.closeCtx, int(header.length))
		if err != nil {
			return hctx, err
		}
		hctx.request = req
		hctx.reqTaken = reqTaken
		if hctx.request != nil {
			hctx.Request = *hctx.request
		}
		hctx.Request, err = sc.conn.readPacketBodyUnlocked(&header, hctx.Request)
		if hctx.request != nil {
			*hctx.request = hctx.Request[:0] // prepare for reuse immediately
		}
		if err != nil {
			// failing to fully read packet is always problem we want to report
			s.rareLog(&s.lastReadErrorLog, "rpc: error reading packet body (%d/%d bytes read) from %v, disconnecting: %v", len(hctx.Request), header.length, sc.conn.remoteAddr, err)
			return hctx, err
		}

		resp, respTaken, err := s.acquireResponseBuf(sc.closeCtx)
		if err != nil {
			return hctx, err
		}
		hctx.response = resp
		hctx.Response = *hctx.response
		hctx.respTaken = respTaken

		s.statRequestsTotal.Inc()

		if !s.syncHandler(header.tip, sc, hctx, &readFIN) {
			if s.opts.MaxWorkers <= 0 {
				// We keep this code, because in many projects maxWorkers are cmd argument,
				// and they want to disable worker pool sometimes with this argument
				sc.handle(hctx)
			} else {
				w := s.acquireWorker()
				if w == nil {
					return hctx, errServerClosed
				}
				w.ch <- workerWork{sc: sc, hctx: hctx}
			}
		}
	}
}

func (s *Server) sendLoop(sc *serverConn) error {
	toRelease, err := s.sendLoopImpl(sc) // err is logged inside, if needed
	if len(toRelease) != 0 {
		s.rareLog(&s.lastPushToClosedLog, "failed to push %d responses because connection was closed to %v", len(toRelease), sc.conn.remoteAddr)
	}
	for _, hctx := range toRelease {
		sc.releaseHandlerCtx(hctx)
	}
	return err
}

func (s *Server) sendLoopImpl(sc *serverConn) ([]*HandlerContext, error) { // returns contexts to release
	writeQ := make([]*HandlerContext, 0, s.opts.maxInflightPacketsPreAlloc())
	sent := false // true if there is data to flush

	sc.mu.Lock()
	for {
		shouldStop := sc.closedFlag || (sc.readFINFlag && sc.canGracefullyShutdown())
		if !(sent || sc.writeLetsFin || sc.writeBuiltin || len(sc.writeQ) != 0 || shouldStop) {
			sc.writeQCond.Wait()
			continue
		}

		writeLetsFin := sc.writeLetsFin
		sc.writeLetsFin = false
		writeBuiltin := sc.writeBuiltin
		sc.writeBuiltin = false
		writeQ, sc.writeQ = sc.writeQ, writeQ[:0]
		sc.mu.Unlock()
		sentNow := false
		if writeBuiltin {
			sentNow = true
			if err := sc.conn.WritePacketBuiltinNoFlushUnlocked(DefaultPacketTimeout); err != nil {
				// No log here, presumably failing to send ping/pong to closed connection is not a problem
				return writeQ, err // release remaining contexts
			}
		}
		if writeLetsFin {
			if debugPrint {
				fmt.Printf("%v server %p conn %p writes Let's FIN\n", time.Now(), s, sc)
			}
			sentNow = true
			if s.opts.DebugRPC {
				s.opts.Logf("rpc: %s->%s Write Let's FIN packet\n", sc.conn.remoteAddr, sc.conn.localAddr)
			}
			if err := sc.conn.writePacketHeaderUnlocked(tl.RpcServerWantsFin{}.TLTag(), 0, DefaultPacketTimeout); err != nil {
				// No log here, presumably failing to send letsFIN to closed connection is not a problem
				return writeQ, err // release remaining contexts
			}
			sc.conn.writePacketTrailerUnlocked()
		}
		for i, hctx := range writeQ {
			sentNow = true
			if s.opts.DebugRPC {
				s.opts.Logf("rpc: %s->%s Response packet queryID=%d extra=%s body=%x\n", sc.conn.remoteAddr, sc.conn.localAddr, hctx.queryID, hctx.ResponseExtra.String(), hctx.Response[:hctx.extraStart])
			}
			err := writeResponseUnlocked(sc.conn, hctx)
			if err != nil {
				s.rareLog(&s.lastOtherLog, "rpc: error writing packet reqTag #%08x to %v, disconnecting: %v", hctx.reqTag, sc.conn.remoteAddr, err)
				return writeQ[i:], err // release remaining contexts
			}
			sc.releaseHandlerCtx(hctx)
		}
		if (sent && !sentNow) || shouldStop {
			if s.opts.DebugRPC && !shouldStop { // this log during disconnect can be confusing, so last condition
				s.opts.Logf("rpc: %s->%s Flush\n", sc.conn.remoteAddr, sc.conn.localAddr)
			}
			if err := sc.conn.FlushUnlocked(); err != nil {
				sc.server.rareLog(&sc.server.lastOtherLog, "rpc: error flushing packet to %v, disconnecting: %v", sc.conn.remoteAddr, err)
				return nil, err
			}
			if shouldStop {
				if debugPrint {
					fmt.Printf("%v server %p conn %p writes FIN + sendLoop stop\n", time.Now(), s, sc)
				}
				if err := sc.conn.ShutdownWrite(); err != nil { // make sure client receives all responses
					if !commonConnCloseError(err) {
						sc.server.rareLog(&sc.server.lastOtherLog, "rpc: error writing FIN packet to %v, disconnecting: %v", sc.conn.remoteAddr, err)
					}
					return nil, err
				}
				return nil, nil
			}
		}
		sent = sentNow
		sc.mu.Lock()
	}
}

func (s *Server) syncHandler(reqHeaderTip uint32, sc *serverConn, hctx *HandlerContext, readFIN *bool) bool {
	err := s.doSyncHandler(reqHeaderTip, sc, hctx, readFIN)
	if err == ErrNoHandler {
		return false
	}
	if err == errHijackResponse {
		// We must not touch hctx.UserData here because it can be released already in SendHijackResponse
		// User is now responsible for calling hctx.SendHijackedResponse
		return true
	}
	sc.pushResponse(hctx, err, false)
	return true
}

func (s *Server) doSyncHandler(reqHeaderTip uint32, sc *serverConn, hctx *HandlerContext, readFIN *bool) error {
	switch reqHeaderTip {
	case tl.RpcCancelReq{}.TLTag():
		cancelReq := tl.RpcCancelReq{}
		if _, err := cancelReq.Read(hctx.Request); err != nil {
			return err
		}
		if s.opts.DebugRPC {
			s.opts.Logf("rpc: %s->%s Cancel queryID=%d\n", sc.conn.remoteAddr, sc.conn.localAddr, cancelReq.QueryId)
		}
		hctx.noResult = true
		sc.cancelLongpollResponse(cancelReq.QueryId)
		return nil
	case tl.RpcClientWantsFin{}.TLTag():
		*readFIN = true
		sc.SetReadFIN()
		hctx.noResult = true
		return nil
	case tl.RpcInvokeReqHeader{}.TLTag():
		err := hctx.ParseInvokeReq(&s.opts)
		if err != nil {
			return err
		}
		if s.opts.DebugRPC {
			s.opts.Logf("rpc: %s->%s SyncHandler packet tag=#%08x queryID=%d extra=%s body=%x\n", sc.conn.remoteAddr, sc.conn.localAddr, reqHeaderTip, hctx.queryID, hctx.RequestExtra.String(), hctx.Request)
		}
		if s.opts.RequestHook != nil {
			s.opts.RequestHook(hctx)
		}
		if s.opts.SyncHandler == nil {
			return ErrNoHandler
		}
		// No deadline on sync handler context, too costly
		return s.opts.SyncHandler(sc.closeCtx, hctx)
	}
	hctx.noResult = true
	s.rareLog(&s.lastPacketTypeLog, "unknown packet type 0x%x", reqHeaderTip)
	return nil
}

func (sc *serverConn) handle(hctx *HandlerContext) {
	err := sc.server.callHandler(sc.closeCtx, hctx)
	sc.pushResponse(hctx, err, false)
}

func (sc *serverConn) pushResponse(hctx *HandlerContext, err error, isLongpoll bool) {
	if !isLongpoll { // already released for longpoll
		sc.releaseRequest(hctx)
	}
	hctx.PrepareResponse(err)
	if !hctx.noResult { // do not spend time for accounting, will release anyway couple lines below
		hctx.respTaken, _ = sc.server.accountResponseMem(sc.closeCtx, hctx.respTaken, cap(hctx.Response), true)
	}
	if sc.server.opts.ResponseHook != nil {
		// we'd like to avoid calling handler for cancelled response,
		// but we do not want to do it under lock in push and cannot do it after lock due to potential race
		sc.server.opts.ResponseHook(hctx, err)
	}
	sc.push(hctx, isLongpoll)
}

// We serialize extra after body into Body, then write into reversed order
// so full response is concatentation of hctx.Reponse[extraStart:], then hctx.Reponse[:extraStart]
func (hctx *HandlerContext) PrepareResponse(err error) (extraStart int) {
	if err = hctx.prepareResponseBody(err); err == nil {
		return hctx.extraStart
	}
	// Too large packet. Very rare.
	hctx.Response = hctx.Response[:0]
	if err = hctx.prepareResponseBody(err); err == nil {
		return hctx.extraStart
	}
	// err we passed above should be small, something is very wrong here
	panic("PrepareResponse with too large error is itself too large")
}

func (hctx *ServerRequest) prepareResponseBody(err error, rareLog func(format string, args ...any)) error {
	resp := hctx.Response
	if err != nil {
		respErr := Error{}
		var respErr2 *Error
		switch {
		case err == ErrNoHandler: // this case is only to include reqTag into description
			respErr.Code = TlErrorNoHandler
			respErr.Description = fmt.Sprintf("RPC handler for #%08x not found", hctx.reqTag)
		case errors.As(err, &respErr2):
			respErr = *respErr2 // OK, forward the error as-is
		case errors.Is(err, context.DeadlineExceeded):
			respErr.Code = TlErrorTimeout
			respErr.Description = fmt.Sprintf("%s (server-adjusted request timeout was %v)", err.Error(), hctx.timeout)
		default:
			respErr.Code = TlErrorUnknown
			respErr.Description = err.Error()
		}

		if hctx.noResult {
			rareLog("rpc: failed to handle no_result query #%v to 0x%x: %s", hctx.QueryID, hctx.reqTag, respErr.Error())
			return nil
		}

		resp = resp[:0]
		// vkext compatibility hack instead of
		// packetTypeRPCReqError in packet header
		ret := tl.RpcReqResultError{
			QueryId:   hctx.QueryID,
			ErrorCode: respErr.Code,
			Error:     respErr.Description,
		}
		resp = ret.WriteBoxed(resp)
	}
	if hctx.noResult { //We do not care what is in Response, might be any trash
		return nil
	}
	if len(resp) == 0 {
		// Handler should return ErrNoHandler if it does not know how to return response
		rareLog("rpc: handler returned empty response with no error query #%v to 0x%x", hctx.QueryID, hctx.reqTag)
	}
	hctx.extraStart = len(resp)
	rest := tl.RpcReqResultHeader{QueryId: hctx.QueryID}
	resp = rest.Write(resp)
	hctx.ResponseExtra.Flags &= hctx.requestExtraFieldsmask // return only fields they understand
	if hctx.ResponseExtra.Flags != 0 {
		// extra := tl.ReqResultHeader{Extra: hctx.ResponseExtra}
		// resp, _ = extra.WriteBoxed(resp) // should be no errors during writing
		// we optimize copy of large extra here
		resp = basictl.NatWrite(resp, tl.ReqResultHeader{}.TLTag())
		resp = hctx.ResponseExtra.Write(resp) // should be no errors during writing
	}
	hctx.Response = resp
	return validBodyLen(len(resp))
}

func (hctx *HandlerContext) prepareResponseBody(err error) error {
	return hctx.ServerRequest.prepareResponseBody(err, hctx.commonConn.RareLog)
}

func (s *Server) callHandler(ctx context.Context, hctx *HandlerContext) (err error) {
	if !s.opts.RecoverPanics {
		return s.callHandlerNoRecover(ctx, hctx)
	}
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, tracebackBufSize)
			buf = buf[:runtime.Stack(buf, false)]
			s.opts.Logf("rpc: panic serving %v: %v\n%s", hctx.remoteAddr.String(), r, buf)
			err = &Error{Code: TlErrorInternal, Description: fmt.Sprintf("rpc: HandlerFunc panic: %v serving %v", r, hctx.remoteAddr.String())}
		}
	}()
	return s.callHandlerNoRecover(ctx, hctx)
}

func (s *Server) callHandlerNoRecover(ctx context.Context, hctx *HandlerContext) (err error) {
	switch hctx.reqTag {
	case constants.EnginePid:
		return s.handleEnginePID(hctx)
	case constants.EngineStat:
		return s.handleEngineStat(hctx)
	case constants.EngineFilteredStat:
		return s.handleEngineFilteredStat(hctx)
	case constants.EngineVersion:
		return s.handleEngineVersion(hctx)
	case constants.EngineSetVerbosity:
		return s.handleEngineSetVerbosity(hctx)
	case constants.EngineSleep:
		return s.handleEngineSleep(ctx, hctx)
	case constants.EngineAsyncSleep:
		return s.handleEngineAsyncSleep(ctx, hctx)
	case constants.GoPprof:
		return s.handleGoPProf(hctx)
	default:
		if hctx.timeout != 0 {
			deadline := hctx.RequestTime.Add(hctx.timeout)
			dt := time.Since(deadline)
			if dt >= 0 {
				return &Error{
					Code:        TlErrorTimeout,
					Description: fmt.Sprintf("RPC query timeout (%v after deadline)", dt),
				}
			}

			if !s.opts.DisableContextTimeout {
				var cancel context.CancelFunc
				ctx, cancel = context.WithDeadline(ctx, deadline)
				defer cancel()
			}
		}

		err = s.opts.Handler(ctx, hctx)
		if err == errHijackResponse {
			panic("you must hijack responses from SyncHandler, not from normal handler")
		}
		return err
	}
}

func (s *Server) trackConn(sc *serverConn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.serverStatus >= serverStatusShutdown { // no new connections when shutting down
		return false
	}
	s.conns[sc] = struct{}{}
	return true
}

func (s *Server) dropConn(sc *serverConn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.conns[sc]; !ok {
		s.opts.Logf("rpc: connections tracking invariant violated in dropConn")
		return
	}
	delete(s.conns, sc)
}

func commonConnCloseError(err error) bool {
	// TODO - better classification in some future go version
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

func controlSetTCPReuseAddrPort(_ /*network*/ string, _ /*address*/ string, c syscall.RawConn) error {
	var opErr error
	err := c.Control(func(fd uintptr) {
		// this is a no-op for Unix sockets
		opErr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEADDR, 1)
		if opErr == nil {
			opErr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)
		}
	})
	if err != nil {
		return err
	}
	return opErr
}

func IsHijackedResponse(err error) bool {
	return err == errHijackResponse
}

// Also helps garbage collect workers
func (s *Server) rpsCalcLoop(wg *sync.WaitGroup) {
	defer wg.Done()
	tick := time.NewTicker(rpsCalcInterval)
	defer tick.Stop()
	prev := s.statRequestsTotal.Load()
	for {
		select {
		case now := <-tick.C:
			cur := s.statRequestsTotal.Load()
			s.statRPS.Store((cur - prev) / int64(rpsCalcInterval/time.Second))
			prev = cur
			// if rpsCalcInterval is 5, will collect one worker per second
			s.workerPool.GC(now)
			s.workerPool.GC(now)
			s.workerPool.GC(now)
			s.workerPool.GC(now)
			s.workerPool.GC(now)
		case <-s.closeCtx.Done():
			return
		}
	}
}

func (s *Server) ConnectionsTotal() int64 {
	return s.statConnectionsTotal.Load()
}

func (s *Server) ConnectionsCurrent() int64 {
	return s.statConnectionsCurrent.Load()
}

func (s *Server) RequestsTotal() int64 {
	return s.statRequestsTotal.Load()
}

func (s *Server) RequestsCurrent() int64 {
	return s.statRequestsCurrent.Load()
}

func (s *Server) WorkersPoolSize() (current int, total int) {
	return s.workerPool.Created()
}

func (s *Server) LongPollsWaiting() int64 {
	return s.statLongPollsWaiting.Load()
}

func (s *Server) RequestsMemory() (current int64, total int64) {
	return s.reqMemSem.Observe()
}

func (s *Server) ResponsesMemory() (current int64, total int64) {
	return s.respMemSem.Observe()
}

func (s *Server) RPS() int64 {
	return s.statRPS.Load()
}
