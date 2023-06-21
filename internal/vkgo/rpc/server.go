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
	"go.uber.org/multierr"

	"golang.org/x/net/netutil"
	"golang.org/x/sys/unix"

	"github.com/vkcom/statshouse/internal/vkgo/basictl"
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
	ErrNoHandler      = &Error{Code: TlErrorNoHandler, Description: "rpc: no handler"} // Never wrap this error
	errHijackResponse = errors.New("rpc: user of Server is now responsible for sending the response")

	statCPUInfo = srvfunc.MakeCPUInfo() // TODO - remove global
)

type (
	HandlerFunc          func(ctx context.Context, hctx *HandlerContext) error
	StatsHandlerFunc     func(map[string]string)
	VerbosityHandlerFunc func(int) error
	LoggerFunc           func(format string, args ...any)
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
	}
	for _, option := range options {
		option(&opts)
	}

	for _, err := range opts.trustedSubnetGroupsParseErrors {
		opts.Logf("[rpc] failed to parse server trusted subnet %q, ignoring", err)
	}

	// if opts.MaxWorkers <= 0 {
	//	opts.Logf("[rpc] no workers requested, consider setting SyncHandler instead of Handler")
	// }

	host, _ := os.Hostname()

	s := &Server{
		serverStatus: serverStatusInitial,
		opts:         opts,
		conns:        map[*serverConn]struct{}{},
		reqMemSem:    semaphore.NewWeighted(int64(opts.RequestMemoryLimit)),
		respMemSem:   semaphore.NewWeighted(int64(opts.ResponseMemoryLimit)),
		startTime:    uniqueStartTime(),
		statHostname: host,
	}
	s.cond = sync.NewCond(&s.mu)

	s.opts.noopHooks()

	s.workerPool = workerPoolNew(s.opts.MaxWorkers, func() {
		if s.opts.MaxWorkers == DefaultMaxWorkers {
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
	serverStatusShutdown = 2 // after first call to Shutdown. Serve calls will fail. Main context is cancelled, Responses are still being sent.
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

	opts ServerOptions

	mu            sync.Mutex
	cond          *sync.Cond // wakes Accept of socketHijacks
	serverStatus  int
	socketHijacks []net.Conn
	listeners     []net.Listener // will close after
	conns         map[*serverConn]struct{}
	connGroup     WaitGroup

	workerPool   *workerPool
	workersGroup WaitGroup

	closeCtx       context.Context
	cancelCloseCtx context.CancelFunc
	reqMemSem      *semaphore.Weighted
	respMemSem     *semaphore.Weighted
	reqBufPool     sync.Pool
	respBufPool    sync.Pool

	startTime uint32

	rareLogMu             sync.Mutex
	lastReqMemWaitLog     time.Time
	lastRespMemWaitLog    time.Time
	lastHctxWaitLog       time.Time
	lastWorkerWaitLog     time.Time
	lastDuplicateExtraLog time.Time
	lastHijackWarningLog  time.Time

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

func (s *Server) RegisterHandlerFunc(h HandlerFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.serverStatus >= serverStatusStarted {
		panic("cannot register handler after first Serve() or Shutdown() call")
	}
	s.opts.Handler = h
}

func (s *Server) RegisterStatsHandlerFunc(h StatsHandlerFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.serverStatus >= serverStatusStarted {
		panic("cannot register stats handler after first Serve() or Shutdown() call")
	}
	s.opts.StatsHandler = h
}

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
	for sc := range s.conns {
		sc.sendLetsFin()
	}

	for _, ln := range s.listeners {
		_ = ln.Close() // We do not care
	}
	s.listeners = s.listeners[:0]

	for _, c := range s.socketHijacks {
		_ = c.Close()
	}
	s.socketHijacks = s.socketHijacks[:0]
	s.cond.Broadcast() // wake up all goroutines waiting on Accept, so they can return errors
}

// Waits for all requests to be handled and responses sent
func (s *Server) CloseWait(ctx context.Context) error {
	s.Shutdown()
	err := s.connGroup.Wait(ctx) // After shutdown, if clients follow protocol, all connections will soon be closed
	_ = s.Close()
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

	var closeErr error

	for sc := range s.conns {
		err := sc.Close()
		multierr.AppendInto(&closeErr, err)
	}

	s.mu.Unlock()

	s.cancelCloseCtx()

	// any worker got before Close will have channel not closed, so work will be sent there and executed
	// after that Put will return false and worker will quit
	s.workerPool.Close()

	s.connGroup.WaitForever()
	s.workersGroup.WaitForever()

	if len(s.conns) != 0 {
		s.opts.Logf("tracking of connection invariant violated after close wait - %d connections", len(s.conns))
	}
	if cur, _ := s.reqMemSem.Observe(); cur != 0 {
		s.opts.Logf("tracking of request memory invariant violated after close wait - %d bytes", cur)
	}
	if cur, _ := s.respMemSem.Observe(); cur != 0 {
		s.opts.Logf("tracking of request memory invariant violated after close wait - %d bytes", cur)
	}
	if cur := s.statRequestsCurrent.Load(); cur != 0 {
		s.opts.Logf("tracking of current requests invariant violated after close wait - %d requests", cur)
	}
	return closeErr
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
			"rpc: waiting to acquire request memory (want %d, mem %s, limit %s, %d conns, %d reqs); consider increasing Server.RequestMemoryLimit",
			take,
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
			"rpc: waiting to acquire response memory (want %d, mem %s, limit %s, %d conns, %d reqs); consider increasing Server.ResponseMemoryLimit or lowering Server.ResponseMemEstimate",
			take,
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
			"rpc: waiting to acquire response memory (want %d, mem %s, limit %s, %d conns, %d reqs); consider increasing Server.ResponseMemoryLimit",
			want,
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
	if network != "tcp4" && network != "unix" {
		return fmt.Errorf("unsupported network type %q", network)
	}

	var lc net.ListenConfig
	if !s.opts.DisableTCPReuseAddr {
		lc.Control = controlSetTCPReuseAddr
	}

	ln, err := lc.Listen(context.Background(), network, address)
	if err != nil {
		return err
	}

	return s.Serve(ln)
}

func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	if s.serverStatus > serverStatusStarted {
		s.mu.Unlock()
		return ErrServerClosed
	}
	s.serverStatus = serverStatusStarted

	ln = netutil.LimitListener(ln, s.opts.MaxConns)
	s.listeners = append(s.listeners, ln)
	s.mu.Unlock()

	var acceptDelay time.Duration
	for {
		nc, err := ln.Accept()
		if err != nil {
			s.mu.Lock()
			if s.serverStatus >= serverStatusShutdown {
				s.mu.Unlock()
				return ErrServerClosed
			}
			s.mu.Unlock()

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
				s.opts.Logf("rpc: Accept error: %v; retrying in %v", err, acceptDelay)
				time.Sleep(acceptDelay)
				continue
			}
			return err
		}
		conn := NewPacketConn(nc, s.opts.ConnReadBufSize, s.opts.ConnWriteBufSize, DefaultConnTimeoutAccuracy)

		s.statConnectionsTotal.Inc()
		s.statConnectionsCurrent.Inc()

		s.connGroup.Add(1)
		go s.goHandshake(conn, ln.Addr(), &s.connGroup)
	}
}

type wrapConnection struct {
	magic []byte
	net.Conn
}

func (w *wrapConnection) Read(p []byte) (int, error) {
	if len(w.magic) != 0 {
		n := copy(p, w.magic)
		w.magic = w.magic[n:]
		return n, nil
	}
	return w.Conn.Read(p)
}

func (s *Server) Accept() (net.Conn, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for !(s.serverStatus >= serverStatusShutdown || len(s.socketHijacks) != 0) {
		s.cond.Wait()
	}
	if s.serverStatus >= serverStatusShutdown {
		return nil, net.ErrClosed
	}
	c := s.socketHijacks[0]
	s.socketHijacks = s.socketHijacks[1:] // naive queue is ok for us
	return c, nil
}

func (s *Server) Addr() net.Addr {
	return s.opts.SocketHijackAddr
}

func (s *Server) goHandshake(conn *PacketConn, lnAddr net.Addr, wg *WaitGroup) {
	defer s.statConnectionsCurrent.Dec()
	defer wg.Done()

	magicHead, flags, err := conn.HandshakeServer(s.opts.CryptoKeys, s.opts.TrustedSubnetGroups, s.opts.ForceEncryption, s.startTime, DefaultHandshakeStepTimeout)
	if err != nil {
		switch string(magicHead) {
		case memcachedStatsReqRN, memcachedStatsReqN, memcachedGetStatsReq:
			s.respondWithMemcachedStats(conn)
		case memcachedVersionReq:
			s.respondWithMemcachedVersion(conn)
		default:
			if len(magicHead) != 0 && s.opts.SocketHijackAddr != nil {
				_ = conn.setReadTimeoutUnlocked(0)
				_ = conn.setWriteTimeoutUnlocked(0)
				s.mu.Lock()
				if s.serverStatus < serverStatusShutdown {
					s.socketHijacks = append(s.socketHijacks, &wrapConnection{magic: append(magicHead, conn.r.buf[conn.r.begin:conn.r.end]...), Conn: conn.conn})
					// We do not close connection, ownership is moved to listener of Server
					s.mu.Unlock()
					s.cond.Signal()
					return
				}
				s.mu.Unlock()
				_ = conn.Close()
				return
			}
			if !commonConnCloseError(err) || s.opts.LogCommonNetworkErrors {
				s.opts.Logf("rpc: failed to handshake with %v, disconnecting: %v, magic head: %+q", conn.remoteAddr, err, magicHead)
			}
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

	closeCtx, cancelCloseCtx := context.WithCancel(s.closeCtx)

	sc := &serverConn{
		closeCtx:          closeCtx,
		cancelCloseCtx:    cancelCloseCtx,
		server:            s,
		listenAddr:        lnAddr,
		maxInflight:       s.opts.MaxInflightPackets,
		conn:              conn,
		writeQ:            make([]*HandlerContext, 0, s.opts.maxInflightPacketsPreAlloc()),
		longpollResponses: map[int64]hijackedResponse{},
	}
	sc.cond.L = &sc.mu
	sc.writeQCond.L = &sc.mu

	if !s.trackConn(sc) {
		_ = conn.Close()
		return
	}

	wg.Add(1)

	readErrCC := make(chan error, 1)
	go s.receiveLoop(sc, readErrCC)
	s.sendLoop(sc, wg)
	if debugPrint {
		fmt.Printf("%v server %p conn %p sendLoop quit\n", time.Now(), s, sc)
	}
	_ = sc.Close() // after writer quit, there is no point to continue connection operation
	<-readErrCC    // wait for reader

	defer s.dropConn(sc)

	_ = sc.WaitClosed()
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
		s:  s,
		ch: make(chan *HandlerContext, 1),
	}

	go w.run(&s.workersGroup)

	return w
}

func (s *Server) receiveLoop(sc *serverConn, readErrCC chan<- error) {
	hctxToRelease, err := s.receiveLoopImpl(sc)
	if hctxToRelease != nil {
		hctxToRelease.serverConn.releaseHandlerCtx(hctxToRelease)
	}
	if err != nil {
		_ = sc.Close()
	}
	sc.cancelAllLongpollResponses() // we always cancel from receiving goroutine.
	readErrCC <- err
}

func (s *Server) receiveLoopImpl(sc *serverConn) (*HandlerContext, error) {
	for {
		// read header first, before acquiring handler context,
		// to be able to disconnect event when all handler contexts are taken
		var header packetHeader
		head, err := sc.conn.readPacketHeaderUnlocked(&header, maxIdleDuration)
		if err != nil {
			if len(head) == 0 && (err == io.EOF || err == io.ErrUnexpectedEOF) {
				if debugPrint {
					fmt.Printf("%v server %p conn %p reader received FIN\n", time.Now(), s, sc)
				}
				sc.SetReadFIN()
				return nil, nil // clean shutdown, finish writing then close
			}
			if (len(head) > 0 && !sc.closed() && !commonConnCloseError(err)) || s.opts.LogCommonNetworkErrors {
				s.opts.Logf("rpc: error reading packet header from %v, disconnecting: %v, head: %+q", sc.conn.remoteAddr, err, head)
			}
			return nil, err
		}
		requestTime := time.Now()

		hctx, ok := sc.acquireHandlerCtx(header.tip, s.opts.Hooks.InitState)
		if !ok {
			return nil, ErrServerClosed
		}
		s.statRequestsCurrent.Inc()

		hctx.RequestTime = requestTime
		hctx.defaultTimeout = s.opts.DefaultResponseTimeout
		hctx.timeoutAdjust = s.opts.ResponseTimeoutAdjust
		hctx.reqHeader = header

		req, reqTaken, err := s.acquireRequestBuf(sc.closeCtx, int(hctx.reqHeader.length))
		if err != nil {
			return hctx, err
		}
		hctx.request = req
		hctx.reqTaken = reqTaken
		if hctx.request != nil {
			hctx.Request = *hctx.request
		}
		hctx.Request, err = sc.conn.readPacketBodyUnlocked(&hctx.reqHeader, hctx.Request, true, maxPacketRWTime)
		if hctx.request != nil {
			*hctx.request = hctx.Request[:0] // prepare for reuse immediately
		}
		if err != nil {
			if !sc.closed() && (!commonConnCloseError(err) || s.opts.LogCommonNetworkErrors) {
				s.opts.Logf("rpc: error reading packet body from %v, disconnecting: %v", sc.conn.remoteAddr, err)
			}
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

		if !s.syncHandler(hctx) {
			if s.opts.MaxWorkers == 0 {
				s.handle(hctx)
			} else {
				w := s.acquireWorker()
				if w == nil {
					return hctx, ErrServerClosed
				}
				w.ch <- hctx
			}
		}
	}
}

func (s *Server) sendLoop(sc *serverConn, wg *WaitGroup) {
	defer wg.Done()

	toRelease := s.sendLoopImpl(sc)
	for _, hctx := range toRelease {
		hctx.serverConn.releaseHandlerCtx(hctx)
	}
}

func (s *Server) sendLoopImpl(sc *serverConn) []*HandlerContext { // returns contexts to release
	writeQ := make([]*HandlerContext, 0, s.opts.maxInflightPacketsPreAlloc())
	sent := false // true if there is data to flush

	for {
		sc.mu.Lock()
		for !(sent || sc.writeLetsFin || len(sc.writeQ) != 0 || sc.closedFlag || (sc.readFINFlag && sc.hctxCreated == len(sc.hctxPool))) {
			sc.cond.Wait()
		}

		if sc.closedFlag || (sc.readFINFlag && sc.hctxCreated == len(sc.hctxPool)) {
			sc.mu.Unlock()
			if sent {
				sent = false
				if err := sc.flush(); err != nil {
					return nil
				}
			}
			if debugPrint {
				fmt.Printf("%v server %p conn %p sendLoop stop\n", time.Now(), s, sc)
			}
			return nil
		}
		writeLetsFin := sc.writeLetsFin
		sc.writeLetsFin = false
		writeQ, sc.writeQ = sc.writeQ, writeQ[:0]
		sc.mu.Unlock()
		if !writeLetsFin && len(writeQ) == 0 { // implies the only true condition above is sent == true, so we flush
			sent = false
			if err := sc.flush(); err != nil {
				return nil
			}
			continue
		}
		sent = true
		if writeLetsFin {
			if debugPrint {
				fmt.Printf("%v server %p conn %p writes Let's FIN\n", time.Now(), s, sc)
			}
			if err := sc.conn.startWritePacketUnlocked(packetTypeRPCServerWantsFin, maxPacketRWTime); err != nil {
				return writeQ // release remaining contexts
			}
			if err := sc.conn.writeSimplePacketUnlocked(nil); err != nil {
				return writeQ // release remaining contexts
			}
		}
		for i, hctx := range writeQ {
			err := sc.writeResponseUnlocked(hctx, maxPacketRWTime)
			if err != nil {
				if !sc.closed() && (!commonConnCloseError(err) || s.opts.LogCommonNetworkErrors) {
					s.opts.Logf("rpc: error writing packet 0x%x#0x%x to %v, disconnecting: %v", hctx.respPacketType, hctx.reqType, sc.conn.remoteAddr, err)
				}
				return writeQ[i:] // release remaining contexts
			}
			sc.releaseHandlerCtx(hctx)
		}
	}
}

func (s *Server) syncHandler(hctx *HandlerContext) bool {
	err := s.doSyncHandler(hctx.serverConn.closeCtx, hctx)
	if err == ErrNoHandler {
		return false
	}
	if err == errHijackResponse {
		// User is now responsible for calling hctx.SendHijackedResponse
		return true
	}
	hctx.releaseRequest()
	hctx.prepareResponse(err)
	s.pushResponse(hctx, false)
	return true
}

func (s *Server) doSyncHandler(ctx context.Context, hctx *HandlerContext) error {
	if hctx.reqHeader.tip == packetTypeRPCCancelReq {
		var queryID int64
		if _, err := basictl.LongRead(hctx.Request, &queryID); err != nil {
			return err
		}
		hctx.noResult = true
		hctx.Response = append(hctx.Response, 0) // only to avoid logging in prepareResponse. Nothing is sent.
		hctx.serverConn.cancelLongpollResponse(queryID)
		return nil
	}
	if hctx.reqHeader.tip != packetTypeRPCInvokeReq || s.opts.SyncHandler == nil {
		return ErrNoHandler
	}
	err := hctx.parseInvokeReq(s)
	if err != nil {
		return err
	}
	// No deadline on sync handler
	return s.opts.SyncHandler(ctx, hctx)
}

func (s *Server) handle(hctx *HandlerContext) {
	err := s.doHandle(hctx.serverConn.closeCtx, hctx)
	if err == errHijackResponse {
		// User is now responsible for calling hctx.SendHijackedResponse
		s.rareLog(&s.lastHijackWarningLog, "you must hijack responses from SyncHandler, not from normal handler")
		return
	}
	hctx.releaseRequest()
	hctx.prepareResponse(err)
	s.pushResponse(hctx, false)
}

func (s *Server) pushResponse(hctx *HandlerContext, isLongpoll bool) {
	if !hctx.noResult { // do not spend time for accounting, will release anyway couple lines below
		hctx.respTaken, _ = s.accountResponseMem(hctx.serverConn.closeCtx, hctx.respTaken, cap(hctx.Response), true)
	}
	hctx.serverConn.push(hctx, isLongpoll)
}

func (s *Server) doHandle(ctx context.Context, hctx *HandlerContext) error {
	switch hctx.reqHeader.tip {
	case PacketTypeRPCPing:
		if len(hctx.Request) != 8 { // we enforce 8 byte size
			return fmt.Errorf("ping packet wrong length %d", len(hctx.Request))
		}
		hctx.respPacketType = PacketTypeRPCPong
		hctx.Response = append(hctx.Response, hctx.Request...)
		return nil
	case packetTypeRPCInvokeReq:
		if s.opts.SyncHandler == nil { // otherwise already passed in connection loop
			if err := hctx.parseInvokeReq(s); err != nil {
				return err
			}
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
			hctx.serverConn.server.opts.Logf("rpc: handler returned empty response with no error query #%v to 0x%x", hctx.queryID, hctx.reqType)
		}
		return
	}
	respErr := Error{}
	switch {
	case err == ErrNoHandler: // this case is only to include reqType into description
		respErr.Code = TlErrorNoHandler
		respErr.Description = fmt.Sprintf("RPC handler for #%08x not found", hctx.reqType)
	case errors.As(err, &respErr):
		// OK, forward the error as-is
	case errors.Is(err, context.DeadlineExceeded):
		respErr.Code = TlErrorTimeout
		respErr.Description = fmt.Sprintf("%s (request timeout was %v)", err.Error(), hctx.timeout())
	default:
		respErr.Code = TlErrorUnknown
		respErr.Description = err.Error()
	}

	if hctx.noResult {
		hctx.serverConn.server.opts.Logf("rpc: failed to handle no_result query #%v to 0x%x: %s", hctx.queryID, hctx.reqType, respErr.Error())
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
			s.opts.Logf("rpc: panic serving %v: %v\n%s", hctx.remoteAddr.String(), r, buf)
			err = &Error{Code: TlErrorInternal, Description: fmt.Sprintf("rpc: HandlerFunc panic: %v serving %v", r, hctx.remoteAddr.String())}
		}
		s.opts.Hooks.Handler.AfterCall(hctx.hooksState, hctx, err)
	}()

	switch hctx.reqType {
	case enginePIDTag:
		return s.handleEnginePID(hctx)
	case engineStatTag:
		return s.handleEngineStat(hctx, false)
	case engineFilteredStatTag:
		return s.handleEngineStat(hctx, true)
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

		s.opts.Hooks.Handler.BeforeCall(hctx.hooksState, hctx)
		err = s.opts.Handler(ctx, hctx)
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
		s.opts.Logf("connections tracking invariant violated in dropConn")
		return
	}
	delete(s.conns, sc)
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

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func IsHijackedResponse(err error) bool {
	return err == errHijackResponse
}

// Also helps garbage collect workers
func (s *Server) rpsCalcLoop(wg *WaitGroup) {
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
