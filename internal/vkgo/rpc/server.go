// Copyright 2025 V Kontakte LLC
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
	"net"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VKCOM/statshouse/internal/vkgo/rpc/internal/gen/constants"
	"github.com/VKCOM/statshouse/internal/vkgo/rpc/internal/gen/tl"
	"github.com/VKCOM/statshouse/internal/vkgo/rpc/udp"
	"github.com/VKCOM/statshouse/internal/vkgo/semaphore"
	"github.com/VKCOM/statshouse/internal/vkgo/srvfunc"
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
	DefaultRequestMemoryLimit     = 256 * 1024 * 1024
	DefaultResponseMemoryLimit    = 2048*1024*1024 - 1
	DefaultServerConnReadBufSize  = maxGoAllocSizeClass
	DefaultServerConnWriteBufSize = maxGoAllocSizeClass
	DefaultServerRequestBufSize   = 4096 // we expect shorter requests on average
	DefaultServerResponseBufSize  = maxGoAllocSizeClass
	DefaultResponseMemEstimate    = 1024 * 1024 // we likely over-account unknown response length before the handler has finished

	minAcceptDelay  = 5 * time.Millisecond
	maxAcceptDelay  = 1 * time.Second
	rpsCalcSeconds  = 5
	rareLogInterval = 1 * time.Second

	maxGoAllocSizeClass = 32768 // from here: https://go.dev/src/runtime/mksizeclasses.go
	tracebackBufSize    = 65536
)

var (
	ErrNoHandler                = &Error{Code: TlErrorNoHandler, Description: "rpc: no handler"} // Never wrap this error
	errLongpollQueryIDCollision = &Error{Code: TlErrorInternal, Description: "rpc: client invariant violation, longpoll queryID repeated"}
	errTooLarge                 = &Error{Code: TLErrorResultToLarge, Description: fmt.Sprintf("rpc: packet size (metadata+extra+response) exceeds %v bytes", maxPacketLen)}
	errGracefulShutdown         = &Error{Code: TlErrorGracefulShutdown, Description: "rpc: server is shutting down"}
	errAlreadyCanceled          = fmt.Errorf("longpoll already have been canceled") // not sent, only written to log
	ErrLongpollNoEmptyResponse  = &Error{Code: TlErrorTimeout, Description: "empty longpoll response not implemented by server"}

	statCPUInfo = srvfunc.MakeCPUInfo() // TODO - remove global
)

type (
	HandlerFunc          func(ctx context.Context, hctx *HandlerContext) error
	StatsHandlerFunc     func(map[string]string)
	VerbosityHandlerFunc func(int) error
	LoggerFunc           func(format string, args ...any)
	ErrHandlerFunc       func(err error)
	RequestHookFunc      func(hctx *HandlerContext, ctx context.Context) context.Context
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
		lc.Control = ControlSetTCPReuse(true, false)
	}

	return lc.Listen(context.Background(), network, address)
}

func NewServer(options ...ServerOptionsFunc) *Server {
	s := &Server{}
	s.newServer(options...)
	return s
}

var serverDebugIDForTests atomic.Int64

// TODO - temporary wrapper for ServerUDP, move into NewServer later
func (s *Server) newServer(options ...ServerOptionsFunc) {
	s.opts = ServerOptions{
		Logf:                   log.Printf,
		MaxConns:               DefaultMaxConns,
		MaxWorkers:             DefaultMaxWorkers,
		RequestMemoryLimit:     DefaultRequestMemoryLimit,
		ResponseMemoryLimit:    DefaultResponseMemoryLimit,
		ConnReadBufSize:        DefaultServerConnReadBufSize,
		ConnWriteBufSize:       DefaultServerConnWriteBufSize,
		RequestBufSize:         DefaultServerRequestBufSize,
		ResponseBufSize:        DefaultServerResponseBufSize,
		ResponseMemEstimate:    DefaultResponseMemEstimate,
		DefaultResponseTimeout: 0,
		StatsHandler:           func(m map[string]string) {},
		Handler:                func(ctx context.Context, hctx *HandlerContext) error { return ErrNoHandler },
		RecoverPanics:          true,
	}
	for _, option := range options {
		option(&s.opts)
	}

	host, _ := os.Hostname()

	s.serverStatus = serverStatusInitial
	s.connsTCP = map[*serverConnTCP]struct{}{}
	s.longpollTree = newLongpollTree()
	s.workersSem = semaphore.NewWeighted(math.MaxInt64)
	s.reqMemSem = semaphore.NewWeighted(int64(s.opts.RequestMemoryLimit))
	s.respMemSem = semaphore.NewWeighted(int64(s.opts.ResponseMemoryLimit))
	s.connSem = semaphore.NewWeighted(int64(s.opts.MaxConns))
	s.startTime = uniqueStartTime()
	s.statHostname = host
	if s.opts.DebugRPC {
		s.debugNameForTests = fmt.Sprintf("%d ", serverDebugIDForTests.Add(1))
	}
	s.reqBufPool.New = func() any {
		var b []byte // allocate heap slice, which will be put into pool in releaseRequest
		return &b
	}
	s.respBufPool.New = func() any {
		var b []byte // allocate heap slice, which will be put into pool in releaseRequest
		return &b
	}
	s.hctxPool.New = func() any {
		return &HandlerContext{}
	}

	s.workerPool = workerPoolNew(s.opts.MaxWorkers, func() {
		if s.opts.MaxWorkers == DefaultMaxWorkers { // print only if user did not change default value
			s.rareLog(&s.lastWorkerWaitLog, "rpc: waiting to acquire worker; consider increasing Server.MaxWorkers")
		}
	})

	s.closeCtx, s.cancelCloseCtx = context.WithCancel(context.Background())

	s.workersSem.ForceAcquire(1)
	go s.rpsCalcLoop(s.workersSem)

	go s.longpollTree.LongpollCheckLoop(s.closeCtx)
}

const (
	serverStatusInitial  = 0 // ordering is important, we use >/< comparisons with statues
	serverStatusStarted  = 1 // after first call to Serve.
	serverStatusShutdown = 2 // after first call to Shutdown(). New Serve() calls will quit immediately. Main context is cancelled, Responses are still being sent.
	serverStatusStopped  = 3 // after first call to Wait. Everything is torn down, except if some handlers did not return
)

type protocolStats struct {
	connectionsTotal   atomic.Int64
	connectionsCurrent atomic.Int64
	requestsTotal      atomic.Int64
	requestsCurrent    atomic.Int64
	rps                atomic.Int64
	longPollsWaiting   atomic.Int64
}

const protocolTCP = 0
const protocolUDP = 1

func protocolName(ID byte) string {
	if ID == 0 {
		return "TCP"
	}
	return "UDP"
}

type Server struct {
	// Place atomics first to ensure proper alignment, see https://pkg.go.dev/sync/atomic#pkg-note-BUG
	protocolStats     [2]protocolStats
	udpStatsPerSecond udp.TransportStats
	engineShutdown    atomic.Bool
	nextConnID        atomic.Int64

	statHostname string

	debugNameForTests string

	opts ServerOptions

	mu           sync.Mutex
	serverStatus int
	listeners    []net.Listener // will close after
	connsTCP     map[*serverConnTCP]struct{}

	// Longpoll timeouts have to be stored in the server, not in the connection struct
	longpollTree *longpollTree

	transportsUDP []*udp.Transport // many transportsUDP, one per listen address

	workerPool *workerPool
	workersSem *semaphore.Weighted // we do not use wait group to reduce # of primitives to implement in custom scheduler

	closeCtx       context.Context
	cancelCloseCtx context.CancelFunc
	connSem        *semaphore.Weighted
	reqMemSem      *semaphore.Weighted
	respMemSem     *semaphore.Weighted
	reqBufPool     sync.Pool
	respBufPool    sync.Pool
	hctxPool       sync.Pool

	startTime uint32

	rareLogMu              sync.Mutex
	lastReqMemWaitLog      time.Time
	lastRespMemWaitLog     time.Time
	lastWorkerWaitLog      time.Time
	lastLongpollWarningLog time.Time
	lastPacketTypeLog      time.Time
	lastReadErrorLog       time.Time
	lastPushToClosedLog    time.Time
	lastOtherLog           time.Time // TODO - may be split this into different error classes
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
	if h == nil {
		panic("stats handler is nil")
	}
	if s.opts.StatsHandler == nil {
		s.opts.StatsHandler = h
	} else {
		oldHandler := s.opts.StatsHandler
		s.opts.StatsHandler = func(m map[string]string) {
			oldHandler(m)
			h(m)
		}
	}
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

// Server automatically responds to engine.Pid as a healthcheck status.
// there is a protocol between barsic, pinger and server, that when barsic commands engine shutdown
// engine must respond to the engine.Pid with a special error condition.
func (s *Server) SetEngineShutdownStatus(engineShutdown bool) {
	s.engineShutdown.Store(engineShutdown)
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
	if s.opts.DebugRPC {
		s.opts.Logf("rpc_debug: Server %sShutdown\n", s.debugNameForTests)
	}

	for _, ln := range s.listeners {
		_ = ln.Close() // We do not care if there was some pending connections
	}
	s.listeners = s.listeners[:0]

	for sc := range s.connsTCP {
		sc.shutdown()
	}

	for _, transport := range s.transportsUDP {
		transport.Shutdown()
	}
}

// Waits for all connections to be closed. If all clients follow protocol, this happens quickly.
func (s *Server) CloseWait(ctx context.Context) error {
	s.Shutdown()

	// After shutdown, if clients follow protocol, all connections will soon be closed
	// we can acquire whole semaphore only if all connections finished
	return s.connSem.WaitEmpty(ctx)
}

func (s *Server) Close() error {
	s.Shutdown()

	s.mu.Lock()
	if s.serverStatus >= serverStatusStopped {
		s.mu.Unlock()
		return nil
	}
	s.serverStatus = serverStatusStopped

	if s.opts.DebugRPC {
		s.opts.Logf("rpc_debug: Server %sClose will close TCP connections\n", s.debugNameForTests)
	}

	cause := fmt.Errorf("server Close called")

	for sc := range s.connsTCP {
		sc.close(cause)
	}

	for _, transport := range s.transportsUDP {
		_ = transport.Close()
	}

	s.mu.Unlock()

	if s.opts.DebugRPC {
		s.opts.Logf("rpc_debug: Server %sClose waiting TCP connections to finish\n", s.debugNameForTests)
	}

	s.cancelCloseCtx()

	_ = s.connSem.WaitEmpty(context.Background())

	if s.opts.DebugRPC {
		s.opts.Logf("rpc_debug: Server %sClose waiting workers to finish\n", s.debugNameForTests)
	}

	// here we should have no executing handlers, because otherwise connections would not close above
	// any worker got before Close will have channel not closed, so work will be sent there and executed
	// after that Put will return false and worker will quit
	s.workerPool.Close()

	_ = s.workersSem.WaitEmpty(context.Background())

	if len(s.connsTCP) != 0 {
		s.opts.Logf("rpc: tracking of connection invariant violated after close wait - %d connections", len(s.connsTCP))
	}
	if cur, _ := s.reqMemSem.Observe(); cur != 0 {
		s.opts.Logf("rpc: tracking of request memory invariant violated after close wait - %d bytes", cur)
	}
	if cur, _ := s.respMemSem.Observe(); cur != 0 {
		s.opts.Logf("rpc: tracking of request memory invariant violated after close wait - %d bytes", cur)
	}
	if cur := s.protocolStats[protocolTCP].requestsCurrent.Load(); cur != 0 {
		s.opts.Logf("rpc: tracking of current TCP requests invariant violated after close wait - %d requests", cur)
	}
	if cur := s.protocolStats[protocolUDP].requestsCurrent.Load(); cur != 0 {
		s.opts.Logf("rpc: tracking of current UDP requests invariant violated after close wait - %d requests", cur)
	}
	if s.opts.DebugRPC {
		s.opts.Logf("rpc_debug: Server %sClose finished\n", s.debugNameForTests)
	}
	return nil
}

func (s *Server) requestBufTake(reqBodySize int) int {
	return max(reqBodySize, s.opts.RequestBufSize) // we consider that most buffers in the pool will be stretched up to max size
}

func (s *Server) responseBufTake(respBodySizeEstimate int) int {
	return max(respBodySizeEstimate, s.opts.ResponseBufSize) // we consider that most buffers in the pool will be stretched up to max size
}

func (s *Server) acquireRequestSema(ctx context.Context, taken int) error {
	ok := s.reqMemSem.TryAcquire(int64(taken))
	if !ok {
		cur, size := s.reqMemSem.Observe()
		s.rareLog(&s.lastReqMemWaitLog,
			"rpc: waiting to acquire request memory (want %s, mem %s, limit %s, %d conns, %d reqs); consider increasing Server.RequestMemoryLimit",
			humanByteCountIEC(int64(taken)),
			humanByteCountIEC(cur),
			humanByteCountIEC(size),
			s.ConnectionsCurrent(),
			s.RequestsCurrent(),
		)
		err := s.reqMemSem.Acquire(ctx, int64(taken))
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) acquireRequestBuf(take int) (*[]byte, []byte) {
	if take > s.opts.RequestBufSize {
		v := make([]byte, take) // large requests will not go back to pool, so fall back to GC
		return nil, v
	}
	v := s.reqBufPool.Get().(*[]byte)
	if cap(*v) < take {
		*v = make([]byte, take)
	} else {
		*v = (*v)[:take]
	}
	return v, *v
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
	taken, err := s.accountResponseMem(ctx, 0, s.opts.ResponseMemEstimate, false)
	if err != nil {
		return nil, 0, err
	}
	// we do not know if handler will write large or small response, so we always give small response from pool
	// if handler will write large response, we will release in releaseResponseBuf, as with requests
	return s.respBufPool.Get().(*[]byte), taken, nil
}

func (s *Server) accountResponseMem(ctx context.Context, taken int, respBodySizeEstimate int, force bool) (int, error) {
	need := s.responseBufTake(respBodySizeEstimate)
	if need <= taken {
		if need == taken {
			return need, nil
		}
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
			s.ConnectionsCurrent(),
			s.RequestsCurrent(),
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

func (s *Server) ListenAndServe(network string, address string) error {
	if network == "udp" || network == "udp4" || network == "udp6" {
		ln, err := ListenUDP(network, address)
		if err != nil {
			return err
		}
		return s.ServeUDP(ln)
	}
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
	if s.opts.DebugRPC {
		s.opts.Logf("rpc_debug: Server %sServe addr=%v\n", s.debugNameForTests, ln.Addr())
	}
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

		s.protocolStats[protocolTCP].connectionsTotal.Add(1)
		s.protocolStats[protocolTCP].connectionsCurrent.Add(1)

		go s.goHandshake(conn, ln.Addr())                        // will release connSem
		if err := s.connSem.Acquire(s.closeCtx, 1); err != nil { // we need to acquire again to Accept()
			return nil
		}
	}
}

func (s *Server) goHandshake(conn *PacketConn, lnAddr net.Addr) {
	defer s.connSem.Release(1)
	defer s.protocolStats[protocolTCP].connectionsCurrent.Add(-1) // we have the same value in connSema, but reading from here is faster

	magicHead, flags, err := conn.HandshakeServer(s.opts.cryptoKeys, s.opts.TrustedSubnetGroups, s.opts.ForceEncryption, s.startTime, DefaultPacketTimeout)
	if err != nil {
		switch string(magicHead) {
		case memcachedStatsReqRN, memcachedStatsReqN, memcachedGetStatsReq:
			s.respondWithMemcachedStats(conn)
		case memcachedVersionReq:
			s.respondWithMemcachedVersion(conn)
		default:
			if len(magicHead) != 0 && s.opts.SocketHijackHandler != nil {
				pc, buf := conn.HijackConnection()
				// We do not close connection, ownership is moved to SocketHijackHandler
				s.opts.SocketHijackHandler(&HijackConnection{Magic: append(magicHead, buf...), Conn: pc})
				return
			}
			// We have some admin scripts which test port by connecting and then disconnecting, looks like port probing, but OK
			// If you want to make logging unconditional, please ask Petr Mikushin @petr8822 first.
			if len(magicHead) != 0 {
				s.rareLog(&s.lastOtherLog, "rpc: failed to handshake with %s, peer sent(hex) %x, disconnecting: %v", conn.RemoteAddr(), magicHead, err)
			}
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

	// contexts of connections are completely separate, they are cancelled individually, when
	// connection is closed, including in Server.Close().
	closeCtx, cancelCloseCtx := context.WithCancelCause(context.Background())

	sc := &serverConnTCP{
		serverConnCommon: serverConnCommon{
			server:         s,
			closeCtx:       closeCtx,
			cancelCloseCtx: cancelCloseCtx,
			longpolls:      map[int64]longpollHctx{},
		},
		listenAddr: lnAddr,
		conn:       conn,
		errHandler: s.opts.ConnErrHandler,
	}
	sc.setDebugName("TCP", conn.RemoteAddr(), conn.LocalAddr())
	sc.writeQCond.L = &sc.mu
	sc.closeWaitCond.L = &sc.mu

	if !s.trackConn(sc) {
		// server is shutting down
		_ = conn.Close()
		return
	}
	if s.opts.DebugRPC {
		s.opts.Logf("rpc_debug: %s Handshake", sc.debugName)
	}

	readErrCC := make(chan error, 1)
	go s.receiveLoop(sc, readErrCC)
	writeErr := s.sendLoop(sc)
	if s.opts.DebugRPC {
		s.opts.Logf("rpc_debug: %s SendLoop quit with err %v", sc.debugName, writeErr)
	}
	sc.close(writeErr)
	<-readErrCC // wait for reader
	if s.opts.DebugRPC {
		s.opts.Logf("rpc_debug: %s Waiting for inFlight 0", sc.debugName)
	}

	sc.waitClosed()

	s.dropConn(sc)
	if s.opts.DebugRPC {
		s.opts.Logf("rpc_debug: %s Finished", sc.debugName)
	}
}

func (s *Server) rareLog(last *time.Time, format string, args ...any) {
	now := time.Now()

	s.rareLogMu.Lock()
	defer s.rareLogMu.Unlock()

	if s.opts.DebugRPC || now.Sub(*last) > rareLogInterval {
		*last = now
		s.opts.Logf(format, args...)
	}
}

func (s *Server) acquireWorker() *worker {
	v, ok := s.workerPool.Get(s.workersSem)
	if !ok { // closed, must be never. TODO - add panic and test
		return nil
	}
	if v != nil {
		return v
	}

	w := &worker{
		workerPool: s.workerPool,
		ch:         make(chan workerWork, 1),
	}

	go w.run(s, s.workersSem)
	return w
}

func (s *Server) receiveLoop(sc *serverConnTCP, readErrCC chan<- error) {
	hctxToRelease, err := s.receiveLoopImpl(sc)
	if hctxToRelease != nil {
		sc.releaseHandlerCtx(hctxToRelease)
	}
	if s.opts.DebugRPC {
		s.opts.Logf("rpc_debug: %s ReceiveLoop quit with err %v", sc.debugName, err)
	}
	sc.close(err)
	readErrCC <- err
}

func (s *Server) acquireHCtxResponse(ctx context.Context, hctx *HandlerContext) error {
	resp, respTaken, err := s.acquireResponseBuf(ctx)
	if err != nil {
		return err
	}
	hctx.response = resp
	hctx.Response = (*hctx.response)[:0]
	hctx.respTaken = respTaken

	return nil
}

func (s *Server) receiveLoopImpl(sc *serverConnTCP) (*HandlerContext, error) {
	for {
		// read header first, before acquiring handler context,
		// to be able to disconnect event when all handler contexts are taken
		var header packetHeader
		head, isBuiltin, _, err := sc.conn.ReadPacketHeaderUnlocked(&header, DefaultPacketTimeout*11/10)
		// motivation for slightly increasing timeout is so that client and server will not send pings to each other, client will do it first
		if err != nil {
			if len(head) != 0 {
				// We complain only if partially read header.
				s.rareLog(&s.lastReadErrorLog, "rpc: %s error reading packet header (hex: %x), disconnecting: %v", sc.debugName, head, err)
				return nil, err
			}
			return nil, nil // otherwise we consider it a clean shutdown. If sendLoop did not finish writing, it will complain
		}
		if isBuiltin {
			sc.SetWriteBuiltin()
			continue
		}
		// TODO - read packet body for tl.RpcCancelReq without waiting on semaphores
		requestTime := time.Now()

		hctx := sc.acquireHandlerCtx()

		hctx.requestTime = requestTime

		reqTaken := s.requestBufTake(int(header.length))
		if err := s.acquireRequestSema(sc.closeCtx, reqTaken); err != nil {
			return hctx, err
		}
		hctx.reqTaken = reqTaken
		hctx.request, hctx.Request = s.acquireRequestBuf(reqTaken)
		hctx.Request, err = sc.conn.ReadPacketBodyUnlocked(&header, hctx.Request)
		if err != nil {
			// failing to fully read packet is always problem we want to report
			s.rareLog(&s.lastReadErrorLog, "rpc: %s error reading packet body (%d/%d bytes read), disconnecting: %v", sc.debugName, len(hctx.Request), header.length, err)
			return hctx, err
		}

		if err := s.acquireHCtxResponse(sc.closeCtx, hctx); err != nil {
			return hctx, err
		}

		s.protocolStats[protocolTCP].requestsTotal.Add(1)

		if header.tip == (tl.RpcClientWantsFin{}.TLTag()) {
			// We handle it normally as noResult request below, to simplify hctx accounting
			sc.shutdown()
		}
		w, ctx := s.handleRequest(sc.closeCtx, header.tip, hctx)
		if w != nil {
			w.ch <- workerWork{hctx: hctx, ctx: ctx}
		}
	}
}

func (s *Server) sendLoop(sc *serverConnTCP) error {
	toRelease, err := s.sendLoopImpl(sc) // err is logged inside, if needed
	if len(toRelease) != 0 {
		s.rareLog(&s.lastPushToClosedLog, "rpc: %s failed to push %d responses because connection was closed", sc.debugName, len(toRelease))
	}
	for _, hctx := range toRelease {
		sc.releaseHandlerCtx(hctx)
	}
	return err
}

func (s *Server) sendLoopImpl(sc *serverConnTCP) ([]*HandlerContext, error) { // returns contexts to release
	var writeQ []*HandlerContext
	sent := false // true if there is data to flush
	writtenLetsFin := false

	sc.mu.Lock()
	for {
		shouldStop := sc.connectionStatus >= serverStatusStopped
		writeLetsFin := sc.connectionStatus == serverStatusShutdown
		if !(sent || (writeLetsFin && !writtenLetsFin) || sc.writeBuiltin || len(sc.writeQ) != 0 || shouldStop) {
			sc.writeQCond.Wait()
			continue
		}

		writeBuiltin := sc.writeBuiltin
		sc.writeBuiltin = false
		writeQ, sc.writeQ = sc.writeQ, writeQ[:0]
		sc.mu.Unlock()
		if shouldStop {
			return writeQ, nil
		}
		sentNow := false
		if writeBuiltin {
			sentNow = true
			if err := sc.conn.WritePacketBuiltinNoFlushUnlocked(DefaultPacketTimeout); err != nil {
				// No log here, presumably failing to send ping/pong to closed connection is not a problem
				return writeQ, err // release remaining contexts
			}
		}
		if writeLetsFin && !writtenLetsFin {
			writtenLetsFin = true
			if sc.conn.FlagCancelReq() {
				sentNow = true
				if s.opts.DebugRPC {
					s.opts.Logf("rpc_debug: %s Write serverWantsFIN packet\n", sc.debugName)
				}
				if err := sc.conn.WritePacketHeaderUnlocked(tl.RpcServerWantsFin{}.TLTag(), 0, DefaultPacketTimeout); err != nil {
					// No log here, presumably failing to send letsFIN to closed connection is not a problem
					return writeQ, err // release remaining contexts
				}
				sc.conn.WritePacketTrailerUnlocked()
			}
		}
		for i, hctx := range writeQ {
			sentNow = true
			if s.opts.DebugRPC {
				s.opts.Logf("rpc_debug: %s Writing response queryID=%d extra=%s body=%x\n", sc.debugName, hctx.queryID, hctx.ResponseExtra.String(), hctx.Response[:hctx.extraStart])
			}
			err := writeResponseUnlocked(sc.conn, hctx)
			if err != nil {
				s.rareLog(&s.lastOtherLog, "rpc: %s error writing packet reqTag #%08x, disconnecting: %v", sc.debugName, hctx.reqTag, err)
				return writeQ[i:], err // release remaining contexts
			}
			sc.releaseHandlerCtx(hctx)
		}
		if sent && !sentNow {
			if s.opts.DebugRPC {
				s.opts.Logf("rpc_debug: %s Flush\n", sc.debugName)
			}
			if err := sc.conn.FlushUnlocked(); err != nil {
				sc.server.rareLog(&sc.server.lastOtherLog, "rpc: %s error flushing packet, disconnecting: %v", sc.debugName, err)
				return nil, err
			}
		}
		sent = sentNow
		sc.mu.Lock()
	}
}

func (s *Server) handleRequest(ctx context.Context, reqHeaderTip uint32, hctx *HandlerContext) (*worker, context.Context) {
	ctx, err := s.doSyncHandler(ctx, reqHeaderTip, hctx)
	// if error is ErrNoHandler, that means SyncHandler did not process request.
	// Request will go through common RPC-request handler's logic with acquiring RPC-worker.
	// Also, if longpollStarted, we release hctx and reduce inFlight
	if hctx.longpollStarted || !errors.Is(err, ErrNoHandler) {
		hctx.SendLongpollResponse(err)
		return nil, nil
	}

	// in many projects maxWorkers is cmd argument, and ppl want to disable worker pool with this argument
	if s.opts.MaxWorkers > 0 {
		w := s.acquireWorker()
		if w != nil {
			return w, ctx
		}
		// otherwise pool is closed and we call synchronously, TODO - check this is impossible
	}

	err = s.callHandler(ctx, hctx)
	hctx.SendLongpollResponse(err)
	return nil, ctx
}

func (s *Server) cancelLongpoll(sc HandlerContextConnection, queryID int64) {
	canceller, deadline := sc.CancelLongpoll(queryID)
	if canceller == nil { // already cancelled or sent
		if s.opts.DebugRPC {
			s.opts.Logf("rpc_debug: %s longpoll cancel (NOP) queryID=%d\n", sc.DebugName(), queryID)
		}
		return
	}
	if s.opts.DebugRPC {
		s.opts.Logf("rpc_debug: %s longpoll cancel queryID=%d\n", sc.DebugName(), queryID)
	}
	lh := LongpollHandle{QueryID: queryID, CommonConn: sc}
	if deadline != 0 {
		s.longpollTree.DeleteLongpoll(lh, deadline)
	}
	canceller.CancelLongpoll(lh)
	// if sc.server.opts.ResponseHook != nil {
	// 	sc.server.opts.ResponseHook(resp.lctx, errCancelHijack) // TODO: что тут делать в случае с лонгполлом?
	// }
}

func (s *Server) doSyncHandler(ctx context.Context, reqHeaderTip uint32, hctx *HandlerContext) (context.Context, error) {
	switch reqHeaderTip {
	case tl.RpcCancelReq{}.TLTag():
		cancelReq := tl.RpcCancelReq{}
		if _, err := cancelReq.Read(hctx.Request); err != nil {
			return ctx, err
		}
		if s.opts.DebugRPC {
			s.opts.Logf("rpc_debug: %s RpcCancelReq received queryID=%d\n", hctx.commonConn.DebugName(), cancelReq.QueryId)
		}
		hctx.noResult = true
		s.cancelLongpoll(hctx.commonConn, cancelReq.QueryId)
		return ctx, nil
	case tl.RpcClientWantsFin{}.TLTag():
		// also processed in reader code
		hctx.noResult = true
		return ctx, nil
	case tl.RpcInvokeReqHeader{}.TLTag():
		err := hctx.ParseInvokeReq(&s.opts)
		if err != nil {
			return ctx, err
		}
		if s.opts.DebugRPC {
			s.opts.Logf("rpc_debug: %s SyncHandler packet tag=#%08x queryID=%d extra=%s body=%x\n", hctx.commonConn.DebugName(), reqHeaderTip, hctx.queryID, hctx.RequestExtra.String(), hctx.Request)
		}
		if s.opts.RequestHook != nil {
			ctx = s.opts.RequestHook(hctx, ctx)
		}
		if s.opts.SyncHandler == nil {
			return ctx, ErrNoHandler
		}
		// No deadline on sync handler context, too costly
		return ctx, s.opts.SyncHandler(ctx, hctx)
	}
	hctx.noResult = true
	s.rareLog(&s.lastPacketTypeLog, "rpc: %s unknown packet type 0x%x", hctx.commonConn.DebugName(), reqHeaderTip)
	return ctx, nil
}

func (s *Server) callHandler(ctx context.Context, hctx *HandlerContext) (err error) {
	if !s.opts.RecoverPanics {
		return s.callHandlerNoRecover(ctx, hctx)
	}
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, tracebackBufSize)
			buf = buf[:runtime.Stack(buf, false)]
			debugName := hctx.commonConn.DebugName()
			s.opts.Logf("rpc: %s panic in handler: %v\n%s", debugName, r, buf)
			err = &Error{Code: TlErrorInternal, Description: fmt.Sprintf("rpc: %s panic in handler: %v", debugName, r)}
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
	case 0xabcb5b38: // TODO why netDumpUdpTargets is missing in internal constants?
		return s.handleNetDumpUdpTargets(ctx, hctx)
	default:
		if hctx.timeout != 0 {
			deadline := hctx.requestTime.Add(hctx.timeout)
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
		if hctx.longpollStarted {
			panic("you can only start longpoll from SyncHandler (to keep ordering between invokeReq and subsequent cancelReq)")
		}
		return err
	}
}

func (s *Server) trackConn(sc *serverConnTCP) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.serverStatus >= serverStatusShutdown { // no new connections when shutting down
		return false
	}
	s.connsTCP[sc] = struct{}{}
	return true
}

func (s *Server) dropConn(sc *serverConnTCP) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.connsTCP[sc]; !ok {
		s.opts.Logf("rpc: %s connections tracking invariant violated in dropConn", sc.debugName)
		return
	}
	delete(s.connsTCP, sc)
}

func (s *Server) acquireHandlerCtx(conn HandlerContextConnection, protocolTransportID byte) *HandlerContext {
	hctx := s.hctxPool.Get().(*HandlerContext)
	hctx.protocolTransportID = protocolTransportID
	hctx.commonConn = conn
	s.protocolStats[protocolTransportID].requestsCurrent.Add(1)
	return hctx
}

func (s *Server) releaseHandlerCtx(hctx *HandlerContext) {
	s.protocolStats[hctx.protocolTransportID].requestsCurrent.Add(-1)

	hctx.releaseRequest(s)
	hctx.releaseResponse(s)
	hctx.reset()

	s.hctxPool.Put(hctx)
}

func commonConnCloseError(err error) bool {
	// TODO - better classification in some future go version
	s := err.Error()
	return strings.HasSuffix(s, "EOF") ||
		strings.HasSuffix(s, "broken pipe") ||
		strings.HasSuffix(s, "reset by peer") ||
		strings.HasSuffix(s, "use of closed network connection")
}

func IsLongpollResponse(err error) bool { // TODO - remove after updating tlgen
	return false
}

// Also helps garbage collect workers
func (s *Server) rpsCalcLoop(wg *semaphore.Weighted) {
	defer wg.Release(1)
	tick := time.NewTicker(rpsCalcSeconds * time.Second)
	defer tick.Stop()
	var prev [2]int64
	prevUdpStats := new(udp.TransportStats)
	udpStats := new(udp.TransportStats)
	for {
		select {
		case now := <-tick.C:
			for i := range s.protocolStats {
				cur := s.protocolStats[i].requestsTotal.Load()
				s.protocolStats[i].rps.Store((cur - prev[i]) / rpsCalcSeconds)
				prev[i] = cur
			}
			for _, t := range s.transportsUDP {
				t.GetStats(udpStats)
			}
			// per second udp metrics
			s.udpStatsPerSecond.NewIncomingMessages.Store((udpStats.NewIncomingMessages.Load() - prevUdpStats.NewIncomingMessages.Load()) / rpsCalcSeconds)
			s.udpStatsPerSecond.MessageHandlerCalled.Store((udpStats.MessageHandlerCalled.Load() - prevUdpStats.MessageHandlerCalled.Load()) / rpsCalcSeconds)
			s.udpStatsPerSecond.MessageReleased.Store((udpStats.MessageReleased.Load() - prevUdpStats.MessageReleased.Load()) / rpsCalcSeconds)
			s.udpStatsPerSecond.DatagramRead.Store((udpStats.DatagramRead.Load() - prevUdpStats.DatagramRead.Load()) / rpsCalcSeconds)
			s.udpStatsPerSecond.DatagramWritten.Store((udpStats.DatagramWritten.Load() - prevUdpStats.DatagramWritten.Load()) / rpsCalcSeconds)
			s.udpStatsPerSecond.UnreliableMessagesReceived.Store((udpStats.UnreliableMessagesReceived.Load() - prevUdpStats.UnreliableMessagesReceived.Load()) / rpsCalcSeconds)
			s.udpStatsPerSecond.ObsoletePidReceived.Store((udpStats.ObsoletePidReceived.Load() - prevUdpStats.ObsoletePidReceived.Load()) / rpsCalcSeconds)
			s.udpStatsPerSecond.ObsoleteHashReceived.Store((udpStats.ObsoleteHashReceived.Load() - prevUdpStats.ObsoleteHashReceived.Load()) / rpsCalcSeconds)
			s.udpStatsPerSecond.ObsoleteGenerationReceived.Store((udpStats.ObsoleteGenerationReceived.Load() - prevUdpStats.ObsoleteGenerationReceived.Load()) / rpsCalcSeconds)
			s.udpStatsPerSecond.ResendRequestReceived.Store((udpStats.ResendRequestReceived.Load() - prevUdpStats.ResendRequestReceived.Load()) / rpsCalcSeconds)
			s.udpStatsPerSecond.UnreliableMessagesSent.Store((udpStats.UnreliableMessagesSent.Load() - prevUdpStats.UnreliableMessagesSent.Load()) / rpsCalcSeconds)
			s.udpStatsPerSecond.ObsoletePidSent.Store((udpStats.ObsoletePidSent.Load() - prevUdpStats.ObsoletePidSent.Load()) / rpsCalcSeconds)
			s.udpStatsPerSecond.ObsoleteHashSent.Store((udpStats.ObsoleteHashSent.Load() - prevUdpStats.ObsoleteHashSent.Load()) / rpsCalcSeconds)
			s.udpStatsPerSecond.ObsoleteGenerationSent.Store((udpStats.ObsoleteGenerationSent.Load() - prevUdpStats.ObsoleteGenerationSent.Load()) / rpsCalcSeconds)
			s.udpStatsPerSecond.ResendRequestSent.Store((udpStats.ResendRequestSent.Load() - prevUdpStats.ResendRequestSent.Load()) / rpsCalcSeconds)
			s.udpStatsPerSecond.ResendTimerBurned.Store((udpStats.ResendTimerBurned.Load() - prevUdpStats.ResendTimerBurned.Load()) / rpsCalcSeconds)
			s.udpStatsPerSecond.AckTimerBurned.Store((udpStats.AckTimerBurned.Load() - prevUdpStats.AckTimerBurned.Load()) / rpsCalcSeconds)
			s.udpStatsPerSecond.ResendRequestTimerBurned.Store((udpStats.ResendRequestTimerBurned.Load() - prevUdpStats.ResendRequestTimerBurned.Load()) / rpsCalcSeconds)
			s.udpStatsPerSecond.RegenerateTimerBurned.Store((udpStats.RegenerateTimerBurned.Load() - prevUdpStats.RegenerateTimerBurned.Load()) / rpsCalcSeconds)
			s.udpStatsPerSecond.HoleSeqNumsSent.Store((udpStats.HoleSeqNumsSent.Load() - prevUdpStats.HoleSeqNumsSent.Load()) / rpsCalcSeconds)
			s.udpStatsPerSecond.RequestedSeqNumsOutOfWindow.Store((udpStats.RequestedSeqNumsOutOfWindow.Load() - prevUdpStats.RequestedSeqNumsOutOfWindow.Load()) / rpsCalcSeconds)
			s.udpStatsPerSecond.RequestedSeqNumsActuallyAcked.Store((udpStats.RequestedSeqNumsActuallyAcked.Load() - prevUdpStats.RequestedSeqNumsActuallyAcked.Load()) / rpsCalcSeconds)
			s.udpStatsPerSecond.RequestedSeqNumsNotAcked.Store((udpStats.RequestedSeqNumsNotAcked.Load() - prevUdpStats.RequestedSeqNumsNotAcked.Load()) / rpsCalcSeconds)
			// absolute udp metrics
			s.udpStatsPerSecond.IncomingMessagesInflight.Store(udpStats.IncomingMessagesInflight.Load())
			s.udpStatsPerSecond.OutgoingMessagesInflight.Store(udpStats.OutgoingMessagesInflight.Load())
			s.udpStatsPerSecond.ConnectionsMapSize.Store(udpStats.ConnectionsMapSize.Load())
			s.udpStatsPerSecond.MemoryWaitersSize.Store(udpStats.MemoryWaitersSize.Load())
			s.udpStatsPerSecond.AcquiredMemory.Store(udpStats.AcquiredMemory.Load())

			prevUdpStats, udpStats = udpStats, prevUdpStats
			*udpStats = udp.TransportStats{}

			for i := 0; i < rpsCalcSeconds; i++ { // collect one worker per second
				s.workerPool.GC(now)
			}
		case <-s.closeCtx.Done():
			return
		}
	}
}

func (s *Server) ConnectionsTotal() int64 {
	return s.protocolStats[protocolTCP].connectionsTotal.Load() + s.protocolStats[protocolUDP].connectionsTotal.Load()
}

func (s *Server) ConnectionsTCPTotal() int64 {
	return s.protocolStats[protocolTCP].connectionsTotal.Load()
}

func (s *Server) ConnectionsUDPTotal() int64 {
	return s.protocolStats[protocolUDP].connectionsTotal.Load()
}

func (s *Server) ConnectionsCurrent() int64 {
	return s.protocolStats[protocolTCP].connectionsCurrent.Load() + s.protocolStats[protocolUDP].connectionsCurrent.Load()
}

func (s *Server) ConnectionsTCPCurrent() int64 {
	return s.protocolStats[protocolTCP].connectionsCurrent.Load()
}

func (s *Server) ConnectionsUDPCurrent() int64 {
	return s.protocolStats[protocolUDP].connectionsCurrent.Load()
}

func (s *Server) RequestsTotal() int64 {
	return s.protocolStats[protocolTCP].requestsTotal.Load() + s.protocolStats[protocolUDP].requestsTotal.Load()
}

func (s *Server) RequestsCurrent() int64 {
	return s.protocolStats[protocolTCP].requestsCurrent.Load() + s.protocolStats[protocolUDP].requestsCurrent.Load()
}

func (s *Server) WorkersPoolSize() (current int, total int) {
	return s.workerPool.Created()
}

func (s *Server) LongPollsWaiting() int64 {
	return s.protocolStats[protocolTCP].longPollsWaiting.Load() + s.protocolStats[protocolUDP].longPollsWaiting.Load()
}

func (s *Server) RequestsMemory() (current int64, total int64) {
	return s.reqMemSem.Observe()
}

func (s *Server) ResponsesMemory() (current int64, total int64) {
	return s.respMemSem.Observe()
}

func (s *Server) RPS() int64 {
	return s.protocolStats[protocolTCP].rps.Load() + s.protocolStats[protocolUDP].rps.Load()
}

func (s *Server) toLongpollContext(hctx *HandlerContext, canceller LongpollCanceller) (LongpollHandle, longpollHctx) {
	timeout := hctx.timeout
	// Don't forget to check that timeout is big enough
	if timeout != 0 && timeout < s.opts.MinimumLongpollTimeout {
		timeout = s.opts.MinimumLongpollTimeout
	}
	var deadline int64
	if timeout != 0 {
		deadline = hctx.RequestTime().Add(timeout * 7 / 8).UnixNano()
	}

	lh := LongpollHandle{
		CommonConn: hctx.commonConn,
		QueryID:    hctx.queryID,
	}
	resp := longpollHctx{
		canceller:            canceller,
		deadline:             deadline,
		handlerContextFields: hctx.handlerContextFields,
	}
	return lh, resp
}
