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
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/vkcom/statshouse/internal/vkgo/rpc/internal/gen/constants"
	"github.com/vkcom/statshouse/internal/vkgo/rpc/internal/gen/tl"
	"github.com/vkcom/statshouse/internal/vkgo/rpc/udp"
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
	rpsCalcSeconds  = 5
	rareLogInterval = 1 * time.Second

	maxGoAllocSizeClass = 32768 // from here: https://go.dev/src/runtime/mksizeclasses.go
	tracebackBufSize    = 65536
)

var (
	errServerClosed     = errors.New("rpc: server closed")
	ErrNoHandler        = &Error{Code: TlErrorNoHandler, Description: "rpc: no handler"} // Never wrap this error
	errHijackResponse   = errors.New("rpc: user of server is now responsible for sending the response")
	errCancelHijack     = &Error{Code: TlErrorTimeout, Description: "rpc: longpoll cancelled"} // passed to response hook. Decided to add separate code later.
	errTooLarge         = &Error{Code: TLErrorResultToLarge, Description: fmt.Sprintf("rpc: packet size (metadata+extra+response) exceeds %v bytes", maxPacketLen)}
	errGracefulShutdown = &Error{Code: TlErrorGracefulShutdown, Description: "rpc: engine is shutting down"}

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
	s := &Server{}
	s.newServer(options...)
	return s
}

// TODO - temporary wrapper for ServerUDP, move into NewServer later
func (s *Server) newServer(options ...ServerOptionsFunc) {
	s.opts = ServerOptions{
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
		option(&s.opts)
	}

	host, _ := os.Hostname()

	s.serverStatus = serverStatusInitial
	s.conns = map[*serverConnTCP]struct{}{}
	s.reqMemSem = semaphore.NewWeighted(int64(s.opts.RequestMemoryLimit))
	s.respMemSem = semaphore.NewWeighted(int64(s.opts.ResponseMemoryLimit))
	s.connSem = semaphore.NewWeighted(int64(s.opts.MaxConns))
	s.startTime = uniqueStartTime()
	s.statHostname = host
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

	s.workersGroup.Add(1)
	go s.rpsCalcLoop(&s.workersGroup)
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

func protocolName(ID int) string {
	if ID == 0 {
		return "TCP"
	}
	return "UDP"
}

type Server struct {
	// Place atomics first to ensure proper alignment, see https://pkg.go.dev/sync/atomic#pkg-note-BUG
	protocolStats  [2]protocolStats
	engineShutdown atomic.Bool

	statHostname string

	opts ServerOptions

	mu           sync.Mutex
	serverStatus int
	listeners    []net.Listener // will close after
	conns        map[*serverConnTCP]struct{}
	nextConnID   int64

	transportsUDP []*udp.Transport // many transportsUDP, one per listen address

	workerPool   *workerPool
	workersGroup sync.WaitGroup

	closeCtx       context.Context
	cancelCloseCtx context.CancelFunc
	connSem        *semaphore.Weighted
	reqMemSem      *semaphore.Weighted
	respMemSem     *semaphore.Weighted
	reqBufPool     sync.Pool
	respBufPool    sync.Pool
	hctxPool       sync.Pool

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

	for _, ln := range s.listeners {
		_ = ln.Close() // We do not care
	}
	s.listeners = s.listeners[:0]

	for sc := range s.conns {
		sc.sendLetsFin()
	}

	for _, transport := range s.transportsUDP {
		transport.Shutdown()
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

	// TODO - wait UDP connections also
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

	for _, transport := range s.transportsUDP {
		_ = transport.Close()
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
	if cur := s.protocolStats[protocolTCP].requestsCurrent.Load(); cur != 0 {
		s.opts.Logf("rpc: tracking of current TCP requests invariant violated after close wait - %d requests", cur)
	}
	if cur := s.protocolStats[protocolUDP].requestsCurrent.Load(); cur != 0 {
		s.opts.Logf("rpc: tracking of current UDP requests invariant violated after close wait - %d requests", cur)
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
	taken := s.responseBufTake(s.opts.ResponseMemEstimate)
	ok := s.respMemSem.TryAcquire(int64(taken))
	if !ok {
		cur, size := s.respMemSem.Observe()
		s.rareLog(&s.lastRespMemWaitLog,
			"rpc: waiting to acquire response memory (want %s, mem %s, limit %s, %d conns, %d reqs); consider increasing Server.ResponseMemoryLimit or lowering Server.ResponseMemEstimate",
			humanByteCountIEC(int64(taken)),
			humanByteCountIEC(cur),
			humanByteCountIEC(size),
			s.ConnectionsCurrent(),
			s.RequestsCurrent(),
		)
		err := s.respMemSem.Acquire(ctx, int64(taken))
		if err != nil {
			return nil, 0, err
		}
	}

	// we do not know if handler will write large or small response, so we always give small response from pool
	// if handler will write large response, we will release in releaseResponseBuf, as with requests
	return s.respBufPool.Get().(*[]byte), taken, nil
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

	sc := &serverConnTCP{
		serverConnCommon: serverConnCommon{
			server:            s,
			closeCtx:          closeCtx,
			cancelCloseCtx:    cancelCloseCtx,
			longpollResponses: map[int64]hijackedResponse{},
		},
		listenAddr:  lnAddr,
		maxInflight: s.opts.MaxInflightPackets,
		conn:        conn,
		writeQ:      make([]*HandlerContext, 0, s.opts.maxInflightPacketsPreAlloc()),
		errHandler:  s.opts.ConnErrHandler,
	}
	sc.releaseFun = sc.releaseHandlerCtx
	sc.pushUnlockFun = sc.pushUnlock
	sc.inflightCond.L = &sc.mu
	sc.writeQCond.L = &sc.mu
	sc.closeWaitCond.L = &sc.mu

	if !s.trackConn(sc) {
		// server is shutting down
		_ = conn.Close()
		return
	}
	if s.opts.DebugRPC {
		s.opts.Logf("rpc: %s Handshake", sc.debugName)
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
		s.opts.Logf("rpc: %s Disconnect with readErr=%v writeErr=%v", sc.debugName, readErr, writeErr)
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

	go w.run(&s.workersGroup)

	return w
}

func (s *Server) receiveLoop(sc *serverConnTCP, readErrCC chan<- error) {
	hctxToRelease, err := s.receiveLoopImpl(sc)
	if hctxToRelease != nil {
		sc.releaseHandlerCtx(hctxToRelease)
	}
	if err != nil {
		sc.close(err)
	}
	sc.cancelAllLongpollResponses(false) // we always cancel from receiving goroutine.
	readErrCC <- err
}

func (s *Server) receiveLoopImpl(sc *serverConnTCP) (*HandlerContext, error) {
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
		// TODO - read packet body for tl.RpcCancelReq without waiting on semaphores
		requestTime := time.Now()

		hctx, ok := sc.acquireHandlerCtx()
		if !ok {
			return nil, errServerClosed
		}

		hctx.RequestTime = requestTime

		reqTaken := s.requestBufTake(int(header.length))
		if err := s.acquireRequestSema(sc.closeCtx, reqTaken); err != nil {
			return hctx, err
		}
		hctx.reqTaken = reqTaken
		hctx.request, hctx.Request = s.acquireRequestBuf(reqTaken)
		hctx.Request, err = sc.conn.readPacketBodyUnlocked(&header, hctx.Request)
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
		hctx.Response = (*hctx.response)[:0]
		hctx.respTaken = respTaken

		s.protocolStats[protocolTCP].requestsTotal.Add(1)

		if header.tip == (tl.RpcClientWantsFin{}.TLTag()) {
			readFIN = true
			sc.SetReadFIN()
			// We handle it normally as noResult request below, to simplify hctx accounting
		}
		w := s.handleRequest(header.tip, &sc.serverConnCommon, hctx)
		if w != nil {
			w.ch <- workerWork{sc: &sc.serverConnCommon, hctx: hctx}
		}
	}
}

func (s *Server) sendLoop(sc *serverConnTCP) error {
	toRelease, err := s.sendLoopImpl(sc) // err is logged inside, if needed
	if len(toRelease) != 0 {
		s.rareLog(&s.lastPushToClosedLog, "failed to push %d responses because connection was closed to %v", len(toRelease), sc.conn.remoteAddr)
	}
	for _, hctx := range toRelease {
		sc.releaseHandlerCtx(hctx)
	}
	return err
}

func (s *Server) sendLoopImpl(sc *serverConnTCP) ([]*HandlerContext, error) { // returns contexts to release
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
				fmt.Printf("%v server %p conn %p writes serverWantsFIN\n", time.Now(), s, sc)
			}
			sentNow = true
			if s.opts.DebugRPC {
				s.opts.Logf("rpc: %s Write serverWantsFIN packet\n", sc.debugName)
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
				s.opts.Logf("rpc: %s Response packet queryID=%d extra=%s body=%x\n", sc.debugName, hctx.queryID, hctx.ResponseExtra.String(), hctx.Response[:hctx.extraStart])
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
				s.opts.Logf("rpc: %s Flush\n", sc.debugName)
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

func (s *Server) handleRequest(reqHeaderTip uint32, sc *serverConnCommon, hctx *HandlerContext) *worker {
	err := s.doSyncHandler(reqHeaderTip, sc, hctx)
	if err == errHijackResponse {
		// We must not touch hctx.UserData here because it can be released already in SendHijackResponse
		// User is now responsible for calling hctx.SendHijackedResponse
		return nil
	}
	if err != ErrNoHandler {
		sc.pushResponse(hctx, err, false)
		return nil
	}
	// in many projects maxWorkers is cmd argument, and ppl want to disable worker pool with this argument
	if s.opts.MaxWorkers > 0 {
		w := s.acquireWorker()
		if w != nil {
			return w
		}
		// otherwise pool is closed and we call synchronously, TODO - check this is impossible
	}
	err = sc.server.callHandler(sc.closeCtx, hctx)
	sc.pushResponse(hctx, err, false)
	return nil
}

func (s *Server) doSyncHandler(reqHeaderTip uint32, sc *serverConnCommon, hctx *HandlerContext) error {
	switch reqHeaderTip {
	case tl.RpcCancelReq{}.TLTag():
		cancelReq := tl.RpcCancelReq{}
		if _, err := cancelReq.Read(hctx.Request); err != nil {
			return err
		}
		if s.opts.DebugRPC {
			s.opts.Logf("rpc: %s Cancel queryID=%d\n", sc.debugName, cancelReq.QueryId)
		}
		hctx.noResult = true
		sc.cancelLongpollResponse(cancelReq.QueryId)
		return nil
	case tl.RpcClientWantsFin{}.TLTag():
		// also processed in reader code
		hctx.noResult = true
		return nil
	case tl.RpcInvokeReqHeader{}.TLTag():
		err := hctx.ParseInvokeReq(&s.opts)
		if err != nil {
			return err
		}
		if s.opts.DebugRPC {
			s.opts.Logf("rpc: %s SyncHandler packet tag=#%08x queryID=%d extra=%s body=%x\n", sc.debugName, reqHeaderTip, hctx.queryID, hctx.RequestExtra.String(), hctx.Request)
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
	s.rareLog(&s.lastPacketTypeLog, "rpc server: unknown packet type 0x%x", reqHeaderTip)
	return nil
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
			panic("you can only hijack responses from SyncHandler")
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
	s.conns[sc] = struct{}{}
	s.nextConnID++
	if s.opts.DebugRPC {
		sc.debugName = fmt.Sprintf("cid=%d %s->%s", s.nextConnID, sc.conn.remoteAddr, sc.conn.localAddr)
	}
	return true
}

func (s *Server) dropConn(sc *serverConnTCP) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.conns[sc]; !ok {
		s.opts.Logf("rpc: connections tracking invariant violated in dropConn")
		return
	}
	delete(s.conns, sc)
}

func (s *Server) acquireHandlerCtx(protocolID int) *HandlerContext {
	hctx := s.hctxPool.Get().(*HandlerContext)
	hctx.protocolID = protocolID
	s.protocolStats[protocolID].requestsCurrent.Add(1)
	return hctx
}

func (s *Server) releaseHandlerCtx(hctx *HandlerContext) {
	s.protocolStats[hctx.protocolID].requestsCurrent.Add(-1)

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
	tick := time.NewTicker(rpsCalcSeconds * time.Second)
	defer tick.Stop()
	var prev [2]int64
	for {
		select {
		case now := <-tick.C:
			for i := range s.protocolStats {
				cur := s.protocolStats[i].requestsTotal.Load()
				s.protocolStats[i].rps.Store((cur - prev[i]) / rpsCalcSeconds)
				prev[i] = cur
			}
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

func (s *Server) ConnectionsCurrent() int64 {
	return s.protocolStats[protocolTCP].connectionsCurrent.Load() + s.protocolStats[protocolUDP].connectionsCurrent.Load()
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
