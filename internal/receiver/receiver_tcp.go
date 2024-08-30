// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package receiver

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"sync"
	"time"

	"github.com/vkcom/statshouse/internal/agent"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/vkgo/semaphore"
)

const TCPPrefix = "statshousev1"

const (
	serverStatusInitial  = 0 // ordering is important, we use >/< comparisons with statues
	serverStatusStarted  = 1 // after first call to Serve.
	serverStatusShutdown = 2 // after first call to Shutdown(). New Serve() calls will quit immediately. Main context is cancelled, Responses are still being sent.
	serverStatusStopped  = 3 // after first call to Wait. Everything is torn down, except if some handlers did not return

	minAcceptDelay  = 5 * time.Millisecond
	maxAcceptDelay  = 1 * time.Second
	maxConnections  = 1024
	rareLogInterval = 1 * time.Second
)

type serverConn struct {
	conn net.Conn
}

// We do not write anything to our clients, so can gre
type TCP struct {
	parser

	mu           sync.Mutex
	serverStatus int
	listeners    []net.Listener // will close after
	conns        map[*serverConn]struct{}

	closeCtx       context.Context
	cancelCloseCtx context.CancelFunc
	connSem        *semaphore.Weighted

	rareLogMu        sync.Mutex
	lastReadErrorLog time.Time
	lastOtherLog     time.Time // TODO - may be split this into different error classes
}

func NewTCPReceiver(sh2 *agent.Agent, logPacket func(format string, args ...interface{})) *TCP {
	result := &TCP{
		parser:       parser{logPacket: logPacket, sh2: sh2, network: "tcp"},
		serverStatus: serverStatusInitial,
		conns:        map[*serverConn]struct{}{},
		connSem:      semaphore.NewWeighted(maxConnections),
	}
	result.parser.createMetrics()
	result.closeCtx, result.cancelCloseCtx = context.WithCancel(context.Background())
	return result
}

func (s *TCP) rareLog(last *time.Time, format string, args ...any) {
	now := time.Now()

	s.rareLogMu.Lock()
	defer s.rareLogMu.Unlock()

	if now.Sub(*last) > rareLogInterval {
		*last = now
		log.Printf(format, args...)
	}
}

func (s *TCP) Serve(h Handler, ln net.Listener) error {
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
				s.rareLog(&s.lastOtherLog, "tcp: Accept error: %v; retrying in %v", err, acceptDelay)
				time.Sleep(acceptDelay)
				continue
			}
			s.connSem.Release(1)
			return err
		}
		go s.goHandshake(h, nc, ln.Addr())                       // will release connSem
		if err := s.connSem.Acquire(s.closeCtx, 1); err != nil { // we need to acquire again to Accept()
			return nil
		}
	}
}

func (s *TCP) Shutdown() {
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
		_ = shutdownConn(sc.conn)
	}
}

func (s *TCP) CloseWait(ctx context.Context) error {
	s.Shutdown()
	// After shutdown, if clients follow protocol, all connections will soon be closed
	// we can acquire whole semaphore only if all connections finished
	err := s.connSem.Acquire(ctx, maxConnections)
	if err == nil {
		s.connSem.Release(maxConnections) // we support multiple calls to Close/CloseWait
	}
	return err
}

func (s *TCP) Close() error {
	s.Shutdown()
	s.mu.Lock()
	if s.serverStatus >= serverStatusStopped {
		s.mu.Unlock()
		return nil
	}
	s.serverStatus = serverStatusStopped

	for sc := range s.conns {
		_ = sc.conn.Close()
	}

	s.mu.Unlock()

	s.cancelCloseCtx()

	// we can acquire whole semaphore only if all connections finished
	_ = s.connSem.Acquire(context.Background(), maxConnections)
	s.connSem.Release(maxConnections)

	if len(s.conns) != 0 {
		log.Printf("rpc: tracking of connection invariant violated after close wait - %d connections", len(s.conns))
	}
	return nil
}

func (s *TCP) goHandshake(h Handler, conn net.Conn, lnAddr net.Addr) {
	defer s.connSem.Release(1)

	defer setValueSize(s.packetSizeDisconnect, 0)
	setValueSize(s.packetSizeConnect, 0)

	sc := &serverConn{
		conn: conn,
	}

	if !s.trackConn(sc) {
		// server is shutting down
		_ = conn.Close()
		return
	}
	defer s.dropConn(sc)

	err := s.receiveLoop(h, sc)

	if err != nil {
		s.rareLog(&s.lastReadErrorLog, "tcp error: %v", err)
	}
	_ = conn.Close()
}

func (s *TCP) trackConn(sc *serverConn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.serverStatus >= serverStatusShutdown { // no new connections when shutting down
		return false
	}
	s.conns[sc] = struct{}{}
	return true
}

func (s *TCP) dropConn(sc *serverConn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.conns[sc]; !ok {
		log.Printf("tcp: connections tracking invariant violated in dropConn")
		return
	}
	delete(s.conns, sc)
}

func (s *TCP) receiveLoop(h Handler, sc *serverConn) error {
	var batch tlstatshouse.AddMetricsBatchBytes
	data := make([]byte, 4+math.MaxUint16) // max size we support
	size := 0
	for {
		n, err := sc.conn.Read(data[size:]) // fill as much as possible
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil
			}
			setValueSize(s.packetSizeNetworkError, n)
			return err
		}
		size += n
		offset := 0
		for {
			if offset+4 > size {
				break
			}
			bodyLen := binary.LittleEndian.Uint32(data[offset:])
			if bodyLen > math.MaxUint16 {
				setValueSize(s.packetSizeFramingError, 0) // bodyLen does not represent actual packet here, do not report
				return fmt.Errorf("framing error: %d bytes (max supported %d)", bodyLen, math.MaxUint16)
			}
			if 4+offset+int(bodyLen) > size { // careful with overflow!
				break
			}
			if err := s.parse(h, nil, nil, nil, data[4+offset:4+offset+int(bodyLen)], &batch); err != nil {
				return fmt.Errorf("parsing error: %w", err) // do not allow format violation in TCP, as next data will be garbage
			}
			offset += 4 + int(bodyLen)
		}
		if offset != 0 { // OK, somewhat inefficient move
			copy(data, data[offset:size])
			size -= offset
		}
	}
}

type closeWriter interface {
	CloseWrite() error
}

func shutdownConn(conn net.Conn) error {
	cw, ok := conn.(closeWriter) // UnixConn, TCPConn, and any other
	if !ok {
		return io.ErrShortWrite // TODO - better error
	}
	return cw.CloseWrite()
}
