// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/VKCOM/statshouse/internal/vkgo/basictl"
	"github.com/VKCOM/statshouse/internal/vkgo/rpc/udp"
)

func ListenUDP(network, address string) (*net.UDPConn, error) {
	if network != "udp" && network != "udp4" && network != "udp6" {
		return nil, fmt.Errorf("unsupported network type %q", network)
	}
	udpAddr, err := net.ResolveUDPAddr(network, address)
	if err != nil {
		return nil, err
	}
	return net.ListenUDP(network, udpAddr)
}

// TODO - support UDP listening on 0.0.0.0 in UDP Transport instead
func ListenUDPWildcard(host string, port uint16) ([]*net.UDPConn, error) {
	var udpConns []*net.UDPConn
	listenSingle := func(h string, p uint16) error {
		u, err := ListenUDP("udp4", fmt.Sprintf("%s:%d", h, p))
		if err != nil {
			return err
		}
		udpConns = append(udpConns, u)
		return nil
	}
	// TODO - support UDP listening on all adapters in UDP Transport
	if host != "" && host != "0.0.0.0" {
		if err := listenSingle(host, port); err != nil {
			return nil, err
		}
		return udpConns, nil
	}
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, fmt.Errorf("could not read interface addresses: %w", err)
	}
	for _, addr := range addrs {
		if v, ok := addr.(*net.IPNet); ok {
			if v.IP.To4() != nil {
				if err := listenSingle(v.IP.String(), port); err != nil {
					return nil, err
				}
			}
		}
	}
	return udpConns, nil
}

func (s *Server) ServeUDP(conn *net.UDPConn) error {
	_, _ = statCPUInfo.GetSelfCpuUsage()
	t, err := s.serveUDP(conn)
	if err != nil {
		_ = conn.Close() // usually server closes all listeners passed to Serve(), let's make no exceptions
		return err
	}
	if t == nil {
		// probably some goroutine was delayed to call Serve, and user already called Shutdown(). No problem, simply quit
		return nil
	}
	return t.Run()
}

func (s *Server) serveUDP(conn *net.UDPConn) (*udp.Transport, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.serverStatus > serverStatusStarted {
		return nil, nil
	}
	for _, cryptoKey := range s.opts.cryptoKeys {
		if len(cryptoKey) < MinCryptoKeyLen {
			return nil, fmt.Errorf("crypto key is too short (%d bytes), must be at least %d bytes", len(cryptoKey), MinCryptoKeyLen)
		}
	}

	transport, err := udp.NewTransport(
		int64(s.opts.RequestMemoryLimit),
		s.opts.cryptoKeys,
		conn,
		s.startTime,
		s.acceptHandlerUDP(conn.LocalAddr()),
		s.closeHandlerUDP,
		s.allocateRequestBufUDP,
		s.deAllocateRequestBufUDP, // we copy response to slice we get using allocateRequestBufUDP
		s.opts.ResponseTimeoutAdjust/10,
		s.opts.DebugUdpRPC,
	)
	if err != nil {
		return nil, err
	}
	s.serverStatus = serverStatusStarted
	s.transportsUDP = append(s.transportsUDP, transport)
	return transport, nil
}

func (s *Server) allocateRequestBufUDP(take int) *[]byte {
	// acquireRequestBuf will allocate more in test-server
	v := s.reqBufPool.Get().(*[]byte)
	if cap(*v) < take {
		*v = make([]byte, take)
	} else {
		*v = (*v)[:take]
	}
	return v
}

func (s *Server) deAllocateRequestBufUDP(buf *[]byte) {
	s.releaseRequestBuf(0, buf)
}

func (s *Server) acceptHandlerUDP(listenAddr net.Addr) func(conn *udp.Connection) {
	return func(conn *udp.Connection) {
		if s.opts.DebugUdpRPC >= 1 {
			log.Printf("new udp connection accept listenAddr:%s connAddr:%s", listenAddr.String(), conn.LocalAddr())
		}
		closeCtx, cancelCloseCtx := context.WithCancelCause(context.Background())
		sc := &UdpServerConn{
			serverConnCommon: serverConnCommon{
				server:            s,
				closeCtx:          closeCtx,
				cancelCloseCtx:    cancelCloseCtx,
				longpollResponses: map[int64]hijackedResponse{},
			},
			conn: conn,
		}
		sc.releaseFun = s.releaseHandlerCtx
		sc.pushUnlockFun = sc.pushUnlock

		conn.StreamLikeIncoming = false
		conn.MessageHandle = func(message *[]byte, canSave bool) {
			if s.opts.DebugUdpRPC >= 2 {
				log.Printf("udp message handle")
			}
			requestTime := time.Now()
			hctx := s.acquireHandlerCtx(protocolUDP)
			hctx.commonConn = sc
			hctx.listenAddr = listenAddr
			hctx.localAddr = conn.LocalAddr()
			hctx.remoteAddr = conn.RemoteAddr()
			hctx.keyID = sc.conn.KeyID()
			hctx.protocolVersion = 0 // TODO protocol version
			hctx.RequestTime = requestTime

			hctx.Request = *message
			if canSave {
				hctx.request = message
			}
			hctx.response = s.respBufPool.Get().(*[]byte)
			hctx.Response = (*hctx.response)[:0]

			s.protocolStats[protocolUDP].requestsTotal.Add(1)

			var reqHeaderTip uint32
			var err error
			reqHeaderTip, hctx.Request, err = basictl.NatReadTag(hctx.Request)
			if err != nil {
				s.rareLog(&s.lastReadErrorLog, "rpc: error reading request tag (%d bytes request) from %v, disconnecting: %v", len(hctx.Request), sc.conn.RemoteAddr(), err)
				return
			}

			w, ctx := s.handleRequest(reqHeaderTip, &sc.serverConnCommon, hctx)
			if w != nil {
				if !canSave {
					// if we call Handler in worker and don't own the message, then we must copy it
					parsedReq := hctx.Request
					hctx.request, hctx.Request = s.acquireRequestBuf(len(hctx.Request))
					copy(hctx.Request, parsedReq)
				}
				w.ch <- workerWork{sc: &sc.serverConnCommon, hctx: hctx, ctx: ctx}
			}
		}
		conn.UserData = sc

		s.protocolStats[protocolUDP].connectionsTotal.Add(1)
		s.protocolStats[protocolUDP].connectionsCurrent.Add(1)
	}
}

func (s *Server) closeHandlerUDP(conn *udp.Connection) {
	if s.opts.DebugUdpRPC >= 1 {
		log.Printf("udp connection close")
	}

	s.protocolStats[protocolUDP].connectionsCurrent.Add(-1)
	if conn.UserData != nil {
		if sc, ok := conn.UserData.(*UdpServerConn); ok {
			sc.cancelAllLongpollResponses(false)
		}
	}
	// TODO - will all sending hctx be released?
}
