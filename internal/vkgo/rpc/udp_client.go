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
	"net/netip"
	"strconv"
	"sync"

	"github.com/VKCOM/statshouse/internal/vkgo/algo"
	"github.com/VKCOM/statshouse/internal/vkgo/etchosts"
	"github.com/VKCOM/statshouse/internal/vkgo/rpc/udp"
)

type udpClient struct {
	client *ClientImpl

	transport *udp.Transport

	mu     sync.RWMutex
	closed bool // TODO reuse closed from client ??
	conns  map[NetAddr]*udpClientConn
}

func newUdpClient(client *ClientImpl) *udpClient {
	if len(client.opts.CryptoKey) < MinCryptoKeyLen {
		log.Panicf("crypto key is too short (%d bytes), must be at least %d bytes", len(client.opts.CryptoKey), MinCryptoKeyLen)
	}

	udpAddr, err := net.ResolveUDPAddr("udp", client.opts.localUDPAddress)
	if err != nil {
		panic(err)
	}

	c := &udpClient{
		client: client,
		conns:  make(map[NetAddr]*udpClientConn),
	}

	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		panic(err)
	}
	t, err := udp.NewTransport(
		/*TODO tune max incoming memory*/ 1000000000,
		[]string{client.opts.CryptoKey},
		conn,
		uniqueStartTime(),
		func(_ *udp.Connection) {}, // acceptHandlerUDP
		c.closeHandler,
		c.allocateMessage,
		c.deallocateMessage,
		client.opts.PacketTimeout/10,
		client.opts.DebugUdp,
	)
	if err != nil {
		panic(err)
	}

	c.transport = t

	go func() {
		err = c.transport.Run()
		if err != nil {
			panic(err) // TODO return err
		}
	}()

	return c
}

func (c *udpClient) closeHandler(conn *udp.Connection) {
	var pc *udpClientConn
	c.mu.Lock()
	if conn.UserData != nil {
		pc = conn.UserData.(*udpClientConn)
		for address := range pc.addresses {
			delete(c.conns, address)
		}
	}
	c.mu.Unlock()

	if pc == nil {
		return
	}
	pc.mu.Lock()
	defer pc.mu.Unlock()
	for _, resp := range pc.calls {
		resp.err = ErrClientConnClosedSideEffect
		resp.deliverResult(c.client)
	}
	clear(pc.calls)
}

func (c *udpClient) deallocateMessage(messagePtr *[]byte) {
	if messagePtr != nil {
		c.client.responseDataPool.Put(messagePtr)
	}
}

func (c *udpClient) allocateMessage(size int) *[]byte {
	msgPtr := c.client.getResponseData()
	*msgPtr = algo.ResizeSlice(*msgPtr, size)
	return msgPtr
}

func (c *udpClient) Do(ctx context.Context, network string, address string, req *Request) (*Response, error) {
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

func (c *udpClient) setupCall(ctx context.Context, address NetAddr, req *Request, multiResult chan *Response, cb ClientCallback, userData any) (*udpClientConn, *Response, error) {
	if req.hookState != nil && req.hookState.NeedToDropRequest(ctx, address, req) {
		return nil, nil, ErrClientDropRequest
	}

	if req.Extra.IsSetNoResult() {
		// We consider it antipattern. TODO - implement)
		return nil, nil, fmt.Errorf("sending no_result requests is not supported")
	}

	deadline, err := c.client.fillRequestTimeout(ctx, req)
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

	if pc != nil {
		pc.mu.Lock()
		c.mu.RUnlock() // Do not hold while working with pc
		cctx, err := pc.setupCallLocked(req, deadline, multiResult, cb, userData)
		pc.mu.Unlock()
		return pc, cctx, err
	}
	c.mu.RUnlock()

	if address.Network != "udp4" && address.Network != "udp6" && address.Network != "udp" { // optimization: check only if not found in client.conns
		return nil, nil, fmt.Errorf("unsupported network type %q", address.Network)
	}

	c.mu.Lock()
	defer c.mu.Unlock() // for simplicity, this is not a fastpath

	if c.closed {
		return nil, nil, ErrClientClosed
	}

	pc = c.conns[address]
	if pc == nil {
		host, portStr, err := net.SplitHostPort(address.Address)
		if err != nil {
			// TODO return understandable error
			return nil, nil, err
		}
		port, err := strconv.Atoi(portStr)
		if err != nil {
			return nil, nil, err
		}

		if ip := net.ParseIP(host); ip == nil {
			if hostIP := etchosts.Resolve(host); hostIP != "" {
				host = hostIP
			} else {
				return nil, nil, fmt.Errorf("cannot resolve hostname %s", host)
			}
		}

		pc = &udpClientConn{
			client:    c,
			mu:        sync.Mutex{},
			calls:     make(map[int64]*Response),
			addresses: make(map[NetAddr]struct{}),
			// TODO add this udpClientConn to conn to delete udpClientConn when closeConnection
		}
		conn, err := c.transport.ConnectTo(
			netip.AddrPortFrom(
				netip.MustParseAddr(host),
				uint16(port),
			),
			func(message *[]byte, canSave bool) {
				pc.handle(message, canSave)
			},
			false,
			pc,
		)
		if err != nil {
			if err == udp.ErrTransportClosed {
				return nil, nil, ErrClientClosed
			}
			return nil, nil, err
		}
		if conn.UserData != nil {
			pc = conn.UserData.(*udpClientConn)
		} else {
			conn.UserData = pc
		}
		pc.conn = conn
		pc.addresses[address] = struct{}{}
		c.conns[address] = pc
	}

	pc.mu.Lock()
	cctx, err := pc.setupCallLocked(req, deadline, multiResult, cb, userData)
	pc.mu.Unlock()
	return pc, cctx, err
}

func (c *udpClient) Close() error {
	c.transport.Shutdown()
	c.mu.Lock()
	c.closed = true

	// This code creates data race with client.handlePacket() and client.deliverResult() which also finish call
	// for _, cctx := range client.calls {
	//	cctx.err = rpc.ErrClientClosed
	//	cctx.result <- cctx
	//}

	c.mu.Unlock()
	return c.transport.Close()
}
