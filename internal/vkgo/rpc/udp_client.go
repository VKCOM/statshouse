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
	"time"

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
		conn.LocalAddr(),
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
	finishedCallbacks := pc.massCancelRequests()
	for _, finishedCall := range finishedCallbacks {
		finishedCall.resp.cb(pc.client.client, finishedCall.resp, finishedCall.err)
	}
}

func (pc *udpClientConn) massCancelRequests() (finishedCallbacks []callResult) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	for _, cctx := range pc.calls {
		finishedCall := callResult{resp: cctx, err: ErrClientConnClosedSideEffect}
		if cctx.result != nil {
			// results must be delivered to channels under lock
			cctx.result <- finishedCall
		} else {
			// we do some allocations here, but after disconnect we allocate anyway
			// we cannot call callbacks under any lock, otherwise deadlock
			finishedCallbacks = append(finishedCallbacks, finishedCall)
		}
	}
	clear(pc.calls)
	return
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
	req.startTime = time.Now()
	response := c.client.getResponse(req)

	hook := response.hook
	if hook != nil {
		err := hook.BeforeSend(ctx, NetAddr{Network: network, Address: address}, req)
		if err != nil {
			return response, err
		}
	}
	err := c.do(ctx, network, address, req, response)
	if hook != nil {
		hook.AfterSend(response, err)
	}
	return response, err
}

func (c *udpClient) do(ctx context.Context, network string, address string, req *Request, response *Response) error {

	response.result = response.singleResult // delivery to singleResult channel
	pc, err := c.setupCall(ctx, NetAddr{network, address}, req, response)
	if err != nil {
		// response.singleResult is empty (not sent), reuse
		return err
	}
	select {
	case <-ctx.Done():
		_ = pc.cancelCall(response.queryID) // do not unblock, reuse normally

		// response.singleResult is dirty (could have being sent, but not received).
		// but it will not be sent after pc.cancelCall, so we drain channel for reuse
		select {
		case <-response.singleResult:
		default:
		}
		return ctx.Err()
	case r := <-response.singleResult: // got ownership of cctx
		// response.singleResult is empty (sent+received), reuse
		return r.err
	}
}

func (c *udpClient) setupCall(ctx context.Context, address NetAddr, req *Request, cctx *Response) (*udpClientConn, error) {
	deadline, err := c.client.prepareCall(ctx, req, req.startTime)
	if err != nil {
		return nil, err
	}
	cctx.deadline = deadline

	c.mu.RLock()
	// ------ to test RACE detector, replace lines below
	if c.closed {
		c.mu.RUnlock()
		return nil, ErrClientClosed
	}
	pc := c.conns[address]

	if pc != nil {
		pc.mu.Lock()
		c.mu.RUnlock() // Do not hold while working with pc
		err := pc.setupCallLocked(req, cctx)
		pc.mu.Unlock()
		return pc, err
	}
	c.mu.RUnlock()

	if address.Network != "udp4" && address.Network != "udp6" && address.Network != "udp" { // optimization: check only if not found in client.conns
		return nil, fmt.Errorf("unsupported network type %q", address.Network)
	}

	c.mu.Lock()
	defer c.mu.Unlock() // for simplicity, this is not a fastpath

	if c.closed {
		return nil, ErrClientClosed
	}

	pc = c.conns[address]
	if pc == nil {
		host, portStr, err := net.SplitHostPort(address.Address)
		if err != nil {
			// TODO return understandable error
			return nil, err
		}
		port, err := strconv.Atoi(portStr)
		if err != nil {
			return nil, err
		}

		if ip := net.ParseIP(host); ip == nil {
			if hostIP := etchosts.Resolve(host); hostIP != "" {
				host = hostIP
			} else {
				return nil, fmt.Errorf("cannot resolve hostname %s", host)
			}
		}

		pc = &udpClientConn{
			client:    c,
			mu:        sync.Mutex{},
			calls:     make(map[int64]*Response),
			addresses: make(map[NetAddr]struct{}),
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
				return nil, ErrClientClosed
			}
			return nil, err
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
	err = pc.setupCallLocked(req, cctx)
	pc.mu.Unlock()
	return pc, err
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
