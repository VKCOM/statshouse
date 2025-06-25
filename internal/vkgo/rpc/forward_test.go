// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"pgregory.net/rapid"
)

type forwardPacketMachine struct {
	encryptClient  bool
	encryptServer  bool
	protocolClient uint32
	protocolServer uint32

	client      *PacketConn
	server      *PacketConn
	proxyClient *PacketConn
	proxyServer *PacketConn
}

func newForwardPacketMachine(t *rapid.T) (_ forwardPacketMachine, _ func(), err error) {
	startTime := uint32(time.Now().Unix())
	cryptoKey := "crypto_key" + strings.Repeat("_", 32) // crypto_key must be longer then 32 bytes
	handshakeServer := func(conn net.Conn, forceEncryption bool) (*PacketConn, error) {
		res := NewPacketConn(conn, DefaultServerRequestBufSize, DefaultServerResponseBufSize)
		_, _, err := res.HandshakeServer([]string{cryptoKey}, nil, forceEncryption, startTime, 0)
		return res, err
	}
	handshakeClient := func(conn net.Conn, version uint32, forceEncryption bool) (*PacketConn, error) {
		res := NewPacketConn(conn, DefaultServerRequestBufSize, DefaultServerResponseBufSize)
		err := res.HandshakeClient(cryptoKey, nil, forceEncryption, startTime, 0, 0, version)
		return res, err
	}
	protocolVersion := func(label string) uint32 {
		if rapid.Bool().Draw(t, label) {
			return DefaultProtocolVersion
		} else {
			return LatestProtocolVersion
		}
	}
	client, proxyServer := net.Pipe()
	proxyClient, server := net.Pipe()
	cancel := func() {
		client.Close()
		proxyServer.Close()
		proxyClient.Close()
		server.Close()
	}
	defer func() {
		if err != nil {
			cancel()
		}
	}()
	res := forwardPacketMachine{
		encryptClient:  rapid.Bool().Draw(t, "encrypt_client"),
		encryptServer:  rapid.Bool().Draw(t, "encrypt_server"),
		protocolClient: protocolVersion("protocol_client"),
		protocolServer: protocolVersion("protocol_proxy"),
	}
	var group errgroup.Group
	group.Go(func() (err error) {
		res.server, err = handshakeServer(server, res.encryptServer)
		return err
	})
	group.Go(func() (err error) {
		if res.proxyServer, err = handshakeServer(proxyServer, res.encryptClient); err == nil {
			res.proxyClient, err = handshakeClient(proxyClient, res.protocolServer, res.encryptServer)
		}
		return err
	})
	if res.client, err = handshakeClient(client, res.protocolClient, res.encryptClient); err == nil {
		err = group.Wait()
	}
	return res, cancel, err
}

func (m *forwardPacketMachine) run(t *rapid.T) {
	type message struct {
		tip  uint32
		body []byte
	}
	minBodyLen := 1
	legacyProtocol := m.protocolClient == 0
	if legacyProtocol {
		minBodyLen = 4
	}
	bodyBuf := make([]byte, 256)
	for i := 0; i < 512; i++ {
		var forward errgroup.Group
		forward.Go(func() error {
			res := ForwardPacket(m.proxyClient, m.proxyServer, bodyBuf, forwardPacketOptions{testEnv: false})
			return res.Error()
		})
		sent := message{
			tip:  0x1234567,
			body: rapid.SliceOfN(rapid.Byte(), minBodyLen, 1024).Draw(t, "body"),
		}
		if legacyProtocol {
			sent.body = sent.body[:len(sent.body)-len(sent.body)%4]
		}
		err := m.client.WritePacket(sent.tip, sent.body, DefaultPacketTimeout)
		require.NoError(t, err)
		err = m.client.Flush()
		require.NoError(t, err)
		var receive errgroup.Group
		var received message
		receive.Go(func() (err error) {
			received.tip, received.body, err = m.server.ReadPacket(nil, DefaultPacketTimeout)
			return err
		})
		require.NoError(t, forward.Wait())
		require.NoError(t, receive.Wait())
		if m.protocolServer == 0 {
			writeAlignTo4 := int(-uint(len(sent.body)) & 3)
			sent.body = append(sent.body, forwardPacketTrailer[writeAlignTo4]...)
		}
		require.Equal(t, sent, received)
	}
}

func TestForwardPacket(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		machine, shutdown, err := newForwardPacketMachine(t)
		require.NoError(t, err)
		machine.run(t)
		shutdown()
	})
}
