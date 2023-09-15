// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	cryptorand "crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"time"
)

// Function returns all groups that parsed successfully and all errors
func ParseTrustedSubnets(groups [][]string) (trustedSubnetGroups [][]*net.IPNet, errs []error) {
	for _, group := range groups {
		var g []*net.IPNet
		for _, sn := range group {
			_, n, err := net.ParseCIDR(sn)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			g = append(g, n)
		}
		trustedSubnetGroups = append(trustedSubnetGroups, g)
	}
	return trustedSubnetGroups, errs
}

func (pc *PacketConn) HandshakeClient(cryptoKey string, trustedSubnetGroups [][]*net.IPNet, forceEncryption bool, startTime uint32, flags uint32, handshakeStepTimeout time.Duration) error {
	body, keys, err := pc.nonceExchangeClient(nil, cryptoKey, trustedSubnetGroups, forceEncryption, handshakeStepTimeout)
	if err != nil {
		return fmt.Errorf("nonce exchange failed: %w", err)
	}

	if keys != nil {
		err := pc.encrypt(keys.readKey[:], keys.readIV[:], keys.writeKey[:], keys.writeIV[:])
		if err != nil {
			return fmt.Errorf("encryption setup failed: %w", err)
		}
	}

	hs, _, err := pc.handshakeExchangeClient(body, startTime, flags, handshakeStepTimeout)
	if err != nil {
		return fmt.Errorf("handshake exchange failed between %v (local) and %v: %w", pc.conn.LocalAddr(), pc.conn.RemoteAddr(), err)
	}

	if (hs.Flags & flagCRC32C) != 0 {
		pc.setCRC32C()
	}
	if (hs.Flags & flagCancelReq) != 0 {
		pc.flagCancelReq = true
	}

	return nil
}

func (pc *PacketConn) HandshakeServer(cryptoKeys []string, trustedSubnetGroups [][]*net.IPNet, forceEncryption bool, startTime uint32, handshakeStepTimeout time.Duration) ([]byte, uint32, error) {
	magicHead, keys, body, err := pc.nonceExchangeServer(nil, cryptoKeys, trustedSubnetGroups, forceEncryption, handshakeStepTimeout)
	if err != nil {
		return magicHead, 0, fmt.Errorf("nonce exchange failed: %w", err)
	}

	if keys != nil {
		err := pc.encrypt(keys.readKey[:], keys.readIV[:], keys.writeKey[:], keys.writeIV[:])
		if err != nil {
			return nil, 0, fmt.Errorf("encryption setup failed: %w", err)
		}
	}

	clientHandshake, _, err := pc.handshakeExchangeServer(body, startTime, handshakeStepTimeout)
	if err != nil {
		return nil, 0, fmt.Errorf("handshake exchange failed between %v (local) and %v: %w", pc.conn.LocalAddr(), pc.conn.RemoteAddr(), err)
	}

	if (clientHandshake.Flags & flagCRC32C) != 0 {
		pc.setCRC32C()
	}
	if (clientHandshake.Flags & flagCancelReq) != 0 {
		pc.flagCancelReq = true
	}
	return nil, clientHandshake.Flags, nil
}

func (pc *PacketConn) nonceExchangeClient(body []byte, cryptoKey string, trustedSubnetGroups [][]*net.IPNet, forceEncryption bool, handshakeStepTimeout time.Duration) ([]byte, *cryptoKeys, error) {
	client, err := prepareNonceClient(cryptoKey, trustedSubnetGroups, forceEncryption, pc.conn.LocalAddr(), pc.conn.RemoteAddr())
	if err != nil {
		return body, nil, err
	}

	body = client.writeTo(body[:0])
	err = pc.WritePacket(packetTypeRPCNonce, body, handshakeStepTimeout)
	if err != nil {
		return body, nil, err
	}

	respType, body, err := pc.ReadPacket(body, handshakeStepTimeout)
	if err != nil {
		return body, nil, err
	}
	var server nonceMsg
	body, err = server.readFrom(respType, body)
	if err != nil {
		return body, nil, err
	}

	if client.LegacySchema() == cryptoSchemaAES && server.LegacySchema() != cryptoSchemaAES {
		return body, nil, fmt.Errorf("refusing to setup unencrypted connection between %v (local) and %v, server schema is %s", pc.conn.LocalAddr(), pc.conn.RemoteAddr(), SchemaToString(server.Schema))
	}

	if server.LegacySchema() == cryptoSchemaAES {
		key, err := pc.deriveKeysClient(cryptoKey, client.Time, client.Nonce, server.Time, server.Nonce)
		if err == nil {
			pc.keyID = server.KeyID
		}
		return body, key, err
	}

	return body, nil, nil
}

func (pc *PacketConn) nonceExchangeServer(body []byte, cryptoKeys []string, trustedSubnetGroups [][]*net.IPNet, forceEncryption bool, handshakeStepTimeout time.Duration) (magicHead []byte, keys *cryptoKeys, _ []byte, err error) {
	reqType, magicHead, body, err := pc.readPacketWithMagic(body[:0], handshakeStepTimeout)
	if err != nil {
		return magicHead, nil, body, err
	}
	var client nonceMsg
	body, err = client.readFrom(reqType, body)
	if err != nil {
		return nil, nil, body, err
	}

	server, cryptoKey, err := prepareNonceServer(cryptoKeys, trustedSubnetGroups, forceEncryption, client, pc.conn.LocalAddr(), pc.conn.RemoteAddr())
	if err != nil {
		return nil, nil, body, err
	}

	body = server.writeTo(body[:0])
	err = pc.WritePacket(packetTypeRPCNonce, body, handshakeStepTimeout)
	if err != nil {
		return nil, nil, body, err
	}

	if server.LegacySchema() == cryptoSchemaAES {
		keys, err := pc.deriveKeysServer(cryptoKey, client.Time, client.Nonce, server.Time, server.Nonce)
		if err == nil {
			pc.keyID = server.KeyID
		}
		return nil, keys, body, err
	}

	return nil, nil, body, nil
}

func prepareNonceClient(cryptoKey string, trustedSubnetGroups [][]*net.IPNet, forceEncryption bool, localAddr net.Addr, remoteAddr net.Addr) (*nonceMsg, error) {
	m := &nonceMsg{
		KeyID:  KeyIDFromCryptoKey(cryptoKey),
		Schema: cryptoSchemaNoneOrAES,
		Time:   uint32(time.Now().Unix()),
	}

	_, err := cryptorand.Read(m.Nonce[:])
	if err != nil {
		return nil, err
	}

	if forceEncryption || !(sameMachine(localAddr, remoteAddr) || sameSubnetGroup(localAddr, remoteAddr, trustedSubnetGroups)) {
		m.Schema = cryptoSchemaAES
	}

	return m, nil
}

func prepareNonceServer(cryptoKeys []string, trustedSubnetGroups [][]*net.IPNet, forceEncryption bool, client nonceMsg, localAddr net.Addr, remoteAddr net.Addr) (*nonceMsg, string, error) {
	clientRequiresEncryption := client.LegacySchema() == cryptoSchemaAES
	clientSupportsEncryption := client.LegacySchema() != cryptoSchemaNone
	requireEncryption := forceEncryption || !(sameMachine(localAddr, remoteAddr) || sameSubnetGroup(localAddr, remoteAddr, trustedSubnetGroups))

	if !clientRequiresEncryption && !requireEncryption {
		return &nonceMsg{Schema: cryptoSchemaNone}, "", nil
	}
	if !clientRequiresEncryption && requireEncryption && !clientSupportsEncryption {
		return nil, "", fmt.Errorf("refusing to setup unencrypted connection between %v (local) and %v, client schema is %s", localAddr, remoteAddr, SchemaToString(client.Schema))
	}
	m := &nonceMsg{
		Schema: cryptoSchemaAES,
		Time:   uint32(time.Now().Unix()),
	}
	cryptoKey := "" // We disallow empty crypto keys as protection against misconfigurations, when key is empty because error reading key file is ignored
	for _, c := range cryptoKeys {
		k := KeyIDFromCryptoKey(c)
		if c != "" && c != cryptoKey && k == client.KeyID { // skip empty, allow duplicate keys, disallow different keys with the same KeyID
			if cryptoKey != "" {
				return nil, "", fmt.Errorf("client key ID %s matches more than 1 of %d server keys IDs %s between %v (local) and %v, client schema is %s", hex.EncodeToString(client.KeyID[:]), len(cryptoKeys), hex.EncodeToString(m.KeyID[:]), localAddr, remoteAddr, SchemaToString(client.Schema))
			}
			m.KeyID = k
			cryptoKey = c
		}
	}
	if cryptoKey == "" {
		return nil, "", fmt.Errorf("client key key %s does not match any of %d server key IDs, but connection requires encryption between %v (local) and %v, client schema is %s", hex.EncodeToString(client.KeyID[:]), len(cryptoKeys), localAddr, remoteAddr, SchemaToString(client.Schema))
	}

	_, err := cryptorand.Read(m.Nonce[:])
	if err != nil {
		return nil, "", err
	}

	return m, cryptoKey, nil
}

func (pc *PacketConn) deriveKeysClient(cryptoKey string, clientTime uint32, clientNonce [16]byte, serverTime uint32, serverNonce [16]byte) (*cryptoKeys, error) {
	dt := (time.Duration(clientTime) - time.Duration(serverTime)) * time.Second
	if dt < -cryptoMaxTimeDelta || dt > cryptoMaxTimeDelta {
		return nil, fmt.Errorf("client-server time delta %v is more than maximum %v", dt, cryptoMaxTimeDelta)
	}

	clientIP, clientPort := extractIPPort(pc.conn.LocalAddr())
	serverIP, serverPort := extractIPPort(pc.conn.RemoteAddr())

	return deriveCryptoKeys(true, cryptoKey, clientTime,
		clientNonce, clientIP, clientPort,
		serverNonce, serverIP, serverPort), nil
}

func (pc *PacketConn) deriveKeysServer(cryptoKey string, clientTime uint32, clientNonce [16]byte, serverTime uint32, serverNonce [16]byte) (*cryptoKeys, error) {
	dt := (time.Duration(clientTime) - time.Duration(serverTime)) * time.Second
	if dt < -cryptoMaxTimeDelta || dt > cryptoMaxTimeDelta {
		return nil, fmt.Errorf("client-server time delta %v is more than maximum %v", dt, cryptoMaxTimeDelta)
	}

	clientIP, clientPort := extractIPPort(pc.conn.RemoteAddr())
	serverIP, serverPort := extractIPPort(pc.conn.LocalAddr())

	return deriveCryptoKeys(false, cryptoKey, clientTime,
		clientNonce, clientIP, clientPort,
		serverNonce, serverIP, serverPort), nil
}

func (pc *PacketConn) handshakeExchangeClient(body []byte, startTime uint32, flags uint32, handshakeStepTimeout time.Duration) (*handshakeMsg, []byte, error) {
	client := prepareHandshakeClient(pc.conn.LocalAddr(), startTime, flags)

	body = client.writeTo(body[:0])
	err := pc.WritePacket(packetTypeRPCHandshake, body, handshakeStepTimeout)
	if err != nil {
		return nil, body, err
	}

	respType, body, err := pc.ReadPacket(body, handshakeStepTimeout)
	if err != nil {
		return nil, body, err
	}
	var server handshakeMsg
	body, err = server.readFrom(respType, body)
	if err != nil {
		return nil, body, err
	}

	return &handshakeMsg{
		Flags:     server.Flags,
		SenderPID: client.SenderPID,
		PeerPID:   server.SenderPID,
	}, body, nil
}

func (pc *PacketConn) handshakeExchangeServer(body []byte, startTime uint32, handshakeStepTimeout time.Duration) (*handshakeMsg, []byte, error) {
	reqType, body, err := pc.ReadPacket(body, handshakeStepTimeout)
	if err != nil {
		return nil, body, err
	}
	var client handshakeMsg
	body, err = client.readFrom(reqType, body)
	if err != nil {
		return nil, body, err
	}

	server := prepareHandshakeServer(client, pc.conn.LocalAddr(), startTime)

	body = server.writeTo(body[:0])
	err = pc.WritePacket(packetTypeRPCHandshake, body, handshakeStepTimeout)
	if err != nil {
		return nil, body, err
	}

	return &client, body, nil
}

func prepareHandshakeClient(localAddr net.Addr, startTime uint32, flags uint32) handshakeMsg {
	ip, _ := extractIPPort(localAddr) // ignore port as client

	return handshakeMsg{
		Flags: flags | flagCRC32C | flagCancelReq,
		SenderPID: NetPID{
			Ip:      ip,
			PortPid: asPortPid(0, processID),
			Utime:   startTime,
		},
	}
}

func prepareHandshakeServer(client handshakeMsg, localAddr net.Addr, startTime uint32) handshakeMsg {
	flags := uint32(0)
	if client.Flags&flagCRC32C != 0 {
		flags |= flagCRC32C
	}
	if client.Flags&flagCancelReq != 0 {
		flags |= flagCancelReq
	}

	return handshakeMsg{
		Flags:     flags,
		SenderPID: prepareHandshakePIDServer(localAddr, startTime),
		PeerPID:   client.SenderPID,
	}
}

func prepareHandshakePIDServer(localAddr net.Addr, startTime uint32) NetPID {
	ip, port := extractIPPort(localAddr)

	return NetPID{
		Ip:      ip,
		PortPid: asPortPid(port, processID),
		Utime:   startTime,
	}
}
