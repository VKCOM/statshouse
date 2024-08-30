// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"time"

	"golang.org/x/crypto/curve25519"

	"github.com/vkcom/statshouse/internal/vkgo/semaphore"
)

// if we are performing many heavy handshakes (protocol 2 Diffie-Hellman),
// we want to limit # of parallel handshakes so scheduler allows reads and writes
// for connections which are fully ready (handshake complete).
// We acquire twice because 1. we have socket write between 2 cpu intensive tasks. 2. we are in different functions (simplicity).
var handshakeSem = semaphore.NewWeighted(1 + int64(runtime.GOMAXPROCS(0)))

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

func (pc *PacketConn) HandshakeClient(cryptoKey string, trustedSubnetGroups [][]*net.IPNet, forceEncryption bool, startTime uint32, flags uint32, packetTimeout time.Duration, protocolVersion uint32) error {
	keyID := KeyIDFromCryptoKey(cryptoKey)
	body, err := pc.nonceExchangeClient(nil, cryptoKey, trustedSubnetGroups, forceEncryption, packetTimeout, protocolVersion)
	if err != nil {
		if err == io.EOF {
			return fmt.Errorf("EOF after sending nonce between %v (local) and %v, most likely server has no crypto key with prefix 0x%s, see server logs", pc.conn.LocalAddr(), pc.conn.RemoteAddr(), hex.EncodeToString(keyID[:]))
		}
		return fmt.Errorf("nonce exchange failed: %w", err)
	}

	hs, _, err := pc.handshakeExchangeClient(body, startTime, flags, packetTimeout)
	if err != nil {
		if err == io.EOF {
			return fmt.Errorf("EOF after sending handshake between %v (local) and %v, most likely client and server crypto keys with prefix 0x%s have different tail bytes, see server logs", pc.conn.LocalAddr(), pc.conn.RemoteAddr(), hex.EncodeToString(keyID[:]))
		}
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

func (pc *PacketConn) HandshakeServer(cryptoKeys []string, trustedSubnetGroups [][]*net.IPNet, forceEncryption bool, startTime uint32, packetTimeout time.Duration) ([]byte, uint32, error) {
	magicHead, body, keyID, err := pc.nonceExchangeServer(nil, cryptoKeys, trustedSubnetGroups, forceEncryption, packetTimeout)
	if err != nil {
		return magicHead, 0, fmt.Errorf("nonce exchange failed: %w", err)
	}

	clientHandshake, _, err := pc.handshakeExchangeServer(body, startTime, packetTimeout)
	if err != nil {
		if errors.Is(err, errHeaderCorrupted) {
			return nil, 0, fmt.Errorf("handshake exchange failed between %v (local) and %v, most likely client and server crypto keys with prefix 0x%s have different tail bytes: %w", pc.conn.LocalAddr(), pc.conn.RemoteAddr(), hex.EncodeToString(keyID[:]), err)
		}
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

func (pc *PacketConn) nonceExchangeClient(body []byte, cryptoKey string, trustedSubnetGroups [][]*net.IPNet, forceEncryption bool, packetTimeout time.Duration, protocolVersion uint32) ([]byte, error) {
	var x25519Scalar [32]byte
	client, err := prepareNonceClient(cryptoKey, trustedSubnetGroups, forceEncryption, pc.conn.LocalAddr(), pc.conn.RemoteAddr(), x25519Scalar[:], protocolVersion)
	if err != nil {
		return body, err
	}

	body = client.writeTo(body[:0])
	err = pc.WritePacket(packetTypeRPCNonce, body, packetTimeout)
	if err != nil {
		return body, err
	}

	respType, body, err := pc.ReadPacket(body, packetTimeout)
	if err != nil {
		return body, err
	}
	var server nonceMsg
	body, err = server.readFrom(respType, body)
	if err != nil {
		return body, err
	}

	if server.EncryptionSchema() != cryptoSchemaNone && server.EncryptionSchema() != cryptoSchemaAES {
		return body, fmt.Errorf("server returned unsupported encryption schema between %v (local) and %v, client version %d, server version %d, server schema is %s", pc.conn.LocalAddr(), pc.conn.RemoteAddr(), client.ProtocolVersion(), server.ProtocolVersion(), EncryptionToString(server.EncryptionSchema()))
	}
	if client.EncryptionSchema() == cryptoSchemaAES && server.EncryptionSchema() != cryptoSchemaAES {
		return body, fmt.Errorf("refusing to setup unencrypted connection between %v (local) and %v, client version %d, server version %d, server schema is %s", pc.conn.LocalAddr(), pc.conn.RemoteAddr(), client.ProtocolVersion(), server.ProtocolVersion(), EncryptionToString(server.EncryptionSchema()))
	}
	if server.ProtocolVersion() > client.ProtocolVersion() {
		return body, fmt.Errorf("protocol negotiation mismatch, requested version %d got %d between %v (local) and %v", client.ProtocolVersion(), server.ProtocolVersion(), pc.conn.LocalAddr(), pc.conn.RemoteAddr())
	}
	pc.protocolVersion = server.ProtocolVersion() // all next packets will be read/written in a new transport format
	if server.EncryptionSchema() == cryptoSchemaAES {
		dt := (time.Duration(client.Time) - time.Duration(server.Time)) * time.Second
		if dt < -cryptoMaxTimeDelta || dt > cryptoMaxTimeDelta {
			return body, fmt.Errorf("client-server time delta %v is more than maximum %v", dt, cryptoMaxTimeDelta)
		}
		var sharedSecret []byte
		if pc.protocolVersion >= 2 {
			sharedSecret, err = curve25519.X25519(x25519Scalar[:], server.DHPoint[:])
			if err != nil {
				return body, fmt.Errorf("Diffie-Hellman setup failed: %w", err)
			}
		}

		clientSend, serverSend := pc.deriveCryptoKeys(cryptoKey,
			pc.conn.LocalAddr(), client.Time, client.Nonce,
			pc.conn.RemoteAddr(), server.Time, server.Nonce, sharedSecret)
		err = pc.encrypt(serverSend.Key[:], serverSend.IV[:], clientSend.Key[:], clientSend.IV[:])
		if err != nil {
			return body, fmt.Errorf("encryption setup failed: %w", err)
		}

		pc.keyID = server.KeyID // must be equal to client.KeyID
		return body, err
	}
	return body, nil
}

func (pc *PacketConn) nonceExchangeServer(body []byte, cryptoKeys []string, trustedSubnetGroups [][]*net.IPNet, forceEncryption bool, packetTimeout time.Duration) (magicHead []byte, _ []byte, keyID [4]byte, err error) {
	reqType, magicHead, body, _, _, err := pc.readPacketWithMagic(body[:0], packetTimeout)
	if err != nil {
		return magicHead, body, [4]byte{}, err
	}
	var client nonceMsg
	body, err = client.readFrom(reqType, body)
	if err != nil {
		return nil, body, [4]byte{}, err
	}

	var x25519Scalar [32]byte
	server, cryptoKey, err := prepareNonceServer(cryptoKeys, trustedSubnetGroups, forceEncryption, client, pc.conn.LocalAddr(), pc.conn.RemoteAddr(), x25519Scalar[:])
	if err != nil {
		return nil, body, server.KeyID, err
	}

	body = server.writeTo(body[:0])
	err = pc.WritePacket(packetTypeRPCNonce, body, packetTimeout)
	if err != nil {
		return nil, body, server.KeyID, err
	}

	pc.protocolVersion = server.ProtocolVersion() // all next packets will be read/written in a new transport format

	if server.EncryptionSchema() == cryptoSchemaAES {
		defer handshakeSem.Release(1)
		_ = handshakeSem.Acquire(context.Background(), 1) // no error for background context

		var sharedSecret []byte
		if pc.protocolVersion >= 2 {
			sharedSecret, err = curve25519.X25519(x25519Scalar[:], client.DHPoint[:])
			if err != nil {
				return nil, body, server.KeyID, fmt.Errorf("Diffie-Hellman setup failed: %w", err)
			}
		}
		clientSend, serverSend := pc.deriveCryptoKeys(cryptoKey,
			pc.conn.RemoteAddr(), client.Time, client.Nonce,
			pc.conn.LocalAddr(), server.Time, server.Nonce, sharedSecret)
		err = pc.encrypt(clientSend.Key[:], clientSend.IV[:], serverSend.Key[:], serverSend.IV[:])

		if err != nil {
			return nil, body, server.KeyID, fmt.Errorf("encryption setup failed: %w", err)
		}
		pc.keyID = server.KeyID
		return nil, body, server.KeyID, nil
	}

	return nil, body, server.KeyID, nil
}

func prepareNonceClient(cryptoKey string, trustedSubnetGroups [][]*net.IPNet, forceEncryption bool, localAddr net.Addr, remoteAddr net.Addr, x25519Scalar []byte, protocolVersion uint32) (nonceMsg, error) {
	if protocolVersion > LatestProtocolVersion {
		protocolVersion = LatestProtocolVersion
	}

	client := nonceMsg{
		KeyID:  KeyIDFromCryptoKey(cryptoKey),
		Schema: (protocolVersion << 8),
		Time:   uint32(time.Now().Unix()),
	}

	_, err := cryptorand.Read(client.Nonce[:])
	if err != nil {
		return nonceMsg{}, err
	}
	if protocolVersion >= 2 {
		_, err := cryptorand.Read(x25519Scalar)
		if err != nil {
			return nonceMsg{}, err
		}
		pk, err := curve25519.X25519(x25519Scalar, curve25519.Basepoint)
		if err != nil {
			return nonceMsg{}, err
		}
		copy(client.DHPoint[:], pk)
	}

	if forceEncryption || !(sameMachine(localAddr, remoteAddr) || sameSubnetGroup(localAddr, remoteAddr, trustedSubnetGroups)) {
		if cryptoKey == "" {
			return nonceMsg{}, fmt.Errorf("encryption is required, but client has empty encryption key")
		}
		client.Schema |= cryptoSchemaAES
	} else if cryptoKey != "" {
		client.Schema |= cryptoSchemaNoneOrAES
	} else {
		client.Schema |= cryptoSchemaNone
	}

	return client, nil
}

func prepareNonceServer(cryptoKeys []string, trustedSubnetGroups [][]*net.IPNet, forceEncryption bool, client nonceMsg, localAddr net.Addr, remoteAddr net.Addr, x25519Scalar []byte) (nonceMsg, string, error) {
	clientRequiresEncryption := client.EncryptionSchema() == cryptoSchemaAES
	clientSupportsEncryption := client.EncryptionSchema() != cryptoSchemaNone
	requireEncryption := forceEncryption || !(sameMachine(localAddr, remoteAddr) || sameSubnetGroup(localAddr, remoteAddr, trustedSubnetGroups))

	protocol := client.ProtocolVersion()
	if protocol > LatestProtocolVersion {
		protocol = LatestProtocolVersion // we do not know newer protocols yet
	}

	if !clientRequiresEncryption && !requireEncryption {
		return nonceMsg{Schema: (protocol << 8) | cryptoSchemaNone}, "", nil
	}
	if !clientRequiresEncryption && requireEncryption && !clientSupportsEncryption {
		return nonceMsg{}, "", &tagError{
			tag: "encryption_schema_mismatch",
			err: fmt.Errorf("refusing to setup unencrypted connection between %v (local) and %v, client protocol %d schema %s", localAddr, remoteAddr, client.ProtocolVersion(), EncryptionToString(client.EncryptionSchema())),
		}
	}
	server := nonceMsg{
		KeyID:  client.KeyID, // just report back. Client should ignore this field.
		Schema: (protocol << 8) | cryptoSchemaAES,
		Time:   uint32(time.Now().Unix()),
	}
	dt := (time.Duration(client.Time) - time.Duration(server.Time)) * time.Second
	if dt < -cryptoMaxTimeDelta || dt > cryptoMaxTimeDelta { // check as early as possible
		return nonceMsg{}, "", &tagError{
			tag: "out_of_range_time_delta",
			err: fmt.Errorf("client-server time delta %v is more than maximum %v", dt, cryptoMaxTimeDelta),
		}
	}

	cryptoKey := "" // We disallow empty crypto keys as protection against misconfigurations, when key is empty because error reading key file is ignored
	var emptyKeyID [4]byte
	for _, key := range cryptoKeys {
		// TODO - disallow short keys
		keyID := KeyIDFromCryptoKey(key)
		if key != "" && key != cryptoKey && keyID == client.KeyID { // skip empty, allow duplicate keys, disallow different keys with the same KeyID
			if keyID == emptyKeyID {
				return nonceMsg{}, "", &tagError{
					tag: "zero_key_id",
					err: fmt.Errorf("client key with prefix 0x%s must not be 4 zero bytes between %v (local) and %v, client protocol %d schema %s", hex.EncodeToString(client.KeyID[:]), localAddr, remoteAddr, client.ProtocolVersion(), EncryptionToString(client.EncryptionSchema())),
				}
			}
			if cryptoKey != "" {
				return nonceMsg{}, "", &tagError{
					tag: "key_id_collision",
					err: fmt.Errorf("client key with prefix 0x%s matches more than 1 of %d server keys IDs %s between %v (local) and %v, client protocol %d schema %s", hex.EncodeToString(client.KeyID[:]), len(cryptoKeys), hex.EncodeToString(server.KeyID[:]), localAddr, remoteAddr, client.ProtocolVersion(), EncryptionToString(client.EncryptionSchema())),
				}
			}
			cryptoKey = key
		}
	}
	if cryptoKey == "" {
		return nonceMsg{}, "", fmt.Errorf("client key with prefix 0x%s does not match any of %d server keys, but connection requires encryption between %v (local) and %v, client protocol %d schema %s", hex.EncodeToString(client.KeyID[:]), len(cryptoKeys), localAddr, remoteAddr, client.ProtocolVersion(), EncryptionToString(client.EncryptionSchema()))
	}

	_, err := cryptorand.Read(server.Nonce[:])
	if err != nil {
		return nonceMsg{}, "", err
	}
	if protocol >= 2 {
		defer handshakeSem.Release(1)
		_ = handshakeSem.Acquire(context.Background(), 1) // no error for background context
		_, err := cryptorand.Read(x25519Scalar)
		if err != nil {
			return nonceMsg{}, "", err
		}
		pk, err := curve25519.X25519(x25519Scalar, curve25519.Basepoint)
		if err != nil {
			return nonceMsg{}, "", err
		}
		copy(server.DHPoint[:], pk)
		return server, cryptoKey, nil
	}
	return server, cryptoKey, nil
}

func (pc *PacketConn) deriveCryptoKeys(cryptoKey string, clientAddr net.Addr, clientTime uint32, clientNonce [16]byte, serverAddr net.Addr, serverTime uint32, serverNonce [16]byte, sharedSecret []byte) (cryptoKeys, cryptoKeys) {
	var clientIP, serverIP uint32
	var clientPort, serverPort uint16
	if pc.protocolVersion >= 1 {
		serverIP = serverTime // 60x better attack resistance)
	} else {
		clientIP, clientPort = extractIPPort(clientAddr)
		serverIP, serverPort = extractIPPort(serverAddr)
	}
	clientSend := deriveCryptoKeys(true, cryptoKey, clientTime, clientNonce, clientIP, clientPort, serverNonce, serverIP, serverPort, sharedSecret)
	serverSend := deriveCryptoKeys(false, cryptoKey, clientTime, clientNonce, clientIP, clientPort, serverNonce, serverIP, serverPort, sharedSecret)

	return clientSend, serverSend
}

func (pc *PacketConn) handshakeExchangeClient(body []byte, startTime uint32, flags uint32, packetTimeout time.Duration) (*handshakeMsg, []byte, error) {
	client := prepareHandshakeClient(pc.conn.LocalAddr(), startTime, flags)

	body = client.writeTo(body[:0])
	err := pc.WritePacket(packetTypeRPCHandshake, body, packetTimeout)
	if err != nil {
		return nil, body, err
	}

	respType, body, err := pc.ReadPacket(body, packetTimeout)
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

func (pc *PacketConn) handshakeExchangeServer(body []byte, startTime uint32, packetTimeout time.Duration) (*handshakeMsg, []byte, error) {
	reqType, body, err := pc.ReadPacket(body, packetTimeout)
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
	err = pc.WritePacket(packetTypeRPCHandshake, body, packetTimeout)
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
