// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"crypto/md5"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"os"
	"time"

	"go.uber.org/atomic"

	"github.com/vkcom/statshouse/internal/vkgo/basictl"
	"github.com/vkcom/statshouse/internal/vkgo/rpc/internal/gen/tl"
)

const (
	// Previous limit of 1 second sometimes causes sporadic handshake failures
	// when TCP is feeling bad (e.g. >200ms delay between SYN-ACK and ACK),
	// so try to bump it to avoid warnings about handshake failures in logs.
	DefaultHandshakeStepTimeout = 10 * time.Second

	cryptoSchemaNone      = 0 // RPC_CRYPTO_NONE in C++ engine
	cryptoSchemaAES       = 1 // RPC_CRYPTO_AES in C++ engine
	cryptoSchemaNoneOrAES = 2 // RPC_CRYPTO_NONE_OR_AES in C++ engine

	cryptoMaxTimeDelta = 30 * time.Second
)

var (
	processID        = uint16(os.Getpid())
	processStartTime = atomic.NewInt32(int32(time.Now().Unix()))
)

func SchemaToString(schema uint32) string {
	switch schema {
	case cryptoSchemaNone:
		return "None"
	case cryptoSchemaAES:
		return "AES"
	case cryptoSchemaNoneOrAES:
		return "NoneOrAES"
	default:
		return fmt.Sprintf("Unknown (0x%x)", schema)
	}
}

// dirty hack to try to give everyone realistically-looking but unique PID (which C implementation requires)
func uniqueStartTime() uint32 {
	return uint32(processStartTime.Dec())
}

type nonceMsg struct {
	KeyID  [4]byte
	Schema uint32
	Time   uint32
	Nonce  [16]byte
}

func (m *nonceMsg) LegacySchema() int {
	return int(m.Schema & 0xFF)
}

func (m *nonceMsg) Protocol() int {
	return int((m.Schema >> 8) & 0xFF)
}

// first 4 bytes of cryptoKey are identifier. This is not a problem because arbitrary long keys are allowed.
func KeyIDFromCryptoKey(cryptoKey string) (keyID [4]byte) {
	copy(keyID[:], cryptoKey)
	return keyID
}

func (m *nonceMsg) writeTo(buf []byte) []byte {
	buf = append(buf, m.KeyID[:]...)
	buf = basictl.NatWrite(buf, m.Schema)
	buf = basictl.NatWrite(buf, m.Time)
	return append(buf, m.Nonce[:]...)
}

func (m *nonceMsg) readFrom(packetType uint32, body []byte) (_ []byte, err error) {
	wasSize := len(body)
	if packetType != packetTypeRPCNonce {
		return body, fmt.Errorf("nonce packet type 0x%x instead of 0x%x", packetType, packetTypeRPCNonce)
	}
	if len(body) < len(m.KeyID) {
		return body, fmt.Errorf("nonce packet too short (%d) bytes", wasSize)
	}
	copy(m.KeyID[:], body)
	body = body[len(m.KeyID):]
	if body, err = basictl.NatRead(body, &m.Schema); err != nil {
		return body, fmt.Errorf("nonce packet too short: %w", err)
	}
	if body, err = basictl.NatRead(body, &m.Time); err != nil {
		return body, fmt.Errorf("nonce packet too short: %w", err)
	}
	if len(body) < len(m.Nonce) {
		return body, fmt.Errorf("nonce packet too short (%d) bytes", wasSize)
	}
	copy(m.Nonce[:], body)
	// We expect future protocol extensions to have additional fields here
	return body, nil
}

type handshakeMsg struct {
	Flags     uint32
	SenderPID NetPID
	PeerPID   NetPID
}

type NetPID = tl.NetPID

func asTextStat(m NetPID) string {
	var ip [4]byte
	binary.BigEndian.PutUint32(ip[:], m.Ip)
	return fmt.Sprintf("[%d.%d.%d.%d:%d:%d:%d]", ip[0], ip[1], ip[2], ip[3], portFromPortPid(m.PortPid), pidFromPortPid(m.PortPid), m.Utime)
}

func asPortPid(port uint16, PID uint16) uint32 {
	return uint32(PID)<<16 | uint32(port)
}

func portFromPortPid(portPid uint32) uint16 {
	return uint16(portPid)
}

func pidFromPortPid(portPid uint32) uint16 {
	return uint16(portPid >> 16)
}

func (m *handshakeMsg) writeTo(buf []byte) []byte {
	buf = basictl.NatWrite(buf, m.Flags)
	buf, _ = m.SenderPID.Write(buf)
	buf, _ = m.PeerPID.Write(buf)
	return buf
}

func (m *handshakeMsg) readFrom(packetType uint32, body []byte) (_ []byte, err error) {
	if packetType != packetTypeRPCHandshake {
		return body, fmt.Errorf("handshake packet type 0x%x instead of 0x%x", packetType, packetTypeRPCHandshake)
	}

	if body, err = basictl.NatRead(body, &m.Flags); err != nil {
		return body, fmt.Errorf("failed to read handshake data: %w", err)
	}
	if body, err = m.SenderPID.Read(body); err != nil {
		return body, fmt.Errorf("failed to read handshake data: %w", err)
	}
	if body, err = m.PeerPID.Read(body); err != nil {
		return body, fmt.Errorf("failed to read handshake data: %w", err)
	}
	// We expect future protocol extensions to have additional fields here
	return body, nil
}

type cryptoKeys struct {
	readKey  [32]byte
	readIV   [16]byte
	writeKey [32]byte
	writeIV  [16]byte
}

func deriveCryptoKeys(client bool, key string, clientTime uint32,
	clientNonce [16]byte, clientIP uint32, clientPort uint16,
	serverNonce [16]byte, serverIP uint32, serverPort uint16,
) *cryptoKeys {
	w := writeCryptoInitMsg(client, key, clientTime, clientNonce, clientIP, clientPort, serverNonce, serverIP, serverPort)

	var keys cryptoKeys
	w1 := md5.Sum(w[1:])
	w2 := sha1.Sum(w)
	w3 := md5.Sum(w[2:])
	copy(keys.writeKey[:], w1[:])
	copy(keys.writeKey[12:], w2[:])
	copy(keys.writeIV[:], w3[:])

	r := writeCryptoInitMsg(!client, key, clientTime, clientNonce, clientIP, clientPort, serverNonce, serverIP, serverPort)

	r1 := md5.Sum(r[1:])
	r2 := sha1.Sum(r)
	r3 := md5.Sum(r[2:])
	copy(keys.readKey[:], r1[:])
	copy(keys.readKey[12:], r2[:])
	copy(keys.readIV[:], r3[:])

	return &keys
}

func writeUint16(buf []byte, v uint16) []byte {
	return append(buf, byte(v&0xFF), byte((v>>8)&0xFF))
}

func writeCryptoInitMsg(client bool, key string, clientTime uint32,
	clientNonce [16]byte, clientIP uint32, clientPort uint16,
	serverNonce [16]byte, serverIP uint32, serverPort uint16,
) []byte {
	var message []byte
	message = append(message, serverNonce[:]...)
	message = append(message, clientNonce[:]...)
	message = basictl.NatWrite(message, clientTime)
	message = basictl.NatWrite(message, serverIP)
	message = writeUint16(message, clientPort)
	if client {
		message = append(message, "CLIENT"...)
	} else {
		message = append(message, "SERVER"...)
	}
	message = basictl.NatWrite(message, clientIP)
	message = writeUint16(message, serverPort)
	message = append(message, key...)
	message = append(message, serverNonce[:]...)
	message = append(message, clientNonce[:]...)

	return message
}
