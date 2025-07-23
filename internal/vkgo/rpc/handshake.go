// Copyright 2025 V Kontakte LLC
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
	"sync/atomic"
	"time"

	"github.com/VKCOM/statshouse/internal/vkgo/basictl"
	"github.com/VKCOM/statshouse/internal/vkgo/rpc/internal/gen/tlnet"
)

const (
	cryptoSchemaNone      = 0 // RPC_CRYPTO_NONE in C++ engine
	cryptoSchemaAES       = 1 // RPC_CRYPTO_AES in C++ engine
	cryptoSchemaNoneOrAES = 2 // RPC_CRYPTO_NONE_OR_AES in C++ engine

	cryptoMaxTimeDelta = 30 * time.Second

	DefaultProtocolVersion = 0 // TODO - bump to 2 after testing
	LatestProtocolVersion  = 2
)

var processStartTime atomic.Int64

func init() {
	processStartTime.Store(time.Now().Unix())
}

func EncryptionToString(schema int) string {
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
	return uint32(processStartTime.Add(1))
}

type nonceMsg struct {
	KeyID  [4]byte
	Schema uint32 // lower byte is encryption schema, next byte is protocol version, higher 16 bits are flags
	Time   uint32
	Nonce  [16]byte

	DHPoint [32]byte // only for version >= 2
}

func (m *nonceMsg) EncryptionSchema() int {
	return int(m.Schema & 0xFF)
}

func (m *nonceMsg) ProtocolVersion() uint32 {
	return (m.Schema >> 8) & 0xFF
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
	buf = append(buf, m.Nonce[:]...)
	if m.ProtocolVersion() >= 2 {
		buf = append(buf, m.DHPoint[:]...)
	}
	return buf
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
	body = body[len(m.Nonce):]
	if m.ProtocolVersion() >= 2 {
		if len(body) < len(m.DHPoint) {
			return body, fmt.Errorf("nonce packet too short (%d) bytes", wasSize)
		}
		copy(m.DHPoint[:], body)
		body = body[len(m.DHPoint):]
	}
	// We expect future protocol extensions to have additional fields here
	return body, nil
}

type handshakeMsg struct {
	Flags     uint32
	SenderPID NetPID // those fields are broken, because actual PID is 32-bit, not 16-bit
	PeerPID   NetPID
}

type NetPID = tlnet.Pid

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
	buf = m.SenderPID.Write(buf)
	buf = m.PeerPID.Write(buf)
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
	Key [32]byte
	IV  [16]byte
}

func deriveCryptoKeys(clientSend bool, cryptoKey string, clientTime uint32,
	clientNonce [16]byte, clientIP uint32, clientPort uint16,
	serverNonce [16]byte, serverIP uint32, serverPort uint16,
	sharedSecret []byte,
) (keys cryptoKeys) {
	w := writeCryptoInitMsg(clientSend, cryptoKey, clientTime, clientNonce, clientIP, clientPort, serverNonce, serverIP, serverPort, sharedSecret)

	w1 := md5.Sum(w[1:])
	w2 := sha1.Sum(w)
	w3 := md5.Sum(w[2:])
	copy(keys.Key[:], w1[:])
	copy(keys.Key[12:], w2[:])
	copy(keys.IV[:], w3[:])
	return
}

func writeUint16(buf []byte, v uint16) []byte {
	return append(buf, byte(v&0xFF), byte((v>>8)&0xFF))
}

func writeCryptoInitMsg(clientSend bool, key string, clientTime uint32,
	clientNonce [16]byte, clientIP uint32, clientPort uint16,
	serverNonce [16]byte, serverIP uint32, serverPort uint16,
	sharedSecret []byte, // not empty for protocol version 2, where Diffie-Hellman is used
) []byte {
	var message []byte
	message = append(message, serverNonce[:]...)
	message = append(message, clientNonce[:]...)
	message = basictl.NatWrite(message, clientTime)
	message = basictl.NatWrite(message, serverIP)
	message = writeUint16(message, clientPort)
	if clientSend {
		message = append(message, "CLIENT"...)
	} else {
		message = append(message, "SERVER"...)
	}
	message = basictl.NatWrite(message, clientIP)
	message = writeUint16(message, serverPort)
	message = append(message, key...)
	message = append(message, serverNonce[:]...)
	message = append(message, clientNonce[:]...)
	message = append(message, sharedSecret...)

	return message
}
