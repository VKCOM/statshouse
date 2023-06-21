// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"bytes"
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

func SchemaToString(schema int32) string {
	switch schema {
	case cryptoSchemaNone:
		return "None"
	case cryptoSchemaAES:
		return "AES"
	case cryptoSchemaNoneOrAES:
		return "NoneOrAES"
	default:
		return fmt.Sprintf("Unknown (%d)", schema)
	}
}

// dirty hack to try to give everyone realistically-looking but unique PID (which C implementation requires)
func uniqueStartTime() uint32 {
	return uint32(processStartTime.Dec())
}

type nonceMsg struct {
	KeyID  [4]byte
	Schema int32
	Time   int32 // TODO - uint32, because Y2038
	Nonce  [16]byte
}

// first 4 bytes of cryptoKey are identifier. This is not a problem because arbitrary long keys are allowed.
func KeyIDFromCryptoKey(cryptoKey string) (keyID [4]byte) {
	copy(keyID[:], cryptoKey)
	return keyID
}

func (m *nonceMsg) writeTo(buf []byte) []byte {
	buf = append(buf, m.KeyID[:]...)
	buf = basictl.IntWrite(buf, m.Schema)
	buf = basictl.IntWrite(buf, m.Time)
	return append(buf, m.Nonce[:]...)
}

func (m *nonceMsg) readFrom(packetType uint32, body []byte) (_ []byte, err error) {
	if packetType != packetTypeRPCNonce {
		return body, fmt.Errorf("nonce packet type 0x%x instead of 0x%x", packetType, packetTypeRPCNonce)
	}
	if len(body) < len(m.KeyID) {
		return body, fmt.Errorf("nonce packet too short")
	}
	copy(m.KeyID[:], body)
	body = body[len(m.KeyID):]
	if body, err = basictl.IntRead(body, &m.Schema); err != nil {
		return body, fmt.Errorf("nonce packet too short: %w", err)
	}
	if body, err = basictl.IntRead(body, &m.Time); err != nil {
		return body, fmt.Errorf("nonce packet too short: %w", err)
	}
	if len(body) != len(m.Nonce) {
		return body, fmt.Errorf("nonce packet size mismatch")
	}
	copy(m.Nonce[:], body)
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
	if len(body) != 0 {
		return body, fmt.Errorf("extra %v bytes in handshake packet", len(body))
	}
	return body, nil
}

type cryptoKeys struct {
	readKey  [32]byte
	readIV   [16]byte
	writeKey [32]byte
	writeIV  [16]byte
}

func deriveCryptoKeys(client bool, key string, clientTime int32,
	clientNonce [16]byte, clientIP uint32, clientPort uint16,
	serverNonce [16]byte, serverIP uint32, serverPort uint16,
) (*cryptoKeys, error) {
	w, err := writeCryptoInitMsg(client, key, clientTime, clientNonce, clientIP, clientPort, serverNonce, serverIP, serverPort)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize write key derivation data: %w", err)
	}

	var keys cryptoKeys
	w1 := md5.Sum(w[1:])
	w2 := sha1.Sum(w)
	w3 := md5.Sum(w[2:])
	copy(keys.writeKey[:], w1[:])
	copy(keys.writeKey[12:], w2[:])
	copy(keys.writeIV[:], w3[:])

	r, err := writeCryptoInitMsg(!client, key, clientTime, clientNonce, clientIP, clientPort, serverNonce, serverIP, serverPort)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize read key derivation data: %w", err)
	}

	r1 := md5.Sum(r[1:])
	r2 := sha1.Sum(r)
	r3 := md5.Sum(r[2:])
	copy(keys.readKey[:], r1[:])
	copy(keys.readKey[12:], r2[:])
	copy(keys.readIV[:], r3[:])

	return &keys, nil
}

func writeCryptoInitMsg(client bool, key string, clientTime int32,
	clientNonce [16]byte, clientIP uint32, clientPort uint16,
	serverNonce [16]byte, serverIP uint32, serverPort uint16,
) ([]byte, error) {
	buf := &bytes.Buffer{}
	side := map[bool][]byte{true: []byte("CLIENT"), false: []byte("SERVER")}

	if err := binary.Write(buf, binary.LittleEndian, serverNonce); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, clientNonce); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, clientTime); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, serverIP); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, clientPort); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, side[client]); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, clientIP); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, serverPort); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, []byte(key)); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, serverNonce); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, clientNonce); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
