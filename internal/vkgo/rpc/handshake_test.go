// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"bytes"
	cryptorand "crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"reflect"
	"testing"

	"pgregory.net/rapid"

	"github.com/vkcom/statshouse/internal/vkgo/rpc/udp"

	"golang.org/x/crypto/curve25519"
)

func TestDeriveCryptoKeys(t *testing.T) {
	clientNonce := [16]byte{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p'}
	serverNonce := [16]byte{'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P'}
	serverSend := deriveCryptoKeys(false, "hren", 0x01020304,
		clientNonce, 0x05060708, 0x090a,
		serverNonce, 0x0d0e0f10, 0x1112, nil)
	clientSend := deriveCryptoKeys(true, "hren", 0x01020304,
		clientNonce, 0x05060708, 0x090a,
		serverNonce, 0x0d0e0f10, 0x1112, nil)
	if hex.EncodeToString(clientSend.Key[:]) != "28b5a5313b3ea9e2f6f0293e0748b2f743b0e112779faa77a3ee9d71ae70dda6" {
		t.Fatalf("readKey")
	}
	if hex.EncodeToString(clientSend.IV[:]) != "80387128489168b336d998762bce6fef" {
		t.Fatalf("readIV")
	}
	if hex.EncodeToString(serverSend.Key[:]) != "e3cf8557ea4ad963c3b637d466388403841d2e989a1fc684ac691c44b05ac9bb" {
		t.Fatalf("writeKey")
	}
	if hex.EncodeToString(serverSend.IV[:]) != "1efd4c8aa43a87d1ea5488a1bc669269" {
		t.Fatalf("writeIV")
	}
}

func TestDeriveCryptoKeysV1(t *testing.T) {
	clientNonce := [16]byte{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p'}
	serverNonce := [16]byte{'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P'}
	serverSend := deriveCryptoKeys(false, "hren", 0x01020304,
		clientNonce, 0, 0,
		serverNonce, 0x0d0e0f10, 0, nil)
	clientSend := deriveCryptoKeys(true, "hren", 0x01020304,
		clientNonce, 0, 0,
		serverNonce, 0x0d0e0f10, 0, nil)
	if hex.EncodeToString(clientSend.Key[:]) != "373374076f52d8f6bb5b063f17b9eb9fb4194e429cf02e207300add4c28a8e57" {
		t.Fatalf("readKey")
	}
	if hex.EncodeToString(clientSend.IV[:]) != "cea8f827019de36741f73e5948aea5be" {
		t.Fatalf("readIV")
	}
	if hex.EncodeToString(serverSend.Key[:]) != "3ce0c95487d99754688e0508a036c8c02727f297d0311db6273d69c07ac7a0d2" {
		t.Fatalf("writeKey")
	}
	if hex.EncodeToString(serverSend.IV[:]) != "34411262ac3e172bc1a2d086b4f1ecb5" {
		t.Fatalf("writeIV")
	}
}

func TestDeriveCryptoKeysV2(t *testing.T) {
	clientScalar := [32]byte{'0', '1', '2', '3', '4', '4', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'}
	serverScalar := [32]byte{'5', '6', '7', '8', '9', '9', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z'}
	clientPoint, err := curve25519.X25519(clientScalar[:], curve25519.Basepoint)
	if err != nil {
		t.Fatalf("client D-H point error: %v", err)
	}
	serverPoint, err := curve25519.X25519(serverScalar[:], curve25519.Basepoint)
	if err != nil {
		t.Fatalf("server D-H point error:%v", err)
	}
	clientSharedSecret, err := curve25519.X25519(clientScalar[:], serverPoint)
	if err != nil {
		t.Fatalf("client D-H shared secret  error:%v", err)
	}
	serverSharedSecret, err := curve25519.X25519(serverScalar[:], clientPoint)
	if err != nil {
		t.Fatalf("server D-H shared secret error:%v", err)
	}
	if hex.EncodeToString(serverSharedSecret) != hex.EncodeToString(clientSharedSecret) {
		t.Fatalf("different shared secrets")
	}
	if hex.EncodeToString(clientSharedSecret) != "4541d9fd5263298736d6ecdfa8c5834e12b54e2ad3bb95a50d2085dd4075f458" {
		t.Fatalf("clientSharedSecret")
	}

	clientNonce := [16]byte{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p'}
	serverNonce := [16]byte{'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P'}
	serverSend := deriveCryptoKeys(false, "hren", 0x01020304,
		clientNonce, 0, 0,
		serverNonce, 0x0d0e0f10, 0, serverSharedSecret)
	clientSend := deriveCryptoKeys(true, "hren", 0x01020304,
		clientNonce, 0, 0,
		serverNonce, 0x0d0e0f10, 0, serverSharedSecret)
	if hex.EncodeToString(clientSend.Key[:]) != "c513a88366728c719ffe885d943b0faa701ff7f0b061311b9af5fa5a0ec830ef" {
		t.Fatalf("readKey")
	}
	if hex.EncodeToString(clientSend.IV[:]) != "bbaf9484282c1d021c21d9da05e822c0" {
		t.Fatalf("readIV")
	}
	if hex.EncodeToString(serverSend.Key[:]) != "987d9938b0ea97bae1604e78d47131a5b0dc426054d5f9423d14f867480dce1d" {
		t.Fatalf("writeKey")
	}
	if hex.EncodeToString(serverSend.IV[:]) != "cf55ffd9615629f9cc7fc6b14d9a48f8" {
		t.Fatalf("writeIV")
	}
}

var result cryptoKeys

func BenchmarkCryptoKeysV1(b *testing.B) {
	var serverNonce [16]byte
	_, _ = cryptorand.Read(serverNonce[:])
	for i := 0; i < b.N; i++ {
		var clientNonce [16]byte
		_, _ = cryptorand.Read(clientNonce[:])
		serverSend := deriveCryptoKeys(false, "hren", 0x01020304,
			clientNonce, 0, 0,
			serverNonce, 0x0d0e0f10, 0, nil)
		clientSend := deriveCryptoKeys(true, "hren", 0x01020304,
			clientNonce, 0, 0,
			serverNonce, 0x0d0e0f10, 0, nil)
		for j, b := range serverSend.Key {
			result.Key[j] ^= b
		}
		for j, b := range serverSend.IV {
			result.IV[j] ^= b
		}
		for j, b := range clientSend.Key {
			result.Key[j] ^= b
		}
		for j, b := range clientSend.IV {
			result.IV[j] ^= b
		}
	}
}

func BenchmarkCryptoKeysV2(b *testing.B) {
	var serverNonce [16]byte
	_, _ = cryptorand.Read(serverNonce[:])
	var serverScalar [32]byte
	_, _ = cryptorand.Read(serverScalar[:])

	for i := 0; i < b.N; i++ {
		var clientScalar [32]byte
		_, _ = cryptorand.Read(clientScalar[:])

		serverPoint, _ := curve25519.X25519(serverScalar[:], curve25519.Basepoint)
		clientSharedSecret, _ := curve25519.X25519(clientScalar[:], serverPoint)

		var clientNonce [16]byte
		_, _ = cryptorand.Read(clientNonce[:])
		serverSend := deriveCryptoKeys(false, "hren", 0x01020304,
			clientNonce, 0, 0,
			serverNonce, 0x0d0e0f10, 0, clientSharedSecret)
		clientSend := deriveCryptoKeys(true, "hren", 0x01020304,
			clientNonce, 0, 0,
			serverNonce, 0x0d0e0f10, 0, clientSharedSecret)
		for j, b := range serverSend.Key {
			result.Key[j] ^= b
		}
		for j, b := range serverSend.IV {
			result.IV[j] ^= b
		}
		for j, b := range clientSend.Key {
			result.Key[j] ^= b
		}
		for j, b := range clientSend.IV {
			result.IV[j] ^= b
		}
	}
}

func TestDeriveCryptoKeysUDP(t *testing.T) {
	localPID := NetPID{
		Ip:      0x01020304,
		PortPid: 0x05060708,
		Utime:   0x090a0b0c,
	}
	remotePID := NetPID{
		Ip:      0x11121314,
		PortPid: 0x15161718,
		Utime:   0x191a1b1c,
	}
	keys := udp.DeriveCryptoKeysUdp("hren", &localPID, &remotePID, 0x20212223)
	if hex.EncodeToString(keys.ReadKey[:]) != "be5a0ca85071077fb48f030ab7627f88bf6c4ac3dc4c2c72b96d12692f4c33cf" {
		t.Fatalf("ReadKey")
	}
	if hex.EncodeToString(keys.WriteKey[:]) != "ff861195b0cffc0562e10667acd5ee3b7d884fb2bdcd63a68e03a7294c18be60" {
		t.Fatalf("WriteKey")
	}
}

func TestNetPIDRoundtrip(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		pid1 := NetPID{
			Ip:      rapid.Uint32().Draw(t, "ip"),
			PortPid: rapid.Uint32().Draw(t, "portPid"),
			Utime:   rapid.Uint32().Draw(t, "time"),
		}

		b1 := pid1.Write(nil)

		var b2 bytes.Buffer
		err := binary.Write(&b2, binary.LittleEndian, pid1)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(b1, b2.Bytes()) {
			t.Fatalf("got 0x%x instead of 0x%x", b1, b2.Bytes())
		}

		var pid2 NetPID
		_, err = pid2.Read(b1)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(pid1, pid2) {
			t.Fatalf("read back %#v, expected %#v", pid2, pid1)
		}
	})
}

func TestInvokeExtraRoundtrip(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		extra1 := InvokeReqExtra{}
		if rapid.Bool().Draw(t, "WaitBinlogPos") {
			extra1.SetWaitBinlogPos(rapid.Int64().Draw(t, "WaitBinlogPos"))
		}
		if rapid.Bool().Draw(t, "SetStringForwardKeys") {
			extra1.SetStringForwardKeys(rapid.SliceOfN(rapid.String(), 0, 2).Draw(t, "SetStringForwardKeys"))
			if len(extra1.StringForwardKeys) == 0 {
				extra1.StringForwardKeys = nil
			}
		}
		if rapid.Bool().Draw(t, "SetIntForwardKeys") {
			extra1.SetIntForwardKeys(rapid.SliceOfN(rapid.Int64(), 0, 2).Draw(t, "SetIntForwardKeys"))
			if len(extra1.IntForwardKeys) == 0 {
				extra1.IntForwardKeys = nil
			}
		}
		if rapid.Bool().Draw(t, "StringForward") {
			extra1.SetStringForward(rapid.String().Draw(t, "StringForward"))
		}
		if rapid.Bool().Draw(t, "IntForward") {
			extra1.SetIntForward(rapid.Int64().Draw(t, "IntForward"))
		}
		if rapid.Bool().Draw(t, "CustomTimeoutMs") {
			extra1.SetCustomTimeoutMs(rapid.Int32().Draw(t, "CustomTimeoutMs"))
		}
		if rapid.Bool().Draw(t, "SupportedCompressionVersion") {
			extra1.SetSupportedCompressionVersion(rapid.Int32().Draw(t, "SupportedCompressionVersion"))
		}
		if rapid.Bool().Draw(t, "RandomDelay") {
			extra1.SetRandomDelay(rapid.Float64().Draw(t, "RandomDelay"))
		}
		if rapid.Bool().Draw(t, "ReturnBinlogPos") {
			extra1.SetReturnBinlogPos(true)
		}
		if rapid.Bool().Draw(t, "NoResult") {
			extra1.SetNoResult(true)
		}

		b1 := extra1.Write(nil)

		var extra2 InvokeReqExtra
		if _, err := extra2.Read(b1); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(extra1, extra2) {
			t.Fatalf("read back %#v, expected %#v", extra2, extra1)
		}
	})
}

func TestResultExtraRoundtrip(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		pid1 := NetPID{
			Ip:      rapid.Uint32().Draw(t, "ip"),
			PortPid: rapid.Uint32().Draw(t, "portPid"),
			Utime:   rapid.Uint32().Draw(t, "time"),
		}

		extra1 := ResponseExtra{}
		if rapid.Bool().Draw(t, "BinlogPos") {
			extra1.SetBinlogPos(rapid.Int64().Draw(t, "BinlogPos"))
		}
		if rapid.Bool().Draw(t, "BinlogTime") {
			extra1.SetBinlogTime(rapid.Int64().Draw(t, "BinlogTime"))
		}
		if rapid.Bool().Draw(t, "EnginePID") {
			extra1.SetEnginePid(pid1)
		}
		if rapid.Bool().Draw(t, "SetRequestSize") {
			extra1.SetRequestSize(rapid.Int32().Draw(t, "SetRequestSize"))
		}
		if rapid.Bool().Draw(t, "SetResponseSize") {
			extra1.SetResponseSize(rapid.Int32().Draw(t, "SetResponseSize"))
		}
		if rapid.Bool().Draw(t, "SetFailedSubqueries") {
			extra1.SetFailedSubqueries(rapid.Int32().Draw(t, "SetFailedSubqueries"))
		}
		if rapid.Bool().Draw(t, "SetCompressionVersion") {
			extra1.SetCompressionVersion(rapid.Int32().Draw(t, "SetCompressionVersion"))
		}
		if rapid.Bool().Draw(t, "SetStats") {
			extra1.SetStats(rapid.MapOfN(rapid.String(), rapid.String(), 0, 2).Draw(t, "SetStats"))
			if len(extra1.Stats) == 0 {
				extra1.Stats = nil // canonical form
			}
		}
		if rapid.Bool().Draw(t, "SetViewNumber") {
			extra1.SetViewNumber(rapid.Int64Min(0).Draw(t, "SetViewNumber"))
			if rapid.Bool().Draw(t, "SetEpochNumber") {
				extra1.EpochNumber = rapid.Int64Min(0).Draw(t, "SetEpochNumber")
			}
		}

		b1 := extra1.Write(nil)

		var extra2 ResponseExtra
		if _, err := extra2.Read(b1); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(extra1, extra2) {
			t.Fatalf("read back %#v, expected %#v", extra2, extra1)
		}
	})
}
