// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"crypto/md5"
	"crypto/sha1"

	"github.com/vkcom/statshouse/internal/vkgo/basictl"
)

type HandshakeMsgUdp struct {
	Flags     uint32
	SenderPID NetPID
	PeerPID   NetPID
}

type CryptoKeysUdp struct {
	ReadKey  [32]byte
	WriteKey [32]byte
}

func DeriveCryptoKeysUdp(key string, localPid *NetPID, remotePid *NetPID, generation uint32) *CryptoKeysUdp {
	w := writeCryptoInitMsgUdp(key, localPid, remotePid, generation)

	//fmt.Println("init write crypto buf", w)
	var keys CryptoKeysUdp
	w1 := md5.Sum(w[1:])
	w2 := sha1.Sum(w)
	copy(keys.WriteKey[:], w1[:])
	copy(keys.WriteKey[12:], w2[:])

	r := writeCryptoInitMsgUdp(key, remotePid, localPid, generation)
	//fmt.Println("init read crypto buf", r)

	r1 := md5.Sum(r[1:])
	r2 := sha1.Sum(r)
	copy(keys.ReadKey[:], r1[:])
	copy(keys.ReadKey[12:], r2[:])

	return &keys
}

func writeCryptoInitMsgUdp(key string, localPid *NetPID, remotePid *NetPID, generation uint32) []byte {
	var message []byte

	message, _ = localPid.Write(message)
	message = append(message, key...)
	message, _ = remotePid.Write(message)
	message = basictl.NatWrite(message, generation)
	return message
}
