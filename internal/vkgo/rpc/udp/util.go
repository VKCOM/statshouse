// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package udp

import (
	"bytes"
	"encoding/binary"
	"hash/crc64"

	"github.com/vkcom/statshouse/internal/vkgo/basictl"
	"github.com/vkcom/statshouse/internal/vkgo/rpc/internal/gen/tlnet"
)

var (
	// workaround for https://github.com/golang/go/issues/41911, before we use Go 1.16 everywhere
	ecmaTable = crc64.MakeTable(crc64.ECMA)
)

func CalcHash(localPid *tlnet.Pid, remotePid *tlnet.Pid, generation uint32) uint64 {
	var buffer []byte
	var byteLocalPid []byte
	var byteRemotePid []byte

	byteLocalPid = localPid.Write(byteLocalPid)
	byteRemotePid = remotePid.Write(byteRemotePid)

	if bytes.Compare(byteLocalPid, byteRemotePid) < 0 {
		//fmt.Println("Smallest: ", localPid)
		//fmt.Println("Largest: ", remotePid)
		buffer = append(buffer, byteLocalPid...)
		buffer = append(buffer, byteRemotePid...)
	} else {
		//fmt.Println("Smallest: ", remotePid)
		//fmt.Println("Largest: ", localPid)
		buffer = append(buffer, byteRemotePid...)
		buffer = append(buffer, byteLocalPid...)
	}
	buffer = basictl.NatWrite(buffer, generation)

	//fmt.Println(buffer)

	return crc64.Checksum(buffer, ecmaTable)
}

func CopyIVTo(w *[32]byte, vec [8]uint32) {
	// for i, elem := range vec {
	//	binary.LittleEndian.PutUint32((*w)[i*4:], elem)
	// }
	// Manual unroll is extremely fast
	binary.LittleEndian.PutUint32((*w)[0:], vec[0])
	binary.LittleEndian.PutUint32((*w)[4:], vec[1])
	binary.LittleEndian.PutUint32((*w)[8:], vec[2])
	binary.LittleEndian.PutUint32((*w)[12:], vec[3])
	binary.LittleEndian.PutUint32((*w)[16:], vec[4])
	binary.LittleEndian.PutUint32((*w)[20:], vec[5])
	binary.LittleEndian.PutUint32((*w)[24:], vec[6])
	binary.LittleEndian.PutUint32((*w)[28:], vec[7])
}

func CopyIVFrom(vec *[8]uint32, w [32]byte) {
	// for i, elem := range vec {
	//	binary.LittleEndian.PutUint32((*w)[i*4:], elem)
	// }
	// Manual unroll is extremely fast
	(*vec)[0] = binary.LittleEndian.Uint32(w[0:])
	(*vec)[1] = binary.LittleEndian.Uint32(w[4:])
	(*vec)[2] = binary.LittleEndian.Uint32(w[8:])
	(*vec)[3] = binary.LittleEndian.Uint32(w[12:])
	(*vec)[4] = binary.LittleEndian.Uint32(w[16:])
	(*vec)[5] = binary.LittleEndian.Uint32(w[20:])
	(*vec)[6] = binary.LittleEndian.Uint32(w[24:])
	(*vec)[7] = binary.LittleEndian.Uint32(w[28:])
}
