// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package fsbinlog

/*
 *
 */

import (
	"crypto/md5"
	"encoding/binary"
)

func calcNextLogHash(prevHash uint64, nextLogPos int64, crc uint32) uint64 {
	var buf [8 + 8 + 4]byte
	binary.LittleEndian.PutUint64(buf[0:], prevHash)
	binary.LittleEndian.PutUint64(buf[8:], uint64(nextLogPos))
	binary.LittleEndian.PutUint32(buf[8+8:], crc)
	md5raw := md5.Sum(buf[:])
	return binary.LittleEndian.Uint64(md5raw[:])
}
