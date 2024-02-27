// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package fsbinlog

const (
	magicLevCrc32                    = uint32(0x04435243)
	magicLevRotateFrom               = uint32(0x04724cd2)
	magicLevRotateTo                 = uint32(0x04464c72)
	magicLevTag                      = uint32(0x04476154)
	magicLevTimestamp                = uint32(0x04d931a8)
	magicKfsBinlogZipMagic           = uint32(0x047a4c4b)
	magicLevSetPersistentConfigValue = uint32(0xe133cd0d)
	magicLevSetPersistentConfigArray = uint32(0xe375d4a8)
)

type (
	levRotateFrom struct {
		Type        uint32
		Timestamp   int32
		CurLogPos   int64
		Crc32       uint32
		PrevLogHash uint64
		CurLogHash  uint64
	}

	levRotateTo struct {
		Type        uint32
		Timestamp   int32
		NextLogPos  int64
		Crc32       uint32
		CurLogHash  uint64
		NextLogHash uint64
	}

	levTimestamp struct {
		Type      uint32
		Timestamp int32
	}
	levCrc32 struct {
		Type      uint32
		Timestamp int32
		Pos       int64
		Crc32     uint32
	}

	levTag struct {
		Type uint32
		Tag  [16]uint8
	}

	levSetPersistentConfigValue struct { // lev_set_persistent_config_value
		Type  uint32
		Name  []byte
		Value int32
	}
)
