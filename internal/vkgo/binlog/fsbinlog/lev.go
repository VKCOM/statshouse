// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package fsbinlog

import (
	"encoding/binary"
	"fmt"

	"github.com/vkcom/statshouse/internal/vkgo/binlog"
	"github.com/vkcom/statshouse/internal/vkgo/binlog/fsbinlog/internal/gen/tlfsbinlog"
)

const levCrcSize = 20
const levRotateSize = 36

func readLevStart(lev *tlfsbinlog.LevStart, data []byte) (int, error) {
	newData, err := lev.ReadBoxed(data)
	return len(data) - len(newData), err
}

func readLevRotateFrom(lev *levRotateFrom, data []byte) (int, error) {
	lev.Type = magicLevRotateFrom

	if len(data) < levRotateSize {
		return 0, binlog.ErrorNotEnoughData
	}

	lev.Timestamp = int32(binary.LittleEndian.Uint32(data[4:]))
	lev.CurLogPos = int64(binary.LittleEndian.Uint64(data[8:]))
	lev.Crc32 = binary.LittleEndian.Uint32(data[16:])
	lev.PrevLogHash = binary.LittleEndian.Uint64(data[20:])
	lev.CurLogHash = binary.LittleEndian.Uint64(data[28:])

	return levRotateSize, nil
}

func readLevTag(lev *levTag, data []byte) (int, error) {
	if len(data) < 20 {
		return 0, binlog.ErrorNotEnoughData
	}

	lev.Type = magicLevTag

	copy(lev.Tag[:], data[4:20])
	return 20, nil
}

func readLevCrc32(lev *levCrc32, data []byte) (int, error) {
	if len(data) < levCrcSize {
		return 0, binlog.ErrorNotEnoughData
	}

	lev.Type = magicLevCrc32
	lev.Timestamp = int32(binary.LittleEndian.Uint32(data[4:]))
	lev.Pos = int64(binary.LittleEndian.Uint64(data[8:]))
	lev.Crc32 = binary.LittleEndian.Uint32(data[16:])

	return levCrcSize, nil
}

func readLevTimestamp(lev *levTimestamp, data []byte) (int, error) {
	if len(data) < 8 {
		return 0, binlog.ErrorNotEnoughData
	}

	lev.Type = magicLevTimestamp
	lev.Timestamp = int32(binary.LittleEndian.Uint32(data[4:]))

	return 8, nil
}

func readLevRotateTo(lev *levRotateTo, data []byte) (int, error) {
	if len(data) < levRotateSize {
		return 0, binlog.ErrorNotEnoughData
	}

	magic := binary.LittleEndian.Uint32(data)
	if magic != magicLevRotateTo {
		return 0, fmt.Errorf("magic not equal")
	}

	lev.Type = magicLevRotateTo
	lev.Timestamp = int32(binary.LittleEndian.Uint32(data[4:]))
	lev.NextLogPos = int64(binary.LittleEndian.Uint64(data[8:]))
	lev.Crc32 = binary.LittleEndian.Uint32(data[16:])
	lev.CurLogHash = binary.LittleEndian.Uint64(data[20:])
	lev.NextLogHash = binary.LittleEndian.Uint64(data[28:])
	return levRotateSize, nil
}

func readLevSetPersistentConfigValue(lev *levSetPersistentConfigValue, data []byte) (int, error) {
	if len(data) < 12 {
		return 0, binlog.ErrorNotEnoughData
	}

	lev.Type = magicLevTag
	nameLen := int(binary.LittleEndian.Uint32(data[4:]))
	lev.Value = int32(binary.LittleEndian.Uint32(data[8:]))
	data = data[12:]

	if len(data) < nameLen {
		return 0, binlog.ErrorNotEnoughData
	}
	copy(lev.Name, data[:nameLen])
	return 12 + nameLen, nil
}

func writeLevCrc32(lev *levCrc32) []byte {
	lev.Type = magicLevCrc32

	buff := [20]byte{}
	binary.LittleEndian.PutUint32(buff[:], lev.Type)
	binary.LittleEndian.PutUint32(buff[4:], uint32(lev.Timestamp))
	binary.LittleEndian.PutUint64(buff[8:], uint64(lev.Pos))
	binary.LittleEndian.PutUint32(buff[16:], lev.Crc32)

	// TODO: no tmp buffers
	return buff[:]
}

func writeLevRotateTo(lev *levRotateTo) []byte {
	buff := [36]byte{}

	binary.LittleEndian.PutUint32(buff[:], lev.Type)
	binary.LittleEndian.PutUint32(buff[4:], uint32(lev.Timestamp))
	binary.LittleEndian.PutUint64(buff[8:], uint64(lev.NextLogPos))
	binary.LittleEndian.PutUint32(buff[16:], lev.Crc32)
	binary.LittleEndian.PutUint64(buff[20:], lev.CurLogHash)
	binary.LittleEndian.PutUint64(buff[28:], lev.NextLogHash)
	return buff[:]
}

func writeLevRotateFrom(lev *levRotateFrom) []byte {
	buff := [36]byte{}

	binary.LittleEndian.PutUint32(buff[:], lev.Type)
	binary.LittleEndian.PutUint32(buff[4:], uint32(lev.Timestamp))
	binary.LittleEndian.PutUint64(buff[8:], uint64(lev.CurLogPos))
	binary.LittleEndian.PutUint32(buff[16:], lev.Crc32)
	binary.LittleEndian.PutUint64(buff[20:], lev.PrevLogHash)
	binary.LittleEndian.PutUint64(buff[28:], lev.CurLogHash)

	return buff[:]
}
