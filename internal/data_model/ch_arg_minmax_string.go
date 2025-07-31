// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package data_model

import (
	"bufio"
	"encoding/binary"
	"io"
	"math"
	"slices"
)

type ArgMinMaxStringFloat32 struct {
	AsString string
	AsInt32  int32
	Val      float32
}

type ArgMinStringFloat32 struct {
	ArgMinMaxStringFloat32
}

type ArgMaxStringFloat32 struct {
	ArgMinMaxStringFloat32
}

func (arg *ArgMinMaxStringFloat32) ReadFrom(r io.Reader, buf []byte) ([]byte, error) {
	br := bufio.NewReaderSize(r, 16)
	buf = slices.Grow(buf, 6)[:6]
	// read string
	if _, err := io.ReadFull(br, buf[:4]); err != nil {
		return buf, err
	}
	if n := int32(binary.LittleEndian.Uint32(buf)); n > 0 {
		buf = slices.Grow(buf, int(n))[:n]
		if _, err := io.ReadFull(br, buf); err != nil {
			return buf, err
		}
		if buf[0] == 0 { // 1 string/int flag + 4 int bytes
			if n != 5 {
				return buf, io.ErrUnexpectedEOF
			}
			arg.AsInt32 = int32(binary.LittleEndian.Uint32(buf[1:]))

		} else if n > 1 {
			arg.AsString = string(buf[1 : n-1])
		}
	}
	// read value
	hasValue, err := br.ReadByte()
	if err != nil {
		return buf, err
	}
	if hasValue != 0 {
		buf = buf[:4]
		if _, err := io.ReadFull(br, buf); err != nil {
			return buf, err
		}
		arg.Val = math.Float32frombits(binary.LittleEndian.Uint32(buf))
	}
	return buf, nil
}

func (arg *ArgMinStringFloat32) Merge(rhs ArgMinStringFloat32) {
	if arg.Empty() {
		*arg = rhs
	} else if rhs.Val < arg.Val {
		*arg = rhs
	}
}

func (arg *ArgMaxStringFloat32) Merge(rhs ArgMaxStringFloat32) {
	if arg.Empty() {
		*arg = rhs
	} else if arg.Val < rhs.Val {
		*arg = rhs
	}
}

// TODO: remove if unused
func (arg *ArgMinMaxStringFloat32) Merge(rhs ArgMinMaxStringFloat32, x int) {
	if arg.Empty() {
		*arg = rhs
	} else if x == 0 {
		// min
		if rhs.Val < arg.Val {
			*arg = rhs
		}
	} else {
		// max
		if arg.Val < rhs.Val {
			*arg = rhs
		}
	}
}

func (arg *ArgMinMaxStringFloat32) Empty() bool {
	return arg.AsInt32 == 0 && arg.AsString == ""
}

func (arg *ArgMinMaxStringFloat32) MarshallAppend(buf []byte) []byte {
	if arg.AsString != "" {
		dataLen := uint32(len(arg.AsString) + 2)
		buf = binary.LittleEndian.AppendUint32(buf, dataLen)
		buf = append(buf, 1) // string marker
		buf = append(buf, []byte(arg.AsString)...)
		buf = append(buf, 0) // for some reason ClickHouse likes to add string terminator
	} else {
		dataLen := uint32(5) // sizeof(int32) + 1
		buf = binary.LittleEndian.AppendUint32(buf, dataLen)
		buf = append(buf, 0) // int marker
		buf = binary.LittleEndian.AppendUint32(buf, uint32(arg.AsInt32))
	}
	buf = append(buf, 1) // has value flag
	buf = binary.LittleEndian.AppendUint32(buf, math.Float32bits(arg.Val))
	return buf
}
