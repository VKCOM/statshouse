// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package data_model

import (
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

func (arg *ArgMinMaxStringFloat32) ReadFrom(r io.ByteReader, buf []byte) ([]byte, error) {
	buf = slices.Grow(buf, 6)[:6]
	// read string
	len, err := readUint32LE(r)
	if err != nil {
		return buf, err
	}
	// 0xffffffff is a special value that means "no string"
	if len != 0xffffffff && len > 0 {
		strFlag, err := r.ReadByte()
		if err != nil {
			return buf, err
		}
		if strFlag == 1 {
			buf = slices.Grow(buf, int(len-1))[:len-1]
			lastNonNull := 0
			for i := 0; i < int(len-1); i++ {
				buf[i], err = r.ReadByte()
				if err != nil {
					return buf, err
				}
				if buf[i] != 0 {
					lastNonNull = i
				}
			}
			arg.AsString = string(buf[:lastNonNull+1])
		} else {
			arg.AsInt32, err = readInt32LE(r)
			if err != nil {
				return buf, err
			}
			// ClickHouse might add extra bytes to string (null terminator) for compatibility with old versions
			// so we need to skip them
			for i := uint32(5); i < len; i++ { // 5 is the size of int32 + 1 for string flag
				_, _ = r.ReadByte()
			}
		}
	}
	// read value
	valueFlag, err := r.ReadByte()
	if err != nil {
		return buf, err
	}
	if valueFlag != 0 {
		val, err := readFloat32LE(r)
		if err != nil {
			return buf, err
		}
		arg.Val = val
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
		buf = append(buf, 0) // ClickHouse string format for arg* requires string terminator
	} else {
		dataLen := uint32(6) // sizeof(int32) + 2
		buf = binary.LittleEndian.AppendUint32(buf, dataLen)
		buf = append(buf, 0) // int marker
		buf = binary.LittleEndian.AppendUint32(buf, uint32(arg.AsInt32))
		buf = append(buf, 0) // ClickHouse string format for arg* requires string terminator
	}
	buf = append(buf, 1) // has value flag
	buf = binary.LittleEndian.AppendUint32(buf, math.Float32bits(arg.Val))
	return buf
}
