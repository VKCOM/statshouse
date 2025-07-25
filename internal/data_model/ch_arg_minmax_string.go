// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package data_model

import (
	"encoding/binary"
	"math"
	"slices"
)

type ArgMinMaxStringFloat32 struct {
	AsString string // TODO: maybe []byte?
	AsInt32  int32
	Val      float32
}

type ArgMinStringFloat32 struct {
	ArgMinMaxStringFloat32
}

type ArgMaxStringFloat32 struct {
	ArgMinMaxStringFloat32
}

func (res *ArgMinMaxStringFloat32) ReadFromProto(r ProtoReader, buf []byte) ([]byte, error) {
	buf = slices.Grow(buf, 6)[:6]
	// read string
	if err := r.ReadFull(buf[:4]); err != nil {
		return buf, err
	}
	if n := int32(binary.LittleEndian.Uint32(buf)); n > 0 {
		buf = slices.Grow(buf, int(n))[:n]
		if err := r.ReadFull(buf); err != nil {
			return buf, err
		}
		if n == 6 && buf[0] == 0 {
			res.AsInt32 = int32(binary.LittleEndian.Uint32(buf[1:]))
		} else if n > 1 {
			res.AsString = string(buf[:n-1]) // exclude trailing 0 byte (string terminator)
		}
	}
	// read value
	hasValue, err := r.ReadByte()
	if err != nil {
		return buf, err
	}
	if hasValue != 0 {
		buf = buf[:4]
		if err := r.ReadFull(buf); err != nil {
			return buf, err
		}
		res.Val = math.Float32frombits(binary.LittleEndian.Uint32(buf))
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
	buf = AppendArgMinMaxBytesFloat32(buf, []byte(arg.AsString), arg.Val)
	return buf
}

// TODO: remove if unused
func AppendArgMinMaxStringFloat32(buf []byte, arg string, v float32) []byte {
	return AppendArgMinMaxBytesFloat32(buf, []byte(arg), v)
}

// TODO: inline in MarshallAppend if possible
func AppendArgMinMaxBytesFloat32(buf []byte, arg []byte, v float32) []byte {
	var tmp1 [4]byte
	var tmp2 [4]byte
	binary.LittleEndian.PutUint32(tmp1[:], uint32(len(arg)+1)) // string size + 1, or -1 if aggregate is empty
	binary.LittleEndian.PutUint32(tmp2[:], math.Float32bits(v))
	buf = append(buf, tmp1[:]...)
	buf = append(buf, arg...)
	buf = append(buf, 0, 1) // string terminator, bool
	return append(buf, tmp2[:]...)
}
