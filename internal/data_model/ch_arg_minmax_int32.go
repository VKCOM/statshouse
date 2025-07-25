// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package data_model

import (
	"encoding/binary"
	"math"

	"github.com/ClickHouse/ch-go/proto"
)

type ArgMinMaxInt32Float32 struct {
	Arg int32
	Val float32
}

type ArgMinInt32Float32 struct {
	ArgMinMaxInt32Float32
}

type ArgMaxInt32Float32 struct {
	ArgMinMaxInt32Float32
}

func (arg *ArgMinMaxInt32Float32) ReadFromProto(r ProtoReader) error {
	var buf [4]byte
	hasArg, err := r.ReadByte()
	if err != nil {
		return err
	}
	if hasArg != 0 {
		if err := r.ReadFull(buf[:4]); err != nil {
			return err
		}
		arg.Arg = int32(binary.LittleEndian.Uint32(buf[:]))
	}
	hasVal, err := r.ReadByte()
	if err != nil {
		return err
	}
	if hasVal != 0 {
		if err := r.ReadFull(buf[:4]); err != nil {
			return err
		}
		arg.Val = math.Float32frombits(binary.LittleEndian.Uint32(buf[:]))
	}
	return nil
}

func (arg *ArgMinInt32Float32) Merge(rhs ArgMinInt32Float32) {
	if rhs.Val < arg.Val {
		*arg = rhs
	}
}

func (arg *ArgMaxInt32Float32) Merge(rhs ArgMaxInt32Float32) {
	if arg.Val < rhs.Val {
		*arg = rhs
	}
}

// TODO: remove
func (arg *ArgMinInt32Float32) AsArgMinMaxStringFloat32() ArgMinMaxStringFloat32 {
	return ArgMinMaxStringFloat32{
		AsInt32: arg.Arg,
		Val:     arg.Val,
	}
}

// TODO: remove
func (arg *ArgMaxInt32Float32) AsArgMinMaxStringFloat32() ArgMinMaxStringFloat32 {
	return ArgMinMaxStringFloat32{
		AsInt32: arg.Arg,
		Val:     arg.Val,
	}
}

// MarshalBinary serializes ArgMinMaxInt32Float32 to ClickHouse RowBinary format
func (a *ArgMinMaxInt32Float32) MarshalAppend(buf []byte) []byte {
	if a.Arg != 0 {
		buf = append(buf, 1)
		var tmp [4]byte
		binary.LittleEndian.PutUint32(tmp[:], uint32(a.Arg))
		buf = append(buf, tmp[:]...)
	} else {
		buf = append(buf, 0)
	}
	if a.Val != 0 {
		buf = append(buf, 1)
		var tmp [4]byte
		binary.LittleEndian.PutUint32(tmp[:], math.Float32bits(a.Val))
		buf = append(buf, tmp[:]...)
	} else {
		buf = append(buf, 0)
	}
	return buf
}

// TODO: remove
// ToStringFormat converts ArgMinMaxInt32Float32 to ArgMinMaxStringFloat32 (for V3)
func (a *ArgMinMaxInt32Float32) ToStringFormat() ArgMinMaxStringFloat32 {
	return ArgMinMaxStringFloat32{
		AsString: string([]byte{0, byte(a.Arg), byte(a.Arg >> 8), byte(a.Arg >> 16), byte(a.Arg >> 24)}),
		AsInt32:  a.Arg,
		Val:      a.Val,
	}
}

// TODO: remove
// UnmarshalArgMinMaxInt32Float32 unmarshals from proto.Reader into ArgMinMaxInt32Float32
func UnmarshalArgMinMaxInt32Float32(r *proto.Reader, v *ArgMinMaxInt32Float32) error {
	return v.ReadFromProto(r)
}
