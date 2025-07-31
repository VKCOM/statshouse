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

func (arg *ArgMinMaxInt32Float32) ReadFrom(r io.Reader) error {
	var buf [4]byte
	br := bufio.NewReaderSize(r, 16)
	hasArg, err := br.ReadByte()
	if err != nil {
		return err
	}
	if hasArg != 0 {
		if _, err := io.ReadFull(br, buf[:4]); err != nil {
			return err
		}
		arg.Arg = int32(binary.LittleEndian.Uint32(buf[:]))
	}
	hasVal, err := br.ReadByte()
	if err != nil {
		return err
	}
	if hasVal != 0 {
		if _, err := io.ReadFull(br, buf[:4]); err != nil {
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

// MarshalBinary serializes ArgMinMaxInt32Float32 to ClickHouse RowBinary format
func (arg *ArgMinMaxInt32Float32) MarshalAppend(buf []byte) []byte {
	if arg.Arg != 0 {
		buf = append(buf, 1)
		var tmp [4]byte
		binary.LittleEndian.PutUint32(tmp[:], uint32(arg.Arg))
		buf = append(buf, tmp[:]...)
	} else {
		buf = append(buf, 0)
	}
	if arg.Val != 0 {
		buf = append(buf, 1)
		var tmp [4]byte
		binary.LittleEndian.PutUint32(tmp[:], math.Float32bits(arg.Val))
		buf = append(buf, tmp[:]...)
	} else {
		buf = append(buf, 0)
	}
	return buf
}
