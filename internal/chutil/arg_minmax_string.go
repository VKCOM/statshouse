// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package chutil

import (
	"encoding/binary"
	"math"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/VKCOM/statshouse/internal/data_model"
)

type ColArgMinStringFloat32 []data_model.ArgMinStringFloat32
type ColArgMaxStringFloat32 []data_model.ArgMaxStringFloat32

func (col *ColArgMinStringFloat32) DecodeColumn(r *proto.Reader, rows int) (err error) {
	var res ColArgMinStringFloat32
	if cap(*col) < rows {
		res = make(ColArgMinStringFloat32, rows)
	} else {
		res = (*col)[:rows]
	}
	buf := make([]byte, 6)
	for i := 0; i < len(res); i++ {
		if buf, err = res[i].ReadFrom(r, buf); err != nil {
			return err
		}
	}
	*col = res
	return nil
}

func (col *ColArgMaxStringFloat32) DecodeColumn(r *proto.Reader, rows int) (err error) {
	var res ColArgMaxStringFloat32
	if cap(*col) < rows {
		res = make(ColArgMaxStringFloat32, rows)
	} else {
		res = (*col)[:rows]
	}
	buf := make([]byte, 6)
	for i := 0; i < len(res); i++ {
		if buf, err = res[i].ReadFrom(r, buf); err != nil {
			return err
		}
	}
	*col = res
	return nil
}

func (col *ColArgMinStringFloat32) Reset() {
	*col = (*col)[:0]
}

func (col *ColArgMaxStringFloat32) Reset() {
	*col = (*col)[:0]
}

func (col *ColArgMinStringFloat32) Rows() int {
	return len(*col)
}

func (col *ColArgMaxStringFloat32) Rows() int {
	return len(*col)
}

func (col *ColArgMinStringFloat32) Type() proto.ColumnType {
	return "AggregateFunction(argMin, String, Float32)"
}

func (col *ColArgMaxStringFloat32) Type() proto.ColumnType {
	return "AggregateFunction(argMax, String, Float32)"
}

// AppendArgMinMaxBytesFloat32 serializes argMin/argMax aggregate function data
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
