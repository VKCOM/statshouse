// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package chutil

import (
	"github.com/ClickHouse/ch-go/proto"
	"github.com/VKCOM/statshouse/internal/data_model"
)

type ColArgMinInt32Float32 []data_model.ArgMinInt32Float32
type ColArgMaxInt32Float32 []data_model.ArgMaxInt32Float32

func (col *ColArgMinInt32Float32) DecodeColumn(r *proto.Reader, rows int) error {
	var res ColArgMinInt32Float32
	if cap(*col) < rows {
		res = make(ColArgMinInt32Float32, rows)
	} else {
		res = (*col)[:rows]
	}
	for i := 0; i < len(res); i++ {
		res[i].ReadFrom(r)
	}
	*col = res
	return nil
}

func (col *ColArgMaxInt32Float32) DecodeColumn(r *proto.Reader, rows int) error {
	var res ColArgMaxInt32Float32
	if cap(*col) < rows {
		res = make(ColArgMaxInt32Float32, rows)
	} else {
		res = (*col)[:rows]
	}
	for i := 0; i < len(res); i++ {
		res[i].ReadFrom(r)
	}
	*col = res
	return nil
}

func (col *ColArgMinInt32Float32) Reset() {
	*col = (*col)[:0]
}

func (col *ColArgMaxInt32Float32) Reset() {
	*col = (*col)[:0]
}

func (col *ColArgMinInt32Float32) Rows() int {
	return len(*col)
}

func (col *ColArgMaxInt32Float32) Rows() int {
	return len(*col)
}

func (col *ColArgMinInt32Float32) Type() proto.ColumnType {
	return "AggregateFunction(argMin, Int32, Float32)"
}

func (col *ColArgMaxInt32Float32) Type() proto.ColumnType {
	return "AggregateFunction(argMax, Int32, Float32)"
}
