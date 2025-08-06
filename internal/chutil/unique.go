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

type ColUnique []data_model.ChUnique

func (col *ColUnique) Type() proto.ColumnType {
	return "AggregateFunction(uniq, Int64)"
}

func (col *ColUnique) Reset() {
	*col = nil // objects are owned by cache after reading, can not reuse
}

func (col *ColUnique) DecodeColumn(r *proto.Reader, rows int) error {
	var res ColUnique
	if cap(*col) < rows {
		res = make(ColUnique, rows)
	} else {
		res = (*col)[:rows]
	}
	for i := 0; i < len(res); i++ {
		if err := res[i].ReadFrom(r); err != nil {
			return err
		}
	}
	*col = res
	return nil
}

func (col *ColUnique) Rows() int {
	return len(*col)
}
