// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package dac

import (
	"fmt"
	"time"

	"github.com/vkcom/statshouse/internal/format"
)

const (
	Version1 = "1"
	Version2 = "2"

	userTokenName = "user"

	maxSeriesRows = 10_000_000
	maxTableRows  = 100_000

	fastQueryTimeInterval = (86400 + 3600) * 2
	chDialTimeout         = 5 * time.Second
)

var (
	errTooManyRows = fmt.Errorf("can't fetch more than %v rows", maxSeriesRows)

	preKeyTableNames = map[string]string{
		_1sTableSH2: "statshouse_value_1s_prekey_dist",
		_1mTableSH2: "statshouse_value_1m_prekey_dist",
		_1hTableSH2: "statshouse_value_1h_prekey_dist",
	}
)

type PSelectRow struct {
	TsTags
	TsValues
}

type TsSelectRow struct {
	Time    int64
	StepSec int64 // TODO - do not get using strange logic in clickhouse, set directly
	TsTags
	TsValues
}

// all numeric tags are stored as int32 to save space
type TsTags struct {
	Tag      [format.MaxTags]int32
	TagStr   StringFixed
	ShardNum uint32
}

type TsValues struct {
	CountNorm float64
	Val       [7]float64
	Host      [2]int32 // "min" at [0], "max" at [1]
}

type LodInfo struct {
	FromSec    int64 // inclusive
	ToSec      int64 // exclusive
	StepSec    int64
	Table      string // is only here because we can't cleanly deduce it for v1 (unique-related madness etc.)
	HasPreKey  bool
	PreKeyOnly bool
	Location   *time.Location
}

func (lod LodInfo) IsFast() bool {
	return lod.FromSec+fastQueryTimeInterval >= lod.ToSec
}

func (lod LodInfo) IndexOf(timestamp int64) (int, error) {
	if lod.StepSec == _1M {
		n := 0
		t := lod.FromSec
		for ; t < timestamp; n++ {
			t = time.Unix(t, 0).In(lod.Location).AddDate(0, 1, 0).UTC().Unix()
		}
		if t == timestamp {
			return n, nil
		}
	} else {
		d := timestamp - lod.FromSec
		if d%lod.StepSec == 0 {
			return int(d / lod.StepSec), nil
		}
	}
	return 0, fmt.Errorf("timestamp %d is out of [%d,%d), step %d", timestamp, lod.FromSec, lod.ToSec, lod.StepSec)
}
