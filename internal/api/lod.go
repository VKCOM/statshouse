// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"time"
)

const (
	_0s  = 0
	_1s  = 1
	_5s  = 5
	_15s = 15
	_1m  = 60
	_5m  = 5 * _1m
	_15m = 15 * _1m
	_1h  = 60 * _1m
	_4h  = 4 * _1h
	_24h = 24 * _1h
	_7d  = 7 * _24h
	_1M  = 31 * _24h

	_1mTableSH1          = "statshouse_value_dist"
	_1hTableSH1          = "statshouse_value_dist_1h"
	_1hTableStringTopSH1 = "stats_1h_agg_stop_dist"
	_1dTableUniquesSH1   = "stats_1d_agg_dist"
	_1sTableSH2          = "statshouse_value_1s_dist"
	_1mTableSH2          = "statshouse_value_1m_dist"
	_1hTableSH2          = "statshouse_value_1h_dist"
)

var (
	preKeyTableNames = map[string]string{
		_1sTableSH2: "statshouse_value_1s_prekey_dist",
		_1mTableSH2: "statshouse_value_1m_prekey_dist",
		_1hTableSH2: "statshouse_value_1h_prekey_dist",
	}
)

func mathDiv(a int64, b int64) int64 {
	quo := a / b
	if (a >= 0) == (b >= 0) || a%b == 0 {
		return quo
	}
	return quo - 1
}

func roundTime(t int64, step int64, utcOffset int64) int64 {
	return mathDiv(t+utcOffset, step)*step - utcOffset
}

func shiftTimestamp(timestamp, stepSec, shift int64, location *time.Location) int64 {
	if stepSec == _1M {
		months := int(shift / _1M)
		return time.Unix(timestamp, 0).In(location).AddDate(months/12, months%12, 0).Unix()
	}

	return timestamp + shift
}

func calcUTCOffset(location *time.Location, weekStartsAt time.Weekday) int64 {
	date := time.Unix(0, 0).In(time.UTC)
	weekDay := date.Weekday()
	if weekDay == time.Sunday && weekStartsAt != time.Sunday {
		weekDay = 7
	}

	days := int64(weekDay - weekStartsAt)
	seconds := date.Unix() - time.Date(date.Year(), date.Month(), date.Day(), date.Hour(), date.Minute(), date.Second(), date.Nanosecond(), location).Unix()

	return days*24*3600 + seconds
}
