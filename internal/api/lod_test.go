// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const defaultTestTimezone = "Europe/Moscow"

type Int64Slice []int64

func (x Int64Slice) Len() int           { return len(x) }
func (x Int64Slice) Less(i, j int) bool { return x[i] < x[j] }
func (x Int64Slice) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }

func TestShiftTimestamp(t *testing.T) {
	location, err := time.LoadLocation(defaultTestTimezone)
	if err != nil {
		t.Errorf("can't load location %q: %v", defaultTestTimezone, err)
	}
	type params struct {
		timestamp int64
		stepSec   int64
		shift     int64
	}
	tests := []struct {
		data params
		want int64
	}{{
		data: params{
			timestamp: 1656622800, // пятница, 1 июля 2022 г., 0:00:00 GMT+03:00
			stepSec:   _1M,
			shift:     -_1M,
		},
		want: 1654030800, // среда, 1 июня 2022 г., 0:00:00 GMT+03:00
	}, {
		data: params{
			timestamp: 1656622800, // пятница, 1 июля 2022 г., 0:00:00 GMT+03:00
			stepSec:   _1M,
			shift:     -15 * _1M,
		},
		want: 1617224400, // четверг, 1 апреля 2021 г., 0:00:00 GMT+03:00
	}, {
		data: params{
			timestamp: 1656882000, // понедельник, 4 июля 2022 г., 0:00:00 GMT+03:00
			stepSec:   _7d,
			shift:     -4 * _7d,
		},
		want: 1654462800, // понедельник, 6 июня 2022 г., 0:00:00 GMT+03:00
	}}
	for _, test := range tests {
		t.Run(fmt.Sprintf("data: %+v", test.data), func(t *testing.T) {
			shiftedDate := shiftTimestamp(test.data.timestamp, test.data.stepSec, test.data.shift, location)
			assert.Equal(t, test.want, shiftedDate, "check shifted timestamp")
		})
	}
}

func TestCalcUTCOffset(t *testing.T) {
	type params struct {
		weekStartsAt time.Weekday
		location     string
	}
	tests := []struct {
		data params
		want int64
	}{{
		data: params{
			weekStartsAt: time.Sunday,
			location:     "Europe/Moscow",
		},
		want: (4*24 + 3) * 3600,
	}, {
		data: params{
			weekStartsAt: time.Monday,
			location:     "Europe/Moscow",
		},
		want: (3*24 + 3) * 3600,
	}, {
		data: params{
			weekStartsAt: time.Tuesday,
			location:     "Europe/Moscow",
		},
		want: (2*24 + 3) * 3600,
	}, {
		data: params{
			weekStartsAt: time.Wednesday,
			location:     "Europe/Moscow",
		},
		want: (1*24 + 3) * 3600,
	}, {
		data: params{
			weekStartsAt: time.Thursday,
			location:     "Europe/Moscow",
		},
		want: 3 * 3600,
	}, {
		data: params{
			weekStartsAt: time.Friday,
			location:     "Europe/Moscow",
		},
		want: (3 - 24) * 3600,
	}, {
		data: params{
			weekStartsAt: time.Saturday,
			location:     "Europe/Moscow",
		},
		want: (3 - 2*24) * 3600,
	}, {
		data: params{
			weekStartsAt: time.Sunday,
			location:     "Asia/Tokyo",
		},
		want: (4*24 + 9) * 3600,
	}, {
		data: params{
			weekStartsAt: time.Monday,
			location:     "Asia/Tokyo",
		},
		want: (3*24 + 9) * 3600,
	}, {
		data: params{
			weekStartsAt: time.Tuesday,
			location:     "Asia/Tokyo",
		},
		want: (2*24 + 9) * 3600,
	}, {
		data: params{
			weekStartsAt: time.Wednesday,
			location:     "Asia/Tokyo",
		},
		want: (1*24 + 9) * 3600,
	}, {
		data: params{
			weekStartsAt: time.Thursday,
			location:     "Asia/Tokyo",
		},
		want: 9 * 3600,
	}, {
		data: params{
			weekStartsAt: time.Friday,
			location:     "Asia/Tokyo",
		},
		want: (9 - 24) * 3600,
	}, {
		data: params{
			weekStartsAt: time.Saturday,
			location:     "Asia/Tokyo",
		},
		want: (9 - 2*24) * 3600,
	}}
	for _, test := range tests {
		t.Run(fmt.Sprintf("data: %+v", test.data), func(t *testing.T) {
			loc, err := time.LoadLocation(test.data.location)
			if err != nil {
				t.Errorf("can't load location: %s", test.data.location)
			}

			assert.Equal(t, test.want, calcUTCOffset(loc, test.data.weekStartsAt))
		})
	}
}
