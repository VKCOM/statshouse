// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

const defaultTestTimezone = "Europe/Moscow"

type Int64Slice []int64

func (x Int64Slice) Len() int           { return len(x) }
func (x Int64Slice) Less(i, j int) bool { return x[i] < x[j] }
func (x Int64Slice) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }

func init() {
	time.Local = time.UTC // for testing the correctness of location based date calculations
}

func TestRoundRange(t *testing.T) {
	location, err := time.LoadLocation(defaultTestTimezone)
	if err != nil {
		t.Errorf("can't load location %q: %v", defaultTestTimezone, err)
	}

	rapid.Check(t, func(t *rapid.T) {
		var (
			start     = rapid.Int64Range(_1M*2, _1M*2+_7d).Draw(t, "start")
			end       = rapid.Int64Range(start, _1M*6).Draw(t, "end")
			step      = rapid.SampledFrom([]int64{_1s, _5s, _15s, _1m, _5m, _15m, _1h, _4h, _24h, _7d, _1M}).Draw(t, "step")
			utcOffset = rapid.Int64Range(-168, 168).Draw(t, "utcOffset") * 3600
		)
		rStart, rEnd := roundRange(start, end, step, utcOffset, location)
		t.Logf("start %v, end %v", rStart, rEnd)
		switch {
		case step == 1:
			require.Equal(t, start, rStart)
			require.Equal(t, end, rEnd)
		case step <= _1h:
			uStart, uEnd := roundRange(start, end, step, 0, location)
			require.Equal(t, rStart, uStart)
			require.Equal(t, rEnd, uEnd)
		}

		require.LessOrEqual(t, rStart, rEnd)
		require.GreaterOrEqual(t, start, rStart)
		if end != start {
			if step == _1M {
				require.LessOrEqual(
					t,
					end-1,
					time.Unix(rEnd, 0).In(location).Add(-time.Duration(step)).Unix(),
				)
			} else {
				require.LessOrEqual(t, end-1, rEnd-step)
			}
		}

		require.True(t, isTimestampValid(rStart, step, utcOffset, location))
	})
}

func TestRoundRangeExact(t *testing.T) {
	location, err := time.LoadLocation(defaultTestTimezone)
	if err != nil {
		t.Errorf("can't load location %q: %v", defaultTestTimezone, err)
	}

	type params struct {
		start     int64
		end       int64
		step      int64
		utcOffset int64
	}
	type result struct {
		start int64
		end   int64
	}
	tests := []struct {
		data params
		want result
	}{{
		data: params{
			start:     1654604458, // вторник, 7 июня 2022 г., 15:20:58 GMT+03:00
			end:       1657196462, // четверг, 7 июля 2022 г., 15:21:02 GMT+03:00
			step:      _1M,
			utcOffset: 75 * 3600,
		},
		want: result{
			start: 1654030800, // среда, 1 июня 2022 г., 0:00:00 GMT+03:00
			end:   1661979600, // четверг, 1 сентября 2022 г., 0:00:00 GMT+03:00
		},
	}, {
		data: params{
			start:     1654604458, // вторник, 7 июня 2022 г., 15:20:58 GMT+03:00
			end:       1656622800, // пятница, 1 июля 2022 г., 0:00:00 GMT+03:00
			step:      _1M,
			utcOffset: 75 * 3600,
		},
		want: result{
			start: 1654030800, // среда, 1 июня 2022 г., 0:00:00 GMT+03:00
			end:   1659301200, // понедельник, 1 августа 2022 г., 0:00:00 GMT+03:00
		},
	}}
	for _, test := range tests {
		t.Run(fmt.Sprintf("data: %+v", test.data), func(t *testing.T) {
			start, end := roundRange(test.data.start, test.data.end, test.data.step, test.data.utcOffset, location)
			assert.Equal(t, test.want.start, start, "check start")
			assert.Equal(t, test.want.end, end, "check end")
		})
	}
}

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

func TestGetIndexForTimestamp(t *testing.T) {
	location, err := time.LoadLocation(defaultTestTimezone)
	if err != nil {
		t.Errorf("can't load location %q: %v", defaultTestTimezone, err)
	}

	type params struct {
		lod        lodInfo
		timestamp  int64
		shiftDelta int64
	}
	tests := []struct {
		data params
		want int
	}{{
		data: params{
			lod: lodInfo{
				fromSec:  1654030800, // среда, 1 июня 2022 г., 0:00:00 GMT+03:00
				toSec:    1661979600, // четверг, 1 сентября 2022 г., 0:00:00 GMT+03:00
				stepSec:  _1M,
				location: location,
			},
			timestamp:  1654030800, // среда, 1 июня 2022 г., 0:00:00 GMT+03:00
			shiftDelta: 0,
		},
		want: 0,
	}, {
		data: params{
			lod: lodInfo{
				fromSec:  1654030800, // среда, 1 июня 2022 г., 0:00:00 GMT+03:00
				toSec:    1661979600, // четверг, 1 сентября 2022 г., 0:00:00 GMT+03:00
				stepSec:  _1M,
				location: location,
			},
			timestamp:  1656622800, // пятница, 1 июля 2022 г., 0:00:00 GMT+03:00
			shiftDelta: 0,
		},
		want: 1,
	}, {
		data: params{
			lod: lodInfo{
				fromSec:  1654030800, // среда, 1 июня 2022 г., 0:00:00 GMT+03:00
				toSec:    1661979600, // четверг, 1 сентября 2022 г., 0:00:00 GMT+03:00
				stepSec:  _1M,
				location: location,
			},
			timestamp:  1659301200, // понедельник, 1 августа 2022 г., 0:00:00 GMT+03:00
			shiftDelta: 0,
		},
		want: 2,
	}, {
		data: params{
			lod: lodInfo{
				fromSec:  1654030800, // среда, 1 июня 2022 г., 0:00:00 GMT+03:00
				toSec:    1661979600, // четверг, 1 сентября 2022 г., 0:00:00 GMT+03:00
				stepSec:  _1M,
				location: location,
			},
			timestamp:  1661979600, // четверг, 1 сентября 2022 г., 0:00:00 GMT+03:00
			shiftDelta: 0,
		},
		want: 3,
	}, {
		data: params{
			lod: lodInfo{
				fromSec:  1654030800, // среда, 1 июня 2022 г., 0:00:00 GMT+03:00
				toSec:    1661979600, // четверг, 1 сентября 2022 г., 0:00:00 GMT+03:00
				stepSec:  _1M,
				location: location,
			},
			timestamp:  1654030800, // среда, 1 июня 2022 г., 0:00:00 GMT+03:00
			shiftDelta: -_1M,
		},
		want: 1,
	}, {
		data: params{
			lod: lodInfo{
				fromSec:  1654030800, // среда, 1 июня 2022 г., 0:00:00 GMT+03:00
				toSec:    1661979600, // четверг, 1 сентября 2022 г., 0:00:00 GMT+03:00
				stepSec:  _1M,
				location: location,
			},
			timestamp:  1654030800, // среда, 1 июня 2022 г., 0:00:00 GMT+03:00
			shiftDelta: -3 * _1M,
		},
		want: 3,
	}, {
		data: params{
			lod: lodInfo{
				fromSec:  1654030800, // среда, 1 июня 2022 г., 0:00:00 GMT+03:00
				toSec:    1661979600, // четверг, 1 сентября 2022 г., 0:00:00 GMT+03:00
				stepSec:  _1M,
				location: location,
			},
			timestamp:  1622494800, // вторник, 1 июня 2021 г., 0:00:00 GMT+03:00
			shiftDelta: -12 * _1M,
		},
		want: 0,
	}, {
		data: params{
			lod: lodInfo{
				fromSec:  1641157200, // понедельник, 3 января 2022 г., 0:00:00 GMT+03:00
				toSec:    1657486800, // понедельник, 11 июля 2022 г., 0:00:00 GMT+03:00
				stepSec:  _7d,
				location: location,
			},
			timestamp:  1649624400, // понедельник, 11 апреля 2022 г., 0:00:00 GMT+03:00
			shiftDelta: 0,
		},
		want: 14,
	}, {
		data: params{
			lod: lodInfo{
				fromSec:  1641157200, // понедельник, 3 января 2022 г., 0:00:00 GMT+03:00
				toSec:    1657486800, // понедельник, 11 июля 2022 г., 0:00:00 GMT+03:00
				stepSec:  _7d,
				location: location,
			},
			timestamp:  1649624400, // понедельник, 11 апреля 2022 г., 0:00:00 GMT+03:00
			shiftDelta: -3 * _7d,
		},
		want: 17,
	}}
	for _, test := range tests {
		t.Run(fmt.Sprintf("data: %+v", test.data), func(t *testing.T) {
			idx := test.data.lod.getIndexForTimestamp(test.data.timestamp, test.data.shiftDelta)
			assert.Equal(t, test.want, idx, "check index")
		})
	}
}

func TestLODGenerateTimePoints(t *testing.T) {
	location, err := time.LoadLocation(defaultTestTimezone)
	if err != nil {
		t.Errorf("can't load location %q: %v", defaultTestTimezone, err)
	}

	rapid.Check(t, func(t *rapid.T) {
		var (
			start     = rapid.Int64Range(0, _1M*2).Draw(t, "start")
			end       = rapid.Int64Range(start, _1M*6).Draw(t, "end")
			step      = rapid.SampledFrom([]int64{_1s, _5s, _15s, _1m, _5m, _15m, _1h, _4h, _24h, _7d, _1M}).Draw(t, "step")
			utcOffset = rapid.Int64Range(-168, 168).Draw(t, "utcOffset") * 3600
		)
		rStart, rEnd := roundRange(start, end, step, utcOffset, location)
		lod := lodInfo{
			fromSec:  rStart,
			toSec:    rEnd,
			stepSec:  step,
			location: location,
		}
		t.Logf("lod %+v", lod)

		if end != start {
			points := lod.generateTimePoints(0)
			require.Greater(t, len(points), 0)
			require.Equal(t, lod.fromSec, points[0])
			require.Less(t, points[len(points)-1], lod.toSec)
			require.True(t, sort.IsSorted(Int64Slice(points)))
		}
	})
}

func TestLODGenerateTimePointsExact(t *testing.T) {
	location, err := time.LoadLocation(defaultTestTimezone)
	if err != nil {
		t.Errorf("can't load location %q: %v", defaultTestTimezone, err)
	}

	type params struct {
		lod   lodInfo
		shift int64
	}
	tests := []struct {
		data params
		want []int64
	}{{
		data: params{
			lod: lodInfo{
				fromSec:  1625086800,
				toSec:    1659301200,
				stepSec:  _1M,
				location: location,
			},
			shift: 0,
		},
		want: []int64{1625086800, 1627765200, 1630443600, 1633035600, 1635714000, 1638306000, 1640984400, 1643662800, 1646082000, 1648760400, 1651352400, 1654030800, 1656622800},
	}, {
		data: params{
			lod: lodInfo{
				fromSec:  1654462800,
				toSec:    1658091600,
				stepSec:  _7d,
				location: location,
			},
			shift: 0,
		},
		want: []int64{1654462800, 1655067600, 1655672400, 1656277200, 1656882000, 1657486800},
	}}
	for _, test := range tests {
		t.Run(fmt.Sprintf("data: %+v", test.data), func(t *testing.T) {
			points := test.data.lod.generateTimePoints(test.data.shift)
			if !cmp.Equal(test.want, points) {
				t.Errorf("result (%v) is not equal to (%v)", points, test.want)
			}
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

			assert.Equal(t, test.want, CalcUTCOffset(loc, test.data.weekStartsAt))
		})
	}
}

func Test_mergeLODs(t *testing.T) {
	tests := []struct {
		name string
		lods []lodInfo
		want []lodInfo
	}{
		{"merge lods", []lodInfo{{fromSec: 0, toSec: 10, stepSec: 1}, {fromSec: 10, toSec: 20, stepSec: 1}}, []lodInfo{{fromSec: 0, toSec: 20, stepSec: 1}}},
		{"skip merge lods", []lodInfo{{fromSec: 0, toSec: 10, stepSec: 2}, {fromSec: 10, toSec: 20, stepSec: 1}}, []lodInfo{{fromSec: 0, toSec: 10, stepSec: 2}, {fromSec: 10, toSec: 20, stepSec: 1}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := mergeLODs(tt.lods); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("mergeLODs() = %v, want %v", got, tt.want)
			}
		})
	}
}
