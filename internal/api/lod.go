// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"math"
	"time"
)

const (
	//   >2h at  1s resolution
	//  >10h at  5s resolution
	// >2.5d at 30s resolution
	//  >26d at  5m resolution
	//  320d at  1h resolution
	maxPoints = 7680 // horizontal resolution of 8K display
	maxSlice  = 8192 // maxPoints next power of 2

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

type lodSwitch struct {
	relSwitch int64 // must be properly aligned
	levels    []int64
	tables    map[int64]string
	hasPreKey bool
}

var (
	lodTables = map[string]map[int64]string{
		Version1: {
			_1M:  _1hTableSH1,
			_7d:  _1hTableSH1,
			_24h: _1hTableSH1,
			_4h:  _1hTableSH1,
			_1h:  _1hTableSH1,
			_15m: _1mTableSH1,
			_5m:  _1mTableSH1,
			_1m:  _1mTableSH1,
		},
		Version2: {
			_1M:  _1hTableSH2,
			_7d:  _1hTableSH2,
			_24h: _1hTableSH2,
			_4h:  _1hTableSH2,
			_1h:  _1hTableSH2,
			_15m: _1mTableSH2,
			_5m:  _1mTableSH2,
			_1m:  _1mTableSH2,
			_15s: _1sTableSH2,
			_5s:  _1sTableSH2,
			_1s:  _1sTableSH2,
		},
	}

	lodLevels = map[string][]lodSwitch{
		Version1: {{
			relSwitch: 33 * _24h,
			levels:    []int64{_7d, _24h, _4h, _1h},
			tables:    lodTables[Version1],
		}, {
			relSwitch: _0s,
			levels:    []int64{_7d, _24h, _4h, _1h, _15m, _5m, _1m},
			tables:    lodTables[Version1],
		}},
		// Subtract from relSwitch to facilitate calculation of derivative.
		// Subtrahend should be multiple of the next lodSwitch minimum level.
		Version2: {{
			relSwitch: 33*_24h - _1m,
			levels:    []int64{_7d, _24h, _4h, _1h},
			tables:    lodTables[Version2],
		}, {
			relSwitch: 52*_1h - _1s,
			levels:    []int64{_7d, _24h, _4h, _1h, _15m, _5m, _1m},
			tables:    lodTables[Version2],
		}, {
			relSwitch: _0s,
			levels:    []int64{_7d, _24h, _4h, _1h, _15m, _5m, _1m, _15s, _5s, _1s},
			tables:    lodTables[Version2],
		}},
	}

	lodLevelsV1StringTop = []lodSwitch{{
		relSwitch: _0s,
		levels:    []int64{_7d, _24h, _4h, _1h},
		tables: map[int64]string{
			_7d:  _1hTableStringTopSH1,
			_24h: _1hTableStringTopSH1,
			_4h:  _1hTableStringTopSH1,
			_1h:  _1hTableStringTopSH1,
		},
	}}

	lodLevelsV1Unique = []lodSwitch{{
		relSwitch: _0s,
		levels:    []int64{_7d, _24h},
		tables: map[int64]string{
			_7d:  _1dTableUniquesSH1,
			_24h: _1dTableUniquesSH1,
		},
	}}

	lodLevelsV2Monthly = []lodSwitch{{
		relSwitch: _0s,
		levels:    []int64{_1M},
		tables: map[int64]string{
			_1M: _1hTableSH2,
		},
	}}

	lodLevelsV1Monthly = []lodSwitch{{
		relSwitch: _0s,
		levels:    []int64{_1M},
		tables: map[int64]string{
			_1M: _1hTableSH1,
		},
	}}

	lodLevelsV1MonthlyUnique = []lodSwitch{{
		relSwitch: _0s,
		levels:    []int64{_1M},
		tables: map[int64]string{
			_1M: _1dTableUniquesSH1,
		},
	}}

	lodLevelsV1MonthlyStringTop = []lodSwitch{{
		relSwitch: _0s,
		levels:    []int64{_1M},
		tables: map[int64]string{
			_1M: _1hTableStringTopSH1,
		},
	}}

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

// resulting range covers every point in input range (which we assume has 1-second step), but is properly aligned
func roundRange(start int64, end int64, step int64, utcOffset int64, location *time.Location) (int64, int64) {
	if step == _1M {
		startTime := time.Unix(start, 0).In(location)
		endTime := time.Unix(end, 0).In(location)

		firstDayOfTheMonth := time.Date(endTime.Year(), endTime.Month(), 1, 0, 0, 0, 0, endTime.Location())

		var moveEndTimeByMonths int
		switch {
		case firstDayOfTheMonth.Unix() == endTime.Unix():
			moveEndTimeByMonths = 1
		default:
			moveEndTimeByMonths = 2
		}

		return time.Date(startTime.Year(), startTime.Month(), 1, 0, 0, 0, 0, startTime.Location()).Unix(),
			firstDayOfTheMonth.AddDate(0, moveEndTimeByMonths, 0).Unix()
	}

	rStart := roundTime(start, step, utcOffset)
	rEnd := roundTime(end, step, utcOffset)
	if end != start {
		for rEnd-step < end-1 {
			rEnd += step
		}
	}
	return rStart, rEnd
}

func calcLevels(version string, preKeyForm int64, isUnique bool, isStringTop bool, now int64, utcOffset int64, width int) []lodSwitch {
	if width == _1M {
		switch {
		case version == Version1:
			switch {
			case isUnique:
				return lodLevelsV1MonthlyUnique
			case isStringTop:
				return lodLevelsV1MonthlyStringTop
			default:
				return lodLevelsV1Monthly
			}
		default:
			return lodLevelsV2Monthly
		}
	}

	if version == Version1 {
		switch {
		case isUnique:
			return lodLevelsV1Unique
		case isStringTop:
			return lodLevelsV1StringTop
		default:
			return lodLevels[version]
		}
	}

	preKeyForm = roundTime(preKeyForm, _1h, utcOffset)
	var levels []lodSwitch
	split := false
	for _, s := range lodLevels[version] {
		cut := now - s.relSwitch
		switch {
		case cut < preKeyForm:
			levels = append(levels, s)
		case !split:
			s1 := s
			s1.relSwitch = now - preKeyForm
			levels = append(levels, s1)
			s2 := s
			s2.hasPreKey = true
			levels = append(levels, s2)
			split = true
		default:
			s2 := s
			s2.hasPreKey = true
			levels = append(levels, s2)
		}
	}
	return levels
}

type lodInfo struct {
	fromSec   int64 // inclusive
	toSec     int64 // exclusive
	stepSec   int64
	table     string // is only here because we can't cleanly deduce it for v1 (unique-related madness etc.)
	hasPreKey bool
	location  *time.Location
}

func (lod lodInfo) generateTimePoints(shift int64) []int64 {
	if lod.stepSec == _1M {
		length := lod.getIndexForTimestamp(lod.toSec, 0)
		times := make([]int64, 0, length)
		startTime := time.Unix(lod.fromSec, 0).In(lod.location)
		shiftMonths := int(shift / _1M * -1)
		for i := 0; i < length; i++ {
			times = append(times, startTime.AddDate(0, i+shiftMonths, 0).Unix())
		}

		return times
	}

	times := make([]int64, 0, (lod.toSec-lod.fromSec)/lod.stepSec)
	for t := lod.fromSec; t < lod.toSec; t += lod.stepSec {
		times = append(times, t-shift)
	}
	return times
}

func (lod lodInfo) getIndexForTimestamp(timestamp, shiftDelta int64) int {
	if lod.stepSec == _1M {
		startTime := time.Unix(lod.fromSec, 0).In(lod.location)
		shiftMonths := int(shiftDelta / _1M * -1)
		currentTime := time.Unix(timestamp, 0).In(lod.location).AddDate(shiftMonths/12, shiftMonths%12, 0)
		monthDiff := int(currentTime.Month() - startTime.Month())
		yearDiff := currentTime.Year() - startTime.Year()
		if monthDiff < 0 {
			monthDiff += 12
			yearDiff--
		}

		return yearDiff*12 + monthDiff
	}

	return int((timestamp - (lod.fromSec + shiftDelta)) / lod.stepSec)
}

func isTimestampValid(timestamp, stepSec, utcOffset int64, location *time.Location) bool {
	if stepSec == _1M {
		timePoint := time.Unix(timestamp, 0).In(location)
		return time.Date(timePoint.Year(), timePoint.Month(), 1, 0, 0, 0, 0, timePoint.Location()).Unix() == timestamp
	}

	return (timestamp+utcOffset)%stepSec == 0
}

func shiftTimestamp(timestamp, stepSec, shift int64, location *time.Location) int64 {
	if stepSec == _1M {
		months := int(shift / _1M)
		return time.Unix(timestamp, 0).In(location).AddDate(months/12, months%12, 0).Unix()
	}

	return timestamp + shift
}

func selectTagValueLODs(version string, preKeyFrom int64, resolution int, isUnique bool, isStringTop bool, now int64, from int64, to int64, utcOffset int64, location *time.Location) []lodInfo {
	return selectQueryLODs(version, preKeyFrom, resolution, isUnique, isStringTop, now, from, to, utcOffset, 100, widthAutoRes, location) // really dumb
}

func selectQueryLODs(version string, preKeyFrom int64, resolution int, isUnique bool, isStringTop bool, now int64, from int64, to int64, utcOffset int64, width int, widthKind int, location *time.Location) []lodInfo {
	var ret []lodInfo
	pps := float64(width) / float64(to-from+1)
	lodFrom := from
	levels := calcLevels(version, preKeyFrom, isUnique, isStringTop, now, utcOffset, width)
	for _, s := range levels {
		cut := now - s.relSwitch
		if cut < lodFrom {
			continue
		}
		lodTo := to
		if cut < to {
			lodTo = cut
		}
		var lod lodInfo
		switch widthKind {
		case widthAutoRes:
			lod = selectQueryLOD(s, lodFrom, lodTo, int64(resolution), utcOffset, location, pps)
		default:
			lod = selectLastQueryLOD(s, lodFrom, lodTo, int64(width), utcOffset, location)
		}
		ret = append(ret, lod)
		if lodTo == to || lod.toSec >= to {
			break
		}
		lodFrom = lod.toSec
	}
	return mergeLods(ret)
}

func mergeLods(lods []lodInfo) []lodInfo {
	if len(lods) == 0 {
		return lods
	}
	ret := lods[:1]
	for _, lod := range lods[1:] {
		l := ret[len(ret)-1]
		if l.toSec == lod.fromSec && l.table == lod.table && l.stepSec == lod.stepSec && l.hasPreKey == lod.hasPreKey {
			ret[len(ret)-1].toSec = lod.toSec
		} else {
			ret = append(ret, lod)
		}
	}
	return ret
}

func selectLastQueryLOD(s lodSwitch, from int64, to int64, minStep int64, utcOffset int64, location *time.Location) lodInfo {
	lodLevel := s.levels[0]
	for _, stepSec := range s.levels {
		fromSec, toSec := roundRange(from, to, stepSec, utcOffset, location)
		n := (toSec - fromSec) / stepSec
		if stepSec < minStep || n > maxPoints {
			break
		}
		lodLevel = stepSec
	}
	fromSec, toSec := roundRange(from, to, lodLevel, utcOffset, location)
	return lodInfo{
		fromSec:   fromSec,
		toSec:     toSec,
		stepSec:   lodLevel,
		table:     s.tables[lodLevel],
		hasPreKey: s.hasPreKey,
		location:  location,
	}
}

func selectQueryLOD(s lodSwitch, from int64, to int64, minStep int64, utcOffset int64, location *time.Location, pps float64) lodInfo {
	lodLevel := s.levels[0]
	points := int64(math.Ceil(pps * float64(to-from)))
	for _, stepSec := range s.levels {
		fromSec, toSec := roundRange(from, to, stepSec, utcOffset, location)
		n := (toSec - fromSec) / stepSec
		if stepSec < minStep || n > maxPoints {
			break
		}
		lodLevel = stepSec // we will get the first level with more points than pixels
		if lodLevel < (to-from) && n > points {
			break
		}
	}
	fromSec, toSec := roundRange(from, to, lodLevel, utcOffset, location)
	return lodInfo{
		fromSec:   fromSec,
		toSec:     toSec,
		stepSec:   lodLevel,
		table:     s.tables[lodLevel],
		hasPreKey: s.hasPreKey,
		location:  location,
	}
}

func CalcUTCOffset(location *time.Location, weekStartsAt time.Weekday) int64 {
	date := time.Unix(0, 0).In(time.UTC)
	weekDay := date.Weekday()
	if weekDay == time.Sunday && weekStartsAt != time.Sunday {
		weekDay = 7
	}

	days := int64(weekDay - weekStartsAt)
	seconds := date.Unix() - time.Date(date.Year(), date.Month(), date.Day(), date.Hour(), date.Minute(), date.Second(), date.Nanosecond(), location).Unix()

	return days*24*3600 + seconds
}
