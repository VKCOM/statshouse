// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"math"
	"time"

	"github.com/vkcom/statshouse/internal/api/dac"
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

func calcLevels(version string, preKeyFrom int64, preKeyOnly bool, isUnique bool, isStringTop bool, now int64, utcOffset int64, width int) []dac.LodSwitch {
	if width == _1M {
		switch {
		case version == Version1:
			switch {
			case isUnique:
				return dac.LodLevelsV1MonthlyUnique
			case isStringTop:
				return dac.LodLevelsV1MonthlyStringTop
			default:
				return dac.LodLevelsV1Monthly
			}
		default:
			return dac.LodLevelsV2Monthly
		}
	}

	if version == Version1 {
		switch {
		case isUnique:
			return dac.LodLevelsV1Unique
		case isStringTop:
			return dac.LodLevelsV1StringTop
		default:
			return dac.LodLevels[version]
		}
	}

	if preKeyFrom != 0 {
		preKeyFrom = roundTime(preKeyFrom, _1h, utcOffset)
	} else {
		preKeyFrom = math.MaxInt64 // "cut < preKeyFrom" is always false
	}
	var levels []dac.LodSwitch
	split := false
	for _, s := range dac.LodLevels[version] {
		cut := now - s.RelSwitch
		switch {
		case cut < preKeyFrom:
			levels = append(levels, s)
		case !split:
			s1 := s
			s1.RelSwitch = now - preKeyFrom
			levels = append(levels, s1)
			s2 := s
			s2.HasPreKey = true
			s2.PreKeyOnly = preKeyOnly
			levels = append(levels, s2)
			split = true
		default:
			s2 := s
			s2.HasPreKey = true
			s2.PreKeyOnly = preKeyOnly
			levels = append(levels, s2)
		}
	}
	return levels
}

func shiftTimestamp(timestamp, stepSec, shift int64, location *time.Location) int64 {
	if stepSec == _1M {
		months := int(shift / _1M)
		return time.Unix(timestamp, 0).In(location).AddDate(months/12, months%12, 0).Unix()
	}

	return timestamp + shift
}

func selectTagValueLODs(version string, preKeyFrom int64, preKeyOnly bool, resolution int, isUnique bool, isStringTop bool, now int64, from int64, to int64, utcOffset int64, location *time.Location) []dac.LodInfo {
	return selectQueryLODs(version, preKeyFrom, preKeyOnly, resolution, isUnique, isStringTop, now, from, to, utcOffset, 100, widthAutoRes, location) // really dumb
}

func selectQueryLODs(version string, preKeyFrom int64, preKeyOnly bool, resolution int, isUnique bool, isStringTop bool, now int64, from int64, to int64, utcOffset int64, width int, widthKind int, location *time.Location) []dac.LodInfo {
	var ret []dac.LodInfo
	var pps float64
	var minStep int
	if widthKind == widthAutoRes {
		minStep = resolution
		pps = float64(width) / float64(to-from+1)
	} else {
		minStep = width
	}
	lodFrom := from
	levels := calcLevels(version, preKeyFrom, preKeyOnly, isUnique, isStringTop, now, utcOffset, width)
	for _, s := range levels {
		cut := now - s.RelSwitch
		if cut < lodFrom {
			continue
		}
		lodTo := to
		if cut < to {
			lodTo = cut
		}
		lod := selectQueryLOD(s, lodFrom, lodTo, int64(minStep), utcOffset, location, pps)
		ret = append(ret, lod)
		if lodTo == to || lod.ToSec >= to {
			break
		}
		lodFrom = lod.ToSec
	}
	return mergeLODs(ret)
}

func mergeLODs(lods []dac.LodInfo) []dac.LodInfo {
	if len(lods) == 0 {
		return lods
	}
	ret := lods[:1]
	for _, lod := range lods[1:] {
		l := ret[len(ret)-1]
		if l.ToSec == lod.FromSec && l.Table == lod.Table && l.StepSec == lod.StepSec && l.HasPreKey == lod.HasPreKey && l.PreKeyOnly == lod.PreKeyOnly {
			ret[len(ret)-1].ToSec = lod.ToSec
		} else {
			ret = append(ret, lod)
		}
	}
	return ret
}

func selectQueryLOD(s dac.LodSwitch, from int64, to int64, minStep int64, utcOffset int64, location *time.Location, pps float64) dac.LodInfo {
	lodLevel := s.Levels[0]
	points := int64(math.Ceil(pps * float64(to-from)))
	for _, stepSec := range s.Levels {
		fromSec, toSec := roundRange(from, to, stepSec, utcOffset, location)
		n := (toSec - fromSec) / stepSec
		if stepSec < minStep || n > maxPoints {
			break
		}
		lodLevel = stepSec
		if pps != 0 && lodLevel < (to-from) && n > points {
			break // on the first level with more points than pixels
		}
	}
	fromSec, toSec := roundRange(from, to, lodLevel, utcOffset, location)
	return dac.LodInfo{
		FromSec:    fromSec,
		ToSec:      toSec,
		StepSec:    lodLevel,
		Table:      s.Tables[lodLevel],
		HasPreKey:  s.HasPreKey,
		PreKeyOnly: s.PreKeyOnly,
		Location:   location,
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
