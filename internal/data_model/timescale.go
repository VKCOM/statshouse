// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package data_model

import (
	"fmt"
	"time"

	"github.com/vkcom/statshouse/internal/format"
)

const (
	Version1 = "1"
	Version2 = "2"
	Version3 = "3"

	//   >2h at  1s resolution
	//  >10h at  5s resolution
	// >2.5d at 30s resolution
	//  >26d at  5m resolution
	//  320d at  1h resolution
	maxPoints = 7680 // horizontal resolution of 8K display
	MaxSlice  = 8192 // maxPoints next power of 2

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

	_1sTableSH3 = "statshouse_v3_1s_dist"
	_1mTableSH3 = "statshouse_v3_1m_dist"
	_1hTableSH3 = "statshouse_v3_1h_dist"

	fastQueryTimeInterval = (86400 + 3600) * 2
)

const (
	RangeQuery QueryMode = iota
	InstantQuery
	PointQuery
	TagsQuery
)

type Timescale struct {
	Version    string
	Location   *time.Location
	UTCOffset  int64
	Time       []int64
	LODs       []TimescaleLOD
	Step       int64 // aggregation interval requested (former "desiredStepMul")
	StartX     int   // requested time interval starts at "Time[StartX]"
	ViewStartX int
	ViewEndX   int
}

type TimescaleLOD struct {
	Step int64
	Len  int // number of elements LOD occupies in time array
}

type QueryMode int

type QueryStat struct {
	MetricOffset    map[*format.MetricMetaValue]int64
	MaxMetricOffset int64
	MaxMetricRes    int64 // max metric resolution
	HasStringTop    bool
	HasUnique       bool
}

type GetTimescaleArgs struct {
	QueryStat
	Version     string
	Start       int64 // inclusive
	End         int64 // exclusive
	Step        int64
	TimeNow     int64
	ScreenWidth int64
	Mode        QueryMode
	Extend      bool
	Metric      *format.MetricMetaValue
	Offset      int64
	Location    *time.Location
	UTCOffset   int64
}

type LOD struct {
	FromSec    int64 // inclusive
	ToSec      int64 // exclusive
	StepSec    int64
	Table      string // is only here because we can't cleanly deduce it for v1 (unique-related madness etc.)
	HasPreKey  bool
	PreKeyOnly bool
	Location   *time.Location
}

type lodSwitch struct {
	relSwitch int64 // must be properly aligned
	levels    []int64
	tables    map[int64]string
}

var (
	LODTables = map[string]map[int64]string{
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
		Version3: {
			_1M:  _1hTableSH3,
			_7d:  _1hTableSH3,
			_24h: _1hTableSH3,
			_4h:  _1hTableSH3,
			_1h:  _1hTableSH3,
			_15m: _1mTableSH3,
			_5m:  _1mTableSH3,
			_1m:  _1mTableSH3,
			_15s: _1sTableSH3,
			_5s:  _1sTableSH3,
			_1s:  _1sTableSH3,
		},
	}

	lodLevels = map[string][]lodSwitch{
		Version1: {{
			relSwitch: 33 * _24h,
			levels:    []int64{_7d, _24h, _4h, _1h},
			tables:    LODTables[Version1],
		}, {
			relSwitch: _0s,
			levels:    []int64{_7d, _24h, _4h, _1h, _15m, _5m, _1m},
			tables:    LODTables[Version1],
		}},
		// Subtract from relSwitch to facilitate calculation of derivative.
		// Subtrahend should be multiple of the next lodSwitch minimum level.
		Version2: {{
			relSwitch: 33*_24h - 2*_1m,
			levels:    []int64{_7d, _24h, _4h, _1h},
			tables:    LODTables[Version2],
		}, {
			relSwitch: 52*_1h - 2*_1s,
			levels:    []int64{_7d, _24h, _4h, _1h, _15m, _5m, _1m},
			tables:    LODTables[Version2],
		}, {
			relSwitch: _0s,
			levels:    []int64{_7d, _24h, _4h, _1h, _15m, _5m, _1m, _15s, _5s, _1s},
			tables:    LODTables[Version2],
		}},
		Version3: {{
			relSwitch: 33*_24h - 2*_1m,
			levels:    []int64{_7d, _24h, _4h, _1h},
			tables:    LODTables[Version3],
		}, {
			relSwitch: 52*_1h - 2*_1s,
			levels:    []int64{_7d, _24h, _4h, _1h, _15m, _5m, _1m},
			tables:    LODTables[Version3],
		}, {
			relSwitch: _0s,
			levels:    []int64{_7d, _24h, _4h, _1h, _15m, _5m, _1m, _15s, _5s, _1s},
			tables:    LODTables[Version3],
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
)

var errQueryOutOfRange = fmt.Errorf("exceeded maximum resolution of %d points per timeseries", MaxSlice)

func GetTimescale(args GetTimescaleArgs) (Timescale, error) {
	if args.End <= args.Start || args.Step < 0 {
		return Timescale{}, nil
	}
	// gather query info
	var (
		maxOffset    int
		maxMetricRes int64 = 1 // max metric resolution
		hasStringTop bool
		hasUnique    bool
	)
	for k, v := range args.MetricOffset {
		if maxOffset < int(v) {
			maxOffset = int(v)
		}
		if maxMetricRes < int64(k.Resolution) {
			maxMetricRes = int64(k.Resolution)
		}
		if len(k.StringTopDescription) != 0 {
			hasStringTop = true
		}
	}
	var (
		levels  []lodSwitch // depends on query and version
		version = VersionOrDefault(args.Version)
	)
	// find appropriate LOD table
	switch {
	case args.Step == _1M:
		switch {
		case version == Version1:
			switch {
			case hasUnique:
				levels = lodLevelsV1MonthlyUnique
			case hasStringTop:
				levels = lodLevelsV1MonthlyStringTop
			default:
				levels = lodLevelsV1Monthly
			}
		default:
			levels = lodLevelsV2Monthly
		}
	case version == Version1:
		switch {
		case hasUnique:
			levels = lodLevelsV1Unique
		case hasStringTop:
			levels = lodLevelsV1StringTop
		default:
			levels = lodLevels[version]
		}
	default:
		levels = lodLevels[version]
	}
	// generate LODs
	var minStep int64
	pointQuery := args.Mode == PointQuery
	if pointQuery {
		minStep = maxMetricRes
	} else {
		if 0 < args.Step {
			minStep = args.Step
		} else {
			minStep = maxMetricRes
		}
	}
	start := args.Start - int64(maxOffset)
	end := args.End - int64(maxOffset)
	res := Timescale{
		Version:   version,
		Location:  args.Location,
		UTCOffset: args.UTCOffset,
		Step:      args.Step,
	}
	var resLen int
	var lod TimescaleLOD // last LOD
	for i := 0; i < len(levels) && start < end; i++ {
		edge := args.TimeNow - levels[i].relSwitch
		if edge < start {
			continue
		}
		if end < edge || pointQuery {
			edge = end
		}
		lod.Len = 0      // reset LOD length, keep last step
		var lodEnd int64 // next "start"
		for _, step := range levels[i].levels {
			if 0 < lod.Step && lod.Step < step {
				continue // step can not grow
			}
			lodStart := start
			if len(res.LODs) == 0 {
				lodStart = startOfLOD(start, step, args.Location, args.UTCOffset)
			}
			// calculate number of points up to the "edge"
			var lodLen, n int
			lodEnd, lodLen = endOfLOD(lodStart, step, edge, false, args.Location)
			if !pointQuery {
				// plus up to the query and to ensure current "step" does not exceed "maxPoints" limit
				_, m := endOfLOD(lodEnd, step, end, false, args.Location)
				n = resLen + lodLen + m
				if maxPoints < n {
					// "maxPoints" limit exceed
					if lod.Step == 0 {
						// at largest "step" possible
						return Timescale{}, errQueryOutOfRange
					}
					// use previous (larger) "step" to the end
					if len(res.LODs) == 0 {
						lodStart = startOfLOD(start, lod.Step, args.Location, args.UTCOffset)
					}
					lodEnd, lod.Len = endOfLOD(lodStart, lod.Step, end, false, args.Location)
					break
				}
			}
			lod = TimescaleLOD{Step: step, Len: lodLen}
			if step <= minStep || (args.ScreenWidth != 0 && int(args.ScreenWidth) < n) {
				// use current "step" to the end
				lodEnd, lodLen = endOfLOD(lodEnd, step, end, false, args.Location)
				lod.Len += lodLen
				break
			}
		}
		if lod.Step <= 0 || lod.Step > _1M || lod.Len <= 0 || !(pointQuery || lod.Len <= maxPoints) {
			// should not happen
			return Timescale{}, fmt.Errorf("LOD out of range: step=%d, len=%d", lod.Step, lod.Len)
		}
		start = lodEnd
		resLen += lod.Len
		if len(res.LODs) != 0 && res.LODs[len(res.LODs)-1].Step == lod.Step {
			res.LODs[len(res.LODs)-1].Len += lod.Len
		} else {
			res.LODs = append(res.LODs, lod)
		}
	}
	if len(res.LODs) == 0 {
		return Timescale{}, nil
	}
	// verify offset is multiple of largest LOD step
	for _, v := range args.MetricOffset {
		if v%res.LODs[0].Step != 0 {
			return Timescale{}, fmt.Errorf("offset %d is not multiple of step %d", v, res.LODs[0].Step)
		}
	}
	// generate time
	p := &res.LODs[0]
	t := startOfLOD(args.Start, p.Step, args.Location, args.UTCOffset)
	if pointQuery {
		if t < args.Start && !args.Extend {
			t = StepForward(t, p.Step, args.Location)
		}
		res.Time = []int64{t, 0}
		res.Time[1], _ = endOfLOD(t, p.Step, args.End, !args.Extend, args.Location)
		if res.Time[0] == res.Time[1] {
			return Timescale{}, nil
		}
		res.ViewEndX = 1
	} else {
		if t < args.Start {
			if !args.Extend {
				res.StartX++
			}
			res.ViewStartX++
		} else if args.Extend {
			t = startOfLOD(t-1, p.Step, args.Location, args.UTCOffset)
			p.Len++
			res.ViewStartX++
		}
		if res.StartX == 0 {
			t = startOfLOD(t-1, p.Step, args.Location, args.UTCOffset)
			p.Len++
			res.StartX++
			res.ViewStartX++
		}
		resLen += 3 // account all possible extensions
		res.Time = make([]int64, 0, resLen)
		for i := range res.LODs {
			p = &res.LODs[i]
			for j := 0; j < p.Len; j++ {
				res.Time = append(res.Time, t)
				t = StepForward(t, p.Step, args.Location)
			}
		}
		if res.ViewStartX < len(res.Time) {
			res.ViewEndX = len(res.Time)
		} else {
			res.ViewEndX = res.ViewStartX
		}
		if args.Extend {
			res.Time = append(res.Time, t) // last "StepForward" result
			p.Len++
		}
	}
	return res, nil
}

func GetLODs(args GetTimescaleArgs) ([]LOD, error) {
	args.QueryStat.Add(args.Metric, args.Offset)
	t, err := GetTimescale(args)
	if err != nil {
		return nil, err
	}
	return t.GetLODs(args.Metric, args.Offset), nil
}

func (t *Timescale) GetLODs(metric *format.MetricMetaValue, offset int64) []LOD {
	if len(t.Time) == 0 {
		return nil
	}
	start := t.Time[0]
	if offset != 0 {
		start = startOfLOD(start-offset, t.LODs[0].Step, t.Location, t.UTCOffset)
	}
	res := make([]LOD, 0, len(t.LODs))
	for _, lod := range t.LODs {
		end := start
		for i := 0; i < lod.Len; i++ {
			end = StepForward(end, lod.Step, t.Location)
		}
		res = append(res, LOD{
			FromSec:    start,
			ToSec:      end,
			StepSec:    lod.Step,
			Table:      LODTables[t.Version][lod.Step],
			HasPreKey:  metric.PreKeyOnly || (metric.PreKeyFrom != 0 && int64(metric.PreKeyFrom) <= start),
			PreKeyOnly: metric.PreKeyOnly,
			Location:   t.Location,
		})
		start = end
	}
	return res
}

func (t *Timescale) Empty() bool {
	return t.StartX == len(t.Time)
}

func (lod LOD) IndexOf(timestamp int64) (int, error) {
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

func (lod LOD) IsFast() bool {
	return lod.FromSec+fastQueryTimeInterval >= lod.ToSec
}

func (s *QueryStat) Add(m *format.MetricMetaValue, offset int64) {
	if s.MetricOffset == nil {
		s.MetricOffset = make(map[*format.MetricMetaValue]int64)
	}
	curOffset, ok := s.MetricOffset[m]
	if !ok || curOffset < offset {
		if s.MaxMetricOffset < offset {
			s.MaxMetricOffset = offset
		}
		s.MetricOffset[m] = offset
	}
	if s.MaxMetricRes < int64(m.Resolution) {
		s.MaxMetricRes = int64(m.Resolution)
	}
	if len(m.StringTopDescription) != 0 {
		s.HasStringTop = true
	}
	if m.Kind == format.MetricKindUnique {
		s.HasUnique = true
	}
}

func StepForward(start, step int64, loc *time.Location) int64 {
	if step == _1M {
		return time.Unix(start, 0).In(loc).AddDate(0, 1, 0).UTC().Unix()
	} else {
		return start + step
	}
}

func startOfLOD(start, step int64, loc *time.Location, utcOffset int64) int64 {
	if step == _1M {
		t := time.Unix(start, 0).In(loc)
		return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, loc).UTC().Unix()
	} else {
		return roundTime(start, step, utcOffset)
	}
}

func endOfLOD(start, step, end int64, le bool, loc *time.Location) (int64, int) {
	if step <= 0 {
		// infinite loop guard
		panic(fmt.Errorf("negative step not allowed: %v", step))
	}
	n := 0
	for ; start < end; n++ {
		t := StepForward(start, step, loc)
		if le && end < t {
			break
		}
		start = t
	}
	return start, n
}

func roundTime(t int64, step int64, utcOffset int64) int64 {
	return mathDiv(t+utcOffset, step)*step - utcOffset
}

func mathDiv(a int64, b int64) int64 {
	quo := a / b
	if (a >= 0) == (b >= 0) || a%b == 0 {
		return quo
	}
	return quo - 1
}

func VersionOrDefault(version string) string {
	if len(version) != 0 {
		return version
	}
	return Version2
}
