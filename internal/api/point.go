package api

import (
	"time"
)

type pointQuery struct {
	fromSec   int64 // inclusive
	toSec     int64 // exclusive
	table     string
	hasPreKey bool
	location  *time.Location
}

func (pq pointQuery) isFast() bool {
	return pq.fromSec+fastQueryTimeInterval >= pq.toSec
}

func selectQueryPoint(version string, preKeyFrom int64, preKeyOnly bool, resolution int, isUnique bool, isStringTop bool, now int64, from int64, to int64, utcOffset int64, excessPoints bool, location *time.Location) *pointQuery {
	lods := selectQueryLODs(version, preKeyFrom, preKeyOnly, resolution, isUnique, isStringTop, now, from, to, utcOffset, resolution, widthLODRes, location)
	lod := mergeForPointQuery(from, to, mergeLODs(lods), utcOffset, excessPoints)
	if len(lods) == 0 {
		return nil
	}
	return &pointQuery{
		fromSec:   lod.fromSec,
		toSec:     lod.toSec,
		table:     lod.table,
		hasPreKey: lod.hasPreKey,
		location:  lod.location,
	}
}

func roundRangeForPoint(start int64, end int64, step int64, utcOffset int64, round bool) (int64, int64) {
	rStart := roundTime(start, step, utcOffset)
	rEnd := roundTime(end, step, utcOffset)
	if end != start {
		if rStart < start && !round {
			rStart += step
		}
		if rEnd < end && round {
			rEnd += step
		}
	}
	return rStart, rEnd
}

func mergeForPointQuery(reqFrom, reqTo int64, lods []lodInfo, utcOffset int64, excessPoints bool) lodInfo {
	hasPreKey := lods[0].hasPreKey
	preKeyOnly := lods[0].preKeyOnly
	for _, lod := range lods {
		hasPreKey = hasPreKey && lod.hasPreKey
	}
	fromSec, toSec := roundRangeForPoint(reqFrom, reqTo, lods[0].stepSec, utcOffset, excessPoints)
	lod := lods[0]
	lod.hasPreKey = hasPreKey
	lod.preKeyOnly = preKeyOnly
	lod.fromSec = fromSec
	lod.toSec = toSec
	return lod
}
