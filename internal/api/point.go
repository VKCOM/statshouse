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

func selectQueryPoint(version string, preKeyFrom int64, resolution int, isUnique bool, isStringTop bool, now int64, from int64, to int64, utcOffset int64, location *time.Location) *pointQuery {
	var lods []lodInfo
	lods = selectQueryLODs(version, preKeyFrom, resolution, isUnique, isStringTop, now, from, to, utcOffset, resolution, widthLODRes, location)
	lods = mergeForPointQuery(mergeLODs(lods), utcOffset, location)
	if len(lods) == 0 {
		return nil
	}
	lod := lods[0]
	return &pointQuery{
		fromSec:   lod.fromSec,
		toSec:     lod.toSec,
		table:     lod.table,
		hasPreKey: lod.hasPreKey,
		location:  lod.location,
	}
}

func mergeForPointQuery(lods []lodInfo, utcOffset int64, location *time.Location) []lodInfo {
	if len(lods) <= 1 {
		return lods
	}
	hasPreKey := lods[0].hasPreKey
	from := lods[0].fromSec
	to := lods[0].toSec
	for _, lod := range lods {
		hasPreKey = hasPreKey && lod.hasPreKey
		to = lod.toSec
	}
	fromSec, toSec := roundRange(from, to, lods[0].stepSec, utcOffset, location)
	lods = lods[:1]
	lods[0].hasPreKey = hasPreKey
	lods[0].fromSec = fromSec
	lods[0].toSec = toSec
	return lods
}
