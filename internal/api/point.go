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

func selectQueryPoint(q *query, version string, preKeyFrom int64, resolution int, isUnique bool, isStringTop bool, now int64, from int64, to int64, utcOffset int64, location *time.Location) []pointQuery {
	var lods []lodInfo
	lodFrom := from
	levels := calcLevels(version, preKeyFrom, isUnique, isStringTop, now, utcOffset, -1)
	for _, s := range levels {
		cut := now - s.relSwitch
		if cut < lodFrom {
			continue
		}
		lodTo := to
		if cut < to {
			lodTo = cut
		}
		lod := selectLastQueryLOD(s, lodFrom, lodTo, int64(resolution), utcOffset, location)
		lods = append(lods, lod)
		if lodTo == to || lod.toSec >= to {
			break
		}
		lodFrom = lod.toSec
	}
	lods = mergeForPointQuery(q, mergeLODs(lods), utcOffset, location)
	ret := make([]pointQuery, 0, len(lods))
	for _, lod := range lods {
		ret = append(ret, pointQuery{
			fromSec:   lod.fromSec,
			toSec:     lod.toSec,
			table:     lod.table,
			hasPreKey: lod.hasPreKey,
			location:  lod.location,
		})
	}
	return ret
}

func mergeForPointQuery(q *query, lods []lodInfo, utcOffset int64, location *time.Location) []lodInfo {
	if len(lods) <= 1 {
		return lods
	}
	if canMergeResult(q) {
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

func canMergeResult(q *query) bool {
	return q.what == queryFnCount || q.what == queryFnSum
}
