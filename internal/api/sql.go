// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/util"
)

type preparedTagValuesQuery struct {
	version     string
	metricID    int32
	preKeyTagID string
	tagID       string
	numResults  int
	filterIn    map[string][]interface{}
	filterNotIn map[string][]interface{}
}

func (q *preparedTagValuesQuery) stringTag() bool {
	return q.tagID == format.StringTopTagID
}

type preparedPointsQuery struct {
	user        string
	version     string
	metricID    int32
	preKeyTagID string
	isStringTop bool
	kind        queryFnKind
	by          []string
	filterIn    map[string][]interface{}
	filterNotIn map[string][]interface{}

	// for table view requests
	orderBy bool
	desc    bool
}

type tagValuesQueryMeta struct {
	stringValue bool
}

func (pq *preparedPointsQuery) isLight() bool {
	return pq.kind != queryFnKindUnique && pq.kind != queryFnKindPercentiles
}

func tagValuesQuery(pq *preparedTagValuesQuery, lod lodInfo) (string, tagValuesQueryMeta, error) {
	meta := tagValuesQueryMeta{}
	valueName := "_value"
	if pq.stringTag() {
		meta.stringValue = true
		valueName = "_string_value"
	}

	// no need to escape anything as long as table and tag names are fixed
	query := fmt.Sprintf(`
SELECT
  %s AS %s, toFloat64(%s(count)) AS _count
FROM
  %s
WHERE
  %s = ?
  AND time >= ? AND time < ?%s`,
		columnName(lod.hasPreKey, pq.tagID, pq.preKeyTagID),
		valueName,
		sqlAggFn(pq.version, "sum"),
		pq.preKeyTableName(lod),
		metricColumn(pq.version),
		datePredicate(pq.version),
	)
	args := []interface{}{pq.metricID, lod.fromSec, lod.toSec}
	if pq.version == Version1 {
		args = append(args, lod.fromSec, lod.toSec)
	}
	for k, ids := range pq.filterIn {
		if len(ids) > 0 {
			query += fmt.Sprintf(`
  AND %s IN (%s)`, columnName(lod.hasPreKey, k, pq.preKeyTagID), expandBindVars(len(ids)))
			args = append(args, ids...)
		} else {
			query += `
  AND 1=0`
		}
	}
	for k, ids := range pq.filterNotIn {
		if len(ids) > 0 {
			query += fmt.Sprintf(`
  AND %s NOT IN (%s)`, columnName(lod.hasPreKey, k, pq.preKeyTagID), expandBindVars(len(ids)))
			args = append(args, ids...)
		} else {
			query += `
  AND 1=1`
		}
	}
	query += fmt.Sprintf(`
GROUP BY
  %s
ORDER BY
  _count DESC,
  %s
LIMIT %v
SETTINGS
  optimize_aggregation_in_order = 1
`, columnName(lod.hasPreKey, pq.tagID, pq.preKeyTagID), valueName, pq.numResults+1) // +1 so we can set "more":true

	q, err := util.BindQuery(query, args...)
	return q, meta, err
}

type pointsQueryMeta struct {
	vals    int
	tags    []string
	maxHost bool
	version string
}

func loadPointsSelectWhat(pq *preparedPointsQuery) (string, int, error) {
	var (
		version     = pq.version
		isStringTop = pq.isStringTop
		kind        = pq.kind
	)
	if version == Version1 && isStringTop {
		return `
  toFloat64(sumMerge(count)) AS _count`, 0, nil // count is the only column available
	}

	switch kind {
	case queryFnKindCount:
		return fmt.Sprintf(`
  toFloat64(%s(count)) AS _count,
  toFloat64(sum(1)) AS _val0,
  toFloat64(%s(max)) AS _val1`, sqlAggFn(version, "sum"), sqlAggFn(version, "max")), 2, nil
	case queryFnKindValue:
		return fmt.Sprintf(`
  toFloat64(%s(count)) AS _count,
  toFloat64(%s(min)) AS _val0,
  toFloat64(%s(max)) AS _val1,
  toFloat64(%s(sum))/toFloat64(%s(count)) AS _val2,
  toFloat64(%s(sum)) AS _val3,
  if(%s(count) < 2, 0, sqrt(greatest(   (%s(sumsquare) - pow(%s(sum), 2) / %s(count)) / (%s(count) - 1)   , 0))) AS _val4,
  toFloat64(sum(1)) AS _val5,
  %s as _maxHost`,
			sqlAggFn(version, "sum"),
			sqlAggFn(version, "min"),
			sqlAggFn(version, "max"),
			sqlAggFn(version, "sum"), sqlAggFn(version, "sum"),
			sqlAggFn(version, "sum"),
			// https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance, "Naïve algorithm", poor numeric stability
			sqlAggFn(version, "sum"), sqlAggFn(version, "sum"), sqlAggFn(version, "sum"), sqlAggFn(version, "sum"), sqlAggFn(version, "sum"),
			sqlMaxHost(version)), 6, nil
	case queryFnKindPercentilesLow:
		return fmt.Sprintf(`
	  toFloat64(%s(count)) AS _count,
	  toFloat64((quantilesTDigestMerge(0.001, 0.01, 0.05, 0.1)(percentiles) AS digest)[1]) AS _val0,
	  toFloat64(digest[2]) AS _val1,
	  toFloat64(digest[3]) AS _val2,
	  toFloat64(digest[4]) AS _val3,
	  toFloat64(0) AS _val4,
	  toFloat64(0) AS _val5,
	  toFloat64(0) AS _val6,
	  %s as _maxHost`,
			sqlAggFn(version, "sum"), sqlMaxHost(version)), 7, nil
	case queryFnKindPercentiles:
		return fmt.Sprintf(`
  toFloat64(%s(count)) AS _count,
  toFloat64((quantilesTDigestMerge(0.25, 0.5, 0.75, 0.90, 0.95, 0.99, 0.999)(percentiles) AS digest)[1]) AS _val0,
  toFloat64(digest[2]) AS _val1,
  toFloat64(digest[3]) AS _val2,
  toFloat64(digest[4]) AS _val3,
  toFloat64(digest[5]) AS _val4,
  toFloat64(digest[6]) AS _val5,
  toFloat64(digest[7]) AS _val6,
  %s as _maxHost`,
			sqlAggFn(version, "sum"), sqlMaxHost(version)), 7, nil
	case queryFnKindUnique:
		return fmt.Sprintf(`
  toFloat64(%s(count)) AS _count,
  toFloat64(uniqMerge(uniq_state)) AS _val0,
  %s as _maxHost`,
			sqlAggFn(version, "sum"), sqlMaxHost(version)), 1, nil
	default:
		return "", 0, fmt.Errorf("unsupported operation kind: %q", kind)
	}
}

func loadPointsQuery(pq *preparedPointsQuery, lod lodInfo, utcOffset int64) (string, pointsQueryMeta, error) {
	what, cnt, err := loadPointsSelectWhat(pq)
	if err != nil {
		return "", pointsQueryMeta{}, err
	}

	var commaBy string
	if len(pq.by) > 0 {
		for _, b := range pq.by {
			commaBy += fmt.Sprintf(", %s AS key%s", columnName(lod.hasPreKey, b, pq.preKeyTagID), b)
		}
	}

	var timeInterval string
	if lod.stepSec == _1M {
		timeInterval = fmt.Sprintf(`
toInt64(toDateTime(toStartOfInterval(time, INTERVAL 1 MONTH, '%s'), '%s')) AS _time, 
toInt64(toDateTime(_time, '%s') + INTERVAL 1 MONTH) - _time AS _stepSec`, lod.location.String(), lod.location.String(), lod.location.String())
	} else {
		timeInterval = fmt.Sprintf(`
toInt64(toStartOfInterval(time + %d, INTERVAL %d second)) - %d AS _time,
toInt64(%d) AS _stepSec`, utcOffset, lod.stepSec, utcOffset, lod.stepSec)
	}

	// no need to escape anything as long as table and tag names are fixed
	query := fmt.Sprintf(`
SELECT
  %s%s, %s
FROM
  %s
WHERE
  %s = ?
  AND time >= ? AND time < ?%s`,
		timeInterval,
		commaBy,
		what,
		pq.preKeyTableName(lod),
		metricColumn(pq.version),
		datePredicate(pq.version),
	)
	args := []interface{}{pq.metricID, lod.fromSec, lod.toSec}
	if pq.version == Version1 {
		args = append(args, lod.fromSec, lod.toSec)
	}
	for k, ids := range pq.filterIn {
		if len(ids) > 0 {
			query += fmt.Sprintf(`
  AND %s IN (%s)`, columnName(lod.hasPreKey, k, pq.preKeyTagID), expandBindVars(len(ids)))
			args = append(args, ids...)
		} else {
			query += `
  AND 1=0`
		}
	}
	for k, ids := range pq.filterNotIn {
		if len(ids) > 0 {
			query += fmt.Sprintf(`
  AND %s NOT IN (%s)`, columnName(lod.hasPreKey, k, pq.preKeyTagID), expandBindVars(len(ids)))
			args = append(args, ids...)
		} else {
			query += `
  AND 1=1`
		}
	}

	query += fmt.Sprintf(`
GROUP BY
  _time%s`, commaBy)

	limit := maxSeriesRows
	if pq.orderBy {
		limit = maxTableRows
		desc := ""
		if pq.desc {
			desc = " DESC"
		}
		query += fmt.Sprintf(`
ORDER BY
  _time%s%s`, commaBy, desc)
	}
	query += fmt.Sprintf(`
LIMIT %v
SETTINGS
  optimize_aggregation_in_order = 1
`, limit)
	q, err := util.BindQuery(query, args...)
	return q, pointsQueryMeta{vals: cnt, tags: pq.by, maxHost: pq.kind != queryFnKindCount, version: pq.version}, err
}

func loadPointQuery(pq *preparedPointsQuery, pointQuery pointQuery, utcOffset int64) (string, pointsQueryMeta, error) {
	what, cnt, err := loadPointsSelectWhat(pq)
	if err != nil {
		return "", pointsQueryMeta{}, err
	}

	var commaBy string
	if len(pq.by) > 0 {
		for i, b := range pq.by {
			if i == 0 {
				commaBy += fmt.Sprintf("%s AS key%s", columnName(pointQuery.hasPreKey, b, pq.preKeyTagID), b)
			} else {
				commaBy += fmt.Sprintf(", %s AS key%s", columnName(pointQuery.hasPreKey, b, pq.preKeyTagID), b)
			}
		}
	}

	commaBySelect := commaBy
	if commaBy != "" {
		commaBySelect += ","
	}
	// no need to escape anything as long as table and tag names are fixed
	query := fmt.Sprintf(`
SELECT
  %s %s
FROM
  %s
WHERE
  %s = ?
  AND time >= ? AND time < ?%s`,
		commaBySelect,
		what,
		preKeyTableNameFromPoint(pointQuery, "", pq.preKeyTagID, pq.filterIn, pq.filterNotIn),
		metricColumn(pq.version),
		datePredicate(pq.version),
	)
	args := []interface{}{pq.metricID, pointQuery.fromSec, pointQuery.toSec}
	if pq.version == Version1 {
		args = append(args, pointQuery.fromSec, pointQuery.toSec)
	}
	for k, ids := range pq.filterIn {
		if len(ids) > 0 {
			query += fmt.Sprintf(`
  AND %s IN (%s)`, columnName(pointQuery.hasPreKey, k, pq.preKeyTagID), expandBindVars(len(ids)))
			args = append(args, ids...)
		} else {
			query += `
  AND 1=0`
		}
	}
	for k, ids := range pq.filterNotIn {
		if len(ids) > 0 {
			query += fmt.Sprintf(`
  AND %s NOT IN (%s)`, columnName(pointQuery.hasPreKey, k, pq.preKeyTagID), expandBindVars(len(ids)))
			args = append(args, ids...)
		} else {
			query += `
  AND 1=1`
		}
	}

	if commaBy != "" {
		query += fmt.Sprintf(`
GROUP BY %s
`, commaBy)
	}
	query += fmt.Sprintf(`
LIMIT %v
SETTINGS
  optimize_aggregation_in_order = 1
`, maxSeriesRows)
	q, err := util.BindQuery(query, args...)
	return q, pointsQueryMeta{vals: cnt, tags: pq.by, maxHost: pq.kind != queryFnKindCount, version: pq.version}, err
}

func sqlAggFn(version string, fn string) string {
	if version == Version1 {
		return fn + "Merge"
	}
	return fn
}

func sqlMaxHost(version string) string {
	if version == Version1 {
		return "0"
	}
	return "argMaxMerge(max_host)"
}

func metricColumn(version string) string {
	if version == Version1 {
		return "stats"
	}
	return "metric"
}

func datePredicate(version string) string {
	if version == Version1 {
		return " AND date >= toDate(?) AND date <= toDate(?)"
	}
	return ""
}

type stringFixed [format.MaxStringLen]byte

func (s *stringFixed) UnmarshalBinary(data []byte) error {
	copy(s[:], data)
	return nil
}

func (s *stringFixed) String() string {
	nullIx := bytes.IndexByte(s[:], 0)
	switch nullIx {
	case 0:
		return ""
	case -1:
		return string(s[:])
	default:
		return string(s[:nullIx])
	}
}

func (pq *preparedPointsQuery) preKeyTableName(lod lodInfo) string {
	var usePreKey bool
	if lod.hasPreKey {
		usePreKey = lod.preKeyOnly ||
			len(pq.filterIn[pq.preKeyTagID]) > 0 ||
			len(pq.filterNotIn[pq.preKeyTagID]) > 0
		if !usePreKey {
			for _, v := range pq.by {
				if v == pq.preKeyTagID {
					usePreKey = true
					break
				}
			}
		}
	}
	if usePreKey {
		return preKeyTableNames[lod.table]
	}
	return lod.table
}

func (pq *preparedTagValuesQuery) preKeyTableName(lod lodInfo) string {
	usePreKey := (lod.hasPreKey &&
		(lod.preKeyOnly ||
			(pq.tagID != "" && pq.tagID == pq.preKeyTagID) ||
			len(pq.filterIn[pq.preKeyTagID]) > 0 ||
			len(pq.filterNotIn[pq.preKeyTagID]) > 0))
	if usePreKey {
		return preKeyTableNames[lod.table]
	}
	return lod.table
}

func preKeyTableNameFromPoint(point pointQuery, tagID string, preKeyTagID string, filterIn map[string][]interface{}, filterNotIn map[string][]interface{}) string {
	usePreKey := point.hasPreKey && ((tagID != "" && tagID == preKeyTagID) || len(filterIn[preKeyTagID]) > 0 || len(filterNotIn[preKeyTagID]) > 0)
	if usePreKey {
		return preKeyTableNames[point.table]
	}
	return point.table
}

func columnName(hasPreKey bool, tagID string, preKeyTagID string) string {
	// intentionally not using constants from 'format' package,
	// because it is a table column name, not an external contract
	switch tagID {
	case format.StringTopTagID:
		return "skey"
	case format.ShardTagID:
		return "_shard_num"
	default:
		if hasPreKey && tagID == preKeyTagID {
			return "prekey"
		}
		// 'tagID' assumed to be a number from 0 to 15,
		// dont't verify (ClickHouse just won't find a column)
		return "key" + tagID
	}
}

func expandBindVars(n int) string {
	var b strings.Builder
	for i := 0; i < n; i++ {
		b.WriteByte('?')
		if i < n-1 {
			b.WriteString(", ")
		}
	}
	return b.String()
}
