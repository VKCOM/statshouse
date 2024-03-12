// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package dac

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/vkcom/statshouse/internal/api/model"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/util"
)

type PreparedTagValuesQuery struct {
	Version     string
	MetricID    int32
	PreKeyTagID string
	TagID       string
	NumResults  int
	FilterIn    map[string][]interface{}
	FilterNotIn map[string][]interface{}
}

func (q *PreparedTagValuesQuery) StringTag() bool {
	return q.TagID == format.StringTopTagID
}

type PreparedPointsQuery struct {
	User        string
	Version     string
	MetricID    int32
	PreKeyTagID string
	IsStringTop bool
	Kind        model.QueryFnKind
	By          []string
	FilterIn    map[string][]interface{}
	FilterNotIn map[string][]interface{}

	// for table view requests
	OrderBy bool
	Desc    bool
}

type TagValuesQueryMeta struct {
	StringValue bool
}

func (pq *PreparedPointsQuery) IsLight() bool {
	return pq.Kind != model.QueryFnKindUnique && pq.Kind != model.QueryFnKindPercentiles
}

func TagValuesQuery(pq *PreparedTagValuesQuery, lod LodInfo) (string, TagValuesQueryMeta, error) {
	meta := TagValuesQueryMeta{}
	valueName := "_value"
	if pq.StringTag() {
		meta.StringValue = true
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
		columnName(lod.HasPreKey, pq.TagID, pq.PreKeyTagID),
		valueName,
		sqlAggFn(pq.Version, "sum"),
		pq.PreKeyTableName(lod),
		metricColumn(pq.Version),
		datePredicate(pq.Version),
	)
	args := []interface{}{pq.MetricID, lod.FromSec, lod.ToSec}
	if pq.Version == Version1 {
		args = append(args, lod.FromSec, lod.ToSec)
	}
	for k, ids := range pq.FilterIn {
		if len(ids) > 0 {
			query += fmt.Sprintf(`
  AND %s IN (%s)`, columnName(lod.HasPreKey, k, pq.PreKeyTagID), expandBindVars(len(ids)))
			args = append(args, ids...)
		} else {
			query += `
  AND 1=0`
		}
	}
	for k, ids := range pq.FilterNotIn {
		if len(ids) > 0 {
			query += fmt.Sprintf(`
  AND %s NOT IN (%s)`, columnName(lod.HasPreKey, k, pq.PreKeyTagID), expandBindVars(len(ids)))
			args = append(args, ids...)
		} else {
			query += `
  AND 1=1`
		}
	}
	query += fmt.Sprintf(`
GROUP BY
  %s
HAVING _count > 0
ORDER BY
  _count DESC,
  %s
LIMIT %v
SETTINGS
  optimize_aggregation_in_order = 1
`, columnName(lod.HasPreKey, pq.TagID, pq.PreKeyTagID), valueName, pq.NumResults+1) // +1 so we can set "more":true

	q, err := util.BindQuery(query, args...)
	return q, meta, err
}

type PointsQueryMeta struct {
	Vals       int
	Tags       []string
	MinMaxHost bool
	Version    string
}

func loadPointsSelectWhat(pq *PreparedPointsQuery) (string, int, error) {
	var (
		version     = pq.Version
		isStringTop = pq.IsStringTop
		kind        = pq.Kind
	)
	if version == Version1 && isStringTop {
		return `
  toFloat64(sumMerge(count)) AS _count`, 0, nil // count is the only column available
	}

	switch kind {
	case model.QueryFnKindCount:
		return fmt.Sprintf(`
  toFloat64(%s(count)) AS _count,
  toFloat64(sum(1)) AS _val0,
  toFloat64(%s(max)) AS _val1`, sqlAggFn(version, "sum"), sqlAggFn(version, "max")), 2, nil
	case model.QueryFnKindValue:
		return fmt.Sprintf(`
  toFloat64(%s(count)) AS _count,
  toFloat64(%s(min)) AS _val0,
  toFloat64(%s(max)) AS _val1,
  toFloat64(%s(sum))/toFloat64(%s(count)) AS _val2,
  toFloat64(%s(sum)) AS _val3,
  if(%s(count) < 2, 0, sqrt(greatest(   (%s(sumsquare) - pow(%s(sum), 2) / %s(count)) / (%s(count) - 1)   , 0))) AS _val4,
  toFloat64(sum(1)) AS _val5,
  %s as _minHost, %s as _maxHost`,
			sqlAggFn(version, "sum"),
			sqlAggFn(version, "min"),
			sqlAggFn(version, "max"),
			sqlAggFn(version, "sum"), sqlAggFn(version, "sum"),
			sqlAggFn(version, "sum"),
			// https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance, "Naïve algorithm", poor numeric stability
			sqlAggFn(version, "sum"), sqlAggFn(version, "sum"), sqlAggFn(version, "sum"), sqlAggFn(version, "sum"), sqlAggFn(version, "sum"),
			sqlMinHost(version), sqlMaxHost(version)), 6, nil
	case model.QueryFnKindPercentilesLow:
		return fmt.Sprintf(`
	  toFloat64(%s(count)) AS _count,
	  toFloat64((quantilesTDigestMerge(0.001, 0.01, 0.05, 0.1)(percentiles) AS digest)[1]) AS _val0,
	  toFloat64(digest[2]) AS _val1,
	  toFloat64(digest[3]) AS _val2,
	  toFloat64(digest[4]) AS _val3,
	  toFloat64(0) AS _val4,
	  toFloat64(0) AS _val5,
	  toFloat64(0) AS _val6,
	  %s as _minHost, %s as _maxHost`,
			sqlAggFn(version, "sum"), sqlMinHost(version), sqlMaxHost(version)), 7, nil
	case model.QueryFnKindPercentiles:
		return fmt.Sprintf(`
  toFloat64(%s(count)) AS _count,
  toFloat64((quantilesTDigestMerge(0.25, 0.5, 0.75, 0.90, 0.95, 0.99, 0.999)(percentiles) AS digest)[1]) AS _val0,
  toFloat64(digest[2]) AS _val1,
  toFloat64(digest[3]) AS _val2,
  toFloat64(digest[4]) AS _val3,
  toFloat64(digest[5]) AS _val4,
  toFloat64(digest[6]) AS _val5,
  toFloat64(digest[7]) AS _val6,
  %s as _minHost, %s as _maxHost`,
			sqlAggFn(version, "sum"), sqlMinHost(version), sqlMaxHost(version)), 7, nil
	case model.QueryFnKindUnique:
		return fmt.Sprintf(`
  toFloat64(%s(count)) AS _count,
  toFloat64(uniqMerge(uniq_state)) AS _val0,
  %s as _minHost, %s as _maxHost`,
			sqlAggFn(version, "sum"), sqlMinHost(version), sqlMaxHost(version)), 1, nil
	default:
		return "", 0, fmt.Errorf("unsupported operation kind: %q", kind)
	}
}

func loadPointsQuery(pq *PreparedPointsQuery, lod LodInfo, utcOffset int64) (string, PointsQueryMeta, error) {
	what, cnt, err := loadPointsSelectWhat(pq)
	if err != nil {
		return "", PointsQueryMeta{}, err
	}

	var commaBy string
	if len(pq.By) > 0 {
		for _, b := range pq.By {
			commaBy += fmt.Sprintf(", %s AS key%s", columnName(lod.HasPreKey, b, pq.PreKeyTagID), b)
		}
	}

	var timeInterval string
	if lod.StepSec == _1M {
		timeInterval = fmt.Sprintf(`
toInt64(toDateTime(toStartOfInterval(time, INTERVAL 1 MONTH, '%s'), '%s')) AS _time, 
toInt64(toDateTime(_time, '%s') + INTERVAL 1 MONTH) - _time AS _stepSec`, lod.Location.String(), lod.Location.String(), lod.Location.String())
	} else {
		timeInterval = fmt.Sprintf(`
toInt64(toStartOfInterval(time + %d, INTERVAL %d second)) - %d AS _time,
toInt64(%d) AS _stepSec`, utcOffset, lod.StepSec, utcOffset, lod.StepSec)
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
		pq.PreKeyTableName(lod),
		metricColumn(pq.Version),
		datePredicate(pq.Version),
	)
	args := []interface{}{pq.MetricID, lod.FromSec, lod.ToSec}
	if pq.Version == Version1 {
		args = append(args, lod.FromSec, lod.ToSec)
	}
	for k, ids := range pq.FilterIn {
		if len(ids) > 0 {
			query += fmt.Sprintf(`
  AND %s IN (%s)`, columnName(lod.HasPreKey, k, pq.PreKeyTagID), expandBindVars(len(ids)))
			args = append(args, ids...)
		} else {
			query += `
  AND 1=0`
		}
	}
	for k, ids := range pq.FilterNotIn {
		if len(ids) > 0 {
			query += fmt.Sprintf(`
  AND %s NOT IN (%s)`, columnName(lod.HasPreKey, k, pq.PreKeyTagID), expandBindVars(len(ids)))
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
	if pq.OrderBy {
		limit = maxTableRows
		desc := ""
		if pq.Desc {
			desc = " DESC"
		}
		query += fmt.Sprintf(`
HAVING _count > 0
ORDER BY
  _time%s%s`, commaBy, desc)
	}
	query += fmt.Sprintf(`
LIMIT %v
SETTINGS
  optimize_aggregation_in_order = 1
`, limit)
	q, err := util.BindQuery(query, args...)
	return q, PointsQueryMeta{Vals: cnt, Tags: pq.By, MinMaxHost: pq.Kind != model.QueryFnKindCount, Version: pq.Version}, err
}

func loadPointQuery(pq *PreparedPointsQuery, lod LodInfo, utcOffset int64) (string, PointsQueryMeta, error) {
	what, cnt, err := loadPointsSelectWhat(pq)
	if err != nil {
		return "", PointsQueryMeta{}, err
	}

	var commaBy string
	if len(pq.By) > 0 {
		for i, b := range pq.By {
			if i == 0 {
				commaBy += fmt.Sprintf("%s AS key%s", columnName(lod.HasPreKey, b, pq.PreKeyTagID), b)
			} else {
				commaBy += fmt.Sprintf(", %s AS key%s", columnName(lod.HasPreKey, b, pq.PreKeyTagID), b)
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
		preKeyTableNameFromPoint(lod, "", pq.PreKeyTagID, pq.FilterIn, pq.FilterNotIn),
		metricColumn(pq.Version),
		datePredicate(pq.Version),
	)
	args := []interface{}{pq.MetricID, lod.FromSec, lod.ToSec}
	if pq.Version == Version1 {
		args = append(args, lod.FromSec, lod.ToSec)
	}
	for k, ids := range pq.FilterIn {
		if len(ids) > 0 {
			query += fmt.Sprintf(`
  AND %s IN (%s)`, columnName(lod.HasPreKey, k, pq.PreKeyTagID), expandBindVars(len(ids)))
			args = append(args, ids...)
		} else {
			query += `
  AND 1=0`
		}
	}
	for k, ids := range pq.FilterNotIn {
		if len(ids) > 0 {
			query += fmt.Sprintf(`
  AND %s NOT IN (%s)`, columnName(lod.HasPreKey, k, pq.PreKeyTagID), expandBindVars(len(ids)))
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
HAVING _count > 0
LIMIT %v
SETTINGS
  optimize_aggregation_in_order = 1
`, maxSeriesRows)
	q, err := util.BindQuery(query, args...)
	return q, PointsQueryMeta{Vals: cnt, Tags: pq.By, MinMaxHost: pq.Kind != model.QueryFnKindCount, Version: pq.Version}, err
}

func sqlAggFn(version string, fn string) string {
	if version == Version1 {
		return fn + "Merge"
	}
	return fn
}

func sqlMinHost(version string) string {
	if version == Version1 {
		return "0"
	}
	return "argMinMerge(min_host)"
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

type StringFixed [format.MaxStringLen]byte

func (s *StringFixed) UnmarshalBinary(data []byte) error {
	copy(s[:], data)
	return nil
}

func (s *StringFixed) String() string {
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

func (pq *PreparedPointsQuery) PreKeyTableName(lod LodInfo) string {
	var usePreKey bool
	if lod.HasPreKey {
		usePreKey = lod.PreKeyOnly ||
			len(pq.FilterIn[pq.PreKeyTagID]) > 0 ||
			len(pq.FilterNotIn[pq.PreKeyTagID]) > 0
		if !usePreKey {
			for _, v := range pq.By {
				if v == pq.PreKeyTagID {
					usePreKey = true
					break
				}
			}
		}
	}
	if usePreKey {
		return preKeyTableNames[lod.Table]
	}
	return lod.Table
}

func (pq *PreparedTagValuesQuery) PreKeyTableName(lod LodInfo) string {
	usePreKey := (lod.HasPreKey &&
		(lod.PreKeyOnly ||
			(pq.TagID != "" && pq.TagID == pq.PreKeyTagID) ||
			len(pq.FilterIn[pq.PreKeyTagID]) > 0 ||
			len(pq.FilterNotIn[pq.PreKeyTagID]) > 0))
	if usePreKey {
		return preKeyTableNames[lod.Table]
	}
	return lod.Table
}

func preKeyTableNameFromPoint(lod LodInfo, tagID string, preKeyTagID string, filterIn map[string][]interface{}, filterNotIn map[string][]interface{}) string {
	usePreKey := lod.HasPreKey && ((tagID != "" && tagID == preKeyTagID) || len(filterIn[preKeyTagID]) > 0 || len(filterNotIn[preKeyTagID]) > 0)
	if usePreKey {
		return preKeyTableNames[lod.Table]
	}
	return lod.Table
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
