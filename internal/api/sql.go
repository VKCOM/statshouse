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
	"text/template"

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/util"
)

type maybeMappedTag struct {
	Value  string
	Mapped int32
}

type preparedTagValuesQuery struct {
	version     string
	metricID    int32
	preKeyTagID string
	tagID       string
	numResults  int
	filterIn    map[string][]interface{}
	filterNotIn map[string][]interface{}

	filterInV3    map[string][]maybeMappedTag
	filterNotInV3 map[string][]maybeMappedTag
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
	kind        data_model.DigestKind
	by          []string
	filterIn    map[string][]interface{}
	filterNotIn map[string][]interface{}

	filterInV3    map[string][]maybeMappedTag
	filterNotInV3 map[string][]maybeMappedTag

	// for table view requests
	orderBy bool
	desc    bool
}

type tagValuesQueryMeta struct {
	stag bool
	// both mapped and unmapped values for v3 requests
	mixed bool
}

func (pq *preparedPointsQuery) isLight() bool {
	return pq.kind != data_model.DigestKindUnique && pq.kind != data_model.DigestKindPercentiles
}

func (pq *preparedPointsQuery) IsHardware() bool {
	return format.HardwareMetric(pq.metricID)
}

func tagValuesQuery(pq *preparedTagValuesQuery, lod data_model.LOD) (string, tagValuesQueryMeta, error) {
	if pq.version == Version3 {
		return tagValuesQueryV3(pq, lod)
	}
	return tagValuesQueryV2(pq, lod)
}

func tagValuesQueryV2(pq *preparedTagValuesQuery, lod data_model.LOD) (string, tagValuesQueryMeta, error) {
	meta := tagValuesQueryMeta{}
	valueName := "_value"
	if pq.stringTag() {
		meta.stag = true
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
		mappedColumnName(lod.HasPreKey, pq.tagID, pq.preKeyTagID),
		valueName,
		sqlAggFn(pq.version, "sum"),
		pq.preKeyTableName(lod),
		metricColumn(pq.version),
		datePredicate(pq.version),
	)
	args := []interface{}{pq.metricID, lod.FromSec, lod.ToSec}
	if pq.version == Version1 {
		args = append(args, lod.FromSec, lod.ToSec)
	}
	for k, ids := range pq.filterIn {
		if len(ids) > 0 {
			query += fmt.Sprintf(`
  AND %s IN (%s)`, mappedColumnName(lod.HasPreKey, k, pq.preKeyTagID), expandBindVars(len(ids)))
			args = append(args, ids...)
		} else {
			query += `
  AND 1=0`
		}
	}
	for k, ids := range pq.filterNotIn {
		if len(ids) > 0 {
			query += fmt.Sprintf(`
  AND %s NOT IN (%s)`, mappedColumnName(lod.HasPreKey, k, pq.preKeyTagID), expandBindVars(len(ids)))
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
`, mappedColumnName(lod.HasPreKey, pq.tagID, pq.preKeyTagID), valueName, pq.numResults+1) // +1 so we can set "more":true

	q, err := util.BindQuery(query, args...)
	return q, meta, err
}

func tagValuesQueryV3(pq *preparedTagValuesQuery, lod data_model.LOD) (string, tagValuesQueryMeta, error) {
	qt, err := template.New("").Parse(`
SELECT {{.MappedColumnName}} AS _mapped, {{.UnmappedColumnName}} AS _unmapped, toFloat64(sum(count)) AS _count
FROM {{.TableName}}
WHERE metric = {{.MetricId}}
  AND time >= {{.From}} AND time < {{.To}}
{{.TagConditions}}
GROUP BY _mapped, _unmapped
HAVING _count > 0
ORDER BY _count DESC, _mapped, _unmapped
LIMIT {{.Limit}}
SETTINGS optimize_aggregation_in_order = 1
`)
	if err != nil {
		return "", tagValuesQueryMeta{}, fmt.Errorf("failed to create query template: %v", err)
	}
	type templParams struct {
		MappedColumnName   string
		UnmappedColumnName string
		TableName          string
		MetricId           int32
		TagConditions      string
		From               int64
		To                 int64
		Limit              int
	}

	p := &templParams{
		MappedColumnName:   mappedColumnNameV3(pq.tagID),
		UnmappedColumnName: unmappedColumnNameV3(pq.tagID),
		TableName:          pq.preKeyTableName(lod),
		MetricId:           pq.metricID,
		From:               lod.FromSec,
		To:                 lod.ToSec,
		Limit:              pq.numResults + 1,
	}
	cond := strings.Builder{}
	writeTagCond(&cond, pq.filterInV3, true)
	writeTagCond(&cond, pq.filterNotInV3, false)
	p.TagConditions = cond.String()
	cond.Reset()

	var b bytes.Buffer
	err = qt.Execute(&b, p)
	if err != nil {
		return "", tagValuesQueryMeta{}, fmt.Errorf("failed to execute query template: %v", err)
	}
	return b.String(), tagValuesQueryMeta{mixed: true}, nil
}

func writeTagCond(cond *strings.Builder, f map[string][]maybeMappedTag, in bool) {
	strValues := make([]string, 0, 16)
	intValues := make([]string, 0, 16)
	for tag, values := range f {
		strValues = strValues[:0]
		intValues = intValues[:0]
		if len(values) == 0 {
			continue
		}
		for _, valPair := range values {
			if len(valPair.Value) > 0 {
				strValues = append(strValues, "'"+valPair.Value+"'")
			}
			// we allow 0 here because it is a valid value for raw tags
			intValues = append(intValues, fmt.Sprint(valPair.Mapped))
		}
		cond.WriteString("  AND (")
		if len(intValues) > 0 {
			cond.WriteString(mappedColumnNameV3(tag))
			if in {
				cond.WriteString(" IN (")
			} else {
				cond.WriteString(" NOT IN (")
			}
			cond.WriteString(strings.Join(intValues, ", "))
			cond.WriteString(")")
			if len(strValues) > 0 {
				if in {
					cond.WriteString(" OR ")
				} else {
					cond.WriteString(" AND ")
				}
			}
		}
		if len(strValues) > 0 {
			cond.WriteString(unmappedColumnNameV3(tag))
			if in {
				cond.WriteString(" IN (")
			} else {
				cond.WriteString(" NOT IN (")
			}
			cond.WriteString(strings.Join(strValues, ", "))
			cond.WriteString(")")
		}
		cond.WriteString(")\n")
	}
	strValues = nil
	intValues = nil
}

type pointsQueryMeta struct {
	vals       int
	tags       []string
	minMaxHost bool
	version    string
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
	case data_model.DigestKindCount:
		return fmt.Sprintf(`
  toFloat64(%s(count)) AS _count,
  toFloat64(sum(1)) AS _val0,
  toFloat64(%s(max)) AS _val1`, sqlAggFn(version, "sum"), sqlAggFn(version, "max")), 2, nil
	case data_model.DigestKindValue:
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
	case data_model.DigestKindPercentilesLow:
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
	case data_model.DigestKindPercentiles:
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
	case data_model.DigestKindUnique:
		return fmt.Sprintf(`
  toFloat64(%s(count)) AS _count,
  toFloat64(uniqMerge(uniq_state)) AS _val0,
  %s as _minHost, %s as _maxHost`,
			sqlAggFn(version, "sum"), sqlMinHost(version), sqlMaxHost(version)), 1, nil
	default:
		return "", 0, fmt.Errorf("unsupported operation kind: %q", kind)
	}
}

func loadPointsQuery(pq *preparedPointsQuery, lod data_model.LOD, utcOffset int64) (string, pointsQueryMeta, error) {
	switch pq.version {
	case Version3:
		return loadPointsQueryV3(pq, lod, utcOffset)
	default:
		return loadPointsQueryV2(pq, lod, utcOffset)
	}
}

func loadPointsQueryV2(pq *preparedPointsQuery, lod data_model.LOD, utcOffset int64) (string, pointsQueryMeta, error) {
	what, cnt, err := loadPointsSelectWhat(pq)
	if err != nil {
		return "", pointsQueryMeta{}, err
	}

	var commaBy string
	if len(pq.by) > 0 {
		for _, b := range pq.by {
			commaBy += fmt.Sprintf(", %s AS key%s", mappedColumnName(lod.HasPreKey, b, pq.preKeyTagID), b)
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
		pq.preKeyTableName(lod),
		metricColumn(pq.version),
		datePredicate(pq.version),
	)
	args := []interface{}{pq.metricID, lod.FromSec, lod.ToSec}
	if pq.version == Version1 {
		args = append(args, lod.FromSec, lod.ToSec)
	}
	for k, ids := range pq.filterIn {
		if len(ids) > 0 {
			query += fmt.Sprintf(`
  AND %s IN (%s)`, mappedColumnName(lod.HasPreKey, k, pq.preKeyTagID), expandBindVars(len(ids)))
			args = append(args, ids...)
		} else {
			query += `
  AND 1=0`
		}
	}
	for k, ids := range pq.filterNotIn {
		if len(ids) > 0 {
			query += fmt.Sprintf(`
  AND %s NOT IN (%s)`, mappedColumnName(lod.HasPreKey, k, pq.preKeyTagID), expandBindVars(len(ids)))
			args = append(args, ids...)
		} else {
			query += `
  AND 1=1`
		}
	}

	query += fmt.Sprintf(`
GROUP BY
  _time%s`, commaBy)

	var having string
	switch pq.kind {
	case data_model.DigestKindPercentiles:
		having = `
HAVING not(isNaN(_val0) AND isNaN(_val1) AND isNaN(_val2) AND isNaN(_val3) AND isNaN(_val4) AND isNaN(_val5) AND isNaN(_val6))`
	case data_model.DigestKindPercentilesLow:
		having = `
HAVING not(isNaN(_val0) AND isNaN(_val1) AND isNaN(_val2) AND isNaN(_val3))`
	}
	var oderBy string
	limit := maxSeriesRows
	if pq.orderBy {
		limit = maxTableRows
		desc := ""
		if pq.desc {
			desc = " DESC"
		}
		if having != "" {
			having += " && _count > 0"
		} else {
			having = `
HAVING _count > 0`
		}
		oderBy = fmt.Sprintf(`
ORDER BY _time%s%s`, commaBy, desc)
	}
	query += fmt.Sprintf(`%s%s
LIMIT %v
SETTINGS
  optimize_aggregation_in_order = 1
`, having, oderBy, limit)
	q, err := util.BindQuery(query, args...)
	return q, pointsQueryMeta{vals: cnt, tags: pq.by, minMaxHost: pq.kind != data_model.DigestKindCount, version: pq.version}, err
}

func loadPointsQueryV3(pq *preparedPointsQuery, lod data_model.LOD, utcOffset int64) (string, pointsQueryMeta, error) {
	what, cnt, err := loadPointsSelectWhat(pq)
	if err != nil {
		return "", pointsQueryMeta{}, err
	}

	var byTagsB strings.Builder
	if len(pq.by) > 0 {
		for _, b := range pq.by {
			byTagsB.WriteString(fmt.Sprintf(", %s AS tag%s", mappedColumnNameV3(b), b))
			byTagsB.WriteString(fmt.Sprintf(", %s AS stag%s", unmappedColumnNameV3(b), b))
		}
	}
	byTags := byTagsB.String()

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

	queryBuilder := strings.Builder{}
	queryBuilder.WriteString("SELECT")
	queryBuilder.WriteString(timeInterval)
	queryBuilder.WriteString(byTags)
	queryBuilder.WriteString(", ")
	queryBuilder.WriteString(what)
	queryBuilder.WriteString("\nFROM ")
	queryBuilder.WriteString(pq.preKeyTableName(lod))
	queryBuilder.WriteString("\nWHERE index_type = 0 AND metric = ")
	queryBuilder.WriteString(fmt.Sprint(pq.metricID))
	queryBuilder.WriteString(" AND time >= ")
	queryBuilder.WriteString(fmt.Sprint(lod.FromSec))
	queryBuilder.WriteString(" AND time < ")
	queryBuilder.WriteString(fmt.Sprint(lod.ToSec))
	writeTagCond(&queryBuilder, pq.filterInV3, true)
	writeTagCond(&queryBuilder, pq.filterNotInV3, false)
	queryBuilder.WriteString("\nGROUP BY _time")
	queryBuilder.WriteString(byTags)
	if pq.orderBy {
		queryBuilder.WriteString("\n HAVING _count > 0")
		switch pq.kind {
		case data_model.DigestKindPercentiles:
			queryBuilder.WriteString("AND not(isNaN(_val0) AND isNaN(_val1) AND isNaN(_val2) AND isNaN(_val3) AND isNaN(_val4) AND isNaN(_val5) AND isNaN(_val6))")
		case data_model.DigestKindPercentilesLow:
			queryBuilder.WriteString("AND not(isNaN(_val0) AND isNaN(_val1) AND isNaN(_val2) AND isNaN(_val3))")
		}
		queryBuilder.WriteString("\nORDER BY _time")
		queryBuilder.WriteString(byTags)
		if pq.desc {
			queryBuilder.WriteString(" DESC")
		}
		queryBuilder.WriteString("\nLIMIT ")
		queryBuilder.WriteString(fmt.Sprint(maxTableRows))
	} else {
		queryBuilder.WriteString("\nLIMIT ")
		queryBuilder.WriteString(fmt.Sprint(maxSeriesRows))
	}
	queryBuilder.WriteString("\nSETTINGS optimize_aggregation_in_order = 1")
	q := queryBuilder.String()
	return q, pointsQueryMeta{vals: cnt, tags: pq.by, minMaxHost: pq.kind != data_model.DigestKindCount, version: pq.version}, err
}

func loadPointQuery(pq *preparedPointsQuery, lod data_model.LOD, utcOffset int64) (string, pointsQueryMeta, error) {
	what, cnt, err := loadPointsSelectWhat(pq)
	if err != nil {
		return "", pointsQueryMeta{}, err
	}

	var commaBy string
	if len(pq.by) > 0 {
		for i, b := range pq.by {
			if i == 0 {
				commaBy += fmt.Sprintf("%s AS key%s", mappedColumnName(lod.HasPreKey, b, pq.preKeyTagID), b)
			} else {
				commaBy += fmt.Sprintf(", %s AS key%s", mappedColumnName(lod.HasPreKey, b, pq.preKeyTagID), b)
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
		preKeyTableNameFromPoint(lod, "", pq.preKeyTagID, pq.filterIn, pq.filterNotIn),
		metricColumn(pq.version),
		datePredicate(pq.version),
	)
	args := []interface{}{pq.metricID, lod.FromSec, lod.ToSec}
	if pq.version == Version1 {
		args = append(args, lod.FromSec, lod.ToSec)
	}
	for k, ids := range pq.filterIn {
		if len(ids) > 0 {
			query += fmt.Sprintf(`
  AND %s IN (%s)`, mappedColumnName(lod.HasPreKey, k, pq.preKeyTagID), expandBindVars(len(ids)))
			args = append(args, ids...)
		} else {
			query += `
  AND 1=0`
		}
	}
	for k, ids := range pq.filterNotIn {
		if len(ids) > 0 {
			query += fmt.Sprintf(`
  AND %s NOT IN (%s)`, mappedColumnName(lod.HasPreKey, k, pq.preKeyTagID), expandBindVars(len(ids)))
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
	return q, pointsQueryMeta{vals: cnt, tags: pq.by, minMaxHost: pq.kind != data_model.DigestKindCount, version: pq.version}, err
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

func (pq *preparedPointsQuery) preKeyTableName(lod data_model.LOD) string {
	var usePreKey bool
	if lod.HasPreKey {
		usePreKey = lod.PreKeyOnly ||
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
		return preKeyTableNames[lod.Table]
	}
	return lod.Table
}

func (pq *preparedTagValuesQuery) preKeyTableName(lod data_model.LOD) string {
	usePreKey := (lod.HasPreKey &&
		(lod.PreKeyOnly ||
			(pq.tagID != "" && pq.tagID == pq.preKeyTagID) ||
			len(pq.filterIn[pq.preKeyTagID]) > 0 ||
			len(pq.filterNotIn[pq.preKeyTagID]) > 0))
	if usePreKey {
		return preKeyTableNames[lod.Table]
	}
	return lod.Table
}

func preKeyTableNameFromPoint(lod data_model.LOD, tagID string, preKeyTagID string, filterIn map[string][]interface{}, filterNotIn map[string][]interface{}) string {
	usePreKey := lod.HasPreKey && ((tagID != "" && tagID == preKeyTagID) || len(filterIn[preKeyTagID]) > 0 || len(filterNotIn[preKeyTagID]) > 0)
	if usePreKey {
		return preKeyTableNames[lod.Table]
	}
	return lod.Table
}

func mappedColumnName(hasPreKey bool, tagID string, preKeyTagID string) string {
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

func mappedColumnNameV3(tagID string) string {
	return "tag" + tagID
}

func unmappedColumnNameV3(tagID string) string {
	return "stag" + tagID
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
