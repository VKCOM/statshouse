// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/format"
)

type preparedTagValuesQuery struct {
	metricID    int32
	preKeyTagX  int
	preKeyTagID string
	tagID       string
	numResults  int
	filterIn    data_model.TagFilters
	filterNotIn data_model.TagFilters
}

func (q *preparedTagValuesQuery) stringTag() bool {
	return q.tagID == format.StringTopTagID
}

type pointsQuery struct {
	version     string
	user        string
	metricID    int32
	preKeyTagX  int
	preKeyTagID string
	isStringTop bool
	kind        data_model.DigestKind
	by          []string
	filterIn    data_model.TagFilters
	filterNotIn data_model.TagFilters
	sort        pointsQuerySort // for table view requests
}

type pointsQuerySort int

const (
	sortNone pointsQuerySort = iota
	sortAscending
	sortDescending
)

func (pq *pointsQuery) cacheKey() string {
	var sb strings.Builder
	sb.WriteString("v=")
	switch pq.version {
	case Version1:
		sb.WriteString(Version1)
	default:
		sb.WriteString(Version2)
	}
	sb.WriteString(";m=")
	sb.WriteString(fmt.Sprint(pq.metricID))
	sb.WriteString(";pk=")
	sb.WriteString(fmt.Sprint(pq.preKeyTagX))
	sb.WriteString(";st=")
	sb.WriteString(fmt.Sprint(pq.isStringTop))
	sb.WriteString(";kind=")
	sb.WriteString(fmt.Sprint(int(pq.kind)))
	sb.WriteString(";by=")
	if len(pq.by) != 0 {
		sort.Strings(pq.by)
		sb.WriteString(pq.by[0])
		for i := 1; i < len(pq.by); i++ {
			sb.WriteString(",")
			sb.WriteString(pq.by[i])
		}
	}
	sb.WriteString(";inc=")
	s := make([]string, 0, 16)
	s = writeTagFiltersCacheKey(&sb, pq.filterIn, s)
	sb.WriteString(";exl=")
	writeTagFiltersCacheKey(&sb, pq.filterNotIn, s)
	sb.WriteString(";sort=")
	sb.WriteString(fmt.Sprint(int(pq.sort)))
	return sb.String()
}

func writeTagFiltersCacheKey(sb *strings.Builder, f data_model.TagFilters, s []string) []string {
	var n int
	for i, filter := range f.Tags {
		if filter.Empty() {
			continue
		}
		if n != 0 {
			sb.WriteString(",")
		}
		sb.WriteString("{")
		sb.WriteString(fmt.Sprint(i))
		if filter.Re2 != "" {
			sb.WriteString("~")
			sb.WriteString(filter.Re2)
		} else if len(filter.Values) != 0 {
			sb.WriteString("=")
			s := s[:0]
			for _, v := range filter.Values {
				s = append(s, v.String())
			}
			sort.Strings(s)
			sb.WriteString(s[0])
			for i := 1; i < len(s); i++ {
				sb.WriteString(",")
				sb.WriteString(s[i])
			}
		}
		sb.WriteString("}")
		n++
	}
	if f.StringTopRe2 != "" {
		if n != 0 {
			sb.WriteString(",")
		}
		sb.WriteString("{_s~")
		sb.WriteString(f.StringTopRe2)
		sb.WriteString("}")
	} else if len(f.StringTop) != 0 {
		if n != 0 {
			sb.WriteString(",")
		}
		sb.WriteString("{_s=")
		s = append(s[:0], f.StringTop...)
		sort.Strings(s)
		sb.WriteString(s[0])
		for i := 1; i < len(s); i++ {
			sb.WriteString(",")
			sb.WriteString(s[i])
		}
		sb.WriteString("}")
	}
	return s
}

type tagValuesQueryMeta struct {
	queryMeta
	stag bool
	// both mapped and unmapped values for v3 requests
	mixed bool
}

func (pq *queryMeta) isLight() bool {
	return pq.kind != data_model.DigestKindUnique && pq.kind != data_model.DigestKindPercentiles
}

func (pq *queryMeta) IsHardware() bool {
	return format.HardwareMetric(pq.metricID)
}

func tagValuesQuery(pq *preparedTagValuesQuery, lod data_model.LOD) (string, *tagValuesSelectCols) {
	switch lod.Version {
	case Version3:
		return pq.tagValuesQueryV3(lod)
	default:
		return pq.tagValuesQueryV2(lod)
	}
}

func (pq *preparedTagValuesQuery) tagValuesQueryV2(lod data_model.LOD) (string, *tagValuesSelectCols) {
	meta := tagValuesQueryMeta{}
	valueName := "_value"
	if pq.stringTag() {
		meta.stag = true
		valueName = "_string_value"
	}
	// no need to escape anything as long as table and tag names are fixed
	var sb strings.Builder
	sb.WriteString("SELECT ")
	sb.WriteString(mappedColumnName(lod.HasPreKey, pq.tagID, pq.preKeyTagID))
	sb.WriteString(" AS ")
	sb.WriteString(valueName)
	sb.WriteString(",toFloat64(")
	sb.WriteString(sqlAggFn(lod.Version, "sum"))
	sb.WriteString("(count)) AS _count FROM ")
	sb.WriteString(pq.preKeyTableName(lod))
	writeWhereTimeFilter(&sb, &lod)
	writeMetricFilter(&sb, pq.metricID, pq.filterIn.Metrics, pq.filterNotIn.Metrics, lod.Version)
	writeV1DateFilter(&sb, &lod)
	for i, ids := range pq.filterIn.Tags {
		if len(ids.Values) > 0 {
			sb.WriteString(" AND ")
			sb.WriteString(mappedColumnName(lod.HasPreKey, format.TagID(i), pq.preKeyTagID))
			sb.WriteString(" IN (")
			expandTagsMapped(&sb, ids.Values)
			sb.WriteString(")")
		}
	}
	if len(pq.filterIn.StringTop) > 0 {
		sb.WriteString(" AND skey IN (")
		expandStrings(&sb, pq.filterIn.StringTop)
		sb.WriteString(")")
	}
	for i, ids := range pq.filterNotIn.Tags {
		if len(ids.Values) > 0 {
			sb.WriteString(" AND ")
			sb.WriteString(mappedColumnName(lod.HasPreKey, format.TagID(i), pq.preKeyTagID))
			sb.WriteString(" NOT IN (")
			expandTagsMapped(&sb, ids.Values)
			sb.WriteString(")")
		}
	}
	if len(pq.filterNotIn.StringTop) > 0 {
		sb.WriteString(" AND skey NOT IN (")
		expandStrings(&sb, pq.filterNotIn.StringTop)
		sb.WriteString(")")
	}
	sb.WriteString(" GROUP BY ")
	sb.WriteString(mappedColumnName(lod.HasPreKey, pq.tagID, pq.preKeyTagID))
	sb.WriteString(" HAVING _count>0 ORDER BY _count DESC,")
	sb.WriteString(valueName)
	sb.WriteString(" LIMIT ")
	sb.WriteString(fmt.Sprint(pq.numResults + 1))
	sb.WriteString(" SETTINGS optimize_aggregation_in_order=1")
	return sb.String(), newTagValuesSelectCols(meta)
}

func (pq *preparedTagValuesQuery) tagValuesQueryV3(lod data_model.LOD) (string, *tagValuesSelectCols) {
	var sb strings.Builder
	sb.WriteString("SELECT ")
	sb.WriteString(mappedColumnNameV3(pq.tagID))
	sb.WriteString(" AS _mapped,")
	sb.WriteString(unmappedColumnNameV3(pq.tagID))
	sb.WriteString(" AS _unmapped,toFloat64(sum(count)) AS _count FROM ")
	sb.WriteString(pq.preKeyTableName(lod))
	writeWhereTimeFilter(&sb, &lod)
	writeMetricFilter(&sb, pq.metricID, pq.filterIn.Metrics, pq.filterNotIn.Metrics, lod.Version)
	writeTagCond(&sb, pq.filterIn, true)
	writeTagCond(&sb, pq.filterNotIn, false)
	sb.WriteString(" GROUP BY _mapped,_unmapped HAVING _count>0 ORDER BY _count,_mapped,_unmapped DESC LIMIT ")
	sb.WriteString(fmt.Sprint(pq.numResults + 1))
	sb.WriteString(" SETTINGS optimize_aggregation_in_order=1")
	return sb.String(), newTagValuesSelectColsV3(tagValuesQueryMeta{mixed: true})
}

func (pq *preparedTagValuesQuery) tagValueIDsQuery(lod data_model.LOD) (string, *tagValuesSelectCols) {
	switch lod.Version {
	case Version3:
		return pq.tagValueIDsQueryV3(lod)
	default:
		return pq.tagValuesQueryV2(lod)
	}
}

func (pq *preparedTagValuesQuery) tagValueIDsQueryV3(lod data_model.LOD) (string, *tagValuesSelectCols) {
	var sb strings.Builder
	sb.WriteString("SELECT ")
	sb.WriteString(mappedColumnNameV3(pq.tagID))
	sb.WriteString(" AS _mapped,toFloat64(sum(count)) AS _count FROM ")
	sb.WriteString(pq.preKeyTableName(lod))
	writeWhereTimeFilter(&sb, &lod)
	writeMetricFilter(&sb, pq.metricID, pq.filterIn.Metrics, pq.filterNotIn.Metrics, lod.Version)
	writeTagCond(&sb, pq.filterIn, true)
	writeTagCond(&sb, pq.filterNotIn, false)
	sb.WriteString(" GROUP BY _mapped HAVING _count>0 ORDER BY _count,_mapped DESC LIMIT ")
	sb.WriteString(fmt.Sprint(pq.numResults + 1))
	sb.WriteString(" SETTINGS optimize_aggregation_in_order=1")
	cols := &tagValuesSelectCols{tagValuesQueryMeta: tagValuesQueryMeta{}}
	cols.res = append(cols.res, proto.ResultColumn{Name: "_mapped", Data: &cols.valID})
	cols.res = append(cols.res, proto.ResultColumn{Name: "_count", Data: &cols.cnt})
	return sb.String(), cols
}

func writeTagCond(sb *strings.Builder, f data_model.TagFilters, in bool) {
	var sep, predicate string
	if in {
		sep, predicate = " OR ", " IN "
	} else {
		sep, predicate = " AND ", " NOT IN "
	}
	for i, filter := range f.Tags {
		if filter.Empty() {
			continue
		}
		sb.WriteString(" AND (")
		// mapped
		tagID := format.TagID(i)
		var hasMapped bool
		var hasValue bool
		var hasEmpty bool
		var started bool
		for _, v := range filter.Values {
			if v.Empty() {
				hasEmpty = true
				continue
			}
			if v.HasValue() {
				hasValue = true
			}
			if v.IsMapped() {
				if !hasMapped {
					if started {
						sb.WriteString(sep)
					} else {
						started = true
					}
					sb.WriteString(mappedColumnNameV3(tagID))
					sb.WriteString(predicate)
					sb.WriteString("(")
					hasMapped = true
				} else {
					sb.WriteString(",")
				}
				sb.WriteString(fmt.Sprint(v.Mapped))
			}
		}
		if hasMapped {
			sb.WriteString(")")
		}
		// not mapped
		if filter.Re2 != "" {
			if started {
				sb.WriteString(sep)
			} else {
				started = true
			}
			if !in {
				sb.WriteString("NOT ")
			}
			sb.WriteString("match(")
			sb.WriteString(unmappedColumnNameV3(tagID))
			sb.WriteString(",'")
			sb.WriteString(escapeReplacer.Replace(filter.Re2))
			sb.WriteString("')")
		} else if hasValue {
			hasValue = false
			for _, v := range filter.Values {
				if v.Empty() {
					continue
				}
				if v.HasValue() {
					if !hasValue {
						if started {
							sb.WriteString(sep)
						} else {
							started = true
						}
						sb.WriteString(unmappedColumnNameV3(tagID))
						sb.WriteString(predicate)
						sb.WriteString("('")
						hasValue = true
					} else {
						sb.WriteString("','")
					}
					sb.WriteString(escapeReplacer.Replace(v.Value))
				}
			}
			sb.WriteString("')")
		}
		// empty
		if hasEmpty {
			if started {
				sb.WriteString(sep)
			}
			if !in {
				sb.WriteString("NOT ")
			}
			sb.WriteString("(")
			sb.WriteString(mappedColumnNameV3(tagID))
			sb.WriteString("=0 AND ")
			sb.WriteString(unmappedColumnNameV3(tagID))
			sb.WriteString("='')")
		}
		sb.WriteString(")")
	}
}

type queryMeta struct {
	metricID int32
	kind     data_model.DigestKind
	user     string
}

type pointsQueryMeta struct {
	queryMeta
	vals       int
	tags       []string
	minMaxHost bool
	version    string
}

func loadPointsSelectWhat(sb *strings.Builder, pq *pointsQuery, version string) (int, error) {
	var (
		isStringTop = pq.isStringTop
		kind        = pq.kind
	)
	if version == Version1 && isStringTop {
		sb.WriteString(",toFloat64(sumMerge(count)) AS _count")
		return 0, nil // count is the only column available
	}

	switch kind {
	case data_model.DigestKindCount:
		sb.WriteString(fmt.Sprintf(",toFloat64(%s(count)) AS _count", sqlAggFn(version, "sum")))
		sb.WriteString(",toFloat64(sum(1)) AS _val0")
		sb.WriteString(fmt.Sprintf(",toFloat64(%s(max)) AS _val1", sqlAggFn(version, "max")))
		return 2, nil
	case data_model.DigestKindValue:
		sb.WriteString(fmt.Sprintf(",toFloat64(%s(count)) AS _count", sqlAggFn(version, "sum")))
		sb.WriteString(fmt.Sprintf(",toFloat64(%s(min)) AS _val0", sqlAggFn(version, "min")))
		sb.WriteString(fmt.Sprintf(",toFloat64(%s(max)) AS _val1", sqlAggFn(version, "max")))
		sb.WriteString(fmt.Sprintf(",toFloat64(%s(sum))/toFloat64(%s(count)) AS _val2", sqlAggFn(version, "sum"), sqlAggFn(version, "sum")))
		sb.WriteString(fmt.Sprintf(",toFloat64(%s(sum)) AS _val3", sqlAggFn(version, "sum")))
		// https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance, "Naïve algorithm", poor numeric stability
		sb.WriteString(fmt.Sprintf(",if(%s(count)<2,0,sqrt(greatest((%s(sumsquare)-pow(%s(sum),2)/%s(count))/(%s(count)-1),0))) AS _val4",
			sqlAggFn(version, "sum"), sqlAggFn(version, "sum"), sqlAggFn(version, "sum"), sqlAggFn(version, "sum"), sqlAggFn(version, "sum")))
		sb.WriteString(",toFloat64(sum(1)) AS _val5")
		sb.WriteString(fmt.Sprintf(",%s AS _minHost,%s AS _maxHost", sqlMinHost(version), sqlMaxHost(version)))
		return 6, nil
	case data_model.DigestKindPercentilesLow:
		sb.WriteString(fmt.Sprintf(",toFloat64(%s(count)) AS _count", sqlAggFn(version, "sum")))
		sb.WriteString(",toFloat64((quantilesTDigestMerge(0.001, 0.01, 0.05, 0.1)(percentiles) AS digest)[1]) AS _val0")
		sb.WriteString(",toFloat64(digest[2]) AS _val1")
		sb.WriteString(",toFloat64(digest[3]) AS _val2")
		sb.WriteString(",toFloat64(digest[4]) AS _val3")
		sb.WriteString(",toFloat64(0) AS _val4")
		sb.WriteString(",toFloat64(0) AS _val5")
		sb.WriteString(",toFloat64(0) AS _val6")
		sb.WriteString(fmt.Sprintf(", %s as _minHost, %s as _maxHost", sqlMinHost(version), sqlMaxHost(version)))
		return 7, nil
	case data_model.DigestKindPercentiles:
		sb.WriteString(fmt.Sprintf(",toFloat64(%s(count)) AS _count", sqlAggFn(version, "sum")))
		sb.WriteString(",toFloat64((quantilesTDigestMerge(0.25, 0.5, 0.75, 0.90, 0.95, 0.99, 0.999)(percentiles) AS digest)[1]) AS _val0")
		sb.WriteString(",toFloat64(digest[2]) AS _val1")
		sb.WriteString(",toFloat64(digest[3]) AS _val2")
		sb.WriteString(",toFloat64(digest[4]) AS _val3")
		sb.WriteString(",toFloat64(digest[5]) AS _val4")
		sb.WriteString(",toFloat64(digest[6]) AS _val5")
		sb.WriteString(",toFloat64(digest[7]) AS _val6")
		sb.WriteString(fmt.Sprintf(", %s as _minHost, %s as _maxHost", sqlMinHost(version), sqlMaxHost(version)))
		return 7, nil
	case data_model.DigestKindUnique:
		sb.WriteString(fmt.Sprintf(",toFloat64(%s(count)) AS _count", sqlAggFn(version, "sum")))
		sb.WriteString(",toFloat64(uniqMerge(uniq_state)) AS _val0")
		sb.WriteString(fmt.Sprintf(",%s AS _minHost, %s AS _maxHost", sqlMinHost(version), sqlMaxHost(version)))
		return 1, nil
	default:
		return 0, fmt.Errorf("unsupported operation kind: %q", kind)
	}
}

func (pq *pointsQuery) loadPointsQuery(lod data_model.LOD, utcOffset int64, useTime bool) (string, *pointsSelectCols, error) {
	switch lod.Version {
	case Version3:
		return pq.loadPointsQueryV3(lod, utcOffset, useTime)
	default:
		return pq.loadPointsQueryV2(lod, utcOffset, useTime)
	}
}

func (pq *pointsQuery) loadPointsQueryV2(lod data_model.LOD, utcOffset int64, useTime bool) (string, *pointsSelectCols, error) {
	var sb strings.Builder
	sb.WriteString("SELECT ")
	if lod.StepSec == _1M {
		sb.WriteString(fmt.Sprintf("toInt64(toDateTime(toStartOfInterval(time, INTERVAL 1 MONTH, '%s'), '%s')) AS _time,", lod.Location.String(), lod.Location.String()))
		sb.WriteString(fmt.Sprintf("toInt64(toDateTime(_time,'%s')+INTERVAL 1 MONTH)-_time AS _stepSec", lod.Location.String()))
	} else {
		sb.WriteString(fmt.Sprintf("toInt64(toStartOfInterval(time+%d,INTERVAL %d second))-%d AS _time,", utcOffset, lod.StepSec, utcOffset))
		sb.WriteString(fmt.Sprintf("toInt64(%d) AS _stepSec", lod.StepSec))
	}
	for _, b := range pq.by {
		sb.WriteString(fmt.Sprintf(",%s AS key%s", mappedColumnName(lod.HasPreKey, b, pq.preKeyTagID), b))
	}
	cnt, err := loadPointsSelectWhat(&sb, pq, lod.Version)
	if err != nil {
		return "", nil, err
	}
	sb.WriteString(" FROM ")
	sb.WriteString(pq.preKeyTableName(lod))
	writeWhereTimeFilter(&sb, &lod)
	writeMetricFilter(&sb, pq.metricID, pq.filterIn.Metrics, pq.filterNotIn.Metrics, lod.Version)
	writeV1DateFilter(&sb, &lod)
	for i, ids := range pq.filterIn.Tags {
		if len(ids.Values) > 0 {
			sb.WriteString(" AND ")
			sb.WriteString(mappedColumnName(lod.HasPreKey, format.TagID(i), pq.preKeyTagID))
			sb.WriteString(" IN (")
			expandTagsMapped(&sb, ids.Values)
			sb.WriteString(")")
		}
	}
	if len(pq.filterIn.StringTop) > 0 {
		sb.WriteString(" AND skey IN (")
		expandStrings(&sb, pq.filterIn.StringTop)
		sb.WriteString(")")
	}
	for i, ids := range pq.filterNotIn.Tags {
		if len(ids.Values) > 0 {
			sb.WriteString(" AND ")
			sb.WriteString(mappedColumnName(lod.HasPreKey, format.TagID(i), pq.preKeyTagID))
			sb.WriteString(" NOT IN (")
			expandTagsMapped(&sb, ids.Values)
			sb.WriteString(")")
		}
	}
	if len(pq.filterNotIn.StringTop) > 0 {
		sb.WriteString(" AND skey NOT IN (")
		expandStrings(&sb, pq.filterNotIn.StringTop)
		sb.WriteString(")")
	}
	sb.WriteString(" GROUP BY _time")
	for _, b := range pq.by {
		sb.WriteString(fmt.Sprintf(",%s AS key%s", mappedColumnName(lod.HasPreKey, b, pq.preKeyTagID), b))
	}
	var having bool
	switch pq.kind {
	case data_model.DigestKindPercentiles:
		sb.WriteString(" HAVING not(isNaN(_val0) AND isNaN(_val1) AND isNaN(_val2) AND isNaN(_val3) AND isNaN(_val4) AND isNaN(_val5) AND isNaN(_val6))")
		having = true
	case data_model.DigestKindPercentilesLow:
		sb.WriteString(" HAVING not(isNaN(_val0) AND isNaN(_val1) AND isNaN(_val2) AND isNaN(_val3))")
		having = true
	}
	limit := maxSeriesRows
	if pq.sort != sortNone {
		limit = maxTableRows
		if having {
			sb.WriteString(" AND _count>0")
		} else {
			sb.WriteString(" HAVING _count>0")
		}
		sb.WriteString(" ORDER BY _time")
		for _, b := range pq.by {
			sb.WriteString(fmt.Sprintf(",%s AS key%s", mappedColumnName(lod.HasPreKey, b, pq.preKeyTagID), b))
		}
		if pq.sort == sortDescending {
			sb.WriteString(" DESC")
		}
	}
	sb.WriteString(fmt.Sprintf(" LIMIT %v SETTINGS optimize_aggregation_in_order=1", limit))
	cols := newPointsSelectColsV2(pointsQueryMeta{
		queryMeta: queryMeta{
			metricID: pq.metricID,
			kind:     pq.kind,
			user:     pq.user,
		},
		vals:       cnt,
		tags:       pq.by,
		minMaxHost: pq.kind != data_model.DigestKindCount,
		version:    lod.Version,
	}, useTime)
	return sb.String(), cols, err
}

func (pq *pointsQuery) loadPointsQueryV3(lod data_model.LOD, utcOffset int64, useTime bool) (string, *pointsSelectCols, error) {
	var sb strings.Builder
	sb.WriteString("SELECT ")
	if lod.StepSec == _1M {
		sb.WriteString(fmt.Sprintf("toInt64(toDateTime(toStartOfInterval(time,INTERVAL 1 MONTH,'%s'),'%s')) AS _time,", lod.Location.String(), lod.Location.String()))
		sb.WriteString(fmt.Sprintf("toInt64(toDateTime(_time,'%s')+INTERVAL 1 MONTH)-_time AS _stepSec", lod.Location.String()))
	} else {
		sb.WriteString(fmt.Sprintf("toInt64(toStartOfInterval(time+%d,INTERVAL %d second))-%d AS _time,", utcOffset, lod.StepSec, utcOffset))
		sb.WriteString(fmt.Sprintf("toInt64(%d) AS _stepSec", lod.StepSec))
	}
	for _, b := range pq.by {
		if b != format.StringTopTagID {
			sb.WriteString(fmt.Sprintf(",%s AS tag%s", mappedColumnNameV3(b), b))
		}
		sb.WriteString(fmt.Sprintf(",%s AS stag%s", unmappedColumnNameV3(b), b))
	}
	cnt, err := loadPointsSelectWhat(&sb, pq, lod.Version)
	if err != nil {
		return "", nil, err
	}
	sb.WriteString(" FROM ")
	sb.WriteString(pq.preKeyTableName(lod))
	writeWhereTimeFilter(&sb, &lod)
	sb.WriteString(" AND index_type=0")
	writeMetricFilter(&sb, pq.metricID, pq.filterIn.Metrics, pq.filterNotIn.Metrics, lod.Version)
	writeTagCond(&sb, pq.filterIn, true)
	writeTagCond(&sb, pq.filterNotIn, false)
	sb.WriteString(" GROUP BY _time")
	for _, b := range pq.by {
		if b != format.StringTopTagID {
			sb.WriteString(",")
			sb.WriteString(mappedColumnNameV3(b))
			sb.WriteString(" AS tag")
			sb.WriteString(b)
		}
		sb.WriteString(",")
		sb.WriteString(unmappedColumnNameV3(b))
		sb.WriteString(" AS stag")
		sb.WriteString(b)
	}
	if pq.sort != sortNone {
		sb.WriteString(" HAVING _count>0")
		switch pq.kind {
		case data_model.DigestKindPercentiles:
			sb.WriteString("AND not(isNaN(_val0) AND isNaN(_val1) AND isNaN(_val2) AND isNaN(_val3) AND isNaN(_val4) AND isNaN(_val5) AND isNaN(_val6))")
		case data_model.DigestKindPercentilesLow:
			sb.WriteString("AND not(isNaN(_val0) AND isNaN(_val1) AND isNaN(_val2) AND isNaN(_val3))")
		}
		sb.WriteString(" ORDER BY _time")
		for _, b := range pq.by {
			if b != format.StringTopTagID {
				sb.WriteString(fmt.Sprintf(",%s AS tag%s", mappedColumnNameV3(b), b))
			}
			sb.WriteString(fmt.Sprintf(",%s AS stag%s", unmappedColumnNameV3(b), b))
		}
		if pq.sort == sortDescending {
			sb.WriteString(" DESC")
		}
		sb.WriteString(" LIMIT ")
		sb.WriteString(fmt.Sprint(maxTableRows))
	} else {
		sb.WriteString(" LIMIT ")
		sb.WriteString(fmt.Sprint(maxSeriesRows))
	}
	sb.WriteString(" SETTINGS optimize_aggregation_in_order=1")
	cols := newPointsSelectColsV3(pointsQueryMeta{
		queryMeta: queryMeta{
			metricID: pq.metricID,
			kind:     pq.kind,
			user:     pq.user,
		},
		vals:       cnt,
		tags:       pq.by,
		minMaxHost: pq.kind != data_model.DigestKindCount,
		version:    lod.Version,
	}, useTime)
	return sb.String(), cols, err
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

func writeMetricFilter(sb *strings.Builder, metricID int32, filterIn, filterNotIn []*format.MetricMetaValue, version string) {
	if metricID != 0 {
		sb.WriteString(" AND ")
		sb.WriteString(metricColumn(version))
		sb.WriteString("=")
		sb.WriteString(fmt.Sprint(metricID))
		return
	}
	if len(filterIn) != 0 {
		sb.WriteString(" AND ")
		sb.WriteString(metricColumn(version))
		sb.WriteString(" IN (")
		sb.WriteString(fmt.Sprint(filterIn[0].MetricID))
		for i := 1; i < len(filterIn); i++ {
			sb.WriteByte(',')
			sb.WriteString(fmt.Sprint(filterIn[i].MetricID))
		}
		sb.WriteByte(')')
	}
	if len(filterNotIn) != 0 {
		sb.WriteString(" AND ")
		sb.WriteString(metricColumn(version))
		sb.WriteString(" NOT IN (")
		sb.WriteString(fmt.Sprint(filterNotIn[0].MetricID))
		for i := 1; i < len(filterNotIn); i++ {
			sb.WriteByte(',')
			sb.WriteString(fmt.Sprint(filterNotIn[i].MetricID))
		}
		sb.WriteByte(')')
	}
}

func writeWhereTimeFilter(sb *strings.Builder, lod *data_model.LOD) {
	sb.WriteString(" WHERE time>=")
	sb.WriteString(fmt.Sprint(lod.FromSec))
	sb.WriteString(" AND time<")
	sb.WriteString(fmt.Sprint(lod.ToSec))
}

func writeV1DateFilter(sb *strings.Builder, lod *data_model.LOD) {
	if lod.Version != Version1 {
		return
	}
	sb.WriteString(" AND date>=toDate(")
	sb.WriteString(fmt.Sprint(lod.FromSec))
	sb.WriteString(") AND date<=toDate(")
	sb.WriteString(fmt.Sprint(lod.ToSec))
	sb.WriteString(")")
}

func metricColumn(version string) string {
	if version == Version1 {
		return "stats"
	}
	return "metric"
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

func (pq *pointsQuery) preKeyTableName(lod data_model.LOD) string {
	var usePreKey bool
	if lod.HasPreKey {
		usePreKey = lod.PreKeyOnly ||
			pq.filterIn.Contains(pq.preKeyTagX) ||
			pq.filterNotIn.Contains(pq.preKeyTagX)
		if !usePreKey {
			for _, v := range pq.by {
				if v == format.TagID(pq.preKeyTagX) {
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
			pq.filterIn.Contains(pq.preKeyTagX) ||
			pq.filterNotIn.Contains(pq.preKeyTagX)))
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
	if tagID == format.StringTopTagID {
		return "tag" + format.StringTopTagIDV3
	}
	return "tag" + tagID
}

func unmappedColumnNameV3(tagID string) string {
	if tagID == format.StringTopTagID {
		return "stag" + format.StringTopTagIDV3
	}
	return "stag" + tagID
}

func expandTagsMapped(sb *strings.Builder, s []data_model.TagValue) {
	if len(s) == 0 {
		return
	}
	sb.WriteString(fmt.Sprint(s[0].Mapped))
	for i := 1; i < len(s); i++ {
		sb.WriteString(",")
		sb.WriteString(fmt.Sprint(s[i].Mapped))
	}
}

var escapeReplacer = strings.NewReplacer(`'`, `\'`)

func expandStrings(sb *strings.Builder, s []string) {
	if len(s) == 0 {
		return
	}
	sb.WriteString("'")
	escapeReplacer.WriteString(sb, s[0])
	sb.WriteString("'")
	for i := 1; i < len(s); i++ {
		sb.WriteString(",'")
		escapeReplacer.WriteString(sb, s[i])
		sb.WriteString("'")
	}
}
