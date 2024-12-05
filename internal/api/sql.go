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

const (
	sortNone querySort = iota
	sortAscending
	sortDescending
)

var escapeReplacer = strings.NewReplacer(`'`, `\'`)

type querySort int

type queryBuilder struct {
	version     string
	user        string
	metric      *format.MetricMetaValue
	what        tsWhat
	by          []string
	filterIn    data_model.TagFilters
	filterNotIn data_model.TagFilters
	sort        querySort // for table view requests
	strcmpOff   bool      // version 3 experimental
	minMaxHost  [2]bool   // "min" at [0], "max" at [1]

	// tag values query
	tagID      string
	numResults int
}

type pointsQueryMeta struct {
	queryMeta
	vals       int
	tags       []string
	minMaxHost [2]bool
	version    string
}

type tagValuesQueryMeta struct {
	queryMeta
	stag bool
	// both mapped and unmapped values for v3 requests
	mixed bool
}

type queryMeta struct {
	metricID int32
	what     tsWhat
	user     string
}

func (q *queryBuilder) stringTag() bool {
	return q.tagID == format.StringTopTagID
}

func (pq *queryBuilder) cacheKey() string {
	var sb strings.Builder
	sb.WriteString(`{"v":`)
	switch pq.version {
	case Version1:
		sb.WriteString(Version1)
	default:
		sb.WriteString(Version3)
	}
	sb.WriteString(`,"m":`)
	sb.WriteString(fmt.Sprint(pq.metricID()))
	sb.WriteString(`,"pk":"`)
	sb.WriteString(pq.preKeyTagID())
	sb.WriteString(`","st":`)
	sb.WriteString(fmt.Sprint(pq.isStringTop()))
	sb.WriteString(`,"what":[`)
	i := 0
	for ; i < len(pq.what) && pq.what[i].What != data_model.DigestUnspecified; i++ {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(`"`)
		sb.WriteString(pq.what[i].String())
		sb.WriteString(`"`)
	}
	if pq.minMaxHost[0] {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(`"minhost"`)
		i++
	}
	if pq.minMaxHost[1] {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(`"maxhost"`)
		i++
	}
	sb.WriteString(`],"by":[`)
	if len(pq.by) != 0 {
		sort.Strings(pq.by)
		sb.WriteString(`"`)
		sb.WriteString(pq.by[0])
		for i := 1; i < len(pq.by); i++ {
			sb.WriteString(`","`)
			sb.WriteString(pq.by[i])
		}
		sb.WriteString(`"`)
	}
	sb.WriteString(`],"inc":`)
	s := make([]string, 0, 16)
	s = writeTagFiltersCacheKey(&sb, pq.filterIn, s)
	sb.WriteString(`,"exl":`)
	writeTagFiltersCacheKey(&sb, pq.filterNotIn, s)
	sb.WriteString(`,"sort":`)
	sb.WriteString(fmt.Sprint(int(pq.sort)))
	sb.WriteString("}")
	return sb.String()
}

func writeTagFiltersCacheKey(sb *strings.Builder, f data_model.TagFilters, s []string) []string {
	var n int
	sb.WriteString("{")
	for i, filter := range f.Tags {
		if filter.Empty() {
			continue
		}
		if n != 0 {
			sb.WriteString(",")
		}
		sb.WriteString(`"`)
		sb.WriteString(fmt.Sprint(i))
		sb.WriteString(`":`)
		if filter.Re2 != "" {
			sb.WriteString(`"`)
			sb.WriteString(filter.Re2)
			sb.WriteString(`"`)
		} else if len(filter.Values) != 0 {
			s := s[:0]
			for _, v := range filter.Values {
				s = append(s, v.String())
			}
			sort.Strings(s)
			sb.WriteString(`["`)
			sb.WriteString(s[0])
			for i := 1; i < len(s); i++ {
				sb.WriteString(`","`)
				sb.WriteString(s[i])
			}
			sb.WriteString(`"]`)
		}
		n++
	}
	if f.StringTopRe2 != "" {
		if n != 0 {
			sb.WriteString(",")
		}
		sb.WriteString(`"_s":"`)
		sb.WriteString(f.StringTopRe2)
		sb.WriteString(`"`)
	} else if len(f.StringTop) != 0 {
		s = append(s[:0], f.StringTop...)
		sort.Strings(s)
		if n != 0 {
			sb.WriteString(",")
		}
		sb.WriteString(`"_s":["`)
		sb.WriteString(s[0])
		for i := 1; i < len(s); i++ {
			sb.WriteString(`","`)
			sb.WriteString(s[i])
		}
		sb.WriteString(`"]`)
	}
	sb.WriteString("}")
	return s
}

func (pq *queryMeta) isLight() bool {
	for i := 0; i < len(pq.what); i++ {
		switch pq.what[i].What {
		case data_model.DigestUnique, data_model.DigestPercentile:
			return false
		case data_model.DigestUnspecified:
			return true
		}
	}
	return true
}

func (pq *queryMeta) IsHardware() bool {
	return format.HardwareMetric(pq.metricID)
}

func tagValuesQuery(pq *queryBuilder, lod data_model.LOD) (string, *tagValuesSelectCols) {
	switch lod.Version {
	case Version3:
		return pq.tagValuesQueryV3(&lod)
	default:
		return pq.tagValuesQueryV2(&lod)
	}
}

func (pq *queryBuilder) tagValuesQueryV2(lod *data_model.LOD) (string, *tagValuesSelectCols) {
	meta := tagValuesQueryMeta{}
	valueName := "_value"
	if pq.stringTag() {
		meta.stag = true
		valueName = "_string_value"
	}
	// no need to escape anything as long as table and tag names are fixed
	var sb strings.Builder
	sb.WriteString("SELECT ")
	sb.WriteString(pq.mappedColumnNameV2(pq.tagID, lod))
	sb.WriteString(" AS ")
	sb.WriteString(valueName)
	sb.WriteString(",toFloat64(")
	sb.WriteString(sqlAggFn(lod.Version, "sum"))
	sb.WriteString("(count)) AS _count FROM ")
	sb.WriteString(pq.preKeyTableName(lod))
	writeWhereTimeFilter(&sb, lod)
	writeMetricFilter(&sb, pq.metricID(), pq.filterIn.Metrics, pq.filterNotIn.Metrics, lod.Version)
	writeV1DateFilter(&sb, lod)
	pq.writeTagCond(&sb, lod, true)
	pq.writeTagCond(&sb, lod, false)
	sb.WriteString(" GROUP BY ")
	sb.WriteString(pq.mappedColumnNameV2(pq.tagID, lod))
	sb.WriteString(" HAVING _count>0 ORDER BY _count DESC,")
	sb.WriteString(valueName)
	sb.WriteString(" LIMIT ")
	sb.WriteString(fmt.Sprint(pq.numResults + 1))
	sb.WriteString(" SETTINGS optimize_aggregation_in_order=1")
	return sb.String(), newTagValuesSelectCols(meta)
}

func (pq *queryBuilder) tagValuesQueryV3(lod *data_model.LOD) (string, *tagValuesSelectCols) {
	var sb strings.Builder
	sb.WriteString("SELECT ")
	sb.WriteString(pq.mappedColumnNameV3(pq.tagID, lod))
	sb.WriteString(" AS _mapped,")
	sb.WriteString(pq.unmappedColumnNameV3(pq.tagID))
	sb.WriteString(" AS _unmapped,toFloat64(sum(count)) AS _count FROM ")
	sb.WriteString(pq.preKeyTableName(lod))
	writeWhereTimeFilter(&sb, lod)
	writeMetricFilter(&sb, pq.metricID(), pq.filterIn.Metrics, pq.filterNotIn.Metrics, lod.Version)
	pq.writeTagCond(&sb, lod, true)
	pq.writeTagCond(&sb, lod, false)
	sb.WriteString(" GROUP BY _mapped,_unmapped HAVING _count>0 ORDER BY _count,_mapped,_unmapped DESC LIMIT ")
	sb.WriteString(fmt.Sprint(pq.numResults + 1))
	sb.WriteString(" SETTINGS optimize_aggregation_in_order=1")
	return sb.String(), newTagValuesSelectColsV3(tagValuesQueryMeta{mixed: true})
}

func (pq *queryBuilder) tagValueIDsQuery(lod data_model.LOD) (string, *tagValuesSelectCols) {
	switch lod.Version {
	case Version3:
		return pq.tagValueIDsQueryV3(&lod)
	default:
		return pq.tagValuesQueryV2(&lod)
	}
}

func (pq *queryBuilder) tagValueIDsQueryV3(lod *data_model.LOD) (string, *tagValuesSelectCols) {
	var sb strings.Builder
	sb.WriteString("SELECT ")
	sb.WriteString(pq.mappedColumnNameV3(pq.tagID, lod))
	sb.WriteString(" AS _mapped,toFloat64(sum(count)) AS _count FROM ")
	sb.WriteString(pq.preKeyTableName(lod))
	writeWhereTimeFilter(&sb, lod)
	writeMetricFilter(&sb, pq.metricID(), pq.filterIn.Metrics, pq.filterNotIn.Metrics, lod.Version)
	pq.writeTagCond(&sb, lod, true)
	pq.writeTagCond(&sb, lod, false)
	sb.WriteString(" GROUP BY _mapped HAVING _count>0 ORDER BY _count,_mapped DESC LIMIT ")
	sb.WriteString(fmt.Sprint(pq.numResults + 1))
	sb.WriteString(" SETTINGS optimize_aggregation_in_order=1")
	cols := &tagValuesSelectCols{tagValuesQueryMeta: tagValuesQueryMeta{}}
	cols.res = append(cols.res, proto.ResultColumn{Name: "_mapped", Data: &cols.valID})
	cols.res = append(cols.res, proto.ResultColumn{Name: "_count", Data: &cols.cnt})
	return sb.String(), cols
}

func (pq *queryBuilder) version3StrcmpOn(metric *format.MetricMetaValue, tagX int, lod *data_model.LOD) bool {
	version3StrcmpOn := lod.Version == Version3 && !pq.strcmpOff
	if !version3StrcmpOn || metric == nil || len(metric.Tags) <= tagX {
		return version3StrcmpOn
	}
	return metric.Tags[tagX].SkipMapping
}

func (pq *queryBuilder) writeTagCond(sb *strings.Builder, lod *data_model.LOD, in bool) {
	var f data_model.TagFilters
	var sep, predicate string
	if in {
		f = pq.filterIn
		sep, predicate = " OR ", " IN "
	} else {
		f = pq.filterNotIn
		sep, predicate = " AND ", " NOT IN "
	}
	metric := pq.singleMetric()
	for i, filter := range f.Tags {
		if filter.Empty() {
			continue
		}
		sb.WriteString(" AND (")
		// mapped
		tagID := format.TagID(i)
		version3StrcmpOn := pq.version3StrcmpOn(metric, i, lod)
		var hasMapped bool
		var hasValue bool
		var version3HasEmpty bool
		var started bool
		for _, v := range filter.Values {
			if version3StrcmpOn && v.Empty() {
				version3HasEmpty = true
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
					sb.WriteString(pq.mappedColumnName(tagID, lod))
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
		} else {
			if in {
				// empty positive filter means there are no items satisfaing search criteria
				sb.WriteString("0!=0")
			} else {
				// empty negative filter is "nop"
				sb.WriteString("0=0")
			}
			started = true
		}
		// not mapped
		if version3StrcmpOn {
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
				sb.WriteString(pq.unmappedColumnNameV3(tagID))
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
							sb.WriteString(pq.unmappedColumnNameV3(tagID))
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
		}
		// empty
		if version3HasEmpty {
			if started {
				sb.WriteString(sep)
			}
			if !in {
				sb.WriteString("NOT ")
			}
			sb.WriteString("(")
			sb.WriteString(pq.mappedColumnNameV3(tagID, lod))
			sb.WriteString("=0 AND ")
			sb.WriteString(pq.unmappedColumnNameV3(tagID))
			sb.WriteString("='')")
		}
		sb.WriteString(")")
	}
	// String top
	if f.StringTopRe2 != "" {
		sb.WriteString(" AND")
		if !in {
			sb.WriteString(" NOT")
		}
		sb.WriteString(" match(")
		sb.WriteString(pq.unmappedColumnName(format.StringTopTagID, lod.Version))
		sb.WriteString(",'")
		sb.WriteString(escapeReplacer.Replace(f.StringTopRe2))
		sb.WriteString("')")
	} else if len(f.StringTop) != 0 {
		sb.WriteString(" AND ")
		sb.WriteString(pq.unmappedColumnName(format.StringTopTagID, lod.Version))
		sb.WriteString(predicate)
		sb.WriteString(" ('")
		sb.WriteString(escapeReplacer.Replace(f.StringTop[0]))
		for i := 1; i < len(f.StringTop); i++ {
			sb.WriteString("','")
			sb.WriteString(escapeReplacer.Replace(f.StringTop[i]))
		}
		sb.WriteString("')")
	}
}

func loadPointsSelectWhat(sb *strings.Builder, pq *queryBuilder, queryMeta *pointsQueryMeta) error {
	version := queryMeta.version
	if version == Version1 && pq.isStringTop() {
		sb.WriteString(",toFloat64(sumMerge(count)) AS _val0")
		queryMeta.vals++
		return nil // count is the only column available
	}
	for i := 0; i < len(pq.what) && pq.what[i].What != data_model.DigestUnspecified; i++ {
		sb.WriteString(",")
		switch pq.what[i].What {
		case data_model.DigestAvg:
			sb.WriteString(fmt.Sprintf("toFloat64(%s(sum))/toFloat64(%s(count))", sqlAggFn(version, "sum"), sqlAggFn(version, "sum")))
		case data_model.DigestCount:
			sb.WriteString(fmt.Sprintf("toFloat64(%s(count))", sqlAggFn(version, "sum")))
		case data_model.DigestMax:
			sb.WriteString(fmt.Sprintf("toFloat64(%s(max))", sqlAggFn(version, "max")))
		case data_model.DigestMin:
			sb.WriteString(fmt.Sprintf("toFloat64(%s(min))", sqlAggFn(version, "min")))
		case data_model.DigestSum:
			sb.WriteString(fmt.Sprintf("toFloat64(%s(sum))", sqlAggFn(version, "sum")))
		case data_model.DigestStdDev:
			// https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance, "Naïve algorithm", poor numeric stability
			sb.WriteString(fmt.Sprintf("if(%s(count)<2,0,sqrt(greatest((%s(sumsquare)-pow(%s(sum),2)/%s(count))/(%s(count)-1),0)))",
				sqlAggFn(version, "sum"), sqlAggFn(version, "sum"), sqlAggFn(version, "sum"), sqlAggFn(version, "sum"), sqlAggFn(version, "sum")))
		case data_model.DigestPercentile:
			sb.WriteString("toFloat64((quantilesTDigestMerge(")
			sb.WriteString(fmt.Sprintf("%.3f", pq.what[i].Argument))
			var n int
			for j := i + 1; j < len(pq.what) && pq.what[j].What == data_model.DigestPercentile; j++ {
				sb.WriteString(fmt.Sprintf(",%.3f", pq.what[j].Argument))
				n++
			}
			sb.WriteString(")(percentiles) AS digest)[1])")
			for j := 0; j < n; j++ {
				sb.WriteString(" AS _val")
				sb.WriteString(fmt.Sprint(i))
				queryMeta.vals++
				i++
				sb.WriteString(",toFloat64(digest[")
				sb.WriteString(fmt.Sprint(j + 2))
				sb.WriteString("])")
			}
		case data_model.DigestCardinality:
			sb.WriteString("toFloat64(sum(1))")
		case data_model.DigestUnique:
			sb.WriteString("toFloat64(uniqMerge(uniq_state))")
		default:
			return fmt.Errorf("unsupported operation kind: %q", pq.what[i])
		}
		sb.WriteString(" AS _val")
		sb.WriteString(fmt.Sprint(i))
		queryMeta.vals++
	}
	if pq.minMaxHost[0] {
		sb.WriteString(",")
		sb.WriteString(sqlMinHost(version))
		sb.WriteString(" AS _minHost")
		queryMeta.minMaxHost[0] = true
	}
	if pq.minMaxHost[1] {
		sb.WriteString(",")
		sb.WriteString(sqlMaxHost(version))
		sb.WriteString(" AS _maxHost")
		queryMeta.minMaxHost[1] = true
	}
	return nil
}

func (pq *queryBuilder) loadPointsQuery(lod data_model.LOD, utcOffset int64, useTime bool) (string, *pointsSelectCols, error) {
	switch lod.Version {
	case Version3:
		return pq.loadPointsQueryV3(&lod, utcOffset, useTime)
	default:
		return pq.loadPointsQueryV2(&lod, utcOffset, useTime)
	}
}

func (pq *queryBuilder) loadPointsQueryV2(lod *data_model.LOD, utcOffset int64, useTime bool) (string, *pointsSelectCols, error) {
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
		sb.WriteString(fmt.Sprintf(",%s AS key%s", pq.mappedColumnNameV2(b, lod), b))
	}
	queryMeta := pointsQueryMeta{
		queryMeta: queryMeta{
			metricID: pq.metricID(),
			what:     pq.what,
			user:     pq.user,
		},
		tags:    pq.by,
		version: lod.Version,
	}
	if err := loadPointsSelectWhat(&sb, pq, &queryMeta); err != nil {
		return "", nil, err
	}
	sb.WriteString(" FROM ")
	sb.WriteString(pq.preKeyTableName(lod))
	writeWhereTimeFilter(&sb, lod)
	writeMetricFilter(&sb, pq.metricID(), pq.filterIn.Metrics, pq.filterNotIn.Metrics, lod.Version)
	writeV1DateFilter(&sb, lod)
	pq.writeTagCond(&sb, lod, true)
	pq.writeTagCond(&sb, lod, false)
	sb.WriteString(" GROUP BY _time")
	for _, b := range pq.by {
		sb.WriteString(fmt.Sprintf(",%s AS key%s", pq.mappedColumnNameV2(b, lod), b))
	}
	limit := maxSeriesRows
	if pq.sort != sortNone {
		limit = maxTableRows
		// sb.WriteString(" HAVING _count>0")
		sb.WriteString(" ORDER BY _time")
		for _, b := range pq.by {
			sb.WriteString(fmt.Sprintf(",%s AS key%s", pq.mappedColumnNameV2(b, lod), b))
		}
		if pq.sort == sortDescending {
			sb.WriteString(" DESC")
		}
	}
	sb.WriteString(fmt.Sprintf(" LIMIT %v SETTINGS optimize_aggregation_in_order=1", limit))
	cols := newPointsSelectColsV2(queryMeta, useTime)
	return sb.String(), cols, nil
}

func (pq *queryBuilder) loadPointsQueryV3(lod *data_model.LOD, utcOffset int64, useTime bool) (string, *pointsSelectCols, error) {
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
			sb.WriteString(fmt.Sprintf(",%s AS tag%s", pq.mappedColumnNameV3(b, lod), b))
		}
		sb.WriteString(fmt.Sprintf(",%s AS stag%s", pq.unmappedColumnNameV3(b), b))
	}
	queryMeta := pointsQueryMeta{
		queryMeta: queryMeta{
			metricID: pq.metricID(),
			what:     pq.what,
			user:     pq.user,
		},
		tags:    pq.by,
		version: lod.Version,
	}
	if err := loadPointsSelectWhat(&sb, pq, &queryMeta); err != nil {
		return "", nil, err
	}
	sb.WriteString(" FROM ")
	sb.WriteString(pq.preKeyTableName(lod))
	writeWhereTimeFilter(&sb, lod)
	sb.WriteString(" AND index_type=0")
	writeMetricFilter(&sb, pq.metricID(), pq.filterIn.Metrics, pq.filterNotIn.Metrics, lod.Version)
	pq.writeTagCond(&sb, lod, true)
	pq.writeTagCond(&sb, lod, false)
	sb.WriteString(" GROUP BY _time")
	for _, b := range pq.by {
		if b != format.StringTopTagID {
			sb.WriteString(",")
			sb.WriteString(pq.mappedColumnNameV3(b, lod))
			sb.WriteString(" AS tag")
			sb.WriteString(b)
		}
		sb.WriteString(",")
		sb.WriteString(pq.unmappedColumnNameV3(b))
		sb.WriteString(" AS stag")
		sb.WriteString(b)
	}
	if pq.sort != sortNone {
		// sb.WriteString(" HAVING _count>0")
		sb.WriteString(" ORDER BY _time")
		for _, b := range pq.by {
			if b != format.StringTopTagID {
				sb.WriteString(fmt.Sprintf(",%s AS tag%s", pq.mappedColumnNameV3(b, lod), b))
			}
			sb.WriteString(fmt.Sprintf(",%s AS stag%s", pq.unmappedColumnNameV3(b), b))
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
	cols := newPointsSelectColsV3(queryMeta, useTime)
	return sb.String(), cols, nil
}

func (pq *queryBuilder) isStringTop() bool {
	v := pq.singleMetric()
	return v != nil && v.StringTopDescription != ""
}

func (pq *queryBuilder) metricID() int32 {
	if v := pq.singleMetric(); v != nil {
		return v.MetricID
	}
	return 0
}

func (pq *queryBuilder) preKeyTagID() string {
	if v := pq.singleMetric(); v != nil {
		return v.PreKeyTagID
	}
	return ""
}

func (pq *queryBuilder) preKeyTagX() int {
	if v := pq.singleMetric(); v != nil {
		return format.TagIndex(v.PreKeyTagID)
	}
	return -1
}

func (pq *queryBuilder) singleMetric() *format.MetricMetaValue {
	if pq.metric != nil {
		return pq.metric
	}
	if len(pq.filterIn.Metrics) == 1 {
		return pq.filterIn.Metrics[0]
	}
	return nil
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
	emptyFilter := len(filterIn) == 0 && len(filterNotIn) == 0
	if metricID != 0 || emptyFilter {
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

func (pq *queryBuilder) preKeyTableName(lod *data_model.LOD) string {
	var usePreKey bool
	if lod.HasPreKey {
		preKeyTagX := pq.preKeyTagX()
		usePreKey = lod.PreKeyOnly ||
			pq.filterIn.Contains(preKeyTagX) ||
			pq.filterNotIn.Contains(preKeyTagX)
		if !usePreKey {
			for _, v := range pq.by {
				if v == pq.preKeyTagID() {
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

func (pq *queryBuilder) mappedColumnName(tagID string, lod *data_model.LOD) string {
	switch lod.Version {
	case Version3:
		return pq.mappedColumnNameV3(tagID, lod)
	default:
		return pq.mappedColumnNameV2(tagID, lod)
	}
}

func (pq *queryBuilder) mappedColumnNameV2(tagID string, lod *data_model.LOD) string {
	// intentionally not using constants from 'format' package,
	// because it is a table column name, not an external contract
	switch tagID {
	case format.StringTopTagID:
		return "skey"
	case format.ShardTagID:
		return "_shard_num"
	default:
		if lod.HasPreKey && tagID == pq.preKeyTagID() {
			return "prekey"
		}
		// 'tagID' assumed to be a number from 0 to 15,
		// dont't verify (ClickHouse just won't find a column)
		return "key" + tagID
	}
}

func (pq *queryBuilder) mappedColumnNameV3(tagID string, lod *data_model.LOD) string {
	switch tagID {
	case format.StringTopTagID:
		return "tag" + format.StringTopTagIDV3
	case format.ShardTagID:
		return "_shard_num"
	default:
		if lod.HasPreKey && tagID == pq.preKeyTagID() {
			return "pre_tag"
		}
		return "tag" + tagID
	}
}

func (pq *queryBuilder) unmappedColumnName(tagID, version string) string {
	switch version {
	case Version3:
		return pq.unmappedColumnNameV3(tagID)
	default:
		return "skey"
	}
}

func (pq *queryBuilder) unmappedColumnNameV3(tagID string) string {
	if tagID == format.StringTopTagID {
		return "stag" + format.StringTopTagIDV3
	}
	return "stag" + tagID
}
