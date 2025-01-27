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
	strings.Builder
	cacheKey    string
	version     string
	user        string
	metric      *format.MetricMetaValue
	what        tsWhat
	by          []int
	filterIn    data_model.TagFilters
	filterNotIn data_model.TagFilters
	sort        querySort // for table view requests
	strcmpOff   bool      // version 3 experimental
	minMaxHost  [2]bool   // "min" at [0], "max" at [1]
	point       bool      // point query
	utcOffset   int64

	// tag values query
	tagID      string
	numResults int
}

type writeTagsOptions struct {
	cols  bool
	time  bool
	comma bool
}

func (b *queryBuilder) stringTag() bool {
	return b.tagID == format.StringTopTagID
}

func (b *queryBuilder) getOrBuildCacheKey() string {
	if b.cacheKey != "" {
		return b.cacheKey
	}
	b.WriteString(`{"v":`)
	switch b.version {
	case Version1:
		b.WriteString(Version1)
	default:
		b.WriteString(Version3)
	}
	b.WriteString(`,"m":`)
	b.WriteString(fmt.Sprint(b.metricID()))
	b.WriteString(`,"pk":"`)
	b.WriteString(b.preKeyTagID())
	b.WriteString(`","st":`)
	b.WriteString(fmt.Sprint(b.isStringTop()))
	b.WriteString(`,"what":[`)
	i := 0
	lastWhat := data_model.DigestUnspecified
	for j := 0; b.what.specifiedAt(j); j++ {
		if lastWhat == b.what[j].What {
			continue
		}
		lastWhat = b.what[j].What
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(`"`)
		b.WriteString(b.what[j].What.String())
		b.WriteString(`"`)
		i++
	}
	if b.minMaxHost[0] {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(`"minhost"`)
		i++
	}
	if b.minMaxHost[1] {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(`"maxhost"`)
		i++
	}
	b.WriteString(`],"by":[`)
	if len(b.by) != 0 {
		sort.Ints(b.by)
		b.WriteString(`"`)
		b.WriteString(fmt.Sprint(b.by[0]))
		for i := 1; i < len(b.by); i++ {
			b.WriteString(`","`)
			b.WriteString(fmt.Sprint(b.by[i]))
		}
		b.WriteString(`"`)
	}
	b.WriteString(`],"inc":`)
	s := make([]string, 0, 16)
	s = b.writeTagFiltersCacheKey(b.filterIn, s)
	b.WriteString(`,"exl":`)
	b.writeTagFiltersCacheKey(b.filterNotIn, s)
	b.WriteString(`,"sort":`)
	b.WriteString(fmt.Sprint(int(b.sort)))
	b.WriteString("}")
	b.cacheKey = b.String()
	b.Reset()
	return b.cacheKey
}

func (b *queryBuilder) writeTagFiltersCacheKey(f data_model.TagFilters, s []string) []string {
	var n int
	b.WriteString("{")
	for i, filter := range f.Tags {
		if filter.Empty() {
			continue
		}
		if n != 0 {
			b.WriteString(",")
		}
		b.WriteString(`"`)
		b.WriteString(fmt.Sprint(i))
		b.WriteString(`":`)
		if filter.Re2 != "" {
			b.WriteString(`"`)
			b.WriteString(filter.Re2)
			b.WriteString(`"`)
		} else if len(filter.Values) != 0 {
			s := s[:0]
			for _, v := range filter.Values {
				s = append(s, v.String())
			}
			sort.Strings(s)
			b.WriteString(`["`)
			b.WriteString(s[0])
			for i := 1; i < len(s); i++ {
				b.WriteString(`","`)
				b.WriteString(s[i])
			}
			b.WriteString(`"]`)
		}
		n++
	}
	if f.StringTopRe2 != "" {
		if n != 0 {
			b.WriteString(",")
		}
		b.WriteString(`"_s":"`)
		b.WriteString(f.StringTopRe2)
		b.WriteString(`"`)
	} else if len(f.StringTop) != 0 {
		s = append(s[:0], f.StringTop...)
		sort.Strings(s)
		if n != 0 {
			b.WriteString(",")
		}
		b.WriteString(`"_s":["`)
		b.WriteString(s[0])
		for i := 1; i < len(s); i++ {
			b.WriteString(`","`)
			b.WriteString(s[i])
		}
		b.WriteString(`"]`)
	}
	b.WriteString("}")
	return s
}

func (b *queryBuilder) isLight() bool {
	for i := 0; b.what.specifiedAt(i); i++ {
		switch b.what[i].What {
		case data_model.DigestUnique, data_model.DigestPercentile:
			return false
		}
	}
	return true
}

func (b *queryBuilder) isHardware() bool {
	return format.HardwareMetric(b.metricID())
}

func tagValuesQuery(pq *queryBuilder, lod data_model.LOD) *tagValuesSelectCols {
	switch lod.Version {
	case Version3:
		return pq.tagValuesQueryV3(&lod)
	default:
		return pq.tagValuesQueryV2(&lod)
	}
}

func (b *queryBuilder) tagValueIDsQuery(lod data_model.LOD) *tagValuesSelectCols {
	switch lod.Version {
	case Version3:
		return b.tagValueIDsQueryV3(&lod)
	default:
		return b.tagValuesQueryV2(&lod)
	}
}

func (b *queryBuilder) writeTagCond(lod *data_model.LOD, in bool) {
	var f data_model.TagFilters
	var sep, predicate string
	if in {
		f = b.filterIn
		sep, predicate = " OR ", " IN "
	} else {
		f = b.filterNotIn
		sep, predicate = " AND ", " NOT IN "
	}
	version3StrcmpOn := b.version3StrcmpOn(lod)
	for i, filter := range f.Tags {
		if filter.Empty() {
			continue
		}
		b.WriteString(" AND (")
		// mapped
		tagID := format.TagID(i)
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
						b.WriteString(sep)
					} else {
						started = true
					}
					b.WriteString(b.mappedColumnName(tagID, lod))
					b.WriteString(predicate)
					b.WriteString("(")
					hasMapped = true
				} else {
					b.WriteString(",")
				}
				b.WriteString(fmt.Sprint(v.Mapped))
			}
		}
		if hasMapped {
			b.WriteString(")")
		} else {
			if in {
				// empty positive filter means there are no items satisfaing search criteria
				b.WriteString("0!=0")
			} else {
				// empty negative filter is "nop"
				b.WriteString("0=0")
			}
			started = true
		}
		// not mapped
		if version3StrcmpOn {
			if filter.Re2 != "" {
				if started {
					b.WriteString(sep)
				} else {
					started = true
				}
				if !in {
					b.WriteString("NOT ")
				}
				b.WriteString("match(")
				b.WriteString(b.unmappedColumnNameV3(tagID))
				b.WriteString(",'")
				b.WriteString(escapeReplacer.Replace(filter.Re2))
				b.WriteString("')")
			} else if hasValue {
				hasValue = false
				for _, v := range filter.Values {
					if v.Empty() {
						continue
					}
					if v.HasValue() {
						if !hasValue {
							if started {
								b.WriteString(sep)
							} else {
								started = true
							}
							b.WriteString(b.unmappedColumnNameV3(tagID))
							b.WriteString(predicate)
							b.WriteString("('")
							hasValue = true
						} else {
							b.WriteString("','")
						}
						b.WriteString(escapeReplacer.Replace(v.Value))
					}
				}
				b.WriteString("')")
			}
		}
		// empty
		if version3HasEmpty {
			if started {
				b.WriteString(sep)
			}
			if !in {
				b.WriteString("NOT ")
			}
			b.WriteString("(")
			b.WriteString(b.mappedColumnNameV3(tagID, lod))
			b.WriteString("=0 AND ")
			b.WriteString(b.unmappedColumnNameV3(tagID))
			b.WriteString("='')")
		}
		b.WriteString(")")
	}
	// String top
	if f.StringTopRe2 != "" {
		b.WriteString(" AND")
		if !in {
			b.WriteString(" NOT")
		}
		b.WriteString(" match(")
		b.WriteString(b.unmappedColumnName(format.StringTopTagID, lod.Version))
		b.WriteString(",'")
		b.WriteString(escapeReplacer.Replace(f.StringTopRe2))
		b.WriteString("')")
	} else if len(f.StringTop) != 0 {
		b.WriteString(" AND ")
		b.WriteString(b.unmappedColumnName(format.StringTopTagID, lod.Version))
		b.WriteString(predicate)
		b.WriteString(" ('")
		b.WriteString(escapeReplacer.Replace(f.StringTop[0]))
		for i := 1; i < len(f.StringTop); i++ {
			b.WriteString("','")
			b.WriteString(escapeReplacer.Replace(f.StringTop[i]))
		}
		b.WriteString("')")
	}
}

func (q *pointsSelectCols) writeMaybeCommaString(s string, comma bool) bool {
	if comma {
		q.WriteByte(',')
	}
	q.WriteString(s)
	return true
}

func (q *pointsSelectCols) loadPointsSelectWhat(lod *data_model.LOD) {
	var comma bool
	if !q.point {
		q.res = append(q.res, proto.ResultColumn{Name: "_time", Data: &q.time})
		if lod.StepSec == _1M {
			q.WriteString(fmt.Sprintf("toInt64(toDateTime(toStartOfInterval(time,INTERVAL 1 MONTH,'%s'),'%s')) AS _time,", lod.Location.String(), lod.Location.String()))
		} else {
			q.WriteString(fmt.Sprintf("toInt64(toStartOfInterval(time+%d,INTERVAL %d second))-%d AS _time,", q.utcOffset, lod.StepSec, q.utcOffset))
		}
		q.res = append(q.res, proto.ResultColumn{Name: "_stepSec", Data: &q.step})
		if lod.StepSec == _1M {
			q.WriteString(fmt.Sprintf("toInt64(toDateTime(_time,'%s')+INTERVAL 1 MONTH)-_time AS _stepSec", lod.Location.String()))
		} else {
			q.WriteString(fmt.Sprintf("toInt64(%d) AS _stepSec", lod.StepSec))
		}
		comma = true
	}
	if q.version == Version1 && q.isStringTop() {
		q.writeMaybeCommaString("toFloat64(sumMerge(count)) AS _val0", comma)
		q.res = append(q.res, proto.ResultColumn{Name: "_val0", Data: &q.count})
		return // count is the only column available
	}
	var has [data_model.DigestLast]bool
	var hasSumSquare bool
	for i, j := 0, 0; q.what.specifiedAt(i); i++ {
		if has[q.what[i].What] {
			continue
		}
		switch q.what[i].What {
		case data_model.DigestAvg:
			if !has[data_model.DigestSum] {
				q.selectSum(j)
				has[data_model.DigestSum] = true
				j++
			}
			if !has[data_model.DigestCount] {
				q.selectCount(j)
				has[data_model.DigestCount] = true
				j++
			}
		case data_model.DigestCount:
			q.selectCount(j)
			j++
		case data_model.DigestMax:
			colName := fmt.Sprintf("_val%d", j)
			q.res = append(q.res, proto.ResultColumn{Name: colName, Data: &q.max})
			q.writeMaybeCommaString(fmt.Sprintf("toFloat64(%s(max))", sqlAggFn(q.version, "max")), comma)
			q.WriteString(" AS ")
			q.WriteString(colName)
			j++
		case data_model.DigestMin:
			colName := fmt.Sprintf("_val%d", j)
			q.res = append(q.res, proto.ResultColumn{Name: colName, Data: &q.min})
			q.writeMaybeCommaString(fmt.Sprintf("toFloat64(%s(min))", sqlAggFn(q.version, "min")), comma)
			q.WriteString(" AS ")
			q.WriteString(colName)
			j++
		case data_model.DigestSum:
			q.selectSum(j)
			j++
		case data_model.DigestStdDev:
			if !has[data_model.DigestSum] {
				q.selectSum(j)
				has[data_model.DigestSum] = true
				j++
			}
			if !has[data_model.DigestCount] {
				q.selectCount(j)
				has[data_model.DigestCount] = true
				j++
			}
			if !hasSumSquare {
				colName := fmt.Sprintf("_val%d", j)
				q.res = append(q.res, proto.ResultColumn{Name: colName, Data: &q.sumsquare})
				q.writeMaybeCommaString(fmt.Sprintf("toFloat64(%s(sumsquare))", sqlAggFn(q.version, "sum")), comma)
				q.WriteString(" AS ")
				q.WriteString(colName)
				hasSumSquare = true
				j++
			}
		case data_model.DigestPercentile:
			columnName := fmt.Sprintf("_val%d", j)
			q.res = append(q.res, proto.ResultColumn{Name: columnName, Data: &q.percentile})
			q.writeMaybeCommaString("quantilesTDigestMergeState(0.5)(percentiles)", comma)
			q.WriteString(" AS ")
			q.WriteString(columnName)
			j++
		case data_model.DigestCardinality:
			colName := fmt.Sprintf("_val%d", j)
			q.res = append(q.res, proto.ResultColumn{Name: colName, Data: &q.cardinality})
			q.writeMaybeCommaString("toFloat64(sum(1))", comma)
			q.WriteString(" AS ")
			q.WriteString(colName)
			j++
		case data_model.DigestUnique:
			columnName := fmt.Sprintf("_val%d", j)
			q.res = append(q.res, proto.ResultColumn{Name: columnName, Data: &q.unique})
			q.writeMaybeCommaString("uniqMergeState(uniq_state)", comma)
			q.WriteString(" AS ")
			q.WriteString(columnName)
			j++
		default:
			panic(fmt.Errorf("unsupported operation kind: %q", q.what[i].What))
		}
		has[q.what[i].What] = true
		comma = true
	}
	if q.minMaxHost[0] {
		q.writeMaybeCommaString(sqlMinHost(q.version), comma)
		q.WriteString(" AS _minHost")
		switch q.version {
		case Version1:
			q.res = append(q.res, proto.ResultColumn{Name: "_minHost", Data: &q.minHostV1})
		case Version2:
			q.res = append(q.res, proto.ResultColumn{Name: "_minHost", Data: &q.minHostV2})
		case Version3:
			q.res = append(q.res, proto.ResultColumn{Name: "_minHost", Data: &q.minHostV3})
		}
	}
	if q.minMaxHost[1] {
		q.writeMaybeCommaString(sqlMaxHost(q.version), comma)
		q.WriteString(" AS _maxHost")
		switch q.version {
		case Version1:
			q.res = append(q.res, proto.ResultColumn{Name: "_maxHost", Data: &q.maxHostV1})
		case Version2:
			q.res = append(q.res, proto.ResultColumn{Name: "_maxHost", Data: &q.maxHostV2})
		case Version3:
			q.res = append(q.res, proto.ResultColumn{Name: "_maxHost", Data: &q.maxHostV3})
		}
	}
}

func (q *pointsSelectCols) selectSum(i int) {
	colName := fmt.Sprintf("_val%d", i)
	q.res = append(q.res, proto.ResultColumn{Name: colName, Data: &q.sum})
	q.WriteString(fmt.Sprintf(",toFloat64(%s(sum))", sqlAggFn(q.version, "sum")))
	q.WriteString(" AS ")
	q.WriteString(colName)

}

func (q *pointsSelectCols) selectCount(i int) {
	colName := fmt.Sprintf("_val%d", i)
	q.res = append(q.res, proto.ResultColumn{Name: colName, Data: &q.count})
	q.WriteString(fmt.Sprintf(",toFloat64(%s(count))", sqlAggFn(q.version, "sum")))
	q.WriteString(" AS ")
	q.WriteString(colName)
}

func (b *queryBuilder) loadPointsQuery(lod data_model.LOD) *pointsSelectCols {
	q := &pointsSelectCols{
		queryBuilder: b,
		version:      lod.Version,
	}
	q.WriteString("SELECT ")
	q.loadPointsSelectWhat(&lod)
	q.writeTags(&lod, writeTagsOptions{cols: true, comma: true})
	q.WriteString(" FROM ")
	q.WriteString(b.preKeyTableName(&lod))
	q.writeWhereTimeFilter(&lod)
	switch lod.Version {
	case Version1:
		q.writeV1DateFilter(&lod)
	case Version3:
		q.WriteString(" AND index_type=0")
	}
	b.writeMetricFilter(b.metricID(), b.filterIn.Metrics, b.filterNotIn.Metrics, lod.Version)
	b.writeTagCond(&lod, true)
	b.writeTagCond(&lod, false)
	q.WriteString(" GROUP BY ")
	q.writeTags(&lod, writeTagsOptions{time: true})
	limit := maxSeriesRows
	if b.sort != sortNone {
		limit = maxTableRows
		q.WriteString(" ORDER BY ")
		q.writeTags(&lod, writeTagsOptions{time: true})
		if b.sort == sortDescending {
			q.WriteString(" DESC")
		}
	}
	q.WriteString(fmt.Sprintf(" LIMIT %v SETTINGS optimize_aggregation_in_order=1", limit))
	q.body = q.String()
	q.Reset()
	return q
}

func (q *pointsSelectCols) writeTags(lod *data_model.LOD, opt writeTagsOptions) {
	switch lod.Version {
	case Version3:
		q.writeTagsV3(lod, opt)
	default:
		q.writeTagsV2(lod, opt)
	}
}

func (b *queryBuilder) isStringTop() bool {
	v := b.singleMetric()
	return v != nil && v.StringTopDescription != ""
}

func (b *queryBuilder) metricID() int32 {
	if v := b.singleMetric(); v != nil {
		return v.MetricID
	}
	return 0
}

func (b *queryBuilder) preKeyTagID() string {
	if v := b.singleMetric(); v != nil {
		return v.PreKeyTagID
	}
	return ""
}

func (b *queryBuilder) preKeyTagX() int {
	if v := b.singleMetric(); v != nil {
		return format.TagIndex(v.PreKeyTagID)
	}
	return -1
}

func (b *queryBuilder) singleMetric() *format.MetricMetaValue {
	if b.metric != nil {
		return b.metric
	}
	if len(b.filterIn.Metrics) == 1 {
		return b.filterIn.Metrics[0]
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
	return "argMinMergeState(min_host)"
}

func sqlMaxHost(version string) string {
	if version == Version1 {
		return "0"
	}
	return "argMaxMergeState(max_host)"
}

func (b *queryBuilder) writeMetricFilter(metricID int32, filterIn, filterNotIn []*format.MetricMetaValue, version string) {
	emptyFilter := len(filterIn) == 0 && len(filterNotIn) == 0
	if metricID != 0 || emptyFilter {
		b.WriteString(" AND ")
		b.WriteString(metricColumn(version))
		b.WriteString("=")
		b.WriteString(fmt.Sprint(metricID))
		return
	}
	if len(filterIn) != 0 {
		b.WriteString(" AND ")
		b.WriteString(metricColumn(version))
		b.WriteString(" IN (")
		b.WriteString(fmt.Sprint(filterIn[0].MetricID))
		for i := 1; i < len(filterIn); i++ {
			b.WriteByte(',')
			b.WriteString(fmt.Sprint(filterIn[i].MetricID))
		}
		b.WriteByte(')')
	}
	if len(filterNotIn) != 0 {
		b.WriteString(" AND ")
		b.WriteString(metricColumn(version))
		b.WriteString(" NOT IN (")
		b.WriteString(fmt.Sprint(filterNotIn[0].MetricID))
		for i := 1; i < len(filterNotIn); i++ {
			b.WriteByte(',')
			b.WriteString(fmt.Sprint(filterNotIn[i].MetricID))
		}
		b.WriteByte(')')
	}
}

func (b *queryBuilder) writeWhereTimeFilter(lod *data_model.LOD) {
	b.WriteString(" WHERE time>=")
	b.WriteString(fmt.Sprint(lod.FromSec))
	b.WriteString(" AND time<")
	b.WriteString(fmt.Sprint(lod.ToSec))
}

func (b *queryBuilder) writeV1DateFilter(lod *data_model.LOD) {
	if lod.Version != Version1 {
		return
	}
	b.WriteString(" AND date>=toDate(")
	b.WriteString(fmt.Sprint(lod.FromSec))
	b.WriteString(") AND date<=toDate(")
	b.WriteString(fmt.Sprint(lod.ToSec))
	b.WriteString(")")
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

func (b *queryBuilder) preKeyTableName(lod *data_model.LOD) string {
	var usePreKey bool
	if lod.HasPreKey {
		preKeyTagX := b.preKeyTagX()
		usePreKey = lod.PreKeyOnly ||
			b.filterIn.Contains(preKeyTagX) ||
			b.filterNotIn.Contains(preKeyTagX)
		if !usePreKey {
			for _, v := range b.by {
				if format.TagID(v) == b.preKeyTagID() {
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

func (b *queryBuilder) mappedColumnName(tagID string, lod *data_model.LOD) string {
	switch lod.Version {
	case Version3:
		return b.mappedColumnNameV3(tagID, lod)
	default:
		return b.mappedColumnNameV2(tagID, lod)
	}
}

func (b *queryBuilder) unmappedColumnName(tagID, version string) string {
	switch version {
	case Version3:
		return b.unmappedColumnNameV3(tagID)
	default:
		return "skey"
	}
}
