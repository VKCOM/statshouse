package api

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/format"
)

const operatorIn = " IN "
const operatorNotIn = " NOT IN "

type filterOperator [2]string // operator at [0], separator at [1]
var filterOperatorIn = filterOperator{operatorIn, " OR "}
var filterOperatorNotIn = filterOperator{operatorNotIn, " AND "}
var escapeReplacer = strings.NewReplacer(`'`, `\'`)

func (b *queryBuilder) buildSeriesQuery(lod data_model.LOD) *seriesQuery {
	q := &seriesQuery{
		queryBuilder: b,
		version:      lod.Version,
	}
	q.writeSelect(&lod)
	q.writeFrom(&lod)
	b.writeWhere(&lod, buildSeriesQuery)
	q.writeGroupBy(&lod)
	limit := maxSeriesRows
	if b.sort != sortNone {
		limit = maxTableRows
		q.writeOrderBy(&lod)
	}
	q.WriteString(fmt.Sprintf(" LIMIT %v SETTINGS optimize_aggregation_in_order=1", limit))
	q.body = q.String()
	q.Reset()
	return q
}

func (q *seriesQuery) writeSelect(lod *data_model.LOD) {
	q.WriteString("SELECT ")
	comma := q.newListComma()
	q.writeSelectTime(lod, &comma)
	q.writeSelectValues(lod, &comma)
	q.writeSelectTags(lod, &comma)
}

func (q *seriesQuery) writeSelectTime(lod *data_model.LOD, comma *listItemSeparator) {
	comma.maybeWrite()
	if lod.StepSec == _1M {
		q.WriteString(fmt.Sprintf("toInt64(toDateTime(toStartOfInterval(time,INTERVAL 1 MONTH,'%s'),'%s'))", lod.Location.String(), lod.Location.String()))
	} else {
		q.WriteString(fmt.Sprintf("toInt64(toStartOfInterval(time+%d,INTERVAL %d second))-%d", q.utcOffset, lod.StepSec, q.utcOffset))
	}
	q.WriteString(" AS _time")
	q.res = append(q.res, proto.ResultColumn{Name: "_time", Data: &q.time})
}

func (q *seriesQuery) writeSelectValues(lod *data_model.LOD, comma *listItemSeparator) {
	if q.version == Version1 && q.isStringTop() {
		q.WriteString("toFloat64(sumMerge(count)) AS _val0")
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
				q.writeSelectSum(j, lod, comma)
				has[data_model.DigestSum] = true
				j++
			}
			if !has[data_model.DigestCount] {
				q.writeSelectCount(j, lod, comma)
				has[data_model.DigestCount] = true
				j++
			}
		case data_model.DigestCount:
			q.writeSelectCount(j, lod, comma)
			j++
		case data_model.DigestMax:
			colName := fmt.Sprintf("_val%d", j)
			q.res = append(q.res, proto.ResultColumn{Name: colName, Data: &q.max})
			comma.maybeWrite()
			q.WriteString(fmt.Sprintf("toFloat64(%s(max))", sqlAggFn("max", lod)))
			q.WriteString(" AS ")
			q.WriteString(colName)
			j++
		case data_model.DigestMin:
			colName := fmt.Sprintf("_val%d", j)
			q.res = append(q.res, proto.ResultColumn{Name: colName, Data: &q.min})
			comma.maybeWrite()
			q.WriteString(fmt.Sprintf("toFloat64(%s(min))", sqlAggFn("min", lod)))
			q.WriteString(" AS ")
			q.WriteString(colName)
			j++
		case data_model.DigestSum:
			q.writeSelectSum(j, lod, comma)
			j++
		case data_model.DigestStdDev:
			if !has[data_model.DigestSum] {
				q.writeSelectSum(j, lod, comma)
				has[data_model.DigestSum] = true
				j++
			}
			if !has[data_model.DigestCount] {
				q.writeSelectCount(j, lod, comma)
				has[data_model.DigestCount] = true
				j++
			}
			if !hasSumSquare {
				colName := fmt.Sprintf("_val%d", j)
				q.res = append(q.res, proto.ResultColumn{Name: colName, Data: &q.sumsquare})
				comma.maybeWrite()
				q.WriteString(fmt.Sprintf("toFloat64(%s(sumsquare))", sqlAggFn("sum", lod)))
				q.WriteString(" AS ")
				q.WriteString(colName)
				hasSumSquare = true
				j++
			}
		case data_model.DigestPercentile:
			columnName := fmt.Sprintf("_val%d", j)
			q.res = append(q.res, proto.ResultColumn{Name: columnName, Data: &q.percentile})
			comma.maybeWrite()
			q.WriteString("quantilesTDigestMergeState(0.5)(percentiles)")
			q.WriteString(" AS ")
			q.WriteString(columnName)
			j++
		case data_model.DigestCardinality:
			colName := fmt.Sprintf("_val%d", j)
			q.res = append(q.res, proto.ResultColumn{Name: colName, Data: &q.cardinality})
			comma.maybeWrite()
			q.WriteString("toFloat64(sum(1))")
			q.WriteString(" AS ")
			q.WriteString(colName)
			j++
		case data_model.DigestUnique:
			columnName := fmt.Sprintf("_val%d", j)
			q.res = append(q.res, proto.ResultColumn{Name: columnName, Data: &q.unique})
			comma.maybeWrite()
			q.WriteString("uniqMergeState(uniq_state)")
			q.WriteString(" AS ")
			q.WriteString(columnName)
			j++
		default:
			panic(fmt.Errorf("unsupported operation kind: %q", q.what[i].What))
		}
		has[q.what[i].What] = true
	}
	if q.minMaxHost[0] {
		comma.maybeWrite()
		q.WriteString(sqlMinHost(lod))
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
		comma.maybeWrite()
		q.WriteString(sqlMaxHost(lod))
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

func (q *seriesQuery) writeSelectSum(i int, lod *data_model.LOD, comma *listItemSeparator) {
	colName := fmt.Sprintf("_val%d", i)
	q.res = append(q.res, proto.ResultColumn{Name: colName, Data: &q.sum})
	comma.maybeWrite()
	q.WriteString(fmt.Sprintf("toFloat64(%s(sum))", sqlAggFn("sum", lod)))
	q.WriteString(" AS ")
	q.WriteString(colName)

}

func (q *seriesQuery) writeSelectCount(i int, lod *data_model.LOD, comma *listItemSeparator) {
	colName := fmt.Sprintf("_val%d", i)
	q.res = append(q.res, proto.ResultColumn{Name: colName, Data: &q.count})
	comma.maybeWrite()
	q.WriteString(fmt.Sprintf("toFloat64(%s(count))", sqlAggFn("sum", lod)))
	q.WriteString(" AS ")
	q.WriteString(colName)
}

func sqlMinHost(lod *data_model.LOD) string {
	if lod.Version == Version1 {
		return "0"
	}
	return "argMinMergeState(min_host)"
}

func sqlMaxHost(lod *data_model.LOD) string {
	if lod.Version == Version1 {
		return "0"
	}
	return "argMaxMergeState(max_host)"
}

func (q *seriesQuery) writeSelectTags(lod *data_model.LOD, comma *listItemSeparator) {
	switch lod.Version {
	case Version3:
		q.writeSelectTagsV3(lod, comma)
	case Version2:
		q.writeSelectTagsV2(lod, comma)
	case Version1:
		q.writeSelectTagsV1(lod, comma)
	default:
		panic(fmt.Errorf("bad schema version %s", lod.Version))
	}
}

func (q *seriesQuery) writeSelectTagsV3(lod *data_model.LOD, comma *listItemSeparator) {
	for _, x := range q.by {
		comma.maybeWrite()
		switch x {
		case format.ShardTagIndex:
			q.writeSelectShardNum()
		default:
			q.writeSelectInt(x, lod)
			comma.write()
			q.writeSelectStr(x, lod)
		}
	}
}

func (q *seriesQuery) writeSelectTagsV2(lod *data_model.LOD, comma *listItemSeparator) {
	for _, x := range q.by {
		comma.maybeWrite()
		switch x {
		case format.ShardTagIndex:
			q.writeSelectShardNum()
		case format.StringTopTagIndexV3:
			q.writeSelectStr(x, lod)
		default:
			q.writeSelectInt(x, lod)
		}
	}
}

func (q *seriesQuery) writeSelectTagsV1(lod *data_model.LOD, comma *listItemSeparator) {
	for _, x := range q.by {
		switch x {
		case 0, format.StringTopTagIndexV3, format.ShardTagIndex:
			// pass
		default:
			comma.maybeWrite()
			q.writeSelectInt(x, lod)
		}
	}
}

func (q *queryBuilder) writeFrom(lod *data_model.LOD) {
	q.WriteString(" FROM ")
	q.WriteString(q.preKeyTableName(lod))
}

func (b *queryBuilder) writeWhere(lod *data_model.LOD, mode queryBuilderMode) {
	b.WriteString(" WHERE time>=")
	b.WriteString(fmt.Sprint(lod.FromSec))
	b.WriteString(" AND time<")
	b.WriteString(fmt.Sprint(lod.ToSec))
	switch lod.Version {
	case Version1:
		b.writeDateFilterV1(lod)
	case Version3:
		if mode == buildSeriesQuery {
			b.WriteString(" AND index_type=0")
		}
	}
	b.writeMetricFilter(b.metricID(), b.filterIn.Metrics, b.filterNotIn.Metrics, lod)
	b.writeTagFilter(lod, b.filterIn, filterOperatorIn, mode)
	b.writeTagFilter(lod, b.filterNotIn, filterOperatorNotIn, mode)
}

func (b *queryBuilder) writeDateFilterV1(lod *data_model.LOD) {
	b.WriteString(" AND date>=toDate(")
	b.WriteString(fmt.Sprint(lod.FromSec))
	b.WriteString(") AND date<=toDate(")
	b.WriteString(fmt.Sprint(lod.ToSec))
	b.WriteString(")")
}

func (b *queryBuilder) writeMetricFilter(metricID int32, filterIn, filterNotIn []*format.MetricMetaValue, lod *data_model.LOD) {
	emptyFilter := len(filterIn) == 0 && len(filterNotIn) == 0
	if metricID != 0 || emptyFilter {
		b.WriteString(" AND ")
		b.WriteString(metricColumn(lod))
		b.WriteString("=")
		b.WriteString(fmt.Sprint(metricID))
		return
	}
	if len(filterIn) != 0 {
		b.WriteString(" AND ")
		b.WriteString(metricColumn(lod))
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
		b.WriteString(metricColumn(lod))
		b.WriteString(" NOT IN (")
		b.WriteString(fmt.Sprint(filterNotIn[0].MetricID))
		for i := 1; i < len(filterNotIn); i++ {
			b.WriteByte(',')
			b.WriteString(fmt.Sprint(filterNotIn[i].MetricID))
		}
		b.WriteByte(')')
	}
}

func (b *queryBuilder) writeTagFilter(lod *data_model.LOD, f data_model.TagFilters, op filterOperator, mod queryBuilderMode) {
	predicate, sep := op[0], op[1]
	in := predicate == operatorIn
	version3StrcmpOn := b.version3StrcmpOn(lod)
	for tagX, filter := range f.Tags {
		if filter.Empty() {
			continue
		}
		b.WriteString(" AND (")
		// mapped
		legacyStringTOP := tagX == format.StringTopTagIndexV3 && lod.Version != "3"
		var hasMapped bool
		var hasValue bool
		var hasEmpty bool
		var started bool
		var raw bool
		if b.metric != nil && tagX < len(b.metric.Tags) {
			raw = b.metric.Tags[tagX].Raw
		}
		for _, v := range filter.Values {
			if v.Empty() {
				hasEmpty = true
				continue
			}
			if v.HasValue() {
				hasValue = true
			}
			if v.IsMapped() && !legacyStringTOP {
				if !hasMapped {
					if started {
						b.WriteString(sep)
					} else {
						started = true
					}
					b.WriteString(b.whereIntExpr(tagX, lod, mod))
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
		} else if !legacyStringTOP {
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
		if !raw && (version3StrcmpOn || legacyStringTOP) {
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
				b.WriteString(b.colStr(tagX, lod))
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
							b.WriteString(b.colStr(tagX, lod))
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
		if hasEmpty {
			if started {
				b.WriteString(sep)
			}
			if !in {
				b.WriteString("NOT ")
			}
			b.WriteString("(")
			and := b.newListItemSeparator(" AND ")
			if !raw {
				and.maybeWrite()
				b.WriteString(b.colStr(tagX, lod))
				b.WriteString("=''")
			}
			if lod.Version == "3" {
				and.maybeWrite()
				b.WriteString(b.whereIntExpr(tagX, lod, mod))
				b.WriteString("=0")
			}
			b.WriteString(")")
		}
		b.WriteString(")")
	}
}

func (q *queryBuilder) writeGroupBy(lod *data_model.LOD) {
	q.WriteString(" GROUP BY _time")
	q.writeByTags(lod)
}

func (q *seriesQuery) writeOrderBy(lod *data_model.LOD) {
	q.WriteString(" ORDER BY _time")
	q.writeByTags(lod)
	if q.sort == sortDescending {
		q.WriteString(" DESC")
	}
}

func (q *seriesQuery) writeSelectShardNum() {
	q.WriteString("_shard_num")
	q.res = append(q.res, proto.ResultColumn{Name: "_shard_num", Data: &q.shardNum})
}

func (q *seriesQuery) writeSelectStr(tagX int, lod *data_model.LOD) {
	colName := q.colStr(tagX, lod)
	q.WriteString(colName)
	col := &stagCol{tagX: int(tagX)}
	q.stag = append(q.stag, col)
	q.res = append(q.res, proto.ResultColumn{Name: colName, Data: &col.data})
}

func (q *seriesQuery) writeSelectInt(tagX int, lod *data_model.LOD) {
	expr, colName := q.selectIntExpr(tagX, lod)
	q.WriteString(expr)
	col := &tagCol{tagX: int(tagX)}
	q.tag = append(q.tag, col)
	if colName {
		q.res = append(q.res, proto.ResultColumn{Name: expr, Data: &col.dataInt32})
	} else {
		q.WriteString(" AS ")
		alias := q.selAlias(tagX, lod)
		q.WriteString(alias)
		q.res = append(q.res, proto.ResultColumn{Name: alias, Data: &col.dataInt64})
	}
}

// as appears in SELECT clause
// either column name or expression
func (b *queryBuilder) selectIntExpr(tagX int, lod *data_model.LOD) (string, bool) {
	if lod.HasPreKey && tagX == b.preKeyTagX() {
		return "_prekey", true
	}
	if b.raw64(tagX) {
		return b.raw64Expr(tagX, lod), false
	}
	return b.colInt(tagX, lod), true
}

// as appears in WHERE clause
// either alias, expression or column name
func (b *queryBuilder) whereIntExpr(tagX int, lod *data_model.LOD, mod queryBuilderMode) string {
	switch mod {
	case buildSeriesQuery:
		if lod.HasPreKey && tagX == b.preKeyTagX() {
			return "_prekey"
		}
	case buildTagValuesQuery, buildTagValueIDsQuery:
		// pass
	default:
		panic(fmt.Errorf("bad query kind"))
	}
	if b.raw64(tagX) {
		if b.groupedBy(tagX) {
			return "_tag" + strconv.Itoa(int(tagX))
		}
		return b.raw64Expr(tagX, lod)
	}
	return b.colInt(tagX, lod)
}

func (q *queryBuilder) writeByTags(lod *data_model.LOD) {
	switch lod.Version {
	case Version3:
		q.writeByTagsV3(lod)
	default:
		q.writeByTagsV2(lod)
	}
}

func (q *queryBuilder) writeByTagsV3(lod *data_model.LOD) {
	for _, x := range q.by {
		q.WriteString(",")
		switch x {
		case format.ShardTagIndex:
			q.WriteString("_shard_num")
		default:
			q.WriteString(q.selAlias(x, lod))
			q.WriteString(",")
			q.WriteString(q.colStr(x, lod))
		}
	}
}

func (q *queryBuilder) writeByTagsV2(lod *data_model.LOD) {
	for _, x := range q.by {
		q.WriteString(",")
		switch x {
		case format.ShardTagIndex:
			q.WriteString("_shard_num")
		case format.StringTopTagIndex, format.StringTopTagIndexV3:
			q.WriteString("skey")
		default:
			q.WriteString(q.colIntV2(x, lod))
		}
	}
}

func (b *queryBuilder) raw64(tagX int) bool {
	return b.metric != nil &&
		tagX < len(b.metric.Tags) &&
		b.metric.Tags[tagX].Raw64()
}

func metricColumn(lod *data_model.LOD) string {
	if lod.Version == Version1 {
		return "stats"
	}
	return "metric"
}

func (b *queryBuilder) version3StrcmpOn(lod *data_model.LOD) bool {
	return lod.Version == Version3 && !b.strcmpOff
}
