package api

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/format"
)

const operatorIn = " IN "
const operatorNotIn = " NOT IN "

type filterOperator [2]string // operator at [0], separator at [1]
var filterOperatorIn = filterOperator{operatorIn, " OR "}
var filterOperatorNotIn = filterOperator{operatorNotIn, " AND "}
var escapeReplacer = strings.NewReplacer(`'`, `\'`, `\`, `\\`)

var timeCoarseTrimSeconds = map[string]int64{
	"statshouse_v4_1s":      60,
	"statshouse_v4_1m":      60 * 60,
	"statshouse_v4_1h":      60 * 60 * 24,
	"statshouse_v4_1s_dist": 60,
	"statshouse_v4_1m_dist": 60 * 60,
	"statshouse_v4_1h_dist": 60 * 60 * 24,
}

func (b *queryBuilder) buildSeriesQuery(lod data_model.LOD, settings string) (*seriesQuery, error) {
	q := &seriesQuery{
		queryBuilder: b,
		version:      lod.Version,
	}
	var sb strings.Builder
	if err := q.writeSelect(&sb, &lod); err != nil {
		return nil, err
	}
	q.writeFrom(&sb, &lod)
	b.writeWhere(&sb, &lod, buildSeriesQuery)
	q.writeGroupBy(&sb, &lod)
	limit := maxSeriesRows
	if b.sort != sortNone {
		limit = maxTableRows
		q.writeOrderBy(&sb, &lod)
	}
	sb.WriteString(fmt.Sprintf(" LIMIT %v", limit))
	sb.WriteString(settings)
	q.body = sb.String()
	return q, nil
}

func (q *seriesQuery) writeSelect(sb *strings.Builder, lod *data_model.LOD) error {
	sb.WriteString("SELECT ")
	comma := q.newListComma()
	q.writeSelectTime(sb, lod, &comma)
	if err := q.writeSelectValues(sb, lod, &comma); err != nil {
		return err
	}
	return q.writeSelectTags(sb, lod, &comma)
}

func (q *seriesQuery) writeSelectTime(sb *strings.Builder, lod *data_model.LOD, comma *listItemSeparator) {
	comma.maybeWrite(sb)
	if lod.StepSec == _1M {
		sb.WriteString(fmt.Sprintf("toInt64(toDateTime(toStartOfInterval(time,INTERVAL 1 MONTH,'%s'),'%s'))", lod.Location.String(), lod.Location.String()))
	} else {
		sb.WriteString(fmt.Sprintf("toInt64(toStartOfInterval(time+%d,INTERVAL %d second))-%d", q.utcOffset, lod.StepSec, q.utcOffset))
	}
	sb.WriteString(" AS _time")
	q.res = append(q.res, proto.ResultColumn{Name: "_time", Data: &q.time})
}

func (q *seriesQuery) writeSelectValues(sb *strings.Builder, lod *data_model.LOD, comma *listItemSeparator) error {
	var has [data_model.DigestLast]bool
	var hasSumSquare bool
	for i, j := 0, 0; q.what.specifiedAt(i); i++ {
		if has[q.what[i].What] {
			continue
		}
		switch q.what[i].What {
		case data_model.DigestAvg:
			if !has[data_model.DigestSum] {
				q.writeSelectSum(sb, j, lod, comma)
				has[data_model.DigestSum] = true
				j++
			}
			if !has[data_model.DigestCount] {
				q.writeSelectCount(sb, j, lod, comma)
				has[data_model.DigestCount] = true
				j++
			}
		case data_model.DigestCount:
			q.writeSelectCount(sb, j, lod, comma)
			j++
		case data_model.DigestMax:
			colName := fmt.Sprintf("_val%d", j)
			q.res = append(q.res, proto.ResultColumn{Name: colName, Data: &q.max})
			comma.maybeWrite(sb)
			sb.WriteString(fmt.Sprintf("toFloat64(%s(max))", sqlAggFn("max", lod)))
			sb.WriteString(" AS ")
			sb.WriteString(colName)
			j++
		case data_model.DigestMin:
			colName := fmt.Sprintf("_val%d", j)
			q.res = append(q.res, proto.ResultColumn{Name: colName, Data: &q.min})
			comma.maybeWrite(sb)
			sb.WriteString(fmt.Sprintf("toFloat64(%s(min))", sqlAggFn("min", lod)))
			sb.WriteString(" AS ")
			sb.WriteString(colName)
			j++
		case data_model.DigestSum:
			q.writeSelectSum(sb, j, lod, comma)
			j++
		case data_model.DigestStdDev:
			if !has[data_model.DigestSum] {
				q.writeSelectSum(sb, j, lod, comma)
				has[data_model.DigestSum] = true
				j++
			}
			if !has[data_model.DigestCount] {
				q.writeSelectCount(sb, j, lod, comma)
				has[data_model.DigestCount] = true
				j++
			}
			if !hasSumSquare {
				colName := fmt.Sprintf("_val%d", j)
				q.res = append(q.res, proto.ResultColumn{Name: colName, Data: &q.sumsquare})
				comma.maybeWrite(sb)
				sb.WriteString(fmt.Sprintf("toFloat64(%s(sumsquare))", sqlAggFn("sum", lod)))
				sb.WriteString(" AS ")
				sb.WriteString(colName)
				hasSumSquare = true
				j++
			}
		case data_model.DigestPercentile:
			columnName := fmt.Sprintf("_val%d", j)
			q.res = append(q.res, proto.ResultColumn{Name: columnName, Data: &q.percentile})
			comma.maybeWrite(sb)
			sb.WriteString("quantilesTDigestMergeState(0.5)(percentiles)")
			sb.WriteString(" AS ")
			sb.WriteString(columnName)
			j++
		case data_model.DigestCardinality:
			colName := fmt.Sprintf("_val%d", j)
			q.res = append(q.res, proto.ResultColumn{Name: colName, Data: &q.cardinality})
			comma.maybeWrite(sb)
			sb.WriteString("toFloat64(sum(1))")
			sb.WriteString(" AS ")
			sb.WriteString(colName)
			j++
		case data_model.DigestUnique:
			columnName := fmt.Sprintf("_val%d", j)
			q.res = append(q.res, proto.ResultColumn{Name: columnName, Data: &q.unique})
			comma.maybeWrite(sb)
			sb.WriteString("uniqMergeState(uniq_state)")
			sb.WriteString(" AS ")
			sb.WriteString(columnName)
			j++
		default:
			return fmt.Errorf("unsupported operation kind: %q", q.what[i].What)
		}
		has[q.what[i].What] = true
	}
	if q.minMaxHost[0] {
		comma.maybeWrite(sb)
		sb.WriteString(sqlMinHost(lod))
		sb.WriteString(" AS _minHost")
		switch q.version {
		case Version2:
			q.res = append(q.res, proto.ResultColumn{Name: "_minHost", Data: &q.minHostV2})
		case Version3:
			q.res = append(q.res, proto.ResultColumn{Name: "_minHost", Data: &q.minHostV3})
		}
	}
	if q.minMaxHost[1] {
		comma.maybeWrite(sb)
		sb.WriteString(sqlMaxHost(lod))
		sb.WriteString(" AS _maxHost")
		switch q.version {
		case Version2:
			q.res = append(q.res, proto.ResultColumn{Name: "_maxHost", Data: &q.maxHostV2})
		case Version3:
			q.res = append(q.res, proto.ResultColumn{Name: "_maxHost", Data: &q.maxHostV3})
		}
	}
	return nil
}

func (q *seriesQuery) writeSelectSum(sb *strings.Builder, i int, lod *data_model.LOD, comma *listItemSeparator) {
	colName := fmt.Sprintf("_val%d", i)
	q.res = append(q.res, proto.ResultColumn{Name: colName, Data: &q.sum})
	comma.maybeWrite(sb)
	sb.WriteString(fmt.Sprintf("toFloat64(%s(sum))", sqlAggFn("sum", lod)))
	sb.WriteString(" AS ")
	sb.WriteString(colName)

}

func (q *seriesQuery) writeSelectCount(sb *strings.Builder, i int, lod *data_model.LOD, comma *listItemSeparator) {
	colName := fmt.Sprintf("_val%d", i)
	q.res = append(q.res, proto.ResultColumn{Name: colName, Data: &q.count})
	comma.maybeWrite(sb)
	sb.WriteString(fmt.Sprintf("toFloat64(%s(count))", sqlAggFn("sum", lod)))
	sb.WriteString(" AS ")
	sb.WriteString(colName)
}

func sqlMinHost(lod *data_model.LOD) string {
	return "argMinMergeState(min_host)"
}

func sqlMaxHost(lod *data_model.LOD) string {
	return "argMaxMergeState(max_host)"
}

func (q *seriesQuery) writeSelectTags(sb *strings.Builder, lod *data_model.LOD, comma *listItemSeparator) error {
	switch lod.Version {
	case Version3:
		q.writeSelectTagsV3(sb, lod, comma)
	case Version2:
		q.writeSelectTagsV2(sb, lod, comma)
	default:
		return fmt.Errorf("bad schema version %s", lod.Version)
	}
	return nil
}

func (q *seriesQuery) writeSelectTagsV3(sb *strings.Builder, lod *data_model.LOD, comma *listItemSeparator) {
	for _, x := range q.by {
		comma.maybeWrite(sb)
		switch x {
		case format.ShardTagIndex:
			q.writeSelectShardNum(sb)
		default:
			q.writeSelectInt(sb, x, lod)
			comma.write(sb)
			q.writeSelectStr(sb, x, lod)
		}
	}
}

func (q *seriesQuery) writeSelectTagsV2(sb *strings.Builder, lod *data_model.LOD, comma *listItemSeparator) {
	for _, x := range q.by {
		switch x {
		case format.ShardTagIndex:
			comma.maybeWrite(sb)
			q.writeSelectShardNum(sb)
		case format.StringTopTagIndex, format.StringTopTagIndexV3:
			comma.maybeWrite(sb)
			q.writeSelectStr(sb, x, lod)
		default:
			if x < format.MaxTagsV2 {
				comma.maybeWrite(sb)
				q.writeSelectInt(sb, x, lod)
			}
		}
	}
}

func (q *seriesQuery) writeFrom(sb *strings.Builder, lod *data_model.LOD) {
	sb.WriteString(" FROM ")
	sb.WriteString(q.preKeyTableName(lod))
}

func (b *queryBuilder) writeWhere(sb *strings.Builder, lod *data_model.LOD, mode queryBuilderMode) {
	sb.WriteString(" WHERE ")
	b.writeTimeClause(sb, lod)
	switch lod.Version {
	case Version3:
		if lod.UseV4Tables || lod.UseV5Tables || lod.UseV6Tables || lod.UsePKPrefixForV3 {
			b.ensurePrimaryKeyPrefix(sb)
		}
		if lod.UseV4Tables {
			sb.WriteString(" AND ")
			b.writeTimeCoarseClause(sb, lod)
		}
	}
	b.writeMetricFilter(sb, b.metricID(), b.filterIn.Metrics, b.filterNotIn.Metrics, lod)
	b.writeTagFilter(sb, lod, b.filterIn, filterOperatorIn, mode)
	b.writeTagFilter(sb, lod, b.filterNotIn, filterOperatorNotIn, mode)
}

func (b *queryBuilder) ensurePrimaryKeyPrefix(sb *strings.Builder) {
	// NOTE: clickhouse uses logarithmic search only for queries that select a range with a fixed primary key prefix.
	// After filtering by the provided PK prefix it falls back to a less optimal "generic exclusion search" algorithm.
	// So when we filter by a range or multiple values for a column like time/time_coarse range
	// explicitly setting values for preceding PK columns greatly increases performance

	// NOTE2: optimized for v3 and v4 and v5 tables whose PK prefix is (index_type, metric, pre_tag, pre_stag, time/time_coarse)
	// NOTE3: assumes that metric value and time/time_coarse range are set elsewhere
	// NOTE4: assumes that if rows with `index_type != 0 OR pre_tag != 0 OR !empty(pre_stag)` ever appear in CH, they won't be needed in api queries
	sb.WriteString(" AND index_type=0 AND pre_tag=0 AND pre_stag='' ")
}

func (b *queryBuilder) writeTimeClause(sb *strings.Builder, lod *data_model.LOD) {
	sb.WriteString("time>=")
	sb.WriteString(fmt.Sprint(lod.FromSec))
	sb.WriteString(" AND time<")
	sb.WriteString(fmt.Sprint(lod.ToSec))
}

func (b *queryBuilder) writeTimeCoarseClause(sb *strings.Builder, lod *data_model.LOD) {
	coarseSize, ok := timeCoarseTrimSeconds[lod.Table(true)] // sharding shouldn't affect coarseSize
	if ok {
		coarseFrom := lod.FromSec / coarseSize * coarseSize
		coarseTo := (lod.ToSec + coarseSize - 1) / coarseSize * coarseSize
		sb.WriteString("time_coarse>=")
		sb.WriteString(fmt.Sprint(coarseFrom))
		sb.WriteString(" AND time_coarse<")
		sb.WriteString(fmt.Sprint(coarseTo))
	} else {
		// shouldn't happen
		sb.WriteString("1=1")
	}
}

func (b *queryBuilder) writeMetricFilter(sb *strings.Builder, metricID int32, filterIn, filterNotIn []*format.MetricMetaValue, lod *data_model.LOD) {
	emptyFilter := len(filterIn) == 0 && len(filterNotIn) == 0
	if metricID != 0 || emptyFilter {
		sb.WriteString(" AND ")
		sb.WriteString(metricColumn(lod))
		sb.WriteString("=")
		sb.WriteString(fmt.Sprint(metricID))
		return
	}
	if len(filterIn) != 0 {
		sb.WriteString(" AND ")
		sb.WriteString(metricColumn(lod))
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
		sb.WriteString(metricColumn(lod))
		sb.WriteString(" NOT IN (")
		sb.WriteString(fmt.Sprint(filterNotIn[0].MetricID))
		for i := 1; i < len(filterNotIn); i++ {
			sb.WriteByte(',')
			sb.WriteString(fmt.Sprint(filterNotIn[i].MetricID))
		}
		sb.WriteByte(')')
	}
}

func (b *queryBuilder) writeTagFilter(sb *strings.Builder, lod *data_model.LOD, f data_model.TagFilters, op filterOperator, mod queryBuilderMode) error {
	predicate, sep := op[0], op[1]
	in := predicate == operatorIn
	version3StrcmpOn := b.version3StrcmpOn(lod)
	for tagX, filter := range f.Tags {
		if filter.Empty() {
			continue
		}
		sb.WriteString(" AND (")
		// mapped
		legacyStringTOP := tagX == format.StringTopTagIndexV3 && lod.Version != "3"
		var hasMapped bool
		var hasValue bool
		var hasEmpty bool
		var started bool
		var raw bool
		if b.metric != nil && tagX < len(b.metric.Tags) {
			raw = b.metric.Tags[tagX].Raw()
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
						sb.WriteString(sep)
					} else {
						started = true
					}
					if err := b.writeWhereIntExpr(sb, tagX, lod, mod); err != nil {
						return err
					}
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
		} else if !legacyStringTOP {
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
		if !raw && (version3StrcmpOn || legacyStringTOP) {
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
				sb.WriteString(b.colStr(tagX, lod))
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
							sb.WriteString(b.colStr(tagX, lod))
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
		if hasEmpty {
			if started {
				sb.WriteString(sep)
			}
			if !in {
				sb.WriteString("NOT ")
			}
			sb.WriteString("(")
			if lod.Version == "3" {
				if err := b.writeWhereIntExpr(sb, tagX, lod, mod); err != nil {
					return err
				}
				sb.WriteString("=0")
				if !raw {
					sb.WriteString(" AND ")
					sb.WriteString(b.colStr(tagX, lod))
					sb.WriteString("=''")
				}
			} else if tagX == format.StringTopTagIndexV3 {
				sb.WriteString(b.colStr(tagX, lod))
				sb.WriteString("=''")
			} else {
				if err := b.writeWhereIntExpr(sb, tagX, lod, mod); err != nil {
					return err
				}
				sb.WriteString("=0")
			}
			sb.WriteString(")")
		}
		sb.WriteString(")")
	}
	return nil
}

func (q *queryBuilder) writeGroupBy(sb *strings.Builder, lod *data_model.LOD) {
	sb.WriteString(" GROUP BY _time")
	q.writeByTags(sb, lod)
}

func (q *seriesQuery) writeOrderBy(sb *strings.Builder, lod *data_model.LOD) {
	sb.WriteString(" ORDER BY _time")
	q.writeByTags(sb, lod)
	if q.sort == sortDescending {
		sb.WriteString(" DESC")
	}
}

func (q *seriesQuery) writeSelectShardNum(sb *strings.Builder) {
	sb.WriteString("_shard_num")
	q.res = append(q.res, proto.ResultColumn{Name: "_shard_num", Data: &q.shardNum})
}

func (q *seriesQuery) writeSelectStr(sb *strings.Builder, tagX int, lod *data_model.LOD) {
	colName := q.colStr(tagX, lod)
	sb.WriteString(colName)
	col := &stagCol{tagX: int(tagX)}
	q.stag = append(q.stag, col)
	q.res = append(q.res, proto.ResultColumn{Name: colName, Data: &col.data})
}

func (q *seriesQuery) writeSelectInt(sb *strings.Builder, tagX int, lod *data_model.LOD) {
	expr, colName := q.selectIntExpr(tagX, lod)
	sb.WriteString(expr)
	col := &tagCol{tagX: int(tagX)}
	q.tag = append(q.tag, col)
	if colName {
		q.res = append(q.res, proto.ResultColumn{Name: expr, Data: &col.dataInt32})
	} else {
		sb.WriteString(" AS ")
		alias := q.selAlias(tagX, lod)
		sb.WriteString(alias)
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

func (b *queryBuilder) writeWhereIntExpr(sb *strings.Builder, tagX int, lod *data_model.LOD, mod queryBuilderMode) error {
	v, err := b.whereIntExpr(tagX, lod, mod)
	if err != nil {
		return err
	}
	sb.WriteString(v)
	return nil
}

// as appears in WHERE clause
// either alias, expression or column name
func (b *queryBuilder) whereIntExpr(tagX int, lod *data_model.LOD, mod queryBuilderMode) (string, error) {
	switch mod {
	case buildSeriesQuery:
		if lod.HasPreKey && tagX == b.preKeyTagX() {
			return "_prekey", nil
		}
	case buildTagValuesQuery, buildTagValueIDsQuery:
		// pass
	default:
		return "", fmt.Errorf("bad query kind")
	}
	if b.raw64(tagX) {
		if b.groupedBy(tagX) {
			return "_tag" + strconv.Itoa(int(tagX)), nil
		}
		return b.raw64Expr(tagX, lod), nil
	}
	return b.colInt(tagX, lod), nil
}

func (q *queryBuilder) writeByTags(sb *strings.Builder, lod *data_model.LOD) {
	switch lod.Version {
	case Version3:
		q.writeByTagsV3(sb, lod)
	default:
		q.writeByTagsV2(sb, lod)
	}
}

func (q *queryBuilder) writeByTagsV3(sb *strings.Builder, lod *data_model.LOD) {
	for _, x := range q.by {
		sb.WriteString(",")
		switch x {
		case format.ShardTagIndex:
			sb.WriteString("_shard_num")
		default:
			sb.WriteString(q.selAlias(x, lod))
			sb.WriteString(",")
			sb.WriteString(q.colStr(x, lod))
		}
	}
}

func (q *queryBuilder) writeByTagsV2(sb *strings.Builder, lod *data_model.LOD) {
	for _, x := range q.by {
		switch x {
		case format.ShardTagIndex:
			sb.WriteString(",")
			sb.WriteString("_shard_num")
		case format.StringTopTagIndex, format.StringTopTagIndexV3:
			sb.WriteString(",")
			sb.WriteString("skey")
		default:
			if x < format.MaxTagsV2 {
				sb.WriteString(",")
				sb.WriteString(q.colIntV2(x, lod))
			}
		}
	}
}

func (b *queryBuilder) raw64(tagX int) bool {
	return b.metric != nil &&
		tagX < len(b.metric.Tags) &&
		b.metric.Tags[tagX].Raw64()
}

func metricColumn(lod *data_model.LOD) string {
	return "metric"
}

func (b *queryBuilder) version3StrcmpOn(lod *data_model.LOD) bool {
	return lod.Version == Version3 && !b.strcmpOff
}
