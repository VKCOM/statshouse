package api

import (
	"fmt"
	"strings"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/format"
)

func (b *queryBuilder) buildTagValuesQuery(lod data_model.LOD) *tagValuesQuery {
	return b.buildTagValuesQueryEx(lod, buildTagValuesQuery)
}

func (b *queryBuilder) buildTagValueIDsQuery(lod data_model.LOD) *tagValuesQuery {
	return b.buildTagValuesQueryEx(lod, buildTagValueIDsQuery)
}

func (b *queryBuilder) buildTagValuesQueryEx(lod data_model.LOD, mode queryBuilderMode) *tagValuesQuery {
	if b.tag.Index == format.StringTopTagIndex {
		b.tag.Index = format.StringTopTagIndexV3
	}
	q := tagValuesQuery{
		queryBuilder: b,
	}
	var sb strings.Builder
	q.writeSelect(&sb, &lod, mode)
	q.writeFrom(&sb, &lod)
	q.writeWhere(&sb, &lod, mode)
	q.writeGroupBy(&sb, &lod, mode)
	sb.WriteString(" HAVING _count>0")
	q.writeOrderBy(&sb, &lod, mode)
	sb.WriteString(" SETTINGS optimize_aggregation_in_order=1")
	q.body = sb.String()
	return &q
}

func (q *tagValuesQuery) writeSelect(sb *strings.Builder, lod *data_model.LOD, mode queryBuilderMode) {
	sb.WriteString("SELECT ")
	comma := q.newListComma()
	if q.hasInt(lod) {
		comma.maybeWrite(sb)
		q.writeSelectInt(sb, lod)
	}
	if q.hasStr(lod, mode) {
		comma.maybeWrite(sb)
		q.writeSelectStr(sb, lod)
	}
	sb.WriteString(",toFloat64(")
	sb.WriteString(sqlAggFn("sum", lod))
	sb.WriteString("(count)) AS _count")
	q.res = append(q.res, proto.ResultColumn{Name: "_count", Data: &q.cnt})
}

func (q *tagValuesQuery) writeSelectInt(sb *strings.Builder, lod *data_model.LOD) {
	expr, colName := q.selectIntExpr(int(q.tag.Index), lod)
	sb.WriteString(expr)
	if colName {
		q.res = append(q.res, proto.ResultColumn{Name: expr, Data: &q.dataInt32})
	} else {
		sb.WriteString(" AS ")
		alias := q.selAlias(int(q.tag.Index), lod)
		sb.WriteString(alias)
		q.res = append(q.res, proto.ResultColumn{Name: alias, Data: &q.dataInt64})
	}
}

func (q *tagValuesQuery) writeSelectStr(sb *strings.Builder, lod *data_model.LOD) {
	colStr := q.colStr(int(q.tag.Index), lod)
	sb.WriteString(colStr)
	q.res = append(q.res, proto.ResultColumn{Name: colStr, Data: &q.dataStr})
}

func (q *tagValuesQuery) writeFrom(sb *strings.Builder, lod *data_model.LOD) {
	sb.WriteString(" FROM ")
	sb.WriteString(q.preKeyTableName(lod))
}

func (q *tagValuesQuery) writeGroupBy(sb *strings.Builder, lod *data_model.LOD, mode queryBuilderMode) {
	sb.WriteString(" GROUP BY ")
	q.writeByTags(sb, lod, mode)
}

func (q *tagValuesQuery) writeOrderBy(sb *strings.Builder, lod *data_model.LOD, mode queryBuilderMode) {
	sb.WriteString(" ORDER BY _count DESC,")
	q.writeByTags(sb, lod, mode)
	sb.WriteString(" LIMIT ")
	sb.WriteString(fmt.Sprint(q.numResults + 1))
}

func (q *tagValuesQuery) writeByTags(sb *strings.Builder, lod *data_model.LOD, mode queryBuilderMode) {
	comma := q.newListComma()
	if q.hasInt(lod) {
		comma.maybeWrite(sb)
		sb.WriteString(q.selAlias(int(q.tag.Index), lod))
	}
	if q.hasStr(lod, mode) {
		comma.maybeWrite(sb)
		sb.WriteString(q.colStr(int(q.tag.Index), lod))
	}
}

func (q *tagValuesQuery) hasInt(lod *data_model.LOD) bool {
	switch lod.Version {
	case Version3:
		return true
	default:
		return q.tag.Index != format.StringTopTagIndexV3
	}
}

func (q *tagValuesQuery) hasStr(lod *data_model.LOD, mode queryBuilderMode) bool {
	switch lod.Version {
	case Version3:
		return !q.tag.Raw && !q.tag.Raw64() && mode != buildTagValueIDsQuery
	default:
		return q.tag.Index == format.StringTopTagIndexV3
	}
}
