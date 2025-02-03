package api

import (
	"fmt"

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
	q.writeSelect(&lod, mode)
	q.writeFrom(&lod)
	q.writeWhere(&lod, mode)
	q.writeGroupBy(&lod, mode)
	q.WriteString(" HAVING _count>0")
	q.writeOrderBy(&lod, mode)
	q.WriteString(" SETTINGS optimize_aggregation_in_order=1")
	q.body = q.String()
	q.Reset()
	return &q
}

func (q *tagValuesQuery) writeSelect(lod *data_model.LOD, mode queryBuilderMode) {
	q.WriteString("SELECT ")
	comma := q.newListComma()
	if q.hasInt(lod) {
		comma.maybeWrite()
		q.writeSelectInt(lod)
	}
	if q.hasStr(lod, mode) {
		comma.maybeWrite()
		q.writeSelectStr(lod)
	}
	q.WriteString(",toFloat64(sum(count)) AS _count")
	q.res = append(q.res, proto.ResultColumn{Name: "_count", Data: &q.cnt})
}

func (q *tagValuesQuery) writeSelectInt(lod *data_model.LOD) {
	expr := q.selectIntExpr(int(q.tag.Index), lod)
	q.WriteString("toInt64(")
	q.WriteString(expr)
	q.WriteString(") AS ")
	alias := q.selAlias(int(q.tag.Index), lod)
	q.WriteString(alias)
	q.res = append(q.res, proto.ResultColumn{Name: alias, Data: &q.valID})
}

func (q *tagValuesQuery) writeSelectStr(lod *data_model.LOD) {
	colStr := q.colStr(int(q.tag.Index), lod)
	q.WriteString(colStr)
	q.res = append(q.res, proto.ResultColumn{Name: colStr, Data: &q.val})
}

func (q *tagValuesQuery) writeFrom(lod *data_model.LOD) {
	q.WriteString(" FROM ")
	q.WriteString(q.preKeyTableName(lod))
}

func (q *tagValuesQuery) writeGroupBy(lod *data_model.LOD, mode queryBuilderMode) {
	q.WriteString(" GROUP BY ")
	q.writeByTags(lod, mode)
}

func (q *tagValuesQuery) writeOrderBy(lod *data_model.LOD, mode queryBuilderMode) {
	q.WriteString(" ORDER BY _count DESC,")
	q.writeByTags(lod, mode)
	q.WriteString(" LIMIT ")
	q.WriteString(fmt.Sprint(q.numResults + 1))
}

func (q *tagValuesQuery) writeByTags(lod *data_model.LOD, mode queryBuilderMode) {
	comma := q.newListComma()
	if q.hasInt(lod) {
		comma.maybeWrite()
		q.WriteString(q.selAlias(int(q.tag.Index), lod))
	}
	if q.hasStr(lod, mode) {
		comma.maybeWrite()
		q.WriteString(q.colStr(int(q.tag.Index), lod))
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
		return !q.tag.Raw && !q.tag.Raw64 && mode != buildTagValueIDsQuery
	default:
		return q.tag.Index == format.StringTopTagIndexV3
	}
}
