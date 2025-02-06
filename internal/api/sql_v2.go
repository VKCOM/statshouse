package api

import (
	"fmt"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/format"
)

func (q *pointsSelectCols) writeTagsV2(lod *data_model.LOD, opt writeTagsOptions) {
	if !q.point && opt.time {
		q.writeMaybeCommaString("_time", opt.comma)
		opt.comma = true
	}
	if opt.cols {
		q.tag = make([]tagCol, 0, len(q.by))
	}
	for _, x := range q.by {
		switch x {
		case format.ShardTagIndex:
			if opt.cols {
				q.res = append(q.res, proto.ResultColumn{Name: "key_shard_num", Data: &q.shardNum})
				q.writeMaybeCommaString("_shard_num AS key_shard_num", opt.comma)
			} else {
				q.writeMaybeCommaString("key_shard_num", opt.comma)
			}
		case 0, format.StringTopTagIndex, format.StringTopTagIndexV3:
			if lod.Version == Version1 {
				continue // "env" and "skey" tags do not exist in V1 schema
			}
			fallthrough
		default:
			id := format.TagID(x)
			if opt.cols {
				var data proto.ColResult
				if x == format.StringTopTagIndexV3 {
					q.stag = append(q.stag, stagCol{tagX: x})
					data = &q.stag[len(q.stag)-1].data
				} else {
					q.tag = append(q.tag, tagCol{tagX: x})
					data = &q.tag[len(q.tag)-1].data
				}
				q.res = append(q.res, proto.ResultColumn{Name: "key" + id, Data: data})
				q.writeMaybeCommaString(q.mappedColumnNameV2(id, lod), opt.comma)
				q.WriteString(" AS key")
				q.WriteString(id)
			} else {
				q.writeMaybeCommaString("key", opt.comma)
				q.WriteString(id)
			}
		}
		opt.comma = true
	}
}

func (b *queryBuilder) tagValuesQueryV2(lod *data_model.LOD) *tagValuesSelectCols {
	// no need to escape anything as long as table and tag names are fixed
	stag := b.stringTag()
	var valueName string
	if stag {
		valueName = "_string_value"
	} else {
		valueName = "_value"
	}
	b.WriteString("SELECT ")
	b.WriteString(b.mappedColumnNameV2(b.tagID, lod))
	b.WriteString(" AS ")
	b.WriteString(valueName)
	b.WriteString(",toFloat64(")
	b.WriteString(sqlAggFn(lod.Version, "sum"))
	b.WriteString("(count)) AS _count FROM ")
	b.WriteString(b.preKeyTableName(lod))
	b.writeWhereTimeFilter(lod)
	b.writeMetricFilter(b.metricID(), b.filterIn.Metrics, b.filterNotIn.Metrics, lod.Version)
	b.writeV1DateFilter(lod)
	b.writeTagCond(lod, true)
	b.writeTagCond(lod, false)
	b.WriteString(" GROUP BY ")
	b.WriteString(b.mappedColumnNameV2(b.tagID, lod))
	b.WriteString(" HAVING _count>0 ORDER BY _count DESC,")
	b.WriteString(valueName)
	b.WriteString(" LIMIT ")
	b.WriteString(fmt.Sprint(b.numResults + 1))
	b.WriteString(" SETTINGS optimize_aggregation_in_order=1")
	q := tagValuesSelectCols{
		queryBuilder: b,
		stag:         stag,
		body:         b.String(),
	}
	if stag {
		q.res = append(q.res, proto.ResultColumn{Name: "_string_value", Data: &q.val})
	} else {
		q.res = append(q.res, proto.ResultColumn{Name: "_value", Data: &q.valID})
	}
	q.res = append(q.res, proto.ResultColumn{Name: "_count", Data: &q.cnt})
	b.Reset()
	return &q
}

func (b *queryBuilder) mappedColumnNameV2(tagID string, lod *data_model.LOD) string {
	// intentionally not using constants from 'format' package,
	// because it is a table column name, not an external contract
	switch tagID {
	case format.StringTopTagID, format.StringTopTagIDV3:
		return "skey"
	case format.ShardTagID:
		return "_shard_num"
	default:
		if lod.HasPreKey && tagID == b.preKeyTagID() {
			return "prekey"
		}
		// 'tagID' assumed to be a number from 0 to 15,
		// dont't verify (ClickHouse just won't find a column)
		return "key" + tagID
	}
}
