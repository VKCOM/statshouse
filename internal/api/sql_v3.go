package api

import (
	"fmt"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/format"
)

func (q *pointsSelectCols) writeTagsV3(lod *data_model.LOD, opt writeTagsOptions) {
	if !q.point && opt.time {
		q.writeMaybeCommaString("_time", opt.comma)
		opt.comma = true
	}
	if opt.cols {
		q.tag = make([]proto.ColInt32, 0, len(q.by))
		q.stag = make([]proto.ColStr, 0, len(q.by))
	}
	for _, id := range q.by {
		switch id {
		case format.StringTopTagID:
			if opt.cols {
				q.res = append(q.res, proto.ResultColumn{Name: "stag_s", Data: &q.tagStr})
			}
			q.writeMaybeCommaString("stag47 AS stag_s", opt.comma)
		case format.ShardTagID:
			if opt.cols {
				q.res = append(q.res, proto.ResultColumn{Name: "key_shard_num", Data: &q.shardNum})
			}
			q.writeMaybeCommaString("_shard_num AS key_shard_num", opt.comma)
		default:
			if opt.cols {
				q.tag = append(q.tag, proto.ColInt32{})
				q.res = append(q.res, proto.ResultColumn{Name: "tag" + id, Data: &q.tag[len(q.tag)-1]})
				q.stag = append(q.stag, proto.ColStr{})
				q.res = append(q.res, proto.ResultColumn{Name: "stag" + id, Data: &q.stag[len(q.tag)-1]})
				q.tagX = append(q.tagX, format.TagIndex(id))
			}
			q.writeMaybeCommaString(q.mappedColumnNameV3(id, lod), opt.comma)
			q.WriteString(" AS tag")
			q.WriteString(id)
			q.WriteString(",")
			q.WriteString(q.unmappedColumnNameV3(id))
			q.WriteString(" AS stag")
			q.WriteString(id)
		}
		opt.comma = true
	}
}

func (b *queryBuilder) tagValuesQueryV3(lod *data_model.LOD) *tagValuesSelectCols {
	b.WriteString("SELECT ")
	b.WriteString(b.mappedColumnNameV3(b.tagID, lod))
	b.WriteString(" AS _mapped,")
	b.WriteString(b.unmappedColumnNameV3(b.tagID))
	b.WriteString(" AS _unmapped,toFloat64(sum(count)) AS _count FROM ")
	b.WriteString(b.preKeyTableName(lod))
	b.writeWhereTimeFilter(lod)
	b.writeMetricFilter(b.metricID(), b.filterIn.Metrics, b.filterNotIn.Metrics, lod.Version)
	b.writeTagCond(lod, true)
	b.writeTagCond(lod, false)
	b.WriteString(" GROUP BY _mapped,_unmapped HAVING _count>0 ORDER BY _count,_mapped,_unmapped DESC LIMIT ")
	b.WriteString(fmt.Sprint(b.numResults + 1))
	b.WriteString(" SETTINGS optimize_aggregation_in_order=1")
	q := tagValuesSelectCols{
		queryBuilder: b,
		mixed:        true,
		body:         b.String(),
	}
	q.res = proto.Results{
		{Name: "_mapped", Data: &q.valID},
		{Name: "_unmapped", Data: &q.val},
		{Name: "_count", Data: &q.cnt},
	}
	b.Reset()
	return &q
}

func (b *queryBuilder) tagValueIDsQueryV3(lod *data_model.LOD) *tagValuesSelectCols {
	b.WriteString("SELECT ")
	b.WriteString(b.mappedColumnNameV3(b.tagID, lod))
	b.WriteString(" AS _mapped,toFloat64(sum(count)) AS _count FROM ")
	b.WriteString(b.preKeyTableName(lod))
	b.writeWhereTimeFilter(lod)
	b.writeMetricFilter(b.metricID(), b.filterIn.Metrics, b.filterNotIn.Metrics, lod.Version)
	b.writeTagCond(lod, true)
	b.writeTagCond(lod, false)
	b.WriteString(" GROUP BY _mapped HAVING _count>0 ORDER BY _count,_mapped DESC LIMIT ")
	b.WriteString(fmt.Sprint(b.numResults + 1))
	b.WriteString(" SETTINGS optimize_aggregation_in_order=1")
	q := tagValuesSelectCols{
		queryBuilder: b,
		body:         b.String(),
	}
	q.res = proto.Results{
		{Name: "_mapped", Data: &q.valID},
		{Name: "_count", Data: &q.cnt},
	}
	b.Reset()
	return &q
}

func (b *queryBuilder) version3StrcmpOn(metric *format.MetricMetaValue, tagX int, lod *data_model.LOD) bool {
	version3StrcmpOn := lod.Version == Version3 && !b.strcmpOff
	if !version3StrcmpOn || metric == nil || len(metric.Tags) <= tagX {
		return version3StrcmpOn
	}
	return metric.Tags[tagX].SkipMapping
}

func (b *queryBuilder) mappedColumnNameV3(tagID string, lod *data_model.LOD) string {
	switch tagID {
	case format.StringTopTagID:
		return "tag" + format.StringTopTagIDV3
	case format.ShardTagID:
		return "_shard_num"
	default:
		if lod.HasPreKey && tagID == b.preKeyTagID() {
			return "pre_tag"
		}
		return "tag" + tagID
	}
}

func (b *queryBuilder) unmappedColumnNameV3(tagID string) string {
	if tagID == format.StringTopTagID {
		return "stag" + format.StringTopTagIDV3
	}
	return "stag" + tagID
}
