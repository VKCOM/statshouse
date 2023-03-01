package promql

import (
	"context"
	"strconv"

	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/receiver/prometheus"
)

type tagMap struct {
	da        DataAccess
	valueToID map[string]int32
	idToValue map[int32]string
}

type tagValues struct {
	da       DataAccess
	from, to int64
	tagM     tagMap
	values   map[*format.MetricMetaValue][]map[int64]map[int32]string // metric -> tagIx -> offset -> id -> value
	strTop   map[*format.MetricMetaValue]map[int64][]string           // metric -> offset -> values
}

func newTagMap(da DataAccess) tagMap {
	return tagMap{da: da, valueToID: make(map[string]int32), idToValue: make(map[int32]string)}
}

func (tagM tagMap) getTagValue(id int32) string {
	v, ok := tagM.idToValue[id]
	if !ok {
		v = tagM.da.GetTagValue(id)
		tagM.idToValue[id] = v
		tagM.valueToID[v] = id
	}
	return v
}

func (tagM tagMap) getTagValueID(meta *format.MetricMetaValue, tagX int, val string) (id int32, err error) {
	if meta.Tags[tagX].Raw {
		return getRawTagValue(val)
	}
	var ok bool
	id, ok = tagM.valueToID[val]
	if !ok {
		id, err = tagM.da.GetTagValueID(val)
		if err != nil {
			return id, err
		}
		tagM.idToValue[id] = val
		tagM.valueToID[val] = id
	}
	return id, nil
}

func newTagValues(da DataAccess, from int64, to int64, tagM tagMap) tagValues {
	return tagValues{
		da:     da,
		from:   from,
		to:     to,
		tagM:   tagM,
		values: make(map[*format.MetricMetaValue][]map[int64]map[int32]string),
		strTop: make(map[*format.MetricMetaValue]map[int64][]string),
	}
}

func (tagV tagValues) getTagValues(ctx context.Context, meta *format.MetricMetaValue, tagX int, offset int64) (map[int32]string, error) {
	tov, ok := tagV.values[meta] // tag -> offset -> valueID -> value
	if !ok {
		tov = make([]map[int64]map[int32]string, format.MaxTags)
		tagV.values[meta] = tov
	}
	ov := tov[tagX] // offset -> valueID -> value
	if ov == nil {
		ov = make(map[int64]map[int32]string)
		tov[tagX] = ov
	}
	var res map[int32]string
	if res, ok = ov[offset]; ok {
		return res, nil
	}
	ids, err := tagV.da.QueryTagValues(ctx, meta, tagX, tagV.from, tagV.to)
	if err != nil {
		return nil, err
	}
	res = make(map[int32]string, len(ids))
	for _, id := range ids {
		res[id] = tagV.tagM.getTagValue(id)
	}
	ov[offset] = res
	return res, nil
}

func (tagV tagValues) getSTagValues(ctx context.Context, meta *format.MetricMetaValue, offset int64) (res []string, err error) {
	ov, ok := tagV.strTop[meta] // offset -> values
	if !ok {
		ov = make(map[int64][]string)
		tagV.strTop[meta] = ov
	}
	if res, ok = ov[offset]; ok {
		return res, nil
	}
	res, err = tagV.da.QuerySTagValues(ctx, meta, tagV.from, tagV.to)
	if err == nil {
		ov[offset] = res
	}
	return res, err
}

func getRawTagValue(val string) (id int32, err error) {
	var n int64
	n, err = strconv.ParseInt(val, 10, 32)
	if err == nil {
		return int32(n), nil
	}
	var f float64
	f, err = strconv.ParseFloat(val, 32)
	if err != nil {
		return 0, err
	}
	id, err = prometheus.LexEncode(float32(f))
	return id, err
}
