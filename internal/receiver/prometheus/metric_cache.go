// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package prometheus

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/textparse"
	"github.com/prometheus/prometheus/prompb"
	"github.com/vkcom/statshouse-go"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tl"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
)

type (
	metricCache     map[string]*cacheMetricMeta // by metric name
	cacheMetricType int
	cacheMetricKey  labels.Labels
)

const (
	cacheMetricTypeCounter cacheMetricType = iota
	cacheMetricTypeGauge
	cacheMetricTypeHistogram
)

type cacheMetricMeta struct {
	name   string
	typ    cacheMetricType
	series map[string]*cacheDigestPair // by concatenated labels
}

type cacheDigestPair struct {
	prev cacheDigest
	curr cacheDigest
}

type cacheDigest struct {
	val    float64
	cnt    float64
	valSet bool
	cntSet bool
}

type cacheMetric struct {
	name string
	meta *cacheMetricMeta
	d    *cacheDigestPair
	tags []tl.DictionaryFieldStringBytes
}

func (c metricCache) processProtobufMeta(meta prompb.MetricMetadata) {
	var v cacheMetricType
	switch meta.Type {
	case prompb.MetricMetadata_COUNTER:
		v = cacheMetricTypeCounter
	case prompb.MetricMetadata_GAUGE:
		v = cacheMetricTypeGauge
	case prompb.MetricMetadata_HISTOGRAM:
		v = cacheMetricTypeHistogram
	default:
		return // not supported
	}
	c.setMetricType(meta.MetricFamilyName, v)
}

func (c metricCache) processTextParseType(name []byte, typ textparse.MetricType) {
	var v cacheMetricType
	switch typ {
	case textparse.MetricTypeCounter:
		v = cacheMetricTypeCounter
	case textparse.MetricTypeGauge:
		v = cacheMetricTypeGauge
	case textparse.MetricTypeHistogram:
		v = cacheMetricTypeHistogram
	default:
		return // not supported
	}
	c.setMetricType(string(name), v)
}

func (c metricCache) setMetricType(name string, typ cacheMetricType) {
	info := c[name]
	if info == nil || info.typ != typ { // reset if metric type differs
		info := &cacheMetricMeta{
			name:   name,
			typ:    typ,
			series: map[string]*cacheDigestPair{},
		}
		if typ == cacheMetricTypeHistogram {
			c[name+"_sum"] = info
			c[name+"_count"] = info
		} else {
			c[name] = info
		}
	}
}

func metricKeyFromProtobufLabels(s []prompb.Label) cacheMetricKey {
	key := make(cacheMetricKey, 0, len(s))
	for _, l := range s {
		key = append(key, labels.Label{Name: l.Name, Value: l.Value})
	}
	return key
}

func (c metricCache) getMetric(key cacheMetricKey) (cacheMetric, bool) {
	if len(key) == 0 {
		return cacheMetric{}, false // label set is empty
	}
	var (
		name          string
		meta          *cacheMetricMeta
		tags          = make([]tl.DictionaryFieldStringBytes, 0, len(key)-1)
		tagNameValues []string
		le            int32
		err           error
	)
	for _, l := range key {
		switch l.Name {
		case labels.MetricName:
			var ok bool
			meta, ok = c[l.Value]
			if !ok {
				return cacheMetric{}, false // unknown metric
			}
			name = l.Value
		case labels.BucketLabel:
			var x float64
			x, err = strconv.ParseFloat(l.Value, 32)
			if err != nil {
				break
			}
			le = statshouse.LexEncode(float32(x))
			tags = append(tags, tl.DictionaryFieldStringBytes{Key: []byte(format.LETagName), Value: []byte(strconv.FormatInt(int64(le), 10))})
		default:
			tags = append(tags, tl.DictionaryFieldStringBytes{Key: []byte(l.Name), Value: []byte(l.Value)})
			if tagNameValues == nil {
				tagNameValues = make([]string, 0, len(key)-1) // exclude "__name__" label
			}
			tagNameValues = append(tagNameValues, fmt.Sprintf("%v=\"%v\"", l.Name, l.Value))
		}
		if err != nil {
			break
		}
	}
	if meta == nil {
		return cacheMetric{}, false // metric name not found
	}
	var k string
	if tagNameValues != nil {
		sort.Strings(tagNameValues)
		k = strings.Join(tagNameValues, ",")
	} else {
		k = ""
	}
	v, ok := meta.series[k]
	if !ok {
		v = &cacheDigestPair{}
		meta.series[k] = v
	}
	res := cacheMetric{
		name: name,
		meta: meta,
		d:    v,
		tags: tags,
	}
	return res, true
}

func (m *cacheMetric) processSample(v float64, t int64, namespace string, s []tlstatshouse.MetricBytes) []tlstatshouse.MetricBytes {
	var metricName string
	if len(namespace) != 0 {
		metricName = namespace + format.NamespaceSeparator + m.meta.name
	} else {
		metricName = m.meta.name
	}
	switch m.meta.typ {
	case cacheMetricTypeCounter:
		bytes := tlstatshouse.MetricBytes{Name: []byte(metricName), Tags: m.tags}
		bytes.SetCounter(v)
		if t != 0 {
			bytes.SetTs(uint32((t-1)/1_000 + 1))
		}
		s = append(s, bytes)
	case cacheMetricTypeGauge:
		bytes := tlstatshouse.MetricBytes{Name: []byte(metricName), Tags: m.tags}
		bytes.SetValue([]float64{v})
		if t != 0 {
			bytes.SetTs(uint32((t-1)/1_000 + 1))
		}
		s = append(s, bytes)
	case cacheMetricTypeHistogram:
		if strings.HasSuffix(m.name, "_count") {
			if m.d.prev.cntSet {
				m.d.curr.cnt = v
				m.d.curr.cntSet = true
			} else {
				m.d.prev.cnt = v
				m.d.prev.cntSet = true
			}
		}
		if strings.HasSuffix(m.name, "_sum") {
			if m.d.prev.valSet {
				m.d.curr.val = v
				m.d.curr.valSet = true
			} else {
				m.d.prev.val = v
				m.d.prev.valSet = true
			}
		}
		if m.d.prev.cntSet && m.d.prev.valSet && m.d.curr.cntSet && m.d.curr.valSet {
			d := m.d.curr.cnt - m.d.prev.cnt
			if d < 0 {
				d = m.d.curr.cnt // counter reset
			}
			if d > 0 {
				bytes := tlstatshouse.MetricBytes{Name: []byte(metricName + "_sum"), Tags: m.tags}
				bytes.SetCounter(d)
				bytes.SetValue([]float64{m.d.curr.val - m.d.prev.val})
				if t != 0 {
					bytes.SetTs(uint32((t-1)/1_000 + 1))
				}
				s = append(s, bytes)
			}
			m.d.prev = m.d.curr
			m.d.curr = cacheDigest{}
		}
	}
	return s
}
