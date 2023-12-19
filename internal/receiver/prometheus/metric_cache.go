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
	"github.com/vkcom/statshouse/internal/data_model/gen2/tl"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
)

type (
	metricCache     map[string]*metricCacheItem // by metric name
	cacheMetricType int
	cacheMetricKey  labels.Labels
)

const (
	cacheMetricTypeCounter cacheMetricType = iota
	cacheMetricTypeGauge
	cacheMetricTypeHistogram
)

type metricCacheItem struct {
	name    string
	typ     cacheMetricType
	samples map[string]*cacheMetricSample // by concatenated labels
}

type cacheMetricSample struct {
	timestamp int64
	count     float64
	buckets   []cacheMetricBucket // nil unless histogram
}

type cacheMetricBucket struct {
	info  *metricCacheItem
	tags  []tl.DictionaryFieldStringBytes
	le    int32
	count float64
}

type cacheMetric struct {
	name   string
	info   *metricCacheItem
	sample *cacheMetricSample
	tags   []tl.DictionaryFieldStringBytes
	le     int32
}

func (cache metricCache) processProtobufMeta(meta prompb.MetricMetadata) {
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
	cache.setMetricType(meta.MetricFamilyName, v)
}

func (cache metricCache) processTextParseType(name []byte, typ textparse.MetricType) {
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
	cache.setMetricType(string(name), v)
}

func (cache metricCache) setMetricType(name string, typ cacheMetricType) {
	info := cache[name]
	if info == nil || info.typ != typ {
		// reset if metric type differs
		info := &metricCacheItem{
			name:    name,
			typ:     typ,
			samples: map[string]*cacheMetricSample{},
		}
		cache[name] = info
		if typ == cacheMetricTypeHistogram {
			cache[name+_bucket] = info
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

func (cache metricCache) getMetric(key cacheMetricKey) (cacheMetric, bool) {
	if len(key) == 0 {
		return cacheMetric{}, false // label set is empty
	}
	var (
		name          string
		info          *metricCacheItem
		tags          = make([]tl.DictionaryFieldStringBytes, 0, len(key)-1)
		tagNameValues []string
		le            int32
		err           error
	)
	for _, l := range key {
		switch l.Name {
		case labels.MetricName:
			var ok bool
			info, ok = cache[l.Value]
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
			le, err = LexEncode(float32(x))
			if err != nil {
				break
			}
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
	if info == nil {
		return cacheMetric{}, false // metric name not found
	}
	var sampleKey string
	if tagNameValues != nil {
		sort.Strings(tagNameValues)
		sampleKey = strings.Join(tagNameValues, ",")
	} else {
		sampleKey = ""
	}
	sample, ok := info.samples[sampleKey]
	if !ok {
		sample = &cacheMetricSample{}
		if info.typ == cacheMetricTypeCounter {
			sample.count = -1 // to differ never set value from zero value
		}
		info.samples[sampleKey] = sample
	}
	return cacheMetric{name, info, sample, tags, le}, true
}

func (m *cacheMetric) processSample(v float64, t int64, s []tlstatshouse.MetricBytes) []tlstatshouse.MetricBytes {
	switch m.info.typ {
	case cacheMetricTypeCounter:
		if m.sample.count >= 0 { // counter was set at least once
			delta := v - m.sample.count
			if delta < 0 {
				delta = v // counter reset
			}
			if delta > 0 {
				bytes := tlstatshouse.MetricBytes{Name: []byte(m.info.name), Tags: m.tags}
				bytes.SetCounter(delta)
				if t != 0 {
					bytes.SetTs(uint32((t-1)/1_000 + 1))
				}
				s = append(s, bytes)
			}
		}
		m.sample.count = v
	case cacheMetricTypeGauge:
		// store gauges as is
		bytes := tlstatshouse.MetricBytes{Name: []byte(m.info.name), Tags: m.tags}
		bytes.SetValue([]float64{v})
		if t != 0 {
			bytes.SetTs(uint32((t-1)/1_000 + 1))
		}
		s = append(s, bytes)
	case cacheMetricTypeHistogram:
		// calculate histogram if sample timestamp changed
		if m.sample.timestamp != t {
			s = m.calculateHistogram(s)
			m.sample.timestamp = t
		}
		// append sample
		switch m.name {
		case m.info.name + _bucket:
			m.sample.buckets = append(m.sample.buckets, cacheMetricBucket{
				info:  m.info,
				tags:  m.tags,
				le:    m.le,
				count: v,
			})
		}
	}
	return s
}

func (m *cacheMetric) calculateHistogram(s []tlstatshouse.MetricBytes) []tlstatshouse.MetricBytes {
	if len(m.sample.buckets) == 0 {
		return s
	}
	// sort buckets by "le"
	buckets := m.sample.buckets
	sort.Slice(buckets, func(i, j int) bool { return buckets[i].le < buckets[j].le })
	// row per bucket
	for i := len(buckets); i != 0; i-- {
		bucket := buckets[i-1]
		if i != 1 {
			// bucket delta from absolute value
			bucket.count -= buckets[i-2].count
		}
		bytes := tlstatshouse.MetricBytes{
			Name: []byte(m.info.name),
			Tags: bucket.tags,
		}
		bytes.SetValue([]float64{bucket.count})
		if m.sample.timestamp != 0 {
			bytes.SetTs(uint32((m.sample.timestamp-1)/1_000 + 1))
		}
		s = append(s, bytes)
	}
	// clear references to "info" and "tags"
	for i := range m.sample.buckets {
		m.sample.buckets[i] = cacheMetricBucket{}
	}
	// reuse array
	m.sample.buckets = m.sample.buckets[:0]
	return s
}

func (cache metricCache) calculateHistograms(s []tlstatshouse.MetricBytes) []tlstatshouse.MetricBytes {
	for _, item := range cache {
		if item.typ == cacheMetricTypeHistogram {
			for _, sample := range item.samples {
				if len(sample.buckets) != 0 {
					metric := &cacheMetric{name: item.name, info: item, sample: sample}
					s = metric.calculateHistogram(s)
					metric.sample.timestamp = 0
				}
			}
		}
	}
	return s
}
