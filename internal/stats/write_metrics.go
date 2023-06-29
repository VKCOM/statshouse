package stats

import (
	"strconv"

	"github.com/vkcom/statshouse-go"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tl"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/receiver"
)

type MetricWriter interface {
	WriteSystemMetricValue(nowUnix int64, name string, value float64, tagsList ...int32)
	WriteSystemMetricCount(nowUnix int64, name string, count float64, tagsList ...int32)
	WriteSystemMetricCountValue(nowUnix int64, name string, count, value float64, tagsList ...int32)
	WriteSystemMetricValueWithoutHost(nowUnix int64, name string, value float64, tagsList ...int32)
}

type MetricWriterRemoteImpl struct {
	HostName string
}

// MetricWriterSHImpl isn't thread safe
type MetricWriterSHImpl struct {
	HostName []byte
	handler  receiver.Handler
	metric   *tlstatshouse.MetricBytes
}

func buildTags(useHost bool, tags ...int32) statshouse.Tags {
	res := statshouse.Tags{}
	// Tag1 is reserved for host
	for index, tagV := range tags {
		tag := strconv.FormatInt(int64(tagV), 10)
		i := index
		if useHost {
			i++
		}
		if i+1 < len(res) {
			res[i+1] = tag
		}
	}
	return res
}

const (
	reservedKeys = 2
	usedCommon   = 1
)

func fillTags(metric *tlstatshouse.MetricBytes, reservedKeys int, startFrom int, tags ...int32) {
	// Tag1 is reserved for host
	for i, tag := range tags {
		i = i + reservedKeys
		t := &metric.Tags[startFrom]
		t.Key = strconv.AppendInt(t.Key[:0], int64(i), 10)
		t.Value = strconv.AppendInt(t.Value[:0], int64(tag), 10)
		startFrom++
	}
}

func (p *MetricWriterRemoteImpl) WriteSystemMetricValue(nowUnix int64, name string, value float64, tagsList ...int32) {
	tags := buildTags(true, tagsList...)
	tags[1] = p.HostName
	statshouse.Metric(name, tags).Value(value)
}

func (p *MetricWriterRemoteImpl) WriteSystemMetricCountValue(nowUnix int64, name string, count, value float64, tagsList ...int32) {
	tags := buildTags(true, tagsList...)
	tags[1] = p.HostName
	statshouse.Metric(name, tags).Count(count)
	statshouse.Metric(name, tags).Value(value)
}

func (p *MetricWriterRemoteImpl) WriteSystemMetricValueWithoutHost(nowUnix int64, name string, value float64, tagsList ...int32) {
	tags := buildTags(false, tagsList...)
	statshouse.Metric(name, tags).Value(value)
}

func (p *MetricWriterRemoteImpl) WriteSystemMetricCount(nowUnix int64, name string, count float64, tagsList ...int32) {
	tags := buildTags(true, tagsList...)
	tags[1] = p.HostName
	statshouse.Metric(name, tags).Count(count)
}

func (p *MetricWriterSHImpl) fillCommonMetric(m *tlstatshouse.MetricBytes, useHost bool, name string, nowUnix int64, tagsList ...int32) {
	m.Reset()
	m.Name = append(m.Name, name...)
	m.Ts = uint32(nowUnix)
	used := usedCommon
	reserved := reservedKeys
	if !useHost {
		used = 0
		reserved = 1
	}
	tagsLength := len(tagsList) + used
	if cap(m.Tags) < tagsLength {
		m.Tags = make([]tl.DictionaryFieldStringBytes, tagsLength)
	} else {
		m.Tags = m.Tags[:tagsLength]
	}
	if useHost {
		m.Tags[0].Key = append(m.Tags[0].Key[:0], "1"...)
		m.Tags[0].Value = append(m.Tags[0].Value[:0], p.HostName...)
	}
	fillTags(m, reserved, used, tagsList...)
}

func (p *MetricWriterSHImpl) WriteSystemMetricValue(nowUnix int64, name string, value float64, tagsList ...int32) {
	m := p.metric
	p.fillCommonMetric(m, true, name, nowUnix, tagsList...)
	m.Value = append(m.Value, value)
	_, _ = p.handler.HandleMetrics(m, nil)
}

func (p *MetricWriterSHImpl) WriteSystemMetricCountValue(nowUnix int64, name string, count, value float64, tagsList ...int32) {
	m := p.metric
	p.fillCommonMetric(m, true, name, nowUnix, tagsList...)
	m.Counter = count
	m.Value = append(m.Value, value)
	_, _ = p.handler.HandleMetrics(m, nil)
}

func (p *MetricWriterSHImpl) WriteSystemMetricValueWithoutHost(nowUnix int64, name string, value float64, tagsList ...int32) {
	m := p.metric
	p.fillCommonMetric(m, false, name, nowUnix, tagsList...)
	m.Value = append(m.Value, value)
	_, _ = p.handler.HandleMetrics(m, nil)
}

func (p *MetricWriterSHImpl) WriteSystemMetricCount(nowUnix int64, name string, count float64, tagsList ...int32) {
	m := p.metric
	p.fillCommonMetric(m, true, name, nowUnix, tagsList...)
	m.Counter = count
	_, _ = p.handler.HandleMetrics(m, nil)
}
