package stats

import (
	"strconv"

	"github.com/VKCOM/statshouse-go"
	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/data_model/gen2/tl"
	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/VKCOM/statshouse/internal/env"
	"github.com/VKCOM/statshouse/internal/receiver"
)

type MetricWriter interface {
	WriteSystemMetricValue(nowUnix int64, name string, value float64, tagsList ...int32)
	WriteSystemMetricCount(nowUnix int64, name string, count float64, tagsList ...int32)
	WriteSystemMetricCountValue(nowUnix int64, name string, count, value float64, tagsList ...int32)
	WriteSystemMetricValueWithoutHost(nowUnix int64, name string, value float64, tagsList ...int32)
	WriteSystemMetricCountValueWithoutHost(nowUnix int64, name string, count, value float64, tagsList ...int32)

	WriteSystemMetricCountExtendedTag(nowUnix int64, name string, count float64, tagsList ...Tag)
	WriteSystemMetricCountValueExtendedTag(nowUnix int64, name string, count, value float64, tagsList ...Tag)
}

// Golang StatsHouse library cannot fulfill all of our requirements in this case
type MetricWriterRemoteImpl struct {
	HostName  string
	envLoader *env.Loader
}

// MetricWriterSHImpl isn't thread safe
type MetricWriterSHImpl struct {
	HostName  []byte
	handler   receiver.Handler
	metric    *tlstatshouse.MetricBytes
	envLoader *env.Loader
}

type Tag struct {
	Raw int32
	Str string
}

func buildTags[A Tag | int32](useHost bool, tags ...A) statshouse.Tags {
	res := statshouse.Tags{}
	// Tag1 is reserved for host
	for index, tagV := range tags {
		tagI := any(tagV)
		tag := ""
		switch t := tagI.(type) {
		case int32:
			tag = strconv.FormatInt(int64(t), 10)
		case Tag:
			if t.Str == "" {
				tag = strconv.FormatInt(int64(t.Raw), 10)
			} else {
				tag = t.Str
			}
		}
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
	usedCommon   = 6 // envLoader, hostname, dc, group, region, owner
)

func fillTags[A Tag | int32](metric *tlstatshouse.MetricBytes, reservedKeysPrefix int, startFillFrom int, tags ...A) {
	// Tag1 is reserved for host
	for i, tag := range tags {
		i = i + reservedKeysPrefix
		t := &metric.Tags[startFillFrom]
		t.Key = strconv.AppendInt(t.Key[:0], int64(i), 10)
		tagI := any(tag)
		switch tagI := tagI.(type) {
		case int32:
			t.Value = strconv.AppendInt(t.Value[:0], int64(tagI), 10)
		case Tag:
			if tagI.Str == "" {
				t.Value = strconv.AppendInt(t.Value[:0], int64(tagI.Raw), 10)
			} else {
				t.Value = append(t.Value[:0], tagI.Str...)
			}
		}
		startFillFrom++
	}
}

func (p *MetricWriterRemoteImpl) fillCommonTags(tags *statshouse.Tags) {
	e := p.envLoader.Load()
	tags[0] = e.EnvT
	tags[1] = p.HostName
	tags[11] = e.DC
	tags[12] = e.Group
	tags[13] = e.Region
	tags[14] = e.Owner
}

func (p *MetricWriterRemoteImpl) WriteSystemMetricValue(nowUnix int64, name string, value float64, tagsList ...int32) {
	tags := buildTags(true, tagsList...)
	p.fillCommonTags(&tags)
	statshouse.Value(name, tags, value)
}

func (p *MetricWriterRemoteImpl) WriteSystemMetricCountValue(nowUnix int64, name string, count, value float64, tagsList ...int32) {
	tags := buildTags(true, tagsList...)
	p.fillCommonTags(&tags)
	statshouse.Count(name, tags, count)
	statshouse.Value(name, tags, value)
}

func (p *MetricWriterRemoteImpl) WriteSystemMetricCountValueWithoutHost(nowUnix int64, name string, count, value float64, tagsList ...int32) {
	tags := buildTags(false, tagsList...)
	statshouse.Value(name, tags, value)
	statshouse.Count(name, tags, count)
}

func (p *MetricWriterRemoteImpl) WriteSystemMetricValueWithoutHost(nowUnix int64, name string, value float64, tagsList ...int32) {
	tags := buildTags(false, tagsList...)
	statshouse.Value(name, tags, value)
}

func (p *MetricWriterRemoteImpl) WriteSystemMetricCount(nowUnix int64, name string, count float64, tagsList ...int32) {
	tags := buildTags(true, tagsList...)
	p.fillCommonTags(&tags)
	statshouse.Count(name, tags, count)
}

func (p *MetricWriterRemoteImpl) WriteSystemMetricCountValueExtendedTag(nowUnix int64, name string, count, value float64, tagsList ...Tag) {
	tags := buildTags(true, tagsList...)
	p.fillCommonTags(&tags)
	statshouse.Count(name, tags, count)
	statshouse.Value(name, tags, value)
}

func (p *MetricWriterRemoteImpl) WriteSystemMetricCountExtendedTag(nowUnix int64, name string, count float64, tagsList ...Tag) {
	tags := buildTags(true, tagsList...)
	p.fillCommonTags(&tags)
	statshouse.Count(name, tags, count)
}

func fillTag[T string | []byte](m *tlstatshouse.MetricBytes, i int, key string, value T) {
	m.Tags[i].Key = append(m.Tags[i].Key[:0], key...)
	m.Tags[i].Value = append(m.Tags[i].Value[:0], value...)
}

func fillCommonMetric[A Tag | int32](p *MetricWriterSHImpl, m *tlstatshouse.MetricBytes, useHostTags bool, name string, nowUnix int64, tagsList ...A) {
	m.Reset()
	m.Name = append(m.Name, name...)
	m.Ts = uint32(nowUnix)
	used := usedCommon
	reserved := reservedKeys
	if !useHostTags {
		used = 0
		reserved = 1
	}
	tagsLength := len(tagsList) + used
	if cap(m.Tags) < tagsLength {
		m.Tags = make([]tl.DictionaryFieldStringBytes, tagsLength)
	} else {
		m.Tags = m.Tags[:tagsLength]
	}
	e := p.envLoader.Load()
	if useHostTags {
		fillTag(m, 0, "0", e.EnvT)
		fillTag(m, 1, "1", p.HostName)
		fillTag(m, 2, "11", e.DC)
		fillTag(m, 3, "12", e.Group)
		fillTag(m, 4, "13", e.Region)
		fillTag(m, 5, "14", e.Owner)
	}
	fillTags(m, reserved, used, tagsList...)
}

func (p *MetricWriterSHImpl) WriteSystemMetricValue(nowUnix int64, name string, value float64, tagsList ...int32) {
	m := p.metric
	fillCommonMetric(p, m, true, name, nowUnix, tagsList...)
	m.Value = append(m.Value, value)
	_ = p.handler.HandleMetrics(data_model.HandlerArgs{MetricBytes: m})
}

func (p *MetricWriterSHImpl) WriteSystemMetricCountValue(nowUnix int64, name string, count, value float64, tagsList ...int32) {
	m := p.metric
	fillCommonMetric(p, m, true, name, nowUnix, tagsList...)
	m.Counter = count
	m.Value = append(m.Value, value)
	_ = p.handler.HandleMetrics(data_model.HandlerArgs{MetricBytes: m})
}

func (p *MetricWriterSHImpl) WriteSystemMetricValueWithoutHost(nowUnix int64, name string, value float64, tagsList ...int32) {
	m := p.metric
	fillCommonMetric(p, m, false, name, nowUnix, tagsList...)
	m.Value = append(m.Value, value)
	_ = p.handler.HandleMetrics(data_model.HandlerArgs{MetricBytes: m})
}

func (p *MetricWriterSHImpl) WriteSystemMetricCountValueWithoutHost(nowUnix int64, name string, count, value float64, tagsList ...int32) {
	m := p.metric
	fillCommonMetric(p, m, false, name, nowUnix, tagsList...)
	m.Counter = count
	m.Value = append(m.Value, value)
	_ = p.handler.HandleMetrics(data_model.HandlerArgs{MetricBytes: m})
}

func (p *MetricWriterSHImpl) WriteSystemMetricCount(nowUnix int64, name string, count float64, tagsList ...int32) {
	m := p.metric
	fillCommonMetric(p, m, true, name, nowUnix, tagsList...)
	m.Counter = count
	_ = p.handler.HandleMetrics(data_model.HandlerArgs{MetricBytes: m})
}

func (p *MetricWriterSHImpl) WriteSystemMetricCountValueExtendedTag(nowUnix int64, name string, count, value float64, tagsList ...Tag) {
	m := p.metric
	fillCommonMetric(p, m, true, name, nowUnix, tagsList...)
	m.Counter = count
	m.Value = append(m.Value, value)
	_ = p.handler.HandleMetrics(data_model.HandlerArgs{MetricBytes: m})
}

func (p *MetricWriterSHImpl) WriteSystemMetricCountExtendedTag(nowUnix int64, name string, count float64, tagsList ...Tag) {
	m := p.metric
	fillCommonMetric(p, m, true, name, nowUnix, tagsList...)
	m.Counter = count
	_ = p.handler.HandleMetrics(data_model.HandlerArgs{MetricBytes: m})
}
