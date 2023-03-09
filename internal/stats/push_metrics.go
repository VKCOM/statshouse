package stats

import (
	"strconv"
	"time"

	"github.com/vkcom/statshouse-go"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tl"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/receiver"
)

type Pusher interface {
	PushSystemMetricValue(name string, value float64, tagsList ...int32)
	PushSystemMetricCount(name string, count float64, tagsList ...int32)
}

type PusherRemoteImpl struct {
	HostName string
}

// PusherSHImpl isn't thread safe
type PusherSHImpl struct {
	HostName []byte
	handler  receiver.Handler
	metric   *tlstatshouse.MetricBytes
}

func buildTags(tags ...int32) statshouse.RawTags {
	res := statshouse.RawTags{}
	// Tag1 is reserved for host
	for i, tagV := range tags {
		tag := strconv.FormatInt(int64(tagV), 10)
		switch i {
		case 0:
			res.Tag2 = tag
		case 1:
			res.Tag3 = tag
		case 2:
			res.Tag4 = tag
		case 3:
			res.Tag5 = tag
		case 4:
			res.Tag6 = tag
		case 5:
			res.Tag7 = tag
		case 6:
			res.Tag8 = tag
		case 7:
			res.Tag9 = tag
		case 8:
			res.Tag10 = tag
		case 9:
			res.Tag11 = tag
		case 10:
			res.Tag12 = tag
		case 11:
			res.Tag13 = tag
		case 12:
			res.Tag14 = tag
		case 13:
			res.Tag15 = tag
		}
	}
	return res
}

const reservedKeys = 2

func fillTags(metric *tlstatshouse.MetricBytes, tags ...int32) {
	// Tag1 is reserved for host
	for i, tag := range tags {
		i = i + reservedKeys
		t := &metric.Tags[i]
		t.Key = strconv.AppendInt(t.Key, int64(i), 10)
		t.Value = strconv.AppendInt(t.Value, int64(tag), 10)
	}
}

func (p *PusherRemoteImpl) PushSystemMetricValue(name string, value float64, tagsList ...int32) {
	tags := buildTags(tagsList...)
	tags.Tag1 = p.HostName
	statshouse.AccessMetricRaw(name, tags).Value(value)
}

func (p *PusherRemoteImpl) PushSystemMetricCount(name string, count float64, tagsList ...int32) {
	tags := buildTags(tagsList...)
	tags.Tag1 = p.HostName
	statshouse.AccessMetricRaw(name, tags).Count(count)
}

func (p *PusherSHImpl) fillCommonMetric(m *tlstatshouse.MetricBytes, name string, tagsList ...int32) {
	m.Reset()
	m.Name = append(m.Name, name...)
	m.Ts = uint32(time.Now().Unix())
	tagsLength := len(tagsList) + reservedKeys
	if cap(m.Tags) < tagsLength {
		m.Tags = make([]tl.DictionaryFieldStringBytes, tagsLength)
	} else {
		m.Tags = m.Tags[:tagsLength]
	}
	m.Tags[1].Key = append(m.Tags[1].Key, "1"...)
	m.Tags[1].Value = append(m.Tags[1].Value, p.HostName...)
	fillTags(m, tagsList...)
}

func (p *PusherSHImpl) PushSystemMetricValue(name string, value float64, tagsList ...int32) {
	m := p.metric
	p.fillCommonMetric(m, name, tagsList...)
	m.Value = append(m.Value, value)
	_, _ = p.handler.HandleMetrics(m, nil)
}

// todo reuse metric
func (p *PusherSHImpl) PushSystemMetricCount(name string, count float64, tagsList ...int32) {
	m := p.metric
	p.fillCommonMetric(m, name, tagsList...)
	m.Counter = count
	_, _ = p.handler.HandleMetrics(m, nil)
}
