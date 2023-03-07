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

type PusherSHImpl struct {
	HostName []byte
	handler  receiver.Handler
}

func NewPusherSHImpl(hostName string, handler receiver.Handler) Pusher {
	return &PusherSHImpl{
		HostName: []byte(hostName),
		handler:  handler,
	}
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

var indexToKey = [][]byte{[]byte("key0"), []byte("key1"), []byte("key2"), []byte("key3"), []byte("key4"), []byte("key5"), []byte("key6"), []byte("key7"), []byte("key8"), []byte("key9"), []byte("key10"), []byte("key11"), []byte("key12"), []byte("key13"), []byte("key14"), []byte("key15")}

func fillTags(metric *tlstatshouse.MetricBytes, tags ...int32) {
	// Tag1 is reserved for host
	for i, tag := range tags {
		t := tl.DictionaryFieldStringBytes{
			Key: indexToKey[i+2],
		}
		t.Value = strconv.AppendInt(t.Value, int64(tag), 10)
		metric.Tags = append(metric.Tags, t)
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

func (p *PusherSHImpl) PushSystemMetricValue(name string, value float64, tagsList ...int32) {
	metric := &tlstatshouse.MetricBytes{
		Tags:  nil,
		Value: []float64{value},
		Ts:    uint32(time.Now().Unix()),
	}
	metric.Name = append(metric.Name, name...)
	metric.Tags = append(metric.Tags, tl.DictionaryFieldStringBytes{
		Key:   indexToKey[1],
		Value: p.HostName,
	})
	fillTags(metric, tagsList...)
	p.handler.HandleMetrics(metric, nil)
}

// todo reuse metric
func (p *PusherSHImpl) PushSystemMetricCount(name string, count float64, tagsList ...int32) {
	metric := &tlstatshouse.MetricBytes{
		Tags:    nil,
		Counter: count,
		Ts:      uint32(time.Now().Unix()),
	}
	metric.Name = append(metric.Name, name...)
	metric.Tags = append(metric.Tags, tl.DictionaryFieldStringBytes{
		Key:   indexToKey[1],
		Value: p.HostName,
	})
	fillTags(metric, tagsList...)
	p.handler.HandleMetrics(metric, nil)
}

func str(x int32) string {
	return strconv.FormatInt(int64(x), 10)
}
