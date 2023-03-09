package stats

import (
	"testing"

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/mapping"
)

type handlerMock struct {
}

func (*handlerMock) HandleMetrics(*tlstatshouse.MetricBytes, mapping.MapCallbackFunc) (h data_model.MappedMetricHeader, done bool) {
	return data_model.MappedMetricHeader{}, true
}
func (*handlerMock) HandleParseError([]byte, error) {}

func BenchmarkSHPusherImpl(b *testing.B) {
	p := PusherSHImpl{
		HostName: []byte{1},
		handler:  &handlerMock{},
		metric:   &tlstatshouse.MetricBytes{},
	}
	for i := 0; i < b.N; i++ {
		p.PushSystemMetricValue("abc", 1, 1, 2, 3)
		p.PushSystemMetricCount("abc", 1, 1, 2, 3)

	}
}
