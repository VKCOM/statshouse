package stats

import (
	"testing"
	"time"

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

func BenchmarkSHWriterImpl(b *testing.B) {
	p := MetricWriterSHImpl{
		HostName: []byte{1},
		handler:  &handlerMock{},
		metric:   &tlstatshouse.MetricBytes{},
	}
	t := time.Now().Unix()
	for i := 0; i < b.N; i++ {
		p.WriteSystemMetricValue(t, "abc", 1, 1, 2, 3)
	}
}
