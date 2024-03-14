package stats

import (
	"testing"
	"time"

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
)

type handlerMock struct {
}

func (*handlerMock) HandleMetrics(data_model.HandlerArgs) (h data_model.MappedMetricHeader, done bool) {
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
		p.WriteSystemMetricCountValueExtendedTag(t, "abc", 1, 1, Tag{Raw: 2}, Tag{Raw: 3}, Tag{Str: "abc"})
	}
}
