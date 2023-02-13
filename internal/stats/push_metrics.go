package stats

import (
	"strconv"

	"github.com/vkcom/statshouse-go"
)

type Pusher struct {
	HostName string
}

func (p *Pusher) PushCPUUsage(value float64, id int64, mode string) {
	statshouse.AccessMetricRaw("test_cpu_metrics", statshouse.RawTags{
		Tag1: p.HostName,
		Tag2: strconv.FormatInt(id, 10),
		Tag3: mode,
	}).Value(value)
}
