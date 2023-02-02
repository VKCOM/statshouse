package sqlite

import (
	"time"

	"github.com/vkcom/statshouse-go"
)

type stats struct {
	serviceName string
}

const (
	durationMetric       = "sqlite_duration"
	applyQueueSizeMetric = "sqlite_apply_queue_size"
	query                = "sqlite_query"
	tx                   = "sqlite_tx"
	action               = "sqlite_action"
)

func (s *stats) queryDuration(typ, name string, duration time.Duration) {
	statshouse.AccessMetricRaw(durationMetric, statshouse.RawTags{Tag1: s.serviceName, Tag2: typ, Tag3: name}).Value(duration.Seconds())
}

func (s *stats) applyQueueSize(registry *statshouse.Registry, size int64) {
	registry.AccessMetricRaw(applyQueueSizeMetric, statshouse.RawTags{Tag1: s.serviceName}).Value(float64(size))
}
