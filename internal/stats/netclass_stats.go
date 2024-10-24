//go:build !linux

package stats

import (
	"time"
)

type NetClass struct {
}

func (c *NetClass) Skip() bool {
	return false
}

func (*NetClass) Name() string {
	return "net_class_stats"
}

func (c *NetClass) PushDuration(now int64, d time.Duration) {
}

func NewNetClassStats(writer MetricWriter) (*NetClass, error) {
	return &NetClass{}, nil
}

func (c *NetClass) WriteMetrics(nowUnix int64) error {
	return nil
}
