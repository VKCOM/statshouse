package stats

import (
	"runtime/debug"
	"time"

	"github.com/VKCOM/statshouse/internal/format"
)

type GoStats struct {
	stat   *debug.GCStats
	writer MetricWriter
}

func (c *GoStats) Skip() bool {
	return false
}

func (*GoStats) Name() string {
	return "go_stats"
}

func (c *GoStats) PushDuration(now int64, d time.Duration) {
	c.writer.WriteSystemMetricValueWithoutHost(now, format.BuiltinMetricMetaSystemMetricScrapeDuration.Name, d.Seconds(), format.TagValueIDSystemMetricGCStats)
}

func NewGoStats(writer MetricWriter) (*GoStats, error) {
	return &GoStats{writer: writer}, nil
}

func (c *GoStats) WriteMetrics(nowUnix int64) error {
	stats := debug.GCStats{}
	debug.ReadGCStats(&stats)
	if c.stat != nil {
		if stats.NumGC > c.stat.NumGC {
			gcDiff := float64(stats.NumGC - c.stat.NumGC)
			duration := stats.PauseTotal - c.stat.PauseTotal
			c.writer.WriteSystemMetricCountValueWithoutHost(nowUnix, format.BuiltinMetricMetaGCDuration.Name, gcDiff, duration.Seconds()/gcDiff, 0, format.TagValueIDComponentAgent)
		}
	}
	c.stat = &stats
	return nil
}
