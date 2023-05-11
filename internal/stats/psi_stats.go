package stats

import (
	"fmt"
	"time"

	"github.com/prometheus/procfs"
	"go.uber.org/multierr"

	"github.com/vkcom/statshouse/internal/format"
)

type PSIStats struct {
	fs procfs.FS

	stats  map[string]procfs.PSIStats
	writer MetricWriter
}

var psiResources = []string{"cpu", "io", "memory"}

func (*PSIStats) Name() string {
	return "psi_stats"
}

func (c *PSIStats) PushDuration(now int64, d time.Duration) {
	c.writer.WriteSystemMetricValueWithoutHost(now, format.BuiltinMetricNameSystemMetricScrapeDuration, d.Seconds(), format.TagValueIDSystemMetricPSI)
}

func NewPSI(writer MetricWriter) (*PSIStats, error) {
	fs, err := procfs.NewFS(procfs.DefaultMountPoint)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize procfs: %w", err)
	}
	return &PSIStats{
		fs:     fs,
		stats:  map[string]procfs.PSIStats{},
		writer: writer,
	}, nil
}

func (c *PSIStats) WriteMetrics(nowUnix int64) error {
	var err error
	for _, resource := range psiResources {
		psi, error := c.fs.PSIStatsForResource(resource)
		if error != nil {
			err = multierr.Append(err, error)
			continue
		}
		old, ok := c.stats[resource]
		metricName := ""
		switch resource {
		case "mem":
			metricName = format.BuiltinMetricNamePSIMem
		case "cpu":
			metricName = format.BuiltinMetricNamePSICPU
		case "io":
			metricName = format.BuiltinMetricNamePSIIO
		}
		if ok && psi.Some != nil && old.Some != nil {
			c.writer.WriteSystemMetricValue(nowUnix, metricName, float64(psi.Some.Total-old.Some.Total), format.RawIDTagSome)
		}
		if ok && psi.Full != nil && old.Full != nil {
			c.writer.WriteSystemMetricValue(nowUnix, metricName, float64(psi.Full.Total-old.Full.Total), format.RawIDTagFull)
		}
		c.stats[resource] = psi
	}
	return err
}
