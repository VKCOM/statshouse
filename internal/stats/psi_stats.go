package stats

import (
	"fmt"
	"os"
	"time"

	"github.com/prometheus/procfs"
	"go.uber.org/multierr"

	"github.com/VKCOM/statshouse/internal/format"
)

type PSIStats struct {
	fs     procfs.FS
	skip   bool
	stats  map[string]procfs.PSIStats
	writer MetricWriter
}

var psiResources = []string{"cpu", "io", "memory"}

func (*PSIStats) Name() string {
	return "psi_stats"
}

func (c *PSIStats) PushDuration(now int64, d time.Duration) {
	c.writer.WriteSystemMetricValueWithoutHost(now, format.BuiltinMetricMetaSystemMetricScrapeDuration.Name, d.Seconds(), format.TagValueIDSystemMetricPSI)
}

func (c *PSIStats) Skip() bool {
	return c.skip
}

func NewPSI(writer MetricWriter) (*PSIStats, error) {
	_, err := os.Stat(procfs.DefaultMountPoint + "/pressure")
	skip := os.IsNotExist(err)
	fs, err := procfs.NewFS(procfs.DefaultMountPoint)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize procfs: %w", err)
	}
	return &PSIStats{
		fs:     fs,
		skip:   skip,
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
		case "memory":
			metricName = format.BuiltinMetricNamePSIMem
		case "cpu":
			metricName = format.BuiltinMetricNamePSICPU
		case "io":
			metricName = format.BuiltinMetricNamePSIIO
		}
		if ok && psi.Some != nil && old.Some != nil {
			some := 1000 * time.Duration(psi.Some.Total-old.Some.Total)
			c.writer.WriteSystemMetricValue(nowUnix, metricName, some.Seconds(), format.RawIDTagSome)
		}
		if ok && psi.Full != nil && old.Full != nil {
			full := 1000 * time.Duration(psi.Full.Total-old.Full.Total)
			c.writer.WriteSystemMetricValue(nowUnix, metricName, full.Seconds(), format.RawIDTagFull)
		}
		c.stats[resource] = psi
	}
	return err
}
