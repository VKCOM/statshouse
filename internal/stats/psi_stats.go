package stats

import (
	"fmt"

	"github.com/prometheus/procfs"
	"github.com/vkcom/statshouse/internal/format"
	"go.uber.org/multierr"
)

type PSIStats struct {
	fs procfs.FS

	stats  map[string]procfs.PSIStats
	pusher Pusher
}

var psiResources = []string{"cpu", "io", "memory"}

func (*PSIStats) Name() string {
	return "psi_stats"
}

func NewPSI(pusher Pusher) (*PSIStats, error) {
	fs, err := procfs.NewFS(procfs.DefaultMountPoint)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize procfs: %w", err)
	}
	return &PSIStats{
		fs:     fs,
		stats:  map[string]procfs.PSIStats{},
		pusher: pusher,
	}, nil
}

func (c *PSIStats) PushMetrics() error {
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
			c.pusher.PushSystemMetricValue(metricName, float64(psi.Some.Total-old.Some.Total), format.RawIDTagSome)
		}
		if ok && psi.Full != nil && old.Full != nil {
			c.pusher.PushSystemMetricValue(metricName, float64(psi.Full.Total-old.Full.Total), format.RawIDTagFull)
		}
		c.stats[resource] = psi
	}
	return err
}
