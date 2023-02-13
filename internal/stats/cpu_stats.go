package stats

import (
	"fmt"

	"github.com/prometheus/procfs"
)

type CPUStats struct {
	fs procfs.FS

	stats map[int64]procfs.CPUStat

	pushCPUStat func(value float64, id int64, mode string)
}

func NewCpuStats(pushCPUStat func(value float64, id int64, mode string)) (*CPUStats, error) {
	fs, err := procfs.NewFS(procfs.DefaultMountPoint)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize procfs: %w", err)
	}
	return &CPUStats{
		fs:          fs,
		stats:       map[int64]procfs.CPUStat{},
		pushCPUStat: pushCPUStat,
	}, nil
}

func (c *CPUStats) PushMetrics() error {
	err := c.pushCPUUsage()
	if err != nil {
		return fmt.Errorf("failed to push cpu usage: %w", err)
	}
	return nil
}

func (c *CPUStats) pushCPUUsage() error {
	stat, err := c.fs.Stat()
	if err != nil {
		return fmt.Errorf("failed to get cpu stats: %w", err)
	}
	err = c.updateCPUStats(stat.CPU)
	if err != nil {
		return fmt.Errorf("failed to update cpu stats: %w", err)
	}
	for cpu, stat := range c.stats {
		c.pushCPUStat(stat.User, cpu, "user")
		c.pushCPUStat(stat.Nice, cpu, "nice")
		c.pushCPUStat(stat.System, cpu, "system")
		c.pushCPUStat(stat.Idle, cpu, "idle")
		c.pushCPUStat(stat.Iowait, cpu, "iowait")
		c.pushCPUStat(stat.IRQ, cpu, "irq")
		c.pushCPUStat(stat.SoftIRQ, cpu, "softirq")
		c.pushCPUStat(stat.Steal, cpu, "steal")
	}
	return nil
}

func (c *CPUStats) updateCPUStats(new map[int64]procfs.CPUStat) error {
	for cpu, stat := range new {
		c.stats[cpu] = stat
	}
	return nil
}
