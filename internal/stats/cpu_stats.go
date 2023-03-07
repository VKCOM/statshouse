package stats

import (
	"fmt"
	"time"

	"github.com/prometheus/procfs"
	"github.com/vkcom/statshouse/internal/format"
)

type CPUStats struct {
	fs procfs.FS

	stat   procfs.Stat
	stats  map[int64]procfs.CPUStat
	pusher Pusher
}

const bt = "test_system_uptime"
const cpu = format.BuiltinMetricNameCpuUsage
const irq = ""
const sirq = ""
const cs = ""

func (*CPUStats) Name() string {
	return "cpu_stats"
}

func NewCpuStats(pusher Pusher) (*CPUStats, error) {
	fs, err := procfs.NewFS(procfs.DefaultMountPoint)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize procfs: %w", err)
	}
	return &CPUStats{
		fs:     fs,
		stats:  map[int64]procfs.CPUStat{},
		pusher: pusher,
	}, nil
}

func (c *CPUStats) PushMetrics() error {
	stat, err := c.fs.Stat()
	if err != nil {
		return fmt.Errorf("failed to get cpu stats: %w", err)
	}
	pushMetric := true
	if c.stat.BootTime == 0 {
		pushMetric = false
	}
	if pushMetric {
		err = c.pushCPUMetrics(stat)
		if err != nil {
			//todo log
			return fmt.Errorf("failed to push cpu metrics: %w", err)
		}
		err = c.pushSystemMetrics(stat)
		if err != nil {
			//todo log
			return fmt.Errorf("failed to push system metrics: %w", err)
		}
	}
	c.stat = stat
	err = c.updateCPUStats(stat.CPU)
	if err != nil {
		return fmt.Errorf("failed to update cpu stats: %w", err)
	}
	return nil
}

func (c *CPUStats) pushCPUMetrics(stat procfs.Stat) error {
	t := stat.CPUTotal
	oldT := c.stat.CPUTotal
	c.pusher.PushSystemMetricValue(cpu, t.User-oldT.User, format.RawIDTagUser)
	c.pusher.PushSystemMetricValue(cpu, t.Nice-oldT.Nice, format.RawIDTagNice)
	c.pusher.PushSystemMetricValue(cpu, t.System-oldT.System, format.RawIDTagSystem)
	c.pusher.PushSystemMetricValue(cpu, t.Idle-oldT.Idle, format.RawIDTagIdle)
	c.pusher.PushSystemMetricValue(cpu, t.Iowait-oldT.Iowait, format.RawIDTagIOWait)
	c.pusher.PushSystemMetricValue(cpu, t.IRQ-oldT.IRQ, format.RawIDTagIRQ)
	c.pusher.PushSystemMetricValue(cpu, t.SoftIRQ-oldT.SoftIRQ, format.RawIDTagSoftIRQ)
	c.pusher.PushSystemMetricValue(cpu, t.Steal-oldT.Steal, format.RawIDTagSteal)
	irqTotal := stat.IRQTotal - c.stat.IRQTotal
	sirqTotal := stat.SoftIRQTotal - c.stat.SoftIRQTotal
	c.pusher.PushSystemMetricValue(irq, float64(irqTotal))
	c.pusher.PushSystemMetricValue(sirq, float64(sirqTotal))
	return nil
}

func (c *CPUStats) pushSystemMetrics(stat procfs.Stat) error {
	uptime := uint64(time.Now().Unix()) - stat.BootTime
	c.pusher.PushSystemMetricValue(bt, float64(uptime))
	c.pusher.PushSystemMetricValue(format.BuiltinMetricNameProcessStatus, float64(stat.ProcessesRunning), format.RawIDTagRunning)
	c.pusher.PushSystemMetricValue(format.BuiltinMetricNameProcessStatus, float64(stat.ProcessesBlocked), format.RawIDTagBlocked)
	c.pusher.PushSystemMetricCount(format.BuiltinMetricNameProcessCreated, float64(stat.ProcessCreated-c.stat.ProcessCreated))
	c.pusher.PushSystemMetricValue(cs, float64(stat.ContextSwitches))
	return nil
}

func (c *CPUStats) updateCPUStats(new map[int64]procfs.CPUStat) error {
	for cpu, stat := range new {
		c.stats[cpu] = stat
	}
	return nil
}
