package stats

import (
	"fmt"
	"time"

	"github.com/prometheus/procfs"
)

type CPUStats struct {
	fs procfs.FS

	stat   procfs.Stat
	stats  map[int64]procfs.CPUStat
	pusher Pusher
}

const bt = "test_system_uptime"
const cpu = "test_cpu_usage" // "cpu_usage"
const irq = ""
const sirq = ""
const pc = "test_system_process_created"
const pr = "test_system_process_running"
const pb = "test_system_process_blocked"
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
	c.pusher.PushSystemMetricValue(cpu, t.User-oldT.User, "user")
	c.pusher.PushSystemMetricValue(cpu, t.Nice-oldT.Nice, "nice")
	c.pusher.PushSystemMetricValue(cpu, t.System-oldT.System, "system")
	c.pusher.PushSystemMetricValue(cpu, t.Idle-oldT.Idle, "idle")
	c.pusher.PushSystemMetricValue(cpu, t.Iowait-oldT.Iowait, "iowait")
	c.pusher.PushSystemMetricValue(cpu, t.IRQ-oldT.IRQ, "irq")
	c.pusher.PushSystemMetricValue(cpu, t.SoftIRQ-oldT.SoftIRQ, "softirq")
	c.pusher.PushSystemMetricValue(cpu, t.Steal-oldT.Steal, "steal")
	irqTotal := stat.IRQTotal - c.stat.IRQTotal
	sirqTotal := stat.SoftIRQTotal - c.stat.SoftIRQTotal
	c.pusher.PushSystemMetricValue(irq, float64(irqTotal))
	c.pusher.PushSystemMetricValue(sirq, float64(sirqTotal))
	return nil
}

func (c *CPUStats) pushSystemMetrics(stat procfs.Stat) error {
	uptime := uint64(time.Now().Unix()) - stat.BootTime
	c.pusher.PushSystemMetricValue(bt, float64(uptime))
	c.pusher.PushSystemMetricValue(pr, float64(stat.ProcessesRunning))
	c.pusher.PushSystemMetricCount(pc, float64(stat.ProcessCreated-c.stat.ProcessCreated))
	c.pusher.PushSystemMetricValue(pb, float64(stat.ProcessesBlocked))
	c.pusher.PushSystemMetricValue(cs, float64(stat.ContextSwitches))
	fmt.Println("push system metrics")
	return nil
}

func (c *CPUStats) updateCPUStats(new map[int64]procfs.CPUStat) error {
	for cpu, stat := range new {
		c.stats[cpu] = stat
	}
	return nil
}
