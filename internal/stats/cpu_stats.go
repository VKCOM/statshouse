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
	writer MetricWriter
}

const (
	cpu  = format.BuiltinMetricNameCpuUsage
	sirq = format.BuiltinMetricNameSoftIRQ
	cs   = format.BuiltinMetricNameContextSwitch
)

func (c *CPUStats) Skip() bool {
	return false
}

func (*CPUStats) Name() string {
	return "cpu_stats"
}

func (c *CPUStats) PushDuration(now int64, d time.Duration) {
	c.writer.WriteSystemMetricValueWithoutHost(now, format.BuiltinMetricNameSystemMetricScrapeDuration, d.Seconds(), format.TagValueIDSystemMetricCPU)
}

func NewCpuStats(writer MetricWriter) (*CPUStats, error) {
	fs, err := procfs.NewFS(procfs.DefaultMountPoint)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize procfs: %w", err)
	}
	return &CPUStats{
		fs:     fs,
		stats:  map[int64]procfs.CPUStat{},
		writer: writer,
	}, nil
}

func (c *CPUStats) WriteMetrics(nowUnix int64) error {
	stat, err := c.fs.Stat()
	if err != nil {
		return fmt.Errorf("failed to get cpu stats: %w", err)
	}
	writeMetric := true
	if c.stat.BootTime == 0 {
		writeMetric = false
	}
	if writeMetric {
		err = c.writeCPU(nowUnix, stat)
		if err != nil {
			return fmt.Errorf("failed to write cpu metrics: %w", err)
		}
		err = c.writeSystem(nowUnix, stat)
		if err != nil {
			return fmt.Errorf("failed to write system metrics: %w", err)
		}
	}
	c.stat = stat
	if err != nil {
		return fmt.Errorf("failed to update cpu stats: %w", err)
	}
	return nil
}

func (c *CPUStats) writeCPU(nowUnix int64, stat procfs.Stat) error {
	for core, t := range stat.CPU {
		oldT := c.stat.CPU[core]
		coreI := int32(core)
		c.writer.WriteSystemMetricValue(nowUnix, cpu, t.User-oldT.User, format.RawIDTagUser, coreI)
		c.writer.WriteSystemMetricValue(nowUnix, cpu, t.Nice-oldT.Nice, format.RawIDTagNice, coreI)
		c.writer.WriteSystemMetricValue(nowUnix, cpu, t.System-oldT.System, format.RawIDTagSystem, coreI)
		c.writer.WriteSystemMetricValue(nowUnix, cpu, t.Idle-oldT.Idle, format.RawIDTagIdle, coreI)
		c.writer.WriteSystemMetricValue(nowUnix, cpu, t.Iowait-oldT.Iowait, format.RawIDTagIOWait, coreI)
		c.writer.WriteSystemMetricValue(nowUnix, cpu, t.IRQ-oldT.IRQ, format.RawIDTagIRQ, coreI)
		c.writer.WriteSystemMetricValue(nowUnix, cpu, t.SoftIRQ-oldT.SoftIRQ, format.RawIDTagSoftIRQ, coreI)
		c.writer.WriteSystemMetricValue(nowUnix, cpu, t.Steal-oldT.Steal, format.RawIDTagSteal, coreI)
		c.writer.WriteSystemMetricValue(nowUnix, cpu, t.Guest-oldT.Guest, format.RawIDTagGuest, coreI)
		c.writer.WriteSystemMetricValue(nowUnix, cpu, t.GuestNice-oldT.GuestNice, format.RawIDTagGuestNice, coreI)

	}
	c.writer.WriteSystemMetricCount(nowUnix, format.BuiltinMetricNameIRQ, diff(stat.IRQTotal, c.stat.IRQTotal))

	sirqs := stat.SoftIRQ
	sirqsOld := c.stat.SoftIRQ

	c.writer.WriteSystemMetricValue(nowUnix, sirq, diff(sirqs.Hi, sirqsOld.Hi), format.RawIDTagHI)
	c.writer.WriteSystemMetricValue(nowUnix, sirq, diff(sirqs.Timer, sirqsOld.Timer), format.RawIDTagTimer)
	c.writer.WriteSystemMetricValue(nowUnix, sirq, diff(sirqs.NetTx, sirqsOld.NetTx), format.RawIDTagNetTx)
	c.writer.WriteSystemMetricValue(nowUnix, sirq, diff(sirqs.NetRx, sirqsOld.NetRx), format.RawIDTagNetRx)
	c.writer.WriteSystemMetricValue(nowUnix, sirq, diff(sirqs.Block, sirqsOld.Block), format.RawIDTagBlock)
	c.writer.WriteSystemMetricValue(nowUnix, sirq, diff(sirqs.BlockIoPoll, sirqsOld.BlockIoPoll), format.RawIDTagBlockIOPoll)
	c.writer.WriteSystemMetricValue(nowUnix, sirq, diff(sirqs.Tasklet, sirqsOld.Tasklet), format.RawIDTagTasklet)
	c.writer.WriteSystemMetricValue(nowUnix, sirq, diff(sirqs.Sched, sirqsOld.Sched), format.RawIDTagScheduler)
	c.writer.WriteSystemMetricValue(nowUnix, sirq, diff(sirqs.Hrtimer, sirqsOld.Hrtimer), format.RawIDTagHRTimer)
	c.writer.WriteSystemMetricValue(nowUnix, sirq, diff(sirqs.Rcu, sirqsOld.Rcu), format.RawIDTagRCU)

	return nil
}

func (c *CPUStats) writeSystem(nowUnix int64, stat procfs.Stat) error {
	uptime := uint64(time.Now().Unix()) - stat.BootTime
	c.writer.WriteSystemMetricValue(nowUnix, format.BuiltinMetricNameSystemUptime, float64(uptime))
	c.writer.WriteSystemMetricValue(nowUnix, format.BuiltinMetricNameProcessStatus, float64(stat.ProcessesRunning), format.RawIDTagRunning)
	c.writer.WriteSystemMetricValue(nowUnix, format.BuiltinMetricNameProcessStatus, float64(stat.ProcessesBlocked), format.RawIDTagBlocked)
	c.writer.WriteSystemMetricCount(nowUnix, format.BuiltinMetricNameProcessCreated, float64(stat.ProcessCreated-c.stat.ProcessCreated))
	c.writer.WriteSystemMetricCount(nowUnix, cs, diff(stat.ContextSwitches, c.stat.ContextSwitches))
	return nil
}
