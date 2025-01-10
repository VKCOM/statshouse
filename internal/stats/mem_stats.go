package stats

import (
	"fmt"
	"time"

	"github.com/prometheus/procfs"

	"github.com/vkcom/statshouse/internal/format"
)

type MemStats struct {
	fs procfs.FS

	writer MetricWriter
}

const memStat = format.BuiltinMetricNameMemUsage

func (c *MemStats) Skip() bool {
	return false
}

func (*MemStats) Name() string {
	return "mem_stats"
}

func (c *MemStats) PushDuration(now int64, d time.Duration) {
	c.writer.WriteSystemMetricValueWithoutHost(now, format.BuiltinMetricMetaSystemMetricScrapeDuration.Name, d.Seconds(), format.TagValueIDSystemMetricMemory)
}

func NewMemoryStats(writer MetricWriter) (*MemStats, error) {
	fs, err := procfs.NewFS(procfs.DefaultMountPoint)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize procfs: %w", err)
	}
	return &MemStats{
		fs:     fs,
		writer: writer,
	}, nil
}

func (c *MemStats) WriteMetrics(nowUnix int64) error {
	const mult = 1024
	stat, err := c.fs.Meminfo()
	if err != nil {
		return fmt.Errorf("failed to get meminfo: %w", err)
	}

	if stat.SReclaimable != nil && stat.SUnreclaim != nil {
		c.writer.WriteSystemMetricValue(nowUnix, format.BuiltinMetricNameMemSLAB, float64(*stat.SReclaimable*mult), format.RawIDTagReclaimable)
		c.writer.WriteSystemMetricValue(nowUnix, format.BuiltinMetricNameMemSLAB, float64(*stat.SUnreclaim*mult), format.RawIDTagUnreclaimable)
	}
	if stat.Writeback != nil && stat.Dirty != nil {
		c.writer.WriteSystemMetricValue(nowUnix, format.BuiltinMetricNameWriteback, float64(*stat.Writeback*mult), format.RawIDTagWriteback)
		c.writer.WriteSystemMetricValue(nowUnix, format.BuiltinMetricNameWriteback, float64(*stat.Dirty*mult), format.RawIDTagDirty)
	}

	if stat.MemFree != nil && *stat.MemFree > memFreeUpperLimit {
		c.writer.WriteSystemMetricValue(nowUnix, memStat, float64(0), format.RawIDTagBadData)
		return nil
	}
	if stat.MemFree != nil {
		c.writer.WriteSystemMetricValue(nowUnix, memStat, float64(*stat.MemFree*mult), format.RawIDTagFree)
	}
	if stat.Buffers != nil {
		c.writer.WriteSystemMetricValue(nowUnix, memStat, float64(*stat.Buffers*mult), format.RawIDTagBuffers)
	}
	if stat.MemTotal != nil && stat.Buffers != nil && stat.Cached != nil && stat.SReclaimable != nil && stat.Shmem != nil && stat.MemFree != nil {
		cached := *stat.Cached + *stat.SReclaimable - *stat.Shmem
		used := *stat.MemTotal - *stat.MemFree - *stat.Buffers - cached
		c.writer.WriteSystemMetricValue(nowUnix, memStat, float64(used*mult), format.RawIDTagUsed)
		c.writer.WriteSystemMetricValue(nowUnix, memStat, float64(cached*mult), format.RawIDTagCached)
	}

	return nil
}
