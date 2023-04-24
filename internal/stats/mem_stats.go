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

const mem = format.BuiltinMetricNameMemUsage

func (*MemStats) Name() string {
	return "mem_stats"
}

func (c *MemStats) PushDuration(now int64, d time.Duration) {
	c.writer.WriteSystemMetricValueWithoutHost(now, format.BuiltinMetricNameSystemMetricScrapeDuration, d.Seconds(), format.TagValueIDSystemMetricMemory)
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
	stat, err := c.fs.Meminfo()
	if err != nil {
		return fmt.Errorf("failed to get meminfo: %w", err)
	}
	var total uint64
	var free uint64
	var buffers uint64
	var cached uint64
	var sreclaimable uint64
	var shmem uint64

	if stat.MemTotal != nil {
		total = *stat.MemTotal
	}
	if stat.MemFree != nil {
		free = *stat.MemFree
	}
	if stat.Buffers != nil {
		buffers = *stat.Buffers
	}
	if stat.Cached != nil {
		cached = *stat.Cached
	}
	if stat.SReclaimable != nil {
		sreclaimable = *stat.SReclaimable
	}
	if stat.Shmem != nil {
		shmem = *stat.Shmem
	}
	cached = cached + sreclaimable - shmem
	used := total - free - buffers - cached
	c.writer.WriteSystemMetricValue(nowUnix, mem, float64(free), format.RawIDTagFree)
	c.writer.WriteSystemMetricValue(nowUnix, mem, float64(used), format.RawIDTagUsed)

	c.writer.WriteSystemMetricValue(nowUnix, mem, float64(buffers), format.RawIDTagBuffers)
	c.writer.WriteSystemMetricValue(nowUnix, mem, float64(cached), format.RawIDTagCached)
	return nil
}
