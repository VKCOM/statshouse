package stats

import (
	"fmt"

	"github.com/prometheus/procfs"
)

type MemStats struct {
	fs procfs.FS

	pusher Pusher
}

const mem = "test_mem_usage"

func (*MemStats) Name() string {
	return "mem_stats"
}

func NewMemoryStats(pusher Pusher) (*MemStats, error) {
	fs, err := procfs.NewFS(procfs.DefaultMountPoint)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize procfs: %w", err)
	}
	return &MemStats{
		fs:     fs,
		pusher: pusher,
	}, nil
}

func (c *MemStats) PushMetrics() error {
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
	// var dirty uint64
	// var writeBack uint64

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
	//if stat.Dirty != nil {
	//	dirty = *stat.Dirty
	//}
	//if stat.Writeback != nil {
	//	writeBack = *stat.Writeback
	//}
	cached = cached + sreclaimable - shmem //
	used := total - free - buffers - cached
	c.pusher.PushSystemMetricValue(mem, float64(free), "free")
	c.pusher.PushSystemMetricValue(mem, float64(used), "used")

	c.pusher.PushSystemMetricValue(mem, float64(buffers), "buffers")
	c.pusher.PushSystemMetricValue(mem, float64(cached), "cached")
	return nil
}
