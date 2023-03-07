package stats

import (
	"fmt"

	"github.com/prometheus/procfs/blockdevice"
	"github.com/vkcom/statshouse/internal/format"
)

type DiskStats struct {
	fs blockdevice.FS

	pusher Pusher
	old    map[string]blockdevice.Diskstats
}

const disk = "test_block_io"

func (*DiskStats) Name() string {
	return "disk_stats"
}

func NewDiskStats(pusher Pusher) (*DiskStats, error) {
	fs, err := blockdevice.NewFS(procPath, sysPath)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize procfs: %w", err)
	}
	return &DiskStats{
		fs:     fs,
		pusher: pusher,
	}, nil
}

func (c *DiskStats) PushMetrics() error {
	stats, err := c.fs.ProcDiskstats()
	if err != nil {
		return fmt.Errorf("failed to get disk stats: %w", err)
	}
	for _, stat := range stats {
		device := stat.DeviceName
		oldStat, ok := c.old[device]
		c.old[device] = stat
		if !ok {
			continue
		}
		readIO := stat.ReadIOs - oldStat.ReadIOs
		writeIO := stat.WriteIOs - oldStat.WriteIOs
		discardIO := stat.DiscardIOs - oldStat.DiscardIOs

		c.pusher.PushSystemMetricCount(disk, float64(readIO), format.RawIDTagRead)
		c.pusher.PushSystemMetricCount(disk, float64(writeIO), format.RawIDTagWrite)
		c.pusher.PushSystemMetricCount(disk, float64(discardIO), format.RawIDTagDiscard)

		readIOTicks := float64(stat.ReadTicks-oldStat.ReadTicks) / 1000
		writeIOTicks := float64(stat.WriteTicks-oldStat.WriteTicks) / 1000
		discardIOTicks := float64(stat.DiscardTicks-oldStat.DiscardTicks) / 1000

		c.pusher.PushSystemMetricValue(disk, readIOTicks, format.RawIDTagRead)
		c.pusher.PushSystemMetricValue(disk, writeIOTicks, format.RawIDTagWrite)
		c.pusher.PushSystemMetricValue(disk, discardIOTicks, format.RawIDTagDiscard)
	}
	return nil
}
