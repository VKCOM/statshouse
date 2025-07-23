package stats

import (
	"fmt"
	"os"
	"time"

	"github.com/prometheus/procfs"

	"github.com/VKCOM/statshouse/internal/format"
)

type SocksStats struct {
	fs     procfs.FS
	writer MetricWriter
}

var pageSize = os.Getpagesize()

func (c *SocksStats) Skip() bool {
	return false
}

func (*SocksStats) Name() string {
	return "socks_stats"
}

func (c *SocksStats) PushDuration(now int64, d time.Duration) {
	c.writer.WriteSystemMetricValueWithoutHost(now, format.BuiltinMetricMetaSystemMetricScrapeDuration.Name, d.Seconds(), format.TagValueIDSystemMetricSocksStat)
}

func NewSocksStats(writer MetricWriter) (*SocksStats, error) {
	fs, err := procfs.NewFS(procfs.DefaultMountPoint)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize procfs: %w", err)
	}
	return &SocksStats{
		fs:     fs,
		writer: writer,
	}, nil
}

func (c *SocksStats) WriteMetrics(nowUnix int64) error {
	stat, err := c.fs.NetSockstat()
	if err != nil {
		return fmt.Errorf("failed to get socks stats: %w", err)
	}
	c.writeTCPSockstat(nowUnix, stat)
	if err != nil {
		return fmt.Errorf("failed to update cpu stats: %w", err)
	}
	return nil
}

func (c *SocksStats) writeTCPSockstat(nowUnix int64, stat *procfs.NetSockstat) {
	for _, p := range stat.Protocols {
		protocol := p.Protocol
		alloc := p.Alloc
		inuse := p.InUse
		orphan := p.Orphan
		timewait := p.TW
		mem := p.Mem
		switch protocol {
		case "TCP":
		default:
			continue
		}
		c.writer.WriteSystemMetricValue(nowUnix, format.BuiltinMetricNameTCPSocketStatus, float64(inuse), format.RawIDTagInUse)
		if alloc != nil {
			c.writer.WriteSystemMetricValue(nowUnix, format.BuiltinMetricNameTCPSocketStatus, float64(*alloc), format.RawIDTagAlloc)
		}
		if orphan != nil {
			c.writer.WriteSystemMetricValue(nowUnix, format.BuiltinMetricNameTCPSocketStatus, float64(*orphan), format.RawIDTagOrphan)
		}
		if timewait != nil {
			c.writer.WriteSystemMetricValue(nowUnix, format.BuiltinMetricNameTCPSocketStatus, float64(*timewait), format.RawIDTagTW)
		}
		if mem != nil {
			c.writer.WriteSystemMetricValue(nowUnix, format.BuiltinMetricNameTCPSocketMemory, float64(*mem*pageSize))
		}

	}
}
