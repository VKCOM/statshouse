package stats

import (
	"fmt"
	"os"
	"time"

	"github.com/prometheus/procfs"

	"github.com/vkcom/statshouse/internal/format"
)

type SocksStats struct {
	fs     procfs.FS
	writer MetricWriter
}

var pageSize = os.Getpagesize()

func (*SocksStats) Name() string {
	return "socks_stats"
}

func (c *SocksStats) PushDuration(now int64, d time.Duration) {
	c.writer.WriteSystemMetricValueWithoutHost(now, format.BuiltinMetricNameSystemMetricScrapeDuration, d.Seconds(), format.TagValueIDSystemMetricCPU)
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
	c.writeSockstat(nowUnix, stat)
	if err != nil {
		return fmt.Errorf("failed to update cpu stats: %w", err)
	}
	return nil
}

func (c *SocksStats) writeSockstat(nowUnix int64, stat *procfs.NetSockstat) {
	if stat.Used != nil {
		c.writer.WriteSystemMetricValue(nowUnix, format.BuiltinMetricNameSocketUsed, float64(*stat.Used))
	}
	for _, p := range stat.Protocols {
		protocol := p.Protocol
		alloc := p.Alloc
		inuse := p.InUse
		orphan := p.Orphan
		timewait := p.TW
		mem := p.Mem
		var protocolFlag int32
		switch protocol {
		case "TCP":
			protocolFlag = format.RawIDTagTCP
		case "UDP":
			protocolFlag = format.RawIDTagUDP
		default:
			continue
		}
		c.writer.WriteSystemMetricValue(nowUnix, format.BuiltinMetricNameSocketStatus, float64(inuse), protocolFlag, format.RawIDTagInUse)
		if alloc != nil {
			c.writer.WriteSystemMetricValue(nowUnix, format.BuiltinMetricNameSocketStatus, float64(*alloc), protocolFlag, format.RawIDTagAlloc)
		}
		if orphan != nil {
			c.writer.WriteSystemMetricValue(nowUnix, format.BuiltinMetricNameSocketStatus, float64(*orphan), protocolFlag, format.RawIDTagOrphan)
		}
		if timewait != nil {
			c.writer.WriteSystemMetricValue(nowUnix, format.BuiltinMetricNameSocketStatus, float64(*timewait), protocolFlag, format.RawIDTagTW)
		}
		if mem != nil {
			c.writer.WriteSystemMetricValue(nowUnix, format.BuiltinMetricNameSocketMemory, float64(*mem*pageSize), protocolFlag)
		}
	}
}
