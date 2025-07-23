package stats

import (
	"fmt"
	"time"

	"github.com/prometheus/procfs"

	"github.com/VKCOM/statshouse/internal/format"
)

type ProtocolsStats struct {
	fs     procfs.FS
	writer MetricWriter
}

func (c *ProtocolsStats) Skip() bool {
	return false
}

func (*ProtocolsStats) Name() string {
	return "protocols_stats"
}

func (c *ProtocolsStats) PushDuration(now int64, d time.Duration) {
	c.writer.WriteSystemMetricValueWithoutHost(now, format.BuiltinMetricMetaSystemMetricScrapeDuration.Name, d.Seconds(), format.TagValueIDSystemMetricProtocols)
}

func NewProtocolsStats(writer MetricWriter) (*ProtocolsStats, error) {
	fs, err := procfs.NewFS(procfs.DefaultMountPoint)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize procfs: %w", err)
	}
	return &ProtocolsStats{
		fs:     fs,
		writer: writer,
	}, nil
}

func (c *ProtocolsStats) WriteMetrics(nowUnix int64) error {
	stat, err := c.fs.NetProtocols()
	if err != nil {
		return fmt.Errorf("failed to get protocols stats: %w", err)
	}
	c.writeProtocols(nowUnix, stat)
	return nil
}

func (c *ProtocolsStats) writeProtocols(nowUnix int64, stat procfs.NetProtocolStats) {
	const mult = 4096
	for protocolName, stat := range stat {
		var protocol int32
		switch protocolName {
		case "TCP":
			protocol = format.RawIDTagTCP
		case "UDP":
			protocol = format.RawIDTagUDP
		case "NETLINK":
			protocol = format.RawIDTagNetlink
		case "UNIX":
			protocol = format.RawIDTagUnix
		case "UDP-Lite":
			protocol = format.RawIDTagUDPLite
		default:
			continue
		}
		sockets := stat.Sockets
		memory := stat.Memory
		if memory == -1 {
			memory = 0
		}
		if sockets == 0 {
			continue
		}
		c.writer.WriteSystemMetricValue(nowUnix, format.BuiltinMetricNameSocketUsedv2, float64(sockets), protocol)
		c.writer.WriteSystemMetricValue(nowUnix, format.BuiltinMetricNameSocketMemory, float64(memory*mult), protocol)
	}
}
