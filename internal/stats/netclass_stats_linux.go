//go:build linux

package stats

import (
	"fmt"
	"time"

	"github.com/prometheus/procfs/sysfs"

	"github.com/vkcom/statshouse/internal/format"
)

type NetClass struct {
	sysfs      sysfs.FS
	writer     MetricWriter
	deadDevice map[string]bool
}

func (c *NetClass) Skip() bool {
	return false
}

func (*NetClass) Name() string {
	return "net_class_stats"
}

func (c *NetClass) PushDuration(now int64, d time.Duration) {
	c.writer.WriteSystemMetricValueWithoutHost(now, format.BuiltinMetricNameSystemMetricScrapeDuration, d.Seconds(), format.TagValueIDSystemMetricNetClass)
}

func NewNetClassStats(writer MetricWriter) (*NetClass, error) {
	sysfs, err := sysfs.NewFS(sysfs.DefaultMountPoint)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize sysfs: %w", err)
	}
	return &NetClass{
		sysfs:      sysfs,
		writer:     writer,
		deadDevice: map[string]bool{},
	}, nil
}

func (c *NetClass) WriteMetrics(nowUnix int64) error {
	classes, err := c.sysfs.NetClassDevices()
	if err != nil {
		return fmt.Errorf("failed to load netclass: %v", err)
	}
	for _, iface := range classes {
		if c.deadDevice[iface] {
			continue
		}
		dev, err := c.sysfs.NetClassByIface(iface)
		if err != nil {
			c.deadDevice[iface] = true
		}
		c.pushMetric(nowUnix, dev)
	}
	return nil
}

func (c *NetClass) pushMetric(nowUnix int64, iface *sysfs.NetClassIface) {
	if iface.Speed != nil && *iface.Speed >= 0 {
		speedBytes := float64(*iface.Speed * 1000 * 1000 / 8)

		c.writer.WriteSystemMetricCountValueExtendedTag(nowUnix, format.BuiltinMetricNameNetDevSpeed, 1, speedBytes, Tag{
			Str: iface.Name,
		})
	}
}
