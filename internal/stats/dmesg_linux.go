//go:build linux

package stats

import (
	"fmt"
	"syscall"

	"github.com/vkcom/statshouse/internal/format"
	"go.uber.org/multierr"
)

func (c *DMesgStats) WriteMetrics(nowUnix int64) error {
	defer func() {
		err := recover()
		if err != nil {
			c.writer.WriteSystemMetricValueWithoutHost(nowUnix, format.BuiltinMetricNameStatsHouseErrors, 0, format.TagValueIDDMESGParseError)
			panic(err)
		}
	}()
	n, err := syscall.Klogctl(SYSLOG_ACTION_SIZE_BUFFER, nil)
	if err != nil {
		return fmt.Errorf("failed to read klog size: %w", multierr.Append(err, errStopCollector))
	}
	if cap(c.cache) < n {
		c.cache = make([]byte, n)
	}
	c.cache = c.cache[:n]
	k, err := syscall.Klogctl(SYSLOG_ACTION_READ_ALL, c.cache)
	if err != nil {
		return fmt.Errorf("failed to read klog: %w", multierr.Append(err, errStopCollector))
	}
	c.cache = c.cache[:k]
	err = c.handleMsgs(nowUnix, c.cache, c.pushStat, c.pushMetric)
	c.pushStat = true
	if err != nil {
		c.writer.WriteSystemMetricValueWithoutHost(nowUnix, format.BuiltinMetricNameStatsHouseErrors, 0, format.TagValueIDDMESGParseError)
		return errStopCollector
	}
	return nil
}
