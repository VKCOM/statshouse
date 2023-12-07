//go:build linux

package stats

import (
	"fmt"
	"syscall"

	"github.com/vkcom/statshouse/internal/format"
)

func (c *DMesgStats) WriteMetrics(nowUnix int64) error {
	n, err := syscall.Klogctl(SYSLOG_ACTION_SIZE_BUFFER, nil)
	if err != nil {
		return fmt.Errorf("failed to  %w", err)
	}
	b := make([]byte, n)
	k, err := syscall.Klogctl(SYSLOG_ACTION_READ_ALL, b)
	b = b[:k]
	err = c.handleMsgs(nowUnix, b, c.pushStat)
	c.pushStat = true
	if err != nil {
		c.writer.WriteSystemMetricValueWithoutHost(nowUnix, format.BuiltinMetricNameStatsHouseErrors, 0, format.TagValueIDDMESGParseError)
		return errStopCollector
	}
	return nil
}
