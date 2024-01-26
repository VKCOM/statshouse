//go:build !linux

package stats

func (c *DMesgStats) WriteMetrics(nowUnix int64) error {
	_ = c.pushMetric
	_ = c.pushStat
	_ = c.cache
	return nil
}
