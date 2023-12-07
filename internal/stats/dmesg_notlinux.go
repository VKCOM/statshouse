//go:build !linux

package stats

func (c *DMesgStats) WriteMetrics(nowUnix int64) error {
	_ = c.pushStat
	return nil
}
