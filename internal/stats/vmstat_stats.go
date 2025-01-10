package stats

import (
	"bufio"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/vkcom/statshouse/internal/format"
)

type VMStatStats struct {
	old    map[string]float64
	writer MetricWriter
}

const (
	oom_kill = "oom_kill"

	numa_hit               = "numa_hit"
	numa_miss              = "numa_miss"
	numa_foreign           = "numa_foreign"
	numa_interleave        = "numa_interleave"
	numa_local             = "numa_local"
	numa_other             = "numa_other"
	numa_pte_updates       = "numa_pte_updates"
	numa_huge_pte_updates  = "numa_huge_pte_updates"
	numa_hint_faults       = "numa_hint_faults"
	numa_hint_faults_local = "numa_hint_faults_local"
	numa_pages_migrated    = "numa_pages_migrated"

	pgpgin  = "pgpgin"
	pgpgout = "pgpgout"

	pgfault    = "pgfault"
	pgmajfault = "pgmajfault"
)

var parseKeys = map[string]struct{}{
	oom_kill:               {},
	numa_hit:               {},
	numa_miss:              {},
	numa_foreign:           {},
	numa_interleave:        {},
	numa_local:             {},
	numa_other:             {},
	pgpgin:                 {},
	pgpgout:                {},
	pgfault:                {},
	pgmajfault:             {},
	numa_pte_updates:       {},
	numa_huge_pte_updates:  {},
	numa_hint_faults:       {},
	numa_hint_faults_local: {},
	numa_pages_migrated:    {},
}

func (c *VMStatStats) Skip() bool {
	return false
}

func (*VMStatStats) Name() string {
	return "vmstat_stats"
}

func (c *VMStatStats) PushDuration(now int64, d time.Duration) {
	c.writer.WriteSystemMetricValueWithoutHost(now, format.BuiltinMetricMetaSystemMetricScrapeDuration.Name, d.Seconds(), format.TagValueIDSystemMetricVMStat)
}

func NewVMStats(writer MetricWriter) (*VMStatStats, error) {
	return &VMStatStats{
		writer: writer,
		old:    map[string]float64{},
	}, nil
}

func (c *VMStatStats) WriteMetrics(nowUnix int64) error {
	m, err := parseVMStat()
	if err != nil {
		return err
	}
	for name, value := range m {
		oldValue, ok := c.old[name]
		if ok {
			c.pushMetric(nowUnix, name, value-oldValue, m)
		}
		c.old[name] = value
	}
	return nil
}

func (c *VMStatStats) pushMetric(nowUnix int64, name string, valueDiff float64, m map[string]float64) {
	const mult = 1024
	switch name {
	case oom_kill:
		c.writer.WriteSystemMetricCount(nowUnix, format.BuiltinMetricNameOOMKill, valueDiff)
	case pgfault:
		pgmajfaultDiff := m[pgmajfault] - c.old[pgmajfault]
		pgminfaultDiff := valueDiff - pgmajfaultDiff
		c.writer.WriteSystemMetricCount(nowUnix, format.BuiltinMetricNamePageFault, pgmajfaultDiff, format.RawIDTagMajor)
		c.writer.WriteSystemMetricCount(nowUnix, format.BuiltinMetricNamePageFault, pgminfaultDiff, format.RawIDTagMinor)
	case pgpgin:
		c.writer.WriteSystemMetricValue(nowUnix, format.BuiltinMetricNamePagedMemory, valueDiff*mult, format.RawIDTagIn)
	case pgpgout:
		c.writer.WriteSystemMetricValue(nowUnix, format.BuiltinMetricNamePagedMemory, valueDiff*mult, format.RawIDTagOut)
	case numa_foreign:
		c.writer.WriteSystemMetricCount(nowUnix, format.BuiltinMetricNameNumaEvents, valueDiff, format.RawIDTagForeign)
	case numa_interleave:
		c.writer.WriteSystemMetricCount(nowUnix, format.BuiltinMetricNameNumaEvents, valueDiff, format.RawIDTagInterleave)
	case numa_local:
		c.writer.WriteSystemMetricCount(nowUnix, format.BuiltinMetricNameNumaEvents, valueDiff, format.RawIDTagLocal)
	case numa_other:
		c.writer.WriteSystemMetricCount(nowUnix, format.BuiltinMetricNameNumaEvents, valueDiff, format.RawIDTagNumaOther)
	case numa_pte_updates:
		c.writer.WriteSystemMetricCount(nowUnix, format.BuiltinMetricNameNumaEvents, valueDiff, format.RawIDTagPteUpdates)
	case numa_huge_pte_updates:
		c.writer.WriteSystemMetricCount(nowUnix, format.BuiltinMetricNameNumaEvents, valueDiff, format.RawIDTagHugePteUpdates)
	case numa_hint_faults:
		c.writer.WriteSystemMetricCount(nowUnix, format.BuiltinMetricNameNumaEvents, valueDiff, format.RawIDTagHintFaults)
	case numa_hint_faults_local:
		c.writer.WriteSystemMetricCount(nowUnix, format.BuiltinMetricNameNumaEvents, valueDiff, format.RawIDTagHintFaultsLocal)
	case numa_pages_migrated:
		c.writer.WriteSystemMetricCount(nowUnix, format.BuiltinMetricNameNumaEvents, valueDiff, format.RawIDTagPagesMigrated)
	}
}

func parseVMStat() (map[string]float64, error) {
	file, err := os.Open("/proc/vmstat")
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	res := make(map[string]float64)
	for scanner.Scan() {
		parts := strings.Fields(scanner.Text())
		value, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			return nil, err
		}
		if _, ok := parseKeys[parts[0]]; !ok {
			continue
		}
		res[parts[0]] = value

	}
	return res, scanner.Err()
}
