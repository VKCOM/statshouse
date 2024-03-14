package stats

import (
	"time"

	"github.com/vkcom/statshouse/internal/format"
)

const (
	SYSLOG_ACTION_READ_ALL    = 3
	SYSLOG_ACTION_SIZE_BUFFER = 10

	LOG_FACMASK = 0x03f8
	LOG_PRIMASK = 0x07
)

var (
	oomMsgPrefixBytes       = []byte("Killed process")
	oomCGroupMsgPrefixBytes = []byte("Memory cgroup out of memory: Killed process")
	klogParser              parserComb[either[klogMsg, []byte]]
)

type (
	DMesgStats struct {
		cache    []byte
		lastTS   timestamp
		pushStat bool
		writer   MetricWriter
	}

	timestamp struct {
		sec  int64
		usec int64
	}

	fascility struct {
		lev int32
		fac int32
	}

	klogMsg struct {
		level    int32
		facility int32
		ts       timestamp

		oom oom
	}

	oom struct {
		oomProcessName string
	}
)

func init() {
	facilityParser := map_(
		left(
			right(char('<'), number()),
			char('>')),
		func(a int64) fascility {
			lev, fac := extractLevFac(int32(a))
			return fascility{lev: lev, fac: fac}
		})
	secParser := left(right(char('['), blankedNumberParser()), char('.'))
	tsParser := map_(combine(secParser, left(number(), char(']'))), func(cmb comb[int64, int64]) timestamp {
		return timestamp{sec: cmb.a, usec: cmb.b}
	})
	prefixParser := left(combine(facilityParser, tsParser), char(' '))

	msgParser := combine(prefixParser, klogTailParser())
	klogMsgParser := map_(msgParser, func(a comb[comb[fascility, timestamp], either[oom, []byte]]) klogMsg {
		return klogMsg{
			level:    a.a.a.lev,
			facility: a.a.a.fac,
			ts:       timestamp{a.a.b.sec, a.a.b.usec},
			oom:      a.b.left,
		}
	})
	klogParser = or(klogMsgParser, toTheEndOfLine())
}

func (c *DMesgStats) Skip() bool {
	return false
}

func (*DMesgStats) Name() string {
	return "dmesg_stats"
}

func (c *DMesgStats) PushDuration(now int64, d time.Duration) {
	c.writer.WriteSystemMetricValueWithoutHost(now, format.BuiltinMetricNameSystemMetricScrapeDuration, d.Seconds(), format.TagValueIDSystemMetricDMesgStat)
}

func NewDMesgStats(writer MetricWriter) (*DMesgStats, error) {
	return &DMesgStats{writer: writer, lastTS: timestamp{
		sec:  -1,
		usec: 0,
	}}, nil
}

func log_fac(p int32) int32 {
	return ((p) & LOG_FACMASK) >> 3
}

func log_pri(p int32) int32 {
	return (p) & LOG_PRIMASK
}

func leOrEq(a, b timestamp) bool {
	return a.sec < b.sec || (a.sec == b.sec && a.usec <= b.usec)
}

func (c *DMesgStats) pushMetric(nowUnix int64, klog klogMsg) {
	// don't use event ts to avoid historic conveyor
	c.writer.WriteSystemMetricCount(nowUnix, format.BuiltinMetricNameDMesgEvents, 1, klog.facility, klog.level)
	if klog.oom.oomProcessName != "" {
		c.writer.WriteSystemMetricCountExtendedTag(nowUnix, format.BuiltinMetricNameOOMKillDetailed, 1, Tag{
			Str: klog.oom.oomProcessName,
		})
	}
}

func (c *DMesgStats) handleMsgs(nowUnix int64, klog []byte, pushStat bool, pushMsg func(nowUnix int64, klog klogMsg)) error {
	lastTS := c.lastTS
	for len(klog) > 0 {
		msgE, tail, err := klogParser(klog)
		klog = tail
		if err != nil {
			return err
		}

		if !msgE.isLeft {
			continue
		}
		msg := msgE.left
		if leOrEq(msg.ts, lastTS) {
			continue
		}
		if pushStat {
			pushMsg(nowUnix, msg)
		}
		c.lastTS = msg.ts
	}
	return nil

}

func klogTailParser() parserComb[either[oom, []byte]] {
	return or(oomParser(), toTheEndOfLine())
}

func blankedNumberParser() parserComb[int64] {
	return right(satisfy(func(b byte) bool {
		return b == ' '
	}), number())
}

func oomParser() parserComb[oom] {
	// <4>[12555058.445681] Killed process 3029 (Web Content)
	oomPrefParser := anyOf(prefix(oomCGroupMsgPrefixBytes), prefix(oomMsgPrefixBytes))
	// oomPrefParser *> char(' ') *> number() *> char(' ') *> char('(') *> satisfy c => c != ')' <* char(')')
	oomedAppNameParser := left(right(right(right(right(right(oomPrefParser, char(' ')), number()), char(' ')), char('(')), satisfy(func(b byte) bool {
		return b != ')'
	})), char(')'))
	lineParser := left(oomedAppNameParser, toTheEndOfLine())
	return map_(lineParser, func(name []byte) oom {
		return oom{oomProcessName: string(name)}
	})
}

func extractLevFac(levFac int32) (lev int32, fac int32) {
	return log_pri(levFac), log_fac(levFac)
}

/*
To install:
go install github.com/dvyukov/go-fuzz/go-fuzz@latest github.com/dvyukov/go-fuzz/go-fuzz-build@latest

To run:
go-fuzz-build
go-fuzz
*/
func Fuzz(data []byte) int {
	c := &DMesgStats{}
	err := c.handleMsgs(0, data, false, nil)
	if err == nil {
		return 1
	}
	return 0
}
