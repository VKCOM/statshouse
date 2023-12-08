package stats

import (
	"bytes"
	"fmt"
	"io"
	"time"

	"github.com/vkcom/statshouse/internal/format"
	"go4.org/mem"
	"golang.org/x/exp/slices"
)

const (
	SYSLOG_ACTION_READ_ALL    = 3
	SYSLOG_ACTION_SIZE_BUFFER = 10

	LOG_FACMASK = 0x03f8
	LOG_PRIMASK = 0x07
)

var (
	oomMsgPrefixBytes = []byte("Killed process")
	numbers           = []byte{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9'}
)

type (
	DMesgStats struct {
		cache      []byte
		lastTS     timestamp
		pushStat   bool
		writer     MetricWriter
		klogParser *parser
		msgParser  *parser
	}
	parser struct {
		cache []byte
		body  []byte
		err   error
	}

	timestamp struct {
		sec  int64
		usec int64
	}

	klogMsg struct {
		level    int32
		facility int32
		ts       timestamp
		msgtype  string
		msg      string

		oomProcessName string
	}
)

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
	return &DMesgStats{writer: writer, klogParser: &parser{
		cache: []byte{},
	},
		msgParser: &parser{
			cache: []byte{},
		},
	}, nil
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
	if klog.oomProcessName != "" {
		c.writer.WriteSystemMetricCountExtendedTag(nowUnix, format.BuiltinMetricNameOOMKillDetailed, 1, Tag{
			Str: klog.oomProcessName,
		})
	}
}

func (c *DMesgStats) handleMsgs(nowUnix int64, klog []byte, pushStat bool, pushMsg func(nowUnix int64, klog klogMsg)) error {
	c.klogParser.body = klog
	p := c.klogParser
	lastTS := c.lastTS
	for p.hasTail() {
		p.mustChar('<')
		if p.err != nil {
			return p.err
		}
		levFac := p.parseNumber()
		if p.err != nil {
			return p.err
		}
		lev, fac := extractLevFac(int32(levFac))
		p.mustChar('>')
		if p.err != nil {
			return p.err
		}
		p.mustChar('[')
		if p.err != nil {
			return p.err
		}
		sec := p.parseNumber()
		p.mustChar('.')
		if p.err != nil {
			return p.err
		}
		usec := p.parseNumber()
		if p.err != nil {
			return p.err
		}
		p.mustChar(']')
		if p.err != nil {
			return p.err
		}
		p.mustChar(' ')
		if p.err != nil {
			return p.err
		}
		msg := p.parseUntil('\n')
		if p.err != nil {
			return p.err
		}
		klog := klogMsg{
			level:    lev,
			facility: fac,
			ts:       timestamp{sec: sec, usec: usec},
			msgtype:  "",
			msg:      "",
		}
		if leOrEq(klog.ts, lastTS) {
			continue
		}
		err := c.fillMsg(msg, &klog)
		if err != nil {
			return err
		}
		if pushStat {
			pushMsg(nowUnix, klog)
		}
		c.lastTS = klog.ts
	}
	return nil
}

func (c *DMesgStats) fillMsg(data []byte, klog *klogMsg) error {
	switch {
	case bytes.HasPrefix(data, oomMsgPrefixBytes):
		return c.handleOOM(data, klog)
	}
	return nil
}

func (c *DMesgStats) handleOOM(data []byte, klog *klogMsg) error {
	c.msgParser.body = data
	p := c.msgParser
	p.mustSequence(oomMsgPrefixBytes)
	if p.err != nil {
		return p.err
	}
	p.mustChar(' ')
	if p.err != nil {
		return p.err
	}
	_ = p.parseNumber()
	if p.err != nil {
		return p.err
	}
	p.mustChar(' ')
	if p.err != nil {
		return p.err
	}
	p.mustChar('(')
	if p.err != nil {
		return p.err
	}
	processName := p.parseUntil(')')
	if p.err != nil {
		return p.err
	}
	klog.oomProcessName = string(processName)
	return nil
}

func extractLevFac(levFac int32) (lev int32, fac int32) {
	return log_pri(levFac), log_fac(levFac)
}

func (p *parser) hasTail() bool {
	return p.err == nil && len(p.body) > 0
}

func (p *parser) skipN(n int) {
	p.body = p.body[n:]

}

func (p *parser) mustChar(ch byte) {
	if len(p.body) == 0 {
		p.err = fmt.Errorf("expect %s but empty body", string(rune(ch)))
		return
	}
	if p.body[0] == ch {
		p.skipN(1)
		return
	}
	p.err = fmt.Errorf("expect %s", string(rune(ch)))
}

func (p *parser) mustSequence(bs []byte) {
	if bytes.HasPrefix(p.body, bs) {
		p.body = p.body[len(bs):]
		return
	}
	p.err = fmt.Errorf("expect %s, got %s", string(bs), string(p.body))
}
func (p *parser) mustChars(bs []byte) []byte {
	cache := p.cache[:0]
	for {
		b, err := p.checkChar()
		if err != nil {
			break
		}
		if slices.Contains(bs, b) {
			p.skipN(1)
		} else {
			break
		}
		cache = append(cache, b)
	}
	if len(cache) == 0 {
		p.err = fmt.Errorf("mustChars error: %s", bs)
	}
	return cache
}

func (p *parser) checkChar() (byte, error) {
	if !p.hasTail() {
		return 0, io.ErrUnexpectedEOF
	}
	b := p.body[0]
	return b, nil
}

func (p *parser) parseNumber() int64 {
	seq := p.mustChars(numbers)
	n, err := mem.ParseInt(mem.B(seq), 10, 64)
	if err != nil {
		p.err = err
		return 0
	}
	return n
}

func (p *parser) parseUntil(b byte) []byte {
	i := mem.IndexByte(mem.B(p.body), b)
	if i == -1 {
		p.err = fmt.Errorf("parseUntil error %s", string(rune(b)))
		return nil
	}
	p.cache = p.cache[:0]
	p.cache = append(p.cache, p.body[:i]...)
	p.skipN(i + 1)
	return p.cache
}

/*
To run:
go-fuzz-build
go-fuzz
*/
func Fuzz(data []byte) int {
	c := &DMesgStats{klogParser: &parser{}, msgParser: &parser{}}
	err := c.handleMsgs(0, data, false, nil)
	if err == nil {
		return 1
	}
	return 0
}
