package stats

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/procfs"

	"github.com/vkcom/statshouse/internal/format"
)

type NetStats struct {
	fs procfs.FS

	oldNetDevTotal    *procfs.NetDevLine
	oldNetDevByDevice map[string]*procfs.NetDevLine

	oldNetStat netStat

	writer MetricWriter
}

type devStat struct {
	RxBytes   float64
	RxPackets float64
	RxErrors  float64
	RxDropped float64

	TxBytes   float64
	TxPackets float64
	TxErrors  float64
	TxDropped float64
}

type netStat struct {
	ip   ip
	tcp  tcp
	udp  udp
	icmp icmp
}

type ip struct {
	scrapeResult
	Forwarding      *float64
	DefaultTTL      *float64
	InReceives      float64
	InHdrErrors     float64
	InAddrErrors    float64
	ForwDatagrams   float64
	InUnknownProtos float64
	InDiscards      float64
	InDelivers      *float64
	OutRequests     float64
	OutDiscards     float64
	OutNoRoutes     float64
	ReasmTimeout    *float64
	ReasmReqds      *float64
	ReasmOKs        *float64
	ReasmFails      *float64
	FragOKs         *float64
	FragFails       *float64
	FragCreates     *float64
}

type tcp struct {
	scrapeResult
	RtoAlgorithm *float64
	RtoMin       *float64
	RtoMax       *float64
	MaxConn      *float64
	ActiveOpens  *float64
	PassiveOpens *float64
	AttemptFails *float64
	EstabResets  *float64
	CurrEstab    *float64
	InSegs       float64
	OutSegs      float64
	RetransSegs  float64
	InErrs       float64
	OutRsts      *float64
	InCsumErrors float64
}

type udp struct {
	scrapeResult
	InDatagrams  float64
	NoPorts      float64
	InErrors     float64
	OutDatagrams float64
	RcvbufErrors float64
	SndbufErrors float64
	InCsumErrors float64
	IgnoredMulti float64
}

type icmp struct {
	scrapeResult
	InMsgs           float64
	InErrors         *float64
	InCsumErrors     *float64
	InDestUnreachs   *float64
	InTimeExcds      *float64
	InParmProbs      *float64
	InSrcQuenchs     *float64
	InRedirects      *float64
	InEchos          *float64
	InEchoReps       *float64
	InTimestamps     *float64
	InTimestampReps  *float64
	InAddrMasks      *float64
	InAddrMaskReps   *float64
	OutMsgs          float64
	OutErrors        *float64
	OutDestUnreachs  *float64
	OutTimeExcds     *float64
	OutParmProbs     *float64
	OutSrcQuenchs    *float64
	OutRedirects     *float64
	OutEchos         *float64
	OutEchoReps      *float64
	OutTimestamps    *float64
	OutTimestampReps *float64
	OutAddrMasks     *float64
	OutAddrMaskReps  *float64
}

func (c *NetStats) Skip() bool {
	return false
}

func (*NetStats) Name() string {
	return "net_stats"
}

func (c *NetStats) PushDuration(now int64, d time.Duration) {
	c.writer.WriteSystemMetricValueWithoutHost(now, format.BuiltinMetricNameSystemMetricScrapeDuration, d.Seconds(), format.TagValueIDSystemMetricNet)
}

func NewNetStats(writer MetricWriter) (*NetStats, error) {
	fs, err := procfs.NewFS(procfs.DefaultMountPoint)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize procfs: %w", err)
	}
	return &NetStats{
		fs:                fs,
		writer:            writer,
		oldNetDevByDevice: map[string]*procfs.NetDevLine{},
	}, nil
}

func (c *NetStats) WriteMetrics(nowUnix int64) error {
	err := c.writeNetDev(nowUnix)
	if err != nil {
		log.Println("failed to write net/dev", err)
	}
	err = c.writeSNMP(nowUnix)
	if err != nil {
		log.Println("failed to write net/snmp", err)
	}

	return nil
}

func calcNetDev(old, new_ *procfs.NetDevLine) (stat *devStat) {
	if old == nil {
		return nil
	}
	rxBytes := float64(new_.RxBytes) - float64(old.RxBytes)
	rxPackets := float64(new_.RxPackets) - float64(old.RxPackets)
	rxErrors := float64(new_.RxErrors) - float64(old.RxErrors)
	rxDropped := float64(new_.RxDropped) - float64(old.RxDropped)

	txBytes := float64(new_.TxBytes) - float64(old.TxBytes)
	txPackets := float64(new_.TxPackets) - float64(old.TxPackets)
	txErrors := float64(new_.TxErrors) - float64(old.TxErrors)
	txDropped := float64(new_.TxDropped) - float64(old.TxDropped)

	return &devStat{
		RxBytes:   rxBytes,
		RxPackets: rxPackets,
		RxErrors:  rxErrors,
		RxDropped: rxDropped,
		TxBytes:   txBytes,
		TxPackets: txPackets,
		TxErrors:  txErrors,
		TxDropped: txDropped,
	}
}

func (c *NetStats) writeNetDev(nowUnix int64) error {
	dev, err := c.fs.NetDev()
	if err != nil {
		return err
	}
	delete(dev, "lo")
	new_ := dev.Total()
	statTotal := calcNetDev(c.oldNetDevTotal, &new_)
	if statTotal != nil {
		if statTotal.RxPackets > 0 {
			c.writer.WriteSystemMetricCountValue(nowUnix, format.BuiltinMetricNameNetBandwidth, statTotal.RxPackets, statTotal.RxBytes/statTotal.RxPackets, format.RawIDTagReceived)
		}
		if statTotal.TxPackets > 0 {
			c.writer.WriteSystemMetricCountValue(nowUnix, format.BuiltinMetricNameNetBandwidth, statTotal.TxPackets, statTotal.TxBytes/statTotal.TxPackets, format.RawIDTagSent)
		}
	}
	c.oldNetDevTotal = &new_

	for name, devStat := range dev {
		devStatC := devStat
		stat := calcNetDev(c.oldNetDevByDevice[name], &devStatC)
		if stat != nil {
			if stat.RxPackets > 0 {
				c.writer.WriteSystemMetricCountValueExtendedTag(nowUnix, format.BuiltinMetricNameNetDevBandwidth, stat.RxPackets, stat.RxBytes/stat.RxPackets,
					Tag{Raw: format.RawIDTagReceived},
					Tag{Str: name},
				)
			}
			if stat.TxPackets > 0 {
				c.writer.WriteSystemMetricCountValueExtendedTag(nowUnix, format.BuiltinMetricNameNetDevBandwidth, stat.TxPackets, stat.TxBytes/stat.TxPackets,
					Tag{Raw: format.RawIDTagSent},
					Tag{Str: name},
				)
			}
			c.writer.WriteSystemMetricCountExtendedTag(nowUnix, format.BuiltinMetricNameNetDevErrors, stat.RxErrors,
				Tag{Raw: format.RawIDTagReceived},
				Tag{Str: name})
			c.writer.WriteSystemMetricCountExtendedTag(nowUnix, format.BuiltinMetricNameNetDevErrors, stat.TxErrors,
				Tag{Raw: format.RawIDTagSent},
				Tag{Str: name})
			c.writer.WriteSystemMetricCountExtendedTag(nowUnix, format.BuiltinMetricNameNetDevDropped, stat.RxDropped,
				Tag{Raw: format.RawIDTagReceived},
				Tag{Str: name})
			c.writer.WriteSystemMetricCountExtendedTag(nowUnix, format.BuiltinMetricNameNetDevDropped, stat.TxDropped,
				Tag{Raw: format.RawIDTagSent},
				Tag{Str: name})
		}
		c.oldNetDevByDevice[name] = &devStatC
	}
	return nil
}

func (c *NetStats) writeSNMP(nowUnix int64) error {
	f, err := os.Open("/proc/net/snmp")
	if err != nil {
		return err
	}
	defer f.Close()
	netstat, err := parseNetstat(f)
	if err != nil {
		return err
	}
	c.writeIP(nowUnix, netstat)
	c.writeTCP(nowUnix, netstat)
	c.writeUDP(nowUnix, netstat)
	c.writePackets(nowUnix, netstat)
	c.oldNetStat = netstat
	return nil
}

func (c *NetStats) writePackets(nowUnix int64, stat netStat) {
	tcpR := stat.tcp.InSegs - c.oldNetStat.tcp.InSegs
	tcpO := stat.tcp.OutSegs - c.oldNetStat.tcp.OutSegs

	ipR := stat.ip.InReceives - c.oldNetStat.ip.InReceives
	ipO := stat.ip.OutRequests - c.oldNetStat.ip.OutRequests

	udpR := stat.udp.InDatagrams - c.oldNetStat.udp.InDatagrams
	udpO := stat.udp.OutDatagrams - c.oldNetStat.udp.OutDatagrams

	icmpR := stat.icmp.InMsgs - c.oldNetStat.icmp.InMsgs
	icmpO := stat.icmp.OutMsgs - c.oldNetStat.icmp.OutMsgs

	if c.oldNetStat.tcp.isSuccess {
		c.writer.WriteSystemMetricCount(nowUnix, format.BuiltinMetricNameNetPacket, tcpR, format.RawIDTagReceived, format.RawIDTagTCP)
		c.writer.WriteSystemMetricCount(nowUnix, format.BuiltinMetricNameNetPacket, tcpO, format.RawIDTagSent, format.RawIDTagTCP)
	}
	if c.oldNetStat.udp.isSuccess {
		c.writer.WriteSystemMetricCount(nowUnix, format.BuiltinMetricNameNetPacket, udpR, format.RawIDTagReceived, format.RawIDTagUDP)
		c.writer.WriteSystemMetricCount(nowUnix, format.BuiltinMetricNameNetPacket, udpO, format.RawIDTagSent, format.RawIDTagUDP)
	}
	if c.oldNetStat.icmp.isSuccess {
		c.writer.WriteSystemMetricCount(nowUnix, format.BuiltinMetricNameNetPacket, icmpR, format.RawIDTagReceived, format.RawIDTagICMP)
		c.writer.WriteSystemMetricCount(nowUnix, format.BuiltinMetricNameNetPacket, icmpO, format.RawIDTagSent, format.RawIDTagICMP)
	}
	if c.oldNetStat.ip.isSuccess {
		c.writer.WriteSystemMetricCount(nowUnix, format.BuiltinMetricNameNetPacket, ipR-tcpR-udpR-icmpR, format.RawIDTagReceived, format.RawIDTagOther)
		c.writer.WriteSystemMetricCount(nowUnix, format.BuiltinMetricNameNetPacket, ipO-tcpO-udpO-icmpO, format.RawIDTagSent, format.RawIDTagOther)
	}
}

func (c *NetStats) writeIP(nowUnix int64, stat netStat) {
	if !c.oldNetStat.ip.isSuccess {
		return
	}

	inHdrErrs := stat.ip.InHdrErrors - c.oldNetStat.ip.InHdrErrors
	c.writer.WriteSystemMetricCount(nowUnix, format.BuiltinMetricNameNetError, inHdrErrs, format.RawIDTagInHdrError, format.RawIDTagIP)

	inDiscards := stat.ip.InDiscards - c.oldNetStat.ip.InDiscards
	c.writer.WriteSystemMetricCount(nowUnix, format.BuiltinMetricNameNetError, inDiscards, format.RawIDTagInDiscard, format.RawIDTagIP)

	outDiscard := stat.ip.OutDiscards - c.oldNetStat.ip.OutDiscards
	c.writer.WriteSystemMetricCount(nowUnix, format.BuiltinMetricNameNetError, outDiscard, format.RawIDTagOutDiscard, format.RawIDTagIP)

	outNoRoutes := stat.ip.OutNoRoutes - c.oldNetStat.ip.OutNoRoutes
	c.writer.WriteSystemMetricCount(nowUnix, format.BuiltinMetricNameNetError, outNoRoutes, format.RawIDTagOutNoRoute, format.RawIDTagIP)

	inAddrErrors := stat.ip.InAddrErrors - c.oldNetStat.ip.InAddrErrors
	c.writer.WriteSystemMetricCount(nowUnix, format.BuiltinMetricNameNetError, inAddrErrors, format.RawIDTagInAddrError, format.RawIDTagIP)

	inUnknownProtos := stat.ip.InUnknownProtos - c.oldNetStat.ip.InUnknownProtos
	c.writer.WriteSystemMetricCount(nowUnix, format.BuiltinMetricNameNetError, inUnknownProtos, format.RawIDTagInUnknownProto, format.RawIDTagIP)
}

func (c *NetStats) writeTCP(nowUnix int64, stat netStat) {
	if !c.oldNetStat.tcp.isSuccess {
		return
	}

	inErrs := stat.tcp.InErrs - c.oldNetStat.tcp.InErrs
	c.writer.WriteSystemMetricCount(nowUnix, format.BuiltinMetricNameNetError, inErrs, format.RawIDTagInErr, format.RawIDTagTCP)
	inCsumError := stat.tcp.InCsumErrors - c.oldNetStat.tcp.InCsumErrors
	c.writer.WriteSystemMetricCount(nowUnix, format.BuiltinMetricNameNetError, inCsumError, format.RawIDTagInCsumErr, format.RawIDTagTCP)
	retransSegs := stat.tcp.RetransSegs - c.oldNetStat.tcp.RetransSegs
	c.writer.WriteSystemMetricCount(nowUnix, format.BuiltinMetricNameNetError, retransSegs, format.RawIDTagRetransSeg, format.RawIDTagTCP)
}

func (c *NetStats) writeUDP(nowUnix int64, stat netStat) {
	if !c.oldNetStat.udp.isSuccess {
		return
	}
	inErrs := stat.udp.InErrors - c.oldNetStat.udp.InErrors
	c.writer.WriteSystemMetricCount(nowUnix, format.BuiltinMetricNameNetError, inErrs, format.RawIDTagInErrors, format.RawIDTagUDP)
	inCsumError := stat.udp.InCsumErrors - c.oldNetStat.udp.InCsumErrors
	c.writer.WriteSystemMetricCount(nowUnix, format.BuiltinMetricNameNetError, inCsumError, format.RawIDTagInCsumErrors, format.RawIDTagUDP)
	rcvbufErrors := stat.udp.RcvbufErrors - c.oldNetStat.udp.RcvbufErrors
	c.writer.WriteSystemMetricCount(nowUnix, format.BuiltinMetricNameNetError, rcvbufErrors, format.RawIDTagRcvbufErrors, format.RawIDTagUDP)
	sndbufErrors := stat.udp.SndbufErrors - c.oldNetStat.udp.SndbufErrors
	c.writer.WriteSystemMetricCount(nowUnix, format.BuiltinMetricNameNetError, sndbufErrors, format.RawIDTagSndbufErrors, format.RawIDTagUDP)
	noPorts := stat.udp.NoPorts - c.oldNetStat.udp.NoPorts
	c.writer.WriteSystemMetricCount(nowUnix, format.BuiltinMetricNameNetError, noPorts, format.RawIDTagNoPorts, format.RawIDTagUDP)
}

func parseNetstat(reader io.Reader) (netStat, error) {
	result := netStat{}
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		names := strings.Split(scanner.Text(), " ")
		scanner.Scan()
		values := strings.Split(scanner.Text(), " ")
		protocol := strings.ToLower(strings.TrimSuffix(names[0], ":"))
		if len(names) != len(values) {
			continue
		}
		var err error
		names = names[1:]
		values = values[1:]
		switch protocol {
		case "ip":
			result.ip, err = parseIP(names, values)
		case "tcp":
			result.tcp, err = parseTCP(names, values)
		case "udp":
			result.udp, err = parseUDP(names, values)
		case "icmp":
			result.icmp, err = parseICMP(names, values)
		}

		if err != nil {
			log.Println("failed to parse: %w", err)
			continue
		}
	}
	return result, scanner.Err()
}

func parseIP(names, values []string) (ip, error) {
	ip := ip{}
	for i, name := range names {
		value, err := strconv.ParseFloat(values[i], 64)
		if err != nil {
			return ip, err
		}
		switch name {
		case "Forwarding":
			ip.Forwarding = &value
		case "DefaultTTL":
			ip.DefaultTTL = &value
		case "InReceives":
			ip.InReceives = value
		case "InHdrErrors":
			ip.InHdrErrors = value
		case "InAddrErrors":
			ip.InAddrErrors = value
		case "ForwDatagrams":
			ip.ForwDatagrams = value
		case "InUnknownProtos":
			ip.InUnknownProtos = value
		case "InDiscards":
			ip.InDiscards = value
		case "InDelivers":
			ip.InDelivers = &value
		case "OutRequests":
			ip.OutRequests = value
		case "OutDiscards":
			ip.OutDiscards = value
		case "OutNoRoutes":
			ip.OutNoRoutes = value
		case "ReasmTimeout":
			ip.ReasmTimeout = &value
		case "ReasmReqds":
			ip.ReasmReqds = &value
		case "ReasmOKs":
			ip.ReasmOKs = &value
		case "ReasmFails":
			ip.ReasmFails = &value
		case "FragOKs":
			ip.FragOKs = &value
		case "FragFails":
			ip.FragFails = &value
		case "FragCreates":
			ip.FragCreates = &value
		}
	}
	ip.isSuccess = true
	return ip, nil
}

func parseTCP(names, values []string) (tcp, error) {
	tcp := tcp{}
	for i, name := range names {
		value, err := strconv.ParseFloat(values[i], 64)
		if err != nil {
			return tcp, err
		}
		switch name {
		case "RtoAlgorithm":
			tcp.RtoAlgorithm = &value
		case "RtoMin":
			tcp.RtoMin = &value
		case "RtoMax":
			tcp.RtoMax = &value
		case "MaxConn":
			tcp.MaxConn = &value
		case "ActiveOpens":
			tcp.ActiveOpens = &value
		case "PassiveOpens":
			tcp.PassiveOpens = &value
		case "AttemptFails":
			tcp.AttemptFails = &value
		case "EstabResets":
			tcp.EstabResets = &value
		case "CurrEstab":
			tcp.CurrEstab = &value
		case "InSegs":
			tcp.InSegs = value
		case "OutSegs":
			tcp.OutSegs = value
		case "RetransSegs":
			tcp.RetransSegs = value
		case "InErrs":
			tcp.InErrs = value
		case "OutRsts":
			tcp.OutRsts = &value
		case "InCsumErrors":
			tcp.InCsumErrors = value
		}
	}
	tcp.isSuccess = true
	return tcp, nil
}

func parseUDP(names, values []string) (udp, error) {
	udp := udp{}
	for i, name := range names {
		value, err := strconv.ParseFloat(values[i], 64)
		if err != nil {
			return udp, err
		}
		switch name {
		case "InDatagrams":
			udp.InDatagrams = value
		case "NoPorts":
			udp.NoPorts = value
		case "InErrors":
			udp.InErrors = value
		case "OutDatagrams":
			udp.OutDatagrams = value
		case "RcvbufErrors":
			udp.RcvbufErrors = value
		case "SndbufErrors":
			udp.SndbufErrors = value
		case "InCsumErrors":
			udp.InCsumErrors = value
		case "IgnoredMulti":
			udp.IgnoredMulti = value
		}
	}
	udp.isSuccess = true
	return udp, nil
}

func parseICMP(names, values []string) (icmp, error) {
	icmp := icmp{}
	for i, name := range names {
		value, err := strconv.ParseFloat(values[i], 64)
		if err != nil {
			return icmp, err
		}
		switch name {
		case "InMsgs":
			icmp.InMsgs = value
		case "InErrors":
			icmp.InErrors = &value
		case "InCsumErrors":
			icmp.InCsumErrors = &value
		case "InDestUnreachs":
			icmp.InDestUnreachs = &value
		case "InTimeExcds":
			icmp.InTimeExcds = &value
		case "InParmProbs":
			icmp.InParmProbs = &value
		case "InSrcQuenchs":
			icmp.InSrcQuenchs = &value
		case "InRedirects":
			icmp.InRedirects = &value
		case "InEchos":
			icmp.InEchos = &value
		case "InEchoReps":
			icmp.InEchoReps = &value
		case "InTimestamps":
			icmp.InTimestamps = &value
		case "InTimestampReps":
			icmp.InTimestampReps = &value
		case "InAddrMasks":
			icmp.InAddrMasks = &value
		case "InAddrMaskReps":
			icmp.InAddrMaskReps = &value
		case "OutMsgs":
			icmp.OutMsgs = value
		case "OutErrors":
			icmp.OutErrors = &value
		case "OutDestUnreachs":
			icmp.OutDestUnreachs = &value
		case "OutTimeExcds":
			icmp.OutTimeExcds = &value
		case "OutParmProbs":
			icmp.OutParmProbs = &value
		case "OutSrcQuenchs":
			icmp.OutSrcQuenchs = &value
		case "OutRedirects":
			icmp.OutRedirects = &value
		case "OutEchos":
			icmp.OutEchos = &value
		case "OutEchoReps":
			icmp.OutEchoReps = &value
		case "OutTimestamps":
			icmp.OutTimestamps = &value
		case "OutTimestampReps":
			icmp.OutTimestampReps = &value
		case "OutAddrMasks":
			icmp.OutAddrMasks = &value
		case "OutAddrMaskReps":
			icmp.OutAddrMaskReps = &value
		}
	}
	icmp.isSuccess = true
	return icmp, nil
}
