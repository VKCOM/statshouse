package stats

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/prometheus/procfs"
)

type NetStats struct {
	fs procfs.FS

	oldNetDev      procfs.NetDev
	oldNetDevTotal procfs.NetDevLine

	oldNetStat netStat

	pusher Pusher
}

const (
	bandwidth        = "test_net_bandwidth"
	ipPackets        = "test_net_ip_packet"
	ipPacketsErrors  = "test_net_ip_packet_errors"
	tcpPackets       = "test_net_tcp_packets"
	tcpPacketsErrors = "test_net_tcp_packets_errors"
	udpPackets       = "test_net_udp_datagrams"
)

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
	InDatagrams  *float64
	NoPorts      *float64
	InErrors     *float64
	OutDatagrams *float64
	RcvbufErrors *float64
	SndbufErrors *float64
	InCsumErrors *float64
	IgnoredMulti *float64
}

type icmp struct {
	scrapeResult
	InMsgs           *float64
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
	OutMsgs          *float64
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

func (*NetStats) Name() string {
	return "net_stats"
}

func NewNetStats(pusher Pusher) (*NetStats, error) {
	fs, err := procfs.NewFS(procfs.DefaultMountPoint)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize procfs: %w", err)
	}
	return &NetStats{
		fs:     fs,
		pusher: pusher,
	}, nil
}

func (c *NetStats) PushMetrics() error {
	err := c.pushNetDev()
	if err != nil {
		log.Println("failed to push net/dev", err)
	}
	err = c.pushSNMP()
	if err != nil {
		log.Println("failed to push net/snmp", err)
	}

	return nil
}

func (c *NetStats) pushNetDev() error {
	dev, err := c.fs.NetDev()
	if err != nil {
		return err
	}
	delete(dev, "lo")
	total := dev.Total()

	if len(c.oldNetDev) > 0 {
		c.pusher.PushSystemMetricValue(bandwidth, float64(total.RxBytes-c.oldNetDevTotal.RxBytes), "received")
		c.pusher.PushSystemMetricValue(bandwidth, float64(total.TxBytes-c.oldNetDevTotal.TxBytes), "sent")
	}

	c.oldNetDev = dev
	c.oldNetDevTotal = total
	return nil
}

func (c *NetStats) pushSNMP() error {
	f, err := os.Open("/proc/net/snmp")
	if err != nil {
		return err
	}
	defer f.Close()
	netstat, err := parseNetstat(f)
	if err != nil {
		return err
	}
	c.pushIP(netstat)
	c.pushTCP(netstat)
	c.pushUDP(netstat)
	c.oldNetStat = netstat
	return nil
}

func (c *NetStats) pushIP(stat netStat) {
	if !c.oldNetStat.ip.isSuccess {
		return
	}
	received := stat.ip.InReceives - c.oldNetStat.ip.InReceives
	c.pusher.PushSystemMetricCount(ipPackets, received, "received")
	sent := stat.ip.OutRequests - c.oldNetStat.ip.OutRequests
	c.pusher.PushSystemMetricCount(ipPackets, sent, "sent")
	/*
		if stat.ip.InDelivers != nil {
			delivers := *stat.ip.InDelivers - *c.oldNetStat.ip.InDelivers
			c.pusher.PushSystemMetricValue(ipPackets, delivers, "delivers")
		}
	*/
	forwarded := stat.ip.ForwDatagrams - c.oldNetStat.ip.ForwDatagrams
	c.pusher.PushSystemMetricCount(ipPackets, forwarded, "forwarded")

	inHdrErrs := stat.ip.InHdrErrors - c.oldNetStat.ip.InHdrErrors
	c.pusher.PushSystemMetricCount(ipPacketsErrors, inHdrErrs, "InHdrError")

	inDiscards := stat.ip.InDiscards - c.oldNetStat.ip.InDiscards
	c.pusher.PushSystemMetricCount(ipPacketsErrors, inDiscards, "InDiscard")

	outDiscard := stat.ip.OutDiscards - c.oldNetStat.ip.OutDiscards
	c.pusher.PushSystemMetricCount(ipPacketsErrors, outDiscard, "OutDiscards")

	outNoRoutes := stat.ip.OutNoRoutes - c.oldNetStat.ip.OutNoRoutes
	c.pusher.PushSystemMetricCount(ipPacketsErrors, outNoRoutes, "OutNoRoute")

	inAddrErrors := stat.ip.InAddrErrors - c.oldNetStat.ip.InAddrErrors
	c.pusher.PushSystemMetricCount(ipPacketsErrors, inAddrErrors, "InAddrError")

	inUnknownProtos := stat.ip.InUnknownProtos - c.oldNetStat.ip.InUnknownProtos
	c.pusher.PushSystemMetricCount(ipPacketsErrors, inUnknownProtos, "InUnknownProto")
}

func (c *NetStats) pushTCP(stat netStat) {
	if !c.oldNetStat.tcp.isSuccess {
		return
	}

	received := stat.tcp.InSegs - c.oldNetStat.tcp.InSegs
	c.pusher.PushSystemMetricCount(tcpPackets, received, "received")
	sent := stat.tcp.OutSegs - c.oldNetStat.tcp.OutSegs
	c.pusher.PushSystemMetricCount(tcpPackets, sent, "sent")

	inErrs := stat.tcp.InErrs - c.oldNetStat.tcp.InErrs
	c.pusher.PushSystemMetricCount(tcpPacketsErrors, inErrs, "InErr")
	inCsumError := stat.tcp.InCsumErrors - c.oldNetStat.tcp.InCsumErrors
	c.pusher.PushSystemMetricCount(tcpPacketsErrors, inCsumError, "InCsumError")
	retransSegs := stat.tcp.RetransSegs - c.oldNetStat.tcp.RetransSegs
	c.pusher.PushSystemMetricCount(tcpPacketsErrors, retransSegs, "RetransSeg")
}

func (c *NetStats) pushUDP(stat netStat) {
	if !c.oldNetStat.udp.isSuccess {
		return
	}

	if stat.udp.InDatagrams != nil {
		received := *stat.udp.InDatagrams - *c.oldNetStat.udp.InDatagrams
		c.pusher.PushSystemMetricCount(udpPackets, received, "received")
	}
	if stat.udp.OutDatagrams != nil {
		sent := *stat.udp.OutDatagrams - *c.oldNetStat.udp.OutDatagrams
		c.pusher.PushSystemMetricCount(udpPackets, sent, "sent")
	}
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
			//todo log
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
			//todo log
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
			udp.InDatagrams = &value
		case "NoPorts":
			udp.NoPorts = &value
		case "InErrors":
			udp.InErrors = &value
		case "OutDatagrams":
			udp.OutDatagrams = &value
		case "RcvbufErrors":
			udp.RcvbufErrors = &value
		case "SndbufErrors":
			udp.SndbufErrors = &value
		case "InCsumErrors":
			udp.InCsumErrors = &value
		case "IgnoredMulti":
			udp.IgnoredMulti = &value
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
			icmp.InMsgs = &value
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
			icmp.OutMsgs = &value
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
