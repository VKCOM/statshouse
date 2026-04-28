package balancer

import (
	"context"
	"encoding/binary"
	"errors"
	"math/rand"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VKCOM/statshouse/internal/data_model/gen2/tl"
	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"
)

const (
	defaultDNSRefreshInterval = time.Minute
	defaultDialTimeout        = 5 * time.Second
	defaultReconnectDelay     = time.Second
	tcpHandshakeMagic         = "statshousev1"
)

var errWouldBlock = errors.New("would block")

type EgressConfig struct {
	Network            string
	Address            string
	HostTag            string
	DNSRefreshInterval time.Duration
	DialTimeout        time.Duration
	ReconnectDelay     time.Duration
}

type EgressStats struct {
	ForwardedPackets uint64
	DroppedPackets   uint64
	WriteErrors      uint64
	ReconnectErrors  uint64
	DNSRefreshErrors uint64
}

type egressStatsAtomic struct {
	forwardedPackets atomic.Uint64
	droppedPackets   atomic.Uint64
	writeErrors      atomic.Uint64
	reconnectErrors  atomic.Uint64
	dnsRefreshErrors atomic.Uint64
}

type Egress struct {
	cfg   EgressConfig
	stats egressStatsAtomic

	pool      *tcpPool
	closeOnce sync.Once
}

type tcpPool struct {
	primary   *tcpSender
	secondary *tcpSender
	routeMu   sync.Mutex
	closed    chan struct{}
}

type tcpSender struct {
	cfg   EgressConfig
	stats *egressStatsAtomic

	wouldBlockBytes atomic.Int64

	poolMu sync.Mutex
	pool   addressPool
	q      chan []byte

	closeCh  chan struct{}
	closeWg  sync.WaitGroup
	closeErr chan error
}

type addressPool struct {
	addrs []string
	head  int
}

func NewEgress(cfg EgressConfig) *Egress {
	cfg.fillDefaults()
	e := &Egress{cfg: cfg}
	e.pool = &tcpPool{
		primary:   newTCPSender(cfg, &e.stats, addressPool{}),
		secondary: newTCPSender(cfg, &e.stats, addressPool{}),
		closed:    make(chan struct{}),
	}
	go e.pool.runDNSRefresh(cfg, &e.stats)
	time.Sleep(5 * time.Second) // time to connect
	return e
}

func (cfg *EgressConfig) fillDefaults() {
	if cfg.Network == "" {
		cfg.Network = "tcp"
	}
	if cfg.DNSRefreshInterval <= 0 {
		cfg.DNSRefreshInterval = defaultDNSRefreshInterval
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = defaultDialTimeout
	}
	if cfg.ReconnectDelay <= 0 {
		cfg.ReconnectDelay = defaultReconnectDelay
	}
}

func newTCPSender(cfg EgressConfig, stats *egressStatsAtomic, pool addressPool) *tcpSender {
	s := &tcpSender{
		cfg:      cfg,
		stats:    stats,
		pool:     pool,
		q:        make(chan []byte),
		closeCh:  make(chan struct{}),
		closeErr: make(chan error, 1),
	}
	s.closeWg.Add(1)
	go s.sendLoop()
	return s
}

func (e *Egress) WritePacket(pkt []byte) bool {
	if len(pkt) == 0 {
		return true
	}
	b := []byte{}
	for _, t := range pkt {
		b = append(b, t)
	}
	if err := e.pool.write(b); err != nil {
		if errors.Is(err, errWouldBlock) {
			e.stats.droppedPackets.Add(1)
		}
		return false
	}
	e.stats.forwardedPackets.Add(1)
	return true
}

func (e *Egress) Stats() EgressStats {
	return EgressStats{
		ForwardedPackets: e.stats.forwardedPackets.Load(),
		DroppedPackets:   e.stats.droppedPackets.Load(),
		WriteErrors:      e.stats.writeErrors.Load(),
		ReconnectErrors:  e.stats.reconnectErrors.Load(),
		DNSRefreshErrors: e.stats.dnsRefreshErrors.Load(),
	}
}

func (e *Egress) Close() error {
	var err error
	e.closeOnce.Do(func() {
		close(e.pool.closed)
		err = errors.Join(e.pool.primary.close(), e.pool.secondary.close())
	})
	return err
}

func (p *tcpPool) write(pkt []byte) error {
	select {
	case <-p.closed:
		return errWouldBlock
	default:
	}

	p.routeMu.Lock()
	defer p.routeMu.Unlock()

	if err := p.primary.write(pkt); err == nil {
		return nil
	} else if !errors.Is(err, errWouldBlock) {
		return err
	}
	if err := p.secondary.write(pkt); err == nil {
		p.primary, p.secondary = p.secondary, p.primary
		return nil
	} else if !errors.Is(err, errWouldBlock) {
		return err
	}
	p.primary.wouldBlockBytes.Add(int64(len(pkt)))
	return errWouldBlock
}

func (p *tcpPool) runDNSRefresh(cfg EgressConfig, stats *egressStatsAtomic) {
	refresh := func() {
		targets, err := resolveDialTargets(cfg.Network, cfg.Address)
		if err != nil {
			stats.dnsRefreshErrors.Add(1)
			return
		}
		primaryPool, secondaryPool := newAddressPools(targets)
		p.primary.replacePool(primaryPool)
		p.secondary.replacePool(secondaryPool)
	}
	refresh()

	t := time.NewTicker(cfg.DNSRefreshInterval)
	defer t.Stop()
	for {
		select {
		case <-p.closed:
			return
		case <-t.C:
			refresh()
		}
	}
}

func (s *tcpSender) write(pkt []byte) error {
	select {
	case <-s.closeCh:
		return errWouldBlock
	case s.q <- pkt:
		return nil
	default:
		return errWouldBlock
	}
}

func (s *tcpSender) close() error {
	close(s.closeCh)
	close(s.q)
	s.closeWg.Wait()
	return <-s.closeErr
}

func (s *tcpSender) sendLoop() {
	defer s.closeWg.Done()
	var conn net.Conn
	var err error
	var lastDial time.Time
	m := s.getWriteErrM()
	scratch := make([]byte, 102+len(s.cfg.HostTag))
loop:
	for {
		select {
		case <-s.closeCh:
			break loop
		default:
		}
		if conn == nil {
			time.Sleep(s.cfg.ReconnectDelay - time.Since(lastDial))
			lastDial = time.Now()
			conn, err = s.reconnect()
			if err != nil {
				s.stats.reconnectErrors.Add(1)
				continue
			}
		}
		pkt, ok := <-s.q
		if !ok {
			break
		}
		if _, err = conn.Write(pkt); err != nil {
			s.stats.writeErrors.Add(1)
			_ = conn.Close()
			conn = nil
			continue // not resend packet
		}
		s.reportWouldBlockIfAny(conn, m, scratch)
	}
	if conn != nil {
		err = conn.Close()
	}
	s.closeErr <- err
}

func (s *tcpSender) getWriteErrM() tlstatshouse.MetricBytes {
	var m tlstatshouse.MetricBytes
	m.Name = []byte("__src_client_write_err")
	m.Tags = []tl.DictFieldStringStringBytes{
		{[]byte("1"), []byte("1")},           // lang: golang
		{[]byte("2"), []byte("1")},           // kind: would_block
		{[]byte("3"), []byte("sh-balancer")}, // application name
	}
	if s.cfg.HostTag != "" {
		m.SetHost([]byte(s.cfg.HostTag))
	}
	return m
}

func (s *tcpSender) reportWouldBlockIfAny(conn net.Conn, m tlstatshouse.MetricBytes, scratch []byte) {
	n := s.wouldBlockBytes.Swap(0)
	if n == 0 {
		return
	}
	scratch = encodeClientWriteErrPacket(float64(n), m, scratch)
	if len(scratch) == 0 {
		return
	}
	if _, err := conn.Write(scratch); err != nil {
		s.stats.writeErrors.Add(1)
	}
}

func encodeClientWriteErrPacket(bytesDropped float64, m tlstatshouse.MetricBytes, pkt []byte) []byte {
	m.SetValue([]float64{bytesDropped})
	var batch = tlstatshouse.AddMetricsBatchBytes{
		Metrics: []tlstatshouse.MetricBytes{m},
	}
	pkt = append(pkt[:4], batch.WriteTL1Boxed(pkt[4:4])...)
	binary.LittleEndian.PutUint32(pkt[:4], uint32(len(pkt[4:])))
	return pkt
}

func (s *tcpSender) reconnect() (net.Conn, error) {
	s.poolMu.Lock()
	addr, ok := s.pool.pick()
	s.poolMu.Unlock()
	if !ok {
		return nil, errors.New("no resolved upstream addresses")
	}
	conn, err := (&net.Dialer{Timeout: s.cfg.DialTimeout}).Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	if _, err = conn.Write([]byte(tcpHandshakeMagic)); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

func (s *tcpSender) replacePool(pool addressPool) {
	s.poolMu.Lock()
	s.pool = pool
	s.poolMu.Unlock()
}

func newAddressPools(addrs []string) (primary, secondary addressPool) {
	mid := (len(addrs) + 1) / 2
	primary = addressPool{addrs: addrs[:mid]}
	if mid < len(addrs) {
		secondary = addressPool{addrs: addrs[mid:]}
	}
	return primary, secondary
}

func (p *addressPool) pick() (string, bool) {
	if len(p.addrs) == 0 {
		return "", false
	}
	addr := p.addrs[p.head]
	p.head = (p.head + 1) % len(p.addrs)
	return addr, true
}

func resolveDialTargets(network, rawAddr string) ([]string, error) {
	if rawAddr == "" {
		return []string{""}, nil
	}
	if network == "unixgram" {
		return []string{rawAddr}, nil
	}

	var addrs []string
	for _, ep := range strings.Split(rawAddr, ",") {
		host, port, err := net.SplitHostPort(strings.TrimSpace(ep))
		if err != nil {
			return nil, err
		}
		if ip := net.ParseIP(host); ip != nil {
			addrs = append(addrs, net.JoinHostPort(host, port))
			continue
		}
		recs, err := net.DefaultResolver.LookupIPAddr(context.Background(), host)
		if err != nil {
			return nil, err
		}
		for _, rec := range recs {
			addrs = append(addrs, net.JoinHostPort(rec.String(), port))
		}
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	r.Shuffle(len(addrs), func(i, j int) {
		addrs[i], addrs[j] = addrs[j], addrs[i]
	})
	return addrs, nil
}
