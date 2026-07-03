package balancer

import (
	"context"
	"encoding/binary"
	"errors"
	"log"
	"math/rand"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/VKCOM/statshouse/internal/receiver"
)

const (
	bufferSize                = 50 * 1024 * 1024                  // 50Mb
	bufferLen                 = bufferSize / (pktBodyMax) / 2 / 2 // 2 workers with 2 slices
	swapWaitMax               = 1 * time.Second                   // cap wait for 20% batch in swap
	defaultDNSRefreshInterval = time.Minute
	defaultDialTimeout        = 5 * time.Second
	defaultReconnectDelay     = time.Second
	defaultWriteTimeout       = 15 * time.Second
	writeTimeoutAccuracy      = 2 * time.Second
	defaultStuckReconDelay    = 30 * time.Second // protection from slow agents
)

var errWouldBlock = errors.New("would block")
var errNoAddress = errors.New("no resolved upstream addresses")

type EgressConfig struct {
	Network            string
	Address            string
	HostTag            string
	DNSRefreshInterval time.Duration
	DialTimeout        time.Duration
	ReconnectDelay     time.Duration
	WriteTimeout       time.Duration
	StuckReconDelay    time.Duration

	reconnectKey string // copy every connect for safety
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
	primPtr   **tcpSender // ptr to primary, nonblocking swap
	secPtr    **tcpSender // ptr to secondary, nonblocking swap
	primary   *tcpSender
	secondary *tcpSender
	closed    chan struct{}
}

type tcpSender struct {
	cfg   EgressConfig
	stats *egressStatsAtomic

	wouldBlockBytes atomic.Int64

	poolMu sync.Mutex
	pool   addressPool
	buf    *pktBuffer

	reconCh  chan struct{}
	closeCh  chan struct{}
	closeWg  sync.WaitGroup
	closeErr chan error
}

type addressPool struct {
	addrs []string
	head  int
}

type pktBuffer struct {
	mu     sync.Mutex
	cond   *sync.Cond
	closed bool
	rm     int
	ri     int
	wi     int
	r      [bufferLen][]byte
	w      [bufferLen][]byte
}

func NewEgress(cfg EgressConfig) *Egress {
	cfg.fillDefaults()
	e := &Egress{cfg: cfg}
	e.pool = &tcpPool{
		primary:   newTCPSender(cfg, &e.stats, addressPool{}, newPktBuffer()),
		secondary: newTCPSender(cfg, &e.stats, addressPool{}, newPktBuffer()),
		closed:    make(chan struct{}),
	}
	e.pool.primPtr = &e.pool.primary
	e.pool.secPtr = &e.pool.secondary
	go e.pool.runDNSRefresh(cfg, &e.stats)
	time.Sleep(2 * time.Second) // time to connect
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
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = defaultWriteTimeout
	}
	if cfg.StuckReconDelay <= 0 {
		cfg.StuckReconDelay = defaultStuckReconDelay
	}

	buf := make([]byte, 0, len(receiver.TCPPrefix)+1+4+len(cfg.HostTag))
	buf = append(buf, receiver.TCPPrefix...)
	buf = append(buf, receiver.TCPMagicV2Balancer)
	lenH := make([]byte, pktHeadLen)
	binary.LittleEndian.PutUint32(lenH, uint32(len(cfg.HostTag)))
	buf = append(buf, lenH...)
	buf = append(buf, cfg.HostTag...)
	cfg.reconnectKey = string(buf)
}

func newTCPSender(cfg EgressConfig, stats *egressStatsAtomic, pool addressPool, buf *pktBuffer) *tcpSender {
	s := &tcpSender{
		cfg:      cfg,
		stats:    stats,
		pool:     pool,
		buf:      buf,
		reconCh:  make(chan struct{}, 1),
		closeCh:  make(chan struct{}),
		closeErr: make(chan error, 1),
	}
	s.closeWg.Add(1)
	go s.sendLoop()
	return s
}

func (e *Egress) WritePacketLocked(pkt []byte) []byte {
	var err error
	pkt, err = e.pool.writeLocked(pkt)
	if err != nil {
		if errors.Is(err, errWouldBlock) {
			e.stats.droppedPackets.Add(1)
		}
		return pkt
	}
	e.stats.forwardedPackets.Add(1)
	return pkt
}

func (e *Egress) Stats() EgressStats {
	return EgressStats{
		ForwardedPackets: e.stats.forwardedPackets.Swap(0),
		DroppedPackets:   e.stats.droppedPackets.Swap(0),
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

func (p *tcpPool) writeLocked(pkt []byte) ([]byte, error) {
	select {
	case <-p.closed:
		return pkt, errWouldBlock
	default:
	}
	if pkt, ok := (*p.primPtr).buf.push(pkt); ok {
		return pkt, nil
	}
	if pkt, ok := (*p.secPtr).buf.push(pkt); ok {
		select {
		case (*p.primPtr).reconCh <- struct{}{}:
		default:
		}
		p.primPtr, p.secPtr = p.secPtr, p.primPtr
		return pkt, nil
	}
	p.primary.wouldBlockBytes.Add(int64(len(pkt)))
	return pkt, errWouldBlock
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

func (s *tcpSender) close() error {
	close(s.closeCh)
	s.buf.close()
	s.closeWg.Wait()
	return <-s.closeErr
}

func (s *tcpSender) sendLoop() {
	defer s.closeWg.Done()
	var conn net.Conn
	var err error
	var lastDial time.Time
	var lastStuckRecon = time.Now()
	var writeDeadline time.Time
	var bufs = make(net.Buffers, 0, bufferLen)
	m := s.getWriteErrM()
	var scratch = make([]byte, 0, pktHeadLen)
loop:
	for {
		select {
		case <-s.closeCh:
			break loop
		case <-s.reconCh:
			if lastStuckRecon.Add(s.cfg.StuckReconDelay).After(time.Now()) {
				continue
			}
			if conn != nil {
				_ = conn.Close()
				conn = nil
				writeDeadline = time.Time{}
				lastStuckRecon = time.Now()
			}
		default:
		}
		if conn == nil {
			time.Sleep(s.cfg.ReconnectDelay - time.Since(lastDial))
			lastDial = time.Now()
			conn, err = s.reconnect()
			if err != nil {
				if !errors.Is(err, errNoAddress) { // ignore secondary without address
					s.stats.reconnectErrors.Add(1)
				}
				log.Printf("balancer reconnect error: %v", err)
				continue
			}
			writeDeadline = time.Time{}
		}
		if s.cfg.WriteTimeout-time.Until(writeDeadline) > writeTimeoutAccuracy {
			deadline := time.Now().Add(s.cfg.WriteTimeout)
			if err = conn.SetWriteDeadline(deadline); err != nil {
				s.stats.writeErrors.Add(1)
				_ = conn.Close()
				conn = nil
				log.Printf("balancer set deadline error: %v", err)
				continue
			}
			writeDeadline = deadline
		}
		if err = s.buf.pop(func(pkts [][]byte) (int, error) {
			bufs = append(bufs[:0], pkts...) // writeTo changes slice to nil. So copy only const header
			_, err := bufs.WriteTo(conn)
			return len(bufs) - 1, err // not resend for last
		}); err != nil {
			s.stats.writeErrors.Add(1)
			_ = conn.Close()
			conn = nil
			log.Printf("balancer write error: %v", err)
			continue // not resend packet
		}
		scratch = s.reportWouldBlockIfAny(conn, m, scratch)
	}
	if conn != nil {
		err = conn.Close()
	}
	s.closeErr <- err
}

func (s *tcpSender) getWriteErrM() tlstatshouse.Metric {
	var m tlstatshouse.Metric
	m.Name = "__src_client_write_err"
	m.Tags = map[string]string{
		"1":  "1",           // lang: golang
		"2":  "1",           // kind: would_block
		"3":  "sh-balancer", // application name
		"_h": s.cfg.HostTag,
	}
	return m
}

func (s *tcpSender) reportWouldBlockIfAny(conn net.Conn, m tlstatshouse.Metric, scratch []byte) []byte {
	n := s.wouldBlockBytes.Swap(0)
	if n == 0 {
		return scratch
	}
	scratch = encodeClientWriteErrPacket(float64(n), m, scratch)
	if len(scratch) == 0 {
		return scratch
	}
	if _, err := conn.Write(scratch); err != nil {
		s.stats.writeErrors.Add(1)
	}
	return scratch
}

func encodeClientWriteErrPacket(bytesDropped float64, m tlstatshouse.Metric, pkt []byte) []byte {
	m.SetValue([]float64{bytesDropped})
	var batch = tlstatshouse.AddMetricsBatch{
		Metrics: []tlstatshouse.Metric{m},
	}
	pkt = append(pkt[:pktHeadLen], batch.WriteTL1Boxed(pkt[pktHeadLen:pktHeadLen])...)
	binary.LittleEndian.PutUint32(pkt[:pktHeadLen], uint32(len(pkt[pktHeadLen:])))
	return pkt
}

func (s *tcpSender) reconnect() (net.Conn, error) {
	s.poolMu.Lock()
	addr, ok := s.pool.pick()
	s.poolMu.Unlock()
	if !ok {
		return nil, errNoAddress
	}
	conn, err := (&net.Dialer{Timeout: s.cfg.DialTimeout}).Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	if _, err = conn.Write([]byte(s.cfg.reconnectKey)); err != nil {
		_ = conn.Close()
		return nil, err
	}
	log.Printf("balancer connected to upstream: %s", addr)
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

func newPktBuffer() *pktBuffer {
	b := pktBuffer{
		r: [bufferLen][]byte{},
		w: [bufferLen][]byte{},
	}
	b.cond = sync.NewCond(&b.mu)
	for i := 0; i < bufferLen; i++ {
		b.r[i] = make([]byte, 0, pktFrameMax)
		b.w[i] = make([]byte, 0, pktFrameMax)
	}
	return &b
}

func (b *pktBuffer) push(pkt []byte) ([]byte, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.wi >= bufferLen {
		b.cond.Signal()
		return pkt, false
	}
	var cur []byte
	cur, b.w[b.wi] = b.w[b.wi], pkt // swap packets
	b.wi++
	b.cond.Signal()
	return cur, true
}

func (b *pktBuffer) pop(f func(pkts [][]byte) (int, error)) error {
	if b.ri >= b.rm {
		b.swap()
	}
	if b.ri >= b.rm {
		return nil
	}
	n, err := f(b.r[b.ri:b.rm])
	if err != nil {
		b.ri = b.rm - n
		return err
	}
	b.ri = b.rm
	b.swap()
	return nil
}

func (b *pktBuffer) swap() {
	b.mu.Lock()
	defer b.mu.Unlock()
	timeout := false
	timer := time.AfterFunc(swapWaitMax, func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		timeout = true
	})
	defer timer.Stop()
	for b.wi < bufferLen*20/100 && !b.closed && !timeout { // wait for 20% full (or swapWaitMax)
		b.cond.Wait()
	}
	if b.closed {
		return
	}
	b.rm = b.wi
	b.wi = 0
	b.ri = 0
	b.r, b.w = b.w, b.r
}

func (b *pktBuffer) close() {
	b.mu.Lock()
	b.closed = true
	b.mu.Unlock()
	b.cond.Broadcast()
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
