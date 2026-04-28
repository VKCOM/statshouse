package balancer

import (
	"context"
	"errors"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/VKCOM/tl/pkg/rpc"

	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/VKCOM/statshouse/internal/receiver"
)

type Config struct {
	HostName     string
	UpstreamAddr string

	ListenUDP4     string
	ListenUDP6     string
	ListenUnixgram string
	ListenTCP      string

	UDPBufferSize int
	CoresUDP      int
	WorkerCount   int

	Handler HandlerConfig
	Egress  EgressConfig
}

type Service struct {
	cfg Config

	pool *handlerPool

	udpReceivers []*receiver.UDP
	tcpReceiver  *receiver.TCP
	httpReceiver *receiver.HTTP
	hijackTCP    *rpc.HijackListener
	hijackHTTP   *rpc.HijackListener

	rpcServer *rpc.Server
	listeners []net.Listener

	wg sync.WaitGroup
}

func New(cfg Config) (*Service, error) {
	cfg.fillDefaults()
	hostTag := detectHostTag(cfg.HostName)
	cfg.Handler.HostTag = hostTag
	cfg.Egress.Address = cfg.UpstreamAddr
	cfg.Egress.HostTag = hostTag

	s := &Service{
		cfg:       cfg,
		pool:      newHandlerPool(cfg),
		listeners: make([]net.Listener, 0, 3),
	}
	return s, nil
}

func (cfg *Config) fillDefaults() {
	if cfg.UDPBufferSize <= 0 {
		cfg.UDPBufferSize = receiver.DefaultConnBufSize
	}
	if cfg.CoresUDP < 0 {
		cfg.CoresUDP = 0
	}
	if cfg.WorkerCount <= 0 {
		cfg.WorkerCount = 1
	}
}

func (s *Service) Run(ctx context.Context) error {
	if err := s.startUDP(); err != nil {
		return err
	}
	if err := s.startTCPStack(); err != nil {
		return err
	}

	log.Printf("statshouse-balancer started")
	<-ctx.Done()
	log.Printf("statshouse-balancer shutting down")
	return nil
}

func (s *Service) Close() error {
	var closeErr error
	for _, u := range s.udpReceivers {
		closeErr = errors.Join(closeErr, u.Close())
	}
	if s.tcpReceiver != nil {
		closeErr = errors.Join(closeErr, s.tcpReceiver.Close())
	}
	if s.hijackTCP != nil {
		closeErr = errors.Join(closeErr, s.hijackTCP.Close())
	}
	if s.hijackHTTP != nil {
		closeErr = errors.Join(closeErr, s.hijackHTTP.Close())
	}
	for _, ln := range s.listeners {
		closeErr = errors.Join(closeErr, ln.Close())
	}
	if s.rpcServer != nil {
		s.rpcServer.Shutdown()
	}
	s.wg.Wait()
	closeErr = errors.Join(closeErr, s.pool.close())
	return closeErr
}

func (s *Service) startUDP() error {
	start := func(network, addr string) error {
		if addr == "" || s.cfg.CoresUDP == 0 {
			return nil
		}
		reusePort := s.cfg.CoresUDP > 1 && network != "unixgram"
		u, err := receiver.ListenUDP(network, addr, s.cfg.UDPBufferSize, reusePort, nil, nil, nil)
		if err != nil {
			return err
		}
		s.udpReceivers = append(s.udpReceivers, u)
		for i := 1; i < s.cfg.CoresUDP; i++ {
			var dup *receiver.UDP
			if network == "unixgram" {
				dup, err = u.Duplicate()
			} else {
				dup, err = receiver.ListenUDP(network, addr, s.cfg.UDPBufferSize, true, nil, nil, nil)
			}
			if err != nil {
				return err
			}
			s.udpReceivers = append(s.udpReceivers, dup)
		}
		for _, r := range s.udpReceivers[len(s.udpReceivers)-s.cfg.CoresUDP:] {
			s.wg.Add(1)
			go func(rcv *receiver.UDP) {
				defer s.wg.Done()
				if err := rcv.Serve(s.pool); err != nil {
					log.Printf("udp receiver (%s) failed: %v", network, err)
				}
			}(r)
		}
		log.Printf("listening %s on %s by %d cores", network, addr, s.cfg.CoresUDP)
		return nil
	}

	if err := start("udp4", s.cfg.ListenUDP4); err != nil {
		return err
	}
	if err := start("udp6", s.cfg.ListenUDP6); err != nil {
		return err
	}
	if err := start("unixgram", s.cfg.ListenUnixgram); err != nil {
		return err
	}
	return nil
}

func (s *Service) startTCPStack() error {
	if s.cfg.ListenTCP == "" {
		return nil
	}

	ln, err := net.Listen("tcp", s.cfg.ListenTCP)
	if err != nil {
		return err
	}
	s.listeners = append(s.listeners, ln)

	s.hijackTCP = rpc.NewHijackListener(ln.Addr())
	s.hijackHTTP = rpc.NewHijackListener(ln.Addr())

	s.tcpReceiver = receiver.NewTCPReceiver(nil, nil)
	s.httpReceiver = receiver.NewHTTPReceiver(nil, nil)

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.tcpReceiver.Serve(s.pool, s.hijackTCP); err != nil {
			log.Printf("tcp receiver failed: %v", err)
		}
	}()
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.httpReceiver.Serve(s.pool, s.hijackHTTP); err != nil {
			log.Printf("http receiver failed: %v", err)
		}
	}()

	receiverRPC := receiver.MakeRPCReceiver(nil, s.pool)
	handlerRPC := &tlstatshouse.Handler{
		RawAddMetricsBatch: receiverRPC.RawAddMetricsBatch,
	}
	s.rpcServer = rpc.NewServer(
		rpc.ServerWithSyncHandler(handlerRPC.Handle),
		rpc.ServerWithSocketHijackHandler(s.hijackConnection),
	)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.rpcServer.Serve(ln); err != nil && !errors.Is(err, net.ErrClosed) {
			log.Printf("rpc receiver failed: %v", err)
		}
	}()
	log.Printf("listening udp/tcp/http/rpc on shared port %s", s.cfg.ListenTCP)
	return nil
}

func (s *Service) hijackConnection(conn *rpc.HijackConnection) {
	if strings.HasPrefix(string(conn.Magic), receiver.TCPPrefix) {
		conn.Magic = conn.Magic[len(receiver.TCPPrefix):]
		s.hijackTCP.AddConnection(conn)
		return
	}
	s.hijackHTTP.AddConnection(conn)
}

// handlerPool not true workers, just roundrobin
type handlerPool struct {
	mu             sync.Mutex
	head           int
	workers        []*handler
	reportInterval time.Duration
	stop           chan struct{}
}

func newHandlerPool(cfg Config) *handlerPool {
	p := &handlerPool{
		reportInterval: 45 * time.Second,
		stop:           make(chan struct{}),
	}
	for i := 0; i < cfg.WorkerCount; i++ {
		eg := NewEgress(cfg.Egress)
		p.workers = append(p.workers, newHandler(cfg.Handler, eg))
	}
	go p.reportLoop()
	return p
}

func (p *handlerPool) popHead() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	h := p.head
	p.head = (p.head + 1) % len(p.workers)
	return h
}

func (p *handlerPool) reportLoop() {
	t := time.NewTicker(p.reportInterval)
	defer t.Stop()
	for {
		select {
		case <-p.stop:
			return
		case <-t.C:
			p.logStats()
		}
	}
}

func (p *handlerPool) logStats() {
	hs, es := handlerStats{}, EgressStats{}
	for i := 0; i < len(p.workers); i++ {
		h := p.workers[i].Stats()
		s := p.workers[i].egress.Stats()

		hs.ParseErrors += h.ParseErrors
		es.ForwardedPackets += s.ForwardedPackets
		es.DroppedPackets += s.DroppedPackets
		es.DNSRefreshErrors += s.DNSRefreshErrors
		es.WriteErrors += s.WriteErrors
		es.ReconnectErrors += s.ReconnectErrors
	}
	log.Printf("balancer stats: fwd=%d drop=%d parse_err=%d reconnect_err=%d dns_err=%d write_err=%d",
		es.ForwardedPackets, es.DroppedPackets, hs.ParseErrors,
		es.ReconnectErrors, es.DNSRefreshErrors, es.WriteErrors)
}

func (p *handlerPool) close() error {
	var err error
	for _, h := range p.workers {
		h.Close()
		err = errors.Join(err, h.egress.Close())
	}
	close(p.stop)
	p.logStats()
	return err
}

func (p *handlerPool) HandleMetricsBatch(bytes *tlstatshouse.AddMetricsBatchBytes, size int, scratch *[]byte) error {
	h := p.popHead()
	return p.workers[h].HandleMetricsBatch(bytes, size, scratch)
}

func (p *handlerPool) HandleMetrics(args data_model.HandlerArgs) data_model.MappedMetricHeader {
	h := p.popHead()
	return p.workers[h].HandleMetrics(args)
}

func (p *handlerPool) HandleParseError(bytes []byte, err error) {
	h := p.popHead()
	p.workers[h].HandleParseError(bytes, err)
}
