package balancer

import (
	"context"
	"errors"
	"log"
	"net"
	"strings"
	"sync"

	"github.com/VKCOM/tl/pkg/rpc"

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

	Handler HandlerConfig
	Egress  EgressConfig
}

type Service struct {
	cfg Config

	egress  *Egress
	handler *handler

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
	eg := NewEgress(cfg.Egress)
	h := newHandler(cfg.Handler, eg)

	s := &Service{
		cfg:       cfg,
		egress:    eg,
		handler:   h,
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
	s.handler.Close()
	closeErr = errors.Join(closeErr, s.egress.Close())
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
				if err := rcv.Serve(s.handler); err != nil {
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
		if err := s.tcpReceiver.Serve(s.handler, s.hijackTCP); err != nil {
			log.Printf("tcp receiver failed: %v", err)
		}
	}()
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.httpReceiver.Serve(s.handler, s.hijackHTTP); err != nil {
			log.Printf("http receiver failed: %v", err)
		}
	}()

	receiverRPC := receiver.MakeRPCReceiver(nil, s.handler)
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
