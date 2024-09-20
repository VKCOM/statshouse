package aggregator

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/vkcom/statshouse-go"
	"github.com/vkcom/statshouse/internal/agent"
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/constants"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/env"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/vkgo/build"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"
)

const (
	minAcceptDelay  = 5 * time.Millisecond
	maxAcceptDelay  = 1 * time.Second
	rareLogInterval = 1 * time.Second

	// copied from RPC internals
	rpcInvokeReqHeaderTLTag = 0x2374df3d
	rpcCancelReqTLTag       = 0x193f1b22
	rpcServerWantsFinTLTag  = 0xa8ddbc46
)

var errProxyConnShutdown = fmt.Errorf("proxy connection shutdown")

type ingressProxy2 struct {
	// Place atomics first to ensure proper alignment, see https://pkg.go.dev/sync/atomic#pkg-note-BUG
	uniqueStartTime atomic.Uint32
	сonnectionCount atomic.Int64
	requestMemory   atomic.Int64
	responseMemory  atomic.Int64

	*agent.Agent
	cluster      string
	config       tlstatshouse.GetConfigResult
	serverKeys   []string
	serverOpts   rpc.ServerOptions
	clientOpts   rpc.ClientOptions
	listeners    []net.Listener
	wg           sync.WaitGroup
	shutdownCtx  context.Context
	shutdownFunc func()
	startTime    uint32
	rareLogLast  time.Time
	rareLogMu    sync.Mutex

	// metrics
	commonMetricTags      statshouse.Tags
	сonnectionCountMetric *statshouse.MetricRef
	requestMemoryMetric   *statshouse.MetricRef
	responseMemoryMetric  *statshouse.MetricRef
	regularMeasurementID  int
}

type proxyConn struct {
	*ingressProxy2
	clientConn   *rpc.PacketConn
	upstreamConn *rpc.PacketConn

	// no synchronization, owned by "requestLoop"
	clientAddr uint32 // readonly after init
	req        rpc.ServerRequest
	reqBuf     []byte
	reqTip     uint32
	reqBufCap  int // last buffer capacity

	// no synchronization, owned by "responseLoop"
	respBuf    []byte
	respTip    uint32
	respBufCap int // last buffer capacity
}

func NewIngressProxy2(config ConfigIngressProxy, agent *agent.Agent, aesPwd string) (*ingressProxy2, error) {
	log.Printf("Running ingress proxy v2, PID %d\n", os.Getpid())
	if len(config.ExternalAddresses) == 0 {
		return nil, fmt.Errorf("at least one ingress-external-addr must be provided")
	}
	if len(agent.GetConfigResult.Addresses)%len(config.ExternalAddresses) != 0 {
		return nil, fmt.Errorf("number of servers must be multiple of number of ingress-external-addr")
	}
	env := env.ReadEnvironment("statshouse_proxy_v2")
	p := &ingressProxy2{
		Agent:   agent,
		cluster: config.Cluster,
		config: tlstatshouse.GetConfigResult{
			Addresses:         make([]string, 0, len(agent.GetConfigResult.Addresses)),
			MaxAddressesCount: agent.GetConfigResult.MaxAddressesCount,
			PreviousAddresses: agent.GetConfigResult.PreviousAddresses,
		},
		clientOpts: rpc.ClientOptions{CryptoKey: aesPwd},
		serverKeys: config.IngressKeys,
		listeners:  make([]net.Listener, len(agent.GetConfigResult.Addresses)/len(config.ExternalAddresses)),
		startTime:  uint32(time.Now().Unix()),
		commonMetricTags: statshouse.Tags{
			env.Name,
			env.Service,
			env.Cluster,
			env.DataCenter,
		},
	}
	p.uniqueStartTime.Store(p.startTime)
	rpc.ClientWithTrustedSubnetGroups(build.TrustedSubnetGroups())(&p.clientOpts)
	rpc.ServerWithTrustedSubnetGroups(build.TrustedSubnetGroups())(&p.serverOpts)
	p.сonnectionCountMetric = statshouse.Metric("common_rpc_server_conn", p.commonMetricTags)
	p.requestMemoryMetric = statshouse.Metric("common_rpc_server_request_mem", p.commonMetricTags)
	p.responseMemoryMetric = statshouse.Metric("common_rpc_server_response_mem", p.commonMetricTags)
	p.shutdownCtx, p.shutdownFunc = context.WithCancel(context.Background())
	var succeeded bool
	defer func() {
		if !succeeded {
			p.Shutdown()
		}
	}()
	// parse listen address
	listenAddr, err := net.ResolveTCPAddr(config.Network, config.ListenAddr)
	if err != nil {
		return nil, err
	}
	// parse external addresses
	externalAddresses := make([]*net.TCPAddr, len(config.ExternalAddresses))
	for i := range config.ExternalAddresses {
		externalAddresses[i], err = net.ResolveTCPAddr(config.Network, config.ExternalAddresses[i])
		if err != nil {
			return nil, err
		}
	}
	// open ports
	for i := range p.listeners {
		log.Printf("Listen addr %v\n", listenAddr)
		p.listeners[i], err = rpc.Listen(listenAddr.Network(), listenAddr.AddrPort().String(), false)
		if err != nil {
			return nil, err
		}
		listenAddr.Port++
		for j := range externalAddresses {
			p.config.Addresses = append(p.config.Addresses, externalAddresses[j].AddrPort().String())
			externalAddresses[j].Port++
		}
	}
	log.Printf("External addr %s\n", strings.Join(p.config.Addresses, ", "))
	succeeded = true
	return p, nil
}

func (p *ingressProxy2) Run() {
	p.regularMeasurementID = statshouse.StartRegularMeasurement(func(client *statshouse.Client) {
		p.сonnectionCountMetric.Count(float64(p.сonnectionCount.Load()))
		p.requestMemoryMetric.Value(float64(p.requestMemory.Load()))
		p.responseMemoryMetric.Value(float64(p.responseMemory.Load()))
	})
	p.wg.Add(len(p.listeners)) // start listening
	for i := range p.listeners {
		go p.listenAndServe(p.listeners[i])
	}
}

func (p *ingressProxy2) Shutdown() {
	log.Printf("Shutdown %v connection(s)\n", p.сonnectionCount.Load())
	statshouse.StopRegularMeasurement(p.regularMeasurementID)
	p.shutdownFunc()
	for i := range p.listeners {
		if p.listeners[i] != nil {
			_ = p.listeners[i].Close()
		}
	}
}

func (p *ingressProxy2) WaitStopped(timeout time.Duration) error {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(timeout))
	defer cancel()
	done := make(chan bool)
	go func() {
		p.wg.Wait()
		done <- true
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		// reading from both client and upstream is done with infinite timeout, checking shutdown flag when possible
		log.Printf("Shutdown failed to complete in %v, %d connection(s) left\n", timeout, p.сonnectionCount.Load())
		return ctx.Err()
	}
}

func (p *ingressProxy2) listenAndServe(ln net.Listener) {
	defer p.wg.Done() // stop listening
	var acceptDelay time.Duration
	for {
		clientConn, err := ln.Accept()
		if err == nil {
			acceptDelay = 0
			go p.newProxyConn(clientConn).run()
			continue
		}
		if p.shutdownCtx.Err() != nil {
			return
		}
		// report accept failure then backoff
		tags := p.commonMetricTags
		tags[4] = rpc.ErrorTag(err)
		statshouse.Metric("common_rpc_server_accept_error", tags).Count(1)
		if acceptDelay == 0 {
			acceptDelay = minAcceptDelay
		} else {
			acceptDelay *= 2
		}
		if acceptDelay > maxAcceptDelay {
			acceptDelay = maxAcceptDelay
		}
		time.Sleep(acceptDelay)
	}
}

func (p *ingressProxy2) reportClientConnError(err error) {
	if err == nil {
		return
	}
	tags := p.commonMetricTags // copy
	tags[4] = rpc.ErrorTag(err)
	statshouse.Metric("common_rpc_server_conn_error", tags).Count(1)
}

func (p *ingressProxy2) rareLog(format string, args ...any) {
	now := time.Now()
	p.rareLogMu.Lock()
	defer p.rareLogMu.Unlock()
	if now.Sub(p.rareLogLast) > rareLogInterval {
		p.rareLogLast = now
		log.Printf(format, args...)
	}
}

func (p *ingressProxy2) newProxyConn(c net.Conn) *proxyConn {
	clientAddr, _ := addrIPString(c.RemoteAddr())
	clientConn := rpc.NewPacketConn(c, rpc.DefaultServerRequestBufSize, rpc.DefaultServerResponseBufSize)
	p.wg.Add(1)
	p.сonnectionCount.Inc()
	return &proxyConn{
		ingressProxy2: p,
		clientAddr:    clientAddr,
		clientConn:    clientConn,
	}
}

func (p *proxyConn) run() {
	defer p.wg.Done()
	defer p.сonnectionCount.Dec()
	defer p.clientConn.Close() // upstream connection will be closed at "responseLoop" exit
	// handshake client
	_, _, err := p.clientConn.HandshakeServer(p.serverKeys, p.serverOpts.TrustedSubnetGroups, true, p.startTime, rpc.DefaultPacketTimeout)
	if err != nil {
		p.rareLog("Client handshake error: %v\n", err)
		p.reportClientConnError(err)
		return
	}
	// read first request to get shardReplica
	for {
		if err = p.readRequest(); err != nil {
			p.rareLog("Client read error: %v\n", err)
			p.reportClientConnError(err)
			return
		}
		if p.shutdownCtx.Err() != nil {
			return
		}
		if p.reqTip == rpcInvokeReqHeaderTLTag {
			break
		}
		p.rareLog("Client skip #%d looking for invoke request, addr %v\n", p.reqTip, p.clientConn.RemoteAddr())
	}
	shardReplica := binary.LittleEndian.Uint32(p.req.Request[8:])
	shardReplica %= uint32(len(p.GetConfigResult.Addresses))
	upstreamAddr := p.GetConfigResult.Addresses[shardReplica]
	defer p.shutdownClientConn()
	p.rareLog("Connect shard replica %d, addr %v < %v\n", shardReplica, p.clientConn.LocalAddr(), p.clientConn.RemoteAddr())
	for p.shutdownCtx.Err() == nil {
		// (re)connect upstream
		if err := p.connectUpstream(upstreamAddr); err != nil {
			break
		}
		// upstream connection will be closed at "responseLoop" exit, serve
		var responseLoop errgroup.Group
		responseLoop.Go(p.responseLoop)
		clientReadErr := p.requestLoop()
		clientWriteErr := responseLoop.Wait()
		if clientReadErr != nil || clientWriteErr != nil {
			break
		}
	}
	// done serving
	p.rareLog("Disconnect shard replica %d, addr %v < %v\n", shardReplica, p.clientConn.LocalAddr(), p.clientConn.RemoteAddr())
}

func (p *proxyConn) connectUpstream(addr string) error {
	conn, err := net.DialTimeout("tcp4", addr, rpc.DefaultPacketTimeout)
	if err != nil {
		p.rareLog("Upstream connect error: %v\n", err)
		return err
	}
	packetConn := rpc.NewPacketConn(conn, rpc.DefaultClientConnReadBufSize, rpc.DefaultClientConnWriteBufSize)
	err = packetConn.HandshakeClient(p.clientOpts.CryptoKey, p.clientOpts.TrustedSubnetGroups, false, p.uniqueStartTime.Dec(), 0, rpc.DefaultPacketTimeout, rpc.LatestProtocolVersion)
	if err != nil {
		p.rareLog("Upstream handshake error: %v\n", err)
		conn.Close()
		return err
	}
	p.upstreamConn = packetConn
	return nil
}

func (p *proxyConn) requestLoop() error {
	// exit means inability to deliver a request
	// "responseLoop" terminates when server done sending responses
	defer p.upstreamConn.ShutdownWrite()
	cryptoKeyID := p.clientConn.KeyID()
	requestSize := data_model.Key{
		Metric: format.BuiltinMetricIDRPCRequests,
		Keys:   [16]int32{0, format.TagValueIDComponentIngressProxy, int32(p.req.RequestTag()), 0, 0, 0, int32(binary.BigEndian.Uint32(cryptoKeyID[:4])), 0, int32(p.clientConn.ProtocolVersion())},
	}
	for {
		if err := p.shutdownCtx.Err(); err != nil {
			return err // shutdown proxy connection
		}
		p.AddValueCounter(requestSize, float64(len(p.req.Request)), 1, format.BuiltinMetricMetaRPCRequests)
		p.reportRequestBufferSizeChange()
		if err := p.forwardRequest(); err != nil {
			p.rareLog("Upstream write error: %v\n", err)
			return nil // reconnect upstream
		}
		if err := p.readRequest(); err != nil {
			p.rareLog("Client read error: %v\n", err)
			p.reportClientConnError(err)
			return err // shutdown proxy connection
		}
		requestSize.Keys[2] = int32(p.req.RequestTag())
	}
}

func (p *proxyConn) responseLoop() error {
	// exit means inability to deliver a response
	// "requestLoop" terminates on next upstream write
	defer p.upstreamConn.Close()
	for {
		if err := p.shutdownCtx.Err(); err != nil {
			return err // shutdown proxy connection
		}
		if err := p.readResponse(); err != nil {
			p.rareLog("Upstream read error: %v\n", err)
			return nil // reconnect upstream
		}
		if err := p.forwardResponse(); err != nil {
			p.rareLog("Client write error: %v\n", err)
			p.reportClientConnError(err)
			return err // shutdown proxy connection
		}
		p.reportResponseBufferSizeChange()
	}
}

func (p *proxyConn) readRequest() error {
	buf := p.reqBuf
	for {
		var tip uint32
		var req rpc.ServerRequest
		var err error
		tip, req.Request, err = p.clientConn.ReadPacket(buf, time.Duration(0))
		if err != nil {
			p.req, p.reqTip, p.reqBuf = req, tip, req.Request
			return err
		}
		switch tip {
		case rpcCancelReqTLTag:
			p.req, p.reqTip, p.reqBuf = req, tip, req.Request
			return nil
		case rpcInvokeReqHeaderTLTag:
			req.Response = req.Request
			if err = req.ParseInvokeReq(&p.serverOpts); err != nil {
				// goto WriteReponseAndFlush
			} else if len(req.Request) < 32 {
				err = fmt.Errorf("ingress proxy query with tag 0x%x is too short - %d bytes", req.RequestTag(), len(req.Request))
			} else {
				switch req.RequestTag() {
				case constants.StatshouseGetConfig2:
					var args tlstatshouse.GetConfig2
					if _, err = args.ReadBoxed(req.Request); err == nil {
						if args.Cluster != p.cluster {
							err = fmt.Errorf("statshouse misconfiguration! cluster requested %q does not match actual cluster connected %q", args.Cluster, p.cluster)
						} else {
							req.Response, _ = args.WriteResult(req.Response[:0], p.config)
						}
					}
				case constants.StatshouseGetTagMapping2,
					constants.StatshouseSendKeepAlive2,
					constants.StatshouseSendSourceBucket2,
					constants.StatshouseTestConnection2,
					constants.StatshouseGetTargets2,
					constants.StatshouseGetTagMappingBootstrap,
					constants.StatshouseGetMetrics3,
					constants.StatshouseAutoCreate:
					// pass
					fieldsMask := binary.LittleEndian.Uint32(req.Request[4:])
					fieldsMask |= (1 << 31) // args.SetIngressProxy(true)
					binary.LittleEndian.PutUint32(req.Request[4:], fieldsMask)
					binary.LittleEndian.PutUint32(req.Request[28:], p.clientAddr)
					p.req, p.reqTip, p.reqBuf = req, tip, req.Request
					return nil
				default:
					err = rpc.ErrNoHandler
				}
			}
		default:
			err = fmt.Errorf("unknown packet %d", tip)
		}
		// at this point either len(req.Response) != 0 or err != nil
		if err = req.WriteReponseAndFlush(p.clientConn, err, p.rareLog); err != nil {
			p.rareLog("Client write error: %v\n", err)
			p.reportClientConnError(err)
			p.req, p.reqTip, p.reqBuf = req, tip, req.Request
			return err
		}
		buf = req.Response[:0] // buffer reuse
	}
}

func (p *proxyConn) replyError(err error) error {
	return p.req.WriteReponseAndFlush(p.clientConn, err, p.rareLog)
}

func (p *proxyConn) forwardRequest() error {
	return p.req.ForwardAndFlush(p.upstreamConn, p.reqTip, rpc.DefaultPacketTimeout)
}

func (p *proxyConn) readResponse() error {
	var err error
	p.respTip, p.respBuf, err = p.upstreamConn.ReadPacket(p.respBuf[:0], time.Duration(0))
	return err
}

func (p *proxyConn) forwardResponse() error {
	return p.upstreamConn.WritePacket(p.respTip, p.respBuf, rpc.DefaultPacketTimeout)
}

func (p *proxyConn) reportRequestBufferSizeChange() {
	if v := int64(cap(p.reqBuf) - p.reqBufCap); v != 0 {
		p.requestMemory.Add(v)
		p.reqBufCap = cap(p.reqBuf)
	}
}

func (p *proxyConn) reportResponseBufferSizeChange() {
	if v := int64(cap(p.respBuf) - p.respBufCap); v != 0 {
		p.responseMemory.Add(v)
		p.respBufCap = cap(p.respBuf)
	}
}

func (p *proxyConn) shutdownClientConn() {
	// request connection shutdown
	if err := p.clientConn.WritePacket(rpcServerWantsFinTLTag, nil, rpc.DefaultPacketTimeout); err != nil {
		p.rareLog("Client write FIN error: %v\n", err)
		return
	}
	// respond to all remaining requests witn an error until client closes conection
	for {
		if err := p.readRequest(); err != nil {
			break
		}
		if err := p.replyError(errProxyConnShutdown); err != nil {
			break
		}
	}
	p.rareLog("Client shutdown completed, addr %v\n", p.clientConn.RemoteAddr())
}
