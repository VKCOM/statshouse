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

	ctx        context.Context
	agent      *agent.Agent
	group      sync.WaitGroup
	cluster    string
	startTime  uint32
	serverKeys []string
	serverOpts rpc.ServerOptions
	clientOpts rpc.ClientOptions

	// logging
	rareLogLast time.Time
	rareLogMu   sync.Mutex

	// metrics
	commonTags            statshouse.Tags
	сonnectionCountMetric statshouse.MetricRef
	requestMemoryMetric   statshouse.MetricRef
	responseMemoryMetric  statshouse.MetricRef
}

type proxyServer struct {
	*ingressProxy2
	config    tlstatshouse.GetConfigResult
	listeners []net.Listener
}

type proxyConn struct {
	*proxyServer
	clientConn   *rpc.PacketConn
	upstreamConn *rpc.PacketConn

	// no synchronization, owned by "requestLoop"
	clientAddr [4]uint32 // readonly after init
	req        rpc.ServerRequest
	reqBuf     []byte
	reqTip     uint32
	reqBufCap  int // last buffer capacity
}

func RunIngressProxy2(ctx context.Context, agent *agent.Agent, config ConfigIngressProxy, aesPwd string) error {
	env := env.ReadEnvironment("statshouse_proxy_v2")
	commonTags := statshouse.Tags{
		env.Name,
		env.Service,
		env.Cluster,
		env.DataCenter,
	}
	p := ingressProxy2{
		ctx:                   ctx,
		agent:                 agent,
		cluster:               config.Cluster,
		clientOpts:            rpc.ClientOptions{CryptoKey: aesPwd},
		serverKeys:            config.IngressKeys,
		startTime:             uint32(time.Now().Unix()),
		commonTags:            commonTags,
		сonnectionCountMetric: statshouse.GetMetricRef("common_rpc_server_conn", commonTags),
		requestMemoryMetric:   statshouse.GetMetricRef("common_rpc_server_request_mem", commonTags),
		responseMemoryMetric:  statshouse.GetMetricRef("common_rpc_server_response_mem", commonTags),
	}
	p.uniqueStartTime.Store(p.startTime)
	rpc.ClientWithTrustedSubnetGroups(build.TrustedSubnetGroups())(&p.clientOpts)
	rpc.ServerWithTrustedSubnetGroups(build.TrustedSubnetGroups())(&p.serverOpts)
	defer statshouse.StopRegularMeasurement(
		statshouse.StartRegularMeasurement(func(client *statshouse.Client) {
			p.сonnectionCountMetric.Count(float64(p.сonnectionCount.Load()))
			p.requestMemoryMetric.Value(float64(p.requestMemory.Load()))
		}))
	// listen on IPv4
	tcp4 := p.newProxyServer()
	tcp6 := p.newProxyServer()
	shutdown := func() {
		tcp4.shutdown()
		tcp6.shutdown()
	}
	if len(config.ExternalAddresses) != 0 && config.ExternalAddresses[0] != "" {
		err := tcp4.listen("tcp4", config.ListenAddr, config.ExternalAddresses)
		if err != nil {
			shutdown()
			return err
		}
	}
	// listen on IPv6
	defer tcp6.shutdown()
	if len(config.ExternalAddressesIPv6) != 0 && config.ExternalAddressesIPv6[0] != "" {
		err := tcp6.listen("tcp6", config.ListenAddrIPV6, config.ExternalAddressesIPv6)
		if err != nil {
			shutdown()
			return err
		}
	}
	if len(tcp4.listeners) == 0 && len(tcp6.listeners) == 0 {
		return fmt.Errorf("at least one ingress-external-addr must be provided")
	}
	// run
	log.Printf("Running ingress proxy v2, PID %d\n", os.Getpid())
	tcp4.run()
	tcp6.run()
	<-ctx.Done()
	log.Printf("Shutdown %v connection(s)\n", p.сonnectionCount.Load())
	shutdown()
	p.group.Wait()
	return nil
}

func (p *ingressProxy2) reportClientConnError(err error) {
	if err == nil {
		return
	}
	tags := p.commonTags // copy
	tags[4] = rpc.ErrorTag(err)
	statshouse.Count("common_rpc_server_conn_error", tags, 1)
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

func (p *ingressProxy2) newProxyServer() proxyServer {
	return proxyServer{
		ingressProxy2: p,
		config: tlstatshouse.GetConfigResult{
			Addresses:         make([]string, 0, len(p.agent.GetConfigResult.Addresses)),
			MaxAddressesCount: p.agent.GetConfigResult.MaxAddressesCount,
			PreviousAddresses: p.agent.GetConfigResult.PreviousAddresses,
		},
	}
}

func (p *proxyServer) listen(network, addr string, externalAddr []string) error {
	if len(p.agent.GetConfigResult.Addresses)%len(externalAddr) != 0 {
		return fmt.Errorf("number of servers must be multiple of number of ingress-external-addr")
	}
	// parse listen address
	listenAddr, err := net.ResolveTCPAddr(network, addr)
	if err != nil {
		return err
	}
	// parse external addresses
	externalTCPAddr := make([]*net.TCPAddr, len(externalAddr))
	for i := range externalAddr {
		externalTCPAddr[i], err = net.ResolveTCPAddr(network, externalAddr[i])
		if err != nil {
			return err
		}
	}
	// open ports
	p.listeners = make([]net.Listener, len(p.agent.GetConfigResult.Addresses)/len(externalAddr))
	for i := range p.listeners {
		log.Printf("Listen addr %v\n", listenAddr)
		p.listeners[i], err = rpc.Listen(network, listenAddr.String(), false)
		if err != nil {
			return err
		}
		listenAddr.Port++
		for j := range externalTCPAddr {
			p.config.Addresses = append(p.config.Addresses, externalTCPAddr[j].String())
			externalTCPAddr[j].Port++
		}
	}
	log.Printf("External %s addr %s\n", network, strings.Join(p.config.Addresses, ", "))
	return nil
}

func (p *proxyServer) shutdown() {
	for i := range p.listeners {
		if p.listeners[i] != nil {
			_ = p.listeners[i].Close()
		}
	}
}

func (p *proxyServer) run() {
	p.group.Add(len(p.listeners))
	for i := range p.listeners {
		go p.serve(p.listeners[i])
	}
}

func (p *proxyServer) serve(listener net.Listener) {
	defer p.group.Done() // stop listening
	var acceptDelay time.Duration
	for {
		clientConn, err := listener.Accept()
		if err == nil {
			acceptDelay = 0
			go p.newProxyConn(clientConn).run()
			continue
		}
		if p.ctx.Err() != nil {
			return
		}
		// report accept failure then backoff
		tags := p.commonTags
		tags[4] = rpc.ErrorTag(err)
		statshouse.Count("common_rpc_server_accept_error", tags, 1)
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

func (p *proxyServer) newProxyConn(c net.Conn) *proxyConn {
	var clientAddr [4]uint32
	if addr, ok := c.RemoteAddr().(*net.TCPAddr); ok {
		for i, j := 0, 0; i < len(clientAddr) && j+3 < len(addr.IP); i, j = i+1, j+4 {
			clientAddr[i] = binary.BigEndian.Uint32(addr.IP[j:])
		}
	}
	clientConn := rpc.NewPacketConn(c, rpc.DefaultServerRequestBufSize, rpc.DefaultServerResponseBufSize)
	p.group.Add(1)
	p.сonnectionCount.Inc()
	return &proxyConn{
		proxyServer: p,
		clientAddr:  clientAddr,
		clientConn:  clientConn,
	}
}

func (p *proxyConn) run() {
	defer p.group.Done()
	defer p.сonnectionCount.Dec()
	defer p.clientConn.Close() // upstream connection will be closed at "responseLoop" exit
	defer func() {
		p.requestMemory.Sub(int64(p.reqBufCap))
	}()
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
		if p.ctx.Err() != nil {
			return
		}
		if p.reqTip == rpcInvokeReqHeaderTLTag {
			break
		}
		p.rareLog("Client skip #%d looking for invoke request, addr %v\n", p.reqTip, p.clientConn.RemoteAddr())
	}
	shardReplica := binary.LittleEndian.Uint32(p.req.Request[8:])
	shardReplica %= uint32(len(p.agent.GetConfigResult.Addresses))
	upstreamAddr := p.agent.GetConfigResult.Addresses[shardReplica]
	defer p.shutdownClientConn()
	p.rareLog("Connect shard replica %d, addr %v < %v\n", shardReplica, p.clientConn.LocalAddr(), p.clientConn.RemoteAddr())
	for p.ctx.Err() == nil {
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
	conn, err := net.DialTimeout("tcp", addr, rpc.DefaultPacketTimeout)
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
		if err := p.ctx.Err(); err != nil {
			return err // shutdown proxy connection
		}
		p.agent.AddValueCounter(requestSize, float64(len(p.req.Request)), 1, format.BuiltinMetricMetaRPCRequests)
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
	writeErr, readErr := rpc.ForwardPackets(p.ctx, p.clientConn, p.upstreamConn)
	if readErr != nil {
		p.rareLog("Upstream read error: %v\n", readErr)
		return nil // reconnect upstream
	}
	if writeErr != nil {
		p.rareLog("Client write error: %v\n", writeErr)
		p.reportClientConnError(writeErr)
		return writeErr // shutdown proxy connection
	}
	return nil
}

func (p *proxyConn) readRequest() error {
	buf := p.reqBuf
	for {
		var tip uint32
		var req rpc.ServerRequest
		var err error
		tip, req.Request, err = p.clientConn.ReadPacket(buf[:0], time.Duration(0))
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
					binary.LittleEndian.PutUint32(req.Request[16:], p.clientAddr[0])
					binary.LittleEndian.PutUint32(req.Request[20:], p.clientAddr[1])
					binary.LittleEndian.PutUint32(req.Request[24:], p.clientAddr[2])
					binary.LittleEndian.PutUint32(req.Request[28:], p.clientAddr[3])
					p.req, p.reqTip, p.reqBuf = req, tip, req.Response
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
			p.req, p.reqTip, p.reqBuf = req, tip, req.Response
			return err
		}
		buf = req.Response // buffer reuse
	}
}

func (p *proxyConn) replyError(err error) error {
	return p.req.WriteReponseAndFlush(p.clientConn, err, p.rareLog)
}

func (p *proxyConn) forwardRequest() error {
	return p.req.ForwardAndFlush(p.upstreamConn, p.reqTip, rpc.DefaultPacketTimeout)
}

func (p *proxyConn) reportRequestBufferSizeChange() {
	if v := int64(cap(p.reqBuf) - p.reqBufCap); v != 0 {
		p.requestMemory.Add(v)
		p.reqBufCap = cap(p.reqBuf)
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
