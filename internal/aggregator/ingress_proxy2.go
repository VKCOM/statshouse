package aggregator

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"runtime"
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
	"github.com/vkcom/statshouse/internal/vkgo/srvfunc"
	"go.uber.org/atomic"
)

const (
	minAcceptDelay  = 5 * time.Millisecond
	maxAcceptDelay  = 1 * time.Second
	rareLogInterval = 1 * time.Second

	// copied from RPC internals
	rpcInvokeReqHeaderTLTag = 0x2374df3d
	rpcCancelReqTLTag       = 0x193f1b22
	rpcServerWantsFinTLTag  = 0xa8ddbc46
	rpcClientWantsFinTLTag  = 0x0b73429e
)

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
	clientAddr  [4]uint32 // readonly after init
	clientAddrS string    // readonly after init
	reqBuf      []byte
	reqBufCap   int // last buffer capacity
}

type proxyRequest struct {
	tip uint32
	rpc.ServerRequest
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
			var vmSize, vmRSS float64
			if st, _ := srvfunc.GetMemStat(0); st != nil {
				vmSize = float64(st.Size)
				vmRSS = float64(st.Res)
			}
			client.Value(format.BuiltinMetricNameProxyVmSize, statshouse.Tags{1: srvfunc.HostnameForStatshouse()}, vmSize)
			client.Value(format.BuiltinMetricNameProxyVmRSS, statshouse.Tags{1: srvfunc.HostnameForStatshouse()}, vmRSS)
			var memStats runtime.MemStats
			runtime.ReadMemStats(&memStats)
			client.Value(format.BuiltinMetricNameProxyHeapAlloc, statshouse.Tags{1: srvfunc.HostnameForStatshouse()}, float64(memStats.HeapAlloc))
			client.Value(format.BuiltinMetricNameProxyHeapSys, statshouse.Tags{1: srvfunc.HostnameForStatshouse()}, float64(memStats.HeapSys))
			client.Value(format.BuiltinMetricNameProxyHeapIdle, statshouse.Tags{1: srvfunc.HostnameForStatshouse()}, float64(memStats.HeapIdle))
			client.Value(format.BuiltinMetricNameProxyHeapInuse, statshouse.Tags{1: srvfunc.HostnameForStatshouse()}, float64(memStats.HeapInuse))
			//-- TODO: remove when deployed
			client.Value("igp_vm_size", statshouse.Tags{1: srvfunc.HostnameForStatshouse()}, vmSize)
			client.Value("igp_vm_rss", statshouse.Tags{1: srvfunc.HostnameForStatshouse()}, vmRSS)
			client.Value("igp_heap_alloc", statshouse.Tags{1: srvfunc.HostnameForStatshouse()}, float64(memStats.HeapAlloc))
			client.Value("igp_heap_sys", statshouse.Tags{1: srvfunc.HostnameForStatshouse()}, float64(memStats.HeapSys))
			client.Value("igp_heap_idle", statshouse.Tags{1: srvfunc.HostnameForStatshouse()}, float64(memStats.HeapIdle))
			client.Value("igp_heap_inuse", statshouse.Tags{1: srvfunc.HostnameForStatshouse()}, float64(memStats.HeapInuse))
			//--
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

func (p *ingressProxy2) reportClientConnError(format string, err error) {
	if err == nil || errors.Is(err, io.EOF) {
		return
	}
	p.rareLog(format, err)
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
		// report accept error then backoff
		tags := p.commonTags
		if tags[4] = rpc.ErrorTag(err); tags[4] == "" {
			tags[4] = err.Error()
		}
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
	var clientAddrS string
	if addr, ok := c.RemoteAddr().(*net.TCPAddr); ok {
		clientAddrS = addr.AddrPort().Addr().String()
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
		clientAddrS: clientAddrS,
		clientConn:  clientConn,
	}
}

func (p *proxyConn) run() {
	defer p.group.Done()
	defer p.сonnectionCount.Dec()
	defer p.clientConn.Close()
	defer func() {
		p.requestMemory.Sub(int64(p.reqBufCap))
	}()
	// handshake client
	magic_head, _, err := p.clientConn.HandshakeServer(p.serverKeys, p.serverOpts.TrustedSubnetGroups, true, p.startTime, rpc.DefaultPacketTimeout)
	if err != nil {
		p.rareLog("Client handshake error: %v\n", err)
		errStr := rpc.ErrorTag(err)
		if errStr == "" {
			errStr = err.Error()
		}
		tags := statshouse.Tags{
			p.commonTags[0], // env
			srvfunc.HostnameForStatshouse(),
			errStr,
			string(magic_head),
		}
		// TODO: remove when deployed
		statshouse.StringTop("igp_accept_handshake_error", tags, p.clientAddrS)
		statshouse.StringTop(format.BuiltinMetricNameProxyAcceptHandshakeError, tags, p.clientAddrS)
		return
	}
	// read first request to get shardReplica
	var req proxyRequest
	for {
		req, err = p.readRequest(p.ctx)
		if err != nil {
			p.reportClientConnError("Client read error: %v\n", err)
			return
		}
		if p.ctx.Err() != nil {
			return // server shutdown
		}
		if req.tip == rpcInvokeReqHeaderTLTag {
			break
		}
		p.rareLog("Client skip #%d looking for invoke request, addr %v\n", req.tip, p.clientConn.RemoteAddr())
	}
	shardReplica := binary.LittleEndian.Uint32(req.Request[8:])
	shardReplica %= uint32(len(p.agent.GetConfigResult.Addresses))
	upstreamAddr := p.agent.GetConfigResult.Addresses[shardReplica]
	p.rareLog("Connect shard replica %d, addr %v < %v\n", shardReplica, p.clientConn.LocalAddr(), p.clientConn.RemoteAddr())
	defer p.rareLog("Disconnect shard replica %d, addr %v < %v\n", shardReplica, p.clientConn.LocalAddr(), p.clientConn.RemoteAddr())
	// connect upstream
	upstreamConn, err := net.DialTimeout("tcp", upstreamAddr, rpc.DefaultPacketTimeout)
	if err != nil {
		p.rareLog("Upstream connect error: %v\n", err)
		return
	}
	defer upstreamConn.Close()
	p.upstreamConn = rpc.NewPacketConn(upstreamConn, rpc.DefaultClientConnReadBufSize, rpc.DefaultClientConnWriteBufSize)
	err = p.upstreamConn.HandshakeClient(p.clientOpts.CryptoKey, p.clientOpts.TrustedSubnetGroups, false, p.uniqueStartTime.Dec(), 0, rpc.DefaultPacketTimeout, rpc.LatestProtocolVersion)
	if err != nil {
		p.rareLog("Upstream handshake error: %v\n", err)
		return
	}
	// serve
	for ctx, shutdown := p.ctx, false; ; {
		var respLoop sync.WaitGroup
		var respLoopErr error
		respLoop.Add(1)
		go func() {
			defer respLoop.Done()
			respLoopErr = p.responseLoop(ctx)
		}()
		var reqLoopErr error
		req, reqLoopErr = p.requestLoop(ctx, req)
		respLoop.Wait()
		if shutdown || reqLoopErr != nil || respLoopErr != nil {
			return // either graceful shutdown attempt completed or error occurred
		}
		// graceful shutdown
		shutdown = true
		if err = p.clientConn.WritePacket(rpcServerWantsFinTLTag, nil, rpc.DefaultPacketTimeout); err != nil {
			p.reportClientConnError("Client write error: %v\n", err)
			return
		}
		ctx = context.Background() // server "main" exits after timeout
	}
}

func (p *proxyConn) requestLoop(ctx context.Context, req proxyRequest) (_ proxyRequest, err error) {
	defer func() {
		// do not close upstream on server shutdown, calling side initiates graceful shutdown
		if err != nil {
			p.upstreamConn.ShutdownWrite()
		}
	}()
	cryptoKeyID := p.clientConn.KeyID()
	requestSize := data_model.Key{
		Metric: format.BuiltinMetricIDRPCRequests,
		Tags:   [16]int32{0, format.TagValueIDComponentIngressProxy, int32(req.RequestTag()), 0, 0, 0, int32(binary.BigEndian.Uint32(cryptoKeyID[:4])), 0, int32(p.clientConn.ProtocolVersion())},
	}
	for i := uint(0); ; i++ {
		// request (tip) must be set except maybe graceful shutdown first iteration
		if i > 0 || req.tip != 0 {
			p.agent.AddValueCounter(requestSize, float64(len(req.Request)), 1, format.BuiltinMetricMetaRPCRequests)
			p.reportRequestBufferSizeChange()
			switch req.tip {
			case rpcInvokeReqHeaderTLTag:
				switch req.RequestTag() {
				case constants.StatshouseGetConfig2:
					var args tlstatshouse.GetConfig2
					if _, err = args.ReadBoxed(req.Request); err == nil {
						if args.Cluster != p.cluster {
							err = fmt.Errorf("statshouse misconfiguration! cluster requested %q does not match actual cluster connected %q", args.Cluster, p.cluster)
						} else {
							req.Response, _ = args.WriteResult(p.reqBuf[:0], p.config)
							p.reqBuf = req.Response
						}
					}
					if err = req.WriteReponseAndFlush(p.clientConn, err, p.rareLog); err != nil {
						p.reportClientConnError("Client write error: %v\n", err)
						// "requestLoop" exits on request read-write errors only, read next request
					}
				case rpcClientWantsFinTLTag:
					// no more client requests expected
					return proxyRequest{}, io.EOF
				default:
					fieldsMask := binary.LittleEndian.Uint32(req.Request[4:])
					fieldsMask |= (1 << 31) // args.SetIngressProxy(true)
					binary.LittleEndian.PutUint32(req.Request[4:], fieldsMask)
					binary.LittleEndian.PutUint32(req.Request[16:], p.clientAddr[0])
					binary.LittleEndian.PutUint32(req.Request[20:], p.clientAddr[1])
					binary.LittleEndian.PutUint32(req.Request[24:], p.clientAddr[2])
					binary.LittleEndian.PutUint32(req.Request[28:], p.clientAddr[3])
					if err = req.ForwardAndFlush(p.upstreamConn, req.tip, rpc.DefaultPacketTimeout); err != nil {
						p.rareLog("Upstream write error: %v\n", err)
						return proxyRequest{}, err
					}
				}
			}
		}
		if req, err = p.readRequest(ctx); err != nil {
			p.reportClientConnError("Client read error: %v\n", err)
			return proxyRequest{}, err
		}
		if ctx.Err() != nil {
			return req, nil // server shutdown
		}
		requestSize.Tags[2] = int32(req.RequestTag())
	}
}

func (p *proxyConn) responseLoop(ctx context.Context) (err error) {
	defer func() {
		// do not close client on server shutdown, calling side initiates graceful shutdown
		if err != nil {
			p.clientConn.ShutdownWrite()
		}
	}()
	clientErr, upstreamErr := rpc.ForwardPackets(ctx, p.clientConn, p.upstreamConn)
	if upstreamErr != nil {
		return upstreamErr
	}
	if clientErr != nil {
		return clientErr
	}
	return nil
}

func (p *proxyConn) readRequest(ctx context.Context) (proxyRequest, error) {
	var tip uint32
	var err error
	for {
		if ctx.Err() != nil {
			return proxyRequest{}, nil
		}
		if tip, p.reqBuf, err = p.clientConn.ReadPacket(p.reqBuf[:0], rpc.DefaultPacketTimeout); err == nil {
			break
		}
		var netErr net.Error
		if timeout := errors.As(err, &netErr) && netErr.Timeout(); !timeout {
			return proxyRequest{}, err
		}
	}
	switch tip {
	case rpcCancelReqTLTag, rpcClientWantsFinTLTag:
		return proxyRequest{tip, rpc.ServerRequest{Request: p.reqBuf}}, nil
	case rpcInvokeReqHeaderTLTag:
		req := rpc.ServerRequest{Request: p.reqBuf}
		if err = req.ParseInvokeReq(&p.serverOpts); err != nil {
			return proxyRequest{}, err
		}
		if len(req.Request) < 32 {
			return proxyRequest{}, fmt.Errorf("ingress proxy query with tag 0x%x is too short - %d bytes", req.RequestTag(), len(req.Request))
		}
		switch req.RequestTag() {
		case constants.StatshouseGetConfig2,
			constants.StatshouseGetTagMapping2,
			constants.StatshouseSendKeepAlive2,
			constants.StatshouseSendSourceBucket2,
			constants.StatshouseTestConnection2,
			constants.StatshouseGetTargets2,
			constants.StatshouseGetTagMappingBootstrap,
			constants.StatshouseGetMetrics3,
			constants.StatshouseAutoCreate:
			// pass
			return proxyRequest{tip, req}, nil
		default:
			return proxyRequest{}, rpc.ErrNoHandler
		}
	default:
		return proxyRequest{}, fmt.Errorf("unknown packet %d", tip)
	}
}

func (p *proxyConn) reportRequestBufferSizeChange() {
	if v := int64(cap(p.reqBuf) - p.reqBufCap); v != 0 {
		p.requestMemory.Add(v)
		p.reqBufCap = cap(p.reqBuf)
	}
}
