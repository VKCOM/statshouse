package aggregator

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
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
	rpcPingTLTag            = 0xa677ee41
)

type ConfigIngressProxy struct {
	Cluster               string
	Network               string
	ListenAddr            string
	ListenAddrIPV6        string
	ExternalAddresses     []string // exactly 3 comma-separated external ingress points
	ExternalAddressesIPv6 []string
	IngressKeys           []string
	ResponseMemoryLimit   int
	Version               string
}

type ingressProxy struct {
	// Place atomics first to ensure proper alignment, see https://pkg.go.dev/sync/atomic#pkg-note-BUG
	uniqueStartTime atomic.Uint32
	connectionCount atomic.Int64
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
	connectionCountMetric statshouse.MetricRef
	requestMemoryMetric   statshouse.MetricRef
	responseMemoryMetric  statshouse.MetricRef
}

type proxyServer struct {
	*ingressProxy
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
	req         proxyRequest
	reqKey      data_model.Key
	reqBuf      []byte
	reqBufCap   int // last buffer capacity
}

type proxyRequest struct {
	tip uint32
	rpc.HandlerContext
}

func (config *ConfigIngressProxy) ReadIngressKeys(ingressPwdDir string) error {
	dis, err := os.ReadDir(ingressPwdDir)
	if err != nil {
		return fmt.Errorf("warning - could not read ingress-pwd-dir %q: %v", ingressPwdDir, err)
	}
	for _, di := range dis {
		dn := filepath.Join(ingressPwdDir, di.Name())
		if di.IsDir() {
			continue
		}
		pwd, err := os.ReadFile(dn)
		if err != nil {
			return fmt.Errorf("warning - could not read ingress password file %q: %v", dn, err)
		}
		keyID := rpc.KeyIDFromCryptoKey(string(pwd))
		log.Printf("%s %s (%d bytes)", hex.EncodeToString(keyID[:]), dn, len(pwd))
		config.IngressKeys = append(config.IngressKeys, string(pwd))
	}
	log.Printf("Successfully read %d ingress keys from ingress-pwd-dir %q", len(config.IngressKeys), ingressPwdDir)
	return nil
}

func RunIngressProxy2(ctx context.Context, agent *agent.Agent, config ConfigIngressProxy, aesPwd string) error {
	env := env.ReadEnvironment("statshouse_proxy_v2")
	commonTags := statshouse.Tags{
		env.Name,
		env.Service,
		env.Cluster,
		env.DataCenter,
	}
	p := ingressProxy{
		ctx:                   ctx,
		agent:                 agent,
		cluster:               config.Cluster,
		clientOpts:            rpc.ClientOptions{CryptoKey: aesPwd},
		serverKeys:            config.IngressKeys,
		startTime:             uint32(time.Now().Unix()),
		commonTags:            commonTags,
		connectionCountMetric: statshouse.GetMetricRef("common_rpc_server_conn", commonTags),
		requestMemoryMetric:   statshouse.GetMetricRef("common_rpc_server_request_mem", commonTags),
		responseMemoryMetric:  statshouse.GetMetricRef("common_rpc_server_response_mem", commonTags),
	}
	p.uniqueStartTime.Store(p.startTime)
	rpc.ClientWithTrustedSubnetGroups(build.TrustedSubnetGroups())(&p.clientOpts)
	rpc.ServerWithTrustedSubnetGroups(build.TrustedSubnetGroups())(&p.serverOpts)
	defer statshouse.StopRegularMeasurement(
		statshouse.StartRegularMeasurement(func(client *statshouse.Client) {
			p.connectionCountMetric.Count(float64(p.connectionCount.Load()))
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
		}))
	// listen on IPv4
	tcp4 := p.newProxyServer()
	tcp6 := p.newProxyServer()
	shutdown := func() {
		tcp4.shutdown()
		tcp6.shutdown()
	}
	if len(config.ExternalAddresses) != 0 && config.ExternalAddresses[0] != "" {
		err := tcp4.listen("tcp4", config.ListenAddr, config.ExternalAddresses, config.Version)
		if err != nil {
			shutdown()
			return err
		}
	}
	// listen on IPv6
	defer tcp6.shutdown()
	if len(config.ExternalAddressesIPv6) != 0 && config.ExternalAddressesIPv6[0] != "" {
		err := tcp6.listen("tcp6", config.ListenAddrIPV6, config.ExternalAddressesIPv6, config.Version)
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
	log.Printf("Shutdown %v connection(s)\n", p.connectionCount.Load())
	shutdown()
	p.group.Wait()
	return nil
}

func (p *ingressProxy) reportClientConnError(format string, err error) {
	if err == nil || errors.Is(err, io.EOF) {
		return
	}
	p.rareLog(format, err)
	tags := p.commonTags // copy
	tags[4] = rpc.ErrorTag(err)
	statshouse.Count("common_rpc_server_conn_error", tags, 1)
}

func (p *ingressProxy) rareLog(format string, args ...any) {
	now := time.Now()
	p.rareLogMu.Lock()
	defer p.rareLogMu.Unlock()
	if now.Sub(p.rareLogLast) > rareLogInterval {
		p.rareLogLast = now
		log.Printf(format, args...)
	}
}

func (p *ingressProxy) newProxyServer() proxyServer {
	return proxyServer{
		ingressProxy: p,
		config: tlstatshouse.GetConfigResult{
			Addresses:         make([]string, 0, len(p.agent.GetConfigResult.Addresses)),
			MaxAddressesCount: p.agent.GetConfigResult.MaxAddressesCount,
			PreviousAddresses: p.agent.GetConfigResult.PreviousAddresses,
		},
	}
}

func (p *proxyServer) listen(network, addr string, externalAddr []string, version string) error {
	if len(p.agent.GetConfigResult.Addresses)%len(externalAddr) != 0 {
		return fmt.Errorf("number of servers must be multiple of number of ingress-external-addr")
	}
	// parse listen address
	listenAddr, err := net.ResolveTCPAddr(network, addr)
	if err != nil {
		return err
	}
	// build external address and listen
	if version == "2" {
		externalTCPAddr := make([]*net.TCPAddr, len(externalAddr))
		for i := range externalAddr {
			externalTCPAddr[i], err = net.ResolveTCPAddr(network, externalAddr[i])
			if err != nil {
				return err
			}
		}
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
	} else {
		log.Printf("Listen addr %v\n", listenAddr)
		p.listeners = make([]net.Listener, 1)
		p.listeners[0], err = rpc.Listen(network, listenAddr.String(), false)
		if err != nil {
			return err
		}
		n := len(p.agent.GetConfigResult.Addresses)
		s := make([]string, 0, n)
		for len(s) < n {
			for i := 0; i < len(externalAddr) && len(s) < n; i++ {
				s = append(s, externalAddr[i])
			}
		}
		p.config.Addresses = s
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
	p.connectionCount.Inc()
	return &proxyConn{
		proxyServer: p,
		clientAddr:  clientAddr,
		clientAddrS: clientAddrS,
		clientConn:  clientConn,
		reqKey: data_model.Key{
			Metric: format.BuiltinMetricIDRPCRequests,
			Tags:   [16]int32{0, format.TagValueIDComponentIngressProxy},
		},
	}
}

func (p *proxyConn) run() {
	defer p.group.Done()
	defer p.connectionCount.Dec()
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
		statshouse.StringTop(format.BuiltinMetricNameProxyAcceptHandshakeError, tags, p.clientAddrS)
		return
	}
	// initialize connection specific "__rpc_request_size" tags
	cryptoKeyID := p.clientConn.KeyID()
	p.reqKey.Tags[6] = int32(binary.BigEndian.Uint32(cryptoKeyID[:4]))
	p.reqKey.Tags[8] = int32(p.clientConn.ProtocolVersion())
	// read first request to get shardReplica
	var req = &p.req
	for {
		if err = req.read(p); err != nil {
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
	shardReplica := req.shardReplica(p)
	upstreamAddr := p.agent.GetConfigResult.Addresses[shardReplica]
	p.rareLog("Connect shard replica %d, addr %v < %v\n", shardReplica, p.clientConn.LocalAddr(), p.clientConn.RemoteAddr())
	defer p.rareLog("Disconnect shard replica %d, addr %v < %v\n", shardReplica, p.clientConn.LocalAddr(), p.clientConn.RemoteAddr())
	// connect upstream
	upstreamConn, err := net.DialTimeout("tcp", upstreamAddr, rpc.DefaultPacketTimeout)
	if err != nil {
		p.rareLog("Upstream connect error: %v\n", err)
		_ = req.WriteReponseAndFlush(p.clientConn, err)
		return
	}
	defer upstreamConn.Close()
	p.upstreamConn = rpc.NewPacketConn(upstreamConn, rpc.DefaultClientConnReadBufSize, rpc.DefaultClientConnWriteBufSize)
	err = p.upstreamConn.HandshakeClient(p.clientOpts.CryptoKey, p.clientOpts.TrustedSubnetGroups, false, p.uniqueStartTime.Dec(), 0, rpc.DefaultPacketTimeout, rpc.LatestProtocolVersion)
	if err != nil {
		p.rareLog("Upstream handshake error: %v\n", err)
		_ = req.WriteReponseAndFlush(p.clientConn, err)
		return
	}
	// serve
	var ctx = p.ctx
	var gracefulShutdown bool
	for { // two iterations at most, the latter is graceful shutdown
		var respLoop sync.WaitGroup
		var respLoopRes rpc.ForwardPacketsResult
		respLoop.Add(1)
		go func() {
			defer respLoop.Done()
			respLoopRes = p.responseLoop(ctx)
		}()
		reqLoopRes := p.requestLoop(ctx)
		respLoop.Wait()
		if gracefulShutdown || reqLoopRes.ClientWantsFin || respLoopRes.ServerWantsFin || reqLoopRes.Error() != nil || respLoopRes.Error() != nil {
			return // either graceful shutdown already attempted or error occurred
		}
		gracefulShutdown = true
		if err = p.clientConn.WritePacket(rpcServerWantsFinTLTag, nil, rpc.DefaultPacketTimeout); err != nil {
			p.reportClientConnError("Client write error: %v\n", err)
			return
		}
		// no timeout for connection graceful shutdown (has server level shutdown timeout)
		ctx = context.Background()
	}
}

func (p *proxyConn) requestLoop(ctx context.Context) (res rpc.ForwardPacketsResult) {
	defer func() {
		// do not close upstream on server shutdown, graceful shutdown attempt follows
		if res.Error() != nil {
			p.upstreamConn.ShutdownWrite()
		}
	}()
	req := &p.req
	var err error
	for i := uint(0); ctx.Err() == nil; i++ {
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
						req.Response, _ = args.WriteResult(req.Response[:0], p.config)
					}
				}
				if err = req.WriteReponseAndFlush(p.clientConn, err); err != nil {
					p.reportClientConnError("Client write error: %v\n", err)
					// "requestLoop" exits on request read-write errors only, read next request
				}
			default:
				req.setIngressProxy(p)
				if err = req.forwardAndFlush(p); err != nil {
					res.WriteErr = err
					return res
				}
			}
		case rpcClientWantsFinTLTag:
			res.ClientWantsFin = true
			res.WriteErr = req.forwardAndFlush(p)
			return res // graceful shutdown, no more client requests expected
		}
		if err = req.read(p); err != nil {
			p.reportClientConnError("Client read error: %v\n", err)
			res.ReadErr = err
			return res
		}
	}
	return res
}

func (p *proxyConn) responseLoop(ctx context.Context) rpc.ForwardPacketsResult {
	res := rpc.ForwardPackets(ctx, p.clientConn, p.upstreamConn)
	if err := res.Error(); err != nil {
		p.clientConn.ShutdownWrite()
	}
	// do not close client on server shutdown, calling side initiates graceful shutdown
	return res
}

func (p *proxyConn) reportRequestBufferSizeChange() {
	if v := int64(cap(p.reqBuf) - p.reqBufCap); v != 0 {
		p.requestMemory.Add(v)
		p.reqBufCap = cap(p.reqBuf)
	}
}

func (req *proxyRequest) read(p *proxyConn) error {
	var err error
	if req.tip, req.Request, err = p.clientConn.ReadPacket(p.reqBuf[:0], rpc.DefaultPacketTimeout); err != nil {
		return err
	}
	switch p.req.tip {
	case rpcCancelReqTLTag, rpcClientWantsFinTLTag:
		return nil
	case rpcInvokeReqHeaderTLTag:
		if cap(p.reqBuf) < cap(req.Request) {
			p.reqBuf = req.Request // buffer reuse
		}
		requestLen := len(req.Request)
		if err = req.ParseInvokeReq(&p.serverOpts); err != nil {
			return err
		}
		requestTag := req.RequestTag()
		if p.agent.Shards != nil {
			p.reqKey.Tags[2] = int32(requestTag)
			p.agent.AddValueCounter(&p.reqKey, float64(requestLen), 1, format.BuiltinMetricMetaRPCRequests)
		}
		switch requestTag {
		case constants.StatshouseGetConfig2,
			constants.StatshouseGetTagMapping2,
			constants.StatshouseSendKeepAlive2,
			constants.StatshouseSendSourceBucket2,
			constants.StatshouseTestConnection2,
			constants.StatshouseGetTargets2,
			constants.StatshouseGetTagMappingBootstrap,
			constants.StatshouseGetMetrics3,
			constants.StatshouseAutoCreate,
			rpcPingTLTag:
			// pass
			return nil
		default:
			return rpc.ErrNoHandler
		}
	default:
		return fmt.Errorf("unknown packet %d", req.tip)
	}
}

func (req *proxyRequest) forwardAndFlush(p *proxyConn) error {
	if err := req.ForwardAndFlush(p.upstreamConn, req.tip, rpc.DefaultPacketTimeout); err != nil {
		p.rareLog("Upstream write error: %v\n", err)
		return err
	}
	if cap(p.reqBuf) < cap(req.Request) {
		p.reqBuf = req.Request // buffer reuse
	}
	return nil
}

func (req *proxyRequest) setIngressProxy(p *proxyConn) {
	if len(req.Request) < 32 {
		return // test environment
	}
	fieldsMask := binary.LittleEndian.Uint32(req.Request[4:])
	fieldsMask |= (1 << 31) // args.SetIngressProxy(true)
	binary.LittleEndian.PutUint32(req.Request[4:], fieldsMask)
	binary.LittleEndian.PutUint32(req.Request[16:], p.clientAddr[0])
	binary.LittleEndian.PutUint32(req.Request[20:], p.clientAddr[1])
	binary.LittleEndian.PutUint32(req.Request[24:], p.clientAddr[2])
	binary.LittleEndian.PutUint32(req.Request[28:], p.clientAddr[3])
}

func (req *proxyRequest) shardReplica(p *proxyConn) uint32 {
	if len(req.Request) < 12 {
		return 0
	}
	n := binary.LittleEndian.Uint32(req.Request[8:])
	return n % uint32(len(p.agent.GetConfigResult.Addresses))
}
