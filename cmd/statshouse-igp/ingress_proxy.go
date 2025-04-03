// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package main

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
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/vkcom/statshouse/internal/agent"
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/constants"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/pcache"
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
	ListenAddr            string
	ListenAddrIPV6        string
	ExternalAddresses     []string // exactly 3 comma-separated external ingress points
	ExternalAddressesIPv6 []string
	UpstreamAddr          string
	IngressKeys           []string
	ResponseMemoryLimit   int
	Version               string
	ConfigAgent           agent.Config
}

type ingressProxy struct {
	// Place atomics first to ensure proper alignment, see https://pkg.go.dev/sync/atomic#pkg-note-BUG
	uniqueStartTime atomic.Uint32
	hostnameID      atomic.Int32

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

	firstClientConn   map[string]bool
	firstClientConnMu sync.Mutex
}

type proxyServer struct {
	*ingressProxy
	config2   tlstatshouse.GetConfigResult
	config3   tlstatshouse.GetConfigResult3
	network   string
	listeners []net.Listener
}

type proxyConn struct {
	*proxyServer
	clientConn            *rpc.PacketConn
	upstreamConn          *rpc.PacketConn
	clientCryptoKeyID     int32
	clientProtocolVersion int32

	// no synchronization, owned by "requestLoop"
	clientAddr  [4]uint32 // readonly after init
	clientAddrS string    // readonly after init
	reqBuf      []byte
}

type proxyRequest struct {
	tip  uint32
	size int
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

func RunIngressProxy(ctx context.Context, config ConfigIngressProxy, aesPwd string, mappingsCache *pcache.MappingsCache) error {
	p := ingressProxy{
		ctx:             ctx,
		cluster:         config.ConfigAgent.Cluster,
		clientOpts:      rpc.ClientOptions{CryptoKey: aesPwd},
		serverKeys:      config.IngressKeys,
		startTime:       uint32(time.Now().Unix()),
		firstClientConn: make(map[string]bool),
	}
	restart := make(chan int)
	if config.UpstreamAddr != "" {
		addresses := strings.Split(config.UpstreamAddr, ",")
		p.agent = &agent.Agent{GetConfigResult: tlstatshouse.GetConfigResult3{
			Addresses:          addresses,
			ShardByMetricCount: uint32(len(addresses) / 3),
		}}
	} else {
		var err error
		p.agent, err = agent.MakeAgent(
			"tcp", "", aesPwd, config.ConfigAgent, srvfunc.HostnameForStatshouse(),
			format.TagValueIDComponentIngressProxy,
			nil, mappingsCache,
			nil, nil, nil,
			log.Printf,
			func(s *agent.Agent, nowUnix uint32) {
				// __igp_vm_size
				var vmSize, vmRSS float64
				if st, _ := srvfunc.GetMemStat(0); st != nil {
					vmSize = float64(st.Size)
					vmRSS = float64(st.Res)
				}
				tags := []int32{1: p.hostnameID.Load()}
				s.AddValueCounter(nowUnix, format.BuiltinMetricMetaProxyVmSize,
					tags, 1, vmSize)
				// __igp_vm_rss
				s.AddValueCounter(nowUnix, format.BuiltinMetricMetaProxyVmRSS,
					tags, 1, vmRSS)
				// __igp_heap_alloc
				var memStats runtime.MemStats
				runtime.ReadMemStats(&memStats)
				s.AddValueCounter(nowUnix, format.BuiltinMetricMetaProxyHeapAlloc,
					tags, 1, float64(memStats.HeapAlloc))
				// __igp_heap_sys
				s.AddValueCounter(nowUnix, format.BuiltinMetricMetaProxyHeapSys,
					tags, 1, float64(memStats.HeapSys))
				// __igp_heap_idle
				s.AddValueCounter(nowUnix, format.BuiltinMetricMetaProxyHeapIdle,
					tags, 1, float64(memStats.HeapIdle))
				// __igp_heap_inuse
				s.AddValueCounter(nowUnix, format.BuiltinMetricMetaProxyHeapInuse,
					tags, 1, float64(memStats.HeapInuse))
			},
			nil, nil)
		if err != nil {
			log.Fatalf("error creating agent: %v", err)
		}
		go func() {
			p.agent.GoGetConfig() // if terminates, proxy must restart
			close(restart)
		}()
		p.agent.Run(0, 0, 0)
	}
	p.uniqueStartTime.Store(p.startTime)
	rpc.ClientWithTrustedSubnetGroups(build.TrustedSubnetGroups())(&p.clientOpts)
	rpc.ServerWithTrustedSubnetGroups(build.TrustedSubnetGroups())(&p.serverOpts)
	// resolve hostname ID
	p.hostnameID.Store(format.TagValueIDMappingFlood)
	go func() {
		p.hostnameID.Store(p.getHostnameID(aesPwd))
	}()
	// listen on IPv4
	tcp4 := p.newProxyServer("tcp4")
	tcp6 := p.newProxyServer("tcp6")
	shutdown := func() {
		tcp4.shutdown()
		tcp6.shutdown()
	}
	if len(config.ExternalAddresses) != 0 && config.ExternalAddresses[0] != "" {
		err := tcp4.listen(config.ListenAddr, config.ExternalAddresses, config.Version)
		if err != nil {
			shutdown()
			return err
		}
	}
	// listen on IPv6
	defer tcp6.shutdown()
	if len(config.ExternalAddressesIPv6) != 0 && config.ExternalAddressesIPv6[0] != "" {
		err := tcp6.listen(config.ListenAddrIPV6, config.ExternalAddressesIPv6, config.Version)
		if err != nil {
			shutdown()
			return err
		}
	}
	if len(tcp4.listeners) == 0 && len(tcp6.listeners) == 0 {
		return fmt.Errorf("at least one ingress-external-addr must be provided")
	}
	// run
	log.Printf("Running IGPv2, commit %s, PID %d\n", build.Commit(), os.Getpid())
	tcp4.run()
	tcp6.run()
	select {
	case <-ctx.Done():
	case <-restart:
	}
	shutdown()
	p.group.Wait()
	return nil
}

func (p *ingressProxy) newProxyServer(network string) proxyServer {
	return proxyServer{
		ingressProxy: p,
		config2: tlstatshouse.GetConfigResult{
			MaxAddressesCount: int32(len(p.agent.GetConfigResult.Addresses)),
			PreviousAddresses: int32(len(p.agent.GetConfigResult.Addresses)),
		},
		config3: tlstatshouse.GetConfigResult3{
			ShardByMetricCount: p.agent.GetConfigResult.ShardByMetricCount,
		},
		network: network,
	}
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

func (p *ingressProxy) getHostnameID(aesPwd string) int32 {
	if len(p.agent.GetConfigResult.Addresses) == 0 {
		return format.TagValueIDMappingFlood
	}
	client := rpc.NewClient(
		rpc.ClientWithProtocolVersion(rpc.LatestProtocolVersion),
		rpc.ClientWithCryptoKey(aesPwd),
		rpc.ClientWithTrustedSubnetGroups(build.TrustedSubnetGroups()),
		rpc.ClientWithLogf(log.Printf))
	defer client.Close()
	agg := tlstatshouse.Client{
		Client:  client,
		Network: "tcp",
		Address: p.agent.GetConfigResult.Addresses[0],
	}
	args := tlstatshouse.GetTagMapping2{
		Metric: format.BuiltinMetricMetaBudgetAggregatorHost.Name,
		Key:    srvfunc.HostnameForStatshouse(),
		Header: tlstatshouse.CommonProxyHeader{
			ShardReplicaTotal: int32(len(p.agent.GetConfigResult.Addresses)),
			HostName:          srvfunc.HostnameForStatshouse(),
			ComponentTag:      format.TagValueIDComponentIngressProxy,
		},
	}
	args.SetCreate(true)
	args.Header.SetIngressProxy(true, &args.FieldsMask)
	res := tlstatshouse.GetTagMappingResult{}
	if err := agg.GetTagMapping2(p.ctx, args, nil, &res); err != nil {
		return format.TagValueIDMappingFlood
	}
	return res.Value
}

func (p *proxyServer) listen(addr string, externalAddr []string, version string) error {
	if len(p.agent.GetConfigResult.Addresses)%len(externalAddr) != 0 {
		return fmt.Errorf("number of servers must be multiple of number of ingress-external-addr")
	}
	// parse listen address
	listenAddr, err := net.ResolveTCPAddr(p.network, addr)
	if err != nil {
		return err
	}
	// build external address and listen
	if version == "2" {
		externalTCPAddr := make([]*net.TCPAddr, len(externalAddr))
		for i := range externalAddr {
			externalTCPAddr[i], err = net.ResolveTCPAddr(p.network, externalAddr[i])
			if err != nil {
				return err
			}
		}
		p.listeners = make([]net.Listener, len(p.agent.GetConfigResult.Addresses)/len(externalAddr))
		for i := range p.listeners {
			log.Printf("Listen addr %v\n", listenAddr)
			p.listeners[i], err = rpc.Listen(p.network, listenAddr.String(), false)
			if err != nil {
				return err
			}
			listenAddr.Port++
		}
	} else {
		log.Printf("Listen addr %v\n", listenAddr)
		p.listeners = make([]net.Listener, 1)
		p.listeners[0], err = rpc.Listen(p.network, listenAddr.String(), false)
		if err != nil {
			return err
		}
	}
	n := len(p.agent.GetConfigResult.Addresses)
	s := make([]string, 0, n)
	for len(s) < n {
		for i := 0; i < len(externalAddr) && len(s) < n; i++ {
			s = append(s, externalAddr[i])
		}
	}
	p.config2.Addresses = s
	p.config3.Addresses = s
	log.Printf("External %s addr %s\n", p.network, strings.Join(p.config2.Addresses, ", "))
	return nil
}

func (p *proxyServer) run() {
	p.group.Add(len(p.listeners))
	for i := range p.listeners {
		go p.serve(p.listeners[i])
	}
}

func (p *proxyServer) shutdown() {
	for i := range p.listeners {
		if p.listeners[i] != nil {
			_ = p.listeners[i].Close()
		}
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
		log.Printf("accept error: %v", err)
		if p.ctx.Err() != nil {
			return
		}
		// report accept error then backoff
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
	return &proxyConn{
		proxyServer: p,
		clientAddr:  clientAddr,
		clientAddrS: clientAddrS,
		clientConn:  clientConn,
	}
}

func (p *proxyConn) run() {
	defer p.group.Done()
	defer p.clientConn.Close()
	// handshake client
	_, _, err := p.clientConn.HandshakeServer(p.serverKeys, p.serverOpts.TrustedSubnetGroups, true, p.startTime, rpc.DefaultPacketTimeout)
	cryptoKeyID := p.clientConn.KeyID()
	p.clientCryptoKeyID = int32(binary.BigEndian.Uint32(cryptoKeyID[:4]))
	p.clientProtocolVersion = int32(p.clientConn.ProtocolVersion())
	if err != nil {
		p.rareLog("error handshake client addr %s, version %d, key 0x%X: %v\n", p.clientConn.RemoteAddr(), p.clientProtocolVersion, p.clientCryptoKeyID, err)
		return
	}
	// read first request to get shardReplica
	var firstReq proxyRequest
	for {
		firstReq, err = p.readRequest()
		if err != nil {
			return
		}
		if p.ctx.Err() != nil {
			return // server shutdown
		}
		if firstReq.tip == rpcInvokeReqHeaderTLTag {
			if firstReq.RequestTag() == constants.StatshouseGetConfig3 {
				// GetConfig3 does not send shardReplica
				if res := firstReq.process(p); res.Error() != nil {
					return // failed serve GetConfig3 request
				}
				continue
			}
			break
		}
		log.Printf("Client skip #%d looking for invoke request, addr %v\n", firstReq.tip, p.clientConn.RemoteAddr())
	}
	shardReplica := firstReq.shardReplica(p)
	upstreamAddr := p.agent.GetConfigResult.Addresses[shardReplica]
	// connect upstream
	upstreamConn, err := net.DialTimeout("tcp", upstreamAddr, rpc.DefaultPacketTimeout)
	if err != nil {
		log.Printf("error connect upstream addr %s < %s: %v\n", upstreamAddr, p.clientConn.RemoteAddr(), err)
		_ = firstReq.WriteReponseAndFlush(p.clientConn, err)
		return
	}
	defer upstreamConn.Close()
	p.upstreamConn = rpc.NewPacketConn(upstreamConn, rpc.DefaultClientConnReadBufSize, rpc.DefaultClientConnWriteBufSize)
	err = p.upstreamConn.HandshakeClient(p.clientOpts.CryptoKey, p.clientOpts.TrustedSubnetGroups, false, p.uniqueStartTime.Dec(), 0, rpc.DefaultPacketTimeout, rpc.LatestProtocolVersion)
	if err != nil {
		p.logUpstreamError("handshake", err, rpc.PacketHeaderCircularBuffer{})
		_ = firstReq.WriteReponseAndFlush(p.clientConn, err)
		return
	}
	p.logFirstClientConn()
	// process first request
	firstReqRes := firstReq.process(p)
	if firstReqRes.Error() != nil {
		return
	}
	// serve
	var ctx = p.ctx
	var reqLoopRes rpc.ForwardPacketsResult
	var respLoopRes rpc.ForwardPacketsResult
	gracefulShutdown := firstReqRes.ClientWantsFin
	for { // two iterations at most, the latter is graceful shutdown
		var respLoop sync.WaitGroup
		respLoop.Add(1)
		go func() {
			defer respLoop.Done()
			respLoopRes = p.responseLoop(ctx)
		}()
		reqLoopRes = p.requestLoop(ctx)
		respLoop.Wait()
		if !gracefulShutdown {
			gracefulShutdown = reqLoopRes.ClientWantsFin || respLoopRes.ServerWantsFin
		}
		if gracefulShutdown || reqLoopRes.Error() != nil || respLoopRes.Error() != nil {
			break // either graceful shutdown already attempted or error occurred
		}
		gracefulShutdown = true
		if err = p.clientConn.WritePacket(rpcServerWantsFinTLTag, nil, rpc.DefaultPacketTimeout); err != nil {
			p.logClientError("write fin", err, rpc.PacketHeaderCircularBuffer{})
			break
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
	for ctx.Err() == nil {
		req, err := p.readRequest()
		if err != nil {
			res.ReadErr = err
			break
		}
		res = req.process(p)
		if res.Error() != nil || res.ClientWantsFin {
			break
		}
	}
	return res
}

func (p *proxyConn) responseLoop(ctx context.Context) rpc.ForwardPacketsResult {
	res := rpc.ForwardPackets(ctx, p.clientConn, p.upstreamConn)
	if err := res.Error(); err != nil {
		if res.ReadErr != nil {
			p.logUpstreamError("read", res.ReadErr, res.PacketHeaderCircularBuffer)
		}
		if res.WriteErr != nil {
			p.logClientError("write", res.WriteErr, res.PacketHeaderCircularBuffer)
		}
		p.clientConn.ShutdownWrite()
	}
	// do not close client on server shutdown, calling side initiates graceful shutdown
	return res
}

func (p *proxyConn) readRequest() (req proxyRequest, err error) {
	if req.tip, req.Request, err = p.clientConn.ReadPacket(p.reqBuf[:0], rpc.DefaultPacketTimeout); err != nil {
		p.logClientError("read", err, rpc.PacketHeaderCircularBuffer{})
		return proxyRequest{}, err
	}
	req.size = len(req.Request)
	switch req.tip {
	case rpcCancelReqTLTag, rpcClientWantsFinTLTag:
		return req, nil
	case rpcInvokeReqHeaderTLTag:
		if cap(p.reqBuf) < cap(req.Request) {
			p.reqBuf = req.Request // buffer reuse
		}
		if err = req.ParseInvokeReq(&p.serverOpts); err != nil {
			p.logClientError("parse", err, rpc.PacketHeaderCircularBuffer{})
			return proxyRequest{}, err
		}
		switch req.RequestTag() {
		case constants.StatshouseGetConfig2,
			constants.StatshouseGetConfig3,
			constants.StatshouseGetTagMapping2,
			constants.StatshouseSendKeepAlive2,
			constants.StatshouseSendKeepAlive3,
			constants.StatshouseSendSourceBucket2,
			constants.StatshouseSendSourceBucket3,
			constants.StatshouseTestConnection2,
			constants.StatshouseGetTargets2,
			constants.StatshouseGetTagMappingBootstrap,
			constants.StatshouseGetMetrics3,
			constants.StatshouseAutoCreate,
			rpcPingTLTag:
			// pass
			return req, nil
		default:
			p.logClientError("not supported request", err, rpc.PacketHeaderCircularBuffer{})
			return proxyRequest{}, rpc.ErrNoHandler
		}
	default:
		p.logClientError("not supported packet", err, rpc.PacketHeaderCircularBuffer{})
		return proxyRequest{}, rpc.ErrNoHandler
	}
}

func (p *proxyConn) reportRequestSize(req *proxyRequest) {
	if p.agent.Shards == nil {
		return
	}
	p.agent.AddValueCounter(uint32(time.Now().Unix()), format.BuiltinMetricMetaRPCRequests,
		[]int32{
			1: format.TagValueIDComponentIngressProxy,
			2: int32(req.tag()),
			6: p.clientCryptoKeyID,
			8: p.clientProtocolVersion,
		}, float64(req.size), 1)
}

func (p *proxyConn) logFirstClientConn() {
	p.firstClientConnMu.Lock()
	defer p.firstClientConnMu.Unlock()
	if !p.firstClientConn[p.clientAddrS] {
		log.Println("First connection from", p.clientAddrS)
		p.firstClientConn[p.clientAddrS] = true
	}
}

func (p *proxyConn) logClientError(tag string, err error, lastPackets rpc.PacketHeaderCircularBuffer) {
	if err == nil || errors.Is(err, io.EOF) {
		return
	}
	var addr string
	var encrypted bool
	if p.clientConn != nil {
		addr = p.clientConn.RemoteAddr()
		encrypted = p.clientConn.Encrypted()
	}
	log.Printf("error %s, client addr %s, version %d, encrypted %t, key 0x%X: %v, %s\n", tag, addr, p.clientProtocolVersion, encrypted, p.clientCryptoKeyID, err, lastPackets.String())
}

func (p *proxyConn) logUpstreamError(tag string, err error, lastPackets rpc.PacketHeaderCircularBuffer) {
	if err == nil || errors.Is(err, io.EOF) {
		return
	}
	var addr string
	var version uint32
	var encrypted bool
	if p.upstreamConn != nil {
		addr = p.upstreamConn.RemoteAddr()
		version = p.upstreamConn.ProtocolVersion()
		encrypted = p.upstreamConn.Encrypted()
	}
	log.Printf("error %s, upstream addr %s, version %d, encrypted %t, %v, %s\n", tag, addr, version, encrypted, err, lastPackets.String())
}

func (req *proxyRequest) process(p *proxyConn) (res rpc.ForwardPacketsResult) {
	var err error
	switch req.tip {
	case rpcInvokeReqHeaderTLTag:
		switch req.RequestTag() {
		case constants.StatshouseGetConfig2:
			var autoConfigStatus int32
			var args tlstatshouse.GetConfig2
			if _, err = args.ReadBoxed(req.Request); err == nil {
				if args.Cluster != p.cluster {
					err = fmt.Errorf("statshouse misconfiguration! cluster requested %q does not match actual cluster connected %q", args.Cluster, p.cluster)
					p.logClientError("GetConfig2", err, rpc.PacketHeaderCircularBuffer{})
					autoConfigStatus = format.TagValueIDAutoConfigWrongCluster
				} else {
					req.Response, _ = args.WriteResult(req.Response[:0], p.config2)
					autoConfigStatus = format.TagValueIDAutoConfigOK
				}
				p.sendAutoConfigStatus(&args.Header, autoConfigStatus)
			}
			if err = req.WriteReponseAndFlush(p.clientConn, err); err != nil {
				p.logClientError("write", err, rpc.PacketHeaderCircularBuffer{})
				// not an error ("requestLoop" exits on request read-write errors only)
			}
		case constants.StatshouseGetConfig3:
			var autoConfigStatus int32
			var args tlstatshouse.GetConfig3
			if _, err = args.ReadBoxed(req.Request); err == nil {
				if args.Cluster != p.cluster {
					err = fmt.Errorf("statshouse misconfiguration! cluster requested %q does not match actual cluster connected %q", args.Cluster, p.cluster)
					p.logClientError("GetConfig3", err, rpc.PacketHeaderCircularBuffer{})
					autoConfigStatus = format.TagValueIDAutoConfigWrongCluster
				} else {
					equalConfig := args.IsSetPreviousConfig() &&
						slices.Equal(p.config3.Addresses, args.PreviousConfig.Addresses) &&
						p.config3.ShardByMetricCount == args.PreviousConfig.ShardByMetricCount
					if equalConfig {
						autoConfigStatus = format.TagValueIDAutoConfigErrorKeepAlive
					} else {
						req.Response, _ = args.WriteResult(req.Response[:0], p.config3)
						autoConfigStatus = format.TagValueIDAutoConfigOK
					}
				}
				p.sendAutoConfigStatus(&args.Header, autoConfigStatus)
			}
			if autoConfigStatus == format.TagValueIDAutoConfigOK || err != nil {
				if err = req.WriteReponseAndFlush(p.clientConn, err); err != nil {
					p.logClientError("write", err, rpc.PacketHeaderCircularBuffer{})
					// not an error ("requestLoop" exits on request read-write errors only)
				}
			}
		default:
			req.setIngressProxy(p)
			if err = req.forwardAndFlush(p); err != nil {
				p.logUpstreamError("forward InvokeReq", err, rpc.PacketHeaderCircularBuffer{})
				res.WriteErr = err
			}
		}
	case rpcClientWantsFinTLTag:
		res.ClientWantsFin = true
		if err = req.forwardAndFlush(p); err != nil {
			res.WriteErr = err
			p.logUpstreamError("forward ClientWantsFin", err, rpc.PacketHeaderCircularBuffer{})
		}
		// graceful shutdown, no more client requests expected
	}
	p.reportRequestSize(req)
	return res
}

func (req *proxyRequest) forwardAndFlush(p *proxyConn) error {
	if err := req.ForwardAndFlush(p.upstreamConn, req.tip, rpc.DefaultPacketTimeout); err != nil {
		p.logUpstreamError("write", err, rpc.PacketHeaderCircularBuffer{})
		return err
	}
	if cap(p.reqBuf) < cap(req.Request) {
		p.reqBuf = req.Request // buffer reuse
	}
	return nil
}

func (p *ingressProxy) sendAutoConfigStatus(h *tlstatshouse.CommonProxyHeader, status int32) {
	p.agent.AddCounterHostAERA(
		uint32(time.Now().Unix()),
		format.BuiltinMetricMetaAutoConfig,
		[]int32{0, 0, 0, 0, status},
		1, // count
		data_model.TagUnionBytes{I: p.hostnameID.Load()},
		format.AgentEnvRouteArch{
			AgentEnv:  format.TagValueIDProduction,
			Route:     format.TagValueIDRouteIngressProxy,
			BuildArch: format.FilterBuildArch(h.BuildArch),
		})

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
	return n % uint32(len(p.agent.GetConfigResult.Addresses)) // if configured number is wrong, request will end on wrong aggregator returning error and writing metric
}

func (r *proxyRequest) tag() uint32 {
	switch r.tip {
	case rpcInvokeReqHeaderTLTag:
		return r.RequestTag()
	default:
		return r.tip
	}
}
