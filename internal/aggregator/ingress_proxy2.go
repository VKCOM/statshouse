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
	"github.com/vkcom/statshouse/internal/vkgo/semaphore"
	"go.uber.org/atomic"
)

const (
	minAcceptDelay  = 5 * time.Millisecond
	maxAcceptDelay  = 1 * time.Second
	maxConns        = rpc.DefaultMaxConns
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
	listenersG   sync.WaitGroup
	connSem      semaphore.Weighted
	shutdownCtx  context.Context
	shutdownFunc func()
	startTime    uint32
	rareLogLast  time.Time
	rareLogMu    sync.Mutex

	commonMetricTags      statshouse.Tags
	сonnectionCountMetric *statshouse.MetricRef
	requestMemoryMetric   *statshouse.MetricRef
	responseMemoryMetric  *statshouse.MetricRef
	regularMeasurementID  int
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
		connSem:    *semaphore.NewWeighted(maxConns),
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
		externalAddresses[i], err = net.ResolveTCPAddr(config.Network, config.ListenAddr)
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
	p.listenersG.Add(len(p.listeners))
	for i := range p.listeners {
		go p.listenAndServe(p.listeners[i])
	}
}

func (p *ingressProxy2) Shutdown() {
	n, _ := p.connSem.Observe()
	log.Printf("Shutdown %v connection(s)\n", n)
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
	p.listenersG.Wait()
	err := p.connSem.Acquire(ctx, maxConns)
	if err != nil {
		// reading from both client and upstream is done with infinite timeout, checking shutdown flag when possible
		n, _ := p.connSem.Observe()
		log.Printf("Shutdown failed to complete in %v, %d connection(s) left\n", timeout, n)
	}
	return err
}

func (p *ingressProxy2) listenAndServe(ln net.Listener) {
	defer p.listenersG.Done()
	var acceptDelay time.Duration
	for {
		if err := p.connSem.Acquire(p.shutdownCtx, 1); err != nil {
			panic(err) // should never happen
		}
		clientConn, err := ln.Accept()
		if err == nil {
			acceptDelay = 0
			p.сonnectionCount.Inc()
			go p.serve(clientConn)
			continue
		}
		p.connSem.Release(1)
		if p.shutdownCtx.Err() != nil {
			return
		}
		// report failure
		tags := p.commonMetricTags
		tags[4] = rpc.ErrorTag(err)
		statshouse.Metric("common_rpc_server_accept_error", tags).Count(1)
		// backoff
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

func (p *ingressProxy2) serve(c net.Conn) {
	defer p.connSem.Release(1)
	defer p.сonnectionCount.Dec()
	client := rpc.NewPacketConn(c, rpc.DefaultServerRequestBufSize, rpc.DefaultServerResponseBufSize)
	defer client.Close()
	// handshake client
	_, _, err := client.HandshakeServer(p.serverKeys, p.serverOpts.TrustedSubnetGroups, true, p.startTime, rpc.DefaultPacketTimeout)
	if err != nil {
		p.rareLog("Server handshake failed: %v", err)
		return
	}
	// read first request to get shardReplica
	clientAddr, _ := addrIPString(c.RemoteAddr())
	var req rpc.ServerRequest
	var tip uint32
	var buf []byte
	for {
		if req, tip, buf, err = p.requestRead(client, clientAddr, buf[:0]); err != nil {
			return
		}
		if p.shutdownCtx.Err() != nil {
			return
		}
		if tip == rpcInvokeReqHeaderTLTag {
			break
		}
	}
	// connect upstream
	shardReplica := binary.LittleEndian.Uint32(req.Request[8:])
	shardReplica %= uint32(len(p.GetConfigResult.Addresses))
	shardReplicaAddr := p.GetConfigResult.Addresses[shardReplica]
	upstreamConn, err := net.DialTimeout("tcp4", shardReplicaAddr, rpc.DefaultPacketTimeout)
	if err != nil {
		p.rareLog("Client connect failed: %v", err)
		return
	}
	// handshake upstream
	upstream := rpc.NewPacketConn(upstreamConn, rpc.DefaultClientConnReadBufSize, rpc.DefaultClientConnWriteBufSize)
	defer upstream.Close()
	err = upstream.HandshakeClient(p.clientOpts.CryptoKey, p.clientOpts.TrustedSubnetGroups, false, p.uniqueStartTime.Dec(), 0, rpc.DefaultPacketTimeout, rpc.LatestProtocolVersion)
	if err != nil {
		p.rareLog("Client handshake failed: %v", err)
		return
	}
	// all good, run
	p.rareLog("Connect shard replica %d, addr %v < %v < %v", shardReplica, upstream.RemoteAddr(), client.LocalAddr(), client.RemoteAddr())
	var respRWLoop sync.WaitGroup
	var respReadErr, respWriteErr error
	respRWLoop.Add(1)
	go func() {
		defer respRWLoop.Done()
		respReadErr, respWriteErr = p.responseRWLoop(upstream, client)
	}()
	reqReadErr, reqWriteErr := p.requestRWLoop(upstream, client, clientAddr, req, tip, buf)
	// request stream closed, wait for response stream completion
	respRWLoop.Wait()
	p.rareLog("Disconnect shard replica %d, addr %v < %v < %v: %v, %v, %v, %v", shardReplica, upstream.RemoteAddr(), client.LocalAddr(), client.RemoteAddr(), reqReadErr, reqWriteErr, respReadErr, respWriteErr)
}

func (p *ingressProxy2) requestRWLoop(upstream, client *rpc.PacketConn, clientAddr uint32, req rpc.ServerRequest, tip uint32, buf []byte) (readErr, writeErr error) {
	var bufCap int
	defer func() {
		p.requestMemory.Sub(int64(bufCap))
	}()
	cryptoKeyID := client.KeyID()
	metric := data_model.Key{
		Metric: format.BuiltinMetricIDRPCRequests,
		Keys:   [16]int32{0, format.TagValueIDComponentIngressProxy, int32(req.RequestTag()), 0, 0, 0, int32(binary.BigEndian.Uint32(cryptoKeyID[:4])), 0, int32(client.ProtocolVersion())},
	}
	for p.shutdownCtx.Err() == nil {
		// report request memory size change
		p.AddValueCounter(metric, float64(len(req.Request)), 1, format.BuiltinMetricMetaRPCRequests)
		if v := int64(cap(buf) - bufCap); v != 0 {
			p.requestMemory.Add(v)
			bufCap = cap(buf)
		}
		// write request
		if writeErr = req.ForwardAndFlush(upstream, tip, rpc.DefaultPacketTimeout); writeErr != nil {
			if err := req.WriteReponseAndFlush(client, writeErr, p.rareLog); err != nil {
				p.reportServerConnError(err)
			}
			break
		}
		// read next request
		if req, tip, buf, readErr = p.requestRead(client, clientAddr, buf[:0]); readErr != nil {
			break
		}
		// update request tag
		metric.Keys[2] = int32(req.RequestTag())
	}
	p.shutdownProxyConn(upstream, client)
	// respond to all remaining requests with an error
	for {
		var err error
		if req, _, buf, err = p.requestRead(client, clientAddr, buf[:0]); err != nil {
			break
		}
		if err = req.WriteReponseAndFlush(client, errProxyConnShutdown, p.rareLog); err != nil {
			break
		}
	}
	return readErr, writeErr
}

func (p *ingressProxy2) responseRWLoop(upstream, client *rpc.PacketConn) (readErr, writeErr error) {
	defer p.shutdownProxyConn(upstream, client)
	var buf []byte
	var bufCap int
	defer func() {
		p.responseMemory.Sub(int64(bufCap))
	}()
	for p.shutdownCtx.Err() == nil {
		// read response
		var tip uint32
		if tip, buf, readErr = upstream.ReadPacket(buf[:0], time.Duration(0)); readErr != nil {
			return
		}
		// write response
		if writeErr = client.WritePacket(tip, buf, rpc.DefaultPacketTimeout); writeErr != nil {
			p.reportServerConnError(writeErr)
			return
		}
		// report response memory size change
		if v := int64(cap(buf) - bufCap); v != 0 {
			p.responseMemory.Add(v)
			bufCap = cap(buf)
		}
	}
	return readErr, writeErr
}

func (p *ingressProxy2) requestRead(client *rpc.PacketConn, clientAddr uint32, buf []byte) (rpc.ServerRequest, uint32, []byte, error) {
	for {
		var tip uint32
		var req rpc.ServerRequest
		var err error
		tip, req.Request, err = client.ReadPacket(buf, time.Duration(0))
		if err != nil {
			return req, tip, req.Request, err
		}
		switch tip {
		case rpcCancelReqTLTag:
			return req, tip, req.Request, err
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
					binary.LittleEndian.PutUint32(req.Request[28:], clientAddr)
					return req, tip, req.Response, nil
				default:
					err = rpc.ErrNoHandler
				}
			}
		default:
			err = fmt.Errorf("unknown packet %d", tip)
		}
		// at this point either len(req.Response) != 0 or err != nil
		if err = req.WriteReponseAndFlush(client, err, p.rareLog); err != nil {
			p.rareLog("Request write failed: %v", err)
			p.reportServerConnError(err)
			return req, tip, req.Response, err
		}
		buf = req.Response[:0] // buffer reuse
	}
}

func (p *ingressProxy2) shutdownProxyConn(upstream, client *rpc.PacketConn) {
	_ = upstream.ShutdownWrite()
	_ = client.WritePacket(rpcServerWantsFinTLTag, nil, rpc.DefaultPacketTimeout)
}

func (p *ingressProxy2) reportServerConnError(err error) {
	if err == nil || errors.Is(err, io.EOF) {
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
