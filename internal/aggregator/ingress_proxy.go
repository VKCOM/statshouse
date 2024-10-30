// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package aggregator

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.uber.org/atomic"

	"github.com/vkcom/statshouse/internal/agent"
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/constants"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/util"
	"github.com/vkcom/statshouse/internal/vkgo/basictl"
	"github.com/vkcom/statshouse/internal/vkgo/build"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
)

type clientPool struct {
	aesPwd string
	mu     sync.RWMutex
	// shardReplica -> free clients
	clients map[string]*rpc.Client
}

type longpollClient struct {
	queryID    int64
	requestLen int
}

type longpollShard struct {
	proxy      *IngressProxy
	mu         sync.Mutex
	clientList map[*rpc.HandlerContext]longpollClient
}

const longPollShardsCount = 256 // we want to shard lock to reduce contention

type IngressProxy struct {
	nextShardLock atomic.Uint64 // round-robin for lock shards must be good

	sh2    *agent.Agent
	pool   *clientPool
	server *rpc.Server
	config ConfigIngressProxy

	longpollShards [longPollShardsCount]*longpollShard
}

type ConfigIngressProxy struct {
	Cluster               string
	Network               string
	ListenAddr            string
	ListenAddrIPV6        string
	ExternalAddresses     []string // exactly 3 comma-separated external ingress points
	ExternalAddressesIPv6 []string
	IngressKeys           []string
	ResponseMemoryLimit   int
}

func newClientPool(aesPwd string) *clientPool {
	cl := &clientPool{
		aesPwd:  aesPwd,
		clients: map[string]*rpc.Client{},
	}
	return cl
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

func RunIngressProxy(ln net.Listener, sh2 *agent.Agent, aesPwd string, config ConfigIngressProxy) error {
	// Now we configure our clients using repetition of 3 ingress proxy addresses per shard
	extAddr := config.ExternalAddresses
	for i := 1; i < len(sh2.GetConfigResult.Addresses)/3; i++ { // GetConfig returns only non-empty list divisible by 3
		config.ExternalAddresses = append(config.ExternalAddresses, extAddr...)
	}
	proxy := &IngressProxy{
		sh2:  sh2,
		pool: newClientPool(aesPwd),
		// TODO - server settings must be tuned
		config: config,
	}
	for i := 0; i < longPollShardsCount; i++ {
		proxy.longpollShards[i] = &longpollShard{
			proxy:      proxy,
			clientList: map[*rpc.HandlerContext]longpollClient{},
		}
	}
	metrics := util.NewRPCServerMetrics("statshouse_proxy")
	options := []rpc.ServerOptionsFunc{
		rpc.ServerWithCryptoKeys(config.IngressKeys),
		rpc.ServerWithHandler(proxy.handler),
		rpc.ServerWithSyncHandler(proxy.syncHandler),
		rpc.ServerWithForceEncryption(true),
		rpc.ServerWithLogf(log.Printf),
		rpc.ServerWithDisableContextTimeout(true),
		rpc.ServerWithTrustedSubnetGroups(build.TrustedSubnetGroups()),
		rpc.ServerWithVersion(build.Info()),
		rpc.ServerWithDefaultResponseTimeout(data_model.MaxConveyorDelay * time.Second),
		rpc.ServerWithMaxInflightPackets(aggregatorMaxInflightPackets * 100), // enough for up to 100 shards
		rpc.ServerWithResponseBufSize(1024),
		rpc.ServerWithResponseMemEstimate(1024),
		rpc.ServerWithRequestMemoryLimit(8 << 30), // see server settings in aggregator. We do not multiply here
		rpc.ServerWithResponseMemoryLimit(config.ResponseMemoryLimit),
		metrics.ServerWithMetrics,
	}
	proxy.server = rpc.NewServer(options...)
	defer metrics.Run(proxy.server)()
	log.Printf("Running ingress proxy listening %s with %d crypto keys", ln.Addr(), len(config.IngressKeys))
	return proxy.server.Serve(ln)
}

func keyFromHctx(hctx *rpc.HandlerContext, resultTag int32) (data_model.Key, *format.MetricMetaValue) {
	keyID := hctx.KeyID()
	keyIDTag := int32(binary.BigEndian.Uint32(keyID[:4]))
	protocol := int32(hctx.ProtocolVersion())
	return data_model.Key{
		Metric: format.BuiltinMetricIDRPCRequests,
		Tags:   [16]int32{0, format.TagValueIDComponentIngressProxy, int32(hctx.RequestTag()), resultTag, 0, 0, keyIDTag, 0, protocol},
	}, format.BuiltinMetricMetaRPCRequests
}

func (ls *longpollShard) callback(client *rpc.Client, resp *rpc.Response, err error) {
	defer client.PutResponse(resp)
	userData := resp.UserData()
	hctx := userData.(*rpc.HandlerContext)
	ls.mu.Lock()
	defer ls.mu.Unlock()
	lpc, ok := ls.clientList[hctx]
	queryID := resp.QueryID()
	if !ok || lpc.queryID != queryID {
		// server already cancelled longpoll call
		// or hctx was cancelled and reused by server before client response arrived
		// since we have no client cancellation, we rely on fact that client queryId does not repeat often
		return
	}
	delete(ls.clientList, hctx)
	var key data_model.Key
	var meta *format.MetricMetaValue
	if err != nil {
		key, meta = keyFromHctx(hctx, format.TagValueIDRPCRequestsStatusErrUpstream)
	} else {
		key, meta = keyFromHctx(hctx, format.TagValueIDRPCRequestsStatusOK)
	}
	ls.proxy.sh2.AddValueCounter(key, float64(lpc.requestLen), 1, meta)
	if resp != nil {
		hctx.Response = append(hctx.Response, resp.Body...)
	}
	hctx.SendHijackedResponse(err)
}

func (ls *longpollShard) CancelHijack(hctx *rpc.HandlerContext) {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	if lpc, ok := ls.clientList[hctx]; ok {
		key, meta := keyFromHctx(hctx, format.TagValueIDRPCRequestsStatusErrCancel)
		ls.proxy.sh2.AddValueCounter(key, float64(lpc.requestLen), 1, meta)
	}
	delete(ls.clientList, hctx)
}

func (proxy *IngressProxy) syncHandler(ctx context.Context, hctx *rpc.HandlerContext) error {
	requestLen := len(hctx.Request)
	resultTag, err := proxy.syncHandlerImpl(ctx, hctx)
	if resultTag != 0 {
		key, meta := keyFromHctx(hctx, resultTag)
		proxy.sh2.AddValueCounter(key, float64(requestLen), 1, meta)
	}
	return err
}

func (proxy *IngressProxy) syncHandlerImpl(ctx context.Context, hctx *rpc.HandlerContext) (resultTag int32, err error) {
	requestLen := len(hctx.Request)
	switch hctx.RequestTag() {
	case constants.StatshouseGetTagMapping2:
		hctx.RequestFunctionName = "statshouse.getTagMapping2"
	case constants.StatshouseSendKeepAlive2:
		hctx.RequestFunctionName = "statshouse.sendKeepAlive2"
	case constants.StatshouseSendSourceBucket2:
		hctx.RequestFunctionName = "statshouse.sendSourceBucket2"
	case constants.StatshouseTestConnection2:
		hctx.RequestFunctionName = "statshouse.testConnection2"
	case constants.StatshouseGetTargets2:
		hctx.RequestFunctionName = "statshouse.getTargets2"
	case constants.StatshouseGetTagMappingBootstrap:
		hctx.RequestFunctionName = "statshouse.getTagMappingBootstrap"
	case constants.StatshouseGetMetrics3:
		hctx.RequestFunctionName = "statshouse.getMetrics3"
	case constants.StatshouseAutoCreate:
		hctx.RequestFunctionName = "statshouse.autoCreate"
	case constants.StatshouseGetConfig2:
		hctx.RequestFunctionName = "statshouse.getConfig2"
		return 0, rpc.ErrNoHandler // call SyncHandler in worker
	default:
		// we want fast reject of unknown requests in sync handler
		return format.TagValueIDRPCRequestsStatusNoHandler, fmt.Errorf("ingress proxy does not support tag 0x%x", hctx.RequestTag())
	}
	req, client, address, err := proxy.fillProxyRequest(hctx)
	if err != nil {
		return format.TagValueIDRPCRequestsStatusErrLocal, err
	}
	queryID := req.QueryID()
	lockShardID := int(proxy.nextShardLock.Inc() % longPollShardsCount)
	ls := proxy.longpollShards[lockShardID]
	ls.mu.Lock() // to avoid race with longpoll cancellation, all code below must run under lock
	defer ls.mu.Unlock()
	if _, err := client.DoCallback(ctx, proxy.config.Network, address, req, ls.callback, hctx); err != nil {
		return format.TagValueIDRPCRequestsStatusErrLocal, err
	}
	ls.clientList[hctx] = longpollClient{queryID: queryID, requestLen: requestLen}
	return 0, hctx.HijackResponse(ls)
}

func (proxy *IngressProxy) handler(ctx context.Context, hctx *rpc.HandlerContext) error {
	requestLen := len(hctx.Request)
	resultTag, err := proxy.handlerImpl(ctx, hctx)
	key, meta := keyFromHctx(hctx, resultTag)
	proxy.sh2.AddValueCounter(key, float64(requestLen), 1, meta)
	return err
}

func (proxy *IngressProxy) handlerImpl(ctx context.Context, hctx *rpc.HandlerContext) (resultTag int32, err error) {
	switch hctx.RequestTag() {
	case constants.StatshouseGetConfig2:
		// Record metrics on aggregator with correct host, IP, etc.
		// We do not care if it succeeded or not, we make our own response anyway
		_, _ = proxy.syncProxyRequest(ctx, hctx)

		var args tlstatshouse.GetConfig2
		var ret tlstatshouse.GetConfigResult
		_, err = args.ReadBoxed(hctx.Request)
		if err != nil {
			return format.TagValueIDRPCRequestsStatusErrLocal, fmt.Errorf("failed to deserialize statshouse.getConfig2 request: %w", err)
		}
		if args.Cluster != proxy.config.Cluster {
			return format.TagValueIDRPCRequestsStatusErrLocal, fmt.Errorf("statshouse misconfiguration! cluster requested %q does not match actual cluster connected %q", args.Cluster, proxy.config.Cluster)
		}
		ret.Addresses = proxy.config.ExternalAddresses
		ret.MaxAddressesCount = proxy.sh2.GetConfigResult.MaxAddressesCount
		ret.PreviousAddresses = proxy.sh2.GetConfigResult.PreviousAddresses
		hctx.Response, _ = args.WriteResult(hctx.Response[:0], ret)
		return format.TagValueIDRPCRequestsStatusOK, nil
	default:
		return format.TagValueIDRPCRequestsStatusNoHandler, fmt.Errorf("ingress proxy does not support tag 0x%x", hctx.RequestTag())
	}
}

func (proxy *IngressProxy) fillProxyRequest(hctx *rpc.HandlerContext) (request *rpc.Request, client *rpc.Client, address string, err error) {
	if len(hctx.Request) < 32 {
		return nil, nil, "", fmt.Errorf("ingress proxy query with tag 0x%x is too short - %d bytes", hctx.RequestTag(), len(hctx.Request))
	}
	addrIPV4, _ := addrIPString(hctx.RemoteAddr())

	fieldsMask := binary.LittleEndian.Uint32(hctx.Request[4:])
	shardReplica := binary.LittleEndian.Uint32(hctx.Request[8:])
	fieldsMask |= (1 << 31) // args.SetIngressProxy(true)
	binary.LittleEndian.PutUint32(hctx.Request[4:], fieldsMask)
	binary.LittleEndian.PutUint32(hctx.Request[28:], addrIPV4) // source_ip[3] in header. TODO - ipv6
	// We override this field if set by previous proxy. Because we do not care about agent IPs in their cuber/internal networks
	hostName, err := parseHostname(hctx.Request)
	if err != nil {
		return nil, nil, "", err
	}
	// Motivation of % len - we pass through badly configured requests for now, so aggregators will record them in builtin metric
	shardReplicaIx := shardReplica % uint32(len(proxy.sh2.GetConfigResult.Addresses))
	address = proxy.sh2.GetConfigResult.Addresses[shardReplicaIx]

	client = proxy.pool.getClient(hostName, hctx.RemoteAddr().String())
	req := client.GetRequest()
	req.Body = append(req.Body, hctx.Request...)
	req.FailIfNoConnection = true
	return req, client, address, nil
}

func (proxy *IngressProxy) syncProxyRequest(ctx context.Context, hctx *rpc.HandlerContext) (resultTag int32, err error) {
	req, client, address, err := proxy.fillProxyRequest(hctx)
	if err != nil {
		return format.TagValueIDRPCRequestsStatusErrLocal, err
	}

	resp, err := client.Do(ctx, proxy.config.Network, address, req)
	defer client.PutResponse(resp)
	if err != nil {
		return format.TagValueIDRPCRequestsStatusErrUpstream, err
	}

	hctx.Response = append(hctx.Response, resp.Body...)

	return format.TagValueIDRPCRequestsStatusOK, nil
}

func parseHostname(req []byte) (clientHost string, _ error) {
	_, err := basictl.StringRead(req[32:], &clientHost)
	return clientHost, err
}

func (pool *clientPool) getClient(clientHost, remoteAddress string) *rpc.Client {
	var client *rpc.Client
	pool.mu.RLock()
	client = pool.clients[clientHost]
	pool.mu.RUnlock()
	if client != nil {
		return client
	}
	pool.mu.Lock()
	defer pool.mu.Unlock()
	client = pool.clients[clientHost]
	if client != nil {
		return client
	}
	log.Printf("First connection from agent host: %s, host IP: %s", clientHost, remoteAddress)
	client = rpc.NewClient(
		rpc.ClientWithProtocolVersion(rpc.LatestProtocolVersion),
		rpc.ClientWithLogf(log.Printf),
		rpc.ClientWithCryptoKey(pool.aesPwd),
		rpc.ClientWithTrustedSubnetGroups(build.TrustedSubnetGroups()))
	pool.clients[clientHost] = client
	return client
}
