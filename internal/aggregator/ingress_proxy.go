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
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/vkcom/statshouse/internal/vkgo/basictl"
	"github.com/vkcom/statshouse/internal/vkgo/build"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"

	"github.com/vkcom/statshouse/internal/agent"
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/constants"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
)

type clientPool struct {
	aesPwd string
	mu     sync.RWMutex
	// shardReplica -> free clients
	clients map[string]*rpc.Client
}

type IngressProxy struct {
	sh2    *agent.Agent
	pool   *clientPool
	server *rpc.Server
	config ConfigIngressProxy
}

type ConfigIngressProxy struct {
	Cluster           string
	Network           string
	ListenAddr        string
	ExternalAddresses []string // exactly 3 comma-separated external ingress points
	IngressKeys       []string
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

func RunIngressProxy(sh2 *agent.Agent, aesPwd string, config ConfigIngressProxy) error {
	if len(config.IngressKeys) == 0 {
		return fmt.Errorf("ingress proxy must have non-empty list of ingress crypto keys")
	}
	if len(config.ExternalAddresses)%3 != 0 || len(config.ExternalAddresses) == 0 {
		return fmt.Errorf("--ingress-external-addr must contain exactly 3 comma-separated addresses of ingress proxies, contains '%q'", strings.Join(config.ExternalAddresses, ","))
	}
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
	proxy.server = rpc.NewServer(rpc.ServerWithCryptoKeys(config.IngressKeys),
		rpc.ServerWithHandler(proxy.handler),
		rpc.ServerWithForceEncryption(true),
		rpc.ServerWithLogf(log.Printf),
		rpc.ServerWithDisableContextTimeout(true),
		rpc.ServerWithTrustedSubnetGroups(build.TrustedSubnetGroups()),
		rpc.ServerWithVersion(build.Info()),
		rpc.ServerWithDefaultResponseTimeout(data_model.MaxConveyorDelay*time.Second),
		rpc.ServerWithMaxInflightPackets((data_model.MaxConveyorDelay+data_model.MaxHistorySendStreams)*3*100000), // see server settings in aggregator
		rpc.ServerWithMaxWorkers(128<<13),
		rpc.ServerWithResponseBufSize(1024),
		rpc.ServerWithResponseMemEstimate(1024),
		rpc.ServerWithRequestMemoryLimit(8<<30)) // see server settings in aggregator. We do not multiply here

	log.Printf("Running ingress proxy listening %s with %d crypto keys", config.ListenAddr, len(config.IngressKeys))
	return proxy.server.ListenAndServe("tcp", config.ListenAddr)
}

func (proxy *IngressProxy) handler(ctx context.Context, hctx *rpc.HandlerContext) error {
	tag, _ := basictl.NatPeekTag(hctx.Request)
	keyID := hctx.KeyID()
	keyIDTag := int32(binary.BigEndian.Uint32(keyID[:4]))
	protocol := int32(hctx.ProtocolVersion())
	key := data_model.Key{
		Metric: format.BuiltinMetricIDRPCRequests,
		Keys:   [16]int32{0, format.TagValueIDComponentIngressProxy, int32(tag), format.TagValueIDRPCRequestsStatusOK, 0, 0, keyIDTag, 0, protocol},
	}
	isLocal, err := proxy.handlerImpl(ctx, hctx)
	if err != nil && isLocal {
		key.Keys[3] = format.TagValueIDRPCRequestsStatusErrLocal
	}
	if err != nil && !isLocal {
		key.Keys[3] = format.TagValueIDRPCRequestsStatusErrUpstream
	}
	proxy.sh2.AddValueCounter(key, float64(len(hctx.Request)), 1, nil)
	return err
}

func (proxy *IngressProxy) handlerImpl(ctx context.Context, hctx *rpc.HandlerContext) (isLocal bool, err error) {
	tag, _ := basictl.NatPeekTag(hctx.Request)
	switch tag {
	case constants.StatshouseGetConfig2:
		// Record metrics on aggregator with correct host, IP, etc.
		// We do not care if it succeeded or not, we make our own response anyway
		_, _ = proxy.proxyRequest(tag, ctx, hctx)

		var args tlstatshouse.GetConfig2
		var ret tlstatshouse.GetConfigResult
		_, err := args.ReadBoxed(hctx.Request)
		if err != nil {
			return true, fmt.Errorf("failed to deserialize statshouse.getConfig2 request: %w", err)
		}
		if args.Cluster != proxy.config.Cluster {
			return true, fmt.Errorf("statshouse misconfiguration! cluster requested %q does not match actual cluster connected %q", args.Cluster, proxy.config.Cluster)
		}
		ret.Addresses = proxy.config.ExternalAddresses
		ret.MaxAddressesCount = proxy.sh2.GetConfigResult.MaxAddressesCount
		ret.PreviousAddresses = proxy.sh2.GetConfigResult.PreviousAddresses
		hctx.Response, err = args.WriteResult(hctx.Response[:0], ret)
		return true, err
	case constants.StatshouseGetTagMapping2,
		constants.StatshouseSendKeepAlive2, constants.StatshouseSendSourceBucket2,
		constants.StatshouseTestConnection2, constants.StatshouseGetTargets2,
		constants.StatshouseGetTagMappingBootstrap, constants.StatshouseGetMetrics3,
		constants.StatshouseAutoCreate:
		return proxy.proxyRequest(tag, ctx, hctx)
	default:
		return true, fmt.Errorf("ingress proxy does not support tag 0x%x", tag)
	}
}

func (proxy *IngressProxy) proxyRequest(tag uint32, ctx context.Context, hctx *rpc.HandlerContext) (isLocal bool, err error) {
	if len(hctx.Request) < 32 {
		return true, fmt.Errorf("ingress proxy query with tag 0x%x is too short - %d bytes", tag, len(hctx.Request))
	}
	addrIPV4, remoteAddress := addrIPString(hctx.RemoteAddr())

	fieldsMask := binary.LittleEndian.Uint32(hctx.Request[4:])
	shardReplica := binary.LittleEndian.Uint32(hctx.Request[8:])
	fieldsMask |= (1 << 31) // args.SetIngressProxy(true)
	binary.LittleEndian.PutUint32(hctx.Request[4:], fieldsMask)
	binary.LittleEndian.PutUint32(hctx.Request[28:], addrIPV4) // source_ip[3] in header. TODO - ipv6
	// We override this field if set by previous proxy. Because we do not care about agent IPs in their cuber/internal networks
	hostName, err := parseHostname(hctx.Request)
	if err != nil {
		return true, err
	}
	// Motivation of % len - we pass through badly configured requests for now, so aggregators will record them in builtin metric
	// TODO - collect metric, send to aggregator, reply with error to clients
	shardReplicaIx := shardReplica % uint32(len(proxy.sh2.GetConfigResult.Addresses))
	address := proxy.sh2.GetConfigResult.Addresses[shardReplicaIx]

	client := proxy.pool.getClient(hostName, remoteAddress)
	req := client.GetRequest()
	req.Body = append(req.Body, hctx.Request...)
	req.Extra.FailIfNoConnection = true

	resp, err := client.Do(ctx, proxy.config.Network, address, req)
	if err != nil {
		return false, err
	}
	defer client.PutResponse(resp)

	hctx.Response = append(hctx.Response, resp.Body...)

	return false, nil
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
	client = rpc.NewClient(rpc.ClientWithLogf(log.Printf), rpc.ClientWithCryptoKey(pool.aesPwd), rpc.ClientWithTrustedSubnetGroups(build.TrustedSubnetGroups()))
	pool.clients[clientHost] = client
	return client

}
