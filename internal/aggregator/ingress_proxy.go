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

type IngressProxy struct {
	sh2 *agent.Agent

	mu      sync.Mutex
	aesPwd  string
	clients map[string]*rpc.Client // client per incoming IP
	server  rpc.Server
	config  ConfigIngressProxy
}

type ConfigIngressProxy struct {
	Network           string
	ListenAddr        string
	ExternalAddresses []string // exactly 3 comma-separated external ingress points
	IngressKeys       []string
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
		return fmt.Errorf("Ingress proxy must have non-empty list of ingress crypto keys")
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
		sh2:     sh2,
		aesPwd:  aesPwd,
		clients: map[string]*rpc.Client{},
		// TODO - server settings must be tuned
		server: rpc.Server{
			CryptoKeys:             config.IngressKeys,
			ForceEncryption:        true, // Protection against wrong logic in net masks
			Logf:                   log.Printf,
			DisableContextTimeout:  true,
			TrustedSubnetGroups:    nil,
			Version:                build.Info(),
			DefaultResponseTimeout: data_model.MaxConveyorDelay * time.Second,                                                                 // TODO
			MaxInflightPackets:     (data_model.MaxConveyorDelay + data_model.MaxHistorySendStreams) * 3 * len(sh2.GetConfigResult.Addresses), // see server settings in aggregator
			MaxWorkers:             128 << 10,                                                                                                 // TODO - use no workers in ingress proxy
			ResponseBufSize:        1024,
			ResponseMemEstimate:    1024,
			RequestMemoryLimit:     8 << 30, // see server settings in aggregator. We do not multiply here
		},
		config: config,
	}
	proxy.server.Handler = proxy.handler
	log.Printf("Running ingress proxy listening %s with %d crypto keys", config.ListenAddr, len(config.IngressKeys))
	return proxy.server.ListenAndServe("tcp4", config.ListenAddr)
}

func (proxy *IngressProxy) handler(ctx context.Context, hctx *rpc.HandlerContext) error {
	tag, _ := basictl.NatPeekTag(hctx.Request)
	keyID := hctx.KeyID()
	keyIDTag := int32(binary.BigEndian.Uint32(keyID[:4]))
	key := data_model.Key{
		Metric: format.BuiltinMetricIDRPCRequests,
		Keys:   [16]int32{0, format.TagValueIDComponentIngressProxy, int32(tag), format.TagValueIDRPCRequestsStatusOK, 0, 0, keyIDTag},
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
	case constants.StatshouseGetConfig: // TODO - remove after release version is everywhere
		var args tlstatshouse.GetConfig
		var ret tlstatshouse.GetConfigResult
		_, err := args.ReadBoxed(hctx.Request)
		if err != nil {
			return true, fmt.Errorf("failed to deserialize statshouse.getConfig request: %w", err)
		}
		ret.Addresses = proxy.config.ExternalAddresses
		ret.MaxAddressesCount = proxy.sh2.GetConfigResult.MaxAddressesCount
		ret.PreviousAddresses = proxy.sh2.GetConfigResult.PreviousAddresses
		hctx.Response, err = args.WriteResult(hctx.Response, ret)
		return true, err
	case constants.StatshouseGetConfig2:
		var args tlstatshouse.GetConfig2
		var ret tlstatshouse.GetConfigResult
		_, err := args.ReadBoxed(hctx.Request)
		if err != nil {
			return true, fmt.Errorf("failed to deserialize statshouse.getConfig2 request: %w", err)
		}
		ret.Addresses = proxy.config.ExternalAddresses
		ret.MaxAddressesCount = proxy.sh2.GetConfigResult.MaxAddressesCount
		ret.PreviousAddresses = proxy.sh2.GetConfigResult.PreviousAddresses
		hctx.Response, err = args.WriteResult(hctx.Response, ret)
		return true, err
	case constants.StatshouseGetMetrics, constants.StatshouseGetTagMapping,
		constants.StatshouseSendKeepAlive, constants.StatshouseSendSourceBucket,
		constants.StatshouseTestConnection, constants.StatshouseGetTargets:
		return proxy.proxyLegacyRequest(tag, ctx, hctx) // TODO - remove after release version is everywhere
	case constants.StatshouseGetMetrics2, constants.StatshouseGetTagMapping2,
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
	addrIPV4, remoteAddress := addrIPString(hctx.RemoteAddr()) // without port, so per machine

	fieldsMask := binary.LittleEndian.Uint32(hctx.Request[4:])
	shardReplica := binary.LittleEndian.Uint32(hctx.Request[8:])
	fieldsMask |= (1 << 31) // args.SetIngressProxy(true)
	binary.LittleEndian.PutUint32(hctx.Request[4:], fieldsMask)
	binary.LittleEndian.PutUint32(hctx.Request[28:], addrIPV4) // source_ip[3] in header. TODO - ipv6

	// Motivation of % len - we pass through badly configured requests for now, so aggregators will record them in builtin metric
	// TODO - collect metric, send to aggregator, reply with error to clients
	address := proxy.sh2.GetConfigResult.Addresses[shardReplica%uint32(len(proxy.sh2.GetConfigResult.Addresses))]

	proxy.mu.Lock()
	client, ok := proxy.clients[remoteAddress]
	if !ok {
		client = &rpc.Client{
			Logf:                log.Printf,
			CryptoKey:           proxy.aesPwd,
			TrustedSubnetGroups: nil,
		}
		proxy.clients[remoteAddress] = client
	}
	proxy.mu.Unlock()
	if !ok {
		log.Printf("First connection from %s", remoteAddress)
	}
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

func (proxy *IngressProxy) proxyLegacyRequest(tag uint32, ctx context.Context, hctx *rpc.HandlerContext) (isLocal bool, err error) {
	if len(hctx.Request) < 16 {
		return true, fmt.Errorf("ingress proxy legacy query with tag 0x%x is too short - %d bytes", tag, len(hctx.Request))
	}
	fieldsMask := binary.LittleEndian.Uint32(hctx.Request[4:])
	shardReplica := binary.LittleEndian.Uint32(hctx.Request[8:])
	// shardReplicaTotal := binary.LittleEndian.Uint32(hctx.Request.Bytes()[12:])
	if fieldsMask&(1<<5) != (1 << 5) { // !args.IsSetShardReplica2()
		return true, fmt.Errorf("ingress proxy requires correct field mask, %d is incorrect for tag 0x%x", fieldsMask, tag)
	}
	// if int(shardReplicaTotal) != len(proxy.config.AggregatorAddresses) {
	//	return fmt.Errorf("ingress proxy misconfiguration - expected total %d, actual total %d", len(proxy.config.AggregatorAddresses), shardReplicaTotal)
	// }
	// if shardReplica >= shardReplicaTotal {
	//	return fmt.Errorf("ingress proxy misconfiguration - invalid shard_replica %d, total %d", shardReplica, shardReplicaTotal)
	// }
	fieldsMask |= (1 << 6) // args.SetIngressProxy(true)
	binary.LittleEndian.PutUint32(hctx.Request[4:], fieldsMask)

	// Motivation of % len - we pass through badly configured requests for now, so aggregators will record them in builtin metric
	// TODO - collect metric, send to aggregator, reply with error to clients
	address := proxy.sh2.GetConfigResult.Addresses[shardReplica%uint32(len(proxy.sh2.GetConfigResult.Addresses))]

	_, remoteAddress := addrIPString(hctx.RemoteAddr()) // without port, so per machine
	proxy.mu.Lock()
	client, ok := proxy.clients[remoteAddress]
	if !ok {
		client = &rpc.Client{
			Logf:                log.Printf,
			CryptoKey:           proxy.aesPwd,
			TrustedSubnetGroups: nil,
		}
		proxy.clients[remoteAddress] = client
	}
	proxy.mu.Unlock()
	if !ok {
		log.Printf("First connection from %s", remoteAddress)
	}
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
