// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package agent

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/pcache"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
	"github.com/vkcom/statshouse/internal/vkgo/srvfunc"

	"pgregory.net/rand"
)

func GetConfig(network string, rpcClient *rpc.Client, addressesExt []string, hostName string, stagingLevel int, componentTag int32, archTag int32, cluster string, dc *pcache.DiskCache, logF func(format string, args ...interface{})) tlstatshouse.GetConfigResult {
	addresses := append([]string{}, addressesExt...) // For simulator, where many start concurrently with the copy of the config
	rnd := rand.New()
	rnd.Shuffle(len(addresses), func(i, j int) { // randomize configuration load
		addresses[i], addresses[j] = addresses[j], addresses[i]
	})
	backoffTimeout := time.Duration(0)
	for nextAddr := 0; ; nextAddr = (nextAddr + 1) % len(addresses) {
		addr := addresses[nextAddr]
		dst, err := clientGetConfig(network, rpcClient, nextAddr, addr, hostName, stagingLevel, componentTag, archTag, cluster)
		if err == nil {
			// when running agent from outside run_local docker
			// for i := range dst.Addresses {
			//	dst.Addresses[i] = strings.ReplaceAll(dst.Addresses[i], "aggregator", "localhost")
			// }
			logF("Configuration: success autoconfiguration from (%q), address list is (%q), max is %d", strings.Join(addresses, ","), strings.Join(dst.Addresses, ","), dst.MaxAddressesCount)
			if err = clientSaveConfigToCache(cluster, dc, dst); err != nil {
				logF("Configuration: failed to save autoconfig to disk cache: %v", err)
			}

			aggregatorTime := time.UnixMilli(dst.Ts)
			timeDiff := time.Since(aggregatorTime)
			if timeDiff.Abs() > time.Second {
				logF("Configuration: WARNING time difference with aggregator is %v more then a second", timeDiff)
			} else {
				logF("Configuration: time difference with aggregator is %v", timeDiff)
			}
			return dst
		}
		logF("Configuration: failed autoconfiguration from address (%q) - %v", addr, err)
		if nextAddr == len(addresses)-1 { // last one
			dst, err = clientGetConfigFromCache(cluster, dc)
			if err == nil {
				// We could have a long poll on configuration, but this happens so rare that we decided to simplify.
				// We have protection from misconfig on aggregator, so agents with very old config will be rejected and
				// can be easily tracked in __auto_config metric
				logF("Configuration: failed autoconfiguration from all addresses (%q), loaded previous autoconfiguration from disk cache, address list is (%q), max is %d",
					strings.Join(addresses, ","), strings.Join(dst.Addresses, ","), dst.MaxAddressesCount)
				return dst
			}
			backoffTimeout = data_model.NextBackoffDuration(backoffTimeout)
			logF("Configuration: failed autoconfiguration from all addresses (%q), will retry after %v delay",
				strings.Join(addresses, ","), backoffTimeout)
			time.Sleep(backoffTimeout)
			// This sleep will not affect shutdown time
		}
	}
}

func clientSaveConfigToCache(cluster string, dc *pcache.DiskCache, dst tlstatshouse.GetConfigResult) error {
	if dc == nil {
		return nil
	}
	cacheData := dst.WriteBoxed(nil, 0) // 0 - we do not save fields mask. If additional fields are needed, set mask here and in ReadBoxed
	return dc.Set(data_model.AutoconfigDiskNamespace+cluster, "", cacheData, time.Now(), 0)
}

func clientGetConfigFromCache(cluster string, dc *pcache.DiskCache) (tlstatshouse.GetConfigResult, error) {
	var res tlstatshouse.GetConfigResult
	if dc == nil {
		return res, fmt.Errorf("cannot load autoconfig from disc cache, because no disk cache configured")
	}
	cacheData, _, _, errDiskCache, ok := dc.Get(data_model.AutoconfigDiskNamespace+cluster, "")
	if errDiskCache != nil {
		return res, fmt.Errorf("autoconfig cache data failed to load from disk cache: %w", errDiskCache)
	}
	if !ok {
		return res, fmt.Errorf("autoconfig cache data not stored in disk cache (yet?)")
	}
	if _, err := res.ReadBoxed(cacheData, 0); err != nil { // 0 - we do not store additional fields yet
		return res, err
	}
	return res, nil
}

func clientGetConfig(network string, rpcClient *rpc.Client, shardReplicaNum int, addr string, hostName string, stagingLevel int, componentTag int32, archTag int32, cluster string) (tlstatshouse.GetConfigResult, error) {
	extra := rpc.InvokeReqExtra{FailIfNoConnection: true}
	client := tlstatshouse.Client{
		Client:  rpcClient,
		Network: network,
		Address: addr,
		ActorID: 0,
	}
	args := tlstatshouse.GetConfig2{
		Cluster: cluster,
		Header: tlstatshouse.CommonProxyHeader{
			ShardReplica: int32(shardReplicaNum), // proxies do proxy GetConfig requests to write __autoconfig metric with correct host, which proxy cannot map
			HostName:     hostName,
			ComponentTag: componentTag,
			BuildArch:    archTag,
		},
	}
	args.SetTs(true)
	data_model.SetProxyHeaderStagingLevel(&args.Header, &args.FieldsMask, stagingLevel)
	var ret tlstatshouse.GetConfigResult
	ctx, cancel := context.WithTimeout(context.Background(), data_model.AutoConfigTimeout)
	defer cancel()
	if err := client.GetConfig2(ctx, args, &extra, &ret); err != nil {
		return tlstatshouse.GetConfigResult{}, err
	}
	if len(ret.Addresses)%3 != 0 || len(ret.Addresses) == 0 || ret.MaxAddressesCount <= 0 || ret.MaxAddressesCount%3 != 0 || int(ret.MaxAddressesCount) > len(ret.Addresses) {
		return tlstatshouse.GetConfigResult{}, fmt.Errorf("received invalid address list %q max is %d from aggregator %q", strings.Join(ret.Addresses, ","), ret.MaxAddressesCount, addr)
	}
	return ret, nil
}

func (s *Agent) LoadPromTargets(ctxParent context.Context, version string) (res *tlstatshouse.GetTargetsResult, versionHash string, err error) {
	// This long poll is for config hash, which cannot be compared with > or <, so if aggregators have different configs, we will
	// make repeated calls between them until we randomly select 2 in a row with the same config.
	// so we have to remember the last one we used, and try sending to it, if it is alive.
	s.mu.Lock()
	if s.loadPromTargetsShardReplica == nil || !s.loadPromTargetsShardReplica.alive.Load() {
		s.loadPromTargetsShardReplica, _ = s.getRandomLiveShardReplicas()
	}
	s.mu.Unlock()

	extra := rpc.InvokeReqExtra{FailIfNoConnection: true}
	if s.loadPromTargetsShardReplica == nil {
		return nil, "", fmt.Errorf("cannot load prom groups, all aggregators are dead")
	}
	args := tlstatshouse.GetTargets2{
		PromHostName: srvfunc.Hostname(),
		OldHash:      version,
	}
	args.SetGaugeMetrics(true)
	args.SetMetricRelabelConfigs(true)
	s.loadPromTargetsShardReplica.fillProxyHeader(&args.FieldsMask, &args.Header)

	var ret tlstatshouse.GetTargetsResult

	// We do not need timeout for long poll, RPC has disconnect detection via ping-pong
	err = s.loadPromTargetsShardReplica.client.GetTargets2(ctxParent, args, &extra, &ret)
	if err != nil {
		s.mu.Lock()
		s.loadPromTargetsShardReplica = nil // forget, select random one next time
		s.mu.Unlock()
		return nil, "", fmt.Errorf("cannot load prom config - %w", err)
	}

	return &ret, ret.Hash, nil
}

func (s *Agent) LoadMetaMetricJournal(ctxParent context.Context, version int64, returnIfEmpty bool) ([]tlmetadata.Event, int64, error) {
	extra := rpc.InvokeReqExtra{FailIfNoConnection: true}
	// Actually use only single aggregator for mapping
	s0, _ := s.getRandomLiveShardReplicas()
	// If aggregators cannot insert en masse, system is dead.
	// On the other hand, if aggregators cannot map, but can insert, system is working
	// So, we must have separate live-dead status for mappings - TODO
	if s0 == nil {
		s.AddValueCounter(data_model.Key{Metric: format.BuiltinMetricIDAgentMapping, Tags: [format.MaxTags]int32{0, format.TagValueIDAggMappingMetaMetrics, format.TagValueIDAgentMappingStatusAllDead}}, 0, 1, format.BuiltinMetricMetaAgentMapping)
		return nil, version, fmt.Errorf("cannot load meta journal, all aggregators are dead")
	}
	now := time.Now()

	args := tlstatshouse.GetMetrics3{
		From: version,
	}
	s0.fillProxyHeader(&args.FieldsMask, &args.Header)
	args.SetReturnIfEmpty(returnIfEmpty)

	var ret tlmetadata.GetJournalResponsenew

	// We do not need timeout for long poll, RPC has disconnect detection via ping-pong
	err := s0.client.GetMetrics3(ctxParent, args, &extra, &ret)
	if err != nil {
		s.AddValueCounter(data_model.Key{Metric: format.BuiltinMetricIDAgentMapping, Tags: [format.MaxTags]int32{0, format.TagValueIDAggMappingMetaMetrics, format.TagValueIDAgentMappingStatusErrSingle}}, time.Since(now).Seconds(), 1, format.BuiltinMetricMetaAgentMapping)
		return nil, version, fmt.Errorf("cannot load meta journal - %w", err)
	}
	/*
		for _, r := range ret.Events {
				s.AddValueCounter(data_model.Key{Metric: format.BuiltinMetricIDAgentMapping, Keys: [format.MaxTags]int32{0, format.TagValueIDAggMappingMetaMetrics, format.TagValueIDAgentMappingStatusErrSingle}}, time.Since(now).Seconds(), 1, false, format.BuiltinMetricIDAgentMapping)
		}
	*/
	s.AddValueCounter(data_model.Key{Metric: format.BuiltinMetricIDAgentMapping, Tags: [format.MaxTags]int32{0, format.TagValueIDAggMappingMetaMetrics, format.TagValueIDAgentMappingStatusOKFirst}}, time.Since(now).Seconds(), 1, format.BuiltinMetricMetaAgentMapping)
	return ret.Events, ret.CurrentVersion, nil
}

func (s *Agent) LoadOrCreateMapping(ctxParent context.Context, key string, floodLimitKey interface{}) (pcache.Value, time.Duration, error) {
	extra := rpc.InvokeReqExtra{FailIfNoConnection: true}
	// Use 2 alive random aggregators for mapping
	s0, s1 := s.getRandomLiveShardReplicas()
	if s0 == nil {
		s.AddValueCounter(data_model.Key{Metric: format.BuiltinMetricIDAgentMapping, Tags: [format.MaxTags]int32{0, format.TagValueIDAggMappingMetaMetrics, format.TagValueIDAgentMappingStatusAllDead}}, 0, 1, format.BuiltinMetricMetaAgentMapping)
		return nil, 0, fmt.Errorf("all aggregators are dead")
	}
	now := time.Now()

	args := tlstatshouse.GetTagMapping2{
		Key: key,
	}
	s0.fillProxyHeader(&args.FieldsMask, &args.Header)
	args.SetCreate(true)

	if floodLimitKey != nil {
		// cache passes nil floodLimitKey when updating existing records, so in theory, we will never need to actually create key
		// when extra is nil. But if we attempt to do it, will record attempts in common key for all metrics.
		e := floodLimitKey.(format.CreateMappingExtra)
		args.Metric = e.Metric
		args.ClientEnv = e.ClientEnv
		args.TagIdKey = e.TagIDKey
	}

	var ret tlstatshouse.GetTagMappingResult

	ctx, cancel := context.WithTimeout(ctxParent, data_model.AgentMappingTimeout1)
	defer cancel()
	err := s0.client.GetTagMapping2(ctx, args, &extra, &ret)
	if err == nil {
		s.AddValueCounter(data_model.Key{Metric: format.BuiltinMetricIDAgentMapping, Tags: [format.MaxTags]int32{0, format.TagValueIDAggMappingMetaMetrics, format.TagValueIDAgentMappingStatusOKFirst}}, time.Since(now).Seconds(), 1, format.BuiltinMetricMetaAgentMapping)
		return pcache.Int32ToValue(ret.Value), time.Duration(ret.TtlNanosec), nil
	}
	if s1 == nil {
		s.AddValueCounter(data_model.Key{Metric: format.BuiltinMetricIDAgentMapping, Tags: [format.MaxTags]int32{0, format.TagValueIDAggMappingMetaMetrics, format.TagValueIDAgentMappingStatusErrSingle}}, time.Since(now).Seconds(), 1, format.BuiltinMetricMetaAgentMapping)
		return nil, 0, fmt.Errorf("the only live aggregator %q returned error: %w", s0.client.Address, err)
	}

	s1.fillProxyHeader(&args.FieldsMask, &args.Header)

	ctx2, cancel2 := context.WithTimeout(ctxParent, data_model.AgentMappingTimeout2)
	defer cancel2()
	err2 := s1.client.GetTagMapping2(ctx2, args, &extra, &ret)
	if err2 == nil {
		s.AddValueCounter(data_model.Key{Metric: format.BuiltinMetricIDAgentMapping, Tags: [format.MaxTags]int32{0, format.TagValueIDAggMappingMetaMetrics, format.TagValueIDAgentMappingStatusOKSecond}}, time.Since(now).Seconds(), 1, format.BuiltinMetricMetaAgentMapping)
		return pcache.Int32ToValue(ret.Value), time.Duration(ret.TtlNanosec), nil
	}
	s.AddValueCounter(data_model.Key{Metric: format.BuiltinMetricIDAgentMapping, Tags: [format.MaxTags]int32{0, format.TagValueIDAggMappingMetaMetrics, format.TagValueIDAgentMappingStatusErrBoth}}, time.Since(now).Seconds(), 1, format.BuiltinMetricMetaAgentMapping)
	return nil, 0, fmt.Errorf("two live aggregators %q %q returned errors: %v %w", s0.client.Address, s1.client.Address, err, err2)
}

func (s *Agent) GetTagMappingBootstrap(ctxParent context.Context) ([]tlstatshouse.Mapping, time.Duration, error) {
	extra := rpc.InvokeReqExtra{FailIfNoConnection: true}
	// Use 2 alive random aggregators for mapping
	s0, s1 := s.getRandomLiveShardReplicas()
	if s0 == nil {
		return nil, 0, fmt.Errorf("all aggregators are dead")
	}

	args := tlstatshouse.GetTagMappingBootstrap{}
	s0.fillProxyHeader(&args.FieldsMask, &args.Header)

	var ret tlstatshouse.GetTagMappingBootstrapResult

	ctx, cancel := context.WithTimeout(ctxParent, data_model.AgentMappingTimeout1)
	defer cancel()
	err := s0.client.GetTagMappingBootstrap(ctx, args, &extra, &ret)
	if err == nil {
		return ret.Mappings, 0, nil
	}
	if s1 == nil {
		return nil, 0, fmt.Errorf("the only live aggregator %q returned error: %w", s0.client.Address, err)
	}

	s1.fillProxyHeader(&args.FieldsMask, &args.Header)

	ctx2, cancel2 := context.WithTimeout(ctxParent, data_model.AgentMappingTimeout2)
	defer cancel2()
	err2 := s1.client.GetTagMappingBootstrap(ctx2, args, &extra, &ret)
	if err2 == nil {
		return ret.Mappings, 0, nil
	}
	return nil, 0, fmt.Errorf("two live aggregators %q %q returned errors: %v %w", s0.client.Address, s1.client.Address, err, err2)
}
