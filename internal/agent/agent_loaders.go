// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package agent

import (
	"context"
	"fmt"
	"time"

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/pcache"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
	"github.com/vkcom/statshouse/internal/vkgo/srvfunc"
)

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
	client := s.loadPromTargetsShardReplica.client()
	err = client.GetTargets2(ctxParent, args, &extra, &ret)
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
		s.AddValueCounter(0, format.BuiltinMetricMetaAgentMapping,
			[]int32{0, format.TagValueIDAggMappingMetaMetrics, format.TagValueIDAgentMappingStatusAllDead},
			0, 1)
		return nil, version, fmt.Errorf("cannot load meta journal, all aggregators are dead")
	}
	now := time.Now()

	args := tlstatshouse.GetMetrics3{
		From: version,
	}
	s0.fillProxyHeader(&args.FieldsMask, &args.Header)
	args.SetReturnIfEmpty(returnIfEmpty)
	args.SetCompactJournal(true)

	var ret tlmetadata.GetJournalResponsenew

	// We do not need timeout for long poll, RPC has disconnect detection via ping-pong
	s0client := s0.client()
	err := s0client.GetMetrics3(ctxParent, args, &extra, &ret)
	if err != nil {
		s.AddValueCounter(0, format.BuiltinMetricMetaAgentMapping,
			[]int32{0, format.TagValueIDAggMappingMetaMetrics, format.TagValueIDAgentMappingStatusErrSingle},
			time.Since(now).Seconds(), 1)
		return nil, version, fmt.Errorf("cannot load meta journal - %w", err)
	}
	/*
		for _, r := range ret.Events {
				s.AddValueCounter(data_model.Key{Metric: format.BuiltinMetricIDAgentMapping, Keys: [format.MaxTags]int32{0, format.TagValueIDAggMappingMetaMetrics, format.TagValueIDAgentMappingStatusErrSingle}}, time.Since(now).Seconds(), 1, false, format.BuiltinMetricIDAgentMapping)
		}
	*/
	s.AddValueCounter(0, format.BuiltinMetricMetaAgentMapping,
		[]int32{0, format.TagValueIDAggMappingMetaMetrics, format.TagValueIDAgentMappingStatusOKFirst},
		time.Since(now).Seconds(), 1)
	return ret.Events, ret.CurrentVersion, nil
}

func (s *Agent) LoadOrCreateMapping(ctxParent context.Context, key string, floodLimitKey interface{}) (pcache.Value, time.Duration, error) {
	extra := rpc.InvokeReqExtra{FailIfNoConnection: true}
	// Use 2 alive random aggregators for mapping
	s0, s1 := s.getRandomLiveShardReplicas()
	if s0 == nil {
		s.AddValueCounter(0, format.BuiltinMetricMetaAgentMapping,
			[]int32{0, format.TagValueIDAggMappingMetaMetrics, format.TagValueIDAgentMappingStatusAllDead},
			0, 1)
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
	s0client := s0.client()
	err := s0client.GetTagMapping2(ctx, args, &extra, &ret)
	if err == nil {
		s.AddValueCounter(0, format.BuiltinMetricMetaAgentMapping,
			[]int32{0, format.TagValueIDAggMappingMetaMetrics, format.TagValueIDAgentMappingStatusOKFirst},
			time.Since(now).Seconds(), 1)
		return pcache.Int32ToValue(ret.Value), time.Duration(ret.TtlNanosec), nil
	}
	if s1 == nil {
		s.AddValueCounter(0, format.BuiltinMetricMetaAgentMapping,
			[]int32{0, format.TagValueIDAggMappingMetaMetrics, format.TagValueIDAgentMappingStatusErrSingle},
			time.Since(now).Seconds(), 1)
		return nil, 0, fmt.Errorf("the only live aggregator %q returned error: %w", s0client.Address, err)
	}

	s1.fillProxyHeader(&args.FieldsMask, &args.Header)

	ctx2, cancel2 := context.WithTimeout(ctxParent, data_model.AgentMappingTimeout2)
	defer cancel2()
	s1client := s1.client()
	err2 := s1client.GetTagMapping2(ctx2, args, &extra, &ret)
	if err2 == nil {
		s.AddValueCounter(0, format.BuiltinMetricMetaAgentMapping,
			[]int32{0, format.TagValueIDAggMappingMetaMetrics, format.TagValueIDAgentMappingStatusOKSecond},
			time.Since(now).Seconds(), 1)
		return pcache.Int32ToValue(ret.Value), time.Duration(ret.TtlNanosec), nil
	}
	s.AddValueCounter(0, format.BuiltinMetricMetaAgentMapping,
		[]int32{0, format.TagValueIDAggMappingMetaMetrics, format.TagValueIDAgentMappingStatusErrBoth},
		time.Since(now).Seconds(), 1)
	return nil, 0, fmt.Errorf("two live aggregators %q %q returned errors: %v %w", s0client.Address, s1client.Address, err, err2)
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
	s0client := s0.client()
	err := s0client.GetTagMappingBootstrap(ctx, args, &extra, &ret)
	if err == nil {
		return ret.Mappings, 0, nil
	}
	if s1 == nil {
		return nil, 0, fmt.Errorf("the only live aggregator %q returned error: %w", s0client.Address, err)
	}

	s1.fillProxyHeader(&args.FieldsMask, &args.Header)

	ctx2, cancel2 := context.WithTimeout(ctxParent, data_model.AgentMappingTimeout2)
	defer cancel2()
	s1client := s1.client()
	err2 := s1client.GetTagMappingBootstrap(ctx2, args, &extra, &ret)
	if err2 == nil {
		return ret.Mappings, 0, nil
	}
	return nil, 0, fmt.Errorf("two live aggregators %q %q returned errors: %v %w", s0client.Address, s1client.Address, err, err2)
}
