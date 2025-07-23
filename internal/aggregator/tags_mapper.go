// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package aggregator

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"go4.org/mem"

	"github.com/VKCOM/statshouse/internal/agent"
	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/VKCOM/statshouse/internal/format"
	"github.com/VKCOM/statshouse/internal/mapping"
	"github.com/VKCOM/statshouse/internal/metajournal"
	"github.com/VKCOM/statshouse/internal/pcache"
	"github.com/VKCOM/statshouse/internal/vkgo/rpc"
)

type TagsMapper struct {
	agg *Aggregator // for mapped tag values, which can be copied here if needed

	sh2 *agent.Agent

	metricStorage *metajournal.MetricsStorage // check metric exists to prevent mapping budget mining

	mu         sync.Mutex
	clientList map[*rpc.HandlerContext]*bool // TODO - remove this, make cancellation token in pcache with callback GetOrLoadCallback

	tagValue *pcache.Cache
}

func NewTagsMapper(agg *Aggregator, sh2 *agent.Agent, metricStorage *metajournal.MetricsStorage, dc pcache.DiskCache, loader *metajournal.MetricMetaLoader, suffix string) *TagsMapper {
	ms := &TagsMapper{agg: agg, sh2: sh2, metricStorage: metricStorage, clientList: map[*rpc.HandlerContext]*bool{}}
	ms.tagValue = mapping.NewTagsCache(func(ctx context.Context, askedKey string, extra2 interface{}) (pcache.Value, time.Duration, error) {
		extra, _ := extra2.(format.CreateMappingExtra)
		metricName := extra.Metric
		metricID := int32(0)
		if bm := format.BuiltinMetricByName[extra.Metric]; bm != nil {
			metricID = bm.MetricID
		} else if mm := ms.metricStorage.GetMetaMetricByName(extra.Metric); mm != nil {
			metricID = mm.MetricID
		} else {
			metricID = format.BuiltinMetricIDBudgetUnknownMetric
			metricName = format.BuiltinMetricMetaBudgetUnknownMetric.Name
			// Unknown metrics (also loads from caches after initial error, because cache does not store extra). They all share common limit.
			// Journal can be stale, while mapping works.
			// Explicit metric for this situation allows resetting limit from UI, like any other metric
		}
		keyValue, c, d, err := loader.GetTagMapping(ctx, askedKey, metricName, extra.Create)
		ms.sh2.AddValueCounterHostAERA(0, format.BuiltinMetricMetaAggMappingCreated,
			[]int32{extra.ClientEnv, 0, 0, 0, metricID, c, extra.TagIDKey, format.TagValueIDAggMappingCreatedConveyorOld, 0, keyValue},
			float64(keyValue), 1, data_model.TagUnionBytes{I: extra.Host}, extra.Aera)

		return pcache.Int32ToValue(keyValue), d, err
	}, "_a_"+suffix, dc)

	return ms
}

func (ms *TagsMapper) CancelHijack(hctx *rpc.HandlerContext) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if b, ok := ms.clientList[hctx]; ok {
		*b = false
		delete(ms.clientList, hctx)
	}
}

func (ms *TagsMapper) getTagOr0LoadLater(now time.Time, str []byte, metricName string, shouldWait bool) int32 {
	if len(str) == 0 {
		return 0
	}
	r := ms.tagValue.GetCached(now, str)
	if !r.Found() {
		extra := format.CreateMappingExtra{
			Create:    true,
			Metric:    metricName,
			TagIDKey:  format.TagIDShift, // tag for key 0
			ClientEnv: 0,
			Aera: format.AgentEnvRouteArch{
				AgentEnv:  format.TagValueIDProduction,
				Route:     format.TagValueIDRouteDirect,
				BuildArch: ms.agg.buildArchTag,
			},
			HostName: string(ms.agg.hostName),
			Host:     ms.agg.aggregatorHost,
		}
		if shouldWait {
			r = ms.tagValue.GetOrLoad(now, string(str), extra)
		} else {
			r = ms.tagValue.GetOrLoadCallback(now, string(str), extra, func(v pcache.Result) {}) // initiate load/create process only
		}
		if !r.Found() {
			return 0
		}
	}
	if r.Err != nil {
		return 0
	}
	return pcache.ValueToInt32(r.Value)
}

func (ms *TagsMapper) mapTagAtStartup(tagName []byte, metricName string) int32 {
	if !format.ValidStringValue(mem.B(tagName)) || len(tagName) == 0 {
		panic("conditions checked above")
	}
	for {
		ret := ms.getTagOr0LoadLater(time.Now(), tagName, metricName, true)
		if ret != 0 {
			return ret
		}
		log.Printf("failed to map mandatory string %q at startup. Will retry in 1 second", tagName)
		time.Sleep(time.Second)
		// Repeat until mapped. Will hang forever if no value in cache and meta is unavailable
	}
}

func (ms *TagsMapper) mapOrFlood(now time.Time, value []byte, metricName string, shouldWait bool) int32 {
	// if aggregator fails to map agent host, then sets as a max host for some built-in metric, then
	// when agent sends to another aggregator, max host will be set to original aggregator host, not to "empty" (unknown).
	// This is why we set to invalid "Mapping Flood" value. This is not perfect, but better.
	if !format.ValidStringValue(mem.B(value)) {
		return format.TagValueIDMappingFlood
	}
	ret := ms.getTagOr0LoadLater(now, value, metricName, shouldWait)
	if ret != 0 {
		return ret
	}
	return format.TagValueIDMappingFlood
}

// safe only to access fields mask in args, other fields point to reused memory
func (ms *TagsMapper) sendCreateTagMappingResult(hctx *rpc.HandlerContext, args tlstatshouse.GetTagMapping2Bytes, r pcache.Result, aera format.AgentEnvRouteArch, status int32) (err error) {
	if r.Err != nil {
		status = format.TagValueIDAggMappingStatusErrUncached
	}
	ms.sh2.AddCounterHostAERA(0, format.BuiltinMetricMetaAggMapping,
		[]int32{0, 0, 0, 0, format.TagValueIDAggMappingMetaMetrics, status},
		1, data_model.TagUnionBytes{}, aera)
	if r.Err != nil {
		return r.Err
	}
	result := tlstatshouse.GetTagMappingResult{Value: pcache.ValueToInt32(r.Value), TtlNanosec: int64(r.TTL)}
	hctx.Response, err = args.WriteResult(hctx.Response, result)
	return err
}

func (ms *TagsMapper) handleCreateTagMapping(_ context.Context, hctx *rpc.HandlerContext) error {
	var args tlstatshouse.GetTagMapping2Bytes
	_, err := args.Read(hctx.Request)
	if err != nil {
		return fmt.Errorf("failed to deserialize statshouse.getTagMapping2 request: %w", err)
	}
	now := time.Now()
	host := ms.mapOrFlood(now, args.Header.HostName, format.BuiltinMetricMetaBudgetHost.Name, false)
	aera := format.AgentEnvRouteArch{
		AgentEnv:  ms.agg.getAgentEnv(args.Header.IsSetAgentEnvStaging0(args.FieldsMask), args.Header.IsSetAgentEnvStaging1(args.FieldsMask)),
		Route:     format.TagValueIDRouteDirect,
		BuildArch: format.FilterBuildArch(args.Header.BuildArch),
	}
	isRouteProxy := args.Header.IsSetIngressProxy(args.FieldsMask)
	if isRouteProxy {
		aera.Route = format.TagValueIDRouteIngressProxy
	}
	r := ms.tagValue.GetCached(now, args.Key)
	if !r.Found() {
		extra := format.CreateMappingExtra{
			Create:    args.IsSetCreate(),
			Metric:    string(args.Metric),
			TagIDKey:  args.TagIdKey,
			ClientEnv: args.ClientEnv,
			Aera:      aera,
			HostName:  string(args.Header.HostName),
			Host:      host,
		}
		b := true
		bb := &b // Stupid but correct implementation. TODO - improve, remove this crap
		// For example use normal workers pool from rpc.Server
		r = ms.tagValue.GetOrLoadCallback(now, string(args.Key), extra, func(v pcache.Result) {
			ms.mu.Lock()
			defer ms.mu.Unlock()
			if *bb {
				err := ms.sendCreateTagMappingResult(hctx, args, v, aera, format.TagValueIDAggMappingStatusOKUncached)
				hctx.SendHijackedResponse(err)
			}
		})
		if !r.Found() {
			ms.mu.Lock()
			defer ms.mu.Unlock()
			ms.clientList[hctx] = bb
			return hctx.HijackResponse(ms)
		}
	}
	return ms.sendCreateTagMappingResult(hctx, args, r, aera, format.TagValueIDAggMappingStatusOKCached)
}
