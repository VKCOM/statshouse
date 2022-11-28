// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package aggregator

import (
	"context"
	"log"
	"strconv"
	"time"

	"go4.org/mem"

	"github.com/vkcom/statshouse/internal/agent"
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/mapping"
	"github.com/vkcom/statshouse/internal/metajournal"
	"github.com/vkcom/statshouse/internal/pcache"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
)

type TagsMapper struct {
	agg *Aggregator // for mapped tag values, which can be copied here if needed

	sh2 *agent.Agent

	metricStorage *metajournal.MetricsStorage // check metric exists to prevent mapping budget mining

	tagValue *pcache.Cache
}

func NewTagsMapper(agg *Aggregator, sh2 *agent.Agent, metricStorage *metajournal.MetricsStorage, dc *pcache.DiskCache, a *Aggregator, loader *metajournal.MetricMetaLoader, suffix string) *TagsMapper {
	ms := &TagsMapper{agg: agg, sh2: sh2, metricStorage: metricStorage}
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
			metricName = format.BuiltinMetricNameBudgetUnknownMetric
			// Unknown metrics (also loads from caches after initial error, because cache does not store extra). They all share common limit.
			// Journal can be stale, while mapping works.
			// Explicit metric for this situation allows resetting limit from UI, like any other metric
		}
		keyValue, c, d, err := loader.GetTagMapping(ctx, askedKey, metricName, extra.Create)
		key := ms.sh2.AggKey(0, format.BuiltinMetricIDAggMappingCreated, [16]int32{extra.ClientEnv, 0, 0, 0, metricID, c, extra.TagIDKey})
		key = key.WithAgentEnvRouteArch(extra.AgentEnv, extra.Route, extra.BuildArch)

		if err != nil {
			// TODO - write to actual log from time to time
			a.appendInternalLog("map_tag", strconv.Itoa(int(extra.AgentEnv)), "error", askedKey, extra.Metric, strconv.Itoa(int(metricID)), strconv.Itoa(int(extra.TagIDKey)), err.Error())
			ms.sh2.AddValueCounterHost(key, 0, 1, extra.Host)
		} else {
			switch c {
			case format.TagValueIDAggMappingCreatedStatusFlood:
				// TODO - more efficient flood processing - do not write to log, etc
				a.appendInternalLog("map_tag", strconv.Itoa(int(extra.AgentEnv)), "flood", askedKey, extra.Metric, strconv.Itoa(int(metricID)), strconv.Itoa(int(extra.TagIDKey)), extra.HostName)
				ms.sh2.AddValueCounterHost(key, 0, 1, extra.Host)
			case format.TagValueIDAggMappingCreatedStatusCreated:
				a.appendInternalLog("map_tag", strconv.Itoa(int(extra.AgentEnv)), "created", askedKey, extra.Metric, strconv.Itoa(int(metricID)), strconv.Itoa(int(extra.TagIDKey)), strconv.Itoa(int(keyValue)))
				// if askedKey is created, it is valid and safe to write
				ms.sh2.AddValueCounterHostStringBytes(key, float64(keyValue), 1, extra.Host, []byte(askedKey))
			}
		}
		return pcache.Int32ToValue(keyValue), d, err
	}, "_a_"+suffix, dc)

	return ms
}

func (ms *TagsMapper) getTagOr0LoadLater(now time.Time, str []byte, metricName string, shouldWait bool) int32 {
	r := ms.tagValue.GetCached(now, str)
	if !r.Found() {
		extra := format.CreateMappingExtra{
			Create:    true,
			Metric:    metricName,
			TagIDKey:  format.TagIDShift, // tag for key 0
			ClientEnv: 0,
			AgentEnv:  format.TagValueIDProduction,
			Route:     format.TagValueIDRouteDirect,
			BuildArch: ms.agg.buildArchTag,
			HostName:  string(ms.agg.hostName),
			Host:      ms.agg.aggregatorHost,
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
	if !format.ValidStringValue(mem.B(tagName)) {
		return 0
	}
	if len(tagName) == 0 {
		return 0
	}
	for {
		ret := ms.getTagOr0LoadLater(time.Now(), tagName, metricName, true)
		if ret != 0 {
			return ret
		}
		log.Printf("failed to map mandatory string at startup %q. Will retry in 1 second", tagName)
		time.Sleep(time.Second)
		// Repeat until mapped. Will hang forever if no value in cache and meta is unavailable
	}
}

func (ms *TagsMapper) mapHost(now time.Time, hostName []byte, metricName string, shouldWait bool) int32 {
	if !format.ValidStringValue(mem.B(hostName)) {
		return 0
	}
	if len(hostName) == 0 {
		return 0
	}
	return ms.getTagOr0LoadLater(now, hostName, metricName, shouldWait)
}

// safe only to access fields mask in args, other fields point to reused memory
func (ms *TagsMapper) sendCreateTagMappingResult(hctx *rpc.HandlerContext, args tlstatshouse.GetTagMapping2Bytes, r pcache.Result, key data_model.Key) (err error) {
	if r.Err != nil {
		key.Keys[5] = format.TagValueIDAggMappingStatusErrUncached
		ms.sh2.AddCounter(key, 1)
		return r.Err
	}
	ms.sh2.AddCounter(key, 1)
	result := tlstatshouse.GetTagMappingResult{Value: pcache.ValueToInt32(r.Value), TtlNanosec: int64(r.TTL)}
	hctx.Response, err = args.WriteResult(hctx.Response, result)
	return err
}

func (ms *TagsMapper) handleCreateTagMapping(_ context.Context, hctx *rpc.HandlerContext, args tlstatshouse.GetTagMapping2Bytes) error {
	now := time.Now()
	host := ms.mapHost(now, args.Header.HostName, format.BuiltinMetricNameBudgetHost, false)
	agentEnv := ms.agg.getAgentEnv(args.Header.IsSetAgentEnvStaging(args.FieldsMask))
	buildArch := format.FilterBuildArch(args.Header.BuildArch)
	route := int32(format.TagValueIDRouteDirect) // all config routes are direct
	if args.Header.IsSetIngressProxy(args.FieldsMask) {
		route = int32(format.TagValueIDRouteIngressProxy)
	}
	key := ms.sh2.AggKey(0, format.BuiltinMetricIDAggMapping, [16]int32{0, 0, 0, 0, format.TagValueIDAggMappingTags, format.TagValueIDAggMappingStatusOKCached})
	key = key.WithAgentEnvRouteArch(agentEnv, route, buildArch)

	r := ms.tagValue.GetCached(now, args.Key)
	if !r.Found() {
		extra := format.CreateMappingExtra{
			Create:    args.IsSetCreate(),
			Metric:    string(args.Metric),
			TagIDKey:  args.TagIdKey,
			ClientEnv: args.ClientEnv,
			AgentEnv:  agentEnv,
			Route:     route,
			BuildArch: buildArch,
			HostName:  string(args.Header.HostName),
			Host:      host,
		}
		if args.Header.IsSetIngressProxy(args.FieldsMask) {
			extra.Route = int32(format.TagValueIDRouteIngressProxy)
		}
		r = ms.tagValue.GetOrLoadCallback(now, string(args.Key), extra, func(v pcache.Result) {
			key.Keys[5] = format.TagValueIDAggMappingStatusOKUncached
			err := ms.sendCreateTagMappingResult(hctx, args, v, key)
			hctx.SendHijackedResponse(err)
		})
		if !r.Found() {
			return hctx.HijackResponse()
		}
	}
	return ms.sendCreateTagMappingResult(hctx, args, r, key)
}
