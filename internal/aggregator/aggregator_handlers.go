// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package aggregator

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"time"

	"go4.org/mem"

	"github.com/pierrec/lz4"

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/constants"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/vkgo/basictl"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
)

type userData struct {
	sendSourceBucket2 tlstatshouse.SendSourceBucket2Bytes
	sourceBucket2     tlstatshouse.SourceBucket2Bytes
	sendKeepAlive2    tlstatshouse.SendKeepAlive2Bytes
	getMetrics3       tlstatshouse.GetMetrics3
	getTagMapping2    tlstatshouse.GetTagMapping2Bytes
	getTagBoostrap    tlstatshouse.GetTagMappingBootstrapBytes
	getConfig2        tlstatshouse.GetConfig2
	testConneection2  tlstatshouse.TestConnection2Bytes
	getTargets2       tlstatshouse.GetTargets2Bytes
	autoCreate        tlstatshouse.AutoCreate
	uncompressed      []byte
}

func getUserData(hctx *rpc.HandlerContext) *userData {
	ud, ok := hctx.UserData.(*userData)
	if !ok {
		ud = &userData{}
		hctx.UserData = ud
	}
	return ud
}

func (a *Aggregator) handleClient(ctx context.Context, hctx *rpc.HandlerContext) error {
	tag, _ := basictl.NatPeekTag(hctx.Request)
	keyID := hctx.KeyID()
	keyIDTag := int32(binary.BigEndian.Uint32(keyID[:4]))
	key := data_model.AggKey(0, format.BuiltinMetricIDRPCRequests, [16]int32{0, format.TagValueIDComponentAggregator, int32(tag), format.TagValueIDRPCRequestsStatusOK, 0, 0, keyIDTag}, a.aggregatorHost, a.shardKey, a.replicaKey)
	err := a.handleClientImpl(ctx, hctx)
	if err == rpc.ErrNoHandler {
		key.Keys[3] = format.TagValueIDRPCRequestsStatusNoHandler
	} else if rpc.IsHijackedResponse(err) {
		key.Keys[3] = format.TagValueIDRPCRequestsStatusHijack
	} else if err != nil {
		key.Keys[3] = format.TagValueIDRPCRequestsStatusErrLocal
	}
	a.sh2.AddValueCounter(key, float64(len(hctx.Request)), 1, nil)
	return err
}

func (a *Aggregator) handleClientImpl(ctx context.Context, hctx *rpc.HandlerContext) error {
	var tag uint32
	tag, hctx.Request, _ = basictl.NatReadTag(hctx.Request)
	switch tag {
	case constants.StatshouseGetConfig2:
		ud := getUserData(hctx)
		_, err := ud.getConfig2.Read(hctx.Request)
		if err != nil {
			return fmt.Errorf("failed to deserialize statshouse.getConfig2 request: %w", err)
		}
		return a.handleGetConfig2(ctx, hctx, ud.getConfig2)
	case constants.StatshouseGetMetrics3:
		ud := getUserData(hctx)
		_, err := ud.getMetrics3.Read(hctx.Request)
		if err != nil {
			return fmt.Errorf("failed to deserialize statshouse.getMetrics3 request: %w", err)
		}
		return a.metricStorage.Journal().HandleGetMetrics3(ctx, hctx, ud.getMetrics3)
	case constants.StatshouseGetTagMapping2:
		ud := getUserData(hctx)
		_, err := ud.getTagMapping2.Read(hctx.Request)
		if err != nil {
			return fmt.Errorf("failed to deserialize statshouse.getTagMapping2 request: %w", err)
		}
		return a.tagsMapper.handleCreateTagMapping(ctx, hctx, ud.getTagMapping2)
	case constants.StatshouseGetTagMappingBootstrap:
		ud := getUserData(hctx)
		_, err := ud.getTagBoostrap.Read(hctx.Request)
		if err != nil {
			return fmt.Errorf("failed to deserialize statshouse.getTagMappingBootstrap request: %w", err)
		}
		hctx.Response = append(hctx.Response, a.tagMappingBootstrapResponse...)
		return nil
	case constants.StatshouseSendKeepAlive2:
		{
			ud := getUserData(hctx)
			_, err := ud.sendKeepAlive2.Read(hctx.Request)
			if err != nil {
				return fmt.Errorf("failed to deserialize statshouse.sendKeepAlive2 request: %w", err)
			}
			return a.handleKeepAlive2(ctx, hctx, ud.sendKeepAlive2)
		}
	case constants.StatshouseSendSourceBucket2:
		{
			ud := getUserData(hctx)
			rawSize := len(hctx.Request)
			_, err := ud.sendSourceBucket2.Read(hctx.Request)
			if err != nil {
				return fmt.Errorf("failed to deserialize statshouse.sendSourceBucket2 request: %w", err)
			}
			var readFrom []byte
			if int(ud.sendSourceBucket2.OriginalSize) != len(ud.sendSourceBucket2.CompressedData) {
				if ud.sendSourceBucket2.OriginalSize > data_model.MaxUncompressedBucketSize {
					return fmt.Errorf("failed to deserialize compressed statshouse.sourceBucket - uncompressed size %d too big", ud.sendSourceBucket2.OriginalSize)
				}
				ud.uncompressed = append(ud.uncompressed[:0], make([]byte, int(ud.sendSourceBucket2.OriginalSize))...)
				s, err := lz4.UncompressBlock(ud.sendSourceBucket2.CompressedData, ud.uncompressed)
				if err != nil {
					return fmt.Errorf("failed to deserialize compressed statshouse.sourceBucket: %w", err)
				}
				if s != int(ud.sendSourceBucket2.OriginalSize) {
					return fmt.Errorf("failed to deserialize compressed statshouse.sourceBucket request: expected size %d actual %d", ud.sendSourceBucket2.OriginalSize, s)
				}
				ud.uncompressed = append(ud.uncompressed, 0, 0, 0, 0) // ingestion_status_ok2, TODO - remove when all agent are updated to version 1.0
				readFrom = ud.uncompressed
			} else {
				ud.sendSourceBucket2.CompressedData = append(ud.sendSourceBucket2.CompressedData, 0, 0, 0, 0) // ingestion_status_ok2, TODO - remove when all agent are updated to version 1.0
				readFrom = ud.sendSourceBucket2.CompressedData
			}
			// Uncomment if you change compressed bucket format
			// tag, _ := basictl.NatPeekTag(readFrom)
			// if tag == constants.StatshouseSourceBucket {
			//	_, err = ud.sourceBucket.ReadBoxed(readFrom)
			//	if err != nil {
			//		return fmt.Errorf("failed to deserialize statshouse.sourceBucket: %w", err)
			//	}
			//	return a.handleClientBucket(ctx, hctx, ud.sendSourceBucket, ud.sourceBucket, rawSize)
			// }
			_, err = ud.sourceBucket2.ReadBoxed(readFrom)
			if err != nil {
				return fmt.Errorf("failed to deserialize statshouse.sourceBucket2: %w", err)
			}
			return a.handleClientBucket2(ctx, hctx, ud.sendSourceBucket2, true, ud.sourceBucket2, rawSize)
		}
	case constants.StatshouseTestConnection2:
		{
			ud := getUserData(hctx)
			_, err := ud.testConneection2.Read(hctx.Request)
			if err != nil {
				return fmt.Errorf("failed to deserialize statshouse.testConneection2 request: %w", err)
			}
			return a.testConnection.handleTestConnection(ctx, hctx, ud.testConneection2)
		}
	case constants.StatshouseGetTargets2:
		{
			if a.promUpdater == nil {
				return fmt.Errorf("aggregator not configured for prometheus")
			}
			ud := getUserData(hctx)
			_, err := ud.getTargets2.Read(hctx.Request)
			if err != nil {
				return fmt.Errorf("failed to deserialize statshouse.getTargets2 request: %w", err)
			}
			return a.promUpdater.HandleGetTargets(ctx, hctx, ud.getTargets2)
		}
	case constants.StatshouseAutoCreate:
		{
			if a.autoCreate == nil {
				return rpc.Error{
					Code:        data_model.RPCErrorNoAutoCreate,
					Description: "aggregator not configured for auto-create",
				}
			}
			ud := getUserData(hctx)
			_, err := ud.autoCreate.Read(hctx.Request)
			if err != nil {
				return fmt.Errorf("failed to deserialize statshouse.autoCreate request: %w", err)
			}
			return a.autoCreate.handleAutoCreate(ctx, hctx, ud.autoCreate)
		}
	}
	return rpc.ErrNoHandler
}

func (a *Aggregator) getConfigResult() tlstatshouse.GetConfigResult {
	return tlstatshouse.GetConfigResult{
		Addresses:         a.addresses,
		MaxAddressesCount: int32(len(a.addresses)), // TODO - support reducing list,
		PreviousAddresses: int32(a.config.PreviousNumShards),
	}
}

func (a *Aggregator) getAgentEnv(isEnvStaging bool) int32 {
	if isEnvStaging {
		return format.TagValueIDStaging
	}
	return format.TagValueIDProduction
}

func (a *Aggregator) handleGetConfig2(_ context.Context, hctx *rpc.HandlerContext, args tlstatshouse.GetConfig2) (err error) {
	now := time.Now()
	host := a.tagsMapper.mapHost(now, []byte(args.Header.HostName), format.BuiltinMetricNameBudgetHost, false)
	// hack - we pass host through key0, because we can not yet set per metric host
	agentEnv := a.getAgentEnv(args.Header.IsSetAgentEnvStaging(args.FieldsMask))
	buildArch := format.FilterBuildArch(args.Header.BuildArch)
	route := int32(format.TagValueIDRouteDirect) // all config routes are direct

	if args.Cluster != a.config.Cluster {
		key := data_model.AggKey(0, format.BuiltinMetricIDAutoConfig, [16]int32{0, 0, 0, 0, format.TagValueIDAutoConfigWrongCluster}, a.aggregatorHost, a.shardKey, a.replicaKey)
		a.sh2.AddCounterHost(key.WithAgentEnvRouteArch(agentEnv, route, buildArch), 1, host, nil)
		return fmt.Errorf("statshouse misconfiguration! cluster requested %q does not match actual cluster connected %q", args.Cluster, a.config.Cluster)
	}
	key := data_model.AggKey(0, format.BuiltinMetricIDAutoConfig, [16]int32{0, 0, 0, 0, format.TagValueIDAutoConfigOK}, a.aggregatorHost, a.shardKey, a.replicaKey)
	a.sh2.AddCounterHost(key.WithAgentEnvRouteArch(agentEnv, route, buildArch), 1, host, nil)

	ret := a.getConfigResult()
	hctx.Response, err = args.WriteResult(hctx.Response, ret)
	return err
}

func (a *Aggregator) handleClientBucket2(_ context.Context, hctx *rpc.HandlerContext, args tlstatshouse.SendSourceBucket2Bytes, setShardReplica bool, bucket tlstatshouse.SourceBucket2Bytes, rawSize int) (err error) {
	if !args.IsSetHistoric() && a.config.SimulateRandomErrors > 0 && rand.Float64() < a.config.SimulateRandomErrors { // SimulateRandomErrors > 0 is optimization
		a.mu.Lock()
		defer a.mu.Unlock()
		aggBucket := a.recentBuckets[len(a.recentBuckets)-1]
		aggBucket.contributorsSimulatedErrors = append(aggBucket.contributorsSimulatedErrors, hctx)
		return hctx.HijackResponse()
	}
	now := time.Now()
	receiveDelay := now.Sub(time.Unix(int64(args.Time), 0)).Seconds()
	host := a.tagsMapper.mapHost(now, args.Header.HostName, format.BuiltinMetricNameBudgetHost, false)
	agentEnv := a.getAgentEnv(args.Header.IsSetAgentEnvStaging(args.FieldsMask))
	buildArch := format.FilterBuildArch(args.Header.BuildArch)

	addrIPV4, _ := addrIPString(hctx.RemoteAddr())
	if args.Header.AgentIp[3] != 0 {
		addrIPV4 = uint32(args.Header.AgentIp[3])
	}
	// opportunistic mapping. We do not map addrStr. To find hosts with hostname not set use internal_log

	var aggBucket *aggregatorBucket
	a.mu.Lock()
	if setShardReplica { // Skip old versions not yet updated
		if err := a.checkShardConfiguration(args.Header.ShardReplica, args.Header.ShardReplicaTotal); err != nil {
			key := data_model.AggKey(0, format.BuiltinMetricIDAutoConfig, [16]int32{0, 0, 0, 0, format.TagValueIDAutoConfigErrorSend, args.Header.ShardReplica, args.Header.ShardReplicaTotal}, a.aggregatorHost, a.shardKey, a.replicaKey)
			a.mu.Unlock()
			a.sh2.AddCounterHost(key, 1, host, nil)
			return err // TODO - return code so clients will print into log and discard data
		}
	}

	OldestTime := a.recentBuckets[0].time
	NewestTime := a.recentBuckets[len(a.recentBuckets)-1].time

	if args.IsSetHistoric() {
		if args.Time > NewestTime {
			key := data_model.AggKey(0, format.BuiltinMetricIDTimingErrors, [16]int32{0, format.TagValueIDTimingFutureBucketHistoric}, a.aggregatorHost, a.shardKey, a.replicaKey)
			a.mu.Unlock()
			a.sh2.AddValueCounterHost(key, float64(args.Time-NewestTime), 1, host)
			// We discard, because otherwise clients will flood aggregators with this data
			hctx.Response, err = args.WriteResult(hctx.Response, []byte("historic bucket time is too far in the future"))
			return err
		}
		if OldestTime >= data_model.MaxHistoricWindow+data_model.MaxHistoricWindowLag && args.Time < OldestTime-data_model.MaxHistoricWindow-data_model.MaxHistoricWindowLag {
			key := data_model.AggKey(0, format.BuiltinMetricIDTimingErrors, [16]int32{0, format.TagValueIDTimingLongWindowThrownAggregator}, a.aggregatorHost, a.shardKey, a.replicaKey)
			a.mu.Unlock()
			a.sh2.AddValueCounterHost(key, float64(NewestTime-args.Time), 1, host)
			hctx.Response, err = args.WriteResult(hctx.Response, []byte("Successfully discarded historic bucket beyond historic window"))
			return err
		}
		if args.Time < OldestTime {
			aggBucket = a.historicBuckets[args.Time]
			if aggBucket == nil {
				aggBucket = &aggregatorBucket{time: args.Time}
				a.historicBuckets[args.Time] = aggBucket
				if a.sendHistoricCondition() {
					a.cond.Broadcast() // we are not sure that Signal is enough
				}
			}
		} else {
			// If source receives error from recent conveyor quickly, it will come to spare while bucket is still recent
			// This is useful optimization, because can save half inserts
			aggBucket = a.recentBuckets[args.Time-OldestTime]
		}
	} else {
		if args.Time > NewestTime { // AgentShard too far in a future
			key := data_model.AggKey(0, format.BuiltinMetricIDTimingErrors, [16]int32{0, format.TagValueIDTimingFutureBucketRecent}, a.aggregatorHost, a.shardKey, a.replicaKey)
			a.mu.Unlock()
			a.sh2.AddValueCounterHost(key, float64(args.Time-NewestTime), 1, host)
			// We discard, because otherwise clients will flood aggregators with this data
			hctx.Response, err = args.WriteResult(hctx.Response, []byte("bucket time is too far in the future"))
			return err
		}
		if args.Time < OldestTime {
			key := data_model.AggKey(0, format.BuiltinMetricIDTimingErrors, [16]int32{0, format.TagValueIDTimingLateRecent}, a.aggregatorHost, a.shardKey, a.replicaKey)
			a.mu.Unlock()
			a.sh2.AddValueCounterHost(key, float64(NewestTime-args.Time), 1, host)
			return rpc.Error{
				Code:        data_model.RPCErrorMissedRecentConveyor,
				Description: "bucket time is too far in the past for recent conveyor",
			}
		}
		aggBucket = a.recentBuckets[args.Time-OldestTime]
	}

	aggBucket.sendMu.RLock()
	// This lock order ensures, that if sender gets a.mu.Lock(), then all aggregating clients already have aggBucket.sendMu.RLock()
	if args.IsSetSpare() {
		aggBucket.contributorsSpare.AddValueCounterHost(0, 1, host) // protected by a.mu
	} else {
		aggBucket.contributorsOriginal.AddValueCounterHost(0, 1, host) // protected by a.mu
	}
	aggBucket.contributors = append(aggBucket.contributors, hctx) // protected by a.mu
	aggBucket.rawSize += rawSize                                  // protected by a.mu
	aggHost := a.aggregatorHost
	a.mu.Unlock()
	defer aggBucket.sendMu.RUnlock()

	lockedShard := -1
	sampleFactors := map[int32]float32{}
	var newKeys []data_model.Key
	var usedMetrics []int32

	for _, v := range bucket.SampleFactors {
		sampleFactors[v.Metric] = v.Value
	}
	conveyor := int32(format.TagValueIDConveyorRecent)
	if args.IsSetHistoric() {
		conveyor = format.TagValueIDConveyorHistoric
	}
	spare := int32(format.TagValueIDAggregatorOriginal)
	if args.IsSetSpare() {
		spare = format.TagValueIDAggregatorSpare
	}
	route := int32(format.TagValueIDRouteDirect)
	if args.Header.IsSetIngressProxy(args.FieldsMask) {
		route = int32(format.TagValueIDRouteIngressProxy)
	}

	for _, item := range bucket.Metrics {
		k, sID := data_model.KeyFromStatshouseMultiItem(&item, args.Time)
		if k.Metric < 0 {
			k = k.WithAgentEnvRouteArch(agentEnv, route, buildArch)
			if k.Metric == format.BuiltinMetricIDAgentHeartbeatVersion {
				// Remap legacy metric to a new one
				k.Metric = format.BuiltinMetricIDHeartbeatVersion
				k.Keys[2] = k.Keys[1]
				k.Keys[1] = format.TagValueIDComponentAgent
			}
			if k.Metric == format.BuiltinMetricIDAgentHeartbeatArgs {
				// Remap legacy metric to a new one
				k.Metric = format.BuiltinMetricIDHeartbeatArgs
				k.Keys[2] = k.Keys[1]
				k.Keys[1] = format.TagValueIDComponentAgent
			}
			if k.Metric == format.BuiltinMetricIDHeartbeatVersion ||
				k.Metric == format.BuiltinMetricIDHeartbeatArgs || k.Metric == format.BuiltinMetricIDHeartbeatArgs2 ||
				k.Metric == format.BuiltinMetricIDHeartbeatArgs3 || k.Metric == format.BuiltinMetricIDHeartbeatArgs4 {
				// We need to set IP anyway, so set other keys here, not by source
				k.Keys[5] = args.BuildCommitDate
				k.Keys[6] = args.BuildCommitTs
				k.Keys[7] = host
				k.Keys[8] = int32(addrIPV4)
			}
			if k.Metric == format.BuiltinMetricIDRPCRequests {
				k.Keys[7] = host // agent cannot easily map its own host for now
			}
		}
		s := aggBucket.lockShard(&lockedShard, sID)
		created := false
		mi := data_model.MapKeyItemMultiItem(&s.multiItems, k, data_model.AggregatorStringTopCapacity, &created)
		mi.MergeWithTLMultiItem(&item, host)
		if created {
			if !args.IsSetSpare() { // Data from spares should not affect cardinality estimations
				newKeys = append(newKeys, k)
			}
			usedMetrics = append(usedMetrics, k.Metric)
		}
	}
	aggBucket.lockShard(&lockedShard, -1)

	aggBucket.mu.Lock()

	if aggBucket.usedMetrics == nil {
		aggBucket.usedMetrics = map[int32]struct{}{}
	}
	for _, m := range usedMetrics {
		aggBucket.usedMetrics[m] = struct{}{}
	}
	aggBucket.mu.Unlock()

	// newKeys will not be large, if average cardinality is low
	// we update estimators under sendMu.RLock so that sample factors used for inserting will be already updated
	a.estimator.UpdateWithKeys(args.Time, newKeys)

	now2 := time.Now()
	// Write meta metrics. They all simply go to shard 0 independent of their keys.
	s := aggBucket.lockShard(&lockedShard, 0)
	getMultiItem := func(t uint32, m int32, keys [16]int32) *data_model.MultiItem {
		key := data_model.AggKey(t, m, keys, aggHost, a.shardKey, a.replicaKey).WithAgentEnvRouteArch(agentEnv, route, buildArch)
		return data_model.MapKeyItemMultiItem(&s.multiItems, key, data_model.AggregatorStringTopCapacity, nil)
	}
	getMultiItem(args.Time, format.BuiltinMetricIDAggSizeCompressed, [16]int32{0, 0, 0, 0, conveyor, spare}).Tail.AddValueCounterHost(float64(rawSize), 1, host)

	getMultiItem(args.Time, format.BuiltinMetricIDAggSizeUncompressed, [16]int32{0, 0, 0, 0, conveyor, spare}).Tail.AddValueCounterHost(float64(args.OriginalSize), 1, host)
	getMultiItem(args.Time, format.BuiltinMetricIDAggBucketReceiveDelaySec, [16]int32{0, 0, 0, 0, conveyor, spare, format.TagValueIDSecondReal}).Tail.AddValueCounterHost(receiveDelay, 1, host)
	for i := uint32(0); i < bucket.MissedSeconds && i < data_model.MaxMissedSecondsIntoContributors; i++ {
		d := receiveDelay - float64(i+1)
		getMultiItem(args.Time+i, format.BuiltinMetricIDAggBucketReceiveDelaySec, [16]int32{0, 0, 0, 0, conveyor, spare, format.TagValueIDSecondPhantom}).Tail.AddValueCounterHost(d, 1, host)
	}
	getMultiItem(args.Time, format.BuiltinMetricIDAggBucketAggregateTimeSec, [16]int32{0, 0, 0, 0, conveyor, spare}).Tail.AddValueCounterHost(now2.Sub(now).Seconds(), 1, host)
	getMultiItem(args.Time, format.BuiltinMetricIDAggAdditionsToEstimator, [16]int32{0, 0, 0, 0, conveyor, spare}).Tail.AddValueCounterHost(float64(len(newKeys)), 1, host)
	if bucket.MissedSeconds != 0 {
		getMultiItem(args.Time, format.BuiltinMetricIDTimingErrors, [16]int32{0, format.TagValueIDTimingMissedSeconds}).Tail.AddValueCounterHost(float64(bucket.MissedSeconds), 1, host)
	}
	if args.QueueSizeMemory != 0 {
		getMultiItem(args.Time, format.BuiltinMetricIDAgentHistoricQueueSize, [16]int32{0, format.TagValueIDHistoricQueueMemory}).Tail.AddValueCounterHost(float64(args.QueueSizeMemory), 1, host)
	}
	if args.QueueSizeDisk != 0 {
		getMultiItem(args.Time, format.BuiltinMetricIDAgentHistoricQueueSize, [16]int32{0, format.TagValueIDHistoricQueueDisk}).Tail.AddValueCounterHost(float64(args.QueueSizeDisk), 1, host)
	}

	var bcStr []byte
	componentTag := args.Header.ComponentTag
	if componentTag != format.TagValueIDComponentAgent && componentTag != format.TagValueIDComponentAggregator &&
		componentTag != format.TagValueIDComponentIngressProxy && componentTag != format.TagValueIDComponentAPI {
		// TODO - remove this if after release, because no more agents will send crap here
		componentTag = format.TagValueIDComponentAgent
	}
	if format.ValidStringValue(mem.B(args.BuildCommit)) {
		bcStr = args.BuildCommit
	}
	// This cheap version metric is not affected by agent sampling algorithm in contrast with __heartbeat_version
	getMultiItem((args.Time/60)*60, format.BuiltinMetricIDVersions, [16]int32{0, 0, componentTag, args.BuildCommitDate, args.BuildCommitTs}).MapStringTopBytes(bcStr, 1).AddCounterHost(1, host)

	for _, v := range bucket.SampleFactors {
		// We probably wish to stop splitting by aggregator, because this metric is taking already too much space - about 2% of all data
		// Counter will be +1 for each agent who sent bucket for this second, so millions.
		getMultiItem(args.Time, format.BuiltinMetricIDAgentSamplingFactor, [16]int32{0, v.Metric}).Tail.AddValueCounterHost(float64(v.Value), 1, host)
	}
	ingestionStatus := func(env int32, metricID int32, status int32, value float32) {
		data_model.MapKeyItemMultiItem(&s.multiItems, (data_model.Key{Timestamp: args.Time, Metric: format.BuiltinMetricIDIngestionStatus, Keys: [16]int32{env, metricID, status}}).WithAgentEnvRouteArch(agentEnv, route, buildArch), data_model.AggregatorStringTopCapacity, nil).Tail.AddCounterHost(float64(value), host)
	}
	for _, v := range bucket.IngestionStatusOk {
		// We do not split by aggregator, because this metric is taking already too much space - about 1% of all data
		if v.Value > 0 {
			ingestionStatus(0, v.Metric, format.TagValueIDSrcIngestionStatusOKCached, v.Value)
		} else {
			ingestionStatus(0, v.Metric, format.TagValueIDSrcIngestionStatusOKUncached, -v.Value)
		}
	}
	for _, v := range bucket.IngestionStatusOk2 {
		// We do not split by aggregator, because this metric is taking already too much space - about 1% of all data
		if v.Value > 0 {
			ingestionStatus(v.Env, v.Metric, format.TagValueIDSrcIngestionStatusOKCached, v.Value)
		} else {
			// clients before release used count < 0 for uncached status and count > 0 for cached
			// but there is too little uncached statuses for such optimization, and we wanted
			// to pass tag that caused uncached loading, so we removed this negative value tweak from agents
			ingestionStatus(v.Env, v.Metric, format.TagValueIDSrcIngestionStatusOKUncached, -v.Value)
		}
	}
	aggBucket.lockShard(&lockedShard, -1)
	return hctx.HijackResponse()
}

func (a *Aggregator) handleKeepAlive2(_ context.Context, hctx *rpc.HandlerContext, args tlstatshouse.SendKeepAlive2Bytes) error {
	now := time.Now()
	host := a.tagsMapper.mapHost(now, args.Header.HostName, format.BuiltinMetricNameBudgetHost, false)
	agentEnv := a.getAgentEnv(args.Header.IsSetAgentEnvStaging(args.FieldsMask))
	buildArch := format.FilterBuildArch(args.Header.BuildArch)

	a.mu.Lock()
	if err := a.checkShardConfiguration(args.Header.ShardReplica, args.Header.ShardReplicaTotal); err != nil {
		key := data_model.AggKey(0, format.BuiltinMetricIDAutoConfig, [16]int32{0, 0, 0, 0, format.TagValueIDAutoConfigErrorKeepAlive, args.Header.ShardReplica, args.Header.ShardReplicaTotal}, a.aggregatorHost, a.shardKey, a.replicaKey)
		a.mu.Unlock()
		a.sh2.AddCounterHost(key, 1, host, nil)
		return err
	}
	aggBucket := a.recentBuckets[0] // Most ready for insert
	aggBucket.sendMu.RLock()
	// This lock order ensures, that if sender gets a.mu.Lock(), then all aggregating clients already have aggBucket.sendMu.RLock()
	aggBucket.contributors = append(aggBucket.contributors, hctx) // protected by a.mu
	aggHost := a.aggregatorHost
	a.mu.Unlock()
	defer aggBucket.sendMu.RUnlock()
	// Write meta statistics
	route := int32(format.TagValueIDRouteDirect)
	if args.Header.IsSetIngressProxy(args.FieldsMask) {
		route = int32(format.TagValueIDRouteIngressProxy)
	}

	lockedShard := -1
	s := aggBucket.lockShard(&lockedShard, 0)
	// Counters can contain this metrics while # of contributors is 0. We compensate by adding small fixed budget.
	data_model.MapKeyItemMultiItem(&s.multiItems, data_model.AggKey(aggBucket.time, format.BuiltinMetricIDAggKeepAlive, [16]int32{}, aggHost, a.shardKey, a.replicaKey).WithAgentEnvRouteArch(agentEnv, route, buildArch), data_model.AggregatorStringTopCapacity, nil).Tail.AddCounterHost(1, host)
	aggBucket.lockShard(&lockedShard, -1)

	return hctx.HijackResponse()
}
