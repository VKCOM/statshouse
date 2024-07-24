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
	"time"

	"go4.org/mem"
	"pgregory.net/rand"

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
	autoCreate        tlstatshouse.AutoCreateBytes
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
	protocol := int32(hctx.ProtocolVersion())
	requestLen := len(hctx.Request) // impl will release hctx
	key := a.aggKey(uint32(hctx.RequestTime.Unix()), format.BuiltinMetricIDRPCRequests, [16]int32{0, format.TagValueIDComponentAggregator, int32(tag), format.TagValueIDRPCRequestsStatusOK, 0, 0, keyIDTag, 0, protocol})
	err := a.handleClientImpl(ctx, hctx)
	if err == rpc.ErrNoHandler {
		key.Keys[3] = format.TagValueIDRPCRequestsStatusNoHandler
	} else if rpc.IsHijackedResponse(err) {
		key.Keys[3] = format.TagValueIDRPCRequestsStatusHijack
	} else if err != nil {
		key.Keys[3] = format.TagValueIDRPCRequestsStatusErrLocal
	}
	a.sh2.AddValueCounter(key, float64(requestLen), 1, nil)
	return err
}

// TODO - use generatedd handler
func (a *Aggregator) handleClientImpl(ctx context.Context, hctx *rpc.HandlerContext) error {
	var tag uint32
	tag, hctx.Request, _ = basictl.NatReadTag(hctx.Request)
	switch tag {
	case constants.StatshouseGetConfig2:
		hctx.RequestFunctionName = "statshouse.getConfig2"
		ud := getUserData(hctx)
		_, err := ud.getConfig2.Read(hctx.Request)
		if err != nil {
			return fmt.Errorf("failed to deserialize statshouse.getConfig2 request: %w", err)
		}
		return a.handleGetConfig2(ctx, hctx, ud.getConfig2)
	case constants.StatshouseGetMetrics3:
		hctx.RequestFunctionName = "statshouse.getMetrics3"
		ud := getUserData(hctx)
		_, err := ud.getMetrics3.Read(hctx.Request)
		if err != nil {
			return fmt.Errorf("failed to deserialize statshouse.getMetrics3 request: %w", err)
		}
		return a.metricStorage.Journal().HandleGetMetrics3(ctx, hctx, ud.getMetrics3)
	case constants.StatshouseGetTagMapping2:
		hctx.RequestFunctionName = "statshouse.getTagMapping2"
		ud := getUserData(hctx)
		_, err := ud.getTagMapping2.Read(hctx.Request)
		if err != nil {
			return fmt.Errorf("failed to deserialize statshouse.getTagMapping2 request: %w", err)
		}
		return a.tagsMapper.handleCreateTagMapping(ctx, hctx, ud.getTagMapping2)
	case constants.StatshouseGetTagMappingBootstrap:
		hctx.RequestFunctionName = "statshouse.getTagMappingBootstrap"
		ud := getUserData(hctx)
		_, err := ud.getTagBoostrap.Read(hctx.Request)
		if err != nil {
			return fmt.Errorf("failed to deserialize statshouse.getTagMappingBootstrap request: %w", err)
		}
		hctx.Response = append(hctx.Response, a.tagMappingBootstrapResponse...)
		return nil
	case constants.StatshouseSendKeepAlive2:
		hctx.RequestFunctionName = "statshouse.sendKeepAlive2"
		{
			ud := getUserData(hctx)
			_, err := ud.sendKeepAlive2.Read(hctx.Request)
			if err != nil {
				return fmt.Errorf("failed to deserialize statshouse.sendKeepAlive2 request: %w", err)
			}
			return a.handleKeepAlive2(ctx, hctx, ud.sendKeepAlive2)
		}
	case constants.StatshouseSendSourceBucket2:
		hctx.RequestFunctionName = "statshouse.sendSourceBucket2"
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
				// uncomment if you add fields to the TL
				ud.uncompressed = append(ud.uncompressed, 0, 0, 0, 0) // ingestion_status_ok2, TODO - remove when all agent are updated to version 1.0
				readFrom = ud.uncompressed
			} else {
				// uncomment if you add fields to the TL
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
			return a.handleClientBucket(ctx, hctx, ud.sendSourceBucket2, true, ud.sourceBucket2, rawSize)
		}
	case constants.StatshouseTestConnection2:
		hctx.RequestFunctionName = "statshouse.testConnection2"
		{
			ud := getUserData(hctx)
			_, err := ud.testConneection2.Read(hctx.Request)
			if err != nil {
				return fmt.Errorf("failed to deserialize statshouse.testConneection2 request: %w", err)
			}
			return a.testConnection.handleTestConnection(ctx, hctx, ud.testConneection2)
		}
	case constants.StatshouseGetTargets2:
		hctx.RequestFunctionName = "statshouse.getTargets2"
		{
			ud := getUserData(hctx)
			_, err := ud.getTargets2.Read(hctx.Request)
			if err != nil {
				return fmt.Errorf("failed to deserialize statshouse.getTargets2 request: %w", err)
			}
			return a.scrape.handleGetTargets(ctx, hctx, ud.getTargets2)
		}
	case constants.StatshouseAutoCreate:
		hctx.RequestFunctionName = "statshouse.autoCreate"
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
		Ts:                time.Now().UnixMilli(),
	}
}

func (a *Aggregator) getAgentEnv(isEnvStaging bool) int32 {
	if isEnvStaging {
		return format.TagValueIDStaging
	}
	return format.TagValueIDProduction
}

func (a *Aggregator) aggKey(t uint32, m int32, k [format.MaxTags]int32) data_model.Key {
	return data_model.AggKey(t, m, k, a.aggregatorHost, a.shardKey, a.replicaKey)
}

func (a *Aggregator) handleGetConfig2(_ context.Context, hctx *rpc.HandlerContext, args tlstatshouse.GetConfig2) (err error) {
	now := time.Now()
	nowUnix := uint32(now.Unix())
	host := a.tagsMapper.mapOrFlood(now, []byte(args.Header.HostName), format.BuiltinMetricNameBudgetHost, false)
	agentEnv := a.getAgentEnv(args.Header.IsSetAgentEnvStaging(args.FieldsMask))
	buildArch := format.FilterBuildArch(args.Header.BuildArch)
	route := int32(format.TagValueIDRouteDirect)
	if args.Header.IsSetIngressProxy(args.FieldsMask) {
		route = int32(format.TagValueIDRouteIngressProxy)
	}

	if args.Cluster != a.config.Cluster {
		key := a.aggKey(nowUnix, format.BuiltinMetricIDAutoConfig, [16]int32{0, 0, 0, 0, format.TagValueIDAutoConfigWrongCluster})
		key = key.WithAgentEnvRouteArch(agentEnv, route, buildArch)
		a.sh2.AddCounterHost(key, 1, host, nil)
		return fmt.Errorf("statshouse misconfiguration! cluster requested %q does not match actual cluster connected %q", args.Cluster, a.config.Cluster)
	}
	key := a.aggKey(nowUnix, format.BuiltinMetricIDAutoConfig, [16]int32{0, 0, 0, 0, format.TagValueIDAutoConfigOK})
	key = key.WithAgentEnvRouteArch(agentEnv, route, buildArch)
	a.sh2.AddCounterHost(key, 1, host, nil)

	ret := a.getConfigResult()
	hctx.Response, err = args.WriteResult(hctx.Response, ret)
	return err
}

func (a *Aggregator) handleClientBucket(_ context.Context, hctx *rpc.HandlerContext, args tlstatshouse.SendSourceBucket2Bytes, setShardReplica bool, bucket tlstatshouse.SourceBucket2Bytes, rawSize int) (err error) {
	now := time.Now()
	nowUnix := uint32(now.Unix())
	receiveDelay := now.Sub(time.Unix(int64(args.Time), 0)).Seconds()
	// All hosts must be valid and non-empty
	host := a.tagsMapper.mapOrFlood(now, args.Header.HostName, format.BuiltinMetricNameBudgetHost, false)
	var owner int32
	if args.IsSetOwner() {
		owner = a.tagsMapper.mapOrFlood(now, args.Owner, format.BuiltinMetricNameBudgetOwner, false)
	}
	agentEnv := a.getAgentEnv(args.Header.IsSetAgentEnvStaging(args.FieldsMask))
	buildArch := format.FilterBuildArch(args.Header.BuildArch)
	route := int32(format.TagValueIDRouteDirect)
	if args.Header.IsSetIngressProxy(args.FieldsMask) {
		route = int32(format.TagValueIDRouteIngressProxy)
	}
	var bcStr []byte
	bcTag := int32(0)
	if format.ValidStringValue(mem.B(args.BuildCommit)) {
		bcStr = args.BuildCommit
		bcStrRaw, _ := hex.DecodeString(string(bcStr))
		if len(bcStrRaw) >= 4 {
			bcTag = int32(binary.BigEndian.Uint32(bcStrRaw))
		}
	}
	// TODO - if bcTag == 0 || args.BuildCommitTs == 0 { reply with error "agent must be built correctly with commit timestamp and hash"

	addrIPV4, _ := addrIPString(hctx.RemoteAddr())
	if args.Header.AgentIp[3] != 0 {
		addrIPV4 = uint32(args.Header.AgentIp[3])
	}
	// opportunistic mapping. We do not map addrStr. To find hosts with hostname not set use internal_log

	if a.configR.DenyOldAgents && format.LeastAllowedAgentCommitTs > 0 {
		if args.BuildCommitTs < format.LeastAllowedAgentCommitTs {
			key := a.aggKey(nowUnix, format.BuiltinMetricIDAggOutdatedAgents, [16]int32{0, 0, 0, 0, owner, host, int32(addrIPV4)})
			key = key.WithAgentEnvRouteArch(agentEnv, route, buildArch)
			a.sh2.AddCounterHost(key, 1, host, nil)
			hctx.Response, _ = args.WriteResult(hctx.Response, []byte("agent is too old please update"))
			return nil
		}
	}

	var aggBucket *aggregatorBucket
	a.mu.Lock()
	if setShardReplica { // Skip old versions not yet updated
		if err := a.checkShardConfiguration(args.Header.ShardReplica, args.Header.ShardReplicaTotal); err != nil {
			a.mu.Unlock()
			key := a.aggKey(nowUnix, format.BuiltinMetricIDAutoConfig, [16]int32{0, 0, 0, 0, format.TagValueIDAutoConfigErrorSend, args.Header.ShardReplica, args.Header.ShardReplicaTotal})
			key = key.WithAgentEnvRouteArch(agentEnv, route, buildArch)
			a.sh2.AddCounterHost(key, 1, host, nil)
			return err // TODO - return code so clients will print into log and discard data
		}
	}

	oldestTime := a.recentBuckets[0].time
	newestTime := a.recentBuckets[len(a.recentBuckets)-1].time

	// Each of 3 replicas are responsible for inserting each consecutive second.
	// If primary replica for the second is unavailable, agent sends bucket to spare replica responsible for the next second, so we must round up here.
	// Also, old agents send all buckets to each replica.
	roundedToOurTime := args.Time
	for roundedToOurTime%3 != uint32(a.replicaKey-1) {
		roundedToOurTime++
	}

	if args.IsSetHistoric() {
		if roundedToOurTime > newestTime {
			a.mu.Unlock()
			key := a.aggKey(nowUnix, format.BuiltinMetricIDTimingErrors, [16]int32{0, format.TagValueIDTimingFutureBucketHistoric})
			key = key.WithAgentEnvRouteArch(agentEnv, route, buildArch)
			a.sh2.AddValueCounterHost(key, float64(args.Time)-float64(newestTime), 1, host)
			// We discard, because otherwise clients will flood aggregators with this data
			hctx.Response, _ = args.WriteResult(hctx.Response, []byte("historic bucket time is too far in the future"))
			return nil
		}
		if oldestTime >= data_model.MaxHistoricWindow && roundedToOurTime < oldestTime-data_model.MaxHistoricWindow {
			a.mu.Unlock()
			key := a.aggKey(nowUnix, format.BuiltinMetricIDTimingErrors, [16]int32{0, format.TagValueIDTimingLongWindowThrownAggregator})
			key = key.WithAgentEnvRouteArch(agentEnv, route, buildArch)
			a.sh2.AddValueCounterHost(key, float64(newestTime)-float64(args.Time), 1, host)
			hctx.Response, _ = args.WriteResult(hctx.Response, []byte("Successfully discarded historic bucket beyond historic window"))
			return nil
		}
		if roundedToOurTime < oldestTime {
			aggBucket = a.historicBuckets[args.Time]
			if aggBucket == nil {
				aggBucket = &aggregatorBucket{
					time:                        args.Time,
					contributors:                map[*rpc.HandlerContext]struct{}{},
					contributorsSimulatedErrors: map[*rpc.HandlerContext]struct{}{},
				}
				a.historicBuckets[args.Time] = aggBucket
			}
		} else {
			// If source receives error from recent conveyor quickly, it will come to spare while bucket is still recent
			// This is useful optimization, because can save half inserts
			aggBucket = a.recentBuckets[roundedToOurTime-oldestTime]
		}
	} else {
		if roundedToOurTime > newestTime { // AgentShard too far in a future
			a.mu.Unlock()
			key := a.aggKey(nowUnix, format.BuiltinMetricIDTimingErrors, [16]int32{0, format.TagValueIDTimingFutureBucketRecent})
			key = key.WithAgentEnvRouteArch(agentEnv, route, buildArch)
			a.sh2.AddValueCounterHost(key, float64(args.Time)-float64(newestTime), 1, host)
			// We discard, because otherwise clients will flood aggregators with this data
			hctx.Response, _ = args.WriteResult(hctx.Response, []byte("bucket time is too far in the future"))
			return nil
		}
		if roundedToOurTime < oldestTime {
			a.mu.Unlock()
			key := a.aggKey(nowUnix, format.BuiltinMetricIDTimingErrors, [16]int32{0, format.TagValueIDTimingLateRecent})
			key = key.WithAgentEnvRouteArch(agentEnv, route, buildArch)
			a.sh2.AddValueCounterHost(key, float64(newestTime)-float64(args.Time), 1, host)
			return rpc.Error{
				Code:        data_model.RPCErrorMissedRecentConveyor,
				Description: "bucket time is too far in the past for recent conveyor",
			}
		}
		aggBucket = a.recentBuckets[roundedToOurTime-oldestTime]
		if a.config.SimulateRandomErrors > 0 && rand.Float64() < a.config.SimulateRandomErrors { // SimulateRandomErrors > 0 is optimization
			// repeat lock dance in aggregation code below
			aggBucket.sendMu.RLock()
			a.mu.Unlock()
			defer aggBucket.sendMu.RUnlock()
			aggBucket.mu.Lock()
			defer aggBucket.mu.Unlock()

			aggBucket.contributorsSimulatedErrors[hctx] = struct{}{} // must be under bucket lock
			return hctx.HijackResponse(aggBucket)                    // must be under bucket lock
		}
	}

	aggBucket.sendMu.RLock()
	// This lock order ensures, that if sender gets a.mu.Lock(), then all aggregating clients already have aggBucket.sendMu.RLock()
	if args.IsSetSpare() {
		aggBucket.contributorsSpare.AddCounterHost(1, host) // protected by a.mu
	} else {
		aggBucket.contributorsOriginal.AddCounterHost(1, host) // protected by a.mu
	}
	a.mu.Unlock()
	defer aggBucket.sendMu.RUnlock()

	lockedShard := -1
	var newKeys []data_model.Key
	var usedMetrics []int32

	// We do not want to decompress under lock, so we decompress before ifs, then rarely throw away decompressed data.

	conveyor := int32(format.TagValueIDConveyorRecent)
	if args.IsSetHistoric() {
		conveyor = format.TagValueIDConveyorHistoric
	}
	spare := int32(format.TagValueIDAggregatorOriginal)
	if args.IsSetSpare() {
		spare = format.TagValueIDAggregatorSpare
	}

	for _, item := range bucket.Metrics {
		k, sID := data_model.KeyFromStatshouseMultiItem(&item, args.Time, newestTime)
		if k.Metric < 0 && !format.HardwareMetric(k.Metric) {
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
				k.Metric == format.BuiltinMetricIDHeartbeatArgs {
				// In case of agent we need to set IP anyway, so set other keys here, not by source
				// In case of api other tags are already set, so don't overwrite them
				if k.Keys[4] == 0 {
					k.Keys[4] = bcTag
				}
				if k.Keys[5] == 0 {
					k.Keys[5] = args.BuildCommitDate
				}
				if k.Keys[6] == 0 {
					k.Keys[6] = args.BuildCommitTs
				}
				if k.Keys[7] == 0 {
					k.Keys[7] = host
				}
				// Valid for api as well because it is on the same host as agent
				k.Keys[8] = int32(addrIPV4)
				k.Keys[9] = owner
			}
			if k.Metric == format.BuiltinMetricIDRPCRequests {
				k.Keys[7] = host // agent cannot easily map its own host for now
			}
		}
		s := aggBucket.lockShard(&lockedShard, sID)
		created := false
		mi := data_model.MapKeyItemMultiItem(&s.multiItems, k, data_model.AggregatorStringTopCapacity, nil, &created)
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
	aggBucket.contributors[hctx] = struct{}{}   // must be under bucket lock
	errHijack := hctx.HijackResponse(aggBucket) // must be under bucket lock

	aggBucket.mu.Unlock()

	// newKeys will not be large, if average cardinality is low
	// we update estimators under sendMu.RLock so that sample factors used for inserting will be already updated
	a.estimator.UpdateWithKeys(args.Time, newKeys)

	now2 := time.Now()
	// Write meta metrics. They all simply go to shard 0 independent of their keys.
	s := aggBucket.lockShard(&lockedShard, 0)
	getMultiItem := func(t uint32, m int32, keys [16]int32) *data_model.MultiItem {
		key := a.aggKey(t, m, keys)
		key = key.WithAgentEnvRouteArch(agentEnv, route, buildArch)
		return data_model.MapKeyItemMultiItem(&s.multiItems, key, data_model.AggregatorStringTopCapacity, nil, nil)
	}
	getMultiItem(args.Time, format.BuiltinMetricIDAggSizeCompressed, [16]int32{0, 0, 0, 0, conveyor, spare}).Tail.AddValueCounterHost(float64(rawSize), 1, host)

	getMultiItem(args.Time, format.BuiltinMetricIDAggSizeUncompressed, [16]int32{0, 0, 0, 0, conveyor, spare}).Tail.AddValueCounterHost(float64(args.OriginalSize), 1, host)
	getMultiItem(args.Time, format.BuiltinMetricIDAggBucketReceiveDelaySec, [16]int32{0, 0, 0, 0, conveyor, spare, format.TagValueIDSecondReal}).Tail.AddValueCounterHost(receiveDelay, 1, host)
	getMultiItem(args.Time, format.BuiltinMetricIDAggBucketAggregateTimeSec, [16]int32{0, 0, 0, 0, conveyor, spare}).Tail.AddValueCounterHost(now2.Sub(now).Seconds(), 1, host)
	getMultiItem(args.Time, format.BuiltinMetricIDAggAdditionsToEstimator, [16]int32{0, 0, 0, 0, conveyor, spare}).Tail.AddValueCounterHost(float64(len(newKeys)), 1, host)
	if bucket.MissedSeconds != 0 { // TODO - remove after all agents upgraded to write this metric with tag format.TagValueIDTimingMissedSecondsAgent
		getMultiItem(args.Time, format.BuiltinMetricIDTimingErrors, [16]int32{0, format.TagValueIDTimingMissedSeconds}).Tail.AddValueCounterHost(float64(bucket.MissedSeconds), 1, host)
	}
	if args.QueueSizeMemory > 0 {
		getMultiItem(args.Time, format.BuiltinMetricIDAgentHistoricQueueSize, [16]int32{0, format.TagValueIDHistoricQueueMemory}).Tail.AddValueCounterHost(float64(args.QueueSizeMemory), 1, host)
	}
	if args.QueueSizeMemorySum > 0 {
		getMultiItem(args.Time, format.BuiltinMetricIDAgentHistoricQueueSizeSum, [16]int32{0, format.TagValueIDHistoricQueueMemory}).Tail.AddValueCounterHost(float64(args.QueueSizeMemorySum), 1, host)
	}
	if args.QueueSizeDiskUnsent > 0 {
		getMultiItem(args.Time, format.BuiltinMetricIDAgentHistoricQueueSize, [16]int32{0, format.TagValueIDHistoricQueueDiskUnsent}).Tail.AddValueCounterHost(float64(args.QueueSizeDiskUnsent), 1, host)
	}
	queueSizeDiskSent := float64(args.QueueSizeDisk) - float64(args.QueueSizeDiskUnsent)
	if queueSizeDiskSent > 0 {
		getMultiItem(args.Time, format.BuiltinMetricIDAgentHistoricQueueSize, [16]int32{0, format.TagValueIDHistoricQueueDiskSent}).Tail.AddValueCounterHost(float64(queueSizeDiskSent), 1, host)
	}
	if args.QueueSizeDiskSumUnsent > 0 {
		getMultiItem(args.Time, format.BuiltinMetricIDAgentHistoricQueueSizeSum, [16]int32{0, format.TagValueIDHistoricQueueDiskUnsent}).Tail.AddValueCounterHost(float64(args.QueueSizeDiskSumUnsent), 1, host)
	}
	queueSizeDiskSumSent := float64(args.QueueSizeDiskSum) - float64(args.QueueSizeDiskSumUnsent)
	if queueSizeDiskSumSent > 0 {
		getMultiItem(args.Time, format.BuiltinMetricIDAgentHistoricQueueSizeSum, [16]int32{0, format.TagValueIDHistoricQueueDiskSent}).Tail.AddValueCounterHost(float64(queueSizeDiskSumSent), 1, host)
	}

	componentTag := args.Header.ComponentTag
	if componentTag != format.TagValueIDComponentAgent && componentTag != format.TagValueIDComponentAggregator &&
		componentTag != format.TagValueIDComponentIngressProxy && componentTag != format.TagValueIDComponentAPI {
		// TODO - remove this if after release, because no more agents will send crap here
		componentTag = format.TagValueIDComponentAgent
	}
	// This cheap version metric is not affected by agent sampling algorithm in contrast with __heartbeat_version
	getMultiItem((args.Time/60)*60, format.BuiltinMetricIDVersions, [16]int32{0, 0, componentTag, args.BuildCommitDate, args.BuildCommitTs, bcTag}).MapStringTopBytes(bcStr, 1).AddCounterHost(1, host)

	for _, v := range bucket.SampleFactors {
		// We probably wish to stop splitting by aggregator, because this metric is taking already too much space - about 2% of all data
		// Counter will be +1 for each agent who sent bucket for this second, so millions.
		getMultiItem(args.Time, format.BuiltinMetricIDAgentSamplingFactor, [16]int32{0, v.Metric}).Tail.AddValueCounterHost(float64(v.Value), 1, host)
	}
	ingestionStatus := func(env int32, metricID int32, status int32, value float32) {
		data_model.MapKeyItemMultiItem(&s.multiItems, (data_model.Key{Timestamp: args.Time, Metric: format.BuiltinMetricIDIngestionStatus, Keys: [16]int32{env, metricID, status}}).WithAgentEnvRouteArch(agentEnv, route, buildArch), data_model.AggregatorStringTopCapacity, nil, nil).Tail.AddCounterHost(float64(value), host)
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
	return errHijack
}

func (a *Aggregator) handleKeepAlive2(_ context.Context, hctx *rpc.HandlerContext, args tlstatshouse.SendKeepAlive2Bytes) error {
	now := time.Now()
	nowUnix := uint32(now.Unix())
	host := a.tagsMapper.mapOrFlood(now, args.Header.HostName, format.BuiltinMetricNameBudgetHost, false)
	agentEnv := a.getAgentEnv(args.Header.IsSetAgentEnvStaging(args.FieldsMask))
	buildArch := format.FilterBuildArch(args.Header.BuildArch)
	route := int32(format.TagValueIDRouteDirect)
	if args.Header.IsSetIngressProxy(args.FieldsMask) {
		route = int32(format.TagValueIDRouteIngressProxy)
	}

	a.mu.Lock()
	if err := a.checkShardConfiguration(args.Header.ShardReplica, args.Header.ShardReplicaTotal); err != nil {
		a.mu.Unlock()
		key := a.aggKey(nowUnix, format.BuiltinMetricIDAutoConfig, [16]int32{0, 0, 0, 0, format.TagValueIDAutoConfigErrorKeepAlive, args.Header.ShardReplica, args.Header.ShardReplicaTotal})
		key = key.WithAgentEnvRouteArch(agentEnv, route, buildArch)
		a.sh2.AddCounterHost(key, 1, host, nil)
		return err
	}
	oldestTime := a.recentBuckets[0].time // Most ready for insert
	roundedToOurTime := oldestTime
	for roundedToOurTime%3 != uint32(a.replicaKey-1) {
		roundedToOurTime++
	}
	aggBucket := a.recentBuckets[roundedToOurTime-oldestTime]
	aggBucket.sendMu.RLock()
	// This lock order ensures, that if sender gets a.mu.Lock(), then all aggregating clients already have aggBucket.sendMu.RLock()
	a.mu.Unlock()
	defer aggBucket.sendMu.RUnlock()

	aggBucket.mu.Lock()
	aggBucket.contributors[hctx] = struct{}{}   // must be under bucket lock
	errHijack := hctx.HijackResponse(aggBucket) // must be under bucket lock
	aggBucket.mu.Unlock()
	// Write meta statistics

	lockedShard := -1
	s := aggBucket.lockShard(&lockedShard, 0)
	// Counters can contain this metrics while # of contributors is 0. We compensate by adding small fixed budget.
	key := a.aggKey(aggBucket.time, format.BuiltinMetricIDAggKeepAlive, [16]int32{})
	key = key.WithAgentEnvRouteArch(agentEnv, route, buildArch)
	data_model.MapKeyItemMultiItem(&s.multiItems, key, data_model.AggregatorStringTopCapacity, nil, nil).Tail.AddCounterHost(1, host)
	aggBucket.lockShard(&lockedShard, -1)

	return errHijack
}
