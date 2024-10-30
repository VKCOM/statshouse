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

	"github.com/pierrec/lz4"
	"go4.org/mem"
	"pgregory.net/rand"

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/vkgo/basictl"
	"github.com/vkcom/statshouse/internal/vkgo/build"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
)

func bool2int(b bool) int { // freaking golang clowns
	if b {
		return 1
	}
	return 0
}

func (a *Aggregator) handleClient(ctx context.Context, hctx *rpc.HandlerContext) error {
	tag, _ := basictl.NatPeekTag(hctx.Request)
	keyID := hctx.KeyID()
	keyIDTag := int32(binary.BigEndian.Uint32(keyID[:4]))
	protocol := int32(hctx.ProtocolVersion())
	requestLen := len(hctx.Request) // impl will release hctx
	key := a.aggKey(uint32(hctx.RequestTime.Unix()), format.BuiltinMetricIDRPCRequests, [16]int32{0, format.TagValueIDComponentAggregator, int32(tag), format.TagValueIDRPCRequestsStatusOK, 0, 0, keyIDTag, 0, protocol})
	err := a.h.Handle(ctx, hctx)
	if err == rpc.ErrNoHandler {
		key.Tags[3] = format.TagValueIDRPCRequestsStatusNoHandler
	} else if rpc.IsHijackedResponse(err) {
		key.Tags[3] = format.TagValueIDRPCRequestsStatusHijack
	} else if err != nil {
		key.Tags[3] = format.TagValueIDRPCRequestsStatusErrLocal
	}
	a.sh2.AddValueCounter(key, float64(requestLen), 1, format.BuiltinMetricMetaRPCRequests)
	return err
}

func (a *Aggregator) getConfigResult() tlstatshouse.GetConfigResult {
	return tlstatshouse.GetConfigResult{
		Addresses:         a.addresses,
		MaxAddressesCount: int32(len(a.addresses)), // TODO - support reducing list,
		PreviousAddresses: int32(a.config.PreviousNumShards),
		Ts:                time.Now().UnixMilli(),
	}
}

func (a *Aggregator) getAgentEnv(isSetStaging0 bool, isSetStaging1 bool) int32 {
	mask := 0
	if isSetStaging0 {
		mask |= 1
	}
	if isSetStaging1 {
		mask |= 2
	}
	switch mask {
	case 1:
		return format.TagValueIDStaging1
	case 2:
		return format.TagValueIDStaging2
	case 3:
		return format.TagValueIDStaging3
	}
	return format.TagValueIDProduction
}

func (a *Aggregator) aggKey(t uint32, m int32, k [format.MaxTags]int32) data_model.Key {
	return data_model.AggKey(t, m, k, a.aggregatorHost, a.shardKey, a.replicaKey)
}

func (a *Aggregator) handleGetConfig2(_ context.Context, args tlstatshouse.GetConfig2) (tlstatshouse.GetConfigResult, error) {
	now := time.Now()
	nowUnix := uint32(now.Unix())
	host := a.tagsMapper.mapOrFlood(now, []byte(args.Header.HostName), format.BuiltinMetricNameBudgetHost, false)
	agentEnv := a.getAgentEnv(args.Header.IsSetAgentEnvStaging0(args.FieldsMask), args.Header.IsSetAgentEnvStaging1(args.FieldsMask))
	buildArch := format.FilterBuildArch(args.Header.BuildArch)
	route := int32(format.TagValueIDRouteDirect)
	if args.Header.IsSetIngressProxy(args.FieldsMask) {
		route = int32(format.TagValueIDRouteIngressProxy)
	}

	if args.Cluster != a.config.Cluster {
		key := a.aggKey(nowUnix, format.BuiltinMetricIDAutoConfig, [16]int32{0, 0, 0, 0, format.TagValueIDAutoConfigWrongCluster})
		key = key.WithAgentEnvRouteArch(agentEnv, route, buildArch)
		a.sh2.AddCounterHost(key, 1, host, format.BuiltinMetricMetaAutoConfig)
		return tlstatshouse.GetConfigResult{}, fmt.Errorf("statshouse misconfiguration! cluster requested %q does not match actual cluster connected %q", args.Cluster, a.config.Cluster)
	}
	key := a.aggKey(nowUnix, format.BuiltinMetricIDAutoConfig, [16]int32{0, 0, 0, 0, format.TagValueIDAutoConfigOK})
	key = key.WithAgentEnvRouteArch(agentEnv, route, buildArch)
	a.sh2.AddCounterHost(key, 1, host, format.BuiltinMetricMetaAutoConfig)
	return a.getConfigResult(), nil
}

func (a *Aggregator) handleSendSourceBucket2(_ context.Context, hctx *rpc.HandlerContext) error {
	var args tlstatshouse.SendSourceBucket2Bytes
	if _, err := args.Read(hctx.Request); err != nil {
		return fmt.Errorf("failed to deserialize statshouse.sendSourceBucket2 request: %w", err)
	}
	var bucketBytes []byte
	if int(args.OriginalSize) != len(args.CompressedData) {
		if args.OriginalSize > data_model.MaxUncompressedBucketSize {
			return fmt.Errorf("failed to deserialize compressed statshouse.sourceBucket - uncompressed size %d too big", args.OriginalSize)
		}
		bucketBytes = make([]byte, int(args.OriginalSize))
		s, err := lz4.UncompressBlock(args.CompressedData, bucketBytes)
		if err != nil {
			return fmt.Errorf("failed to deserialize compressed statshouse.sourceBucket: %w", err)
		}
		if s != int(args.OriginalSize) {
			return fmt.Errorf("failed to deserialize compressed statshouse.sourceBucket request: expected size %d actual %d", args.OriginalSize, s)
		}
		// uncomment if you add fields to the TL
		bucketBytes = append(bucketBytes, 0, 0, 0, 0) // ingestion_status_ok2, TODO - remove when all agent are updated to version 1.0
	} else {
		// uncomment if you add fields to the TL
		args.CompressedData = append(args.CompressedData, 0, 0, 0, 0) // ingestion_status_ok2, TODO - remove when all agent are updated to version 1.0
		bucketBytes = args.CompressedData
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
	var bucket tlstatshouse.SourceBucket2Bytes
	if _, err := bucket.ReadBoxed(bucketBytes); err != nil {
		return fmt.Errorf("failed to deserialize statshouse.sourceBucket2: %w", err)
	}
	rng := rand.New()
	now := time.Now()
	nowUnix := uint32(now.Unix())
	receiveDelay := now.Sub(time.Unix(int64(args.Time), 0)).Seconds()
	// All hosts must be valid and non-empty
	hostTagId := a.tagsMapper.mapOrFlood(now, args.Header.HostName, format.BuiltinMetricNameBudgetHost, false)
	var owner int32
	if args.IsSetOwner() {
		owner = a.tagsMapper.mapOrFlood(now, args.Owner, format.BuiltinMetricNameBudgetOwner, false)
	}
	agentEnv := a.getAgentEnv(args.Header.IsSetAgentEnvStaging0(args.FieldsMask), args.Header.IsSetAgentEnvStaging1(args.FieldsMask))
	buildArch := format.FilterBuildArch(args.Header.BuildArch)
	isRouteProxy := args.Header.IsSetIngressProxy(args.FieldsMask)
	route := int32(format.TagValueIDRouteDirect)
	if isRouteProxy {
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
		// ensure that it's not bigger then aggregator ts in order to write BuiltinMetricIDAggOutdatedAgents metric
		effectiveLeastAllowedAgentCommitTs := int32(min(format.LeastAllowedAgentCommitTs, build.CommitTimestamp()))
		if args.BuildCommitTs < effectiveLeastAllowedAgentCommitTs {
			key := a.aggKey(nowUnix, format.BuiltinMetricIDAggOutdatedAgents, [16]int32{0, 0, 0, 0, owner, 0, int32(addrIPV4)})
			key = key.WithAgentEnvRouteArch(agentEnv, route, buildArch)
			a.sh2.AddCounterHost(key, 1, hostTagId, format.BuiltinMetricMetaAggOutdatedAgents)
			hctx.Response, _ = args.WriteResult(hctx.Response, []byte("agent is too old please update"))
			return nil
		}
	}

	var aggBucket *aggregatorBucket
	a.mu.Lock()
	if err := a.checkShardConfiguration(args.Header.ShardReplica, args.Header.ShardReplicaTotal); err != nil {
		a.mu.Unlock()
		key := a.aggKey(nowUnix, format.BuiltinMetricIDAutoConfig, [16]int32{0, 0, 0, 0, format.TagValueIDAutoConfigErrorSend, args.Header.ShardReplica, args.Header.ShardReplicaTotal})
		key = key.WithAgentEnvRouteArch(agentEnv, route, buildArch)
		a.sh2.AddCounterHost(key, 1, hostTagId, format.BuiltinMetricMetaAutoConfig)
		return err // TODO - return code so clients will print into log and discard data
	}

	if a.bucketsToSend == nil {
		a.mu.Unlock()
		// We are in shutdown, recentBuckets stopped moving. We must be very careful
		// to prevent sending agents responses that will make them erase historic data.
		// Also, if we reply with errors, agents will resend data.
		// So we've simply chosen to hijack all responses and do not respond at all.
		return hctx.HijackResponse(aggBucket) // must be under bucket lock
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
			a.sh2.AddValueCounterHost(key, float64(args.Time)-float64(newestTime), 1, hostTagId, format.BuiltinMetricMetaTimingErrors)
			// We discard, because otherwise clients will flood aggregators with this data
			hctx.Response, _ = args.WriteResult(hctx.Response, []byte("historic bucket time is too far in the future"))
			return nil
		}
		if oldestTime >= data_model.MaxHistoricWindow && roundedToOurTime < oldestTime-data_model.MaxHistoricWindow {
			a.mu.Unlock()
			key := a.aggKey(nowUnix, format.BuiltinMetricIDTimingErrors, [16]int32{0, format.TagValueIDTimingLongWindowThrownAggregator})
			key = key.WithAgentEnvRouteArch(agentEnv, route, buildArch)
			a.sh2.AddValueCounterHost(key, float64(newestTime)-float64(args.Time), 1, hostTagId, format.BuiltinMetricMetaTimingErrors)
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
					historicHosts:               [2][2]map[int32]int64{{map[int32]int64{}, map[int32]int64{}}, {map[int32]int64{}, map[int32]int64{}}},
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
			a.sh2.AddValueCounterHost(key, float64(args.Time)-float64(newestTime), 1, hostTagId, format.BuiltinMetricMetaTimingErrors)
			// We discard, because otherwise clients will flood aggregators with this data
			hctx.Response, _ = args.WriteResult(hctx.Response, []byte("bucket time is too far in the future"))
			return nil
		}
		if roundedToOurTime < oldestTime {
			a.mu.Unlock()
			key := a.aggKey(nowUnix, format.BuiltinMetricIDTimingErrors, [16]int32{0, format.TagValueIDTimingLateRecent})
			key = key.WithAgentEnvRouteArch(agentEnv, route, buildArch)
			a.sh2.AddValueCounterHost(key, float64(newestTime)-float64(args.Time), 1, hostTagId, format.BuiltinMetricMetaTimingErrors)
			return &rpc.Error{
				Code:        data_model.RPCErrorMissedRecentConveyor,
				Description: "bucket time is too far in the past for recent conveyor",
			}
		}
		aggBucket = a.recentBuckets[roundedToOurTime-oldestTime]
		if a.config.SimulateRandomErrors > 0 && rng.Float64() < a.config.SimulateRandomErrors { // SimulateRandomErrors > 0 is optimization
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
	aggBucket.contributorsMetric[bool2int(args.IsSetSpare())][bool2int(isRouteProxy)].AddCounterHost(rng, 1, hostTagId) // protected by a.mu
	if args.IsSetHistoric() {
		a.historicHosts[bool2int(args.IsSetSpare())][bool2int(isRouteProxy)][hostTagId]++
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
				k.Tags[2] = k.Tags[1]
				k.Tags[1] = format.TagValueIDComponentAgent
			}
			if k.Metric == format.BuiltinMetricIDAgentHeartbeatArgs {
				// Remap legacy metric to a new one
				k.Metric = format.BuiltinMetricIDHeartbeatArgs
				k.Tags[2] = k.Tags[1]
				k.Tags[1] = format.TagValueIDComponentAgent
			}
			if k.Metric == format.BuiltinMetricIDHeartbeatVersion ||
				k.Metric == format.BuiltinMetricIDHeartbeatArgs {
				// In case of agent we need to set IP anyway, so set other keys here, not by source
				// In case of api other tags are already set, so don't overwrite them
				if k.Tags[4] == 0 {
					k.Tags[4] = bcTag
				}
				if k.Tags[5] == 0 {
					k.Tags[5] = args.BuildCommitDate
				}
				if k.Tags[6] == 0 {
					k.Tags[6] = args.BuildCommitTs
				}
				if k.Tags[7] == 0 {
					k.Tags[7] = hostTagId
				}
				// Valid for api as well because it is on the same host as agent
				k.Tags[8] = int32(addrIPV4)
				k.Tags[9] = owner
			}
			if k.Metric == format.BuiltinMetricIDRPCRequests {
				k.Tags[7] = hostTagId // agent cannot easily map its own host for now
			}
		}
		s := aggBucket.lockShard(&lockedShard, sID)
		created := false
		mi := data_model.MapKeyItemMultiItem(&s.multiItems, k, data_model.AggregatorStringTopCapacity, nil, &created)
		mi.MergeWithTLMultiItem(rng, &item, hostTagId)
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
	if args.IsSetHistoric() {
		aggBucket.historicHosts[bool2int(args.IsSetSpare())][bool2int(isRouteProxy)][hostTagId]++
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
	getMultiItem(args.Time, format.BuiltinMetricIDAggSizeCompressed, [16]int32{0, 0, 0, 0, conveyor, spare}).Tail.AddValueCounterHost(rng, float64(len(hctx.Request)), 1, hostTagId)

	getMultiItem(args.Time, format.BuiltinMetricIDAggSizeUncompressed, [16]int32{0, 0, 0, 0, conveyor, spare}).Tail.AddValueCounterHost(rng, float64(args.OriginalSize), 1, hostTagId)
	getMultiItem(args.Time, format.BuiltinMetricIDAggBucketReceiveDelaySec, [16]int32{0, 0, 0, 0, conveyor, spare, format.TagValueIDSecondReal}).Tail.AddValueCounterHost(rng, receiveDelay, 1, hostTagId)
	getMultiItem(args.Time, format.BuiltinMetricIDAggBucketAggregateTimeSec, [16]int32{0, 0, 0, 0, conveyor, spare}).Tail.AddValueCounterHost(rng, now2.Sub(now).Seconds(), 1, hostTagId)
	getMultiItem(args.Time, format.BuiltinMetricIDAggAdditionsToEstimator, [16]int32{0, 0, 0, 0, conveyor, spare}).Tail.AddValueCounterHost(rng, float64(len(newKeys)), 1, hostTagId)
	if bucket.MissedSeconds != 0 { // TODO - remove after all agents upgraded to write this metric with tag format.TagValueIDTimingMissedSecondsAgent
		getMultiItem(args.Time, format.BuiltinMetricIDTimingErrors, [16]int32{0, format.TagValueIDTimingMissedSeconds}).Tail.AddValueCounterHost(rng, float64(bucket.MissedSeconds), 1, hostTagId)
	}
	if args.QueueSizeMemory > 0 {
		getMultiItem(args.Time, format.BuiltinMetricIDAgentHistoricQueueSize, [16]int32{0, format.TagValueIDHistoricQueueMemory}).Tail.AddValueCounterHost(rng, float64(args.QueueSizeMemory), 1, hostTagId)
	}
	if args.QueueSizeMemorySum > 0 {
		getMultiItem(args.Time, format.BuiltinMetricIDAgentHistoricQueueSizeSum, [16]int32{0, format.TagValueIDHistoricQueueMemory}).Tail.AddValueCounterHost(rng, float64(args.QueueSizeMemorySum), 1, hostTagId)
	}
	if args.QueueSizeDiskUnsent > 0 {
		getMultiItem(args.Time, format.BuiltinMetricIDAgentHistoricQueueSize, [16]int32{0, format.TagValueIDHistoricQueueDiskUnsent}).Tail.AddValueCounterHost(rng, float64(args.QueueSizeDiskUnsent), 1, hostTagId)
	}
	queueSizeDiskSent := float64(args.QueueSizeDisk) - float64(args.QueueSizeDiskUnsent)
	if queueSizeDiskSent > 0 {
		getMultiItem(args.Time, format.BuiltinMetricIDAgentHistoricQueueSize, [16]int32{0, format.TagValueIDHistoricQueueDiskSent}).Tail.AddValueCounterHost(rng, float64(queueSizeDiskSent), 1, hostTagId)
	}
	if args.QueueSizeDiskSumUnsent > 0 {
		getMultiItem(args.Time, format.BuiltinMetricIDAgentHistoricQueueSizeSum, [16]int32{0, format.TagValueIDHistoricQueueDiskUnsent}).Tail.AddValueCounterHost(rng, float64(args.QueueSizeDiskSumUnsent), 1, hostTagId)
	}
	queueSizeDiskSumSent := float64(args.QueueSizeDiskSum) - float64(args.QueueSizeDiskSumUnsent)
	if queueSizeDiskSumSent > 0 {
		getMultiItem(args.Time, format.BuiltinMetricIDAgentHistoricQueueSizeSum, [16]int32{0, format.TagValueIDHistoricQueueDiskSent}).Tail.AddValueCounterHost(rng, float64(queueSizeDiskSumSent), 1, hostTagId)
	}

	componentTag := args.Header.ComponentTag
	if componentTag != format.TagValueIDComponentAgent && componentTag != format.TagValueIDComponentAggregator &&
		componentTag != format.TagValueIDComponentIngressProxy && componentTag != format.TagValueIDComponentAPI {
		// TODO - remove this if after release, because no more agents will send crap here
		componentTag = format.TagValueIDComponentAgent
	}
	// This cheap version metric is not affected by agent sampling algorithm in contrast with __heartbeat_version
	getMultiItem((args.Time/60)*60, format.BuiltinMetricIDVersions, [16]int32{0, 0, componentTag, args.BuildCommitDate, args.BuildCommitTs, bcTag}).MapStringTopBytes(rng, bcStr, 1).AddCounterHost(rng, 1, hostTagId)

	for _, v := range bucket.SampleFactors {
		// We probably wish to stop splitting by aggregator, because this metric is taking already too much space - about 2% of all data
		// Counter will be +1 for each agent who sent bucket for this second, so millions.
		getMultiItem(args.Time, format.BuiltinMetricIDAgentSamplingFactor, [16]int32{0, v.Metric}).Tail.AddValueCounterHost(rng, float64(v.Value), 1, hostTagId)
	}

	ingestionStatus := func(env int32, metricID int32, status int32, value float32) {
		data_model.MapKeyItemMultiItem(&s.multiItems, (data_model.Key{Timestamp: args.Time, Metric: format.BuiltinMetricIDIngestionStatus, Tags: [16]int32{env, metricID, status}}).WithAgentEnvRouteArch(agentEnv, route, buildArch), data_model.AggregatorStringTopCapacity, nil, nil).Tail.AddCounterHost(rng, float64(value), hostTagId)
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

func (a *Aggregator) handleSendKeepAlive2(_ context.Context, hctx *rpc.HandlerContext) error {
	var args tlstatshouse.SendKeepAlive2Bytes
	if _, err := args.Read(hctx.Request); err != nil {
		return fmt.Errorf("failed to deserialize statshouse.sendKeepAlive2 request: %w", err)
	}
	rng := rand.New()
	now := time.Now()
	nowUnix := uint32(now.Unix())
	host := a.tagsMapper.mapOrFlood(now, args.Header.HostName, format.BuiltinMetricNameBudgetHost, false)
	agentEnv := a.getAgentEnv(args.Header.IsSetAgentEnvStaging0(args.FieldsMask), args.Header.IsSetAgentEnvStaging1(args.FieldsMask))
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
		a.sh2.AddCounterHost(key, 1, host, format.BuiltinMetricMetaAutoConfig)
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
	data_model.MapKeyItemMultiItem(&s.multiItems, key, data_model.AggregatorStringTopCapacity, nil, nil).Tail.AddCounterHost(rng, 1, host)
	aggBucket.lockShard(&lockedShard, -1)

	return errHijack
}
