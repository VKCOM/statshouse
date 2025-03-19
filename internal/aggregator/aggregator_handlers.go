// Copyright 2025 V Kontakte LLC
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
	"slices"
	"time"

	"github.com/vkcom/statshouse/internal/compress"
	"go4.org/mem"
	"pgregory.net/rand"

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/vkgo/basictl"
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
	err := a.h.Handle(ctx, hctx)
	status := int32(format.TagValueIDRPCRequestsStatusOK)
	str := ""
	if err == rpc.ErrNoHandler {
		status = format.TagValueIDRPCRequestsStatusNoHandler
	} else if rpc.IsHijackedResponse(err) {
		status = format.TagValueIDRPCRequestsStatusHijack
	} else if err != nil {
		status = format.TagValueIDRPCRequestsStatusErrLocal
		str = err.Error()
	}
	a.sh2.AddValueCounterString(uint32(hctx.RequestTime.Unix()), format.BuiltinMetricMetaRPCRequests,
		[]int32{0, format.TagValueIDComponentAggregator, int32(tag), status, 0, 0, keyIDTag, 0, protocol},
		str, float64(requestLen), 1)
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

func (a *Aggregator) getConfigResult3() tlstatshouse.GetConfigResult3 {
	return tlstatshouse.GetConfigResult3{
		Addresses:          a.addresses,
		ShardByMetricCount: uint32(a.config.ShardByMetricShards),
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
		return format.TagValueIDDevelopment
	}
	return format.TagValueIDProduction
}

func (a *Aggregator) aggKey(t uint32, m int32, k [format.MaxTags]int32) *data_model.Key {
	return data_model.AggKey(t, m, k, a.aggregatorHost, a.shardKey, a.replicaKey)
}

// TODO - remove after all clients are new conveyor
func (a *Aggregator) handleGetConfig2(_ context.Context, args tlstatshouse.GetConfig2) (tlstatshouse.GetConfigResult, error) {
	now := time.Now()
	nowUnix := uint32(now.Unix())
	hostId := a.tagsMapper.mapOrFlood(now, []byte(args.Header.HostName), format.BuiltinMetricMetaBudgetHost.Name, false)
	hostTag := data_model.TagUnionBytes{I: hostId}
	aera := format.AgentEnvRouteArch{
		AgentEnv:  a.getAgentEnv(args.Header.IsSetAgentEnvStaging0(args.FieldsMask), args.Header.IsSetAgentEnvStaging1(args.FieldsMask)),
		Route:     format.TagValueIDRouteDirect,
		BuildArch: format.FilterBuildArch(args.Header.BuildArch),
	}
	isRouteProxy := args.Header.IsSetIngressProxy(args.FieldsMask)
	if isRouteProxy {
		aera.Route = format.TagValueIDRouteIngressProxy
	}
	if args.Cluster != a.config.Cluster {
		a.sh2.AddCounterHostAERA(nowUnix, format.BuiltinMetricMetaAutoConfig,
			[]int32{0, 0, 0, 0, format.TagValueIDAutoConfigWrongCluster}, 1, hostTag, aera)
		return tlstatshouse.GetConfigResult{}, fmt.Errorf("statshouse misconfiguration! cluster requested %q does not match actual cluster connected %q", args.Cluster, a.config.Cluster)
	}
	a.sh2.AddCounterHostAERA(nowUnix, format.BuiltinMetricMetaAutoConfig,
		[]int32{0, 0, 0, 0, format.TagValueIDAutoConfigOK}, 1, hostTag, aera)
	return a.getConfigResult(), nil
}

func equalConfigResult3(a, b tlstatshouse.GetConfigResult3) bool {
	return slices.Equal(a.Addresses, b.Addresses) &&
		a.ShardByMetricCount == b.ShardByMetricCount
}

func (a *Aggregator) handleGetConfig3(_ context.Context, hctx *rpc.HandlerContext) error {
	var args tlstatshouse.GetConfig3
	_, err := args.Read(hctx.Request)
	if err != nil {
		return fmt.Errorf("failed to deserialize statshouse.getConfig3 request: %w", err)
	}

	now := time.Now()
	nowUnix := uint32(now.Unix())
	hostId := a.tagsMapper.mapOrFlood(now, []byte(args.Header.HostName), format.BuiltinMetricMetaBudgetHost.Name, false)
	hostTag := data_model.TagUnionBytes{I: hostId}
	aera := format.AgentEnvRouteArch{
		AgentEnv:  a.getAgentEnv(args.Header.IsSetAgentEnvStaging0(args.FieldsMask), args.Header.IsSetAgentEnvStaging1(args.FieldsMask)),
		Route:     format.TagValueIDRouteDirect,
		BuildArch: format.FilterBuildArch(args.Header.BuildArch),
	}
	isRouteProxy := args.Header.IsSetIngressProxy(args.FieldsMask)
	if isRouteProxy {
		aera.Route = format.TagValueIDRouteIngressProxy
	}
	if args.Cluster != a.config.Cluster {
		a.sh2.AddCounterHostAERA(nowUnix, format.BuiltinMetricMetaAutoConfig,
			[]int32{0, 0, 0, 0, format.TagValueIDAutoConfigWrongCluster},
			1, hostTag, aera)
		return fmt.Errorf("statshouse misconfiguration! cluster requested %q does not match actual cluster connected %q", args.Cluster, a.config.Cluster)
	}
	cc := a.getConfigResult3()
	if args.IsSetPreviousConfig() && equalConfigResult3(args.PreviousConfig, cc) {
		a.sh2.AddCounterHostAERA(nowUnix, format.BuiltinMetricMetaAutoConfig,
			[]int32{0, 0, 0, 0, format.TagValueIDAutoConfigErrorKeepAlive},
			1, hostTag, aera)
		// longpoll forever until aggregator restarts
		return hctx.HijackResponse(a.testConnection) // those hctx are never added there so cancelling is NOP
	}
	a.sh2.AddCounterHostAERA(nowUnix, format.BuiltinMetricMetaAutoConfig,
		[]int32{0, 0, 0, 0, format.TagValueIDAutoConfigOK},
		1, hostTag, aera)
	hctx.Response, err = args.WriteResult(hctx.Response, cc)
	return err
}

func (a *Aggregator) handleGetMetrics3(_ context.Context, hctx *rpc.HandlerContext) error {
	var args tlstatshouse.GetMetrics3
	_, err := args.Read(hctx.Request)
	if err != nil {
		return fmt.Errorf("failed to deserialize statshouse.getMetrics3 request: %w", err)
	}
	if args.IsSetCompactJournal() {
		return a.journalCompact.HandleGetMetrics3(args, hctx)
	}
	if args.IsSetNewJournal() {
		return a.journalFast.HandleGetMetrics3(args, hctx)
	}
	return a.journal.HandleGetMetrics3(args, hctx)
}

func (a *Aggregator) handleSendSourceBucket2(_ context.Context, hctx *rpc.HandlerContext) error {
	var args tlstatshouse.SendSourceBucket2Bytes
	if _, err := args.Read(hctx.Request); err != nil {
		return fmt.Errorf("failed to deserialize statshouse.sendSourceBucket2 request: %w", err)
	}
	bucketBytes, err := compress.Decompress(args.OriginalSize, args.CompressedData)
	if err != nil {
		return err
	}
	// if you add fields to the TL, just add placeholders here for those agents who do not send them
	bucketBytes = append(bucketBytes, 0, 0, 0, 0) // ingestion_status_ok2

	var bucket tlstatshouse.SourceBucket2Bytes
	if _, err := bucket.ReadBoxed(bucketBytes); err != nil {
		return fmt.Errorf("failed to deserialize statshouse.sourceBucket2: %w", err)
	}
	str, err, _ := a.handleSendSourceBucketAny(hctx, args, bucket, false)
	if rpc.IsHijackedResponse(err) {
		return err
	}
	if err != nil {
		return err
	}
	hctx.Response, _ = args.WriteResult(hctx.Response, []byte(str))
	return nil
}

func (a *Aggregator) handleSendSourceBucket3(_ context.Context, hctx *rpc.HandlerContext) error {
	// we must not return errors to agent, we must instead always return
	// optional text to print and order to either discard or keep data
	var args tlstatshouse.SendSourceBucket3Bytes

	writeResponse := func(warning string, discard bool) {
		resp := tlstatshouse.SendSourceBucket3ResponseBytes{
			Warning: []byte(warning),
		}
		resp.SetDiscard(discard)
		hctx.Response, _ = args.WriteResult(hctx.Response, resp)
	}
	if _, err := args.Read(hctx.Request); err != nil {
		writeResponse(fmt.Sprintf("failed to deserialize statshouse.sendSourceBucket3 request: %v", err), true)
		return nil
	}
	bucketBytes, err := compress.Decompress(args.OriginalSize, args.CompressedData)
	if err != nil {
		writeResponse(err.Error(), true)
		return nil
	}
	var bucket tlstatshouse.SourceBucket3Bytes
	if _, err := bucket.ReadBoxed(bucketBytes); err != nil {
		writeResponse(fmt.Sprintf("failed to deserialize statshouse.sourceBucket3: %v", err), true)
		return nil
	}
	// we should clear all legacy fields mask which can be independently used by SourceBucket3
	// we leave only common proxy header maskas, spare and historic which are set to the same bits
	args2 := tlstatshouse.SendSourceBucket2Bytes{
		FieldsMask:     args.FieldsMask,
		Header:         args.Header,
		Time:           args.Time,
		BuildCommit:    args.BuildCommit,
		BuildCommitTs:  args.BuildCommitTs,
		OriginalSize:   args.OriginalSize,
		CompressedData: args.CompressedData,
	}
	bucket2 := tlstatshouse.SourceBucket2Bytes{
		Metrics:            bucket.Metrics,
		SampleFactors:      bucket.SampleFactors,
		IngestionStatusOk2: bucket.IngestionStatusOk2,
	}
	str, err, discard := a.handleSendSourceBucketAny(hctx, args2, bucket2, true)
	if rpc.IsHijackedResponse(err) {
		return err
	}
	// handleSendSourceBucketAny should not return any errors other than hijack for version3
	writeResponse(str, discard)
	return nil
}

func (a *Aggregator) handleSendSourceBucketAny(hctx *rpc.HandlerContext, args tlstatshouse.SendSourceBucket2Bytes, bucket tlstatshouse.SourceBucket2Bytes, version3 bool) (string, error, bool) {
	a.configMu.RLock()
	configR := a.configR
	a.configMu.RUnlock()

	rng := rand.New()
	now := time.Now()
	nowUnix := uint32(now.Unix())
	receiveDelay := now.Sub(time.Unix(int64(args.Time), 0)).Seconds()
	// All hosts must be valid and non-empty
	hostName := string(args.Header.HostName) // allocate once
	hostId := a.tagsMapper.mapOrFlood(now, args.Header.HostName, format.BuiltinMetricMetaBudgetHost.Name, false)
	hostTag := data_model.TagUnionBytes{I: hostId}
	ownerTagId := a.tagsMapper.mapOrFlood(now, args.Header.Owner, format.BuiltinMetricMetaBudgetOwner.Name, false)
	if ownerTagId == 0 {
		ownerTagId = a.tagsMapper.mapOrFlood(now, args.Owner, format.BuiltinMetricMetaBudgetOwner.Name, false)
	}
	aera := format.AgentEnvRouteArch{
		AgentEnv:  a.getAgentEnv(args.Header.IsSetAgentEnvStaging0(args.FieldsMask), args.Header.IsSetAgentEnvStaging1(args.FieldsMask)),
		Route:     format.TagValueIDRouteDirect,
		BuildArch: format.FilterBuildArch(args.Header.BuildArch),
	}
	isRouteProxy := args.Header.IsSetIngressProxy(args.FieldsMask)
	if isRouteProxy {
		aera.Route = format.TagValueIDRouteIngressProxy
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

	if configR.DenyOldAgents && args.BuildCommitTs < format.LeastAllowedAgentCommitTs {
		a.sh2.AddCounterHostAERA(nowUnix, format.BuiltinMetricMetaAggOutdatedAgents,
			[]int32{0, 0, 0, 0, ownerTagId, 0, int32(addrIPV4)},
			1, hostTag, aera)
		return "agent is too old please update", nil, true
	}

	a.mu.Lock()
	if err := a.checkShardConfiguration(args.Header.ShardReplica, args.Header.ShardReplicaTotal); err != nil {
		a.mu.Unlock()
		a.sh2.AddCounterHostAERA(nowUnix, format.BuiltinMetricMetaAutoConfig,
			[]int32{0, 0, 0, 0, format.TagValueIDAutoConfigErrorSend, args.Header.ShardReplica, args.Header.ShardReplicaTotal},
			1, hostTag, aera)
		if version3 {
			return err.Error(), nil, true
		}
		return "", err, false
	}

	if a.bucketsToSend == nil {
		// We are in shutdown, recentBuckets stopped moving. We must be very careful
		// to prevent sending agents responses that will make them erase historic data.
		// Also, if we reply with errors, agents will resend data.
		// So we've simply chosen to hijack all responses and do not respond at all.
		aggBucket := a.recentBuckets[0]
		a.mu.Unlock()
		aggBucket.mu.Lock()
		defer aggBucket.mu.Unlock()
		return "", hctx.HijackResponse(aggBucket), false // must be under bucket lock
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

	var aggBucket *aggregatorBucket
	if args.IsSetHistoric() {
		if roundedToOurTime > newestTime {
			a.mu.Unlock()
			a.sh2.AddValueCounterHostAERA(nowUnix, format.BuiltinMetricMetaTimingErrors,
				[]int32{0, format.TagValueIDTimingFutureBucketHistoric},
				float64(args.Time)-float64(newestTime), 1, hostTag, aera)
			// We discard, because otherwise clients will flood aggregators with this data
			return "historic bucket time is too far in the future", nil, true
		}
		if oldestTime >= data_model.MaxHistoricWindow && roundedToOurTime < oldestTime-data_model.MaxHistoricWindow {
			a.mu.Unlock()
			a.sh2.AddValueCounterHostAERA(nowUnix, format.BuiltinMetricMetaTimingErrors,
				[]int32{0, format.TagValueIDTimingLongWindowThrownAggregator},
				float64(newestTime)-float64(args.Time), 1, hostTag, aera)
			return "Successfully discarded historic bucket beyond historic window", nil, true
		}
		if roundedToOurTime < oldestTime {
			aggBucket = a.historicBuckets[args.Time]
			if aggBucket == nil {
				aggBucket = &aggregatorBucket{
					time:                        args.Time,
					contributors:                map[*rpc.HandlerContext]struct{}{},
					contributors3:               map[*rpc.HandlerContext]tlstatshouse.SendSourceBucket3Response{},
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
			a.sh2.AddValueCounterHostAERA(nowUnix, format.BuiltinMetricMetaTimingErrors,
				[]int32{0, format.TagValueIDTimingFutureBucketRecent},
				float64(args.Time)-float64(newestTime), 1, hostTag, aera)
			// We discard, because otherwise clients will flood aggregators with this data
			return "bucket time is too far in the future", nil, true
		}
		if roundedToOurTime < oldestTime {
			a.mu.Unlock()
			a.sh2.AddValueCounterHostAERA(nowUnix, format.BuiltinMetricMetaTimingErrors,
				[]int32{0, format.TagValueIDTimingLateRecent},
				float64(newestTime)-float64(args.Time), 1, hostTag, aera)
			// agent should resend via historic conveyor
			if version3 {
				return "bucket time is too far in the past for recent conveyor", nil, false
			}
			return "", &rpc.Error{
				Code:        data_model.RPCErrorMissedRecentConveyor,
				Description: "bucket time is too far in the past for recent conveyor",
			}, false
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
			return "", hctx.HijackResponse(aggBucket), false         // must be under bucket lock
		}
	}

	aggBucket.sendMu.RLock()
	// This lock order ensures, that if sender gets a.mu.Lock(), then all aggregating clients already have aggBucket.sendMu.RLock()
	aggBucket.contributorsMetric[bool2int(args.IsSetSpare())][bool2int(isRouteProxy)].AddCounterHost(rng, 1, hostTag) // protected by a.mu
	if args.IsSetHistoric() {
		a.historicHosts[bool2int(args.IsSetSpare())][bool2int(isRouteProxy)][hostId]++
	}
	a.mu.Unlock()
	defer aggBucket.sendMu.RUnlock()

	lockedShard := -1
	var newKeys []data_model.EstimatorMetricHash
	usedMetrics := map[int32]struct{}{}
	measurementIntKeys := 0
	measurementStringKeys := 0
	measurementLocks := 0
	measurementCentroids := 0
	measurementUniqueBytes := 0
	measurementStringTops := 0
	measurementIntTops := 0
	measurementOutdatedRows := 0
	unknownTags := map[string]format.CreateMappingExtra{}
	sendMappings := map[string]int32{} // we want deduplication to efficiently use network
	mappingHits := 0
	mappingMisses := 0

	// We do not want to decompress under lock, so we decompress before ifs, then rarely throw away decompressed data.

	conveyor := int32(format.TagValueIDConveyorRecent)
	if args.IsSetHistoric() {
		conveyor = format.TagValueIDConveyorHistoric
	}
	spare := int32(format.TagValueIDAggregatorOriginal)
	if args.IsSetSpare() {
		spare = format.TagValueIDAggregatorSpare
	}

	type clampedKey struct {
		env        int32
		metricID   int32
		clampedTag int32
	}
	clampedTimestampsMetrics := map[clampedKey]float32{}
	oldMetricBuckets := map[int32][3]int{}

	var resp tlstatshouse.SendSourceBucket3Response
	// we will allocate if key won't fit into this buffer, but it is quite unlikely
	var stackBuf [1024]byte
	keyBytes := stackBuf[:0]
	for _, item := range bucket.Metrics {
		if item.T != 0 && item.T < roundedToOurTime {
			measurementOutdatedRows++
		}
		if item.T != 0 && nowUnix >= data_model.MaxHistoricWindow && item.T < nowUnix-data_model.MaxHistoricWindow {
			b := oldMetricBuckets[item.Metric]
			if nowUnix-item.T >= 48*3600 {
				b[2]++
			} else if nowUnix-item.T >= 24*3600 {
				b[1]++
			} else if nowUnix-item.T >= 6*3600 {
				b[0]++
			}
			oldMetricBuckets[item.Metric] = b
		}
		measurementIntKeys += len(item.Keys)
		measurementStringKeys += len(item.Skeys)
		measurementCentroids += len(item.Tail.Centroids)
		measurementUniqueBytes += len(item.Tail.Uniques)
		for _, v := range item.Top {
			measurementCentroids += len(v.Value.Centroids)
			measurementUniqueBytes += len(v.Value.Uniques)
			if v.IsSetTag() {
				measurementIntTops++
			} else {
				measurementStringTops++
			}
		}
		k, clampedTag := data_model.KeyFromStatshouseMultiItem(&item, args.Time, newestTime)
		if clampedTag != 0 {
			clampedTimestampsMetrics[clampedKey{k.Tags[0], k.Metric, clampedTag}]++
		}
		if k.Metric < 0 && !format.HardwareMetric(k.Metric) {
			// when aggregator receives metric from an agent inside another aggregator, those keys are already set,
			// so we simply keep them. AgentEnvTag or RouteTag are always non-zero in this case.
			if k.Tags[format.AgentEnvTag] == 0 {
				k.Tags[format.AgentEnvTag] = aera.AgentEnv
				k.Tags[format.RouteTag] = aera.Route
				k.Tags[format.BuildArchTag] = aera.BuildArch
			}
			switch k.Metric {
			case format.BuiltinMetricIDAgentHeartbeatVersion:
				// Remap legacy metric to a new one
				k.Metric = format.BuiltinMetricIDHeartbeatVersion
				k.Tags[2] = k.Tags[1]
				k.Tags[1] = format.TagValueIDComponentAgent
			case format.BuiltinMetricIDAgentHeartbeatArgs:
				// Remap legacy metric to a new one
				k.Metric = format.BuiltinMetricIDHeartbeatArgs
				k.Tags[2] = k.Tags[1]
				k.Tags[1] = format.TagValueIDComponentAgent
			case format.BuiltinMetricIDHeartbeatVersion, format.BuiltinMetricIDHeartbeatArgs:
				// In case of agent we need to set IP anyway, so set other keys here, not by source
				// In case of api other tags are already set, so don't overwrite them
				if k.Tags[4] == 0 {
					k.Tags[4] = bcTag
				}
				if k.Tags[6] == 0 {
					k.Tags[6] = int32(args.BuildCommitTs)
				}
				if k.Tags[7] == 0 {
					k.Tags[7] = hostId
				}
				// Valid for api as well because it is on the same host as agent
				k.Tags[8] = int32(addrIPV4)
				k.Tags[9] = ownerTagId
			case format.BuiltinMetricIDRPCRequests:
				k.Tags[7] = hostId // agent cannot easily map its own host for now
			}
		}
		// If agents send lots of strings, this loop is non-trivial amount of work.
		// May be, if mappingHits + mappingMisses > some limit, we should simply copy strings to STags
		mapStringTag := func(i int, str []byte) int32 {
			if len(str) == 0 {
				return 0
			}
			if mapped, ok := a.mappingsCache.GetValueBytes(aggBucket.time, str); ok {
				mappingHits++
				if len(sendMappings) < configR.MaxSendTagsToAgent {
					sendMappings[string(str)] = mapped
				}
				return mapped
			}
			mappingMisses++
			if len(unknownTags) < configR.MaxUnknownTagsInBucket {
				tagId := int32(i + format.TagIDShift)
				if _, ok := unknownTags[string(str)]; !ok {
					unknownTags[string(str)] = format.CreateMappingExtra{
						Create:    true, // passed as is to meta loader
						MetricID:  k.Metric,
						TagIDKey:  tagId,
						ClientEnv: k.Tags[0],
						Aera:      aera,
						HostName:  hostName,
						Host:      hostId,
					}
				}
			}
			return 0
		}
		for i, str := range item.Skeys {
			// in case agents sends more then 16 tags
			if i >= format.MaxTags {
				break
			}
			if m := mapStringTag(i, str); m > 0 {
				k.Tags[i] = m
			} else {
				k.SetSTag(i, string(str))
			}
		}
		if item.Tail.IsSetMaxHostStag(item.FieldsMask) {
			if m := mapStringTag(format.HostTagIndex, item.Tail.MaxHostStag); m > 0 {
				item.Tail.SetMaxHostTag(m, &item.FieldsMask)
				item.Tail.ClearMaxHostStag(&item.FieldsMask)
			}
		}
		if item.Tail.IsSetMaxCounterHostStag(item.FieldsMask) {
			if m := mapStringTag(format.HostTagIndex, item.Tail.MaxCounterHostStag); m > 0 {
				item.Tail.SetMaxCounterHostTag(m, &item.FieldsMask)
				item.Tail.ClearMaxCounterHostStag(&item.FieldsMask)
			}
		}
		if item.Tail.IsSetMinHostStag(item.FieldsMask) {
			if m := mapStringTag(format.HostTagIndex, item.Tail.MinHostStag); m > 0 {
				item.Tail.SetMinHostTag(m, &item.FieldsMask)
				item.Tail.ClearMinHostStag(&item.FieldsMask)
			}
		}
		if configR.MapStringTop {
			for i, tb := range item.Top {
				if m := mapStringTag(i, tb.Stag); m > 0 {
					item.Top[i].Tag = m
				}
				if tb.Value.IsSetMaxHostStag(tb.FieldsMask) {
					if m := mapStringTag(format.HostTagIndex, tb.Value.MaxHostStag); m > 0 {
						tb.Value.SetMaxHostTag(m, &item.FieldsMask)
						tb.Value.ClearMaxHostStag(&item.FieldsMask)
					}
				}
				if tb.Value.IsSetMaxCounterHostStag(tb.FieldsMask) {
					if m := mapStringTag(format.HostTagIndex, tb.Value.MaxCounterHostStag); m > 0 {
						tb.Value.SetMaxCounterHostTag(m, &item.FieldsMask)
						tb.Value.ClearMaxCounterHostStag(&item.FieldsMask)
					}
				}
				if tb.Value.IsSetMinHostStag(tb.FieldsMask) {
					if m := mapStringTag(format.HostTagIndex, tb.Value.MinHostStag); m > 0 {
						tb.Value.SetMinHostTag(m, &item.FieldsMask)
						tb.Value.ClearMinHostStag(&item.FieldsMask)
					}
				}
			}
		}
		var hash uint64
		keyBytes, hash = k.XXHash(keyBytes)
		sID := int(hash % data_model.AggregationShardsPerSecond)
		s := aggBucket.lockShard(&lockedShard, sID, &measurementLocks)
		if s.metricStats == nil {
			s.metricStats = make(map[int32]metricStat)
		}
		ms := s.metricStats[item.Metric]
		ms.total++
		if item.IsSetWeightMultiplier() {
			ms.multipliers++
		}
		s.metricStats[item.Metric] = ms
		mi, created := s.GetOrCreateMultiItem(&k, nil, 1, keyBytes)
		mi.MergeWithTLMultiItem(rng, data_model.AggregatorStringTopCapacity, &item, hostTag)
		// we unlock shard to calculate hash and do other heavy operations not under lock
		aggBucket.lockShard(&lockedShard, -1, &measurementLocks)
		if created {
			if !args.IsSetSpare() { // Data from spares should not affect cardinality estimations
				newKeys = append(newKeys, data_model.EstimatorMetricHash{Metric: k.Metric, Hash: hash})
			}
			usedMetrics[k.Metric] = struct{}{}
		}
	}
	if lockedShard != -1 {
		aggBucket.lockShard(&lockedShard, -1, &measurementLocks)
	}

	for k, v := range sendMappings {
		resp.Mappings = append(resp.Mappings, tlstatshouse.Mapping{Str: k, Value: v})
	}

	unknownMapRemove, unknownMapAdd, unknownListAdd, createMapAdd, avgRemovedHits := a.tagsMapper2.AddUnknownTags(unknownTags, aggBucket.time)

	aggBucket.mu.Lock()

	if aggBucket.usedMetrics == nil {
		aggBucket.usedMetrics = map[int32]struct{}{}
	}
	for m := range usedMetrics {
		aggBucket.usedMetrics[m] = struct{}{}
	}
	if args.IsSetHistoric() {
		aggBucket.historicHosts[bool2int(args.IsSetSpare())][bool2int(isRouteProxy)][hostId]++
	}
	if version3 {
		aggBucket.contributors3[hctx] = resp // must be under bucket lock
	} else {
		aggBucket.contributors[hctx] = struct{}{} // must be under bucket lock
	}
	compressedSize := len(hctx.Request)
	errHijack := hctx.HijackResponse(aggBucket) // must be under bucket lock

	aggBucket.mu.Unlock()

	// newKeys will not be large, if average cardinality is low
	// we update estimators under sendMu.RLock so that sample factors used for inserting will be already updated
	a.estimator.UpdateWithKeys(args.Time, newKeys)

	now2 := time.Now()

	a.sh2.AddValueCounterHostAERA(args.Time, format.BuiltinMetricMetaAggBucketInfo,
		[]int32{0, 0, 0, 0, conveyor, spare, format.TagValueIDAggBucketInfoRows}, float64(len(bucket.Metrics)), 1, hostTag, aera)
	a.sh2.AddValueCounterHostAERA(args.Time, format.BuiltinMetricMetaAggBucketInfo,
		[]int32{0, 0, 0, 0, conveyor, spare, format.TagValueIDAggBucketInfoOutdatedRows}, float64(measurementOutdatedRows), 1, hostTag, aera)
	a.sh2.AddValueCounterHostAERA(args.Time, format.BuiltinMetricMetaAggBucketInfo,
		[]int32{0, 0, 0, 0, conveyor, spare, format.TagValueIDAggBucketInfoIntKeys}, float64(measurementIntKeys), 1, hostTag, aera)
	a.sh2.AddValueCounterHostAERA(args.Time, format.BuiltinMetricMetaAggBucketInfo,
		[]int32{0, 0, 0, 0, conveyor, spare, format.TagValueIDAggBucketInfoStringKeys}, float64(measurementStringKeys), 1, hostTag, aera)
	a.sh2.AddValueCounterHostAERA(args.Time, format.BuiltinMetricMetaAggBucketInfo,
		[]int32{0, 0, 0, 0, conveyor, spare, format.TagValueIDAggBucketInfoMappingHits}, float64(mappingHits), 1, hostTag, aera)
	a.sh2.AddValueCounterHostAERA(args.Time, format.BuiltinMetricMetaAggBucketInfo,
		[]int32{0, 0, 0, 0, conveyor, spare, format.TagValueIDAggBucketInfoMappingMisses}, float64(mappingMisses), 1, hostTag, aera)
	a.sh2.AddValueCounterHostAERA(args.Time, format.BuiltinMetricMetaAggBucketInfo,
		[]int32{0, 0, 0, 0, conveyor, spare, format.TagValueIDAggBucketInfoMappingUnknownKeys}, float64(len(unknownTags)), 1, hostTag, aera)
	a.sh2.AddValueCounterHostAERA(args.Time, format.BuiltinMetricMetaAggBucketInfo,
		[]int32{0, 0, 0, 0, conveyor, spare, format.TagValueIDAggBucketInfoMappingLocks}, float64(measurementLocks), 1, hostTag, aera)
	a.sh2.AddValueCounterHostAERA(args.Time, format.BuiltinMetricMetaAggBucketInfo,
		[]int32{0, 0, 0, 0, conveyor, spare, format.TagValueIDAggBucketInfoCentroids}, float64(measurementCentroids), 1, hostTag, aera)
	a.sh2.AddValueCounterHostAERA(args.Time, format.BuiltinMetricMetaAggBucketInfo,
		[]int32{0, 0, 0, 0, conveyor, spare, format.TagValueIDAggBucketInfoUniqueBytes}, float64(measurementUniqueBytes), 1, hostTag, aera)
	a.sh2.AddValueCounterHostAERA(args.Time, format.BuiltinMetricMetaAggBucketInfo,
		[]int32{0, 0, 0, 0, conveyor, spare, format.TagValueIDAggBucketInfoStringTops}, float64(measurementStringTops), 1, hostTag, aera)
	a.sh2.AddValueCounterHostAERA(args.Time, format.BuiltinMetricMetaAggBucketInfo,
		[]int32{0, 0, 0, 0, conveyor, spare, format.TagValueIDAggBucketInfoIntTops}, float64(measurementIntTops), 1, hostTag, aera)
	a.sh2.AddValueCounterHostAERA(args.Time, format.BuiltinMetricMetaAggBucketInfo,
		[]int32{0, 0, 0, 0, conveyor, spare, format.TagValueIDAggBucketInfoNewKeys}, float64(len(newKeys)), 1, hostTag, aera)
	a.sh2.AddValueCounterHostAERA(args.Time, format.BuiltinMetricMetaAggBucketInfo,
		[]int32{0, 0, 0, 0, conveyor, spare, format.TagValueIDAggBucketInfoMetrics}, float64(len(usedMetrics)), 1, hostTag, aera)

	a.sh2.AddCounterHostAERA(args.Time, format.BuiltinMetricMetaMappingCacheEvent,
		[]int32{0, format.TagValueIDComponentAggregator, format.TagValueIDMappingCacheEventHit},
		float64(mappingHits), hostTag, aera)
	a.sh2.AddCounterHostAERA(args.Time, format.BuiltinMetricMetaMappingCacheEvent,
		[]int32{0, format.TagValueIDComponentAggregator, format.TagValueIDMappingCacheEventMiss},
		float64(mappingMisses), hostTag, aera)

	a.sh2.AddCounterHostAERA(args.Time, format.BuiltinMetricMetaMappingQueueEvent,
		[]int32{0, 0, format.TagValueIDMappingQueueEventUnknownMapRemove},
		float64(unknownMapRemove), hostTag, aera)
	a.sh2.AddCounterHostAERA(args.Time, format.BuiltinMetricMetaMappingQueueEvent,
		[]int32{0, 0, format.TagValueIDMappingQueueEventUnknownMapAdd},
		float64(unknownMapAdd), hostTag, aera)
	a.sh2.AddCounterHostAERA(args.Time, format.BuiltinMetricMetaMappingQueueEvent,
		[]int32{0, 0, format.TagValueIDMappingQueueEventUnknownListAdd},
		float64(unknownListAdd), hostTag, aera)
	a.sh2.AddCounterHostAERA(args.Time, format.BuiltinMetricMetaMappingQueueEvent,
		[]int32{0, 0, format.TagValueIDMappingQueueEventCreateMapAdd},
		float64(createMapAdd), hostTag, aera)
	if avgRemovedHits != 0 {
		a.sh2.AddCounterHostAERA(args.Time, format.BuiltinMetricMetaMappingQueueRemovedHitsAvg,
			[]int32{},
			avgRemovedHits, hostTag, aera)
	}

	a.sh2.AddValueCounterHostAERA(args.Time, format.BuiltinMetricMetaAggSizeCompressed,
		[]int32{0, 0, 0, 0, conveyor, spare},
		float64(compressedSize), 1, hostTag, aera)

	a.sh2.AddValueCounterHostAERA(args.Time, format.BuiltinMetricMetaAggSizeUncompressed,
		[]int32{0, 0, 0, 0, conveyor, spare},
		float64(args.OriginalSize), 1, hostTag, aera)
	a.sh2.AddValueCounterHostAERA(args.Time, format.BuiltinMetricMetaAggBucketReceiveDelaySec,
		[]int32{0, 0, 0, 0, conveyor, spare, format.TagValueIDSecondReal},
		receiveDelay, 1, hostTag, aera)
	a.sh2.AddValueCounterHostAERA(args.Time, format.BuiltinMetricMetaAggBucketAggregateTimeSec,
		[]int32{0, 0, 0, 0, conveyor, spare},
		now2.Sub(now).Seconds(), 1, hostTag, aera)
	if bucket.MissedSeconds != 0 { // TODO - remove after all agents upgraded to write this metric with tag format.TagValueIDTimingMissedSecondsAgent
		a.sh2.AddValueCounterHostAERA(args.Time, format.BuiltinMetricMetaTimingErrors,
			[]int32{0, format.TagValueIDTimingMissedSeconds},
			float64(bucket.MissedSeconds), 1, hostTag, aera)
	}
	// TODO - remove all queue metrics below after all agents upgraded to write this metric directly to bucket
	// we set AggShard/Replica/Host manually for legacy agents for reason, do not change this.
	if args.QueueSizeMemory > 0 {
		a.sh2.AddValueCounterHostAERA(args.Time, format.BuiltinMetricMetaAgentHistoricQueueSize,
			[]int32{0, format.TagValueIDHistoricQueueMemory, format.AggShardTag: a.shardKey, format.AggReplicaTag: a.replicaKey, format.AggHostTag: a.aggregatorHost},
			float64(args.QueueSizeMemory), 1, hostTag, aera)
	}
	if args.QueueSizeDiskUnsent > 0 {
		a.sh2.AddValueCounterHostAERA(args.Time, format.BuiltinMetricMetaAgentHistoricQueueSize,
			[]int32{0, format.TagValueIDHistoricQueueDiskUnsent, format.AggShardTag: a.shardKey, format.AggReplicaTag: a.replicaKey, format.AggHostTag: a.aggregatorHost},
			float64(args.QueueSizeDiskUnsent), 1, hostTag, aera)
	}
	if queueSizeDiskSent := float64(args.QueueSizeDisk) - float64(args.QueueSizeDiskUnsent); queueSizeDiskSent > 0 {
		a.sh2.AddValueCounterHostAERA(args.Time, format.BuiltinMetricMetaAgentHistoricQueueSize,
			[]int32{0, format.TagValueIDHistoricQueueDiskSent, format.AggShardTag: a.shardKey, format.AggReplicaTag: a.replicaKey, format.AggHostTag: a.aggregatorHost},
			float64(queueSizeDiskSent), 1, hostTag, aera)
	}

	componentTag := args.Header.ComponentTag
	if componentTag != format.TagValueIDComponentAgent && componentTag != format.TagValueIDComponentAggregator &&
		componentTag != format.TagValueIDComponentIngressProxy && componentTag != format.TagValueIDComponentAPI {
		// TODO - remove this 'if' after release, because no more agents will send crap here
		componentTag = format.TagValueIDComponentAgent
	}
	{
		// This cheap version metric is not affected by agent sampling algorithm in contrast with __heartbeat_version
		a.sh2.AddCounterHostStringBytesAERA((args.Time/60)*60, format.BuiltinMetricMetaVersions,
			[]int32{0, 0, componentTag, 0, int32(args.BuildCommitTs), bcTag},
			bcStr, 1, hostTag, aera)
	}
	for m, b := range oldMetricBuckets {
		if b[0] > 0 {
			a.sh2.AddCounterHostStringBytesAERA(nowUnix, format.BuiltinMetricMetaAggOldMetrics,
				[]int32{0, format.TagValueIDOldMetricForm6hTo1d, m},
				bcStr, float64(b[0]), hostTag, aera)
		}
		if b[1] > 0 {
			a.sh2.AddCounterHostStringBytesAERA(nowUnix, format.BuiltinMetricMetaAggOldMetrics,
				[]int32{0, format.TagValueIDOldMetricForm1dTo2d, m},
				bcStr, float64(b[1]), hostTag, aera)
		}
		if b[2] > 0 {
			a.sh2.AddCounterHostStringBytesAERA(nowUnix, format.BuiltinMetricMetaAggOldMetrics,
				[]int32{0, format.TagValueIDOldMetricForm2d, m},
				bcStr, float64(b[2]), hostTag, aera)
		}
	}

	// Ingestion statuses, sample factors and badges are written into the same shard as metric itself.
	// They all simply go to merge shard 0 independent of their keys.
	s := aggBucket.lockShard(&lockedShard, 0, &measurementLocks)
	for _, v := range bucket.SampleFactors {
		// We probably wish to stop splitting by aggregator, because this metric is taking already too much space - about 2% of all data
		// Counter will be +1 for each agent who sent bucket for this second, so millions.
		a.sh2.GetMultiItemAERA(&s.MultiItemMap, args.Time, format.BuiltinMetricMetaAgentSamplingFactor, 1,
			[]int32{0, v.Metric, format.AggShardTag: a.shardKey}, aera).
			Tail.AddValueCounterHost(rng, float64(v.Value), 1, hostTag)
	}

	ingestionStatus := func(env int32, metricID int32, status int32, value float32) {
		a.sh2.GetMultiItemAERA(&s.MultiItemMap, args.Time, format.BuiltinMetricMetaIngestionStatus, 1,
			[]int32{env, metricID, status}, aera).
			Tail.AddCounterHost(rng, float64(value), hostTag)
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
	for k, v := range clampedTimestampsMetrics {
		// We do not split by aggregator, because this metric is taking already too much space - about 1% of all data
		ingestionStatus(k.env, k.metricID, k.clampedTag, v)
	}
	aggBucket.lockShard(&lockedShard, -1, &measurementLocks)
	return "", errHijack, false
}

func (a *Aggregator) handleSendKeepAlive2(_ context.Context, hctx *rpc.HandlerContext) error {
	var args tlstatshouse.SendKeepAlive2Bytes
	if _, err := args.Read(hctx.Request); err != nil {
		return fmt.Errorf("failed to deserialize statshouse.sendKeepAlive2 request: %w", err)
	}
	return a.handleSendKeepAliveAny(hctx, tlstatshouse.SendKeepAlive3Bytes(args), false)
}

func (a *Aggregator) handleSendKeepAlive3(_ context.Context, hctx *rpc.HandlerContext) error {
	var args tlstatshouse.SendKeepAlive3Bytes
	if _, err := args.Read(hctx.Request); err != nil {
		return fmt.Errorf("failed to deserialize statshouse.sendKeepAlive3 request: %w", err)
	}
	return a.handleSendKeepAliveAny(hctx, args, true)
}

func (a *Aggregator) handleSendKeepAliveAny(hctx *rpc.HandlerContext, args tlstatshouse.SendKeepAlive3Bytes, version3 bool) error {
	rng := rand.New()
	now := time.Now()
	nowUnix := uint32(now.Unix())
	hostId := a.tagsMapper.mapOrFlood(now, args.Header.HostName, format.BuiltinMetricMetaBudgetHost.Name, false)
	hostTag := data_model.TagUnionBytes{I: hostId}
	aera := format.AgentEnvRouteArch{
		AgentEnv:  a.getAgentEnv(args.Header.IsSetAgentEnvStaging0(args.FieldsMask), args.Header.IsSetAgentEnvStaging1(args.FieldsMask)),
		Route:     format.TagValueIDRouteDirect,
		BuildArch: format.FilterBuildArch(args.Header.BuildArch),
	}
	isRouteProxy := args.Header.IsSetIngressProxy(args.FieldsMask)
	if isRouteProxy {
		aera.Route = format.TagValueIDRouteIngressProxy
	}

	a.mu.Lock()
	if err := a.checkShardConfiguration(args.Header.ShardReplica, args.Header.ShardReplicaTotal); err != nil {
		a.mu.Unlock()
		a.sh2.AddCounterHostAERA(nowUnix, format.BuiltinMetricMetaAutoConfig,
			[]int32{0, 0, 0, 0, format.TagValueIDAutoConfigErrorKeepAlive, args.Header.ShardReplica, args.Header.ShardReplicaTotal},
			1, hostTag, aera)
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
	if version3 {
		aggBucket.contributors3[hctx] = tlstatshouse.SendSourceBucket3Response{} // must be under bucket lock
	} else {
		aggBucket.contributors[hctx] = struct{}{} // must be under bucket lock
	}
	errHijack := hctx.HijackResponse(aggBucket) // must be under bucket lock
	aggBucket.mu.Unlock()
	// Write meta statistics

	lockedShard := -1
	measurementLocks := 0
	s := aggBucket.lockShard(&lockedShard, 0, &measurementLocks)
	// Counters can contain this metrics while # of contributors is 0. We compensate by adding small fixed budget.
	a.sh2.GetMultiItemAERA(&s.MultiItemMap, aggBucket.time, format.BuiltinMetricMetaAggKeepAlive, 1,
		[]int32{}, aera).Tail.
		AddCounterHost(rng, 1, hostTag)
	aggBucket.lockShard(&lockedShard, -1, &measurementLocks)

	return errHijack
}
