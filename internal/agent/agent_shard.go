// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package agent

import (
	"encoding/binary"
	"encoding/hex"
	"sync"
	"syscall"
	"time"

	"go.uber.org/atomic"
	"pgregory.net/rand"

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/vkgo/build"
	"github.com/vkcom/statshouse/internal/vkgo/srvfunc"
)

// we start sending at the end of the minute, plus we need to spread metric around the next 60 seconds
// so 120 slots are actually actively used, while 8 slots serve as a send/receive queue
const superQueueLen = 128

type (
	// Shard gets data after initial hashing and shard number
	Shard struct {
		// Never change, so do not require protection
		agent    *Agent
		ShardNum int
		ShardKey int32
		rng      *rand.Rand

		mu                                   sync.Mutex
		config                               Config       // can change if remotely updated
		hardwareMetricResolutionResolved     atomic.Int32 // depends on config
		hardwareSlowMetricResolutionResolved atomic.Int32 // depends on config

		timeSpreadDelta time.Duration // randomly spread bucket sending through second between sources/machines

		CurrentTime uint32
		SendTime    uint32
		SuperQueue  [superQueueLen]*data_model.MetricsBucket
		// CurrentTime advances with the clock.
		// SendTime follows with some delay, but can lag behind if sampling conveyor is stuck.
		// If CurrentTime is too far in the future relative to SendTime, agent discards all received events
		// beware!
		// We must spread X second resolution metric rows around next X seconds deterministically,
		// all agents must assign the same rows to the same second, so that when aggregator
		// works on that second, all those rows aggregate together.
		stopReceivingIncomingData bool
		// We have lots of async components keeping writing metrics into agent during shutdown.
		// We set this bool as a circuit breaker, so new data will not be added to CurrentBuckets/NextBuckets
		// And shutdown code can flush them to disk without any non-deterministic behavior

		BucketsToSend     chan compressedBucketData
		BuiltInItemValues []*BuiltInItemValue // Collected/reset before flush

		BucketsToPreprocess chan *data_model.MetricsBucket

		HistoricBucketsToSend   []compressedBucketData // Can be slightly out of order here, we sort it every time
		HistoricBucketsDataSize int                    // if too many are with data, will put without data, which will be read from disk
		cond                    *sync.Cond

		HistoricOutOfWindowDropped atomic.Int64
	}

	BuiltInItemValue struct {
		mu         sync.Mutex
		key        data_model.Key
		value      data_model.ItemValue
		metricInfo *format.MetricMetaValue
		weightMul  int
	}

	compressedBucketData struct {
		id      int64 // in disk queue, or 0 if working without disk
		time    uint32
		data    []byte // first 4 bytes are uncompressed size, rest is compressed data
		version uint8  // 3 - SourceBucket3, 2 and everyting else - SourceBucket2
	}
)

func (s *Shard) HistoricBucketsDataSizeMemory() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.HistoricBucketsDataSize
}

func (s *Shard) gapInReceivingQueueLocked() int64 {
	return int64(s.CurrentTime) - (int64(s.SendTime) + superQueueLen - 120)
}

func (s *Shard) shouldDiscardIncomingData() bool {
	return s.stopReceivingIncomingData || s.gapInReceivingQueueLocked() > 0
}

func (s *Shard) HistoricBucketsDataSizeDisk() (total int64, unsent int64) {
	if s.agent.diskBucketCache == nil {
		return 0, 0
	}
	return s.agent.diskBucketCache.TotalFileSize(s.ShardNum)
}

// For low-resolution metrics, we must ensure timestamps are rounded, so they again end up in the same map item,
// and clients should set timestamps freely and not make assumptions on metric resolution (it can be changed on the fly).
// Later, when sending bucket, we will remove timestamps for all items which have it
// equal to bucket timestamp (only for transport efficiency), then reset timestamps on aggregator after receiving.
func (s *Shard) resolutionShardFromHashLocked(key *data_model.Key, resolutionHash uint64, metricInfo *format.MetricMetaValue) *data_model.MetricsBucket {
	resolution := uint32(1)
	if metricInfo != nil {
		if !format.HardwareMetric(metricInfo.MetricID) {
			resolution = uint32(metricInfo.EffectiveResolution)
		} else {
			if metricInfo.IsHardwareSlowMetric {
				resolution = uint32(s.hardwareSlowMetricResolutionResolved.Load())
			} else {
				resolution = uint32(s.hardwareMetricResolutionResolved.Load())
			}
		}
	}
	currentTime := s.CurrentTime
	sendTime := s.SendTime
	if key.Timestamp == 0 {
		// we have lots of builtin metrics in aggregator which should correspond to "current" second.
		// but unfortunately now agent's current second is lagging behind.
		// TODO - add explicit timestamp to all of them, then do panic here
		// panic("all builtin metrics must have correct timestamp set at this point")
		key.Timestamp = currentTime
	}
	// Timestamp will be clamped by aggregators.
	if resolution == 1 {
		slot := key.Timestamp
		if slot < sendTime {
			slot = sendTime // if late, send immediately. Helps those who are late by a tiny amount.
		}
		// if slot >= currentTime - we do no special processing for slots in the future
		return s.SuperQueue[slot%superQueueLen]
	}
	// division is expensive, hence separate code for very common 1-second resolution above
	key.Timestamp = (key.Timestamp / resolution) * resolution
	resolutionShardNum := uint32((resolutionHash & 0xFFFFFFFF) * uint64(resolution) >> 32) // trunc([0..0.9999999] * numShards) in fixed point 32.32
	slot := key.Timestamp + resolution + resolutionShardNum
	// we could start sending 1 second earlier, adding - 1 to the slot in code above, but for now we want compatibility with legacy code.
	if slot < sendTime { // rare?
		slot += ((sendTime - slot + resolution - 1) / resolution) * resolution
		// if late, send immediately, but keep slots aligned with resolution, sometimes identically on several/many agents, hopefully improving aggregation
	}
	// if slot >= currentTime+? - we do no special processing for slots in the future
	return s.SuperQueue[slot%superQueueLen]
}

func (s *Shard) CreateBuiltInItemValue(metricInfo *format.MetricMetaValue, weightMul int, key *data_model.Key) *BuiltInItemValue {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := &BuiltInItemValue{key: *key, metricInfo: metricInfo, weightMul: weightMul}
	s.BuiltInItemValues = append(s.BuiltInItemValues, result)
	return result
}

func (s *Shard) ApplyUnique(key *data_model.Key, resolutionHash uint64, topValue data_model.TagUnionBytes, hashes []int64, count float64, hostTag data_model.TagUnionBytes, metricInfo *format.MetricMetaValue, weightMul int) {
	if count == 0 {
		count = float64(len(hashes))
	}
	if count <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.shouldDiscardIncomingData() {
		return
	}
	resolutionShard := s.resolutionShardFromHashLocked(key, resolutionHash, metricInfo)
	item, _ := resolutionShard.GetOrCreateMultiItem(key, s.config.StringTopCapacity, metricInfo, weightMul, nil)
	mv := item.MapStringTopBytes(s.rng, topValue, count)
	mv.ApplyUnique(s.rng, hashes, count, hostTag)
}

func (s *Shard) ApplyValues(key *data_model.Key, resolutionHash uint64, topValue data_model.TagUnionBytes, histogram [][2]float64, values []float64, count float64, hostTag data_model.TagUnionBytes, metricInfo *format.MetricMetaValue, weightMul int) {
	totalCount := float64(len(values))
	for _, kv := range histogram {
		totalCount += kv[1] // all counts are validated to be >= 0
	}
	if count == 0 {
		count = totalCount
	}
	if count <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.shouldDiscardIncomingData() {
		return
	}
	resolutionShard := s.resolutionShardFromHashLocked(key, resolutionHash, metricInfo)
	item, _ := resolutionShard.GetOrCreateMultiItem(key, s.config.StringTopCapacity, metricInfo, weightMul, nil)
	mv := item.MapStringTopBytes(s.rng, topValue, count)
	mv.ApplyValues(s.rng, histogram, values, count, totalCount, hostTag, data_model.AgentPercentileCompression, metricInfo != nil && metricInfo.HasPercentiles)
}

func (s *Shard) ApplyCounter(key *data_model.Key, resolutionHash uint64, topValue data_model.TagUnionBytes, count float64, hostTag data_model.TagUnionBytes, metricInfo *format.MetricMetaValue, weightMul int) {
	if count <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.shouldDiscardIncomingData() {
		return
	}
	resolutionShard := s.resolutionShardFromHashLocked(key, resolutionHash, metricInfo)
	item, _ := resolutionShard.GetOrCreateMultiItem(key, s.config.StringTopCapacity, metricInfo, weightMul, nil)
	item.MapStringTopBytes(s.rng, topValue, count).AddCounterHost(s.rng, count, hostTag)
}

func (s *Shard) AddCounterHost(key *data_model.Key, resolutionHash uint64, count float64, hostTag data_model.TagUnionBytes, metricInfo *format.MetricMetaValue, weightMul int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.shouldDiscardIncomingData() {
		return
	}
	resolutionShard := s.resolutionShardFromHashLocked(key, resolutionHash, metricInfo)
	item, _ := resolutionShard.GetOrCreateMultiItem(key, s.config.StringTopCapacity, metricInfo, weightMul, nil)
	item.Tail.AddCounterHost(s.rng, count, hostTag)
}

func (s *Shard) AddCounterHostStringBytes(key *data_model.Key, resolutionHash uint64, topValue data_model.TagUnionBytes, count float64, hostTag data_model.TagUnionBytes, metricInfo *format.MetricMetaValue, weightMul int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.shouldDiscardIncomingData() {
		return
	}
	resolutionShard := s.resolutionShardFromHashLocked(key, resolutionHash, metricInfo)
	item, _ := resolutionShard.GetOrCreateMultiItem(key, s.config.StringTopCapacity, metricInfo, weightMul, nil)
	item.MapStringTopBytes(s.rng, topValue, count).AddCounterHost(s.rng, count, hostTag)
}

func (s *Shard) AddValueCounterHost(key *data_model.Key, resolutionHash uint64, value float64, counter float64, hostTag data_model.TagUnionBytes, metricInfo *format.MetricMetaValue, weightMul int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.shouldDiscardIncomingData() {
		return
	}
	resolutionShard := s.resolutionShardFromHashLocked(key, resolutionHash, metricInfo)
	item, _ := resolutionShard.GetOrCreateMultiItem(key, s.config.StringTopCapacity, metricInfo, weightMul, nil)
	if metricInfo != nil && metricInfo.HasPercentiles {
		item.Tail.AddValueCounterHostPercentile(s.rng, value, counter, hostTag, data_model.AgentPercentileCompression)
	} else {
		item.Tail.Value.AddValueCounterHost(s.rng, value, counter, hostTag)
	}
}

func (s *Shard) MergeItemValue(key *data_model.Key, resolutionHash uint64, itemValue *data_model.ItemValue, metricInfo *format.MetricMetaValue, weightMul int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.shouldDiscardIncomingData() {
		return
	}
	resolutionShard := s.resolutionShardFromHashLocked(key, resolutionHash, metricInfo)
	item, _ := resolutionShard.GetOrCreateMultiItem(key, s.config.StringTopCapacity, metricInfo, weightMul, nil)
	item.Tail.Value.Merge(s.rng, itemValue)
}

func (s *Shard) getMultiItem(bucket *data_model.MetricsBucket, t uint32, metricInfo *format.MetricMetaValue, weightMul int, tags []int32) *data_model.MultiItem {
	return s.getMultiItemConfig(bucket, t, metricInfo, weightMul, &s.config, tags)
}

func (s *Shard) getMultiItemConfig(bucket *data_model.MetricsBucket, t uint32, metricInfo *format.MetricMetaValue, weightMul int, config *Config, tags []int32) *data_model.MultiItem {
	key := data_model.Key{Timestamp: t, Metric: metricInfo.MetricID}
	copy(key.Tags[:], tags)
	if metricInfo.WithAggregatorID {
		key.Tags[format.AggHostTag] = s.agent.AggregatorHost
		key.Tags[format.AggShardTag] = s.agent.AggregatorShardKey
		key.Tags[format.AggReplicaTag] = s.agent.AggregatorReplicaKey
	}
	item, _ := bucket.GetOrCreateMultiItem(&key, config.StringTopCapacity, metricInfo, weightMul, nil)
	return item
}

func (s *Shard) addBuiltInsLocked() {
	// TODO - complicated code below must be checked again, because of super queue
	if s.shouldDiscardIncomingData() {
		return
	}
	resolutionShard := s.SuperQueue[s.CurrentTime%superQueueLen] // we aggregate built-ins locally into first second of one second resolution

	for _, v := range s.BuiltInItemValues {
		v.mu.Lock()
		if v.value.Count() > 0 {
			s.getMultiItem(resolutionShard, s.CurrentTime, v.metricInfo, v.weightMul, v.key.Tags[:]).
				Tail.Value.Merge(s.rng, &v.value)
		}
		v.value = data_model.ItemValue{} // simply reset Counter, even if somehow <0
		v.mu.Unlock()
	}
	elements, sumSize, averageTS, adds, evicts, timestampUpdates, timestampUpdateSkips := s.agent.mappingsCache.Stats()
	if elements > 0 {
		s.getMultiItem(resolutionShard, s.CurrentTime, format.BuiltinMetricMetaMappingCacheElements, 1,
			[]int32{0, s.agent.componentTag}).
			Tail.AddValueCounter(s.rng, float64(elements), 1)
		s.getMultiItem(resolutionShard, s.CurrentTime, format.BuiltinMetricMetaMappingCacheSize, 1,
			[]int32{0, s.agent.componentTag}).
			Tail.AddValueCounter(s.rng, float64(sumSize), 1)
		s.getMultiItem(resolutionShard, s.CurrentTime, format.BuiltinMetricMetaMappingCacheAverageTTL, 1,
			[]int32{0, s.agent.componentTag}).
			Tail.AddValueCounter(s.rng, float64(s.CurrentTime)-float64(averageTS), 1)
		s.getMultiItem(resolutionShard, s.CurrentTime, format.BuiltinMetricMetaMappingCacheEvent, 1,
			[]int32{0, s.agent.componentTag, format.TagValueIDMappingCacheEventAdd}).
			Tail.AddCounter(s.rng, float64(adds))
		s.getMultiItem(resolutionShard, s.CurrentTime, format.BuiltinMetricMetaMappingCacheEvent, 1,
			[]int32{0, s.agent.componentTag, format.TagValueIDMappingCacheEventEvict}).
			Tail.AddCounter(s.rng, float64(evicts))
		s.getMultiItem(resolutionShard, s.CurrentTime, format.BuiltinMetricMetaMappingCacheEvent, 1,
			[]int32{0, s.agent.componentTag, format.TagValueIDMappingCacheEventTimestampUpdate}).
			Tail.AddCounter(s.rng, float64(timestampUpdates))
		s.getMultiItem(resolutionShard, s.CurrentTime, format.BuiltinMetricMetaMappingCacheEvent, 1,
			[]int32{0, s.agent.componentTag, format.TagValueIDMappingCacheEventTimestampUpdateSkip}).
			Tail.AddCounter(s.rng, float64(timestampUpdateSkips))
	}

	sizeMem := s.HistoricBucketsDataSize
	sizeDiskTotal, sizeDiskUnsent := s.HistoricBucketsDataSizeDisk()
	if sizeMem > 0 {
		s.getMultiItem(resolutionShard, s.CurrentTime, format.BuiltinMetricMetaAgentHistoricQueueSize, 1,
			[]int32{0, format.TagValueIDHistoricQueueMemory, 0, 0, 0, 0, s.agent.componentTag, format.AggShardTag: s.ShardKey}).
			Tail.AddValueCounter(s.rng, float64(sizeMem), 1)
	}
	if sizeDiskUnsent > 0 {
		s.getMultiItem(resolutionShard, s.CurrentTime, format.BuiltinMetricMetaAgentHistoricQueueSize, 1,
			[]int32{0, format.TagValueIDHistoricQueueDiskUnsent, 0, 0, 0, 0, s.agent.componentTag, format.AggShardTag: s.ShardKey}).
			Tail.AddValueCounter(s.rng, float64(sizeDiskUnsent), 1)
	}
	if sent := sizeDiskTotal - sizeDiskUnsent; sent > 0 {
		s.getMultiItem(resolutionShard, s.CurrentTime, format.BuiltinMetricMetaAgentHistoricQueueSize, 1,
			[]int32{0, format.TagValueIDHistoricQueueDiskSent, 0, 0, 0, 0, s.agent.componentTag, format.AggShardTag: s.ShardKey}).
			Tail.AddValueCounter(s.rng, float64(sent), 1)
	}
	if sizeMem <= 0 && sizeDiskUnsent <= 0 { // no data waiting to be sent
		s.getMultiItem(resolutionShard, s.CurrentTime, format.BuiltinMetricMetaAgentHistoricQueueSize, 1,
			[]int32{0, format.TagValueIDHistoricQueueEmpty, 0, 0, 0, 0, s.agent.componentTag, format.AggShardTag: s.ShardKey}).
			Tail.AddValueCounter(s.rng, 0, 1)
	}

	if s.ShardNum != 0 { // heartbeats are in the first shard
		return
	}
	if s.agent.heartBeatEventType != format.TagValueIDHeartbeatEventHeartbeat { // first run
		s.addBuiltInsHeartbeatsLocked(resolutionShard, s.CurrentTime, 1) // send start event immediately
		s.agent.heartBeatEventType = format.TagValueIDHeartbeatEventHeartbeat
	}

	writeJournalVersion := func(version int64, hashStr string, journalTag int32) {
		hashTag := int32(0)
		hashRaw, _ := hex.DecodeString(hashStr)
		if len(hashRaw) >= 4 {
			hashTag = int32(binary.BigEndian.Uint32(hashRaw))
		}
		s.getMultiItem(resolutionShard, s.CurrentTime, format.BuiltinMetricMetaJournalVersions, 1,
			[]int32{0, s.agent.componentTag, 0, 0, 0, int32(version), hashTag, journalTag}).
			MapStringTop(s.rng, data_model.TagUnion{S: hashStr, I: 0}, 1).
			AddCounterHost(s.rng, 1, data_model.TagUnionBytes{})
	}

	if s.agent.journalHV != nil {
		version, hashStr, hashStrXXH3 := s.agent.journalHV()
		writeJournalVersion(version, hashStr, format.TagValueIDMetaJournalVersionsKindLegacySHA1)
		writeJournalVersion(version, hashStrXXH3, format.TagValueIDMetaJournalVersionsKindLegacyXXH3)
	}
	if s.agent.journalFastHV != nil {
		version, hashStr := s.agent.journalFastHV()
		writeJournalVersion(version, hashStr, format.TagValueIDMetaJournalVersionsKindNormalXXH3)
	}
	if s.agent.journalCompactHV != nil {
		version, hashStr := s.agent.journalCompactHV()
		writeJournalVersion(version, hashStr, format.TagValueIDMetaJournalVersionsKindCompactXXH3)
	}

	prevRUsage := s.agent.rUsage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &s.agent.rUsage)
	userTime := float64(s.agent.rUsage.Utime.Nano()-prevRUsage.Utime.Nano()) / float64(time.Second)
	sysTime := float64(s.agent.rUsage.Stime.Nano()-prevRUsage.Stime.Nano()) / float64(time.Second)

	s.getMultiItem(resolutionShard, s.CurrentTime, format.BuiltinMetricMetaUsageCPU, 1,
		[]int32{0, s.agent.componentTag, format.TagValueIDCPUUsageUser}).
		Tail.AddValueCounter(s.rng, userTime, 1)

	s.getMultiItem(resolutionShard, s.CurrentTime, format.BuiltinMetricMetaUsageCPU, 1,
		[]int32{0, s.agent.componentTag, format.TagValueIDCPUUsageSys}).
		Tail.AddValueCounter(s.rng, sysTime, 1)

	if s.CurrentTime%60 != 0 {
		// IF we sample once per minute, we do it right before sending to reduce latency
		return
	}

	resolutionShard = s.SuperQueue[(s.CurrentTime+uint32(s.agent.heartBeatSecondBucket))%superQueueLen]

	var rss float64
	if st, _ := srvfunc.GetMemStat(0); st != nil {
		rss = float64(st.Res)
	}

	s.getMultiItem(resolutionShard, s.CurrentTime, format.BuiltinMetricMetaUsageMemory, 1,
		[]int32{0, s.agent.componentTag}).
		Tail.AddValueCounter(s.rng, rss, 60)

	s.addBuiltInsHeartbeatsLocked(resolutionShard, s.CurrentTime, 60) // heartbeat once per minute
}

func (s *Shard) addBuiltInsHeartbeatsLocked(resolutionShard *data_model.MetricsBucket, nowUnix uint32, count float64) {
	uptimeSec := float64(nowUnix - s.agent.startTimestamp)

	s.getMultiItemConfig(resolutionShard, nowUnix, format.BuiltinMetricMetaHeartbeatVersion, 1, &s.config,
		[]int32{0, s.agent.componentTag, s.agent.heartBeatEventType}).
		MapStringTop(s.rng, data_model.TagUnion{S: build.Commit(), I: 0}, count).
		AddValueCounter(s.rng, uptimeSec, count)

	s.getMultiItemConfig(resolutionShard, nowUnix, format.BuiltinMetricMetaHeartbeatArgs, 1, &s.config,
		[]int32{0, s.agent.componentTag, s.agent.heartBeatEventType, s.agent.argsHash, 0, 0, 0, 0, 0, s.agent.argsLen}).
		MapStringTop(s.rng, data_model.TagUnion{S: s.agent.args, I: 0}, count).
		AddValueCounter(s.rng, uptimeSec, count)
}
