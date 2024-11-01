// Copyright 2022 V Kontakte LLC
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

type (
	// Shard gets data after initial hashing and shard number
	Shard struct {
		// Never change, so do not require protection
		agent    *Agent
		ShardNum int
		ShardKey int32
		rng      *rand.Rand
		perm     []int

		mu                                   sync.Mutex
		config                               Config       // can change if remotely updated
		hardwareMetricResolutionResolved     atomic.Int32 // depends on config
		hardwareSlowMetricResolutionResolved atomic.Int32 // depends on config

		timeSpreadDelta time.Duration // randomly spread bucket sending through second between sources/machines

		addBuiltInsTime uint32 // separate for simplicity

		CurrentBuckets [61][]*data_model.MetricsBucket // [resolution][shard]. All disallowed resolutions are always skipped
		NextBuckets    [61][]*data_model.MetricsBucket // [resolution][shard]. All disallowed resolutions are always skipped
		FutureQueue    [60][]*data_model.MetricsBucket // 60 seconds long circular buffer.
		// Current buckets work like this, example 4 seconds resolution
		// 1. data collected for 4 seconds into 4 key shards
		//   data(k0,k1,k2,k3)
		// [_  _  _  _ ]
		// 2. at the end pf 4 second interval key shards are put (merged) into future queue
		// [           ] [k1 k2 k3 k4]
		// 3. data from next future second moved into CurrentBucket during second switch
		// Next buckets are simply buckets with timestamp + resolution, when current buckets are moved
		// into future queue, next buckets become current buckets and new next buckets are added
		// beware!
		// we must spread 1-minute resolution metric rows around next minute deterministically,
		// all agent must assign the same rows to the same second, so that when aggregator
		// works on that second, all those rows aggregate together.
		stopReceivingIncomingData bool
		// We have lots of async components keeping writing metrics into agent during shutdown.
		// We set this bool as a circuit breaker, so new data will not be added to CurrentBuckets/NextBuckets
		// And shutdown code can flush them to disk without any non-deterministic behavior

		BucketsToSend     chan compressedBucketData
		BuiltInItemValues []*BuiltInItemValue // Moved into CurrentBuckets before flush

		BucketsToPreprocess chan preprocessorBucketData

		// only used by single shard randomly selected for sending this infp
		currentJournalVersion     int64
		currentJournalHash        string
		currentJournalHashTag     int32
		currentJournalHashSeconds float64 // for how many seconds currentJournalHash did not change and was not added to metrics. This saves tons of traffic

		HistoricBucketsToSend   []compressedBucketData // Can be slightly out of order here, we sort it every time
		HistoricBucketsDataSize int                    // if too many are with data, will put without data, which will be read from disk
		cond                    *sync.Cond

		uniqueValueMu   sync.Mutex
		uniqueValuePool [][][]int64 // reuse pool

		HistoricOutOfWindowDropped atomic.Int64
	}

	BuiltInItemValue struct {
		mu    sync.Mutex
		key   data_model.Key
		value data_model.ItemValue
	}

	compressedBucketData struct {
		id   int64 // in disk queue, or 0 if working without disk
		time uint32
		data []byte // first 4 bytes are uncompressed size, rest is compressed data
	}

	preprocessorBucketData struct {
		time    uint32
		buckets []*data_model.MetricsBucket // contains a bunch of various resolution and timestamp items from future queue
	}
)

func (s *Shard) HistoricBucketsDataSizeMemory() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.HistoricBucketsDataSize
}

func (s *Shard) getUniqueValuesCache(notSkippedShards int) [][]int64 {
	var uniqueValues [][]int64
	s.uniqueValueMu.Lock()
	if l := len(s.uniqueValuePool); l != 0 {
		uniqueValues = s.uniqueValuePool[l-1]
		s.uniqueValuePool = s.uniqueValuePool[:l-1]
	}
	s.uniqueValueMu.Unlock()
	if len(uniqueValues) != notSkippedShards {
		uniqueValues = make([][]int64, notSkippedShards) // We do not care about very rare realloc if notSkippedShards change
	} else {
		for i := range uniqueValues {
			uniqueValues[i] = uniqueValues[i][:0]
		}
	}
	return uniqueValues
}

func (s *Shard) putUniqueValuesCache(uniqueValues [][]int64) {
	s.uniqueValueMu.Lock()
	defer s.uniqueValueMu.Unlock()
	s.uniqueValuePool = append(s.uniqueValuePool, uniqueValues)
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
func (s *Shard) resolutionShardFromHashLocked(key *data_model.Key, keyHash uint64, metricInfo *format.MetricMetaValue) *data_model.MetricsBucket {
	resolution := 1
	if metricInfo != nil {
		if !format.HardwareMetric(metricInfo.MetricID) {
			resolution = metricInfo.EffectiveResolution
		} else {
			if metricInfo.IsHardwareSlowMetric {
				resolution = int(s.hardwareSlowMetricResolutionResolved.Load())
			} else {
				resolution = int(s.hardwareMetricResolutionResolved.Load())
			}
		}
	}
	resolutionShardNum := 0
	if resolution > 1 { // division is expensive
		key.Timestamp = (key.Timestamp / uint32(resolution)) * uint32(resolution)
		resolutionShardNum = int((keyHash & 0xFFFFFFFF) * uint64(resolution) >> 32) // trunc([0..0.9999999] * numShards) in fixed point 32.32
	}
	currentShard := s.CurrentBuckets[resolution][resolutionShardNum]
	currentTimestamp := currentShard.Time
	if key.Timestamp == 0 {
		// we have lots of builtin metrics in aggregator which should correspond to "current" second.
		// but unfortunately now agent's current second is lagging behind.
		// TODO - add explicit timestamp to all of them, then do panic here
		// panic("all builtin metrics must have correct timestamp set at this point")
		key.Timestamp = currentTimestamp
		return currentShard
	}

	if key.Timestamp <= currentTimestamp { // older or current, goes to current bucket
		if currentTimestamp > data_model.BelieveTimestampWindow && key.Timestamp < currentTimestamp-data_model.BelieveTimestampWindow {
			// we shift by the qhole number of minutes, so get correct timestamp for any resolution
			key.Timestamp = currentTimestamp - data_model.BelieveTimestampWindow
		}
		return currentShard
	}
	nextShard := s.NextBuckets[resolution][resolutionShardNum]
	// we cannot disallow timestamps in the future, because our conveyor can be stuck
	// or our clock wrong while client has events with correct timestamps
	// timestamp will be clamped by aggregators
	// if key.Timestamp > nextShard.Time {
	//	key.Timestamp = nextShard.Time
	// }
	return nextShard
}

func (s *Shard) CreateBuiltInItemValue(key data_model.Key) *BuiltInItemValue {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := &BuiltInItemValue{key: key}
	s.BuiltInItemValues = append(s.BuiltInItemValues, result)
	return result
}

func (s *Shard) ApplyUnique(key data_model.Key, keyHash uint64, str []byte, hashes []int64, count float64, hostTag int32, metricInfo *format.MetricMetaValue) {
	if count == 0 {
		count = float64(len(hashes))
	}
	if count <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopReceivingIncomingData {
		return
	}
	resolutionShard := s.resolutionShardFromHashLocked(&key, keyHash, metricInfo)
	mi := data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, key, s.config.StringTopCapacity, metricInfo, nil)
	mv := mi.MapStringTopBytes(s.rng, str, count)
	mv.ApplyUnique(s.rng, hashes, count, hostTag)
}

func (s *Shard) ApplyValues(key data_model.Key, keyHash uint64, str []byte, histogram [][2]float64, values []float64, count float64, hostTag int32, metricInfo *format.MetricMetaValue) {
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
	if s.stopReceivingIncomingData {
		return
	}
	resolutionShard := s.resolutionShardFromHashLocked(&key, keyHash, metricInfo)
	mi := data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, key, s.config.StringTopCapacity, metricInfo, nil)
	mv := mi.MapStringTopBytes(s.rng, str, count)
	mv.ApplyValues(s.rng, histogram, values, count, totalCount, hostTag, data_model.AgentPercentileCompression, metricInfo != nil && metricInfo.HasPercentiles)
}

func (s *Shard) ApplyCounter(key data_model.Key, keyHash uint64, str []byte, count float64, hostTag int32, metricInfo *format.MetricMetaValue) {
	if count <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopReceivingIncomingData {
		return
	}
	resolutionShard := s.resolutionShardFromHashLocked(&key, keyHash, metricInfo)
	mi := data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, key, s.config.StringTopCapacity, metricInfo, nil)
	mi.MapStringTopBytes(s.rng, str, count).AddCounterHost(s.rng, count, hostTag)
}

func (s *Shard) AddCounterHost(key data_model.Key, keyHash uint64, count float64, hostTagId int32, metricInfo *format.MetricMetaValue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopReceivingIncomingData {
		return
	}
	resolutionShard := s.resolutionShardFromHashLocked(&key, keyHash, metricInfo)
	mi := data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, key, s.config.StringTopCapacity, metricInfo, nil)
	mi.Tail.AddCounterHost(s.rng, count, hostTagId)
}

func (s *Shard) AddCounterHostStringBytes(key data_model.Key, keyHash uint64, str []byte, count float64, hostTag int32, metricInfo *format.MetricMetaValue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopReceivingIncomingData {
		return
	}
	resolutionShard := s.resolutionShardFromHashLocked(&key, keyHash, metricInfo)
	mi := data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, key, s.config.StringTopCapacity, metricInfo, nil)
	mi.MapStringTopBytes(s.rng, str, count).AddCounterHost(s.rng, count, hostTag)
}

func (s *Shard) AddValueCounterHostStringBytes(key data_model.Key, keyHash uint64, value float64, count float64, hostTag int32, str []byte, metricInfo *format.MetricMetaValue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopReceivingIncomingData {
		return
	}
	resolutionShard := s.resolutionShardFromHashLocked(&key, keyHash, metricInfo)
	mi := data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, key, s.config.StringTopCapacity, metricInfo, nil)
	mi.MapStringTopBytes(s.rng, str, count).AddValueCounterHost(s.rng, value, count, hostTag)
}

func (s *Shard) AddValueCounterHost(key data_model.Key, keyHash uint64, value float64, counter float64, hostTag int32, metricInfo *format.MetricMetaValue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopReceivingIncomingData {
		return
	}
	resolutionShard := s.resolutionShardFromHashLocked(&key, keyHash, metricInfo)
	mi := data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, key, s.config.StringTopCapacity, metricInfo, nil)
	if metricInfo != nil && metricInfo.HasPercentiles {
		mi.Tail.AddValueCounterHostPercentile(s.rng, value, counter, hostTag, data_model.AgentPercentileCompression)
	} else {
		mi.Tail.Value.AddValueCounterHost(s.rng, value, counter, hostTag)
	}
}

func (s *Shard) AddValueArrayCounterHost(key data_model.Key, keyHash uint64, values []float64, mult float64, hostTag int32, metricInfo *format.MetricMetaValue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopReceivingIncomingData {
		return
	}
	resolutionShard := s.resolutionShardFromHashLocked(&key, keyHash, metricInfo)
	mi := data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, key, s.config.StringTopCapacity, metricInfo, nil)
	if metricInfo != nil && metricInfo.HasPercentiles {
		mi.Tail.AddValueArrayHostPercentile(s.rng, values, mult, hostTag, data_model.AgentPercentileCompression)
	} else {
		mi.Tail.Value.AddValueArrayHost(s.rng, values, mult, hostTag)
	}
}

func (s *Shard) AddValueArrayCounterHostStringBytes(key data_model.Key, keyHash uint64, values []float64, mult float64, hostTag int32, str []byte, metricInfo *format.MetricMetaValue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopReceivingIncomingData {
		return
	}
	resolutionShard := s.resolutionShardFromHashLocked(&key, keyHash, metricInfo)
	mi := data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, key, s.config.StringTopCapacity, metricInfo, nil)
	count := float64(len(values)) * mult
	if metricInfo != nil && metricInfo.HasPercentiles {
		mi.MapStringTopBytes(s.rng, str, count).AddValueArrayHostPercentile(s.rng, values, mult, hostTag, data_model.AgentPercentileCompression)
	} else {
		mi.MapStringTopBytes(s.rng, str, count).Value.AddValueArrayHost(s.rng, values, mult, hostTag)
	}
}

func (s *Shard) MergeItemValue(key data_model.Key, keyHash uint64, item *data_model.ItemValue, metricInfo *format.MetricMetaValue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopReceivingIncomingData {
		return
	}
	resolutionShard := s.resolutionShardFromHashLocked(&key, keyHash, metricInfo)
	mi := data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, key, s.config.StringTopCapacity, metricInfo, nil)
	mi.Tail.Value.Merge(s.rng, item)
}

func (s *Shard) addBuiltInsLocked(nowUnix uint32) {
	if s.stopReceivingIncomingData {
		return
	}
	resolutionShard := s.CurrentBuckets[1][0] // we aggregate built-ins locally into first second of one second resolution
	for _, v := range s.BuiltInItemValues {
		v.mu.Lock()
		if v.value.Count() > 0 {
			mi := data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, v.key, s.config.StringTopCapacity, nil, nil)
			mi.Tail.Value.Merge(s.rng, &v.value)
			v.value = data_model.ItemValue{} // Moving below 'if' would reset Counter if <0. Will complicate debugging, so no.
		}
		v.mu.Unlock()
	}
	if s.ShardNum != 0 { // heartbeats are in the first shard
		return
	}
	if s.agent.heartBeatEventType != format.TagValueIDHeartbeatEventHeartbeat { // first run
		s.addBuiltInsHeartbeatsLocked(resolutionShard, nowUnix, 1) // send start event immediately
		s.agent.heartBeatEventType = format.TagValueIDHeartbeatEventHeartbeat
	}
	// this logic with currentJournalHashSeconds and currentJournalVersion ensures there is exactly 60 samples per minute,
	// sending is once per minute when no changes, but immediate sending of journal version each second when it changed
	// standard metrics do not allow this, but heartbeats are magic.
	writeJournalVersion := func(version int64, hash string, hashTag int32, count float64) {
		key := s.agent.AggKey(resolutionShard.Time, format.BuiltinMetricIDJournalVersions, [format.MaxTags]int32{0, s.agent.componentTag, 0, 0, 0, int32(version), hashTag})
		mi := data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, key, s.config.StringTopCapacity, nil, nil)
		mi.MapStringTop(s.rng, hash, count).AddCounterHost(s.rng, count, 0)
	}
	if s.agent.metricStorage != nil { // nil only on ingress proxy for now
		metricJournalVersion := s.agent.metricStorage.Version()
		metricJournalHash := s.agent.metricStorage.StateHash()

		metricJournalHashTag := int32(0)
		metricJournalHashRaw, _ := hex.DecodeString(metricJournalHash)
		if len(metricJournalHashRaw) >= 4 {
			metricJournalHashTag = int32(binary.BigEndian.Uint32(metricJournalHashRaw))
		}

		if metricJournalHash != s.currentJournalHash {
			if s.currentJournalHashSeconds != 0 {
				writeJournalVersion(s.currentJournalVersion, s.currentJournalHash, s.currentJournalHashTag, s.currentJournalHashSeconds)
				s.currentJournalHashSeconds = 0
			}
			s.currentJournalVersion = metricJournalVersion
			s.currentJournalHash = metricJournalHash
			s.currentJournalHashTag = metricJournalHashTag
			writeJournalVersion(s.currentJournalVersion, s.currentJournalHash, s.currentJournalHashTag, 1)
		} else {
			s.currentJournalHashSeconds++
		}
	}

	resolutionShard = s.CurrentBuckets[60][s.agent.heartBeatSecondBucket]

	prevRUsage := s.agent.rUsage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &s.agent.rUsage)
	userTime := float64(s.agent.rUsage.Utime.Nano()-prevRUsage.Utime.Nano()) / float64(time.Second)
	sysTime := float64(s.agent.rUsage.Stime.Nano()-prevRUsage.Stime.Nano()) / float64(time.Second)

	key := s.agent.AggKey(resolutionShard.Time, format.BuiltinMetricIDUsageCPU, [format.MaxTags]int32{0, s.agent.componentTag, format.TagValueIDCPUUsageUser})
	mi := data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, key, s.config.StringTopCapacity, nil, nil)
	mi.Tail.AddValueCounterHost(s.rng, userTime, 1, 0)

	key = s.agent.AggKey(resolutionShard.Time, format.BuiltinMetricIDUsageCPU, [format.MaxTags]int32{0, s.agent.componentTag, format.TagValueIDCPUUsageSys})
	mi = data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, key, s.config.StringTopCapacity, nil, nil)
	mi.Tail.AddValueCounterHost(s.rng, sysTime, 1, 0)

	if nowUnix%60 != 0 {
		// IF we sample once per minute, we do it right before sending to reduce latency
		return
	}
	if s.currentJournalHashSeconds != 0 {
		writeJournalVersion(s.currentJournalVersion, s.currentJournalHash, s.currentJournalHashTag, s.currentJournalHashSeconds)
		s.currentJournalHashSeconds = 0
	}

	var rss float64
	if st, _ := srvfunc.GetMemStat(0); st != nil {
		rss = float64(st.Res)
	}

	key = s.agent.AggKey(resolutionShard.Time, format.BuiltinMetricIDUsageMemory, [format.MaxTags]int32{0, s.agent.componentTag})
	mi = data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, key, s.config.StringTopCapacity, nil, nil)
	mi.Tail.AddValueCounterHost(s.rng, rss, 60, 0)

	s.addBuiltInsHeartbeatsLocked(resolutionShard, nowUnix, 60) // heartbeat once per minute
}

func (s *Shard) addBuiltInsHeartbeatsLocked(resolutionShard *data_model.MetricsBucket, nowUnix uint32, count float64) {
	uptimeSec := float64(nowUnix - s.agent.startTimestamp)

	key := s.agent.AggKey(resolutionShard.Time, format.BuiltinMetricIDHeartbeatVersion, [format.MaxTags]int32{0, s.agent.componentTag, s.agent.heartBeatEventType})
	mi := data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, key, s.config.StringTopCapacity, nil, nil)
	mi.MapStringTop(s.rng, build.Commit(), count).AddValueCounterHost(s.rng, uptimeSec, count, 0)

	// we send format.BuiltinMetricIDHeartbeatArgs only. Args1, Args2, Args3 are deprecated
	key = s.agent.AggKey(resolutionShard.Time, format.BuiltinMetricIDHeartbeatArgs, [format.MaxTags]int32{0, s.agent.componentTag, s.agent.heartBeatEventType, s.agent.argsHash, 0, 0, 0, 0, 0, s.agent.argsLen})
	mi = data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, key, s.config.StringTopCapacity, nil, nil)
	mi.MapStringTop(s.rng, s.agent.args, count).AddValueCounterHost(s.rng, uptimeSec, count, 0)
}
