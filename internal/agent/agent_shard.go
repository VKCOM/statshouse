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
		perm     []int

		mu                               sync.Mutex
		config                           Config        // can change if remotely updated
		hardwareMetricResolutionResolved *atomic.Int32 // depends on config

		timeSpreadDelta time.Duration // randomly spread bucket sending through second between sources/machines

		CurrentTime    uint32
		CurrentBuckets [][]*data_model.MetricsBucket // [resolution][shard]. All disallowed resolutions are always skipped
		MissedSeconds  uint32                        // If disk is slow or computer sleeps/slows, several seconds can get into single bucket
		FutureQueue    [][]*data_model.MetricsBucket // 60 seconds long circular buffer.

		// Low res buckets work like this, example 4 seconds resolution
		// 1. data collected for 4 seconds into 4 key shards
		//   data(k0,k1,k2,k3)
		// [_  _  _  _ ]
		// 2. at the end pf 4 second interval key shards are put (merged) into future queue
		// [           ] [k1 k2 k3 k4]
		// 3. data from next future second moved into CurrentBucket during second switch

		CurentLowResBucket [][]*data_model.MetricsBucket // [resolution][shard]
		LowResFutureQueue  []*data_model.MetricsBucket   // Max 60 seconds long. Shorter if max resolution is lower.

		BucketsToSend     chan compressedBucketDataOnDisk
		BuiltInItemValues []*BuiltInItemValue // Moved into CurrentBuckets before flush

		PreprocessingBucketTime    uint32
		PreprocessingBuckets       []*data_model.MetricsBucket // CurrentBuckets is moved here, if PreviousBucket empty
		PreprocessingMissedSeconds uint32                      // copy of MissedSeconds for bucket being processed
		condPreprocess             *sync.Cond

		// only used by single shard randomly selected for sending this infp
		currentJournalVersion     int64
		currentJournalHash        string
		currentJournalHashTag     int32
		currentJournalHashSeconds float64 // for how many seconds currentJournalHash did not change and was not added to metrics. This saves tons of traffic

		HistoricBucketsToSend   []compressedBucketData // Slightly out of order here
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
		time uint32
		data []byte // first 4 bytes are uncompressed size, rest is compressed data
	}
	compressedBucketDataOnDisk struct {
		compressedBucketData
		onDisk bool // config.SaveSecondsImmediately can change while in flight
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

// If user did not set timestamp or set to 0 (default timestamp), metric arrived with 0 up to here.
// We do not want metrics with default timestamp and timestamp explicitly set by clients to get into
// different map entries due to key differences, that's why we must set timestamp here.
// Also for low-resolution metrics, we must ensure timestamps are rounded, so they again end up in the same map item,
// and clients should set timestamps freely and not make assumptions on metric resolution (it can be changed on the fly).
// Later, when sending bucket, we will make reverse operation, removing timestamps for all items which have it
// equal to bucket timestamp (only for transport efficiency), then reset timestamps on aggregator after receiving.
// This is the only correct way to operate with timestamps.
func fixKeyTimestamp(key *data_model.Key, resolution int, currentTimestamp uint32) {
	if key.Timestamp == 0 || key.Timestamp >= currentTimestamp {
		key.Timestamp = currentTimestamp
		return
	}
	// - 60 accounts for rounding below
	if currentTimestamp > data_model.BelieveTimestampWindow-60 && key.Timestamp < currentTimestamp-(data_model.BelieveTimestampWindow-60) {
		key.Timestamp = currentTimestamp - (data_model.BelieveTimestampWindow - 60)
	}
	if resolution > 1 {
		key.Timestamp = (key.Timestamp / uint32(resolution)) * uint32(resolution)
	}
}

func (s *Shard) resolutionShardFromHashLocked(hash uint64, metricInfo *format.MetricMetaValue) (*data_model.MetricsBucket, int, int) {
	resolution := 1
	if metricInfo != nil {
		if !format.HardwareMetric(metricInfo.MetricID) {
			resolution = metricInfo.EffectiveResolution // TODO - better idea?
		} else {
			resolution = int(s.hardwareMetricResolutionResolved.Load())
		}
	}
	numShards := uint64(resolution)
	// lower bits of hash are independent of higher bits used by shardFromHash function
	mul := (hash & 0xFFFFFFFF) * numShards >> 32 // trunc([0..0.9999999] * numShards) in fixed point 32.32
	return s.CurrentBuckets[resolution][mul], resolution, int(mul)
}

func (s *Shard) CreateBuiltInItemValue(key data_model.Key) *BuiltInItemValue {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := &BuiltInItemValue{key: key}
	s.BuiltInItemValues = append(s.BuiltInItemValues, result)
	return result
}

func (s *Shard) ApplyUnique(key data_model.Key, keyHash uint64, str []byte, hashes []int64, count float64, hostTag int32, metricInfo *format.MetricMetaValue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	resolutionShard, resolution, _ := s.resolutionShardFromHashLocked(keyHash, metricInfo)
	fixKeyTimestamp(&key, resolution, resolutionShard.Time)
	mi := data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, key, s.config.StringTopCapacity, metricInfo, nil)
	totalCount := float64(len(hashes))
	if count != 0 {
		totalCount = count
	}
	mi.MapStringTopBytes(str, totalCount).ApplyUnique(hashes, count, hostTag)
}

func (s *Shard) ApplyValues(key data_model.Key, keyHash uint64, str []byte, values []float64, count float64, hostTag int32, metricInfo *format.MetricMetaValue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	resolutionShard, resolution, _ := s.resolutionShardFromHashLocked(keyHash, metricInfo)
	fixKeyTimestamp(&key, resolution, resolutionShard.Time)
	mi := data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, key, s.config.StringTopCapacity, metricInfo, nil)
	totalCount := float64(len(values))
	if count != 0 {
		totalCount = count
	}
	mi.MapStringTopBytes(str, totalCount).ApplyValues(values, count, hostTag, data_model.AgentPercentileCompression, metricInfo != nil && metricInfo.HasPercentiles)
}

func (s *Shard) ApplyCounter(key data_model.Key, keyHash uint64, str []byte, count float64, hostTag int32, metricInfo *format.MetricMetaValue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	resolutionShard, resolution, _ := s.resolutionShardFromHashLocked(keyHash, metricInfo)
	fixKeyTimestamp(&key, resolution, resolutionShard.Time)
	mi := data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, key, s.config.StringTopCapacity, metricInfo, nil)
	mi.MapStringTopBytes(str, count).AddCounterHost(count, hostTag)
}

func (s *Shard) AddCounterHost(key data_model.Key, keyHash uint64, count float64, hostTag int32, metricInfo *format.MetricMetaValue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	resolutionShard, resolution, _ := s.resolutionShardFromHashLocked(keyHash, metricInfo)
	fixKeyTimestamp(&key, resolution, resolutionShard.Time)
	mi := data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, key, s.config.StringTopCapacity, metricInfo, nil)
	mi.Tail.AddCounterHost(count, hostTag)
}

func (s *Shard) AddCounterHostStringBytes(key data_model.Key, keyHash uint64, str []byte, count float64, hostTag int32, metricInfo *format.MetricMetaValue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	resolutionShard, resolution, _ := s.resolutionShardFromHashLocked(keyHash, metricInfo)
	fixKeyTimestamp(&key, resolution, resolutionShard.Time)
	mi := data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, key, s.config.StringTopCapacity, metricInfo, nil)
	mi.MapStringTopBytes(str, count).AddCounterHost(count, hostTag)
}

func (s *Shard) AddValueCounterHostStringBytes(key data_model.Key, keyHash uint64, value float64, count float64, hostTag int32, str []byte, metricInfo *format.MetricMetaValue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	resolutionShard, resolution, _ := s.resolutionShardFromHashLocked(keyHash, metricInfo)
	fixKeyTimestamp(&key, resolution, resolutionShard.Time)
	mi := data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, key, s.config.StringTopCapacity, metricInfo, nil)
	mi.MapStringTopBytes(str, count).AddValueCounterHost(value, count, hostTag)
}

func (s *Shard) AddValueCounterHost(key data_model.Key, keyHash uint64, value float64, counter float64, hostTag int32, metricInfo *format.MetricMetaValue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	resolutionShard, resolution, _ := s.resolutionShardFromHashLocked(keyHash, metricInfo)
	fixKeyTimestamp(&key, resolution, resolutionShard.Time)
	mi := data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, key, s.config.StringTopCapacity, metricInfo, nil)
	if metricInfo != nil && metricInfo.HasPercentiles {
		mi.Tail.AddValueCounterHostPercentile(value, counter, hostTag, data_model.AgentPercentileCompression)
	} else {
		mi.Tail.Value.AddValueCounterHost(value, counter, hostTag)
	}
}

func (s *Shard) AddValueArrayCounterHost(key data_model.Key, keyHash uint64, values []float64, mult float64, hostTag int32, metricInfo *format.MetricMetaValue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	resolutionShard, resolution, _ := s.resolutionShardFromHashLocked(keyHash, metricInfo)
	fixKeyTimestamp(&key, resolution, resolutionShard.Time)
	mi := data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, key, s.config.StringTopCapacity, metricInfo, nil)
	if metricInfo != nil && metricInfo.HasPercentiles {
		mi.Tail.AddValueArrayHostPercentile(values, mult, hostTag, data_model.AgentPercentileCompression)
	} else {
		mi.Tail.Value.AddValueArrayHost(values, mult, hostTag)
	}
}

func (s *Shard) AddValueArrayCounterHostStringBytes(key data_model.Key, keyHash uint64, values []float64, mult float64, hostTag int32, str []byte, metricInfo *format.MetricMetaValue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	resolutionShard, resolution, _ := s.resolutionShardFromHashLocked(keyHash, metricInfo)
	fixKeyTimestamp(&key, resolution, resolutionShard.Time)
	mi := data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, key, s.config.StringTopCapacity, metricInfo, nil)
	count := float64(len(values)) * mult
	if metricInfo != nil && metricInfo.HasPercentiles {
		mi.MapStringTopBytes(str, count).AddValueArrayHostPercentile(values, mult, hostTag, data_model.AgentPercentileCompression)
	} else {
		mi.MapStringTopBytes(str, count).Value.AddValueArrayHost(values, mult, hostTag)
	}
}

func (s *Shard) MergeItemValue(key data_model.Key, keyHash uint64, item *data_model.ItemValue, metricInfo *format.MetricMetaValue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	resolutionShard, resolution, _ := s.resolutionShardFromHashLocked(keyHash, metricInfo)
	fixKeyTimestamp(&key, resolution, resolutionShard.Time)
	mi := data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, key, s.config.StringTopCapacity, metricInfo, nil)
	mi.Tail.Value.Merge(item)
}

func (s *Shard) AddUniqueHostStringBytes(key data_model.Key, hostTag int32, str []byte, keyHash uint64, hashes []int64, count float64, metricInfo *format.MetricMetaValue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	resolutionShard, resolution, _ := s.resolutionShardFromHashLocked(keyHash, metricInfo)
	fixKeyTimestamp(&key, resolution, resolutionShard.Time)
	mi := data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, key, s.config.StringTopCapacity, metricInfo, nil)
	mi.MapStringTopBytes(str, count).AddUniqueHost(hashes, count, hostTag)
}

func (s *Shard) addBuiltInsLocked(nowUnix uint32) {
	resolutionShard := s.CurrentBuckets[1][0] // we aggregate built-ins locally into first second of second resolution
	for _, v := range s.BuiltInItemValues {
		v.mu.Lock()
		if v.value.Counter > 0 {
			mi := data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, v.key, s.config.StringTopCapacity, nil, nil)
			mi.Tail.Value.Merge(&v.value)
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
		key := s.agent.AggKey(resolutionShard.Time, format.BuiltinMetricIDJournalVersions, [16]int32{0, s.agent.componentTag, 0, 0, 0, int32(version), hashTag})
		mi := data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, key, s.config.StringTopCapacity, nil, nil)
		mi.MapStringTop(hash, count).AddCounterHost(count, 0)
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

	key := s.agent.AggKey(resolutionShard.Time, format.BuiltinMetricIDUsageCPU, [16]int32{0, s.agent.componentTag, format.TagValueIDCPUUsageUser})
	mi := data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, key, s.config.StringTopCapacity, nil, nil)
	mi.Tail.AddValueCounterHost(userTime, 1, 0)

	key = s.agent.AggKey(resolutionShard.Time, format.BuiltinMetricIDUsageCPU, [16]int32{0, s.agent.componentTag, format.TagValueIDCPUUsageSys})
	mi = data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, key, s.config.StringTopCapacity, nil, nil)
	mi.Tail.AddValueCounterHost(sysTime, 1, 0)

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

	key = s.agent.AggKey(resolutionShard.Time, format.BuiltinMetricIDUsageMemory, [16]int32{0, s.agent.componentTag})
	mi = data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, key, s.config.StringTopCapacity, nil, nil)
	mi.Tail.AddValueCounterHost(rss, 60, 0)

	s.addBuiltInsHeartbeatsLocked(resolutionShard, nowUnix, 60) // heartbeat once per minute
}

func (s *Shard) addBuiltInsHeartbeatsLocked(resolutionShard *data_model.MetricsBucket, nowUnix uint32, count float64) {
	uptimeSec := float64(nowUnix - s.agent.startTimestamp)

	key := s.agent.AggKey(resolutionShard.Time, format.BuiltinMetricIDHeartbeatVersion, [16]int32{0, s.agent.componentTag, s.agent.heartBeatEventType})
	mi := data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, key, s.config.StringTopCapacity, nil, nil)
	mi.MapStringTop(build.Commit(), count).AddValueCounterHost(uptimeSec, count, 0)

	// we send format.BuiltinMetricIDHeartbeatArgs only. Args1, Args2, Args3 are deprecated
	key = s.agent.AggKey(resolutionShard.Time, format.BuiltinMetricIDHeartbeatArgs, [16]int32{0, s.agent.componentTag, s.agent.heartBeatEventType, s.agent.argsHash, 0, 0, 0, 0, 0, s.agent.argsLen})
	mi = data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, key, s.config.StringTopCapacity, nil, nil)
	mi.MapStringTop(s.agent.args, count).AddValueCounterHost(uptimeSec, count, 0)
}
