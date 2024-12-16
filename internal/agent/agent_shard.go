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
		perm     []int

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
	if currentTime > data_model.BelieveTimestampWindow && key.Timestamp < currentTime-data_model.BelieveTimestampWindow {
		// TODO - move this code into aggregators, remove from here
		key.Timestamp = currentTime - data_model.BelieveTimestampWindow
	}
	// we cannot disallow timestamps in the future, because our conveyor can be stuck
	// or our clock wrong while client has events with correct timestamps
	// timestamp will be clamped by aggregators
	if resolution == 1 {
		slot := key.Timestamp
		if slot < sendTime {
			slot = sendTime // if late, send immediately. Helps those who are late by a tiny amount.
		}
		// if slot >= sendTime+superQueueLen - we do no special processing for slots in the future
		return s.SuperQueue[slot%superQueueLen]
	}
	// division is expensive, hence separate code for very common 1-second resolution above
	key.Timestamp = (key.Timestamp / resolution) * resolution
	resolutionShardNum := uint32((keyHash & 0xFFFFFFFF) * uint64(resolution) >> 32) // trunc([0..0.9999999] * numShards) in fixed point 32.32
	slot := key.Timestamp + resolution + resolutionShardNum
	// we could start sending 1 second earlier, adding - 1 to the slot in code above, but for now we want compatibility with legacy code.
	if slot < sendTime { // rare?
		slot += ((sendTime - slot + resolution - 1) / resolution) * resolution
		// if late, send immediately, but keep slots aligned with resolution, sometimes identically on several/many agents, hopefully improving aggregation
	}
	// if slot >= sendTime+superQueueLen - we do no special processing for slots in the future
	return s.SuperQueue[slot%superQueueLen]
}

func (s *Shard) CreateBuiltInItemValue(key *data_model.Key) *BuiltInItemValue {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := &BuiltInItemValue{key: *key}
	s.BuiltInItemValues = append(s.BuiltInItemValues, result)
	return result
}

func (s *Shard) ApplyUnique(key *data_model.Key, keyHash uint64, str []byte, hashes []int64, count float64, hostTag int32, metricInfo *format.MetricMetaValue) {
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
	resolutionShard := s.resolutionShardFromHashLocked(key, keyHash, metricInfo)
	item, _ := resolutionShard.GetOrCreateMultiItem(key, s.config.StringTopCapacity, metricInfo)
	mv := item.MapStringTopBytes(s.rng, str, count)
	mv.ApplyUnique(s.rng, hashes, count, hostTag)
}

func (s *Shard) ApplyValues(key *data_model.Key, keyHash uint64, str []byte, histogram [][2]float64, values []float64, count float64, hostTag int32, metricInfo *format.MetricMetaValue) {
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
	resolutionShard := s.resolutionShardFromHashLocked(key, keyHash, metricInfo)
	item, _ := resolutionShard.GetOrCreateMultiItem(key, s.config.StringTopCapacity, metricInfo)
	mv := item.MapStringTopBytes(s.rng, str, count)
	mv.ApplyValues(s.rng, histogram, values, count, totalCount, hostTag, data_model.AgentPercentileCompression, metricInfo != nil && metricInfo.HasPercentiles)
}

func (s *Shard) ApplyCounter(key *data_model.Key, keyHash uint64, str []byte, count float64, hostTag int32, metricInfo *format.MetricMetaValue) {
	if count <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.shouldDiscardIncomingData() {
		return
	}
	resolutionShard := s.resolutionShardFromHashLocked(key, keyHash, metricInfo)
	item, _ := resolutionShard.GetOrCreateMultiItem(key, s.config.StringTopCapacity, metricInfo)
	item.MapStringTopBytes(s.rng, str, count).AddCounterHost(s.rng, count, hostTag)
}

func (s *Shard) AddCounterHost(key *data_model.Key, keyHash uint64, count float64, hostTagId int32, metricInfo *format.MetricMetaValue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.shouldDiscardIncomingData() {
		return
	}
	resolutionShard := s.resolutionShardFromHashLocked(key, keyHash, metricInfo)
	item, _ := resolutionShard.GetOrCreateMultiItem(key, s.config.StringTopCapacity, metricInfo)
	item.Tail.AddCounterHost(s.rng, count, hostTagId)
}

func (s *Shard) AddCounterHostStringBytes(key *data_model.Key, keyHash uint64, str []byte, count float64, hostTag int32, metricInfo *format.MetricMetaValue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.shouldDiscardIncomingData() {
		return
	}
	resolutionShard := s.resolutionShardFromHashLocked(key, keyHash, metricInfo)
	item, _ := resolutionShard.GetOrCreateMultiItem(key, s.config.StringTopCapacity, metricInfo)
	item.MapStringTopBytes(s.rng, str, count).AddCounterHost(s.rng, count, hostTag)
}

func (s *Shard) AddValueCounterHostStringBytes(key *data_model.Key, keyHash uint64, value float64, count float64, hostTag int32, str []byte, metricInfo *format.MetricMetaValue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.shouldDiscardIncomingData() {
		return
	}
	resolutionShard := s.resolutionShardFromHashLocked(key, keyHash, metricInfo)
	item, _ := resolutionShard.GetOrCreateMultiItem(key, s.config.StringTopCapacity, metricInfo)
	item.MapStringTopBytes(s.rng, str, count).AddValueCounterHost(s.rng, value, count, hostTag)
}

func (s *Shard) AddValueCounterHost(key *data_model.Key, keyHash uint64, value float64, counter float64, hostTag int32, metricInfo *format.MetricMetaValue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.shouldDiscardIncomingData() {
		return
	}
	resolutionShard := s.resolutionShardFromHashLocked(key, keyHash, metricInfo)
	item, _ := resolutionShard.GetOrCreateMultiItem(key, s.config.StringTopCapacity, metricInfo)
	if metricInfo != nil && metricInfo.HasPercentiles {
		item.Tail.AddValueCounterHostPercentile(s.rng, value, counter, hostTag, data_model.AgentPercentileCompression)
	} else {
		item.Tail.Value.AddValueCounterHost(s.rng, value, counter, hostTag)
	}
}

func (s *Shard) AddValueArrayCounterHost(key *data_model.Key, keyHash uint64, values []float64, mult float64, hostTag int32, metricInfo *format.MetricMetaValue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.shouldDiscardIncomingData() {
		return
	}
	resolutionShard := s.resolutionShardFromHashLocked(key, keyHash, metricInfo)
	item, _ := resolutionShard.GetOrCreateMultiItem(key, s.config.StringTopCapacity, metricInfo)
	if metricInfo != nil && metricInfo.HasPercentiles {
		item.Tail.AddValueArrayHostPercentile(s.rng, values, mult, hostTag, data_model.AgentPercentileCompression)
	} else {
		item.Tail.Value.AddValueArrayHost(s.rng, values, mult, hostTag)
	}
}

func (s *Shard) AddValueArrayCounterHostStringBytes(key *data_model.Key, keyHash uint64, values []float64, mult float64, hostTag int32, str []byte, metricInfo *format.MetricMetaValue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.shouldDiscardIncomingData() {
		return
	}
	resolutionShard := s.resolutionShardFromHashLocked(key, keyHash, metricInfo)
	item, _ := resolutionShard.GetOrCreateMultiItem(key, s.config.StringTopCapacity, metricInfo)
	count := float64(len(values)) * mult
	if metricInfo != nil && metricInfo.HasPercentiles {
		item.MapStringTopBytes(s.rng, str, count).AddValueArrayHostPercentile(s.rng, values, mult, hostTag, data_model.AgentPercentileCompression)
	} else {
		item.MapStringTopBytes(s.rng, str, count).Value.AddValueArrayHost(s.rng, values, mult, hostTag)
	}
}

func (s *Shard) MergeItemValue(key *data_model.Key, keyHash uint64, itemValue *data_model.ItemValue, metricInfo *format.MetricMetaValue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.shouldDiscardIncomingData() {
		return
	}
	resolutionShard := s.resolutionShardFromHashLocked(key, keyHash, metricInfo)
	item, _ := resolutionShard.GetOrCreateMultiItem(key, s.config.StringTopCapacity, metricInfo)
	item.Tail.Value.Merge(s.rng, itemValue)
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
			item, _ := resolutionShard.GetOrCreateMultiItem(&v.key, s.config.StringTopCapacity, nil)
			item.Tail.Value.Merge(s.rng, &v.value)
			v.value = data_model.ItemValue{} // Moving below 'if' would reset Counter if <0. Will complicate debugging, so no.
		}
		v.mu.Unlock()
	}
	if s.ShardNum != 0 { // heartbeats are in the first shard
		return
	}
	if s.agent.heartBeatEventType != format.TagValueIDHeartbeatEventHeartbeat { // first run
		s.addBuiltInsHeartbeatsLocked(resolutionShard, s.CurrentTime, 1) // send start event immediately
		s.agent.heartBeatEventType = format.TagValueIDHeartbeatEventHeartbeat
	}
	// this logic with currentJournalHashSeconds and currentJournalVersion ensures there is exactly 60 samples per minute,
	// sending is once per minute when no changes, but immediate sending of journal version each second when it changed
	// standard metrics do not allow this, but heartbeats are magic.
	writeJournalVersion := func(version int64, hash string, hashTag int32, count float64) {
		key := s.agent.AggKey(resolutionShard.Time, format.BuiltinMetricIDJournalVersions, [format.MaxTags]int32{0, s.agent.componentTag, 0, 0, 0, int32(version), hashTag})
		item, _ := resolutionShard.GetOrCreateMultiItem(key, s.config.StringTopCapacity, nil)
		item.MapStringTop(s.rng, hash, count).AddCounterHost(s.rng, count, 0)
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

	resolutionShard = s.SuperQueue[(((s.CurrentTime/60)*60)+60+uint32(s.agent.heartBeatSecondBucket))%superQueueLen]

	prevRUsage := s.agent.rUsage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &s.agent.rUsage)
	userTime := float64(s.agent.rUsage.Utime.Nano()-prevRUsage.Utime.Nano()) / float64(time.Second)
	sysTime := float64(s.agent.rUsage.Stime.Nano()-prevRUsage.Stime.Nano()) / float64(time.Second)

	key := s.agent.AggKey(resolutionShard.Time, format.BuiltinMetricIDUsageCPU, [format.MaxTags]int32{0, s.agent.componentTag, format.TagValueIDCPUUsageUser})
	item, _ := resolutionShard.GetOrCreateMultiItem(key, s.config.StringTopCapacity, nil)
	item.Tail.AddValueCounterHost(s.rng, userTime, 1, 0)

	key = s.agent.AggKey(resolutionShard.Time, format.BuiltinMetricIDUsageCPU, [format.MaxTags]int32{0, s.agent.componentTag, format.TagValueIDCPUUsageSys})
	item, _ = resolutionShard.GetOrCreateMultiItem(key, s.config.StringTopCapacity, nil)
	item.Tail.AddValueCounterHost(s.rng, sysTime, 1, 0)

	if s.CurrentTime%60 != 0 {
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
	item, _ = resolutionShard.GetOrCreateMultiItem(key, s.config.StringTopCapacity, nil)
	item.Tail.AddValueCounterHost(s.rng, rss, 60, 0)

	s.addBuiltInsHeartbeatsLocked(resolutionShard, s.CurrentTime, 60) // heartbeat once per minute
}

func (s *Shard) addBuiltInsHeartbeatsLocked(resolutionShard *data_model.MetricsBucket, nowUnix uint32, count float64) {
	uptimeSec := float64(nowUnix - s.agent.startTimestamp)

	key := s.agent.AggKey(resolutionShard.Time, format.BuiltinMetricIDHeartbeatVersion, [format.MaxTags]int32{0, s.agent.componentTag, s.agent.heartBeatEventType})
	item, _ := resolutionShard.GetOrCreateMultiItem(key, s.config.StringTopCapacity, nil)
	item.MapStringTop(s.rng, build.Commit(), count).AddValueCounterHost(s.rng, uptimeSec, count, 0)

	// we send format.BuiltinMetricIDHeartbeatArgs only. Args1, Args2, Args3 are deprecated
	key = s.agent.AggKey(resolutionShard.Time, format.BuiltinMetricIDHeartbeatArgs, [format.MaxTags]int32{0, s.agent.componentTag, s.agent.heartBeatEventType, s.agent.argsHash, 0, 0, 0, 0, 0, s.agent.argsLen})
	item, _ = resolutionShard.GetOrCreateMultiItem(key, s.config.StringTopCapacity, nil)
	item.MapStringTop(s.rng, s.agent.args, count).AddValueCounterHost(s.rng, uptimeSec, count, 0)
}
