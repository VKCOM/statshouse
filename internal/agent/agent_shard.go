// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package agent

import (
	"sync"
	"time"

	"go.uber.org/atomic"
	"pgregory.net/rand"

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/format"
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

		BucketsToSend chan compressedBucketData

		BucketsToPreprocess chan *data_model.MetricsBucket

		historicBucketsToSend   []compressedBucketData // Can be slightly out of order here, we sort it every time
		historicBucketsDataSize int                    // if too many are with data, will put without data, which will be read from disk
		cond                    *sync.Cond

		HistoricOutOfWindowDropped atomic.Int64
	}

	BuiltInItemValue struct {
		mu         sync.Mutex
		key        data_model.Key
		value      data_model.ItemValue
		metricInfo *format.MetricMetaValue
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
	return s.historicBucketsDataSize
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
	item, _ := resolutionShard.GetOrCreateMultiItem(key, metricInfo, weightMul, nil)
	mv := item.MapStringTopBytes(s.rng, s.config.StringTopCapacity, topValue, count)
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
	item, _ := resolutionShard.GetOrCreateMultiItem(key, metricInfo, weightMul, nil)
	mv := item.MapStringTopBytes(s.rng, s.config.StringTopCapacity, topValue, count)
	if s.config.LegacyApplyValues {
		mv.ApplyValuesLegacy(s.rng, histogram, values, count, totalCount, hostTag, data_model.AgentPercentileCompression, metricInfo != nil && metricInfo.HasPercentiles)
	} else {
		mv.ApplyValues(s.rng, histogram, values, count, totalCount, hostTag, data_model.AgentPercentileCompression, metricInfo != nil && metricInfo.HasPercentiles)
	}
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
	item, _ := resolutionShard.GetOrCreateMultiItem(key, metricInfo, weightMul, nil)
	mv := item.MapStringTopBytes(s.rng, s.config.StringTopCapacity, topValue, count)
	mv.AddCounterHost(s.rng, count, hostTag)
}

func (s *Shard) AddCounterHost(key *data_model.Key, resolutionHash uint64, count float64, hostTag data_model.TagUnionBytes, metricInfo *format.MetricMetaValue, weightMul int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.shouldDiscardIncomingData() {
		return
	}
	resolutionShard := s.resolutionShardFromHashLocked(key, resolutionHash, metricInfo)
	item, _ := resolutionShard.GetOrCreateMultiItem(key, metricInfo, weightMul, nil)
	item.Tail.AddCounterHost(s.rng, count, hostTag)
}

func (s *Shard) AddCounterHostStringBytes(key *data_model.Key, resolutionHash uint64, topValue data_model.TagUnionBytes, count float64, hostTag data_model.TagUnionBytes, metricInfo *format.MetricMetaValue, weightMul int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.shouldDiscardIncomingData() {
		return
	}
	resolutionShard := s.resolutionShardFromHashLocked(key, resolutionHash, metricInfo)
	item, _ := resolutionShard.GetOrCreateMultiItem(key, metricInfo, weightMul, nil)
	mv := item.MapStringTopBytes(s.rng, s.config.StringTopCapacity, topValue, count)
	mv.AddCounterHost(s.rng, count, hostTag)
}

func (s *Shard) AddValueCounterHost(key *data_model.Key, resolutionHash uint64, value float64, count float64, hostTag data_model.TagUnionBytes, metricInfo *format.MetricMetaValue, weightMul int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.shouldDiscardIncomingData() {
		return
	}
	resolutionShard := s.resolutionShardFromHashLocked(key, resolutionHash, metricInfo)
	item, _ := resolutionShard.GetOrCreateMultiItem(key, metricInfo, weightMul, nil)
	if metricInfo != nil && metricInfo.HasPercentiles {
		item.Tail.AddValueCounterHostPercentile(s.rng, value, count, hostTag, data_model.AgentPercentileCompression)
	} else {
		item.Tail.Value.AddValueCounterHost(s.rng, value, count, hostTag)
	}
}

func (s *Shard) AddValueCounterStringHost(key *data_model.Key, resolutionHash uint64, topValue data_model.TagUnion, value float64, count float64, hostTag data_model.TagUnionBytes, metricInfo *format.MetricMetaValue, weightMul int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.shouldDiscardIncomingData() {
		return
	}
	resolutionShard := s.resolutionShardFromHashLocked(key, resolutionHash, metricInfo)
	item, _ := resolutionShard.GetOrCreateMultiItem(key, metricInfo, weightMul, nil)
	mv := item.MapStringTop(s.rng, s.config.StringTopCapacity, topValue, count)
	if metricInfo != nil && metricInfo.HasPercentiles {
		mv.AddValueCounterHostPercentile(s.rng, value, count, hostTag, data_model.AgentPercentileCompression)
	} else {
		mv.AddValueCounterHost(s.rng, value, count, hostTag)
	}
}

func (s *Shard) MergeItemValue(key *data_model.Key, resolutionHash uint64, itemValue *data_model.ItemValue, metricInfo *format.MetricMetaValue, weightMul int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.shouldDiscardIncomingData() {
		return
	}
	resolutionShard := s.resolutionShardFromHashLocked(key, resolutionHash, metricInfo)
	item, _ := resolutionShard.GetOrCreateMultiItem(key, metricInfo, weightMul, nil)
	item.Tail.Value.Merge(s.rng, itemValue)
}
