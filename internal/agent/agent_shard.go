// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package agent

import (
	"sync"
	"syscall"
	"time"

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/vkgo/build"
	"github.com/vkcom/statshouse/internal/vkgo/srvfunc"

	"go.uber.org/atomic"
)

type (
	// Shard gets data after initial hashing and shard number
	Shard struct {
		alive atomic.Bool

		// Never change, so do not require protection
		config          Config
		statshouse      *Agent
		ShardReplicaNum int
		perm            []int

		mu sync.Mutex

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

		BucketsToSend     chan compressedBucketData
		BuiltInItemValues []*BuiltInItemValue // Moved into CurrentBuckets before flush

		PreprocessingBucketTime    uint32
		PreprocessingBuckets       []*data_model.MetricsBucket // CurrentBuckets is moved here, if PreviousBucket empty
		PreprocessingMissedSeconds uint32                      // copy of MissedSeconds for bucket being processed
		condPreprocess             *sync.Cond

		// only used by single shard randomly selected for sending this infp
		currentJournalVersion     int64
		currentJournalHash        string
		currentJournalHashSeconds float64 // for how many seconds currentJournalHash did not change and was not added to metrics. This saves tons of traffic

		HistoricBucketsToSend   []compressedBucketData // Slightly out of order here
		HistoricBucketsDataSize int                    // if too many are with data, will put without data, which will be read from disk
		cond                    *sync.Cond

		client tlstatshouse.Client

		// aggregator is considered live at start.
		// then, if K of L last recent conveyor sends fail, it is considered dead and keepalive process started
		// if L of L keepalives succeed, aggregator is considered live again

		// if original aggregator is live, data is sent to it
		// if original aggregator is dead, spare is selected by time % num_spares
		//      if spare is live, data is sent to it
		//      if spare is also dead, data is sent to original
		lastSendSuccessful []bool
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
)

func (s *Shard) HistoricBucketsDataSizeMemory() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.HistoricBucketsDataSize
}

func (s *Shard) HistoricBucketsDataSizeDisk() int64 {
	if s.statshouse.diskCache == nil {
		return 0
	}
	return s.statshouse.diskCache.TotalFileSize(s.ShardReplicaNum)
}

func (s *Shard) HistoricBucketsDataSizeDiskSum() int64 {
	if s.statshouse.diskCache == nil {
		return 0
	}
	result := int64(0)
	for i := range s.statshouse.Shards {
		result += s.statshouse.diskCache.TotalFileSize(i)
	}
	return result
}

func (s *Shard) HistoricBucketsDataSizeMemSum() int64 {
	return s.statshouse.historicBucketsDataSize.Load()
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
		resolution = metricInfo.EffectiveResolution // TODO - better idea?
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
	mi := data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, key, s.config.StringTopCapacity, nil)
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
	mi := data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, key, s.config.StringTopCapacity, nil)
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
	mi := data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, key, s.config.StringTopCapacity, nil)
	mi.MapStringTopBytes(str, count).AddCounterHost(count, hostTag)
}

func (s *Shard) AddCounterHost(key data_model.Key, keyHash uint64, count float64, hostTag int32, metricInfo *format.MetricMetaValue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	resolutionShard, resolution, _ := s.resolutionShardFromHashLocked(keyHash, metricInfo)
	fixKeyTimestamp(&key, resolution, resolutionShard.Time)
	mi := data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, key, s.config.StringTopCapacity, nil)
	mi.Tail.AddCounterHost(count, hostTag)
}

func (s *Shard) AddCounterHostStringBytes(key data_model.Key, keyHash uint64, str []byte, count float64, hostTag int32, metricInfo *format.MetricMetaValue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	resolutionShard, resolution, _ := s.resolutionShardFromHashLocked(keyHash, metricInfo)
	fixKeyTimestamp(&key, resolution, resolutionShard.Time)
	mi := data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, key, s.config.StringTopCapacity, nil)
	mi.MapStringTopBytes(str, count).AddCounterHost(count, hostTag)
}

func (s *Shard) AddValueCounterHostStringBytes(key data_model.Key, keyHash uint64, value float64, count float64, hostTag int32, str []byte, metricInfo *format.MetricMetaValue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	resolutionShard, resolution, _ := s.resolutionShardFromHashLocked(keyHash, metricInfo)
	fixKeyTimestamp(&key, resolution, resolutionShard.Time)
	mi := data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, key, s.config.StringTopCapacity, nil)
	mi.MapStringTopBytes(str, count).AddValueCounterHost(value, count, hostTag)
}

func (s *Shard) AddValueCounterHost(key data_model.Key, keyHash uint64, value float64, counter float64, hostTag int32, metricInfo *format.MetricMetaValue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	resolutionShard, resolution, _ := s.resolutionShardFromHashLocked(keyHash, metricInfo)
	fixKeyTimestamp(&key, resolution, resolutionShard.Time)
	mi := data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, key, s.config.StringTopCapacity, nil)
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
	mi := data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, key, s.config.StringTopCapacity, nil)
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
	mi := data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, key, s.config.StringTopCapacity, nil)
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
	mi := data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, key, s.config.StringTopCapacity, nil)
	mi.Tail.Value.Merge(item)
}

func (s *Shard) AddUniqueHostStringBytes(key data_model.Key, hostTag int32, str []byte, keyHash uint64, hashes []int64, count float64, metricInfo *format.MetricMetaValue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	resolutionShard, resolution, _ := s.resolutionShardFromHashLocked(keyHash, metricInfo)
	fixKeyTimestamp(&key, resolution, resolutionShard.Time)
	mi := data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, key, s.config.StringTopCapacity, nil)
	mi.MapStringTopBytes(str, count).AddUniqueHost(hashes, count, hostTag)
}

func (s *Shard) addBuiltInsLocked(nowUnix uint32) {
	resolutionShard := s.CurrentBuckets[1][0] // we aggregate built-ins locally into first second of second resolution
	for _, v := range s.BuiltInItemValues {
		v.mu.Lock()
		if v.value.Counter > 0 {
			mi := data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, v.key, s.config.StringTopCapacity, nil)
			mi.Tail.Value.Merge(&v.value)
			v.value = data_model.ItemValue{} // Moving below 'if' would reset Counter if <0. Will complicate debugging, so no.
		}
		v.mu.Unlock()
	}
	if s.ShardReplicaNum != s.statshouse.heartBeatReplicaNum {
		return
	}
	if s.statshouse.heartBeatEventType != format.TagValueIDHeartbeatEventHeartbeat { // first run
		s.addBuiltInsHeartbeatsLocked(resolutionShard, nowUnix, 1) // send start event immediately
		s.statshouse.heartBeatEventType = format.TagValueIDHeartbeatEventHeartbeat
	}
	// this logic with currentJournalHashSeconds and currentJournalVersion ensures there is exactly 60 samples per minute,
	// sending is once per minute when no changes, but immediate sending of journal version each second when it changed
	// standard metrics do not allow this, but heartbeats are magic)
	writeJournalVersion := func(version int64, hash string, count float64) {
		key := s.statshouse.AggKey(resolutionShard.Time, format.BuiltinMetricIDJournalVersions, [16]int32{0, s.statshouse.componentTag, 0, 0, 0, int32(version)})
		mi := data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, key, s.config.StringTopCapacity, nil)
		mi.MapStringTop(hash, count).AddCounterHost(count, 0)
	}
	if s.statshouse.metricStorage != nil { // nil only on ingress proxy for now
		metricJournalVersion := s.statshouse.metricStorage.Version()
		metricJournalHash := s.statshouse.metricStorage.StateHash()
		if metricJournalHash != s.currentJournalHash {
			if s.currentJournalHashSeconds != 0 {
				writeJournalVersion(s.currentJournalVersion, s.currentJournalHash, s.currentJournalHashSeconds)
				s.currentJournalHashSeconds = 0
			}
			s.currentJournalVersion = metricJournalVersion
			s.currentJournalHash = metricJournalHash
			writeJournalVersion(s.currentJournalVersion, s.currentJournalHash, 1)
		} else {
			s.currentJournalHashSeconds++
		}
	}

	resolutionShard = s.CurrentBuckets[60][s.statshouse.heartBeatSecondBucket]

	prevRUsage := s.statshouse.rUsage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &s.statshouse.rUsage)
	userTime := float64(s.statshouse.rUsage.Utime.Nano()-prevRUsage.Utime.Nano()) / float64(time.Second)
	sysTime := float64(s.statshouse.rUsage.Stime.Nano()-prevRUsage.Stime.Nano()) / float64(time.Second)

	key := s.statshouse.AggKey(resolutionShard.Time, format.BuiltinMetricIDUsageCPU, [16]int32{0, s.statshouse.componentTag, format.TagValueIDCPUUsageUser})
	mi := data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, key, s.config.StringTopCapacity, nil)
	mi.Tail.AddValueCounterHost(userTime, 1, 0)

	key = s.statshouse.AggKey(resolutionShard.Time, format.BuiltinMetricIDUsageCPU, [16]int32{0, s.statshouse.componentTag, format.TagValueIDCPUUsageSys})
	mi = data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, key, s.config.StringTopCapacity, nil)
	mi.Tail.AddValueCounterHost(sysTime, 1, 0)

	if nowUnix%60 != 0 {
		// IF we sample once per minute, we do it right before sending to reduce latency
		return
	}
	if s.currentJournalHashSeconds != 0 {
		writeJournalVersion(s.currentJournalVersion, s.currentJournalHash, s.currentJournalHashSeconds)
		s.currentJournalHashSeconds = 0
	}

	var rss float64
	if st, _ := srvfunc.GetMemStat(0); st != nil {
		rss = float64(st.Res)
	}

	key = s.statshouse.AggKey(resolutionShard.Time, format.BuiltinMetricIDUsageMemory, [16]int32{0, s.statshouse.componentTag})
	mi = data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, key, s.config.StringTopCapacity, nil)
	mi.Tail.AddValueCounterHost(rss, 60, 0)

	s.addBuiltInsHeartbeatsLocked(resolutionShard, nowUnix, 60) // heartbeat once per minute
}

func (s *Shard) addBuiltInsHeartbeatsLocked(resolutionShard *data_model.MetricsBucket, nowUnix uint32, count float64) {
	uptimeSec := float64(nowUnix - s.statshouse.startTimestamp)

	key := s.statshouse.AggKey(resolutionShard.Time, format.BuiltinMetricIDHeartbeatVersion, [16]int32{0, s.statshouse.componentTag, s.statshouse.heartBeatEventType})
	mi := data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, key, s.config.StringTopCapacity, nil)
	mi.MapStringTop(build.Commit(), count).AddValueCounterHost(uptimeSec, count, 0)

	heartbitIDs := []int32{format.BuiltinMetricIDHeartbeatArgs, format.BuiltinMetricIDHeartbeatArgs2, format.BuiltinMetricIDHeartbeatArgs3, format.BuiltinMetricIDHeartbeatArgs4}
	// if command line is short, we send format.BuiltinMetricIDHeartbeatArgs only
	for i, args := range s.statshouse.args {
		if i >= len(heartbitIDs) {
			break
		}
		key = s.statshouse.AggKey(resolutionShard.Time, heartbitIDs[i], [16]int32{0, s.statshouse.componentTag, s.statshouse.heartBeatEventType})
		mi = data_model.MapKeyItemMultiItem(&resolutionShard.MultiItems, key, s.config.StringTopCapacity, nil)
		mi.MapStringTopBytes(args, count).AddValueCounterHost(uptimeSec, count, 0)
	}
}

func (s *Shard) fillProxyHeader(fieldsMask *uint32, header *tlstatshouse.CommonProxyHeader) {
	*header = tlstatshouse.CommonProxyHeader{
		ShardReplica:      int32(s.ShardReplicaNum),
		ShardReplicaTotal: int32(s.statshouse.NumShardReplicas()),
		HostName:          string(s.statshouse.hostName),
		ComponentTag:      s.statshouse.componentTag,
		BuildArch:         s.statshouse.buildArchTag,
	}
	header.SetAgentEnvStaging(s.statshouse.isEnvStaging, fieldsMask)
}

func (s *Shard) fillProxyHeaderBytes(fieldsMask *uint32, header *tlstatshouse.CommonProxyHeaderBytes) {
	*header = tlstatshouse.CommonProxyHeaderBytes{
		ShardReplica:      int32(s.ShardReplicaNum),
		ShardReplicaTotal: int32(s.statshouse.NumShardReplicas()),
		HostName:          s.statshouse.hostName,
		ComponentTag:      s.statshouse.componentTag,
		BuildArch:         s.statshouse.buildArchTag,
	}
	header.SetAgentEnvStaging(s.statshouse.isEnvStaging, fieldsMask)
}
