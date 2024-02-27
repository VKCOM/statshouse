// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package agent

import (
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/vkgo/build"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"

	"go.uber.org/atomic"
	"pgregory.net/rand"
)

// Agent gets stat, hashes, estimates cardinality and immediately shards result into ShardReplicas
// No values in this struct are ever changed after initialization, so no locking

type Agent struct {
	historicBucketsDataSize atomic.Int64 // sum of values for each shard

	ShardReplicas   []*ShardReplica              // 3 * number of shards
	NumShards       int                          // so len(ShardReplicas) / 3
	GetConfigResult tlstatshouse.GetConfigResult // for ingress proxy

	diskCache *DiskBucketStorage
	hostName  []byte
	argsHash  int32
	argsLen   int32
	args      string
	config    Config
	logF      rpc.LoggerFunc

	statshouseRemoteConfigString string       // optimization
	skipShards                   atomic.Int32 // copy from config.

	rUsage                syscall.Rusage // accessed without lock by first shard addBuiltIns
	heartBeatEventType    int32          // first time "start", then "heartbeat"
	heartBeatSecondBucket int            // random [0..59] bucket for per minute heartbeat to spread load on aggregator
	heartBeatReplicaNum   int            // random [0..2] first shard replica for per minute heartbeat to spread load on aggregator
	startTimestamp        uint32

	metricStorage format.MetaStorageInterface

	componentTag    int32 // agent or ingress proxy or aggregator (they have agents for built-in metrics)
	isEnvStaging    bool
	commitDateTag   int32
	commitTimestamp int32
	buildArchTag    int32
	// Used for builtin metrics when running inside aggregator
	AggregatorShardKey   int32
	AggregatorReplicaKey int32
	AggregatorHost       int32

	beforeFlushBucketFunc func(s *Agent, now time.Time) // used by aggregator to add built-in metrics

	statErrorsDiskWrite             *BuiltInItemValue
	statErrorsDiskRead              *BuiltInItemValue
	statErrorsDiskErase             *BuiltInItemValue
	statErrorsDiskReadNotConfigured *BuiltInItemValue
	statErrorsDiskCompressFailed    *BuiltInItemValue
	statLongWindowOverflow          *BuiltInItemValue
	statDiskOverflow                *BuiltInItemValue

	mu                          sync.Mutex
	loadPromTargetsShardReplica *ShardReplica
}

func SpareShardReplica(shardReplica int, timestamp uint32) int {
	shard := shardReplica / 3
	replica := shardReplica % 3
	if timestamp%2 == 0 {
		return shard*3 + (replica+1)%3
	}
	return shard*3 + (replica+2)%3
}

// All shard aggregators must be on the same network
func MakeAgent(network string, storageDir string, aesPwd string, config Config, hostName string, componentTag int32, metricStorage format.MetaStorageInterface, logF func(format string, args ...interface{}),
	beforeFlushBucketFunc func(s *Agent, now time.Time), getConfigResult *tlstatshouse.GetConfigResult) (*Agent, error) {
	rpcClient := rpc.NewClient(rpc.ClientWithCryptoKey(aesPwd), rpc.ClientWithTrustedSubnetGroups(build.TrustedSubnetGroups()), rpc.ClientWithLogf(logF))
	rnd := rand.New()
	allArgs := strings.Join(os.Args[1:], " ")
	argsHash := sha1.Sum([]byte(allArgs))

	result := &Agent{
		hostName:              format.ForceValidStringValue(hostName), // worse alternative is do not run at all
		componentTag:          componentTag,
		heartBeatEventType:    format.TagValueIDHeartbeatEventStart,
		heartBeatSecondBucket: rnd.Intn(60),
		heartBeatReplicaNum:   rnd.Intn(3),
		config:                config,
		argsHash:              int32(binary.BigEndian.Uint32(argsHash[:])),
		argsLen:               int32(len(allArgs)),
		args:                  string(format.ForceValidStringValue(allArgs)), // if single arg is too big, it is truncated here
		logF:                  logF,
		commitDateTag:         format.ISO8601Date2BuildDateKey(time.Unix(int64(build.CommitTimestamp()), 0).Format(time.RFC3339)),
		commitTimestamp:       int32(build.CommitTimestamp()),
		buildArchTag:          format.GetBuildArchKey(runtime.GOARCH),
		metricStorage:         metricStorage,
		beforeFlushBucketFunc: beforeFlushBucketFunc,
	}
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &result.rUsage)

	switch config.StatsHouseEnv {
	case "production":
		result.isEnvStaging = false
	case "staging":
		result.isEnvStaging = true
	default:
		// Our built-in metrics are supposed to work without mapping, so all keys must be known in advance
		// Also we protect built-in metrics from sampling, so must ensure their cardinality is limited
		return nil, fmt.Errorf("configuration error: --statshouse-env (%q) should be either 'production' or 'staging' ", config.StatsHouseEnv)
	}
	logF("Configuration: detected build arch key as %d for string %q", result.buildArchTag, runtime.GOARCH)
	if getConfigResult != nil {
		result.GetConfigResult = *getConfigResult // Inside aggregator
	} else {
		if len(config.AggregatorAddresses) < 3 {
			return nil, fmt.Errorf("configuration Error: must have 3 aggregator addresses for configuration redundancy")
		}
		result.GetConfigResult = GetConfig(network, rpcClient, config.AggregatorAddresses, result.isEnvStaging, result.componentTag, result.buildArchTag, config.Cluster, logF)
	}
	config.AggregatorAddresses = result.GetConfigResult.Addresses[:result.GetConfigResult.MaxAddressesCount] // agents simply ignore excess addresses
	result.NumShards = len(config.AggregatorAddresses) / 3
	nowUnix := uint32(time.Now().Unix())
	result.startTimestamp = nowUnix
	if storageDir != "" {
		dc, err := MakeDiskBucketStorage(storageDir, len(config.AggregatorAddresses), logF)
		if err != nil {
			return nil, err
		}
		result.diskCache = dc
	}
	commonSpread := time.Duration(rnd.Int63n(int64(time.Second) / int64(len(config.AggregatorAddresses))))
	for i, a := range config.AggregatorAddresses {
		shard := &ShardReplica{
			config:                           config,
			hardwareMetricResolutionResolved: atomic.NewInt32(int32(config.HardwareMetricResolution)),
			agent:                            result,
			ShardReplicaNum:                  i,
			ShardKey:                         int32(i/3) + 1,
			ReplicaKey:                       int32(i%3) + 1,
			timeSpreadDelta:                  commonSpread + time.Second*time.Duration(i)/time.Duration(len(config.AggregatorAddresses)),
			CurrentTime:                      nowUnix,
			FutureQueue:                      make([][]*data_model.MetricsBucket, 60),
			BucketsToSend:                    make(chan compressedBucketDataOnDisk),
			client: tlstatshouse.Client{
				Client:  rpcClient,
				Network: network,
				Address: a,
				ActorID: 0,
			},
			perm:  rnd.Perm(data_model.AggregationShardsPerSecond),
			stats: &shardStat{shardReplicaNum: strconv.FormatInt(int64(i), 10)},
		}
		shard.CurrentBuckets = make([][]*data_model.MetricsBucket, 61)
		for r := range shard.CurrentBuckets {
			if r != format.AllowedResolution(r) {
				continue
			}
			bucketTime := (nowUnix / uint32(r)) * uint32(r)
			for sh := 0; sh < r; sh++ {
				shard.CurrentBuckets[r] = append(shard.CurrentBuckets[r], &data_model.MetricsBucket{Time: bucketTime})
			}
		}
		shard.alive.Store(true)
		shard.cond = sync.NewCond(&shard.mu)
		shard.condPreprocess = sync.NewCond(&shard.mu)
		result.ShardReplicas = append(result.ShardReplicas, shard)

		// If we write seconds to disk when goSendRecent() receives error, seconds will end up being slightly not in order
		// We correct for this by looking forward in the disk cache
		for j := 0; j < data_model.MaxConveyorDelay*2; j++ {
			shard.readHistoricSecondLocked() // not actually locked here, but we have exclusive access
		}
	}
	// TODO - remove those, simply write metrics to bucket as usual
	result.statErrorsDiskWrite = result.CreateBuiltInItemValue(data_model.Key{Metric: format.BuiltinMetricIDAgentDiskCacheErrors, Keys: [16]int32{0, format.TagValueIDDiskCacheErrorWrite}})
	result.statErrorsDiskRead = result.CreateBuiltInItemValue(data_model.Key{Metric: format.BuiltinMetricIDAgentDiskCacheErrors, Keys: [16]int32{0, format.TagValueIDDiskCacheErrorRead}})
	result.statErrorsDiskErase = result.CreateBuiltInItemValue(data_model.Key{Metric: format.BuiltinMetricIDAgentDiskCacheErrors, Keys: [16]int32{0, format.TagValueIDDiskCacheErrorDelete}})
	result.statErrorsDiskReadNotConfigured = result.CreateBuiltInItemValue(data_model.Key{Metric: format.BuiltinMetricIDAgentDiskCacheErrors, Keys: [16]int32{0, format.TagValueIDDiskCacheErrorReadNotConfigured}})
	result.statErrorsDiskCompressFailed = result.CreateBuiltInItemValue(data_model.Key{Metric: format.BuiltinMetricIDAgentDiskCacheErrors, Keys: [16]int32{0, format.TagValueIDDiskCacheErrorCompressFailed}})
	result.statLongWindowOverflow = result.CreateBuiltInItemValue(data_model.Key{Metric: format.BuiltinMetricIDTimingErrors, Keys: [16]int32{0, format.TagValueIDTimingLongWindowThrownAgent}})
	result.statDiskOverflow = result.CreateBuiltInItemValue(data_model.Key{Metric: format.BuiltinMetricIDTimingErrors, Keys: [16]int32{0, format.TagValueIDTimingLongWindowThrownAgent}})

	result.updateConfigRemotelyExperimental() // first update from stored in sqlite
	return result, nil
}

// separated so we can set AggregatorHost, which is dependent on tagMapper which uses agent to write statistics
func (s *Agent) Run(aggHost int32, aggShardKey int32, aggReplicaKey int32) {
	s.AggregatorHost = aggHost
	s.AggregatorShardKey = aggShardKey
	s.AggregatorReplicaKey = aggReplicaKey
	for _, shard := range s.ShardReplicas {
		shard.InitBuiltInMetric()

		go shard.goPreProcess()
		for j := 0; j < data_model.MaxConveyorDelay; j++ {
			go shard.goSendRecent()
		}
		for j := 0; j < data_model.MaxHistorySendStreams; j++ {
			go shard.goSendHistoric()
		}
		if shard.client.Address != "" {
			go shard.goLiveChecker()
		}
		go shard.goEraseHistoric()
		go shard.goTestConnectionLoop()
	}
	go s.goFlusher()
}

func (s *Agent) Close() {
	// Big TODO - quit all goroutines, including those sending historic data
}

func (s *Agent) NumShardReplicas() int {
	return len(s.ShardReplicas)
}

// if first one is nil, second one is also nil
func (s *Agent) getRandomLiveShardReplicas() (*ShardReplica, *ShardReplica) {
	var liveShardReplicas []*ShardReplica // TODO - do not alloc
	for _, shardReplica := range s.ShardReplicas {
		if shardReplica.alive.Load() {
			liveShardReplicas = append(liveShardReplicas, shardReplica)
		}
	}
	if len(liveShardReplicas) == 0 {
		return nil, nil
	}
	if len(liveShardReplicas) == 1 {
		return liveShardReplicas[0], nil
	}
	i := rand.Intn(len(liveShardReplicas))
	j := rand.Intn(len(liveShardReplicas) - 1)
	if j == i {
		j++
	}
	return liveShardReplicas[i], liveShardReplicas[j]
}

func (s *Agent) updateConfigRemotelyExperimental() {
	if s.config.DisableRemoteConfig {
		return
	}
	if s.metricStorage == nil { // nil only on ingress proxy for now
		return
	}
	// We'll make this metric invisible for now to avoid being edited by anybody
	description := ""
	if mv := s.metricStorage.GetMetaMetricByName(data_model.StatshouseAgentRemoteConfigMetric); mv != nil {
		description = mv.Description
	}
	if description == s.statshouseRemoteConfigString {
		// Optimization. Also, if we have some race with accessing config, we do not want to risk it every second
		return
	}
	s.statshouseRemoteConfigString = description
	s.logF("Remote config:\n%s", description)
	config := s.config
	if err := config.updateFromRemoteDescription(description); err != nil {
		s.logF("Remote config: error updating config from metric %q: %v", data_model.StatshouseAgentRemoteConfigMetric, err)
		return
	}
	s.logF("Remote config: updated config from metric %q", data_model.StatshouseAgentRemoteConfigMetric)
	if config.SkipShards < s.NumShards {
		s.skipShards.Store(int32(config.SkipShards))
	} else {
		s.skipShards.Store(0)
	}
	for _, shardReplica := range s.ShardReplicas {
		shardReplica.mu.Lock()
		shardReplica.config = config
		shardReplica.hardwareMetricResolutionResolved.Store(int32(config.HardwareMetricResolution))
		shardReplica.mu.Unlock()
	}
}

func (s *Agent) goFlusher() {
	now := time.Now()
	for { // TODO - quit
		tick := time.After(data_model.TillStartOfNextSecond(now))
		now = <-tick // We synchronize with calendar second boundary
		if s.beforeFlushBucketFunc != nil {
			s.beforeFlushBucketFunc(s, now)
		}
		for _, shardReplica := range s.ShardReplicas {
			shardReplica.flushBuckets(now)
		}
		s.updateConfigRemotelyExperimental()
	}
}

// For counters, use AddValueCounter(0, 1)
func (s *BuiltInItemValue) AddValueCounter(value float64, count float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.value.AddValueCounter(value, count)
}

func (s *BuiltInItemValue) SetValueCounter(value float64, count float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.value = data_model.ItemValue{}
	s.value.AddValueCounter(value, count)
}

func (s *BuiltInItemValue) Merge(s2 *data_model.ItemValue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.value.Merge(s2)
}

func (s *Agent) shardNumFromHash(hash uint64) int {
	numShardReplicas := s.NumShardReplicas()
	skipShardReplicas := 3 * int(s.skipShards.Load())                  // free on x86
	if skipShardReplicas > 0 && skipShardReplicas < numShardReplicas { // second condition checked during setting skipShards, but cheap enough
		mul := (hash >> 32) * uint64(numShardReplicas-skipShardReplicas) >> 32 // trunc([0..0.9999999] * numShardReplicas) in fixed point 32.32
		return skipShardReplicas + int(mul)
	}
	mul := (hash >> 32) * uint64(numShardReplicas) >> 32 // trunc([0..0.9999999] * numShardReplicas) in fixed point 32.32
	return int(mul)
}

func (s *Agent) shardReplicaFromHash(hash uint64) *ShardReplica {
	return s.ShardReplicas[s.shardNumFromHash(hash)]
}

// Do not create too many. ShardReplicas will iterate through values before flushing bucket
// Useful for watermark metrics.
func (s *Agent) CreateBuiltInItemValue(key data_model.Key) *BuiltInItemValue {
	keyHash := key.Hash()
	shardReplica := s.shardReplicaFromHash(keyHash)
	return shardReplica.CreateBuiltInItemValue(key)
}

func (s *Agent) ApplyMetric(m tlstatshouse.MetricBytes, h data_model.MappedMetricHeader, ingestionStatusOKTag int32) {
	// Simply writing everything we know about metric ingestion errors would easily double how much metrics data we write
	// So below is basically a compromise. All info is stored in MappingMetricHeader, if needed we can easily write more
	// by changing code below
	if h.IngestionStatus != 0 {
		// h.InvalidString was validated before mapping attempt.
		// In case of utf decoding error, it contains hex representation of original string
		s.AddCounterHostStringBytes(data_model.Key{
			Metric: format.BuiltinMetricIDIngestionStatus,
			Keys:   [format.MaxTags]int32{h.Key.Keys[0], h.Key.Metric, h.IngestionStatus, h.IngestionTagKey},
		}, h.InvalidString, 1, 0, nil)
		return
	}
	// now set ok status
	s.AddCounter(data_model.Key{
		Metric: format.BuiltinMetricIDIngestionStatus,
		Keys:   [format.MaxTags]int32{h.Key.Keys[0], h.Key.Metric, ingestionStatusOKTag, h.IngestionTagKey},
	}, 1)
	// now set all warnings
	if h.NotFoundTagName != nil { // this is correct, can be set, but empty
		// NotFoundTagName is validated when discovered
		// This is warning, so written independent of ingestion status
		s.AddCounterHostStringBytes(data_model.Key{
			Metric: format.BuiltinMetricIDIngestionStatus,
			Keys:   [format.MaxTags]int32{h.Key.Keys[0], h.Key.Metric, format.TagValueIDSrcIngestionStatusWarnMapTagNameNotFound}, // tag ID not known
		}, h.NotFoundTagName, 1, 0, nil)
	}
	if h.TagSetTwiceKey != 0 {
		s.AddCounter(data_model.Key{
			Metric: format.BuiltinMetricIDIngestionStatus,
			Keys:   [format.MaxTags]int32{h.Key.Keys[0], h.Key.Metric, format.TagValueIDSrcIngestionStatusWarnMapTagSetTwice, h.TagSetTwiceKey},
		}, 1)
	}
	if h.InvalidRawTagKey != 0 {
		s.AddCounterHostStringBytes(data_model.Key{
			Metric: format.BuiltinMetricIDIngestionStatus,
			Keys:   [format.MaxTags]int32{h.Key.Keys[0], h.Key.Metric, format.TagValueIDSrcIngestionStatusWarnMapInvalidRawTagValue, h.InvalidRawTagKey},
		}, h.InvalidRawValue, 1, 0, nil)
	}
	if h.LegacyCanonicalTagKey != 0 {
		s.AddCounter(data_model.Key{
			Metric: format.BuiltinMetricIDIngestionStatus,
			Keys:   [format.MaxTags]int32{h.Key.Keys[0], h.Key.Metric, format.TagValueIDSrcIngestionStatusWarnDeprecatedKeyName, h.LegacyCanonicalTagKey},
		}, 1)
	}

	// We do not check fields mask here, only fields values
	if m.Ts != 0 { // sending 0 instead of manipulating field mask is more convenient for many clients
		h.Key.Timestamp = m.Ts
	}
	// Ignore metric kind and use all the info we have: this way people can continue
	// using single metric as a "namespace" for "sub-metrics" of different kinds.
	// The only thing we check is if percentiles are allowed. This is configured per metric.

	// here m.Unique and m.Value cannot be both non-empty
	// also m.Counter is >= 0
	//
	// if arrays are empty, this is simple counter event
	// if m.Counter is 0, interpretation of arrays is trivial
	//
	// if both m.Values and m.Counter are set, counter is treated as true number of events,
	// while Values are treated as a subsample of all real values
	// so if counter is 20, and values len is 10, each value wil be recorded with weight 2
	//
	// if both m.Unique and m.Counter are set, counter is treated as true number of events,
	// while Uniques are treated as a set of real values
	// so if counter is 20, and Uniques len is 3, we simply add each of 3 events to HLL
	// with a twist, that we also store min/max/sum/sumsquare of unique values converted to float64
	// for the purpose of this, Uniques are treated exactly as Values
	keyHash := h.Key.Hash()
	shordReplicaNum := s.shardNumFromHash(keyHash)
	shardReplica := s.ShardReplicas[shordReplicaNum]
	// m.Counter is >= 0 here, otherwise IngestionStatus is not OK, and we returned above
	if len(m.Unique) != 0 {
		numShards := s.NumShards
		if h.MetricInfo != nil && h.MetricInfo.ShardUniqueValues && numShards > 1 {
			// we want unique value sets to have no intersections
			// so we first shard by unique value, then shard among 3 replicas by keys
			skipShards := int(s.skipShards.Load())
			notSkippedShards := numShards
			if skipShards > 0 && skipShards < numShards { // second condition checked during setting skipShards, but cheap enough
				notSkippedShards = numShards - skipShards
			}
			mul := int((keyHash >> 32) * 3 >> 32) // trunc([0..0.9999999] * 3) in fixed point 32.32

			if len(m.Unique) == 1 { // very common case, optimize
				uniqueShard := int(m.Unique[0] % int64(notSkippedShards))
				shordReplicaNum2 := (skipShards+uniqueShard)*3 + mul
				shard2 := s.ShardReplicas[shordReplicaNum2]
				shard2.ApplyUnique(h.Key, keyHash, h.SValue, m.Unique, m.Counter, h.HostTag, h.MetricInfo)
				return
			}
			uniqueValuesCache := shardReplica.getUniqueValuesCache(notSkippedShards) // TOO - better reuse without lock?
			defer shardReplica.putUniqueValuesCache(uniqueValuesCache)
			for _, v := range m.Unique {
				uniqueShard := v % int64(notSkippedShards)
				uniqueValuesCache[uniqueShard] = append(uniqueValuesCache[uniqueShard], v)
			}
			for uniqueShard, vv := range uniqueValuesCache {
				if len(vv) == 0 {
					continue
				}
				shordReplicaNum2 := (skipShards+uniqueShard)*3 + mul
				shard2 := s.ShardReplicas[shordReplicaNum2]
				shard2.ApplyUnique(h.Key, keyHash, h.SValue, vv, m.Counter*float64(len(vv))/float64(len(m.Unique)), h.HostTag, h.MetricInfo)
			}
			return
		}
		shardReplica.ApplyUnique(h.Key, keyHash, h.SValue, m.Unique, m.Counter, h.HostTag, h.MetricInfo)
		return
	}
	if len(m.Value) != 0 {
		shardReplica.ApplyValues(h.Key, keyHash, h.SValue, m.Value, m.Counter, h.HostTag, h.MetricInfo)
		return
	}
	if m.Counter > 0 {
		shardReplica.ApplyCounter(h.Key, keyHash, h.SValue, m.Counter, h.HostTag, h.MetricInfo)
	}
}

// count should be > 0 and not NaN
func (s *Agent) AddCounter(key data_model.Key, count float64) {
	s.AddCounterHost(key, count, 0, nil)
}

func (s *Agent) AddCounterHost(key data_model.Key, count float64, hostTag int32, metricInfo *format.MetricMetaValue) {
	if count <= 0 {
		return
	}
	keyHash := key.Hash()
	shardReplica := s.shardReplicaFromHash(keyHash)
	shardReplica.AddCounterHost(key, keyHash, count, hostTag, metricInfo)
}

// str should be reasonably short. Empty string will be undistinguishable from "the rest"
// count should be > 0 and not NaN
func (s *Agent) AddCounterHostStringBytes(key data_model.Key, str []byte, count float64, hostTag int32, metricInfo *format.MetricMetaValue) {
	if count <= 0 {
		return
	}
	keyHash := key.Hash()
	shardReplica := s.shardReplicaFromHash(keyHash)
	shardReplica.AddCounterHostStringBytes(key, keyHash, str, count, hostTag, metricInfo)
}

func (s *Agent) AddValueCounterHost(key data_model.Key, value float64, counter float64, hostTag int32) {
	if counter <= 0 {
		return
	}
	keyHash := key.Hash()
	shardReplica := s.shardReplicaFromHash(keyHash)
	shardReplica.AddValueCounterHost(key, keyHash, value, counter, hostTag, nil)
}

// value should be not NaN.
func (s *Agent) AddValueCounter(key data_model.Key, value float64, counter float64, metricInfo *format.MetricMetaValue) {
	if counter <= 0 {
		return
	}
	keyHash := key.Hash()
	shardReplica := s.shardReplicaFromHash(keyHash)
	shardReplica.AddValueCounterHost(key, keyHash, value, counter, 0, metricInfo)
}

/*
func (s *Agent) AddValueCounterHostArray(key data_model.Key, values []float64, mult float64, hostTag int32, metricInfo *format.MetricMetaValue) {
	if len(values) == 0 || mult < 0 {
		return
	}
	keyHash := key.Hash()
	shard := s.shardReplicaFromHash(keyHash)
	shard.AddValueArrayCounterHost(key, keyHash, values, mult, hostTag, metricInfo)
}
*/

func (s *Agent) AddValueArrayCounterHostStringBytes(key data_model.Key, values []float64, mult float64, hostTag int32, str []byte, metricInfo *format.MetricMetaValue) {
	if len(values) == 0 || mult < 0 {
		return
	}
	keyHash := key.Hash()
	shardReplica := s.shardReplicaFromHash(keyHash)
	shardReplica.AddValueArrayCounterHostStringBytes(key, keyHash, values, mult, hostTag, str, metricInfo)
}

func (s *Agent) AddValueCounterHostStringBytes(key data_model.Key, value float64, counter float64, hostTag int32, str []byte) {
	if counter <= 0 {
		return
	}
	keyHash := key.Hash()
	shardReplica := s.shardReplicaFromHash(keyHash)
	shardReplica.AddValueCounterHostStringBytes(key, keyHash, value, counter, hostTag, str, nil)
}

func (s *Agent) MergeItemValue(key data_model.Key, item *data_model.ItemValue, metricInfo *format.MetricMetaValue) {
	if item.Counter <= 0 {
		return
	}
	keyHash := key.Hash()
	shardReplica := s.shardReplicaFromHash(keyHash)
	shardReplica.MergeItemValue(key, keyHash, item, metricInfo)
}

func (s *Agent) AddUniqueHostStringBytes(key data_model.Key, hostTag int32, str []byte, hashes []int64, count float64, metricInfo *format.MetricMetaValue) {
	if len(hashes) == 0 || count < 0 {
		return
	}
	keyHash := key.Hash()
	shardReplica := s.shardReplicaFromHash(keyHash)
	shardReplica.AddUniqueHostStringBytes(key, hostTag, str, keyHash, hashes, count, metricInfo)
}

func (s *Agent) AggKey(time uint32, metricID int32, keys [format.MaxTags]int32) data_model.Key {
	return data_model.AggKey(time, metricID, keys, s.AggregatorHost, s.AggregatorShardKey, s.AggregatorReplicaKey)
}

func (s *Agent) HistoricBucketsDataSizeMemorySum() int64 {
	return s.historicBucketsDataSize.Load()
}

func (s *Agent) HistoricBucketsDataSizeDiskSum() (total int64, unsent int64) {
	if s.diskCache == nil {
		return 0, 0
	}
	for i := range s.ShardReplicas {
		t, u := s.diskCache.TotalFileSize(i)
		total += t
		unsent += u
	}
	return total, unsent
}
