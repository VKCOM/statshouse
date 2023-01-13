// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package agent

import (
	"fmt"
	"os"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/vkcom/statshouse/internal/vkgo/build"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"

	"pgregory.net/rand"
)

// Agent gets stat, hashes, estimates cardinality and immediately shards result into Shards
// No values in this struct are ever changed after initialization, so no locking

type Agent struct {
	Shards          []*Shard                     // Actually those are shard-replicas
	GetConfigResult tlstatshouse.GetConfigResult // for ingress proxy

	diskCache *DiskBucketStorage
	hostName  []byte
	args      [][]byte // split into ValidString chunks
	config    Config

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

	beforeFlushBucketFunc func(now time.Time) // used by aggregator to add built-in metrics

	statErrorsDiskWrite             *BuiltInItemValue
	statErrorsDiskRead              *BuiltInItemValue
	statErrorsDiskErase             *BuiltInItemValue
	statErrorsDiskReadNotConfigured *BuiltInItemValue
	statErrorsDiskCompressFailed    *BuiltInItemValue
	statLongWindowOverflow          *BuiltInItemValue

	mu                   sync.Mutex
	loadPromTargetsShard *Shard
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
	beforeFlushBucketFunc func(now time.Time), getConfigResult *tlstatshouse.GetConfigResult) (*Agent, error) {
	rpcClient := &rpc.Client{ // single socket per aggregator
		CryptoKey:           aesPwd,
		TrustedSubnetGroups: build.TrustedSubnetGroups(),
		Logf:                logF,
		PongTimeout:         data_model.ClientRPCPongTimeout,
	}
	rnd := rand.New()
	result := &Agent{
		hostName:              format.ForceValidStringValue(hostName), // worse alternative is do not run at all
		componentTag:          componentTag,
		heartBeatEventType:    format.TagValueIDHeartbeatEventStart,
		heartBeatSecondBucket: rnd.Intn(60),
		heartBeatReplicaNum:   rnd.Intn(3),
		config:                config,
		commitDateTag:         format.ISO8601Date2BuildDateKey(time.Unix(int64(build.CommitTimestamp()), 0).Format(time.RFC3339)),
		commitTimestamp:       int32(build.CommitTimestamp()),
		buildArchTag:          format.GetBuildArchKey(runtime.GOARCH),
		metricStorage:         metricStorage,
		beforeFlushBucketFunc: beforeFlushBucketFunc,
	}
	var arg []byte
	for i := 1; i < len(os.Args); i++ {
		w := format.ForceValidStringValue(os.Args[i]) // if single arg is too big, it is truncated here
		if len(w) == 0 {
			continue // preserve single space between args property
		}
		if len(arg) != 0 && len(arg)+1+len(w) > format.MaxStringLen { // if len(arg) == 0. we don't need ' ' and w always fits
			result.args = append(result.args, arg)
			arg = nil
		}
		if len(arg) != 0 {
			arg = append(arg, ' ')
		}
		arg = append(arg, w...)
	}
	if len(arg) != 0 || len(result.args) == 0 {
		result.args = append(result.args, arg) // at least single empty value
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
		shard := &Shard{
			config:          config,
			statshouse:      result,
			ShardReplicaNum: i,
			timeSpreadDelta: commonSpread + time.Second*time.Duration(i)/time.Duration(len(config.AggregatorAddresses)),
			CurrentTime:     nowUnix,
			FutureQueue:     make([][]*data_model.MetricsBucket, 60),
			BucketsToSend:   make(chan compressedBucketData),
			client: tlstatshouse.Client{
				Client:  rpcClient,
				Network: network,
				Address: a,
				ActorID: 0,
			},
			perm: rnd.Perm(data_model.AggregationShardsPerSecond),
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
		result.Shards = append(result.Shards, shard)

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
	return result, nil
}

// separated so we can set AggregatorHost, which is dependent on tagMapper which uses agent to wirte statistics
func (s *Agent) Run(aggHost int32, aggShardKey int32, aggReplicaKey int32) {
	s.AggregatorHost = aggHost
	s.AggregatorShardKey = aggShardKey
	s.AggregatorReplicaKey = aggReplicaKey
	for _, shard := range s.Shards {
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
	}
	go s.goFlusher()
}

func (s *Agent) Close() {
	// Big TODO - quit all goroutines, including those sending historic data
}

func (s *Agent) NumShardReplicas() int {
	return len(s.Shards)
}

// if first one is nil, second one is also nil
func (s *Agent) getRandomLiveShards() (*Shard, *Shard) {
	var liveShards []*Shard
	for _, shard := range s.Shards {
		if shard.alive.Load() {
			liveShards = append(liveShards, shard)
		}
	}
	if len(liveShards) == 0 {
		return nil, nil
	}
	if len(liveShards) == 1 {
		return liveShards[0], nil
	}
	i := rand.Intn(len(liveShards))
	j := rand.Intn(len(liveShards) - 1)
	if j == i {
		j++
	}
	return liveShards[i], liveShards[j]
}

func (s *Agent) goFlusher() {
	now := time.Now()
	for { // TODO - quit
		tick := time.After(data_model.TillStartOfNextSecond(now))
		now = <-tick // We synchronize with calendar second boundary
		if s.beforeFlushBucketFunc != nil {
			s.beforeFlushBucketFunc(now)
		}
		for _, shard := range s.Shards {
			shard.flushBuckets(now)
		}
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
	numShards := s.NumShardReplicas()
	skipShards := s.config.SkipFirstNShards
	if skipShards > 0 && skipShards < numShards {
		mul := (hash >> 32) * uint64(numShards-skipShards) >> 32 // trunc([0..0.9999999] * numShards) in fixed point 32.32
		return skipShards + int(mul)
	}
	mul := (hash >> 32) * uint64(numShards) >> 32 // trunc([0..0.9999999] * numShards) in fixed point 32.32
	return int(mul)
}

func (s *Agent) shardFromHash(hash uint64) *Shard {
	return s.Shards[s.shardNumFromHash(hash)]
}

// Do not create too many. Shards will iterate through values before flushing bucket
// Useful for watermark metrics.
func (s *Agent) CreateBuiltInItemValue(key data_model.Key) *BuiltInItemValue {
	keyHash := key.Hash()
	shard := s.shardFromHash(keyHash)
	return shard.CreateBuiltInItemValue(key)
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
	if h.LegacyCanonicalTagKey != 0 {
		s.AddCounter(data_model.Key{
			Metric: format.BuiltinMetricIDIngestionStatus,
			Keys:   [format.MaxTags]int32{h.Key.Keys[0], h.Key.Metric, format.TagValueIDSrcIngestionStatusWarnDeprecatedKeyName, h.LegacyCanonicalTagKey},
		}, 1)
	}
	if !m.IsSetNewCounterSemantic() && len(m.Value) != 0 && m.Counter > 0 {
		s.AddCounter(data_model.Key{
			Metric: format.BuiltinMetricIDIngestionStatus,
			Keys:   [format.MaxTags]int32{h.Key.Keys[0], h.Key.Metric, format.TagValueIDSrcIngestionStatusWarnOldCounterSemantic},
		}, 1)
	}
	if m.IsSetT() || m.T != 0 {
		key := data_model.Key{
			Metric: format.BuiltinMetricIDIngestionStatus,
			Keys:   [16]int32{0, h.Key.Metric, format.TagValueIDSrcIngestionStatusWarnDeprecatedT}}
		s.AddCounter(key, 1)
	}
	if m.IsSetStop() || len(m.Stop) != 0 {
		key := data_model.Key{
			Metric: format.BuiltinMetricIDIngestionStatus,
			Keys:   [16]int32{0, h.Key.Metric, format.TagValueIDSrcIngestionStatusWarnDeprecatedStop}}
		s.AddCounter(key, 1)
	}

	// We do not check fields mask here, only fields values
	if m.T != 0 { // sending 0 instead of manipulating field mask is more convenient for many clients
		h.Key.Timestamp = uint32(m.T / 1_000_000_000)
	}
	if m.Ts != 0 {
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
	shard := s.shardFromHash(keyHash)
	if len(m.Unique) != 0 {
		shard.ApplyUnique(h.Key, keyHash, h.SValue, m.Unique, m.Counter, h.HostTag, h.MetricInfo)
		return
	}
	if len(m.Value) != 0 {
		shard.ApplyValues(h.Key, keyHash, h.SValue, m.Value, m.Counter, h.HostTag, h.MetricInfo)
		return
	}
	if m.Counter > 0 {
		shard.ApplyCounter(h.Key, keyHash, h.SValue, m.Counter, h.HostTag, h.MetricInfo)
	}
}

// count should be > 0 and not NaN
func (s *Agent) AddCounter(key data_model.Key, count float64) {
	s.AddCounterHost(key, count, 0, nil)
}

func (s *Agent) AddCounterHost(key data_model.Key, count float64, hostTag int32, metricInfo *format.MetricMetaValue) {
	keyHash := key.Hash()
	shard := s.shardFromHash(keyHash)
	shard.AddCounterHost(key, keyHash, count, hostTag, metricInfo)
}

// str should be reasonably short. Empty string will be undistinguishable from "the rest"
// count should be > 0 and not NaN
func (s *Agent) AddCounterHostStringBytes(key data_model.Key, str []byte, count float64, hostTag int32, metricInfo *format.MetricMetaValue) {
	keyHash := key.Hash()
	shard := s.shardFromHash(keyHash)
	shard.AddCounterHostStringBytes(key, keyHash, str, count, hostTag, metricInfo)
}

func (s *Agent) AddValueCounterHost(key data_model.Key, value float64, counter float64, hostTag int32) {
	keyHash := key.Hash()
	shard := s.shardFromHash(keyHash)
	shard.AddValueCounterHost(key, keyHash, value, counter, hostTag, nil)
}

// value should be not NaN.
func (s *Agent) AddValueCounter(key data_model.Key, value float64, counter float64, metricInfo *format.MetricMetaValue) {
	keyHash := key.Hash()
	shard := s.shardFromHash(keyHash)
	shard.AddValueCounterHost(key, keyHash, value, counter, 0, metricInfo)
}

func (s *Agent) AddValueCounterHostArray(key data_model.Key, values []float64, mult float64, hostTag int32, metricInfo *format.MetricMetaValue) {
	keyHash := key.Hash()
	shard := s.shardFromHash(keyHash)
	shard.AddValueArrayCounterHost(key, keyHash, values, mult, hostTag, metricInfo)
}

func (s *Agent) AddValueArrayCounterHostStringBytes(key data_model.Key, values []float64, mult float64, hostTag int32, str []byte, metricInfo *format.MetricMetaValue) {
	keyHash := key.Hash()
	shard := s.shardFromHash(keyHash)
	shard.AddValueArrayCounterHostStringBytes(key, keyHash, values, mult, hostTag, str, metricInfo)
}

func (s *Agent) AddValueCounterHostStringBytes(key data_model.Key, value float64, counter float64, hostTag int32, str []byte) {
	keyHash := key.Hash()
	shard := s.shardFromHash(keyHash)
	shard.AddValueCounterHostStringBytes(key, keyHash, value, counter, hostTag, str, nil)
}

func (s *Agent) MergeItemValue(key data_model.Key, item *data_model.ItemValue, metricInfo *format.MetricMetaValue) {
	keyHash := key.Hash()
	shard := s.shardFromHash(keyHash)
	shard.MergeItemValue(key, keyHash, item, metricInfo)
}

func (s *Agent) AddUniqueHostStringBytes(key data_model.Key, hostTag int32, str []byte, hashes []int64, count float64, metricInfo *format.MetricMetaValue) {
	keyHash := key.Hash()
	shard := s.shardFromHash(keyHash)
	shard.AddUniqueHostStringBytes(key, hostTag, str, keyHash, hashes, count, metricInfo)
}

func (s *Agent) AggKey(time uint32, metricID int32, keys [format.MaxTags]int32) data_model.Key {
	return data_model.AggKey(time, metricID, keys, s.AggregatorHost, s.AggregatorShardKey, s.AggregatorReplicaKey)
}
