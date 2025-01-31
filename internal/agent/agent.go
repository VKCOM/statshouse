// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package agent

import (
	"context"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/vkcom/statshouse/internal/env"
	"github.com/vkcom/statshouse/internal/sharding"
	"github.com/vkcom/statshouse/internal/vkgo/semaphore"

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/pcache"
	"github.com/vkcom/statshouse/internal/vkgo/build"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"

	"go.uber.org/atomic"
	"pgregory.net/rand"
)

// Agent gets stat, hashes, estimates cardinality and immediately shards result into ShardReplicas
// No values in this struct are ever changed after initialization, so no locking

type Agent struct {
	historicBucketsDataSize atomic.Int64 // sum of values for each shard

	ShardReplicas   []*ShardReplica // 3 * number of shards
	Shards          []*Shard
	GetConfigResult tlstatshouse.GetConfigResult // for ingress proxy

	cancelSendsFunc   context.CancelFunc
	cancelSendsCtx    context.Context
	recentSendersSema *semaphore.Weighted
	sendersWG         sync.WaitGroup
	cancelFlushFunc   context.CancelFunc
	cancelFlushCtx    context.Context
	flusherWG         sync.WaitGroup
	preprocessWG      sync.WaitGroup

	diskBucketCache *DiskBucketStorage
	hostName        []byte
	argsHash        int32
	argsLen         int32
	args            string
	config          Config
	logF            rpc.LoggerFunc
	envLoader       *env.Loader

	statshouseRemoteConfigString string       // optimization
	skipShards                   atomic.Int32 // copy from config.
	builtinNewSharding           atomic.Bool  // copy from config.
	builtinNewConveyor           atomic.Bool  // copy from config.

	rUsage                syscall.Rusage // accessed without lock by first shard addBuiltIns
	heartBeatEventType    int32          // first time "start", then "heartbeat"
	heartBeatSecondBucket int            // random [0..59] bucket for per minute heartbeat to spread load on aggregator
	startTimestamp        uint32

	mappingsCache *pcache.MappingsCache
	metricStorage format.MetaStorageInterface

	componentTag int32 // agent or ingress proxy or aggregator (they have agents for built-in metrics)
	stagingLevel int
	buildArchTag int32
	// Used for builtin metrics when running inside aggregator
	AggregatorShardKey   int32
	AggregatorReplicaKey int32
	AggregatorHost       int32

	beforeFlushBucketFunc func(s *Agent, nowUnix uint32) // used by aggregator to add built-in metrics
	beforeFlushTime       uint32                         // changed exclusively by goFlusher

	statErrorsDiskWrite             *BuiltInItemValue
	statErrorsDiskRead              *BuiltInItemValue
	statErrorsDiskErase             *BuiltInItemValue
	statErrorsDiskReadNotConfigured *BuiltInItemValue
	statErrorsDiskCompressFailed    *BuiltInItemValue
	statLongWindowOverflow          *BuiltInItemValue
	statDiskOverflow                *BuiltInItemValue
	statMemoryOverflow              *BuiltInItemValue

	TimingsMapping      *BuiltInItemValue
	TimingsMappingSlow  *BuiltInItemValue
	TimingsApplyMetric  *BuiltInItemValue
	TimingsFlush        *BuiltInItemValue
	TimingsPreprocess   *BuiltInItemValue
	TimingsSendRecent   *BuiltInItemValue
	TimingsSendHistoric *BuiltInItemValue

	mu                          sync.Mutex
	loadPromTargetsShardReplica *ShardReplica
}

// All shard aggregators must be on the same network
func MakeAgent(network string, cacheDir string, aesPwd string, config Config, hostName string, componentTag int32, metricStorage format.MetaStorageInterface,
	mappingsCache *pcache.MappingsCache, logF func(format string, args ...interface{}),
	beforeFlushBucketFunc func(s *Agent, nowUnix uint32), getConfigResult *tlstatshouse.GetConfigResult, envLoader *env.Loader) (*Agent, error) {
	newClient := func() *rpc.Client {
		return rpc.NewClient(
			rpc.ClientWithProtocolVersion(rpc.LatestProtocolVersion),
			rpc.ClientWithCryptoKey(aesPwd),
			rpc.ClientWithTrustedSubnetGroups(build.TrustedSubnetGroups()),
			rpc.ClientWithLogf(logF))
	}
	rpcClient := newClient() // for autoconfig + first shard
	rnd := rand.New()
	allArgs := strings.Join(os.Args[1:], " ")
	argsHash := sha1.Sum([]byte(allArgs))

	cancelSendsCtx, cancelSendsFunc := context.WithCancel(context.Background())
	cancelFlushCtx, cancelFlushFunc := context.WithCancel(context.Background())

	result := &Agent{
		cancelSendsCtx:        cancelSendsCtx,
		cancelSendsFunc:       cancelSendsFunc,
		cancelFlushCtx:        cancelFlushCtx,
		cancelFlushFunc:       cancelFlushFunc,
		hostName:              format.ForceValidStringValue(hostName), // worse alternative is do not run at all
		componentTag:          componentTag,
		heartBeatEventType:    format.TagValueIDHeartbeatEventStart,
		heartBeatSecondBucket: rnd.Intn(60),
		config:                config,
		argsHash:              int32(binary.BigEndian.Uint32(argsHash[:])),
		argsLen:               int32(len(allArgs)),
		args:                  string(format.ForceValidStringValue(allArgs)), // if single arg is too big, it is truncated here
		logF:                  logF,
		buildArchTag:          format.GetBuildArchKey(runtime.GOARCH),
		mappingsCache:         mappingsCache,
		metricStorage:         metricStorage,
		beforeFlushBucketFunc: beforeFlushBucketFunc,
		envLoader:             envLoader,
	}
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &result.rUsage)

	switch config.StatsHouseEnv {
	case "production":
		result.stagingLevel = 0
	case "staging", "staging1":
		result.stagingLevel = 1
	case "staging2":
		result.stagingLevel = 2
	case "staging3":
		result.stagingLevel = 3
	default:
		// Our built-in metrics are supposed to work without mapping, so all keys must be known in advance
		// Also we protect built-in metrics from sampling, so must ensure their cardinality is limited
		return nil, fmt.Errorf("configuration error: --statshouse-env (%q) should be 'production', 'staging1', 'staging2' or 'staging3'", config.StatsHouseEnv)
	}
	logF("Configuration: detected build arch key as %d for string %q", result.buildArchTag, runtime.GOARCH)
	if getConfigResult != nil {
		result.GetConfigResult = *getConfigResult // Inside aggregator
	} else {
		if len(config.AggregatorAddresses) < 3 {
			return nil, fmt.Errorf("configuration Error: must have 3 aggregator addresses for configuration redundancy")
		}
		result.GetConfigResult = GetConfig(network, rpcClient, config.AggregatorAddresses, hostName, result.stagingLevel, result.componentTag, result.buildArchTag, config.Cluster, cacheDir, logF)
	}
	config.AggregatorAddresses = result.GetConfigResult.Addresses[:result.GetConfigResult.MaxAddressesCount] // agents simply ignore excess addresses
	now := time.Now()
	nowUnix := uint32(now.Unix())
	result.beforeFlushTime = nowUnix

	result.startTimestamp = nowUnix
	if cacheDir != "" {
		dbc, err := MakeDiskBucketStorage(cacheDir, len(config.AggregatorAddresses), logF)
		if err != nil {
			return nil, err
		}
		result.diskBucketCache = dbc
	}
	commonSpread := time.Duration(rnd.Int63n(int64(time.Second) / int64(len(config.AggregatorAddresses))))
	for i := 0; i < len(config.AggregatorAddresses)/3; i++ {
		shard := &Shard{
			config:              config,
			agent:               result,
			ShardNum:            i,
			ShardKey:            int32(i) + 1,
			timeSpreadDelta:     3*commonSpread + 3*time.Second*time.Duration(i)/time.Duration(len(config.AggregatorAddresses)),
			BucketsToSend:       make(chan compressedBucketData),
			BucketsToPreprocess: make(chan *data_model.MetricsBucket, 1), // length of preprocessor queue
			rng:                 rnd,
			CurrentTime:         nowUnix,
			SendTime:            nowUnix - 2, // accept previous seconds at the start of the agent
		}
		shard.hardwareMetricResolutionResolved.Store(int32(config.HardwareMetricResolution))
		shard.hardwareSlowMetricResolutionResolved.Store(int32(config.HardwareSlowMetricResolution))
		for j := 0; j < superQueueLen; j++ {
			shard.SuperQueue[j] = &data_model.MetricsBucket{} // timestamp will be assigned at queue flush
		}
		shard.cond = sync.NewCond(&shard.mu)
		result.Shards = append(result.Shards, shard)

		// If we write seconds to disk when goSendRecent() receives error, seconds will end up being slightly not in order
		// We correct for this by looking forward in the disk cache
		// TODO - make historic queue strict queue instead (?)
		for j := 0; j < data_model.MaxConveyorDelay*2; j++ {
			shard.readHistoricSecondLocked() // not actually locked here, but we have exclusive access
		}
	}
	for i, a := range config.AggregatorAddresses {
		shardClient := rpcClient
		if i != 0 {
			// We want separate connection per shard even in case of ingress proxy,
			// where many/all shards have the same address.
			// So proxy can simply proxy packet conn, not rpc
			shardClient = newClient()
		}
		shardReplica := &ShardReplica{
			config:          config,
			agent:           result,
			ShardReplicaNum: i,
			ShardKey:        int32(i/3) + 1,
			ReplicaKey:      int32(i%3) + 1,
			timeSpreadDelta: commonSpread + time.Second*time.Duration(i)/time.Duration(len(config.AggregatorAddresses)),
			client: tlstatshouse.Client{
				Client:  shardClient,
				Network: network,
				Address: a,
				ActorID: 0,
			},
			stats: &shardStat{shardReplicaNum: strconv.FormatInt(int64(i), 10)},
		}
		shardReplica.alive.Store(true)
		result.ShardReplicas = append(result.ShardReplicas, shardReplica)
	}

	result.initBuiltInMetrics()
	result.updateConfigRemotelyExperimental() // first update from stored in sqlite
	return result, nil
}

func (s *Agent) initBuiltInMetrics() {
	// TODO - remove those, simply write metrics to bucket as usual
	s.statErrorsDiskWrite = s.CreateBuiltInItemValue(&data_model.Key{Metric: format.BuiltinMetricIDAgentDiskCacheErrors, Tags: [format.MaxTags]int32{0, format.TagValueIDDiskCacheErrorWrite}}, format.BuiltinMetricMetaAgentDiskCacheErrors)
	s.statErrorsDiskRead = s.CreateBuiltInItemValue(&data_model.Key{Metric: format.BuiltinMetricIDAgentDiskCacheErrors, Tags: [format.MaxTags]int32{0, format.TagValueIDDiskCacheErrorRead}}, format.BuiltinMetricMetaAgentDiskCacheErrors)
	s.statErrorsDiskErase = s.CreateBuiltInItemValue(&data_model.Key{Metric: format.BuiltinMetricIDAgentDiskCacheErrors, Tags: [format.MaxTags]int32{0, format.TagValueIDDiskCacheErrorDelete}}, format.BuiltinMetricMetaAgentDiskCacheErrors)
	s.statErrorsDiskReadNotConfigured = s.CreateBuiltInItemValue(&data_model.Key{Metric: format.BuiltinMetricIDAgentDiskCacheErrors, Tags: [format.MaxTags]int32{0, format.TagValueIDDiskCacheErrorReadNotConfigured}}, format.BuiltinMetricMetaAgentDiskCacheErrors)
	s.statErrorsDiskCompressFailed = s.CreateBuiltInItemValue(&data_model.Key{Metric: format.BuiltinMetricIDAgentDiskCacheErrors, Tags: [format.MaxTags]int32{0, format.TagValueIDDiskCacheErrorCompressFailed}}, format.BuiltinMetricMetaAgentDiskCacheErrors)
	s.statLongWindowOverflow = s.CreateBuiltInItemValue(&data_model.Key{Metric: format.BuiltinMetricIDTimingErrors, Tags: [format.MaxTags]int32{0, format.TagValueIDTimingLongWindowThrownAgent}}, format.BuiltinMetricMetaTimingErrors)
	s.statDiskOverflow = s.CreateBuiltInItemValue(&data_model.Key{Metric: format.BuiltinMetricIDTimingErrors, Tags: [format.MaxTags]int32{0, format.TagValueIDTimingLongWindowThrownAgent}}, format.BuiltinMetricMetaTimingErrors)
	s.statMemoryOverflow = s.CreateBuiltInItemValue(&data_model.Key{Metric: format.BuiltinMetricIDTimingErrors, Tags: [format.MaxTags]int32{0, format.TagValueIDTimingThrownDueToMemory}}, format.BuiltinMetricMetaTimingErrors)

	s.TimingsMapping = s.CreateBuiltInItemValue(&data_model.Key{Metric: format.BuiltinMetricIDAgentTimings, Tags: [format.MaxTags]int32{0, format.TagValueIDAgentTimingGroupPipeline, format.TagValueIDAgentTimingMapping, int32(build.CommitTimestamp()), int32(build.CommitTag())}}, format.BuiltinMetricMetaAgentTimings)
	s.TimingsMappingSlow = s.CreateBuiltInItemValue(&data_model.Key{Metric: format.BuiltinMetricIDAgentTimings, Tags: [format.MaxTags]int32{0, format.TagValueIDAgentTimingGroupPipeline, format.TagValueIDAgentTimingMappingSlow, int32(build.CommitTimestamp()), int32(build.CommitTag())}}, format.BuiltinMetricMetaAgentTimings)
	s.TimingsApplyMetric = s.CreateBuiltInItemValue(&data_model.Key{Metric: format.BuiltinMetricIDAgentTimings, Tags: [format.MaxTags]int32{0, format.TagValueIDAgentTimingGroupPipeline, format.TagValueIDAgentTimingApplyMetric, int32(build.CommitTimestamp()), int32(build.CommitTag())}}, format.BuiltinMetricMetaAgentTimings)
	s.TimingsFlush = s.CreateBuiltInItemValue(&data_model.Key{Metric: format.BuiltinMetricIDAgentTimings, Tags: [format.MaxTags]int32{0, format.TagValueIDAgentTimingGroupPipeline, format.TagValueIDAgentTimingFlush, int32(build.CommitTimestamp()), int32(build.CommitTag())}}, format.BuiltinMetricMetaAgentTimings)
	s.TimingsPreprocess = s.CreateBuiltInItemValue(&data_model.Key{Metric: format.BuiltinMetricIDAgentTimings, Tags: [format.MaxTags]int32{0, format.TagValueIDAgentTimingGroupPipeline, format.TagValueIDAgentTimingPreprocess, int32(build.CommitTimestamp()), int32(build.CommitTag())}}, format.BuiltinMetricMetaAgentTimings)
	s.TimingsSendRecent = s.CreateBuiltInItemValue(&data_model.Key{Metric: format.BuiltinMetricIDAgentTimings, Tags: [format.MaxTags]int32{0, format.TagValueIDAgentTimingGroupSend, format.TagValueIDAgentTimingSendRecent, int32(build.CommitTimestamp()), int32(build.CommitTag())}}, format.BuiltinMetricMetaAgentTimings)
	s.TimingsSendHistoric = s.CreateBuiltInItemValue(&data_model.Key{Metric: format.BuiltinMetricIDAgentTimings, Tags: [format.MaxTags]int32{0, format.TagValueIDAgentTimingGroupSend, format.TagValueIDAgentTimingSendHistoric, int32(build.CommitTimestamp()), int32(build.CommitTag())}}, format.BuiltinMetricMetaAgentTimings)

}

// Idea behind this semaphore is
// 1. semaphore for N
// 2. acquire once for each goroutine
// 3. each goroutine releases once at exit
// 4. main tries to acquire N, it will succeed when all goroutines exit
func (s *Agent) totalRecentSenders() int64 {
	return int64(len(s.Shards) * data_model.MaxConveyorDelay)
}

// separated so we can set AggregatorHost, which is dependent on tagMapper which uses agent to write statistics
func (s *Agent) Run(aggHost int32, aggShardKey int32, aggReplicaKey int32) {
	s.AggregatorHost = aggHost
	s.AggregatorShardKey = aggShardKey
	s.AggregatorReplicaKey = aggReplicaKey
	for _, shardReplica := range s.ShardReplicas {
		shardReplica.InitBuiltInMetric()

		if shardReplica.client.Address != "" {
			go shardReplica.goLiveChecker()
		}
		go shardReplica.goTestConnectionLoop()
	}
	s.recentSendersSema = semaphore.NewWeighted(s.totalRecentSenders())
	for _, shard := range s.Shards {
		s.preprocessWG.Add(1)
		go shard.goPreProcess(&s.preprocessWG)
		for j := 0; j < data_model.MaxConveyorDelay; j++ {
			_ = s.recentSendersSema.Acquire(context.Background(), 1)
			s.sendersWG.Add(1)
			go shard.goSendRecent(j, &s.sendersWG, s.recentSendersSema, s.cancelSendsCtx, shard.BucketsToSend)
		}
		for j := 0; j < data_model.MaxHistorySendStreams; j++ {
			s.sendersWG.Add(1)
			go shard.goSendHistoric(&s.sendersWG, s.cancelSendsCtx)
		}
		s.sendersWG.Add(1)
		go shard.goEraseHistoric(&s.sendersWG, s.cancelSendsCtx)
	}
	s.flusherWG.Add(1)
	go s.goFlusher(s.cancelFlushCtx, &s.flusherWG)
}

func (s *Agent) DisableNewSends() {
	for _, shard := range s.Shards {
		shard.DisableNewSends()
	}
}

func (s *Agent) WaitRecentSenders(timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := s.recentSendersSema.Acquire(ctx, s.totalRecentSenders()); err != nil {
		log.Printf("WaitRecentSenders timeout after %v: %v", timeout, err)
	}
	// either timeout passes or all recent senders quit
}

func (s *Agent) ShutdownFlusher() {
	s.cancelFlushFunc()
	for _, shard := range s.Shards {
		shard.StopReceivingIncomingData()
	}
}

func (s *Agent) WaitFlusher() {
	s.flusherWG.Wait()
}

func (s *Agent) FlushAllData() (nonEmpty int) {
	for i := 0; i != superQueueLen; i++ {
		for _, shard := range s.Shards {
			nonEmpty += shard.FlushAllDataSingleStep(false)
		}
	}
	for _, shard := range s.Shards {
		shard.StopPreprocessor()
	}
	return
}

func (s *Agent) WaitPreprocessor() {
	s.preprocessWG.Wait()
}

func (s *Agent) Close() {
	// Big TODO - quit all goroutines, including those sending historic data
}

func (s *Agent) NumShardReplicas() int {
	return len(s.ShardReplicas)
}

func (s *Agent) NumShards() int {
	return len(s.Shards)
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

func (s *Agent) getShardReplicaForSecond(shardNum int, timestamp uint32) (shardReplica *ShardReplica, spare bool) {
	replicaShift := int(timestamp % 3)
	shardReplica = s.ShardReplicas[shardNum*3+replicaShift]

	if shardReplica.alive.Load() {
		return shardReplica, false
	}
	replicaShift = int((timestamp + 1 + timestamp%2) % 3)
	shardReplica = s.ShardReplicas[shardNum*3+replicaShift]
	if !shardReplica.alive.Load() {
		return nil, false
	}
	return shardReplica, true
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
	if config.SkipShards < s.NumShards() {
		s.skipShards.Store(int32(config.SkipShards))
	} else {
		s.skipShards.Store(0)
	}
	s.builtinNewSharding.Store(config.BuiltinNewSharding)
	s.builtinNewConveyor.Store(config.BuiltinNewConveyor)
	for _, shard := range s.Shards {
		shard.mu.Lock()
		shard.config = config
		shard.hardwareMetricResolutionResolved.Store(int32(config.HardwareMetricResolution))
		shard.hardwareSlowMetricResolutionResolved.Store(int32(config.HardwareSlowMetricResolution))
		shard.mu.Unlock()
	}
	for _, shardReplica := range s.ShardReplicas {
		shardReplica.mu.Lock()
		shardReplica.config = config
		shardReplica.mu.Unlock()
	}
	if s.componentTag != format.TagValueIDComponentAggregator { // aggregator has its own separate remote config for cache sizes
		s.mappingsCache.SetSizeTTL(config.MappingCacheSize, config.MappingCacheTTL)
	}
}

func (s *Agent) goFlusher(cancelFlushCtx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	timer := time.NewTimer(time.Hour) // stupid go, we need timer, but do not yet want to start it
	if !timer.Stop() {
		<-timer.C
	}
	for {
		timer.Reset(100 * time.Millisecond)
		// If flush queue was stuck on some shard for < second, we want to retry, so we get less chance of missed seconds,
		// that's why we wait for 0.1 sec, not until start of the next second (+taking into account data_model.AgentWindow).
		// We could write complicated code here, but we do not want to risk correctness
		select {
		case <-cancelFlushCtx.Done():
			log.Printf("Flusher quit")
			return
		case <-timer.C:
		}
		s.goFlushIteration(time.Now())
		s.updateConfigRemotelyExperimental()
		// code below was used to test agent resilience to jitter
		// if rand.Intn(30) == 0 {
		//	time.Sleep(2 * time.Second)
		// }
	}
}

func (s *Agent) goFlushIteration(now time.Time) {
	nowUnix := uint32(now.Unix())
	if nowUnix > s.beforeFlushTime {
		s.beforeFlushTime = nowUnix
		if s.beforeFlushBucketFunc != nil {
			s.beforeFlushBucketFunc(s, nowUnix) // account to the current second. This is debatable.
		}
	}
	for _, shard := range s.Shards {
		shard.flushBuckets(now)
	}
	s.TimingsFlush.AddValueCounter(float64(time.Since(now).Nanoseconds()), 1)
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

// Do not create too many. ShardReplicas will iterate through values before flushing bucket
// Useful for watermark metrics.
func (s *Agent) CreateBuiltInItemValue(key *data_model.Key, metricInfo *format.MetricMetaValue) *BuiltInItemValue {
	if metricInfo.MetricID != key.Metric { // also panics if metricInfo nil
		panic("incorrectly set key Metric")
	}
	shardId, _, _, err := sharding.Shard(key, metricInfo, s.NumShards(), s.builtinNewSharding.Load())
	if err != nil {
		return nil
	}
	shard := s.Shards[shardId]
	return shard.CreateBuiltInItemValue(key)
}

func (s *Agent) UseNewConveyor() bool {
	return s.builtinNewConveyor.Load()
}

func (s *Agent) ApplyMetric(m tlstatshouse.MetricBytes, h data_model.MappedMetricHeader, ingestionStatusOKTag int32, scratch *[]byte) {
	start := time.Now()
	// Simply writing everything we know about metric ingestion errors would easily double how much metrics data we write
	// So below is basically a compromise. All info is stored in MappingMetricHeader, if needed we can easily write more
	// by changing code below
	if h.IngestionStatus != 0 {
		// h.InvalidString was validated before mapping attempt.
		// In case of utf decoding error, it contains hex representation of original string
		s.AddCounterStringBytes(&data_model.Key{
			Metric: format.BuiltinMetricIDIngestionStatus,
			Tags:   [format.MaxTags]int32{h.Key.Tags[0], h.Key.Metric, h.IngestionStatus, h.IngestionTagKey},
		}, h.InvalidString, 1, format.BuiltinMetricMetaIngestionStatus)
		return
	}
	shardId, strategy, resolutionHash, err := sharding.Shard(&h.Key, h.MetricMeta, s.NumShards(), s.builtinNewSharding.Load())
	if strategy != format.ShardBy16MappedTagsHashId && h.MetricMeta.EffectiveResolution != 1 { // new sharding and need resolution hash
		var scr []byte
		if scratch != nil {
			scr = *scratch
		}
		scr, resolutionHash = h.OriginalHash(scr)
		if scratch != nil {
			*scratch = scr
		}
	}
	if err != nil {
		s.AddCounter(&data_model.Key{
			Metric: format.BuiltinMetricIDIngestionStatus,
			Tags:   [format.MaxTags]int32{h.Key.Tags[0], h.Key.Metric, format.TagValueIDSrcIngestionStatusErrShardingFailed, h.IngestionTagKey},
		}, 1, format.BuiltinMetricMetaIngestionStatus)
		return
	}
	// after this point we are sure that metric will be applied
	defer func() {
		s.TimingsApplyMetric.AddValueCounter(float64(time.Since(start).Nanoseconds()), 1)
	}()
	// now set ok status
	s.AddCounter(&data_model.Key{
		Metric: format.BuiltinMetricIDIngestionStatus,
		Tags:   [format.MaxTags]int32{h.Key.Tags[0], h.Key.Metric, ingestionStatusOKTag, h.IngestionTagKey},
	}, 1, format.BuiltinMetricMetaIngestionStatus)
	// now set all warnings
	if h.NotFoundTagName != nil { // this is correct, can be set, but empty
		// NotFoundTagName is validated when discovered
		// This is warning, so written independent of ingestion status
		s.AddCounterStringBytes(&data_model.Key{
			Metric: format.BuiltinMetricIDIngestionStatus,
			Tags:   [format.MaxTags]int32{h.Key.Tags[0], h.Key.Metric, format.TagValueIDSrcIngestionStatusWarnMapTagNameNotFound}, // tag ID not known
		}, h.NotFoundTagName, 1, format.BuiltinMetricMetaIngestionStatus)
	}
	if h.FoundDraftTagName != nil { // this is correct, can be set, but empty
		// FoundDraftTagName is validated when discovered
		// This is warning, so written independent of ingestion status
		s.AddCounterStringBytes(&data_model.Key{
			Metric: format.BuiltinMetricIDIngestionStatus,
			Tags:   [format.MaxTags]int32{h.Key.Tags[0], h.Key.Metric, format.TagValueIDSrcIngestionStatusWarnMapTagNameFoundDraft}, // tag ID is known, but draft
		}, h.FoundDraftTagName, 1, format.BuiltinMetricMetaIngestionStatus)
	}
	if h.TagSetTwiceKey != 0 {
		s.AddCounter(&data_model.Key{
			Metric: format.BuiltinMetricIDIngestionStatus,
			Tags:   [format.MaxTags]int32{h.Key.Tags[0], h.Key.Metric, format.TagValueIDSrcIngestionStatusWarnMapTagSetTwice, h.TagSetTwiceKey},
		}, 1, format.BuiltinMetricMetaIngestionStatus)
	}
	if h.InvalidRawTagKey != 0 {
		s.AddCounterStringBytes(&data_model.Key{
			Metric: format.BuiltinMetricIDIngestionStatus,
			Tags:   [format.MaxTags]int32{h.Key.Tags[0], h.Key.Metric, format.TagValueIDSrcIngestionStatusWarnMapInvalidRawTagValue, h.InvalidRawTagKey},
		}, h.InvalidRawValue, 1, format.BuiltinMetricMetaIngestionStatus)
	}
	if h.LegacyCanonicalTagKey != 0 {
		s.AddCounter(&data_model.Key{
			Metric: format.BuiltinMetricIDIngestionStatus,
			Tags:   [format.MaxTags]int32{h.Key.Tags[0], h.Key.Metric, format.TagValueIDSrcIngestionStatusWarnDeprecatedKeyName, h.LegacyCanonicalTagKey},
		}, 1, format.BuiltinMetricMetaIngestionStatus)
	}

	// We do not check fields mask in code below, only fields values, because
	// often sending 0 instead of manipulating field mask is more convenient for many clients

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
	// m.Counter is >= 0 here, otherwise IngestionStatus is not OK, and we returned above
	shard := s.Shards[shardId]
	if len(m.Unique) != 0 {
		shard.ApplyUnique(&h.Key, resolutionHash, h.TopValue, m.Unique, m.Counter, h.HostTag, h.MetricMeta)
		return
	}
	if len(m.Histogram) != 0 || len(m.Value) != 0 {
		shard.ApplyValues(&h.Key, resolutionHash, h.TopValue, m.Histogram, m.Value, m.Counter, h.HostTag, h.MetricMeta)
		return
	}
	shard.ApplyCounter(&h.Key, resolutionHash, h.TopValue, m.Counter, h.HostTag, h.MetricMeta)
}

// count should be > 0 and not NaN
func (s *Agent) AddCounter(key *data_model.Key, count float64, metricInfo *format.MetricMetaValue) {
	s.AddCounterHost(key, count, data_model.TagUnionBytes{}, metricInfo)
}

func (s *Agent) AddCounterHost(key *data_model.Key, count float64, hostTag data_model.TagUnionBytes, metricInfo *format.MetricMetaValue) {
	if count <= 0 {
		return
	}
	if metricInfo.MetricID != key.Metric { // also panics if metricInfo nil
		panic("incorrectly set key Metric")
	}
	shardId, _, resolutionHash, err := sharding.Shard(key, metricInfo, s.NumShards(), s.builtinNewSharding.Load())
	// resolutionHash will be 0 for built-in metrics, we are OK with this
	if err != nil {
		return
	}
	shard := s.Shards[shardId]
	shard.AddCounterHost(key, resolutionHash, count, hostTag, metricInfo)
}

func (s *Agent) AddCounterStringBytes(key *data_model.Key, str []byte, count float64, metricInfo *format.MetricMetaValue) {
	s.AddCounterHostStringBytes(key, str, count, data_model.TagUnionBytes{}, metricInfo)
}

// str should be reasonably short. Empty string will be undistinguishable from "the rest"
// count should be > 0 and not NaN
func (s *Agent) AddCounterHostStringBytes(key *data_model.Key, str []byte, count float64, hostTag data_model.TagUnionBytes, metricInfo *format.MetricMetaValue) {
	if count <= 0 {
		return
	}
	if metricInfo.MetricID != key.Metric { // also panics if metricInfo nil
		panic("incorrectly set key Metric")
	}
	shardId, _, resolutionHash, err := sharding.Shard(key, metricInfo, s.NumShards(), s.builtinNewSharding.Load())
	// resolutionHash will be 0 for built-in metrics, we are OK with this
	if err != nil {
		return
	}
	shard := s.Shards[shardId]
	shard.AddCounterHostStringBytes(key, resolutionHash, data_model.TagUnionBytes{S: str, I: 0}, count, hostTag, metricInfo)
}

// value should be not NaN.
func (s *Agent) AddValueCounter(key *data_model.Key, value float64, counter float64, metricInfo *format.MetricMetaValue) {
	s.AddValueCounterHost(key, value, counter, data_model.TagUnionBytes{}, metricInfo)
}

func (s *Agent) AddValueCounterHost(key *data_model.Key, value float64, counter float64, hostTag data_model.TagUnionBytes, metricInfo *format.MetricMetaValue) {
	if counter <= 0 {
		return
	}
	if metricInfo.MetricID != key.Metric { // also panics if metricInfo nil
		panic("incorrectly set key Metric")
	}
	shardId, _, resolutionHash, err := sharding.Shard(key, metricInfo, s.NumShards(), s.builtinNewSharding.Load())
	// resolutionHash will be 0 for built-in metrics, we are OK with this
	if err != nil {
		return
	}
	shard := s.Shards[shardId]
	shard.AddValueCounterHost(key, resolutionHash, value, counter, hostTag, metricInfo)
}

func (s *Agent) MergeItemValue(key *data_model.Key, item *data_model.ItemValue, metricInfo *format.MetricMetaValue) {
	if item.Count() <= 0 {
		return
	}
	shardId, _, resolutionHash, err := sharding.Shard(key, metricInfo, s.NumShards(), s.builtinNewSharding.Load())
	// resolutionHash will be 0 for built-in metrics, we are OK with this
	if err != nil {
		return
	}
	shard := s.Shards[shardId]
	shard.MergeItemValue(key, resolutionHash, item, metricInfo)
}

func (s *Agent) AggKey(time uint32, metricID int32, keys [format.MaxTags]int32) *data_model.Key {
	return data_model.AggKey(time, metricID, keys, s.AggregatorHost, s.AggregatorShardKey, s.AggregatorReplicaKey)
}

func (s *Agent) HistoricBucketsDataSizeMemorySum() int64 {
	return s.historicBucketsDataSize.Load()
}

func (s *Agent) HistoricBucketsDataSizeDiskSum() (total int64, unsent int64) {
	if s.diskBucketCache == nil {
		return 0, 0
	}
	for i := range s.ShardReplicas {
		t, u := s.diskBucketCache.TotalFileSize(i)
		total += t
		unsent += u
	}
	return total, unsent
}
