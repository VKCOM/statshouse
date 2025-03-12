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
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/vkcom/statshouse/internal/env"
	"github.com/vkcom/statshouse/internal/sharding"
	"github.com/vkcom/statshouse/internal/vkgo/semaphore"
	"github.com/vkcom/statshouse/internal/vkgo/srvfunc"

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
	GetConfigResult tlstatshouse.GetConfigResult3 // for ingress proxy

	cancelSendsFunc   context.CancelFunc
	cancelSendsCtx    context.Context
	recentSendersSema *semaphore.Weighted
	sendersWG         sync.WaitGroup
	cancelFlushFunc   context.CancelFunc
	cancelFlushCtx    context.Context
	flusherWG         sync.WaitGroup
	preprocessWG      sync.WaitGroup

	network         string // to communicate to aggregators
	cacheDir        string
	rpcClientConfig *rpc.Client
	diskBucketCache *DiskBucketStorage
	hostName        []byte
	argsHash        int32
	argsLen         int32
	args            string
	config          Config
	logF            rpc.LoggerFunc
	envLoader       *env.Loader

	statshouseRemoteConfigString string        // optimization
	skipShards                   atomic.Int32  // copy from config.
	newShardingByName            atomic.String // copy from config.
	shardByMetricCount           uint32        // never changes, access without lock
	conveyorV3                   atomic.Bool   // copy from config.

	rUsage                syscall.Rusage // accessed without lock by first shard addBuiltIns
	heartBeatEventType    int32          // first time "start", then "heartbeat"
	heartBeatSecondBucket uint32         // random [0..59] bucket for per minute heartbeat to spread load on aggregator
	startTimestamp        uint32

	mappingsCache *pcache.MappingsCache
	metricStorage format.MetaStorageInterface

	journalHV        func() (int64, string, string)
	journalFastHV    func() (int64, string)
	journalCompactHV func() (int64, string)

	componentTag int32 // agent or ingress proxy or aggregator (they have agents for built-in metrics)
	stagingLevel int
	buildArchTag int32
	// Used for builtin metrics when running inside aggregator
	AggregatorShardKey   int32
	AggregatorReplicaKey int32
	AggregatorHost       int32

	beforeFlushBucketFunc func(s *Agent, nowUnix uint32) // used by aggregator to add built-in metrics
	beforeFlushTime       uint32                         // changed exclusively by goFlusher
	BuiltInItemValues     []*BuiltInItemValue            // Collected/reset periodically

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

	// copy of builtin metric, but with resolution set to 1
	// allows agent to manually spread this metric around minute freely
	// while aggregator correctly merges into more coarse resolution
	builtinMetricMetaUsageCPU         format.MetricMetaValue
	builtinMetricMetaUsageMemory      format.MetricMetaValue
	builtinMetricMetaHeartbeatVersion format.MetricMetaValue
	builtinMetricMetaHeartbeatArgs    format.MetricMetaValue
}

func stagingLevel(statsHouseEnv string) int {
	switch statsHouseEnv {
	case "production":
		return 0
	case "staging", "staging1":
		return 1
	case "staging2":
		return 2
	case "development", "staging3": // TODO: staging3 is an alias for development, remove if after all commands lines are updated
		return 3
	default:
		// Our built-in metrics are supposed to work without mapping, so all keys must be known in advance
		// Also we protect built-in metrics from sampling, so must ensure their cardinality is limited
		return -1
	}
}

// All shard aggregators must be on the same network
func MakeAgent(network string, cacheDir string, aesPwd string, config Config, hostName string, componentTag int32, metricStorage format.MetaStorageInterface,
	mappingsCache *pcache.MappingsCache,
	journalHV func() (int64, string, string), journalFastHV func() (int64, string), journalCompactHV func() (int64, string),
	logF func(format string, args ...interface{}),
	beforeFlushBucketFunc func(s *Agent, nowUnix uint32), getConfigResult *tlstatshouse.GetConfigResult3, envLoader *env.Loader) (*Agent, error) {
	newClient := func() *rpc.Client {
		return rpc.NewClient(
			rpc.ClientWithProtocolVersion(rpc.LatestProtocolVersion),
			rpc.ClientWithCryptoKey(aesPwd),
			rpc.ClientWithTrustedSubnetGroups(build.TrustedSubnetGroups()),
			rpc.ClientWithLogf(logF))
	}
	rnd := rand.New()
	allArgs := strings.Join(os.Args[1:], " ")
	argsHash := sha1.Sum([]byte(allArgs))

	cancelSendsCtx, cancelSendsFunc := context.WithCancel(context.Background())
	cancelFlushCtx, cancelFlushFunc := context.WithCancel(context.Background())

	result := &Agent{
		cancelSendsCtx:                    cancelSendsCtx,
		cancelSendsFunc:                   cancelSendsFunc,
		cancelFlushCtx:                    cancelFlushCtx,
		cancelFlushFunc:                   cancelFlushFunc,
		hostName:                          format.ForceValidStringValue(hostName), // worse alternative is do not run at all
		componentTag:                      componentTag,
		heartBeatEventType:                format.TagValueIDHeartbeatEventStart,
		heartBeatSecondBucket:             uint32(rnd.Intn(60)),
		config:                            config,
		cacheDir:                          cacheDir,
		rpcClientConfig:                   newClient(),
		network:                           network,
		argsHash:                          int32(binary.BigEndian.Uint32(argsHash[:])),
		argsLen:                           int32(len(allArgs)),
		args:                              string(format.ForceValidStringValue(allArgs)), // if single arg is too big, it is truncated here
		logF:                              logF,
		buildArchTag:                      format.GetBuildArchKey(runtime.GOARCH),
		mappingsCache:                     mappingsCache,
		journalHV:                         journalHV,
		journalFastHV:                     journalFastHV,
		journalCompactHV:                  journalCompactHV,
		metricStorage:                     metricStorage,
		beforeFlushBucketFunc:             beforeFlushBucketFunc,
		envLoader:                         envLoader,
		builtinMetricMetaUsageCPU:         *format.BuiltinMetricMetaUsageCPU,
		builtinMetricMetaUsageMemory:      *format.BuiltinMetricMetaUsageMemory,
		builtinMetricMetaHeartbeatVersion: *format.BuiltinMetricMetaHeartbeatVersion,
		builtinMetricMetaHeartbeatArgs:    *format.BuiltinMetricMetaHeartbeatArgs,
	}
	result.builtinMetricMetaUsageCPU.Resolution = 1
	result.builtinMetricMetaUsageCPU.EffectiveResolution = 1
	result.builtinMetricMetaUsageMemory.Resolution = 1
	result.builtinMetricMetaUsageMemory.EffectiveResolution = 1
	result.builtinMetricMetaHeartbeatVersion.Resolution = 1
	result.builtinMetricMetaHeartbeatVersion.EffectiveResolution = 1
	result.builtinMetricMetaHeartbeatArgs.Resolution = 1
	result.builtinMetricMetaHeartbeatArgs.EffectiveResolution = 1
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &result.rUsage)

	if l := stagingLevel(config.StatsHouseEnv); l >= 0 {
		result.stagingLevel = l
	} else {
		// Our built-in metrics are supposed to work without mapping, so all keys must be known in advance
		// Also we protect built-in metrics from sampling, so must ensure their cardinality is limited
		return nil, fmt.Errorf("configuration error: --statshouse-env (%q) should be 'production', 'staging1', 'staging2' or 'development'", config.StatsHouseEnv)
	}
	logF("Configuration: detected build arch key as %d for string %q", result.buildArchTag, runtime.GOARCH)
	if getConfigResult != nil {
		result.GetConfigResult = *getConfigResult // Inside aggregator
	} else {
		if len(config.AggregatorAddresses) < 3 {
			return nil, fmt.Errorf("configuration Error: must have 3 aggregator addresses for configuration redundancy")
		}
		result.GetConfigResult = result.getInitialConfig()
	}
	result.shardByMetricCount = result.GetConfigResult.ShardByMetricCount
	now := time.Now()
	nowUnix := uint32(now.Unix())
	result.beforeFlushTime = nowUnix

	result.startTimestamp = nowUnix
	if cacheDir != "" {
		dbc, err := MakeDiskBucketStorage(cacheDir, len(result.GetConfigResult.Addresses)/3, logF)
		if err != nil {
			return nil, err
		}
		result.diskBucketCache = dbc
	}
	commonSpread := time.Duration(rnd.Int63n(int64(time.Second) / int64(len(result.GetConfigResult.Addresses))))
	for i := 0; i < len(result.GetConfigResult.Addresses)/3; i++ {
		shard := &Shard{
			config:              config,
			agent:               result,
			ShardNum:            i,
			ShardKey:            int32(i) + 1,
			timeSpreadDelta:     3*commonSpread + 3*time.Second*time.Duration(i)/time.Duration(len(result.GetConfigResult.Addresses)),
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
	for i, a := range result.GetConfigResult.Addresses {
		shardReplicaClient := result.rpcClientConfig
		if shardNum := i / 3; shardNum != 0 {
			// We recommend giving configuration addresses of first shard,
			// so in this case there would be no extra connections for configuration.
			// In case configuration addresses are of shard other than first or some
			// random addresses, there would be extra connections.
			shardReplicaClient = newClient()
		}
		// We want separate connection per shard even in case of ingress proxy,
		// where many/all shards have the same address.
		// So proxy can simply proxy packet conn, not rpc
		shardReplica := &ShardReplica{
			config:          config,
			agent:           result,
			ShardReplicaNum: i,
			ShardKey:        int32(i/3) + 1,
			ReplicaKey:      int32(i%3) + 1,
			timeSpreadDelta: commonSpread + time.Second*time.Duration(i)/time.Duration(len(result.GetConfigResult.Addresses)),
			clientField: tlstatshouse.Client{
				Client:  shardReplicaClient,
				Network: network,
				Address: a,
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

func (s *Agent) ComponentTag() int32 { return s.componentTag }

func (s *Agent) initBuiltInMetrics() {
	// TODO - remove those, simply write metrics to bucket as usual
	s.statErrorsDiskWrite = s.CreateBuiltInItemValue(format.BuiltinMetricMetaAgentDiskCacheErrors,
		[]int32{0, format.TagValueIDDiskCacheErrorWrite})
	s.statErrorsDiskRead = s.CreateBuiltInItemValue(format.BuiltinMetricMetaAgentDiskCacheErrors,
		[]int32{0, format.TagValueIDDiskCacheErrorRead})
	s.statErrorsDiskErase = s.CreateBuiltInItemValue(format.BuiltinMetricMetaAgentDiskCacheErrors,
		[]int32{0, format.TagValueIDDiskCacheErrorDelete})
	s.statErrorsDiskReadNotConfigured = s.CreateBuiltInItemValue(format.BuiltinMetricMetaAgentDiskCacheErrors,
		[]int32{0, format.TagValueIDDiskCacheErrorReadNotConfigured})
	s.statErrorsDiskCompressFailed = s.CreateBuiltInItemValue(format.BuiltinMetricMetaAgentDiskCacheErrors,
		[]int32{0, format.TagValueIDDiskCacheErrorCompressFailed})
	s.statLongWindowOverflow = s.CreateBuiltInItemValue(format.BuiltinMetricMetaTimingErrors,
		[]int32{0, format.TagValueIDTimingLongWindowThrownAgent})
	s.statDiskOverflow = s.CreateBuiltInItemValue(format.BuiltinMetricMetaTimingErrors,
		[]int32{0, format.TagValueIDTimingLongWindowThrownAgent})
	s.statMemoryOverflow = s.CreateBuiltInItemValue(format.BuiltinMetricMetaTimingErrors,
		[]int32{0, format.TagValueIDTimingThrownDueToMemory})

	s.TimingsMapping = s.CreateBuiltInItemValue(format.BuiltinMetricMetaAgentTimings,
		[]int32{0, format.TagValueIDAgentTimingGroupPipeline, format.TagValueIDAgentTimingMapping, int32(build.CommitTimestamp()), int32(build.CommitTag())})
	s.TimingsMappingSlow = s.CreateBuiltInItemValue(format.BuiltinMetricMetaAgentTimings,
		[]int32{0, format.TagValueIDAgentTimingGroupPipeline, format.TagValueIDAgentTimingMappingSlow, int32(build.CommitTimestamp()), int32(build.CommitTag())})
	s.TimingsApplyMetric = s.CreateBuiltInItemValue(format.BuiltinMetricMetaAgentTimings,
		[]int32{0, format.TagValueIDAgentTimingGroupPipeline, format.TagValueIDAgentTimingApplyMetric, int32(build.CommitTimestamp()), int32(build.CommitTag())})
	s.TimingsFlush = s.CreateBuiltInItemValue(format.BuiltinMetricMetaAgentTimings,
		[]int32{0, format.TagValueIDAgentTimingGroupPipeline, format.TagValueIDAgentTimingFlush, int32(build.CommitTimestamp()), int32(build.CommitTag())})
	s.TimingsPreprocess = s.CreateBuiltInItemValue(format.BuiltinMetricMetaAgentTimings,
		[]int32{0, format.TagValueIDAgentTimingGroupPipeline, format.TagValueIDAgentTimingPreprocess, int32(build.CommitTimestamp()), int32(build.CommitTag())})
	s.TimingsSendRecent = s.CreateBuiltInItemValue(format.BuiltinMetricMetaAgentTimings,
		[]int32{0, format.TagValueIDAgentTimingGroupSend, format.TagValueIDAgentTimingSendRecent, int32(build.CommitTimestamp()), int32(build.CommitTag())})
	s.TimingsSendHistoric = s.CreateBuiltInItemValue(format.BuiltinMetricMetaAgentTimings,
		[]int32{0, format.TagValueIDAgentTimingGroupSend, format.TagValueIDAgentTimingSendHistoric, int32(build.CommitTimestamp()), int32(build.CommitTag())})

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
		if shardReplica.client().Address != "" {
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
	if mv := s.metricStorage.GetMetaMetricByName(format.StatshouseAgentRemoteConfigMetric); mv != nil {
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
		s.logF("Remote config: error updating config from metric %q: %v", format.StatshouseAgentRemoteConfigMetric, err)
		return
	}
	s.logF("Remote config: updated config from metric %q", format.StatshouseAgentRemoteConfigMetric)
	if config.SkipShards < s.NumShards() {
		s.skipShards.Store(int32(config.SkipShards))
	} else {
		s.skipShards.Store(0)
	}
	s.newShardingByName.Store(config.NewShardingByName)
	conveyorV3 := slices.Contains(config.ConveyorV3StagingList, s.stagingLevel)
	if conveyorV3 {
		log.Printf("New conveyor is enabled")
	} else {
		log.Printf("New conveyor is disabled")
	}
	s.conveyorV3.Store(conveyorV3)
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

func (s *Agent) addBuiltins(nowUnix uint32) {
	for _, shard := range s.Shards {
		sizeMem := shard.HistoricBucketsDataSizeMemory()
		sizeDiskTotal, sizeDiskUnsent := shard.HistoricBucketsDataSizeDisk()
		if sizeMem > 0 {
			s.AddValueCounter(nowUnix, format.BuiltinMetricMetaAgentHistoricQueueSize,
				[]int32{0, format.TagValueIDHistoricQueueMemory, 0, 0, 0, 0, s.componentTag, format.AggShardTag: shard.ShardKey},
				float64(sizeMem), 1)
		}
		if sizeDiskUnsent > 0 {
			s.AddValueCounter(nowUnix, format.BuiltinMetricMetaAgentHistoricQueueSize,
				[]int32{0, format.TagValueIDHistoricQueueDiskUnsent, 0, 0, 0, 0, s.componentTag, format.AggShardTag: shard.ShardKey},
				float64(sizeDiskUnsent), 1)
		}
		if sent := sizeDiskTotal - sizeDiskUnsent; sent > 0 {
			s.AddValueCounter(nowUnix, format.BuiltinMetricMetaAgentHistoricQueueSize,
				[]int32{0, format.TagValueIDHistoricQueueDiskSent, 0, 0, 0, 0, s.componentTag, format.AggShardTag: shard.ShardKey},
				float64(sent), 1)
		}
		if sizeMem <= 0 && sizeDiskUnsent <= 0 { // no data waiting to be sent
			s.AddValueCounter(nowUnix, format.BuiltinMetricMetaAgentHistoricQueueSize,
				[]int32{0, format.TagValueIDHistoricQueueEmpty, 0, 0, 0, 0, s.componentTag, format.AggShardTag: shard.ShardKey},
				0, 1)
		}
	}

	elements, sumSize, averageTS, adds, evicts, timestampUpdates, timestampUpdateSkips := s.mappingsCache.Stats()
	if elements > 0 {
		s.AddValueCounter(nowUnix, format.BuiltinMetricMetaMappingCacheElements,
			[]int32{0, s.componentTag},
			float64(elements), 1)
		s.AddValueCounter(nowUnix, format.BuiltinMetricMetaMappingCacheSize,
			[]int32{0, s.componentTag},
			float64(sumSize), 1)
		s.AddValueCounter(nowUnix, format.BuiltinMetricMetaMappingCacheAverageTTL,
			[]int32{0, s.componentTag},
			float64(nowUnix)-float64(averageTS), 1)
		s.AddCounter(nowUnix, format.BuiltinMetricMetaMappingCacheEvent,
			[]int32{0, s.componentTag, format.TagValueIDMappingCacheEventAdd},
			float64(adds))
		s.AddCounter(nowUnix, format.BuiltinMetricMetaMappingCacheEvent,
			[]int32{0, s.componentTag, format.TagValueIDMappingCacheEventEvict},
			float64(evicts))
		s.AddCounter(nowUnix, format.BuiltinMetricMetaMappingCacheEvent,
			[]int32{0, s.componentTag, format.TagValueIDMappingCacheEventTimestampUpdate},
			float64(timestampUpdates))
		s.AddCounter(nowUnix, format.BuiltinMetricMetaMappingCacheEvent,
			[]int32{0, s.componentTag, format.TagValueIDMappingCacheEventTimestampUpdateSkip},
			float64(timestampUpdateSkips))
	}

	writeJournalVersion := func(version int64, hashStr string, journalTag int32) {
		hashTag := int32(0)
		hashRaw, _ := hex.DecodeString(hashStr)
		if len(hashRaw) >= 4 {
			hashTag = int32(binary.BigEndian.Uint32(hashRaw))
		}
		s.AddCounterStringBytes(nowUnix, format.BuiltinMetricMetaJournalVersions,
			[]int32{0, s.componentTag, 0, 0, 0, int32(version), hashTag, journalTag},
			[]byte(hashStr), 1)
	}

	if s.journalHV != nil {
		version, hashStr, hashStrXXH3 := s.journalHV()
		writeJournalVersion(version, hashStr, format.TagValueIDMetaJournalVersionsKindLegacySHA1)
		writeJournalVersion(version, hashStrXXH3, format.TagValueIDMetaJournalVersionsKindLegacyXXH3)
	}
	if s.journalFastHV != nil {
		version, hashStr := s.journalFastHV()
		writeJournalVersion(version, hashStr, format.TagValueIDMetaJournalVersionsKindNormalXXH3)
	}
	if s.journalCompactHV != nil {
		version, hashStr := s.journalCompactHV()
		writeJournalVersion(version, hashStr, format.TagValueIDMetaJournalVersionsKindCompactXXH3)
	}

	prevRUsage := s.rUsage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &s.rUsage)
	userTime := float64(s.rUsage.Utime.Nano()-prevRUsage.Utime.Nano()) / float64(time.Second)
	sysTime := float64(s.rUsage.Stime.Nano()-prevRUsage.Stime.Nano()) / float64(time.Second)

	s.AddValueCounter(nowUnix, &s.builtinMetricMetaUsageCPU,
		[]int32{0, s.componentTag, format.TagValueIDCPUUsageUser},
		userTime, 1)
	s.AddValueCounter(nowUnix, &s.builtinMetricMetaUsageCPU,
		[]int32{0, s.componentTag, format.TagValueIDCPUUsageSys},
		sysTime, 1)

	if s.heartBeatEventType != format.TagValueIDHeartbeatEventHeartbeat { // first run
		s.addBuiltInsHeartbeatsLocked(nowUnix, 1)
		s.heartBeatEventType = format.TagValueIDHeartbeatEventHeartbeat
	}

	if nowUnix%30 == 0 { // do not want to do it too often, because function stops the world
		var rss float64
		if st, _ := srvfunc.GetMemStat(0); st != nil {
			rss = float64(st.Res)
		}
		s.AddValueCounter(nowUnix, &s.builtinMetricMetaUsageMemory,
			[]int32{0, s.componentTag},
			rss, 30)
	}

	if nowUnix%60 == s.heartBeatSecondBucket {
		// we must manually spread this metric around time resolution for now, because otherwise
		// they all will arrive to aggregator in the same second inside minute, with huge spike
		// in sampling factors/insert size
		s.addBuiltInsHeartbeatsLocked(nowUnix, 60)
	}

	s.mu.Lock() // we have very little things blocking on this lock, so simply take it
	defer s.mu.Unlock()
	for _, v := range s.BuiltInItemValues {
		v.mu.Lock()
		if v.value.Count() > 0 {
			s.MergeItemValue(nowUnix, v.metricInfo, v.key.Tags[:], &v.value)
		}
		v.value = data_model.ItemValue{} // simply reset Counter, even if somehow <0
		v.mu.Unlock()
	}
}

func (s *Agent) addBuiltInsHeartbeatsLocked(nowUnix uint32, count float64) {
	uptimeSec := float64(nowUnix - s.startTimestamp)

	s.AddValueCounterString(nowUnix, &s.builtinMetricMetaHeartbeatVersion,
		[]int32{0, s.componentTag, s.heartBeatEventType},
		build.Commit(), uptimeSec, count)
	s.AddValueCounterString(nowUnix, &s.builtinMetricMetaHeartbeatArgs,
		[]int32{0, s.componentTag, s.heartBeatEventType, s.argsHash, 0, 0, 0, 0, 0, s.argsLen},
		s.args, uptimeSec, count)
}

func (s *Agent) goFlushIteration(now time.Time) {
	nowUnix := uint32(now.Unix())
	if nowUnix > s.beforeFlushTime {
		s.addBuiltins(s.beforeFlushTime)
		if s.beforeFlushBucketFunc != nil {
			s.beforeFlushBucketFunc(s, s.beforeFlushTime)
		}
		s.beforeFlushTime = nowUnix
	}
	for _, shard := range s.Shards {
		gap, sendTime := shard.flushBuckets(now)
		if gap > 0 { // never if conveyor is not stuck
			s.AddValueCounter(sendTime, format.BuiltinMetricMetaTimingErrors,
				[]int32{0, format.TagValueIDTimingMissedSecondsAgent},
				float64(gap), 1) // values record jumps for more than 1 second
		}
	}
	s.TimingsFlush.AddValueCounter(time.Since(now).Seconds(), 1)
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

func (s *Agent) shard(key *data_model.Key, metricInfo *format.MetricMetaValue) (shardID uint32, newStrategy bool, weightMul int, legacyKeyHash uint64) {
	return sharding.Shard(key, metricInfo, s.NumShards(), s.shardByMetricCount, s.newShardingByName.Load())
}

// Do not create too many. ShardReplicas will iterate through values before flushing bucket
// Useful for watermark metrics.
func (s *Agent) CreateBuiltInItemValue(metricInfo *format.MetricMetaValue, tags []int32) *BuiltInItemValue {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := data_model.Key{
		Metric: metricInfo.MetricID, // panics if metricInfo nil
	}
	copy(key.Tags[:], tags)
	result := &BuiltInItemValue{key: key, metricInfo: metricInfo}
	s.BuiltInItemValues = append(s.BuiltInItemValues, result)
	return result
}

func (s *Agent) UseConveyorV3() bool {
	return s.conveyorV3.Load()
}

func (s *Agent) ApplyMetric(m tlstatshouse.MetricBytes, h data_model.MappedMetricHeader, ingestionStatusOKTag int32, scratch *[]byte) {
	start := time.Now()
	// Simply writing everything we know about metric ingestion errors would easily double how much metrics data we write
	// So below is basically a compromise. All info is stored in MappingMetricHeader, if needed we can easily write more
	// by changing code below
	if h.IngestionStatus != 0 {
		// h.InvalidString was validated before mapping attempt.
		// In case of utf decoding error, it contains hex representation of original string
		s.AddCounterStringBytes(0, format.BuiltinMetricMetaIngestionStatus,
			[]int32{h.Key.Tags[0], h.Key.Metric, h.IngestionStatus, h.IngestionTagKey},
			h.InvalidString, 1)
		return
	}
	shardId, newStrategy, weightMul, resolutionHash := s.shard(&h.Key, h.MetricMeta)
	if shardId >= uint32(len(s.Shards)) {
		s.AddCounter(0, format.BuiltinMetricMetaIngestionStatus,
			[]int32{h.Key.Tags[0], h.Key.Metric, format.TagValueIDSrcIngestionStatusErrShardingFailed, 0},
			1)
		return
	}
	shard := s.Shards[shardId]
	if newStrategy && h.MetricMeta.EffectiveResolution != 1 { // new sharding and need resolution hash
		var scr []byte
		if scratch != nil {
			scr = *scratch
		}
		scr, resolutionHash = h.OriginalHash(scr)
		if scratch != nil {
			*scratch = scr
		}
	}
	// after this point we are sure that metric will be applied
	defer func() {
		s.TimingsApplyMetric.AddValueCounter(time.Since(start).Seconds(), 1)
	}()
	// now set ok status
	s.AddCounter(0, format.BuiltinMetricMetaIngestionStatus,
		[]int32{h.Key.Tags[0], h.Key.Metric, ingestionStatusOKTag, h.IngestionTagKey},
		1)
	// now set all warnings
	if h.NotFoundTagName != nil { // this is correct, can be set, but empty
		// NotFoundTagName is validated when discovered
		// This is warning, so written independent of ingestion status
		s.AddCounterStringBytes(0, format.BuiltinMetricMetaIngestionStatus,
			[]int32{h.Key.Tags[0], h.Key.Metric, format.TagValueIDSrcIngestionStatusWarnMapTagNameNotFound}, // tag ID not known
			h.NotFoundTagName, 1)
	}
	if h.FoundDraftTagName != nil { // this is correct, can be set, but empty
		// FoundDraftTagName is validated when discovered
		// This is warning, so written independent of ingestion status
		s.AddCounterStringBytes(0, format.BuiltinMetricMetaIngestionStatus,
			[]int32{h.Key.Tags[0], h.Key.Metric, format.TagValueIDSrcIngestionStatusWarnMapTagNameFoundDraft}, // tag ID is known, but draft
			h.FoundDraftTagName, 1)
	}
	if h.TagSetTwiceKey != 0 {
		s.AddCounter(0, format.BuiltinMetricMetaIngestionStatus,
			[]int32{h.Key.Tags[0], h.Key.Metric, format.TagValueIDSrcIngestionStatusWarnMapTagSetTwice, h.TagSetTwiceKey},
			1)
	}
	if h.InvalidRawTagKey != 0 {
		s.AddCounterStringBytes(0, format.BuiltinMetricMetaIngestionStatus,
			[]int32{h.Key.Tags[0], h.Key.Metric, format.TagValueIDSrcIngestionStatusWarnMapInvalidRawTagValue, h.InvalidRawTagKey},
			h.InvalidRawValue, 1)
	}
	if h.LegacyCanonicalTagKey != 0 {
		s.AddCounter(0, format.BuiltinMetricMetaIngestionStatus,
			[]int32{h.Key.Tags[0], h.Key.Metric, format.TagValueIDSrcIngestionStatusWarnDeprecatedKeyName, h.LegacyCanonicalTagKey},
			1)
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
	if len(m.Unique) != 0 {
		shard.ApplyUnique(&h.Key, resolutionHash, h.TopValue, m.Unique, m.Counter, h.HostTag, h.MetricMeta, weightMul)
		return
	}
	if len(m.Histogram) != 0 || len(m.Value) != 0 {
		shard.ApplyValues(&h.Key, resolutionHash, h.TopValue, m.Histogram, m.Value, m.Counter, h.HostTag, h.MetricMeta, weightMul)
		return
	}
	shard.ApplyCounter(&h.Key, resolutionHash, h.TopValue, m.Counter, h.HostTag, h.MetricMeta, weightMul)
}

// count should be > 0 and not NaN
func (s *Agent) AddCounter(t uint32, metricInfo *format.MetricMetaValue, tags []int32, count float64) {
	s.AddCounterHostAERA(t, metricInfo, tags, count, data_model.TagUnionBytes{}, format.AgentEnvRouteArch{})
}

func (s *Agent) AddCounterHost(t uint32, metricInfo *format.MetricMetaValue, tags []int32, count float64, hostTag data_model.TagUnionBytes) {
	s.AddCounterHostAERA(t, metricInfo, tags, count, hostTag, format.AgentEnvRouteArch{})
}

func (s *Agent) AddCounterHostAERA(t uint32, metricInfo *format.MetricMetaValue, tags []int32, count float64, hostTag data_model.TagUnionBytes, aera format.AgentEnvRouteArch) {
	if count <= 0 {
		return
	}
	key := data_model.Key{Timestamp: t, Metric: metricInfo.MetricID} // panics if metricInfo nil
	copy(key.Tags[:], tags)
	if metricInfo.WithAggregatorID {
		key.Tags[format.AggHostTag] = s.AggregatorHost
		key.Tags[format.AggShardTag] = s.AggregatorShardKey
		key.Tags[format.AggReplicaTag] = s.AggregatorReplicaKey
	}
	if metricInfo.WithAgentEnvRouteArch {
		key.Tags[format.AgentEnvTag] = aera.AgentEnv
		key.Tags[format.RouteTag] = aera.Route
		key.Tags[format.BuildArchTag] = aera.BuildArch
	}
	shardId, _, weightMul, resolutionHash := s.shard(&key, metricInfo)
	// resolutionHash will be 0 for built-in metrics, we are OK with this
	shard := s.Shards[shardId]
	shard.AddCounterHost(&key, resolutionHash, count, hostTag, metricInfo, weightMul)
}

func (s *Agent) AddCounterStringBytes(t uint32, metricInfo *format.MetricMetaValue, tags []int32, str []byte, count float64) {
	s.AddCounterHostStringBytesAERA(t, metricInfo, tags, str, count, data_model.TagUnionBytes{}, format.AgentEnvRouteArch{})
}

// str should be reasonably short. Empty string will be undistinguishable from "the rest"
// count should be > 0 and not NaN
func (s *Agent) AddCounterHostStringBytesAERA(t uint32, metricInfo *format.MetricMetaValue, tags []int32, str []byte, count float64, hostTag data_model.TagUnionBytes, aera format.AgentEnvRouteArch) {
	if count <= 0 {
		return
	}
	key := data_model.Key{Timestamp: t, Metric: metricInfo.MetricID} // panics if metricInfo nil
	copy(key.Tags[:], tags)
	if metricInfo.WithAggregatorID {
		key.Tags[format.AggHostTag] = s.AggregatorHost
		key.Tags[format.AggShardTag] = s.AggregatorShardKey
		key.Tags[format.AggReplicaTag] = s.AggregatorReplicaKey
	}
	if metricInfo.WithAgentEnvRouteArch {
		key.Tags[format.AgentEnvTag] = aera.AgentEnv
		key.Tags[format.RouteTag] = aera.Route
		key.Tags[format.BuildArchTag] = aera.BuildArch
	}
	shardId, _, weightMul, resolutionHash := s.shard(&key, metricInfo)
	// resolutionHash will be 0 for built-in metrics, we are OK with this
	shard := s.Shards[shardId]
	shard.AddCounterHostStringBytes(&key, resolutionHash, data_model.TagUnionBytes{S: str, I: 0}, count, hostTag, metricInfo, weightMul)
}

// value should be not NaN.
func (s *Agent) AddValueCounter(t uint32, metricInfo *format.MetricMetaValue, tags []int32, value float64, counter float64) {
	s.AddValueCounterHostAERA(t, metricInfo, tags, value, counter, data_model.TagUnionBytes{}, format.AgentEnvRouteArch{})
}

func (s *Agent) AddValueCounterHost(t uint32, metricInfo *format.MetricMetaValue, tags []int32, value float64, counter float64, hostTag data_model.TagUnionBytes) {
	s.AddValueCounterHostAERA(t, metricInfo, tags, value, counter, hostTag, format.AgentEnvRouteArch{})
}

func (s *Agent) AddValueCounterHostAERA(t uint32, metricInfo *format.MetricMetaValue, tags []int32, value float64, counter float64, hostTag data_model.TagUnionBytes, aera format.AgentEnvRouteArch) {
	if counter <= 0 {
		return
	}
	key := data_model.Key{Timestamp: t, Metric: metricInfo.MetricID} // panics if metricInfo nil
	copy(key.Tags[:], tags)
	if metricInfo.WithAggregatorID {
		key.Tags[format.AggHostTag] = s.AggregatorHost
		key.Tags[format.AggShardTag] = s.AggregatorShardKey
		key.Tags[format.AggReplicaTag] = s.AggregatorReplicaKey
	}
	if metricInfo.WithAgentEnvRouteArch {
		key.Tags[format.AgentEnvTag] = aera.AgentEnv
		key.Tags[format.RouteTag] = aera.Route
		key.Tags[format.BuildArchTag] = aera.BuildArch
	}
	shardId, _, weightMul, resolutionHash := s.shard(&key, metricInfo)
	// resolutionHash will be 0 for built-in metrics, we are OK with this
	shard := s.Shards[shardId]
	shard.AddValueCounterHost(&key, resolutionHash, value, counter, hostTag, metricInfo, weightMul)
}

// value should be not NaN.
func (s *Agent) AddValueCounterString(t uint32, metricInfo *format.MetricMetaValue, tags []int32, str string, value float64, counter float64) {
	s.AddValueCounterStringHostAERA(t, metricInfo, tags, data_model.TagUnion{S: str, I: 0}, value, counter, data_model.TagUnionBytes{}, format.AgentEnvRouteArch{})
}

func (s *Agent) AddValueCounterStringHostAERA(t uint32, metricInfo *format.MetricMetaValue, tags []int32, topValue data_model.TagUnion, value float64, counter float64, hostTag data_model.TagUnionBytes, aera format.AgentEnvRouteArch) {
	if counter <= 0 {
		return
	}
	key := data_model.Key{Timestamp: t, Metric: metricInfo.MetricID} // panics if metricInfo nil
	copy(key.Tags[:], tags)
	if metricInfo.WithAggregatorID {
		key.Tags[format.AggHostTag] = s.AggregatorHost
		key.Tags[format.AggShardTag] = s.AggregatorShardKey
		key.Tags[format.AggReplicaTag] = s.AggregatorReplicaKey
	}
	if metricInfo.WithAgentEnvRouteArch {
		key.Tags[format.AgentEnvTag] = aera.AgentEnv
		key.Tags[format.RouteTag] = aera.Route
		key.Tags[format.BuildArchTag] = aera.BuildArch
	}
	shardId, _, weightMul, resolutionHash := s.shard(&key, metricInfo)
	// resolutionHash will be 0 for built-in metrics, we are OK with this
	shard := s.Shards[shardId]
	shard.AddValueCounterStringHost(&key, resolutionHash, topValue, value, counter, hostTag, metricInfo, weightMul)
}

func (s *Agent) MergeItemValue(t uint32, metricInfo *format.MetricMetaValue, tags []int32, item *data_model.ItemValue) {
	if item.Count() <= 0 {
		return
	}
	key := data_model.Key{Timestamp: t, Metric: metricInfo.MetricID} // panics if metricInfo nil
	copy(key.Tags[:], tags)
	if metricInfo.WithAggregatorID {
		key.Tags[format.AggHostTag] = s.AggregatorHost
		key.Tags[format.AggShardTag] = s.AggregatorShardKey
		key.Tags[format.AggReplicaTag] = s.AggregatorReplicaKey
	}
	shardId, _, weightMul, resolutionHash := s.shard(&key, metricInfo)
	// resolutionHash will be 0 for built-in metrics, we are OK with this
	shard := s.Shards[shardId]
	shard.MergeItemValue(&key, resolutionHash, item, metricInfo, weightMul)
}

func (s *Agent) HistoricBucketsDataSizeMemorySum() int64 {
	return s.historicBucketsDataSize.Load()
}

func (s *Agent) HistoricBucketsDataSizeDiskSum() (total int64, unsent int64) {
	if s.diskBucketCache == nil {
		return 0, 0
	}
	for i := range s.Shards {
		t, u := s.diskBucketCache.TotalFileSize(i)
		total += t
		unsent += u
	}
	return total, unsent
}

// public for aggregator
func (s *Agent) GetMultiItemAERA(resolutionShard *data_model.MultiItemMap, t uint32, metricInfo *format.MetricMetaValue, weightMul int, tags []int32, aera format.AgentEnvRouteArch) *data_model.MultiItem {
	key := data_model.Key{Timestamp: t, Metric: metricInfo.MetricID}
	copy(key.Tags[:], tags)
	if metricInfo.WithAggregatorID {
		key.Tags[format.AggHostTag] = s.AggregatorHost
		key.Tags[format.AggShardTag] = s.AggregatorShardKey
		key.Tags[format.AggReplicaTag] = s.AggregatorReplicaKey
	}
	if metricInfo.WithAgentEnvRouteArch {
		key.Tags[format.AgentEnvTag] = aera.AgentEnv
		key.Tags[format.RouteTag] = aera.Route
		key.Tags[format.BuildArchTag] = aera.BuildArch
	}
	item, _ := resolutionShard.GetOrCreateMultiItem(&key, metricInfo, weightMul, nil)
	return item
}
