// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package aggregator

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"pgregory.net/rand"

	"github.com/vkcom/statshouse/internal/agent"
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/metajournal"
	"github.com/vkcom/statshouse/internal/pcache"
	"github.com/vkcom/statshouse/internal/util"
	"github.com/vkcom/statshouse/internal/vkgo/build"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
	"github.com/vkcom/statshouse/internal/vkgo/semaphore"
)

type (
	// Clients take reader lock, then check sending flag, if true, they were late
	// If false, they take shard lock, then aggregate into shard

	metricStat struct {
		total       int
		multipliers int
	}
	// When time tics, ticker takes writer lock, sets sending flag, then releases writer lock
	// After that it can access shards and 100% know no one accesses them
	aggregatorShard struct {
		mu sync.Mutex // Protects items
		data_model.MultiItemMap
		metricStats map[int32]metricStat
	}
	aggregatorBucket struct {
		time   uint32
		shards [data_model.AggregationShardsPerSecond]aggregatorShard

		contributors       map[*rpc.HandlerContext]struct{}                               // Protected by mu, can be removed if client disconnects. SendKeepAlive2 are also here
		contributors3      map[*rpc.HandlerContext]tlstatshouse.SendSourceBucket3Response // Protected by mu, can be removed if client disconnects.
		historicHosts      [2][2]map[int32]int64                                          // [role][route] Protected by mu
		contributorsMetric [2][2]data_model.ItemValue                                     // [role][route] Not recorded for keep-alive, protected by aggregator mutex

		usedMetrics map[int32]struct{}
		mu          sync.Mutex // Protects everything, except shards

		sendMu sync.RWMutex // Used to wait for all aggregating clients to finish before sending

		contributorsSimulatedErrors map[*rpc.HandlerContext]struct{} // put into most future bucket, so receive error after >7 seconds
	}
	Aggregator struct {
		h                 tlstatshouse.Handler
		recentBuckets     []*aggregatorBucket          // We collect into several buckets before sending
		historicBuckets   map[uint32]*aggregatorBucket // timestamp->bucket. Each agent sends not more than X historic buckets, so size is limited.
		bucketsToSend     chan *aggregatorBucket
		mu                sync.Mutex
		server            *rpc.Server
		hostName          []byte
		aggregatorHost    int32
		aggregatorHostTag data_model.TagUnionBytes
		withoutCluster    bool
		shardKey          int32 // never changes after start, can be used without lock
		replicaKey        int32 // never changes after start, can be used without lock
		buildArchTag      int32 // never changes after start, can be used without lock
		startTimestamp    uint32
		addresses         []string

		cancelInsertsFunc context.CancelFunc
		cancelInsertsCtx  context.Context
		insertsSema       *semaphore.Weighted
		insertsSemaSize   int64 // configured value might change

		tagMappingBootstrapResponse []byte // sending large responses to thousands of clients at once, so must be very efficient

		sh2 *agent.Agent // set to not nil some time after launching aggregator

		internalLog []byte // simply flushed every couple seconds

		estimator data_model.Estimator

		// we potentially have 100+ buckets with 20000+ contributors each,
		// so we have to maintain a sum of waiting hosts if we want accurate and fast metric.
		historicHosts [2][2]map[int32]int64 // [role][route] Protected by mu

		recentSenders   int
		historicSenders int

		config ConfigAggregator

		// Remote config
		configR  ConfigAggregatorRemote
		configS  string
		configMu sync.RWMutex

		metricStorage  *metajournal.MetricsStorage
		journal        *metajournal.Journal
		journalFast    *metajournal.JournalFast
		journalCompact *metajournal.JournalFast
		testConnection *TestConnection
		tagsMapper     *TagsMapper
		tagsMapper2    *tagsMapper2
		mappingsCache  *pcache.MappingsCache

		scrape     *scrapeServer
		autoCreate *autoCreate
	}
	BuiltInStatRecord struct {
		Key  data_model.Key
		SKey string
		data_model.ItemValue
	}
)

const aggregatorMaxInflightPackets = (data_model.MaxConveyorDelay + data_model.MaxHistorySendStreams) * 3 * 3 // *3 is additional load for spares, when original aggregator is down

func (b *aggregatorBucket) CancelHijack(hctx *rpc.HandlerContext) {
	b.mu.Lock()
	defer b.mu.Unlock()
	// we cannot remove merged data or merged set, so data remains in buckets
	// cancels are rare, so we simply remove from all 3 maps, we do not know in which map hctx is
	delete(b.contributors, hctx)
	delete(b.contributors3, hctx)
	delete(b.contributorsSimulatedErrors, hctx)
}

// aggregator is also run in this method
func MakeAggregator(dc *pcache.DiskCache, fj *os.File, fjCompact *os.File, mappingsCache *pcache.MappingsCache,
	cacheDir string, listenAddr string, aesPwd string, config ConfigAggregator, hostName string, logTrace bool) (*Aggregator, error) {
	localAddresses := strings.Split(listenAddr, ",")
	if len(localAddresses) != 1 {
		if len(localAddresses) != 3 {
			return nil, fmt.Errorf("you must set exactly one address or three comma separated addresses in --agg-addr")
		}
		if config.LocalReplica < 1 || config.LocalReplica > 3 {
			return nil, fmt.Errorf("seetting three --agg-addr require setting --local-replica to 1, 2 or 3")
		}
		listenAddr = localAddresses[config.LocalReplica-1]
	}

	_, listenPort, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to split --agg-addr (%q) into host and port for autoconfiguration: %v", listenAddr, err)
	}
	if config.ExternalPort == "" {
		config.ExternalPort = listenPort
	}
	var shardKey int32 = 1
	var replicaKey int32 = 1
	var addresses = []string{listenAddr}
	if config.KHAddr != "" {
		shardKey, replicaKey, addresses, err = selectShardReplica(config.KHAddr, config.KHUser, config.KHPassword, config.Cluster, config.ExternalPort)
		if err != nil {
			return nil, fmt.Errorf("failed to find out local shard and replica in cluster %q, probably wrong --cluster command line parameter set: %v", config.Cluster, err)
		}
	}
	withoutCluster := false
	if len(addresses) == 1 { // mostly demo runs with local non-replicated clusters
		if len(localAddresses) == 3 {
			addresses = localAddresses
			replicaKey = int32(config.LocalReplica)
			log.Printf("[warning] running as a local replica %d with single-host cluster, probably demo", replicaKey)
		} else {
			addresses = []string{addresses[0], addresses[0], addresses[0]}
			withoutCluster = true
			log.Printf("[warning] running with single-host cluster, probably demo")
		}
	}
	if len(addresses)%3 != 0 {
		return nil, fmt.Errorf("failed configuration - must have exactly 3 replicas in cluster %q per shard, probably wrong --cluster command line parameter set: %v", config.Cluster, err)
	}
	if config.ShardByMetricShards < 0 || config.ShardByMetricShards > len(addresses)/3 {
		return nil, fmt.Errorf("failed configuration - shard-by-metric-shards %d is outside %d configured shards", config.ShardByMetricShards, len(addresses)/3)
	}
	if config.ShardByMetricShards == 0 {
		config.ShardByMetricShards = len(addresses) / 3
	}
	log.Printf("success autoconfiguration in cluster %q, localShard=%d localReplica=%d address list is (%q)", config.Cluster, shardKey, replicaKey, strings.Join(addresses, ","))

	metadataClient := &tlmetadata.Client{
		Client:  rpc.NewClient(rpc.ClientWithLogf(log.Printf), rpc.ClientWithCryptoKey(aesPwd), rpc.ClientWithTrustedSubnetGroups(build.TrustedSubnetGroups())),
		Network: config.MetadataNet,
		Address: config.MetadataAddr,
		ActorID: config.MetadataActorID,
	}

	// we do not try several times, because admin must quickly learn aggregator exited
	// if we try forever, admin might think aggregator is running, while it is not
	// but for local run, we want to run in wrong order, aggregator first, metadata second
	tagMappingBootstrapResponse, err := loadBoostrap(dc, metadataClient)
	if err != nil {
		if !withoutCluster {
			// in prod, running without bootstrap is dangerous and can leave large gap in all metrics
			// if agent disk caches are erased
			return nil, fmt.Errorf("failed to load mapping bootstrap: %v", err)
		}
		// ok, empty bootstrap is good for us for running locally
		tagMappingBootstrapResponse, _ = (&tlmetadata.GetTagMappingBootstrap{}).WriteResult(nil, tlstatshouse.GetTagMappingBootstrapResult{})
	}

	cancelInsertCtx, cancelInsertFunc := context.WithCancel(context.Background())

	a := &Aggregator{
		cancelInsertsCtx:            cancelInsertCtx,
		cancelInsertsFunc:           cancelInsertFunc,
		bucketsToSend:               make(chan *aggregatorBucket),
		historicBuckets:             map[uint32]*aggregatorBucket{},
		historicHosts:               [2][2]map[int32]int64{{map[int32]int64{}, map[int32]int64{}}, {map[int32]int64{}, map[int32]int64{}}},
		config:                      config,
		configR:                     config.ConfigAggregatorRemote,
		hostName:                    format.ForceValidStringValue(hostName), // worse alternative is do not run at all
		withoutCluster:              withoutCluster,
		shardKey:                    shardKey,
		replicaKey:                  replicaKey,
		buildArchTag:                format.GetBuildArchKey(runtime.GOARCH),
		addresses:                   addresses,
		tagMappingBootstrapResponse: tagMappingBootstrapResponse,
		mappingsCache:               mappingsCache,
	}
	errNoAutoCreate := &rpc.Error{Code: data_model.RPCErrorNoAutoCreate}
	a.h = tlstatshouse.Handler{
		GetConfig2: a.handleGetConfig2,
		RawGetConfig3: func(ctx context.Context, hctx *rpc.HandlerContext) error {
			return a.handleGetConfig3(ctx, hctx)
		},
		RawGetMetrics3: a.handleGetMetrics3,
		RawGetTagMapping2: func(ctx context.Context, hctx *rpc.HandlerContext) error {
			return a.tagsMapper.handleCreateTagMapping(ctx, hctx)
		},
		RawGetTagMappingBootstrap: func(_ context.Context, hctx *rpc.HandlerContext) error {
			hctx.Response = append(hctx.Response, a.tagMappingBootstrapResponse...)
			return nil
		},
		RawSendKeepAlive2:    a.handleSendKeepAlive2,
		RawSendKeepAlive3:    a.handleSendKeepAlive3,
		RawSendSourceBucket2: a.handleSendSourceBucket2,
		RawSendSourceBucket3: a.handleSendSourceBucket3,
		RawTestConnection2: func(ctx context.Context, hctx *rpc.HandlerContext) error {
			return a.testConnection.handleTestConnection(ctx, hctx)
		},
		RawGetTargets2: func(ctx context.Context, hctx *rpc.HandlerContext) error {
			return a.scrape.handleGetTargets(ctx, hctx)
		},
		RawAutoCreate: func(ctx context.Context, hctx *rpc.HandlerContext) error {
			if a.autoCreate != nil {
				return a.autoCreate.handleAutoCreate(ctx, hctx)
			} else {
				return errNoAutoCreate
			}
		},
	}
	if len(a.hostName) == 0 {
		return nil, fmt.Errorf("failed configuration - aggregator machine must have valid non-empty host name")
	}
	metrics := util.NewRPCServerMetrics("statshouse_aggregator")
	a.scrape = newScrapeServer()
	a.server = rpc.NewServer(rpc.ServerWithCryptoKeys([]string{aesPwd}),
		rpc.ServerWithLogf(log.Printf),
		rpc.ServerWithMaxWorkers(-1),
		rpc.ServerWithSyncHandler(a.handleClient),
		rpc.ServerWithDisableContextTimeout(true),
		rpc.ServerWithTrustedSubnetGroups(build.TrustedSubnetGroups()),
		rpc.ServerWithVersion(build.Info()),
		rpc.ServerWithDefaultResponseTimeout(data_model.MaxConveyorDelay*time.Second),
		rpc.ServerWithMaxInflightPackets(aggregatorMaxInflightPackets),
		rpc.ServerWithResponseBufSize(1024),
		rpc.ServerWithResponseMemEstimate(1024),
		rpc.ServerWithRequestMemoryLimit(2<<33),
		rpc.ServerWithStatsHandler(a.scrape.reportStats),
		metrics.ServerWithMetrics,
	)
	// 1. we do not bother to stop collection
	// 2. we must not use statshouse lib in aggregator, there is nobody listening 13337
	// _ = metrics.Run(a.server)
	metricMetaLoader := metajournal.NewMetricMetaLoader(metadataClient, metajournal.DefaultMetaTimeout)
	if config.AutoCreate {
		a.autoCreate = newAutoCreate(a, metadataClient, config.AutoCreateDefaultNamespace)
	}
	a.metricStorage = metajournal.MakeMetricsStorage(func(configID int32, configS string) {
		a.scrape.applyConfig(configID, configS)
		if a.autoCreate != nil {
			a.autoCreate.applyConfig(configID, configS)
		}
	})
	a.journal = metajournal.MakeJournal(a.config.Cluster, data_model.JournalDDOSProtectionTimeout, dc,
		[]metajournal.ApplyEvent{a.metricStorage.ApplyEvent})
	// we ignore errors because cache can be damaged
	a.journalFast, _ = metajournal.LoadJournalFastFile(fj, data_model.JournalDDOSProtectionTimeout, false,
		nil)
	a.journalFast.SetDumpPathPrefix(filepath.Join(cacheDir, fmt.Sprintf("journal-%s", config.Cluster)))
	a.journalCompact, _ = metajournal.LoadJournalFastFile(fjCompact, data_model.JournalDDOSProtectionTimeout, true,
		nil)
	a.journalCompact.SetDumpPathPrefix(filepath.Join(cacheDir, fmt.Sprintf("journal-compact-%s", config.Cluster)))
	agentConfig := agent.DefaultConfig()
	agentConfig.Cluster = a.config.Cluster
	// We use agent instance for aggregator built-in metrics
	getConfigResult := a.getConfigResult3() // agent will use this config instead of getting via RPC, because our RPC is not started yet
	sh2, err := agent.MakeAgent("tcp4", cacheDir, aesPwd, agentConfig, hostName,
		format.TagValueIDComponentAggregator,
		a.metricStorage, mappingsCache,
		a.journal.VersionHash, a.journalFast.VersionHash, a.journalCompact.VersionHash,
		log.Printf, a.agentBeforeFlushBucketFunc, &getConfigResult, nil)
	if err != nil {
		return nil, fmt.Errorf("built-in agent failed to start: %v", err)
	}
	a.sh2 = sh2
	a.scrape.run(a.metricStorage, metricMetaLoader, sh2)
	if a.autoCreate != nil {
		a.autoCreate.run(a.metricStorage)
	}
	a.journal.Start(a.sh2, a.appendInternalLog, metricMetaLoader.LoadJournal)
	a.journalFast.Start(a.sh2, a.appendInternalLog, metricMetaLoader.LoadJournal)
	a.journalCompact.Start(a.sh2, a.appendInternalLog, metricMetaLoader.LoadJournal)

	a.testConnection = MakeTestConnection()
	a.tagsMapper = NewTagsMapper(a, a.sh2, a.metricStorage, dc, metricMetaLoader, a.config.Cluster)
	a.tagsMapper2 = NewTagsMapper2(a, a.sh2, a.metricStorage, metricMetaLoader)

	a.aggregatorHost = a.tagsMapper.mapTagAtStartup(a.hostName, format.BuiltinMetricMetaBudgetAggregatorHost.Name)
	a.aggregatorHostTag = data_model.TagUnionBytes{I: a.aggregatorHost}

	a.estimator.Init(config.CardinalityWindow, a.config.MaxCardinality/len(addresses))

	now := time.Now()
	a.startTimestamp = uint32(now.Unix())
	_ = a.advanceRecentBuckets(now, true) // Just create initial set of buckets and set LastHour
	a.appendInternalLog("start", "", build.Commit(), build.Info(), strings.Join(os.Args[1:], " "), strings.Join(addresses, ","), "", "Started")

	a.insertsSemaSize = int64(a.config.RecentInserters)
	a.insertsSema = semaphore.NewWeighted(a.insertsSemaSize)
	_ = a.insertsSema.Acquire(context.Background(), a.insertsSemaSize)

	go a.tagsMapper2.goRun()
	go a.goTicker()
	for i := 0; i < a.config.RecentInserters; i++ {
		go a.goInsert(a.insertsSema, a.cancelInsertsCtx, a.bucketsToSend, i)
	}
	go a.goInternalLog()

	go func() { // before sh2.Run because agent will also connect to local aggregator
		_ = a.server.ListenAndServe("tcp4", listenAddr)
	}()

	sh2.Run(a.aggregatorHost, a.shardKey, a.replicaKey)

	go func() {
		for {
			time.Sleep(time.Hour) // arbitrary
			_ = mappingsCache.Save()
			a.SaveJournals()
		}
	}()

	return a, nil
}

func (a *Aggregator) SaveJournals() {
	_ = a.journalFast.Save()
	_ = a.journalCompact.Save()
}

func (a *Aggregator) Agent() *agent.Agent {
	return a.sh2
}

// Also effectively disables all incoming data
func (a *Aggregator) DisableNewInsert() {
	a.mu.Lock()
	close(a.bucketsToSend)
	a.bucketsToSend = nil
	a.mu.Unlock()
}

func (a *Aggregator) WaitInsertsFinish(timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := a.insertsSema.Acquire(ctx, a.insertsSemaSize); err != nil {
		log.Printf("WaitInsertsFinish timeout after %v: %v", timeout, err)
	}
	// either timeout passes or all recent senders quit
}

func (a *Aggregator) ShutdownRPCServer() {
	a.server.Shutdown()
}

func (a *Aggregator) WaitRPCServer(timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := a.server.CloseWait(ctx); err != nil {
		log.Printf("WaitRPCServer timeout after %v: %v", timeout, err)
	}
}

func loadBoostrap(dc *pcache.DiskCache, client *tlmetadata.Client) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second) // TODO - timeout
	defer cancel()
	args := tlmetadata.GetTagMappingBootstrap{}
	var ret tlstatshouse.GetTagMappingBootstrapResult
	if err := client.GetTagMappingBootstrap(ctx, args, nil, &ret); err != nil {
		if dc == nil {
			return nil, fmt.Errorf("bootstrap data failed to load, and no disk cache configured: %w", err)
		}
		cacheData, _, _, errDiskCache, ok := dc.Get(data_model.BootstrapDiskNamespace, "")
		if errDiskCache != nil {
			return nil, fmt.Errorf("bootstrap data failed to load with error %v, and failed to get from disk cache: %w", err, errDiskCache)
		}
		if !ok {
			return nil, fmt.Errorf("bootstrap data failed to load, and not in disk cache: %w", err)
		}
		log.Printf("Loaded bootstrap mappings from cache of size %d", len(cacheData))
		return cacheData, nil // from cache
	}
	cacheData, err := args.WriteResult(nil, ret)
	if err != nil {
		log.Printf("failed to serialize bootstrap of %d mappings", len(ret.Mappings))
	} else if dc != nil {
		if err := dc.Set(data_model.BootstrapDiskNamespace, "", cacheData, time.Now(), 0); err != nil {
			log.Printf("failed to store bootstrap of %d mappings of size %d in disk cache", len(ret.Mappings), len(cacheData))
		}
	}
	log.Printf("Loaded bootstrap of %d mappings of size %d", len(ret.Mappings), len(cacheData))
	return cacheData, nil
}

func (b *aggregatorBucket) contributorsCount() float64 {
	return b.contributorsMetric[0][0].Count() + b.contributorsMetric[0][1].Count() +
		b.contributorsMetric[1][0].Count() + b.contributorsMetric[1][1].Count()
}

func (b *aggregatorBucket) lockShard(lockedShard *int, sID int, measurementLocks *int) *aggregatorShard {
	if *lockedShard == sID {
		return &b.shards[sID]
	}
	if *lockedShard != -1 {
		b.shards[*lockedShard].mu.Unlock()
		*lockedShard = -1
	}
	if sID != -1 {
		*measurementLocks++
		b.shards[sID].mu.Lock()
		*lockedShard = sID
		return &b.shards[sID]
	}
	return nil
}

func addrIPString(remoteAddr net.Addr) (uint32, string) {
	// ipv4 bytes, or ipv6 lower 4 bytes
	switch addr := remoteAddr.(type) {
	case *net.UDPAddr:
		var v uint32
		if len(addr.IP) >= 4 {
			v = binary.BigEndian.Uint32(addr.IP[len(addr.IP)-4:])
		}
		return v, addr.IP.String()
	case *net.TCPAddr:
		var v uint32
		if len(addr.IP) >= 4 {
			v = binary.BigEndian.Uint32(addr.IP[len(addr.IP)-4:])
		}
		return v, addr.IP.String()
	default:
		return 0, addr.String()
	}
}

func (a *Aggregator) agentBeforeFlushBucketFunc(_ *agent.Agent, nowUnix uint32) {
	rng := rand.New()
	a.scrape.reportConfigHash(nowUnix)

	a.mu.Lock()
	recentSenders := a.recentSenders
	historicSends := a.historicSenders
	var bucketsWaiting [2][2]data_model.ItemValue
	var secondsWaiting [2][2]data_model.ItemValue
	var hostsWaiting [2][2]data_model.ItemValue
	for _, v := range a.historicBuckets {
		for i, cc := range v.contributorsMetric {
			for j, bb := range cc {
				// v.contributorsOriginal and v.contributorsSpare are counters, while ItemValues above are values
				bucketsWaiting[i][j].AddValueCounterHost(rng, float64(nowUnix-v.time), bb.Count(), bb.MaxCounterHostTag)
				if bb.Count() > 0 {
					secondsWaiting[i][j].AddValueCounterHost(rng, float64(nowUnix-v.time), 1, bb.MaxCounterHostTag)
				}
			}
		}
	}
	for i, cc := range a.historicHosts {
		for j, bb := range cc {
			for h := range bb { // random sample host every second is very good for max_host combobox under plot
				hostsWaiting[i][j].AddValueCounterHost(rng, float64(len(bb)), 1, data_model.TagUnionBytes{I: h})
				break
			}
		}
	}
	a.mu.Unlock()

	writeWaiting := func(metricInfo *format.MetricMetaValue, item *[2][2]data_model.ItemValue) {
		tagsRole := [2]int32{format.TagValueIDAggregatorOriginal, format.TagValueIDAggregatorSpare}
		tagsRoute := [2]int32{format.TagValueIDRouteDirect, format.TagValueIDRouteIngressProxy}
		for i, cc := range *item {
			for j, bb := range cc {
				a.sh2.MergeItemValue(nowUnix, metricInfo,
					[]int32{0, 0, 0, 0, tagsRole[i], tagsRoute[j]}, &bb)
			}
		}
	}
	writeWaiting(format.BuiltinMetricMetaAggHistoricBucketsWaiting, &bucketsWaiting)
	writeWaiting(format.BuiltinMetricMetaAggHistoricSecondsWaiting, &secondsWaiting)
	writeWaiting(format.BuiltinMetricMetaAggHistoricHostsWaiting, &hostsWaiting)

	a.sh2.AddValueCounterHost(nowUnix, format.BuiltinMetricMetaAggActiveSenders,
		[]int32{0, 0, 0, 0, format.TagValueIDConveyorRecent},
		float64(recentSenders), 1, a.aggregatorHostTag)
	a.sh2.AddValueCounterHost(nowUnix, format.BuiltinMetricMetaAggActiveSenders,
		[]int32{0, 0, 0, 0, format.TagValueIDConveyorHistoric},
		float64(historicSends), 1, a.aggregatorHostTag)

	a.sh2.AddValueCounterHost(nowUnix, format.BuiltinMetricMetaMappingQueueSize,
		[]int32{},
		float64(a.tagsMapper2.UnknownTagsLen()), 1, a.aggregatorHostTag)
	/* TODO - replace with direct agent call

	a.metricStorage.MetricsMu.Lock()
	a.moveBuiltinMetricLocked(&a.metricStorage.BuiltinLongPollImmediateOK, data_model.AggKey(nowUnix, format.BuiltinMetricIDAggMapping, [16]int32{0, 0, 0, 0, format.TagValueIDAggMappingMetaMetrics, format.TagValueIDAggMappingStatusImmediateOK}, aggHost, a.shardKey, a.replicaKey))
	a.moveBuiltinMetricLocked(&a.metricStorage.BuiltinLongPollImmediateError, data_model.AggKey(nowUnix, format.BuiltinMetricIDAggMapping, [16]int32{0, 0, 0, 0, format.TagValueIDAggMappingMetaMetrics, format.TagValueIDAggMappingStatusImmediateErr}, aggHost, a.shardKey, a.replicaKey))
	a.moveBuiltinMetricLocked(&a.metricStorage.BuiltinLongPollEnqueue, data_model.AggKey(nowUnix, format.BuiltinMetricIDAggMapping, [16]int32{0, 0, 0, 0, format.TagValueIDAggMappingMetaMetrics, format.TagValueIDAggMappingStatusEnqueued}, aggHost, a.shardKey, a.replicaKey))
	a.moveBuiltinMetricLocked(&a.metricStorage.BuiltinLongPollDelayedOK, data_model.AggKey(nowUnix, format.BuiltinMetricIDAggMapping, [16]int32{0, 0, 0, 0, format.TagValueIDAggMappingMetaMetrics, format.TagValueIDAggMappingStatusDelayedOK}, aggHost, a.shardKey, a.replicaKey))
	a.moveBuiltinMetricLocked(&a.metricStorage.BuiltinLongPollDelayedError, data_model.AggKey(nowUnix, format.BuiltinMetricIDAggMapping, [16]int32{0, 0, 0, 0, format.TagValueIDAggMappingMetaMetrics, format.TagValueIDAggMappingStatusDelayedErr}, aggHost, a.shardKey, a.replicaKey))
	a.moveBuiltinMetricLocked(&a.metricStorage.BuiltinJournalUpdateOK, data_model.AggKey(nowUnix, format.BuiltinMetricIDAggMapping, [16]int32{0, 0, 0, 0, format.TagValueIDAggMappingJournalUpdate, format.TagValueIDAggMappingStatusImmediateOK}, aggHost, a.shardKey, a.replicaKey))
	a.moveBuiltinMetricLocked(&a.metricStorage.BuiltinJournalUpdateError, data_model.AggKey(nowUnix, format.BuiltinMetricIDAggMapping, [16]int32{0, 0, 0, 0, format.TagValueIDAggMappingJournalUpdate, format.TagValueIDAggMappingStatusImmediateErr}, aggHost, a.shardKey, a.replicaKey))
	a.metricStorage.MetricsMu.Unlock()
	*/
}

func (a *Aggregator) checkShardConfigurationTotal(shardReplicaTotal int32) error {
	if int(shardReplicaTotal) == len(a.addresses) {
		return nil
	}
	if a.config.PreviousNumShards != 0 && int(shardReplicaTotal) == a.config.PreviousNumShards {
		return nil
	}
	return fmt.Errorf("statshouse misconfiguration! shard*replicas total sent by source (%d) does not match shard*replicas total of aggregator (%d) or --previous-shards (%d)", shardReplicaTotal, len(a.addresses), a.config.PreviousNumShards)
}

func (a *Aggregator) checkShardConfiguration(shardReplica int32, shardReplicaTotal int32) error {
	if err := a.checkShardConfigurationTotal(shardReplicaTotal); err != nil {
		return err
	}
	if a.withoutCluster { // No checks for local testing, when config.Replica == ""
		return nil
	}
	ourShardReplica := int((a.shardKey-1)*3 + (a.replicaKey - 1)) // shardKey is 1 for shard 0
	if int(shardReplica) != ourShardReplica {
		return fmt.Errorf("statshouse misconfiguration! shard*replica sent by source (%d) does not match shard*replica expected by aggregator (%d) with shard:replica %d:%d", shardReplica, ourShardReplica, a.shardKey, a.replicaKey)
	}
	return nil
}

func selectShardReplica(khAddr, khUser, khPassword string, cluster string, listenPort string) (shardKey int32, replicaKey int32, addresses []string, err error) {
	log.Printf("[debug] starting autoconfiguration by making SELECT to clickhouse address %q", khAddr)
	httpClient := makeHTTPClient()
	backoffTimeout := time.Duration(0)
	for i := 0; ; i++ {
		// motivation for several attempts is random clickhouse errors, plus starting before clickhouse is ready to process requests in demo mode
		shardKey, replicaKey, addresses, err = selectShardReplicaImpl(httpClient, khAddr, khUser, khPassword, cluster, listenPort)
		if err == nil || i >= data_model.ClickhouseConfigRetries {
			return
		}
		backoffTimeout = data_model.NextBackoffDuration(backoffTimeout)
		log.Printf("[error] failed to read configuration, will retry in %v: %v", backoffTimeout, err)
		time.Sleep(backoffTimeout)
	}
}

func selectShardReplicaImpl(httpClient *http.Client, khAddr, khUser, khPassword string, cluster string, listenPort string) (shardKey int32, replicaKey int32, addresses []string, err error) {
	// We assume replicas 4+ are for readers only
	queryPrefix := url.PathEscape(fmt.Sprintf("SELECT shard_num, replica_num, is_local, host_name FROM system.clusters where cluster='%s' and replica_num <= 3 order by shard_num, replica_num", cluster))
	URL := fmt.Sprintf("http://%s/?input_format_values_interpret_expressions=0&query=%s", khAddr, queryPrefix)

	ctx, cancel := context.WithTimeout(context.Background(), data_model.ClickHouseTimeoutConfig)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "POST", URL, nil)
	if err != nil {
		return 0, 0, nil, err
	}
	if khUser != "" {
		req.Header.Add("X-ClickHouse-User", khUser)
	}
	if khPassword != "" {
		req.Header.Add("X-ClickHouse-Key", khPassword)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return 0, 0, nil, fmt.Errorf("HTTP get to clickhouse %q failed - %v", khAddr, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, 0, nil, fmt.Errorf("HTTP get to clickhouse %q returned bad status - %d", khAddr, resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, 0, nil, fmt.Errorf("HTTP reading get body from clickhouse %q failed - %v", khAddr, err)
	}
	lines := strings.Split(strings.TrimSpace(string(body)), "\n")
	for _, line := range lines {
		values := strings.Split(line, "\t")
		if len(values) != 4 {
			return 0, 0, nil, fmt.Errorf("HTTP get from clickhouse %q for cluster %q returned unexpected body - %q", khAddr, cluster, string(body))
		}
		shard, err := strconv.ParseInt(values[0], 10, 32)
		if err != nil {
			return 0, 0, nil, fmt.Errorf("HTTP reading shard from %q for cluster %q failed - %v", khAddr, cluster, err)
		}
		replica, err := strconv.ParseInt(values[1], 10, 32)
		if err != nil {
			return 0, 0, nil, fmt.Errorf("HTTP reading replica from %q for cluster %q failed - %v", khAddr, cluster, err)
		}
		isLocal, err := strconv.ParseInt(values[2], 10, 32)
		if err != nil {
			return 0, 0, nil, fmt.Errorf("HTTP reading is_local from %q for cluster %q failed - %v", khAddr, cluster, err)
		}
		if (shard-1)*3+(replica-1) != int64(len(addresses)) {
			return 0, 0, nil, fmt.Errorf("HTTP get from clickhouse %q for cluster %q returned unexpected body - %q", khAddr, cluster, string(body))
		}
		addresses = append(addresses, net.JoinHostPort(values[3], listenPort))
		if isLocal == 1 {
			shardKey = int32(shard)
			replicaKey = int32(replica)
		}
	}
	if shardKey != 0 && replicaKey != 0 {
		return shardKey, replicaKey, addresses, nil
	}
	return 0, 0, nil, fmt.Errorf("HTTP get from clickhouse %q for cluster %q returned body with no local replicas - %q", khAddr, cluster, string(body))
}

func (a *Aggregator) updateHistoricHostLocked(my map[int32]int64, del map[int32]int64) {
	for k, v := range del {
		value := my[k] - v
		if value == 0 {
			delete(my, k)
		} else {
			my[k] = value
		}
	}
}

func (a *Aggregator) updateHistoricHostsLocked(my [2][2]map[int32]int64, del [2][2]map[int32]int64) {
	for i, m1 := range my {
		for j, m2 := range m1 {
			a.updateHistoricHostLocked(m2, del[i][j])
		}
	}
}

func (a *Aggregator) goInsert(insertsSema *semaphore.Weighted, cancelCtx context.Context, bucketsToSend chan *aggregatorBucket, senderID int) {
	defer log.Printf("clickhouse inserter %d quit", senderID)
	defer insertsSema.Release(1)

	rnd := rand.New()
	httpClient := makeHTTPClient()
	var buffers data_model.SamplerBuffers
	var aggBuckets []*aggregatorBucket
	var bodyStorage []byte

	for aggBucket := range bucketsToSend {
		aggBuckets = aggBuckets[:0]
		bodyStorage = bodyStorage[:0]

		nowUnix := uint32(time.Now().Unix())
		a.mu.Lock()
		oldestTime := a.recentBuckets[0].time
		newestTime := a.recentBuckets[len(a.recentBuckets)-1].time
		willInsertHistoric := (a.recentSenders+a.historicSenders) < a.config.InsertHistoricWhen &&
			a.historicSenders < a.config.HistoricInserters &&
			len(a.historicBuckets) != 0
		if willInsertHistoric {
			a.historicSenders++
		} else {
			a.recentSenders++
		}
		a.mu.Unlock()

		aggBuckets = append(aggBuckets, aggBucket) // first bucket is always recent
		a.estimator.ReportHourCardinality(rnd, aggBucket.time, &aggBucket.shards[0].MultiItemMap, aggBucket.usedMetrics, a.aggregatorHost, a.shardKey, a.replicaKey, len(a.addresses))

		recentContributors := aggBucket.contributorsCount()
		historicContributors := 0.0
		maxHistoricInsertBatch := data_model.MaxHistorySendStreams / (1 + a.config.HistoricInserters)
		// each historic inserter takes not more than maxHistoricInsertBatch the oldest buckets, so for example with 2 inserters
		// [a, b, c, d, e, f]               <- this is 6 seconds sent by agent and waiting in historicBuckets to be inserted
		//       [c, d, e, f]               <- first inserter takes [a, b] and starts inserting
		//             [e, f]               <- second inserter takes [c, d] and starts inserting
		// Client sends no more historic seconds because it did not receive responses yet.
		// As soon as one of the inserters finish, it sends back responses and there must be 2 seconds available without delay.
		//                  []              <- inserter takes [e, f] and starts inserting, meanwhile agent receives 2 responses and sends 2 more seconds
		//                  [g, h]          <- so that when the other inserter finishes, more 2 seconds will be available.
		// So, we have enough seconds always to perform smooth rolling insert.
		// In case both inserters finish at the same time, this rolling algorithm will perform non-ideal insert, but that is good enough for us.
		// Note: Each historic second in the diagram is aggregation of many agents , each one receiving copy of the response
		// Note: In the worst case, amount of memory is approx. MaxHistorySendStreams * agent insert budget per shard * # of agents

		for willInsertHistoric && len(aggBuckets) < 1+maxHistoricInsertBatch {
			historicBucket, staleBuckets := a.popOldestHistoricBucket(oldestTime)
			for _, b := range staleBuckets {
				b.mu.Lock()
				for hctx := range b.contributors {
					var ssb2 tlstatshouse.SendSourceBucket2 // Dummy
					hctx.Response, _ = ssb2.WriteResult(hctx.Response, "Successfully discarded historic bucket with timestamp before historic window")
					hctx.SendHijackedResponse(nil)
				}
				for hctx, resp := range b.contributors3 {
					var ssb3 tlstatshouse.SendSourceBucket3 // Dummy
					resp.Warning = "Successfully discarded historic bucket with timestamp before historic window"
					resp.SetDiscard(true)
					hctx.Response, _ = ssb3.WriteResult(hctx.Response, resp)
					hctx.SendHijackedResponse(nil)
				}
				clear(b.contributors) // safeguard against sending more than once
				clear(b.contributors3)
				historicHosts := b.historicHosts
				b.mu.Unlock()
				a.mu.Lock()
				a.updateHistoricHostsLocked(a.historicHosts, historicHosts)
				a.mu.Unlock()
				a.sh2.AddValueCounterHost(nowUnix, format.BuiltinMetricMetaTimingErrors,
					[]int32{0, format.TagValueIDTimingLongWindowThrownAggregatorLater},
					float64(newestTime-b.time), 1, a.aggregatorHostTag) // This bucket is combination of many hosts
			}
			if historicBucket == nil {
				break
			}
			historicContributors += historicBucket.contributorsCount()

			aggBuckets = append(aggBuckets, historicBucket)
			a.estimator.ReportHourCardinality(rnd, historicBucket.time, &historicBucket.shards[0].MultiItemMap, historicBucket.usedMetrics, a.aggregatorHost, a.shardKey, a.replicaKey, len(a.addresses))

			if historicContributors > (recentContributors-0.5)*data_model.MaxHistoryInsertContributorsScale {
				// We cannot compare buckets by size, because we can have very little data now, while waiting historic buckets are large
				// But number of contributors is more or less stable, so comparison is valid.
				// As # of contributors fluctuates, we want to stop when # of historic contributors overshoot 3.5
				// Otherwise, we have a good chance to insert 5 historic buckets instead of 4
				break
			}
		}
		a.configMu.RLock()
		mirrorChWrite := a.configR.MirrorChWrite
		writeToV3First := a.configR.WriteToV3First
		v2InsertSettings := a.configR.V2InsertSettings
		v3InsertSettings := a.configR.V3InsertSettings
		a.configMu.RUnlock()
		insertErrTable := func(v3Format bool) string {
			if v3Format {
				return "insert_error_exp_table"
			}
			return "insert_error"
		}

		var marshalDur time.Duration
		var stats insertStats
		bodyStorage, buffers, stats, marshalDur = a.RowDataMarshalAppendPositions(aggBuckets, buffers, rnd, bodyStorage[:0], writeToV3First)

		// Never empty, because adds value stats
		ctx, cancelSendToCh := context.WithTimeout(cancelCtx, data_model.ClickHouseTimeoutInsert)
		settings := v2InsertSettings
		if writeToV3First {
			settings = v3InsertSettings
		}
		status, exception, dur, sendErr := sendToClickhouse(ctx, httpClient, a.config.KHAddr, a.config.KHUser, a.config.KHPassword, getTableDesc(writeToV3First), bodyStorage, settings)
		// if we are mirriring that will happen after second ch write
		if !mirrorChWrite {
			cancelSendToCh()
		}

		a.mu.Lock()
		if willInsertHistoric {
			a.historicSenders--
		} else {
			a.recentSenders--
		}
		a.mu.Unlock()

		if sendErr != nil {
			comment := fmt.Sprintf("time=%d (delta = %d), contributors (recent %v, historic %v) Sender %d", aggBucket.time, int64(nowUnix)-int64(aggBucket.time), recentContributors, historicContributors, senderID)
			a.appendInternalLog(insertErrTable(writeToV3First), "", strconv.Itoa(status), strconv.Itoa(exception), "statshouse_value_incoming_arg_min_max", "", comment, sendErr.Error())
			log.Print(sendErr)
			sendErr = &rpc.Error{
				Code:        data_model.RPCErrorInsert,
				Description: sendErr.Error(),
			}
		}

		for i, b := range aggBuckets {
			b.mu.Lock()
			for hctx := range b.contributors {
				var ssb2 tlstatshouse.SendSourceBucket2 // Dummy
				hctx.Response, _ = ssb2.WriteResult(hctx.Response, "Dummy historic result")
				hctx.SendHijackedResponse(sendErr)
			}
			for hctx, resp := range b.contributors3 {
				var ssb3 tlstatshouse.SendSourceBucket3 // Dummy
				if sendErr != nil {
					resp.Warning = sendErr.Error()
				}
				resp.SetDiscard(sendErr == nil)
				hctx.Response, _ = ssb3.WriteResult(hctx.Response, resp)
				hctx.SendHijackedResponse(nil)
			}
			clear(b.contributors) // safeguard against sending more than once
			clear(b.contributors3)
			historicHosts := b.historicHosts
			b.mu.Unlock()
			a.mu.Lock()
			a.updateHistoricHostsLocked(a.historicHosts, historicHosts)
			a.mu.Unlock()
			is := stats.sizes[b.time]
			a.reportInsertMetric(b.time, format.BuiltinMetricMetaAggInsertSize, i != 0, sendErr, status, exception, writeToV3First, format.TagValueIDSizeCounter, float64(is.counters))
			a.reportInsertMetric(b.time, format.BuiltinMetricMetaAggInsertSize, i != 0, sendErr, status, exception, writeToV3First, format.TagValueIDSizeValue, float64(is.values))
			a.reportInsertMetric(b.time, format.BuiltinMetricMetaAggInsertSize, i != 0, sendErr, status, exception, writeToV3First, format.TagValueIDSizePercentiles, float64(is.percentiles))
			a.reportInsertMetric(b.time, format.BuiltinMetricMetaAggInsertSize, i != 0, sendErr, status, exception, writeToV3First, format.TagValueIDSizeUnique, float64(is.uniques))
			a.reportInsertMetric(b.time, format.BuiltinMetricMetaAggInsertSize, i != 0, sendErr, status, exception, writeToV3First, format.TagValueIDSizeStringTop, float64(is.stringTops))
			a.reportInsertMetric(b.time, format.BuiltinMetricMetaAggInsertTime, i != 0, sendErr, status, exception, writeToV3First, 0, dur.Seconds())
		}
		// insert of all buckets is also accounted into single event at aggBucket.time second, so the graphic will be smoother
		a.reportInsertMetric(aggBucket.time, format.BuiltinMetricMetaAggInsertSizeReal, willInsertHistoric, sendErr, status, exception, writeToV3First, 0, float64(len(bodyStorage)))
		a.reportInsertMetric(aggBucket.time, format.BuiltinMetricMetaAggInsertTimeReal, willInsertHistoric, sendErr, status, exception, writeToV3First, 0, dur.Seconds())
		a.reportInsertMetric(aggBucket.time, format.BuiltinMetricMetaAggSamplingTime, willInsertHistoric, sendErr, status, exception, writeToV3First, 0, marshalDur.Seconds())
		tableTag := int32(format.TagValueIDAggInsertV2)
		if writeToV3First {
			tableTag = format.TagValueIDAggInsertV3
		}
		statusTag := int32(format.TagValueIDStatusOK)
		if sendErr != nil {
			statusTag = format.TagValueIDStatusError
		}
		st := []int32{0, stats.historicTag, statusTag, tableTag}
		a.sh2.AddValueCounterHost(stats.recentTs, format.BuiltinMetricMetaAggSamplingMetricCount, st, float64(stats.samplingMetricCount), 1, a.aggregatorHostTag)
		a.sh2.AddValueCounterHost(stats.recentTs, format.BuiltinMetricMetaAggSamplingBudget, st, float64(stats.samplingBudget), 1, a.aggregatorHostTag)
		a.sh2.AddValueCounterHost(stats.recentTs, format.BuiltinMetricMetaAggContributors, []int32{0, statusTag, tableTag}, float64(stats.contributors), 1, a.aggregatorHostTag)
		for sk, ss := range stats.sampling {
			keepTags := []int32{0, stats.historicTag, format.TagValueIDSamplingDecisionKeep, sk.namespeceId, sk.groupId, 0, statusTag, tableTag}
			discardTags := []int32{0, stats.historicTag, format.TagValueIDSamplingDecisionDiscard, sk.namespeceId, sk.groupId, 0, statusTag, tableTag}
			groupBudgetTags := []int32{0, stats.historicTag, sk.namespeceId, sk.groupId, statusTag, tableTag}
			a.sh2.MergeItemValue(stats.recentTs, format.BuiltinMetricMetaAggSamplingSizeBytes, keepTags, &ss.sampligSizeKeepBytes)
			a.sh2.MergeItemValue(stats.recentTs, format.BuiltinMetricMetaAggSamplingSizeBytes, discardTags, &ss.sampligSizeDiscardBytes)
			a.sh2.MergeItemValue(stats.recentTs, format.BuiltinMetricMetaAggSamplingGroupBudget, groupBudgetTags, &ss.samplingGroupBudget)
		}
		a.sh2.AddValueCounterHost(stats.recentTs, format.BuiltinMetricMetaAggSamplingEngineTime, []int32{0, 1, 0, 0, stats.historicTag, statusTag, tableTag}, stats.sampleTimeAppend, 1, a.aggregatorHostTag)
		a.sh2.AddValueCounterHost(stats.recentTs, format.BuiltinMetricMetaAggSamplingEngineTime, []int32{0, 2, 0, 0, stats.historicTag, statusTag, tableTag}, stats.sampleTimePartition, 1, a.aggregatorHostTag)
		a.sh2.AddValueCounterHost(stats.recentTs, format.BuiltinMetricMetaAggSamplingEngineTime, []int32{0, 3, 0, 0, stats.historicTag, statusTag, tableTag}, stats.sampleTimeBudgeting, 1, a.aggregatorHostTag)
		a.sh2.AddValueCounterHost(stats.recentTs, format.BuiltinMetricMetaAggSamplingEngineTime, []int32{0, 4, 0, 0, stats.historicTag, statusTag, tableTag}, stats.sampleTimeSampling, 1, a.aggregatorHostTag)
		a.sh2.AddValueCounterHost(stats.recentTs, format.BuiltinMetricMetaAggSamplingEngineTime, []int32{0, 5, 0, 0, stats.historicTag, statusTag, tableTag}, stats.sampleTimeMetricMeta, 1, a.aggregatorHostTag)
		a.sh2.AddCounterHost(stats.recentTs, format.BuiltinMetricMetaAggSamplingEngineKeys, []int32{0, 0, 0, 0, stats.historicTag, statusTag, tableTag}, stats.samplingEngineKeys, a.aggregatorHostTag)

		if mirrorChWrite {
			bodyStorage, buffers, stats, marshalDur = a.RowDataMarshalAppendPositions(aggBuckets, buffers, rnd, bodyStorage[:0], !writeToV3First)
			if writeToV3First {
				settings = v2InsertSettings
			} else {
				settings = v3InsertSettings
			}
			status, exception, dur, sendErr = sendToClickhouse(ctx, httpClient, a.config.KHAddr, a.config.KHUser, a.config.KHPassword, getTableDesc(!writeToV3First), bodyStorage, settings)
			cancelSendToCh()
			if sendErr != nil {
				comment := fmt.Sprintf("time=%d (delta = %d), contributors (recent %v, historic %v) Sender %d", aggBucket.time, int64(nowUnix)-int64(aggBucket.time), recentContributors, historicContributors, senderID)
				a.appendInternalLog(insertErrTable(!writeToV3First), "", strconv.Itoa(status), strconv.Itoa(exception), "statshouse_value_incoming_arg_min_max", "", comment, sendErr.Error())
				log.Print(sendErr)
				sendErr = &rpc.Error{
					Code:        data_model.RPCErrorInsert,
					Description: sendErr.Error(),
				}
			}

			for i, b := range aggBuckets {
				is := stats.sizes[b.time]
				a.reportInsertMetric(b.time, format.BuiltinMetricMetaAggInsertSize, i != 0, sendErr, status, exception, !writeToV3First, format.TagValueIDSizeCounter, float64(is.counters))
				a.reportInsertMetric(b.time, format.BuiltinMetricMetaAggInsertSize, i != 0, sendErr, status, exception, !writeToV3First, format.TagValueIDSizeValue, float64(is.values))
				a.reportInsertMetric(b.time, format.BuiltinMetricMetaAggInsertSize, i != 0, sendErr, status, exception, !writeToV3First, format.TagValueIDSizePercentiles, float64(is.percentiles))
				a.reportInsertMetric(b.time, format.BuiltinMetricMetaAggInsertSize, i != 0, sendErr, status, exception, !writeToV3First, format.TagValueIDSizeUnique, float64(is.uniques))
				a.reportInsertMetric(b.time, format.BuiltinMetricMetaAggInsertSize, i != 0, sendErr, status, exception, !writeToV3First, format.TagValueIDSizeStringTop, float64(is.stringTops))
				a.reportInsertMetric(b.time, format.BuiltinMetricMetaAggInsertTime, i != 0, sendErr, status, exception, !writeToV3First, 0, dur.Seconds())
			}
			a.reportInsertMetric(aggBucket.time, format.BuiltinMetricMetaAggInsertSizeReal, willInsertHistoric, sendErr, status, exception, !writeToV3First, 0, float64(len(bodyStorage)))
			a.reportInsertMetric(aggBucket.time, format.BuiltinMetricMetaAggInsertTimeReal, willInsertHistoric, sendErr, status, exception, !writeToV3First, 0, dur.Seconds())
			a.reportInsertMetric(aggBucket.time, format.BuiltinMetricMetaAggSamplingTime, willInsertHistoric, sendErr, status, exception, !writeToV3First, 0, marshalDur.Seconds())
			if writeToV3First {
				tableTag = format.TagValueIDAggInsertV2
			} else {
				tableTag = format.TagValueIDAggInsertV3
			}
			statusTag = int32(format.TagValueIDStatusOK)
			if sendErr != nil {
				statusTag = format.TagValueIDStatusError
			}
			st = []int32{0, stats.historicTag, statusTag, tableTag}
			a.sh2.AddValueCounterHost(stats.recentTs, format.BuiltinMetricMetaAggSamplingMetricCount, st, float64(stats.samplingMetricCount), 1, a.aggregatorHostTag)
			a.sh2.AddValueCounterHost(stats.recentTs, format.BuiltinMetricMetaAggSamplingBudget, st, float64(stats.samplingBudget), 1, a.aggregatorHostTag)
			a.sh2.AddValueCounterHost(stats.recentTs, format.BuiltinMetricMetaAggContributors, []int32{0, statusTag, tableTag, format.AggHostTag: a.aggregatorHost, format.AggShardTag: a.shardKey, format.AggReplicaTag, a.replicaKey}, float64(stats.contributors), 1, a.aggregatorHostTag)
			for sk, ss := range stats.sampling {
				keepTags := []int32{0, stats.historicTag, format.TagValueIDSamplingDecisionKeep, sk.namespeceId, sk.groupId, 0, statusTag, tableTag}
				discardTags := []int32{0, stats.historicTag, format.TagValueIDSamplingDecisionDiscard, sk.namespeceId, sk.groupId, 0, statusTag, tableTag}
				groupBudgetTags := []int32{0, stats.historicTag, sk.namespeceId, sk.groupId, statusTag, tableTag}
				a.sh2.MergeItemValue(stats.recentTs, format.BuiltinMetricMetaAggSamplingSizeBytes, keepTags, &ss.sampligSizeKeepBytes)
				a.sh2.MergeItemValue(stats.recentTs, format.BuiltinMetricMetaAggSamplingSizeBytes, discardTags, &ss.sampligSizeDiscardBytes)
				a.sh2.MergeItemValue(stats.recentTs, format.BuiltinMetricMetaAggSamplingGroupBudget, groupBudgetTags, &ss.samplingGroupBudget)
			}
			a.sh2.AddValueCounterHost(stats.recentTs, format.BuiltinMetricMetaAggSamplingEngineTime, []int32{0, 1, 0, 0, stats.historicTag, statusTag, tableTag}, stats.sampleTimeAppend, 1, a.aggregatorHostTag)
			a.sh2.AddValueCounterHost(stats.recentTs, format.BuiltinMetricMetaAggSamplingEngineTime, []int32{0, 2, 0, 0, stats.historicTag, statusTag, tableTag}, stats.sampleTimePartition, 1, a.aggregatorHostTag)
			a.sh2.AddValueCounterHost(stats.recentTs, format.BuiltinMetricMetaAggSamplingEngineTime, []int32{0, 3, 0, 0, stats.historicTag, statusTag, tableTag}, stats.sampleTimeBudgeting, 1, a.aggregatorHostTag)
			a.sh2.AddValueCounterHost(stats.recentTs, format.BuiltinMetricMetaAggSamplingEngineTime, []int32{0, 4, 0, 0, stats.historicTag, statusTag, tableTag}, stats.sampleTimeSampling, 1, a.aggregatorHostTag)
			a.sh2.AddValueCounterHost(stats.recentTs, format.BuiltinMetricMetaAggSamplingEngineTime, []int32{0, 5, 0, 0, stats.historicTag, statusTag, tableTag}, stats.sampleTimeMetricMeta, 1, a.aggregatorHostTag)
			a.sh2.AddCounterHost(stats.recentTs, format.BuiltinMetricMetaAggSamplingEngineKeys, []int32{0, 0, 0, 0, stats.historicTag, statusTag, tableTag}, stats.samplingEngineKeys, a.aggregatorHostTag)
		}

		sendErr = fmt.Errorf("simulated error")
		aggBucket.mu.Lock()
		for hctx := range aggBucket.contributorsSimulatedErrors {
			hctx.SendHijackedResponse(sendErr)
		}
		for hctx := range aggBucket.contributorsSimulatedErrors { // compiles into map_clear
			delete(aggBucket.contributorsSimulatedErrors, hctx)
		}
		aggBucket.mu.Unlock()
	}
}

// returns bucket with exclusive ownership requiring no locks to access
func (a *Aggregator) popOldestHistoricBucket(oldestTime uint32) (aggBucket *aggregatorBucket, staleBuckets []*aggregatorBucket) {
	a.mu.Lock()
	for _, v := range a.historicBuckets { // Find oldest bucket
		if oldestTime >= data_model.MaxHistoricWindow && v.time < oldestTime-data_model.MaxHistoricWindow {
			staleBuckets = append(staleBuckets, v)
			delete(a.historicBuckets, v.time)
			continue
		}
		if aggBucket == nil || v.time < aggBucket.time {
			aggBucket = v
		}
	}
	if aggBucket != nil {
		delete(a.historicBuckets, aggBucket.time)
		if len(aggBucket.contributorsSimulatedErrors) != 0 {
			panic("len(aggBucket.contributorsSimulatedErrors) != 0 in goSendHistoric")
		}
	}
	a.mu.Unlock()
	if aggBucket != nil {
		aggBucket.sendMu.Lock()   // Lock/Unlock waits all clients to finish aggregation
		aggBucket.sendMu.Unlock() //lint:ignore SA2001 empty critical section
	}
	for _, b := range staleBuckets {
		b.sendMu.Lock()   // Lock/Unlock waits all clients to finish aggregation
		b.sendMu.Unlock() //lint:ignore SA2001 empty critical section
	}
	// Here we have exclusive access to buckets, without locks
	return
}

func (a *Aggregator) advanceRecentBuckets(now time.Time, initial bool) []*aggregatorBucket {
	nowUnix := uint32(now.Unix())
	var readyBuckets []*aggregatorBucket
	// As quickly as possible select which buckets should be sent
	a.mu.Lock()
	defer a.mu.Unlock()

	for len(a.recentBuckets) != 0 && nowUnix > a.recentBuckets[0].time+uint32(a.config.ShortWindow) {
		readyBuckets = append(readyBuckets, a.recentBuckets[0])
		a.recentBuckets = append([]*aggregatorBucket{}, a.recentBuckets[1:]...) // copy
	}
	if len(a.recentBuckets) == 0 { // Jumped into future, also initial state
		b := &aggregatorBucket{
			time:                        nowUnix - uint32(a.config.ShortWindow),
			contributors:                map[*rpc.HandlerContext]struct{}{},
			contributors3:               map[*rpc.HandlerContext]tlstatshouse.SendSourceBucket3Response{},
			contributorsSimulatedErrors: map[*rpc.HandlerContext]struct{}{},
			historicHosts:               [2][2]map[int32]int64{{map[int32]int64{}, map[int32]int64{}}, {map[int32]int64{}, map[int32]int64{}}},
		}
		a.recentBuckets = append(a.recentBuckets, b)
	}
	for len(a.recentBuckets) < a.config.ShortWindow+data_model.FutureWindow {
		b := &aggregatorBucket{
			time:                        a.recentBuckets[0].time + uint32(len(a.recentBuckets)),
			contributors:                map[*rpc.HandlerContext]struct{}{},
			contributors3:               map[*rpc.HandlerContext]tlstatshouse.SendSourceBucket3Response{},
			contributorsSimulatedErrors: map[*rpc.HandlerContext]struct{}{},
			historicHosts:               [2][2]map[int32]int64{{map[int32]int64{}, map[int32]int64{}}, {map[int32]int64{}, map[int32]int64{}}},
		}
		a.recentBuckets = append(a.recentBuckets, b)
	}
	if !initial {
		return readyBuckets
	}
	// rest of func runs once per second while everything is working normally
	// we keep this separation for now, in case we need some logic here
	return readyBuckets
}

func (a *Aggregator) goTicker() {
	defer log.Printf("clickhouse bucket queue ticket quit")

	now := time.Now()
	for { // TODO - quit
		tick := time.After(data_model.TillStartOfNextSecond(now))
		now = <-tick // We synchronize with calendar second boundary

		a.updateConfigRemotelyExperimental()
		readyBuckets := a.advanceRecentBuckets(now, false)
		for _, aggBucket := range readyBuckets {
			aggBucket.sendMu.Lock()   // Lock/Unlock waits all clients to finish aggregation
			aggBucket.sendMu.Unlock() //lint:ignore SA2001 empty critical section
			// Here we have exclusive access to bucket, without locks
			if aggBucket.time%3 != uint32(a.replicaKey-1) { // must be empty
				if len(aggBucket.contributors) != 0 || len(aggBucket.contributors3) != 0 || len(aggBucket.contributorsSimulatedErrors) != 0 {
					log.Panicf("not our (%d) bucket %d has %d (%d) contributors", a.replicaKey, aggBucket.time, len(aggBucket.contributors), len(aggBucket.contributorsSimulatedErrors))
				}
				continue
			}
			a.mu.Lock() // be careful to unlock on all paths below
			if a.bucketsToSend == nil {
				a.mu.Unlock()
				return // aggregator in shutdown
			}
			select {
			case a.bucketsToSend <- aggBucket: // have to send under lock
				a.mu.Unlock()
			default:
				a.mu.Unlock()
				numContributors := aggBucket.contributorsCount()
				err := fmt.Errorf("insert conveyor is full for Bucket time=%d, contributors %f", aggBucket.time, numContributors)
				fmt.Printf("%s\n", err)
				aggBucket.mu.Lock()
				// there must be exactly 0 historic hosts in this bucket, so can skip the next lines
				// historicHosts := aggBucket.historicHosts
				// (under aggregator lock): a.updateHistoricHostsLocked(a.historicHosts, historicHosts)
				for hctx := range aggBucket.contributors {
					hctx.SendHijackedResponse(err)
				}
				for hctx, resp := range aggBucket.contributors3 {
					var ssb3 tlstatshouse.SendSourceBucket3 // Dummy
					resp.Warning = err.Error()
					hctx.Response, _ = ssb3.WriteResult(hctx.Response, resp)
					hctx.SendHijackedResponse(nil)
				}
				clear(aggBucket.contributors) // safeguard against sending more than once
				clear(aggBucket.contributors3)
				aggBucket.mu.Unlock()
			}
		}
		if len(readyBuckets) != 0 && readyBuckets[0].time >= data_model.MaxHistoricWindow {
			oldestTime := readyBuckets[0].time - data_model.MaxHistoricWindow
			a.estimator.GarbageCollect(oldestTime)
		}
	}
}

func (a *Aggregator) updateConfigRemotelyExperimental() {
	if a.config.DisableRemoteConfig || a.metricStorage == nil {
		return
	}
	description := ""
	if mv := a.metricStorage.GetMetaMetricByName(format.StatshouseAggregatorRemoteConfigMetric); mv != nil {
		description = mv.Description
	}
	if description == a.configS {
		return
	}
	a.configS = description
	log.Printf("Remote config:\n%s", description)
	config := a.config.ConfigAggregatorRemote
	if err := config.updateFromRemoteDescription(description); err != nil {
		log.Printf("[error] Remote config: error updating config from metric %q: %v", format.StatshouseAggregatorRemoteConfigMetric, err)
		return
	}
	log.Printf("Remote config: updated config from metric %q", format.StatshouseAggregatorRemoteConfigMetric)
	a.configMu.Lock()
	a.configR = config
	a.configMu.Unlock()
	a.mappingsCache.SetSizeTTL(config.MappingCacheSize, config.MappingCacheTTL)
	a.tagsMapper2.SetConfig(config.configTagsMapper2)
}
