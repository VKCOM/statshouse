// Copyright 2022 V Kontakte LLC
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
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/vkcom/statshouse/internal/vkgo/build"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"

	"github.com/vkcom/statshouse/internal/agent"
	"github.com/vkcom/statshouse/internal/aggregator/prometheus"
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/metajournal"
	"github.com/vkcom/statshouse/internal/pcache"

	"pgregory.net/rand"
)

type (
	// Clients take reader lock, then check sending flag, if true, they were late
	// If false, they take shard lock, then aggregate into shard

	// When time tics, ticker takes writer lock, sets sending flag, then releases writer lock
	// After that it can access shards and 100% know no one accesses them
	aggregatorShard struct {
		mu         sync.Mutex // Protects items
		multiItems map[data_model.Key]*data_model.MultiItem
	}
	aggregatorBucket struct {
		time   uint32
		shards [data_model.AggregationShardsPerSecond]aggregatorShard

		rawSize              int                   // protected by aggregator mutex
		contributors         []*rpc.HandlerContext // protected by aggregator mutex
		contributorsOriginal data_model.ItemValue  // Not recorded for keep-alive, protected by aggregator mutex
		contributorsSpare    data_model.ItemValue  // Not recorded for keep-alive, protected by aggregator mutex

		usedMetrics map[int32]struct{}
		mu          sync.Mutex // Protects everything, except shards

		sendMu sync.RWMutex // Used to wait for all aggregating clients to finish before sending

		contributorsSimulatedErrors []*rpc.HandlerContext // put into most future bucket, so receive error after >7 seconds
	}
	Aggregator struct {
		recentBuckets   []*aggregatorBucket          // We collect into several buckets before sending
		recentNow       uint32                       // Value used to update recentBuckets
		historicBuckets map[uint32]*aggregatorBucket // timestamp->bucket
		bucketsToSend   chan *aggregatorBucket
		mu              sync.Mutex
		cond            *sync.Cond // Signalled when historicBuckets len or activeSenders change
		server          *rpc.Server
		hostName        []byte
		aggregatorHost  int32
		withoutCluster  bool
		shardKey        int32 // never changes after start, can be used without lock
		replicaKey      int32 // never changes after start, can be used without lock
		buildArchTag    int32 // never changes after start, can be used without lock
		startTimestamp  uint32
		addresses       []string

		tagMappingBootstrapResponse []byte // sending large responses to thousands of clients at once, so must be very efficient

		sh2 *agent.Agent // set to not nil some time after launching aggregator

		internalLog []byte // simply flushed every couple seconds

		estimator data_model.Estimator

		activeSenders int

		config ConfigAggregator

		metricStorage  *metajournal.MetricsStorage
		testConnection *TestConnection
		tagsMapper     *TagsMapper

		promUpdater *prometheus.Updater
		autoCreate  *autoCreate
	}
	BuiltInStatRecord struct {
		Key  data_model.Key
		SKey string
		data_model.ItemValue
	}
)

func RunAggregator(dc *pcache.DiskCache, storageDir string, listenAddr string, aesPwd string, config ConfigAggregator, hostName string, logTrace bool) error {
	if dc == nil { // TODO - make sure aggregator works without cache dir?
		return fmt.Errorf("aggregator cannot run without -cache-dir for now")
	}
	_, listenPort, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return fmt.Errorf("failed to split --agg-addr (%q) into host and port for autoconfiguration: %v", listenAddr, err)
	}
	if config.ExternalPort == "" {
		config.ExternalPort = listenPort
	}
	var shardKey int32 = 1
	var replicaKey int32 = 1
	var addresses = []string{listenAddr}
	if config.KHAddr != "" {
		shardKey, replicaKey, addresses, err = selectShardReplica(config.KHAddr, config.Cluster, config.ExternalPort)
		if err != nil {
			return fmt.Errorf("failed to find out local shard and replica in cluster %q, probably wrong --cluster command line parameter set: %v", config.Cluster, err)
		}
	}
	withoutCluster := false
	if len(addresses) == 1 { // mostly demo runs with local non-replicated clusters
		addresses = []string{addresses[0], addresses[0], addresses[0]}
		withoutCluster = true
		log.Printf("[warning] running with single-host cluster, probably demo")
	}
	if len(addresses)%3 != 0 {
		return fmt.Errorf("failed configuration - must have exactly 3 replicas in cluster %q per shard, probably wrong --cluster command line parameter set: %v", config.Cluster, err)
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
			return fmt.Errorf("failed to load mapping bootstrap: %v", err)
		}
		// ok, empty bootstrap is good for us for running locally
		tagMappingBootstrapResponse, _ = (&tlmetadata.GetTagMappingBootstrap{}).WriteResult(nil, tlstatshouse.GetTagMappingBootstrapResult{})
	}

	a := &Aggregator{
		bucketsToSend:   make(chan *aggregatorBucket),
		historicBuckets: map[uint32]*aggregatorBucket{},
		server: rpc.NewServer(rpc.ServerWithCryptoKeys([]string{aesPwd}),
			rpc.ServerWithLogf(log.Printf),
			rpc.ServerWithMaxWorkers(-1),
			rpc.ServerWithDisableContextTimeout(true),
			rpc.ServerWithTrustedSubnetGroups(build.TrustedSubnetGroups()),
			rpc.ServerWithVersion(build.Info()),
			rpc.ServerWithDefaultResponseTimeout(data_model.MaxConveyorDelay*time.Second),
			rpc.ServerWithMaxInflightPackets((data_model.MaxConveyorDelay+data_model.MaxHistorySendStreams)*3), // *3 is additional load for spares, when original aggregator is down
			rpc.ServerWithResponseBufSize(1024),
			rpc.ServerWithResponseMemEstimate(1024),
			rpc.ServerWithRequestMemoryLimit(2<<30)),
		config:                      config,
		hostName:                    format.ForceValidStringValue(hostName), // worse alternative is do not run at all
		withoutCluster:              withoutCluster,
		shardKey:                    shardKey,
		replicaKey:                  replicaKey,
		buildArchTag:                format.GetBuildArchKey(runtime.GOARCH),
		addresses:                   addresses,
		tagMappingBootstrapResponse: tagMappingBootstrapResponse,
	}
	metricMetaLoader := metajournal.NewMetricMetaLoader(metadataClient, metajournal.DefaultMetaTimeout)
	log.Println("starting prom updater")
	a.promUpdater, err = prometheus.RunPromUpdaterAsync(hostName, logTrace)
	a.metricStorage = metajournal.MakeMetricsStorage(a.config.Cluster, dc, a.promUpdater.ApplyConfigFromJournal)
	a.metricStorage.Journal().Start(a.sh2, a.appendInternalLog, metricMetaLoader.LoadJournal)
	if err != nil {
		return fmt.Errorf("failed to run prom updater: %s", err)
	}
	log.Println("prom updater started")
	agentConfig := agent.DefaultConfig()
	agentConfig.Cluster = a.config.Cluster
	// We use agent instance for aggregator built-in metrics
	getConfigResult := a.getConfigResult() // agent will use this config instead of getting via RPC, because our RPC is not started yet
	// TODO - pass storage dir after design is fixed
	sh2, err := agent.MakeAgent("tcp4", storageDir, aesPwd, agentConfig, hostName,
		format.TagValueIDComponentAggregator, a.metricStorage, log.Printf, a.agentBeforeFlushBucketFunc, &getConfigResult)
	if err != nil {
		return fmt.Errorf("built-in agent failed to start: %v", err)
	}
	a.sh2 = sh2

	a.testConnection = MakeTestConnection()
	a.tagsMapper = NewTagsMapper(a, a.sh2, a.metricStorage, dc, a, metricMetaLoader, a.config.Cluster)

	a.aggregatorHost = a.tagsMapper.mapTagAtStartup(a.hostName, format.BuiltinMetricNameBudgetAggregatorHost)

	a.estimator.Init(config.CardinalityWindow, a.config.MaxCardinality/len(addresses))
	a.cond = sync.NewCond(&a.mu)
	a.server.RegisterHandlerFunc(a.handleClient)
	now := time.Now()
	a.startTimestamp = uint32(now.Unix())
	_ = a.advanceRecentBuckets(now, true) // Just create initial set of buckets and set LastHour
	a.appendInternalLog("start", "", build.Commit(), build.Info(), strings.Join(os.Args[1:], " "), strings.Join(addresses, ","), "", "Started")

	go a.goTicker()
	for i := 0; i < a.config.RecentInserters; i++ {
		go a.goSend(i)
	}
	for i := 0; i < a.config.HistoricInserters; i++ {
		go a.goSendHistoric(i)
	}
	go a.goInternalLog()
	if config.AutoCreate {
		a.autoCreate = newAutoCreate(metadataClient, a.metricStorage)
		defer a.autoCreate.shutdown()
	}

	sh2.Run(a.aggregatorHost, a.shardKey, a.replicaKey)

	return a.server.ListenAndServe("tcp4", listenAddr)
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
			return nil, fmt.Errorf("bootstrap data failed to load, and not int disk cache: %w", err)
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

func (b *aggregatorBucket) lockShard(lockedShard *int, sID int) *aggregatorShard {
	if *lockedShard == sID {
		return &b.shards[sID]
	}
	if *lockedShard != -1 {
		b.shards[*lockedShard].mu.Unlock()
		*lockedShard = -1
	}
	if sID != -1 {
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

func (a *Aggregator) agentBeforeFlushBucketFunc(now time.Time) {
	nowUnix := uint32(now.Unix())
	a.mu.Lock()
	activeSenders := a.activeSenders
	var original data_model.ItemValue
	var spare data_model.ItemValue
	var original_unique data_model.ItemValue
	var spare_unique data_model.ItemValue
	for _, v := range a.historicBuckets {
		original.AddValueCounterHost(float64(nowUnix-v.time), v.contributorsOriginal.Counter, v.contributorsOriginal.MaxHostTag)
		spare.AddValueCounterHost(float64(nowUnix-v.time), v.contributorsSpare.Counter, v.contributorsSpare.MaxHostTag)
		original_unique.AddValueCounterHost(float64(nowUnix-v.time), 1, v.contributorsOriginal.MaxHostTag)
		spare_unique.AddValueCounterHost(float64(nowUnix-v.time), 1, v.contributorsSpare.MaxHostTag)
	}
	a.mu.Unlock()

	writeWaiting := func(metricID int32, key4 int32, item *data_model.ItemValue) {
		key := data_model.AggKey(0, metricID, [16]int32{0, 0, 0, 0, key4}, a.aggregatorHost, a.shardKey, a.replicaKey)
		a.sh2.MergeItemValue(key, item, nil)
	}
	writeWaiting(format.BuiltinMetricIDAggHistoricBucketsWaiting, format.TagValueIDAggregatorOriginal, &original)
	writeWaiting(format.BuiltinMetricIDAggHistoricBucketsWaiting, format.TagValueIDAggregatorSpare, &spare)
	writeWaiting(format.BuiltinMetricIDAggHistoricSecondsWaiting, format.TagValueIDAggregatorOriginal, &original_unique)
	writeWaiting(format.BuiltinMetricIDAggHistoricSecondsWaiting, format.TagValueIDAggregatorSpare, &spare_unique)

	key := data_model.AggKey(0, format.BuiltinMetricIDAggActiveSenders, [16]int32{}, a.aggregatorHost, a.shardKey, a.replicaKey)
	a.sh2.AddValueCounterHost(key, float64(activeSenders), 1, a.aggregatorHost)

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

func selectShardReplica(khAddr string, cluster string, listenPort string) (shardKey int32, replicaKey int32, addresses []string, err error) {
	log.Printf("[debug] starting autoconfiguration by making SELECT to clickhouse address %q", khAddr)
	httpClient := makeHTTPClient(data_model.ClickHouseTimeoutConfig)
	backoffTimeout := time.Duration(0)
	for i := 0; ; i++ {
		// motivation for several attempts is random clickhouse errors, plus starting before clickhouse is ready to process requests in demo mode
		shardKey, replicaKey, addresses, err = selectShardReplicaImpl(httpClient, khAddr, cluster, listenPort)
		if err == nil || i >= data_model.ClickhouseConfigRetries {
			return
		}
		backoffTimeout = data_model.NextBackoffDuration(backoffTimeout)
		log.Printf("[error] failed to read configuration, will retry in %v: %v", backoffTimeout, err)
		time.Sleep(backoffTimeout)
	}
}

func selectShardReplicaImpl(httpClient *http.Client, khAddr string, cluster string, listenPort string) (shardKey int32, replicaKey int32, addresses []string, err error) {
	// We assume replicas 4+ are for readers only
	queryPrefix := url.PathEscape(fmt.Sprintf("SELECT shard_num, replica_num, is_local, host_name FROM system.clusters where cluster='%s' and replica_num <= 3 order by shard_num, replica_num", cluster))
	URL := fmt.Sprintf("http://%s/?input_format_values_interpret_expressions=0&query=%s", khAddr, queryPrefix)

	req, err := http.NewRequest("POST", URL, nil)
	if err != nil {
		return 0, 0, nil, err
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

func (a *Aggregator) sendBucket(rnd *rand.Rand, bodyStorage *[]byte, httpClient *http.Client, b *aggregatorBucket, historic bool, comment string) (float64, error) {
	a.estimator.ReportHourCardinality(b.time, b.usedMetrics, &b.shards[0].multiItems, a.aggregatorHost, a.shardKey, a.replicaKey, len(a.addresses))
	*bodyStorage = a.RowDataMarshalAppendPositions(b, rnd, (*bodyStorage)[:0], historic)
	// Never empty, because adds value stats
	status, exception, dur, err := sendToClickhouse(httpClient, a.config.KHAddr, getTableDesc(), *bodyStorage)
	if err != nil {
		a.appendInternalLog("insert_error", "", strconv.Itoa(status), strconv.Itoa(exception), "statshouse_value_incoming_arg_min_max", "", comment, err.Error())
		log.Print(err)
	}
	return dur, err
}

func (a *Aggregator) goSend(senderID int) {
	rnd := rand.New()
	var bodyStorage []byte
	httpClient := makeHTTPClient(data_model.ClickHouseTimeout)

	for aggBucket := range a.bucketsToSend {
		nowUnix := uint32(time.Now().Unix())
		comment := fmt.Sprintf("time=%d (delta = %d), contributors %d Sender %d", aggBucket.time, int64(nowUnix)-int64(aggBucket.time), len(aggBucket.contributors), senderID)
		a.mu.Lock()
		a.activeSenders++
		a.mu.Unlock()
		dur, sendErr := a.sendBucket(rnd, &bodyStorage, httpClient, aggBucket, false, comment)

		a.mu.Lock()
		a.activeSenders--
		if a.sendHistoricCondition() {
			a.cond.Broadcast() // we are not sure that Signal is enough
		}
		a.mu.Unlock()
		if sendErr != nil {
			sendErr = rpc.Error{
				Code:        data_model.RPCErrorInsertRecentConveyor,
				Description: sendErr.Error(),
			}
		}
		a.reporInsert(aggBucket.time, format.TagValueIDConveyorRecent, sendErr, dur, len(bodyStorage))
		var args tlstatshouse.SendSourceBucket2 // Dummy
		for _, c := range aggBucket.contributors {
			c.Response, _ = args.WriteResult(c.Response, "Dummy result")
			c.SendHijackedResponse(sendErr)
		}
		sendErr = fmt.Errorf("simulated error")
		for _, c := range aggBucket.contributorsSimulatedErrors {
			c.SendHijackedResponse(sendErr)
		}
	}
}

func (a *Aggregator) sendHistoricCondition() bool {
	// Called when a.mu locked
	return a.activeSenders <= a.config.InsertHistoricWhen && len(a.historicBuckets) != 0
}

type samplingItem struct {
	time  uint32
	count int
}

func (a *Aggregator) goSendHistoric(senderID int) {
	rnd := rand.New()

	var bodyStorage []byte
	var aggBuckets []*aggregatorBucket
	var aggBucketsSizes []int
	// If we have too little data, we continue to batch up to N buckets together before inserting.

	httpClient := makeHTTPClient(data_model.ClickHouseTimeoutHistoric)
	a.mu.Lock()
	for {
		if !a.sendHistoricCondition() {
			a.cond.Wait()
			continue
		}
		var aggBucket *aggregatorBucket
		var timeList []samplingItem
		for _, v := range a.historicBuckets { // Find oldest bucket
			if aggBucket == nil || v.time < aggBucket.time {
				aggBucket = v
			}
			timeList = append(timeList, samplingItem{time: v.time, count: len(v.contributors)})
		}
		sort.Slice(timeList, func(i, j int) bool {
			return timeList[i].time < timeList[j].time
		})
		delete(a.historicBuckets, aggBucket.time)
		haveMoreBuckets := len(a.historicBuckets) != 0
		if len(aggBucket.contributorsSimulatedErrors) != 0 {
			panic("len(aggBucket.contributorsSimulatedErrors) != 0 in goSendHistoric")
		}
		a.mu.Unlock()
		aggBucket.sendMu.Lock()   // Lock/Unlock waits all clients to finish aggregation
		aggBucket.sendMu.Unlock() //lint:ignore SA2001 empty critical section
		// Here we have exclusive access to bucket, without locks

		a.estimator.ReportHourCardinality(aggBucket.time, aggBucket.usedMetrics, &aggBucket.shards[0].multiItems, a.aggregatorHost, a.shardKey, a.replicaKey, len(a.addresses))
		prevSize := len(bodyStorage)
		bodyStorage = a.RowDataMarshalAppendPositions(aggBucket, rnd, bodyStorage, true)
		aggBuckets = append(aggBuckets, aggBucket)
		aggBucketsSizes = append(aggBucketsSizes, len(bodyStorage)-prevSize)

		if len(bodyStorage) < data_model.HistoryInsertBodySizeLimit && len(aggBuckets) < data_model.MaxHistoryInsertBatch && haveMoreBuckets {
			a.mu.Lock()
			continue
		}
		// Never empty, because adds value stats
		nowUnix := uint32(time.Now().Unix())
		status, exception, dur, sendErr := sendToClickhouse(httpClient, a.config.KHAddr, getTableDesc(), bodyStorage)
		if sendErr != nil {
			comment := fmt.Sprintf("time=%d..%d (delta = %d), contributors %d Sender %d Historic", aggBuckets[0].time, aggBucket.time, int64(nowUnix)-int64(aggBucket.time), len(aggBucket.contributors), senderID)
			a.appendInternalLog("insert_error", "", strconv.Itoa(status), strconv.Itoa(exception), "statshouse_value_incoming_arg_min_max", "", comment, sendErr.Error())
			log.Print(sendErr)
			sendErr = rpc.Error{
				Code:        data_model.RPCErrorInsertHistoricConveyor,
				Description: sendErr.Error(),
			}
		}
		// log.Printf("Historic inserted size %d time %d (%d buckets)", len(bodyStorage), aggBuckets[0].time, len(aggBuckets))

		var args tlstatshouse.SendSourceBucket2 // Dummy
		for i, b := range aggBuckets {
			for _, c := range b.contributors {
				c.Response, _ = args.WriteResult(c.Response, "Dummy historic result")
				c.SendHijackedResponse(sendErr)
			}
			a.reporInsert(b.time, format.TagValueIDConveyorHistoric, sendErr, dur, aggBucketsSizes[i])
		}
		bodyStorage = bodyStorage[:0]
		aggBuckets = aggBuckets[:0]
		aggBucketsSizes = aggBucketsSizes[:0]
		if sendErr != nil {
			time.Sleep(data_model.ClickHouseErrorDelay)
		} else if !haveMoreBuckets {
			// Motivation - when we have a couple of sources with wrong clock, their buckets are always bounced from recent conveyor
			// to historic one every second, so historic becomes non-empty every second. As historic inserter
			// has no delay, as soon as the first historic second appears, we make insert immediately, and when we are finished,
			// historic buckets from the rest of lagging sources arrive. So we are making 1-2 historic inserts per second continuously.
			// To avoid this, we stop when historic is empty, allowing all/many such historic buckets to arrive.
			time.Sleep(data_model.NoHistoricBucketsDelay)
		}
		a.mu.Lock()
	}
}

func (a *Aggregator) advanceRecentBuckets(now time.Time, initial bool) []*aggregatorBucket {
	nowUnix := uint32(now.Unix())
	var readyBuckets []*aggregatorBucket
	// As quickly as possible select which buckets should be sent
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.aggregatorHost == 0 { // Repeat periodically, like we do for incoming hosts
		a.aggregatorHost = a.tagsMapper.mapHost(now, a.hostName, format.BuiltinMetricNameBudgetAggregatorHost, false)
	}

	for len(a.recentBuckets) != 0 && nowUnix > a.recentBuckets[0].time+uint32(a.config.ShortWindow) {
		readyBuckets = append(readyBuckets, a.recentBuckets[0])
		a.recentBuckets = append([]*aggregatorBucket{}, a.recentBuckets[1:]...) // copy
	}
	if len(a.recentBuckets) == 0 { // Jumped into future, also initial state
		b := &aggregatorBucket{
			time: nowUnix - uint32(a.config.ShortWindow),
		}
		a.recentBuckets = append(a.recentBuckets, b)
	}
	for len(a.recentBuckets) < a.config.ShortWindow+data_model.FutureWindow {
		b := &aggregatorBucket{
			time: a.recentBuckets[0].time + uint32(len(a.recentBuckets)),
		}
		a.recentBuckets = append(a.recentBuckets, b)
	}
	if a.recentNow == nowUnix && !initial {
		return readyBuckets
	}
	// rest of func runs once per second while everything is working normally
	// we keep this separation for now, in case we need some logic here
	return readyBuckets
}

func (a *Aggregator) goTicker() {
	now := time.Now()
	for { // TODO - quit
		tick := time.After(data_model.TillStartOfNextSecond(now))
		now = <-tick // We synchronize with calendar second boundary

		readyBuckets := a.advanceRecentBuckets(now, false)
		for _, r := range readyBuckets {
			r.sendMu.Lock()   // Lock/Unlock waits all clients to finish aggregation
			r.sendMu.Unlock() //lint:ignore SA2001 empty critical section
			// Here we have exclusive access to bucket, without locks
			select {
			case a.bucketsToSend <- r:
			default:
				err := fmt.Errorf("insert conveyor is full for Bucket time=%d, contributors %d", r.time, len(r.contributors))
				fmt.Printf("%s\n", err)
				for _, c := range r.contributors {
					c.SendHijackedResponse(err)
				}
			}
		}
		if len(readyBuckets) != 0 && a.recentBuckets[0].time >= data_model.MaxHistoricWindow {
			oldestTime := a.recentBuckets[0].time - data_model.MaxHistoricWindow
			a.estimator.GarbageCollect(oldestTime)
		}
	}
}
