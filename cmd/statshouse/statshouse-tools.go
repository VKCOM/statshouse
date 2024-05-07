// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/atomic"
	"pgregory.net/rand"

	"github.com/vkcom/statshouse/internal/aggregator"
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tl"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouseApi"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/mapping"
	"github.com/vkcom/statshouse/internal/metajournal"
	"github.com/vkcom/statshouse/internal/pcache"
	"github.com/vkcom/statshouse/internal/receiver"
	"github.com/vkcom/statshouse/internal/vkgo/build"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
)

func mainBenchmarks() {
	flag.StringVar(&argv.listenAddr, "p", "127.0.0.1:13337", "RAW UDP & RPC TCP write/listen port")

	build.FlagParseShowVersionHelp()

	FakeBenchmarkMetricsPerSecond(argv.listenAddr)
}

type packetPrinter struct {
}

func (w *packetPrinter) HandleMetrics(args data_model.HandlerArgs) (h data_model.MappedMetricHeader, done bool) {
	log.Printf("Parsed metric: %s\n", args.MetricBytes.String())
	return h, true
}

func (w *packetPrinter) HandleParseError(pkt []byte, err error) {
	log.Printf("Error parsing packet: %v\n", err)
}

func mainTestParser() int {
	flag.StringVar(&argv.listenAddr, "p", ":13337", "RAW UDP & RPC TCP listen address")
	flag.IntVar(&argv.bufferSizeUDP, "buffer-size-udp", receiver.DefaultConnBufSize, "UDP receiving buffer size")

	build.FlagParseShowVersionHelp()

	u, err := receiver.ListenUDP("udp", argv.listenAddr, argv.bufferSizeUDP, false, nil, log.Printf)
	if err != nil {
		logErr.Printf("ListenUDP: %v", err)
		return 1
	}
	logOk.Printf("Listen UDP addr %q by 1 core", argv.listenAddr)

	miniHelp := `You can send packets using command: echo '{"metrics":[{"name":"sentry_issues","tags":{"env":"dev","1":"ok","2":"unknown"},"counter":1}]}' | nc -u 127.0.0.1 13337`
	miniHelp2 := `You can resend packet printed in hex using command: echo "39025856..00001840" | xxd -r -p - | nc -u 127.0.0.1 13337`

	logOk.Printf("%s", miniHelp)
	logOk.Printf("%s", miniHelp2)

	w := &packetPrinter{}

	if err = u.Serve(w); err != nil {
		logErr.Printf("Serve: %v", err)
		return 1
	}
	return 0
}

func mainTestMap() {
	flag.StringVar(&argv.aesPwdFile, "aes-pwd-file", "", "path to AES password file, will try to read "+defaultPathToPwd+" if not set")
	flag.StringVar(&argv.aggAddr, "agg-addr", "", "comma-separated list of aggregator addresses to test.")
	var mapString string
	flag.StringVar(&mapString, "string", "production", "string to map.")
	// TODO - we have no such RPC call
	// var mapInt int
	// flag.IntVar(&mapInt, "int", 0, "int to map back.")

	build.FlagParseShowVersionHelp()

	client, _ := argvCreateClient()
	if argv.aggAddr == "" {
		log.Fatalf("--agg-addr must not be empty")
	}

	aggregator.TestMapper(strings.Split(argv.aggAddr, ","), mapString, client)
}

func mainTestLongpoll() {
	flag.StringVar(&argv.aesPwdFile, "aes-pwd-file", "", "path to AES password file, will try to read "+defaultPathToPwd+" if not set")
	flag.StringVar(&argv.aggAddr, "agg-addr", "", "comma-separated list of aggregator addresses to test.")

	build.FlagParseShowVersionHelp()

	client, _ := argvCreateClient()
	if argv.aggAddr == "" {
		log.Fatalf("--agg-addr must not be empty")
	}

	aggregator.TestLongpoll(strings.Split(argv.aggAddr, ","), client, 60)
}

func mainSimpleFSyncTest() {
	build.FlagParseShowVersionHelp()

	const smallName = "fsync.small.test"
	const bigName = "fsync.big.test"
	const bigSize = 1 << 30
	const smallSize = 1 << 12
	small, err := os.OpenFile(smallName, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatalf("failed to create %q: %v", smallName, err)
	}
	big, err := os.OpenFile(bigName, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatalf("failed to create %q: %v", bigName, err)
	}
	var chunk [1 << 16]byte
	rnd := rand.New()
	_, _ = rnd.Read(chunk[:])
	total := 0
	log.Printf("filling disk cache by writing %d of %q and %d of %q", bigSize, bigName, smallSize, smallName)
	for ; total < bigSize; total += len(chunk) {
		if _, err = big.Write(chunk[:]); err != nil {
			log.Fatalf("failed to write %q: %v", bigName, err)
		}
	}
	if _, err = small.Write(chunk[:smallSize]); err != nil {
		log.Fatalf("failed to write %q: %v", smallName, err)
	}
	simpleFSync(small)
	simpleFSync(small)
	simpleFSync(big)
	simpleFSync(big)
	_ = small.Close()
	_ = big.Close()
	_ = os.Remove(smallName)
	_ = os.Remove(bigName)
}

func simpleFSync(f *os.File) {
	log.Printf("performing fsync of %q", f.Name())
	now := time.Now()
	// on MAC OS X try also
	// _, _, err := syscall.Syscall(syscall.SYS_FCNTL, f.Fd(), syscall.F_FULLFSYNC, 0)
	if err := f.Sync(); err != nil {
		log.Fatalf("failed to fsync %q: %v", f.Name(), err)
	}
	log.Printf("elapsed %v", time.Since(now))
}

func FakeBenchmarkMetricsPerSecond(listenAddr string) {
	const almostReceiveOnly = false // do not spend time on mapping
	const testFastPath = true       // or slow path
	const keyPrefix = "__benchmark"

	dolphinLoader := func(ctx context.Context, lastVersion int64, returnIfEmpty bool) ([]tlmetadata.Event, int64, error) {
		if returnIfEmpty {
			return nil, lastVersion, nil
		}
		if lastVersion != 0 {
			time.Sleep(time.Second * 30) // long poll to avoid printing in console)
			return nil, lastVersion, nil
		}
		result := &format.MetricMetaValue{
			MetricID: 1,
			Name:     "metric1",
			Tags:     []format.MetricMetaTag{{Name: "env"}, {Name: "k1"}, {Name: "k2"}, {Name: "k3"}, {Name: "k4", Raw: true}, {Name: "k5"}},
			Visible:  true,
		}
		_ = result.RestoreCachedInfo()
		data, err := result.MarshalBinary()
		return []tlmetadata.Event{{
			Id:         int64(result.MetricID),
			Name:       result.Name,
			EventType:  0,
			Unused:     0,
			Version:    1,
			UpdateTime: 1,
			Data:       string(data),
		}}, 1, err
	}
	pmcLoader := func(ctxParent context.Context, key string, floodLimitKey interface{}) (pcache.Value, time.Duration, error) {
		key = strings.TrimPrefix(key, keyPrefix)
		i, err := strconv.Atoi(key)
		if err != nil {
			return nil, time.Second * 1000, err
		}
		return pcache.Int32ToValue(int32(i)), time.Second * 1000, nil
	}

	var wrongID atomic.Int64
	var wrongTag1 atomic.Int64
	var mapError atomic.Int64
	var goodMetric atomic.Int64
	var parseErrors atomic.Int64
	var validateErrors atomic.Int64
	var sentMetric atomic.Int64
	var recvMetric atomic.Int64
	var recvMetric2 atomic.Int64

	go func() { // printStats
		for { // forever
			time.Sleep(time.Second)
			bad := wrongID.Load() + wrongTag1.Load() + parseErrors.Load() + validateErrors.Load() + mapError.Load()
			fmt.Printf("Sent %d Received %d %d Success %d Errors %d\n", sentMetric.Load(), recvMetric.Load(), recvMetric2.Load(), goodMetric.Load(), bad)
			sentMetric.Store(0)
			recvMetric.Store(0)
			recvMetric2.Store(0)
			goodMetric.Store(0)
			wrongID.Store(0)
			wrongTag1.Store(0)
			parseErrors.Store(0)
			validateErrors.Store(0)
			mapError.Store(0)
		}
	}()

	handleMappedMetric := func(m tlstatshouse.MetricBytes, h data_model.MappedMetricHeader) {
		if h.IngestionStatus != 0 {
			mapError.Inc()
			return
		}
		if h.Key.Metric != 1 {
			wrongID.Inc()
			return
		}
		if testFastPath && h.Key.Keys[1] != 1 {
			wrongTag1.Inc()
			return
		}
		goodMetric.Inc()
	}
	metricStorage := metajournal.MakeMetricsStorage("", nil, nil)
	metricStorage.Journal().Start(nil, nil, dolphinLoader)
	mapper := mapping.NewMapper("", pmcLoader, nil, nil, 1000, handleMappedMetric)

	recv, err := receiver.ListenUDP("udp", listenAddr, receiver.DefaultConnBufSize, true, nil, nil)
	if err != nil {
		log.Fatalf("%v", err)
	}
	recv2, err := receiver.ListenUDP("udp", listenAddr, receiver.DefaultConnBufSize, true, nil, nil)
	if err != nil {
		log.Fatalf("%v", err)
	}
	writeFunc := func() {
		conn, err := net.Dial("udp", listenAddr)
		if err != nil {
			log.Fatalf("[statlogs] failed to dial statshouse: %v", err)
			return
		}
		uconn := conn.(*net.UDPConn)
		args := tlstatshouse.AddMetricsBatchBytes{
			FieldsMask: 0,
			Metrics: []tlstatshouse.MetricBytes{{
				FieldsMask: 0,
				Name:       []byte("metric1"),
				Tags: []tl.DictionaryFieldStringBytes{{Key: []byte("1"), Value: []byte(keyPrefix + "1")},
					{Key: []byte("2"), Value: []byte(keyPrefix + "2")},
					{Key: []byte("3"), Value: []byte(keyPrefix + "3")},
					{Key: []byte("4"), Value: []byte("404")},
					{Key: []byte("5"), Value: []byte(keyPrefix + "5")},
				},
			}},
		}
		args.Metrics[0].SetCounter(5)
		args.Metrics[0].SetValue([]float64{1, 2, 3, 4, 5})
		var w []byte
		for len(w) < 60000 {
			w, _ = args.WriteBoxed(w)
		}
		// _ = args.WriteBoxed(&w)
		for {
			if !testFastPath {
				w = w[:0]
				args.Metrics[0].Tags[0].Value = append(args.Metrics[0].Tags[0].Value[:0], keyPrefix...)
				args.Metrics[0].Tags[0].Value = strconv.AppendInt(args.Metrics[0].Tags[0].Value, int64(rand.New().Int31()), 10)
				w, _ = args.WriteBoxed(w)
			}
			sentMetric.Inc()
			if _, err := uconn.Write(w); err != nil {
				log.Fatalf("[statlogs] failed to write statshouse: %v", err)
			}
			// if sentMetric%1000 == 0 { uncomment to limit sending speed
			//	time.Sleep(time.Millisecond)
			// }
		}
	}
	//  linux performs stable sharding to receivers by hash of source port|dst port, so you may need several launches until load is spread
	go writeFunc() // Uncomment writers as needed.
	go writeFunc()
	// go writeFunc()
	// go writeFunc()
	serveFunc := func(u *receiver.UDP, rm *atomic.Int64) error {
		return u.Serve(receiver.CallbackHandler{
			Metrics: func(m *tlstatshouse.MetricBytes, cb data_model.MapCallbackFunc) (h data_model.MappedMetricHeader, done bool) {
				r := rm.Inc()
				if almostReceiveOnly && r%1024 != 0 {
					return h, true
				}
				h, done = mapper.Map(data_model.HandlerArgs{MetricBytes: m, MapCallback: cb}, metricStorage.GetMetaMetricByNameBytes(m.Name))
				if done {
					handleMappedMetric(*m, h)
				}
				return h, done
			},
			ParseError: func(pkt []byte, err error) {
				parseErrors.Inc()
			},
		})
	}
	go func() {
		serveErr2 := serveFunc(recv2, &recvMetric2)
		if serveErr2 != nil {
			log.Fatalf("%v", serveErr2)
		}
	}()
	serveErr := serveFunc(recv, &recvMetric)
	if serveErr != nil {
		log.Fatalf("%v", serveErr)
	}
}

func mainTLClient() int {
	flag.StringVar(&argv.aesPwdFile, "aes-pwd-file", "", "path to AES password file, will try to read "+defaultPathToPwd+" if not set")

	var statshouseAddr string
	var statshouseNet string
	flag.StringVar(&statshouseNet, "statshouse-net", "tcp4", "statshouse network for tlclient")
	flag.StringVar(&statshouseAddr, "statshouse-addr", "127.0.0.1:13337", "statshouse address for tlclient")

	build.FlagParseShowVersionHelp()

	client, _ := argvCreateClient()

	// use like this
	// echo '{"metrics":[{"name":"gbuteyko_investigation","tags":{"env":"dev","1":"I_test_statshouse","2":"1"},"counter":1}]}' | /usr/share/engine/bin/statshouse --new-conveyor=tlclient --statshouse-addr=localhost:13333
	tlclient := tlstatshouse.Client{
		Client:  client,
		Network: statshouseNet,
		Address: statshouseAddr,
	}
	pkt, err := io.ReadAll(os.Stdin)
	if err != nil && err != io.EOF {
		_, _ = fmt.Fprintf(os.Stderr, "read JSON from stdin failed - %v", err)
		return 1
	}
	var batch tlstatshouse.AddMetricsBatchBytes
	if err := batch.UnmarshalJSON(pkt); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "parsing metric batch failed - %v", err)
		return 1
	}
	var ret tl.True
	if err := tlclient.AddMetricsBatchBytes(context.Background(), batch, nil, &ret); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "addMetricsBatch failed - %v", err)
		return 1
	}
	log.Printf("Success")
	return 0
}

func mainTLClientAPI() {
	flag.StringVar(&argv.aesPwdFile, "aes-pwd-file", "", "path to AES password file, will try to read "+defaultPathToPwd+" if not set")

	build.FlagParseShowVersionHelp()

	client, _ := argvCreateClient()

	tlapiclient := tlstatshouseApi.Client{
		Client:  client,
		Network: "tcp4",
		Address: "127.0.0.1:13347",
	}
	var requests []tlstatshouseApi.GetQuery
	query := tlstatshouseApi.Query{
		Version:    2,
		TopN:       -1,
		MetricName: "__agg_bucket_receive_delay_sec",
		TimeFrom:   1649764763,
		TimeTo:     1658404763,
		Interval:   "86400s",
		Function:   tlstatshouseApi.FnCount(),
	}
	query.SetWhat([]tlstatshouseApi.Function{tlstatshouseApi.FnCount()})
	rr := tlstatshouseApi.GetQuery{
		Query: query,
	}
	requests = append(requests, rr)

	rr.Query.GroupBy = []string{"2", "3", "4", "5", "6"}
	// requests = append(requests, rr)

	// rr.Query.TimeShift = []int64{-86400}
	rr.Query.Filter = []tlstatshouseApi.Filter{
		{
			Key: "1",
			Values: []tlstatshouseApi.TagValue{
				{
					In:    true,
					Flag:  tlstatshouseApi.FlagRaw(),
					Value: "348099029",
				},
			},
		},
	}
	requests = append(requests, rr)

	ctx := context.Background()
	for _, request := range requests {
		var ret tlstatshouseApi.GetQueryResponse
		if err := tlapiclient.GetQuery(ctx, request, nil, &ret); err != nil {
			log.Fatalf("tlapiclient.GetQuery failed - %v", err)
		}

		log.Printf("response: %v\n\n", ret)
		log.Printf("points: %v\n\n", len(ret.Series.Time))

		for _, cid := range ret.ChunkIds {
			var rc tlstatshouseApi.GetChunkResponse
			if err := tlapiclient.GetChunk(ctx, tlstatshouseApi.GetChunk{ResponseId: ret.ResponseId, ChunkId: cid}, nil, &rc); err != nil {
				log.Fatalf("tlapiclient.GetChunk failed - %v", err)
			}

			log.Printf("chunk response: %v\n\n", rc)
		}

		if ret.ResponseId > 0 {
			var cc tlstatshouseApi.ReleaseChunksResponse
			if err := tlapiclient.ReleaseChunks(ctx, tlstatshouseApi.ReleaseChunks{ResponseId: ret.ResponseId}, nil, &cc); err != nil {
				log.Fatalf("tlapiclient.ReleaseChunks failed - %v", err)
			}
			log.Printf("ReleaseChunks response: %#v\n\n", cc)
		}
	}
}

func mainSimulator() {
	argvAddCommonFlags()
	argvAddAgentFlags(false)

	build.FlagParseShowVersionHelp()

	argv.configAgent.Cluster = argv.cluster
	argv.configAgent.SampleBudget /= 10
	if err := argv.configAgent.ValidateConfigSource(); err != nil {
		log.Fatalf("%s", err)
	}

	argv.configAgent.AggregatorAddresses = strings.Split(argv.aggAddr, ",")

	metricStorage := metajournal.MakeMetricsStorage("simulator", nil, nil)

	client, cryptoKey := argvCreateClient()

	metaDataClient := &tlmetadata.Client{
		Client:  client,
		Network: "tcp4",
		Address: "127.0.0.1:2442",
	}
	loader := metajournal.NewMetricMetaLoader(metaDataClient, metajournal.DefaultMetaTimeout)
	metricStorage.Journal().Start(nil, nil, loader.LoadJournal)
	time.Sleep(time.Second) // enough to sync in testing

	for i := 1; i < 20; i++ {
		m := format.MetricMetaValue{
			Name:          data_model.SimulatorMetricPrefix + strconv.Itoa(i),
			Description:   "Simulator metric " + strconv.Itoa(i),
			Visible:       true,
			Kind:          format.MetricKindMixed,
			Resolution:    1,
			Weight:        1,
			StringTopName: "stop",
		}
		if i == 2 || i == 3 {
			m.Resolution = 10
		}
		if i == 7 || i == 8 {
			m.Weight = 2
		}
		if i == 3 || i == 11 {
			m.Kind = format.MetricKindMixedPercentiles
		}
		for t := 0; t < 10; t++ {
			m.Tags = append(m.Tags, format.MetricMetaTag{
				Raw: true,
			})
		}
		if err := m.RestoreCachedInfo(); err != nil {
			log.Panicf("Simulator metric contains error: %v", err)
		}
		metricInfo := metricStorage.GetMetaMetricByName(m.Name)
		if metricInfo != nil { // will create or update
			m.MetricID = metricInfo.MetricID
			m.Version = metricInfo.Version
		}
		ms, err := loader.SaveMetric(context.Background(), m, "")
		if err != nil {
			log.Panicf("Failed to create simulator metric: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		if err := metricStorage.Journal().WaitVersion(ctx, ms.Version); err != nil {
			log.Panicf("Failed to create simulator metric: %v", err)
		}
		cancel()
	}

	for i := 1; i < 10; i++ {
		go aggregator.RunSimulator(i, metricStorage, argv.cacheDir, cryptoKey, argv.configAgent)
	}
	aggregator.RunSimulator(0, metricStorage, argv.cacheDir, cryptoKey, argv.configAgent)
}

func mainTagMapping() {
	// Parse command line tags
	var (
		metric          string
		tags            string
		budget          int
		metadataNet     string
		metadataAddr    string
		metadataActorID int64
	)
	flag.StringVar(&metric, "metric", "", "metric name, if specified then strings are considered metric tags")
	flag.StringVar(&tags, "tag", "", "string to be searched for a int32 mapping")
	flag.IntVar(&budget, "budget", 0, "mapping budget to set")
	flag.Int64Var(&metadataActorID, "metadata-actor-id", 0, "")
	flag.StringVar(&metadataAddr, "metadata-addr", "127.0.0.1:2442", "")
	flag.StringVar(&metadataNet, "metadata-net", "tcp4", "")
	flag.StringVar(&argv.aesPwdFile, "aes-pwd-file", "", "path to AES password file, will try to read "+defaultPathToPwd+" if not set")
	build.FlagParseShowVersionHelp()
	flag.Parse()
	// Create metadata client
	var (
		aesPwd = readAESPwd()
		client = tlmetadata.Client{
			Client:  rpc.NewClient(rpc.ClientWithLogf(log.Printf), rpc.ClientWithCryptoKey(aesPwd), rpc.ClientWithTrustedSubnetGroups(build.TrustedSubnetGroups())),
			Network: metadataNet,
			Address: metadataAddr,
			ActorID: metadataActorID,
		}
	)
	// Run tag mapping queries
	for _, tag := range strings.Split(tags, ",") {
		if len(tag) == 0 {
			continue
		}
		var (
			qry = tlmetadata.GetMapping{Metric: metric, Key: tag}
			ret tlmetadata.GetMappingResponse
			err = client.GetMapping(context.Background(), qry, nil, &ret)
		)
		if err != nil {
			fmt.Printf("%q ERROR <%v>\n", tag, err)
		} else if res, ok := ret.AsGetMappingResponse(); ok {
			fmt.Printf("%q -> %d\n", tag, res.Id)
		} else {
			fmt.Printf("%q NOT FOUND\n", tag)
		}
		fmt.Println()
	}
	if budget != 0 {
		var ( // Set mapping budget
			arg = tlmetadata.ResetFlood2{Metric: metric}
			res tlmetadata.ResetFloodResponse2
		)
		if budget > 0 {
			arg.SetValue(int32(budget))
		}
		err := client.ResetFlood2(context.Background(), arg, nil, &res)
		if err == nil {
			fmt.Printf("%q set mapping budget %d, was %d, now %d\n", metric, budget, res.BudgetBefore, res.BudgetAfter)
		} else {
			fmt.Printf("%q ERROR <%v> setting mapping budget %d\n", metric, err, budget)
		}
	}
}

func mainPublishTagDrafts() {
	var metricType = map[string]string{
		"etcd_cluster_version": "gauge",
		"etcd_debugging_disk_backend_commit_rebalance_duration_seconds_bucket":     "histogram",
		"etcd_debugging_disk_backend_commit_spill_duration_seconds_bucket":         "histogram",
		"etcd_debugging_disk_backend_commit_write_duration_seconds_bucket":         "histogram",
		"etcd_debugging_lease_granted_total":                                       "counter",
		"etcd_debugging_lease_renewed_total":                                       "counter",
		"etcd_debugging_lease_revoked_total":                                       "counter",
		"etcd_debugging_lease_ttl_total_bucket":                                    "histogram",
		"etcd_debugging_mvcc_db_compaction_keys_total":                             "counter",
		"etcd_debugging_mvcc_db_compaction_pause_duration_milliseconds_bucket":     "histogram",
		"etcd_debugging_mvcc_db_compaction_total_duration_milliseconds_bucket":     "histogram",
		"etcd_debugging_mvcc_db_total_size_in_bytes":                               "gauge",
		"etcd_debugging_mvcc_delete_total":                                         "counter",
		"etcd_debugging_mvcc_events_total":                                         "counter",
		"etcd_debugging_mvcc_index_compaction_pause_duration_milliseconds_bucket":  "histogram",
		"etcd_debugging_mvcc_keys_total":                                           "gauge",
		"etcd_debugging_mvcc_pending_events_total":                                 "gauge",
		"etcd_debugging_mvcc_put_total":                                            "counter",
		"etcd_debugging_mvcc_range_total":                                          "counter",
		"etcd_debugging_mvcc_slow_watcher_total":                                   "gauge",
		"etcd_debugging_mvcc_txn_total":                                            "counter",
		"etcd_debugging_mvcc_watch_stream_total":                                   "gauge",
		"etcd_debugging_mvcc_watcher_total":                                        "gauge",
		"etcd_debugging_server_lease_expired_total":                                "counter",
		"etcd_debugging_snap_save_marshalling_duration_seconds_bucket":             "histogram",
		"etcd_debugging_snap_save_total_duration_seconds_bucket":                   "histogram",
		"etcd_debugging_store_expires_total":                                       "counter",
		"etcd_debugging_store_reads_total":                                         "counter",
		"etcd_debugging_store_watch_requests_total":                                "counter",
		"etcd_debugging_store_watchers":                                            "gauge",
		"etcd_debugging_store_writes_total":                                        "counter",
		"etcd_disk_backend_commit_duration_seconds_bucket":                         "histogram",
		"etcd_disk_backend_defrag_duration_seconds_bucket":                         "histogram",
		"etcd_disk_backend_snapshot_duration_seconds_bucket":                       "histogram",
		"etcd_disk_wal_fsync_duration_seconds_bucket":                              "histogram",
		"etcd_grpc_proxy_cache_hits_total":                                         "gauge",
		"etcd_grpc_proxy_cache_keys_total":                                         "gauge",
		"etcd_grpc_proxy_cache_misses_total":                                       "gauge",
		"etcd_grpc_proxy_events_coalescing_total":                                  "counter",
		"etcd_grpc_proxy_watchers_coalescing_total":                                "gauge",
		"etcd_mvcc_db_open_read_transactions":                                      "gauge",
		"etcd_mvcc_db_total_size_in_bytes":                                         "gauge",
		"etcd_mvcc_db_total_size_in_use_in_bytes":                                  "gauge",
		"etcd_mvcc_delete_total":                                                   "counter",
		"etcd_mvcc_hash_duration_seconds_bucket":                                   "histogram",
		"etcd_mvcc_hash_rev_duration_seconds_bucket":                               "histogram",
		"etcd_mvcc_put_total":                                                      "counter",
		"etcd_mvcc_range_total":                                                    "counter",
		"etcd_mvcc_txn_total":                                                      "counter",
		"etcd_network_active_peers":                                                "gauge",
		"etcd_network_client_grpc_received_bytes_total":                            "counter",
		"etcd_network_client_grpc_sent_bytes_total":                                "counter",
		"etcd_network_peer_received_bytes_total":                                   "counter",
		"etcd_network_peer_round_trip_time_seconds_bucket":                         "histogram",
		"etcd_network_peer_sent_bytes_total":                                       "counter",
		"etcd_server_go_version":                                                   "gauge",
		"etcd_server_has_leader":                                                   "gauge",
		"etcd_server_health_failures":                                              "counter",
		"etcd_server_health_success":                                               "counter",
		"etcd_server_heartbeat_send_failures_total":                                "counter",
		"etcd_server_id":                                                           "gauge",
		"etcd_server_is_leader":                                                    "gauge",
		"etcd_server_is_learner":                                                   "gauge",
		"etcd_server_leader_changes_seen_total":                                    "counter",
		"etcd_server_learner_promote_successes":                                    "counter",
		"etcd_server_proposals_applied_total":                                      "gauge",
		"etcd_server_proposals_committed_total":                                    "gauge",
		"etcd_server_proposals_failed_total":                                       "counter",
		"etcd_server_proposals_pending":                                            "gauge",
		"etcd_server_quota_backend_bytes":                                          "gauge",
		"etcd_server_read_indexes_failed_total":                                    "counter",
		"etcd_server_slow_apply_total":                                             "counter",
		"etcd_server_slow_read_indexes_total":                                      "counter",
		"etcd_server_snapshot_apply_in_progress_total":                             "gauge",
		"etcd_server_version":                                                      "gauge",
		"etcd_snap_db_fsync_duration_seconds_bucket":                               "histogram",
		"etcd_snap_db_save_total_duration_seconds_bucket":                          "histogram",
		"etcd_snap_fsync_duration_seconds_bucket":                                  "histogram",
		"go_gc_duration_seconds":                                                   "summary",
		"go_goroutines":                                                            "gauge",
		"go_info":                                                                  "gauge",
		"go_memstats_alloc_bytes":                                                  "gauge",
		"go_memstats_alloc_bytes_total":                                            "counter",
		"go_memstats_buck_hash_sys_bytes":                                          "gauge",
		"go_memstats_frees_total":                                                  "counter",
		"go_memstats_gc_cpu_fraction":                                              "gauge",
		"go_memstats_gc_sys_bytes":                                                 "gauge",
		"go_memstats_heap_alloc_bytes":                                             "gauge",
		"go_memstats_heap_idle_bytes":                                              "gauge",
		"go_memstats_heap_inuse_bytes":                                             "gauge",
		"go_memstats_heap_objects":                                                 "gauge",
		"go_memstats_heap_released_bytes":                                          "gauge",
		"go_memstats_heap_sys_bytes":                                               "gauge",
		"go_memstats_last_gc_time_seconds":                                         "gauge",
		"go_memstats_lookups_total":                                                "counter",
		"go_memstats_mallocs_total":                                                "counter",
		"go_memstats_mcache_inuse_bytes":                                           "gauge",
		"go_memstats_mcache_sys_bytes":                                             "gauge",
		"go_memstats_mspan_inuse_bytes":                                            "gauge",
		"go_memstats_mspan_sys_bytes":                                              "gauge",
		"go_memstats_next_gc_bytes":                                                "gauge",
		"go_memstats_other_sys_bytes":                                              "gauge",
		"go_memstats_stack_inuse_bytes":                                            "gauge",
		"go_memstats_stack_sys_bytes":                                              "gauge",
		"go_memstats_sys_bytes":                                                    "gauge",
		"go_threads":                                                               "gauge",
		"grpc_server_handled_total":                                                "counter",
		"grpc_server_msg_received_total":                                           "counter",
		"grpc_server_msg_sent_total":                                               "counter",
		"grpc_server_started_total":                                                "counter",
		"promhttp_metric_handler_requests_in_flight":                               "gauge",
		"promhttp_metric_handler_requests_total":                                   "counter",
		"ClickHouseProfileEvents_Query":                                            "counter",
		"ClickHouseProfileEvents_SelectQuery":                                      "counter",
		"ClickHouseProfileEvents_InsertQuery":                                      "counter",
		"ClickHouseProfileEvents_AsyncInsertQuery":                                 "counter",
		"ClickHouseProfileEvents_AsyncInsertBytes":                                 "counter",
		"ClickHouseProfileEvents_FailedQuery":                                      "counter",
		"ClickHouseProfileEvents_FailedSelectQuery":                                "counter",
		"ClickHouseProfileEvents_FailedInsertQuery":                                "counter",
		"ClickHouseProfileEvents_QueryTimeMicroseconds":                            "counter",
		"ClickHouseProfileEvents_SelectQueryTimeMicroseconds":                      "counter",
		"ClickHouseProfileEvents_InsertQueryTimeMicroseconds":                      "counter",
		"ClickHouseProfileEvents_OtherQueryTimeMicroseconds":                       "counter",
		"ClickHouseProfileEvents_FileOpen":                                         "counter",
		"ClickHouseProfileEvents_Seek":                                             "counter",
		"ClickHouseProfileEvents_ReadBufferFromFileDescriptorRead":                 "counter",
		"ClickHouseProfileEvents_ReadBufferFromFileDescriptorReadFailed":           "counter",
		"ClickHouseProfileEvents_ReadBufferFromFileDescriptorReadBytes":            "counter",
		"ClickHouseProfileEvents_WriteBufferFromFileDescriptorWrite":               "counter",
		"ClickHouseProfileEvents_WriteBufferFromFileDescriptorWriteFailed":         "counter",
		"ClickHouseProfileEvents_WriteBufferFromFileDescriptorWriteBytes":          "counter",
		"ClickHouseProfileEvents_FileSync":                                         "counter",
		"ClickHouseProfileEvents_DirectorySync":                                    "counter",
		"ClickHouseProfileEvents_FileSyncElapsedMicroseconds":                      "counter",
		"ClickHouseProfileEvents_DirectorySyncElapsedMicroseconds":                 "counter",
		"ClickHouseProfileEvents_ReadCompressedBytes":                              "counter",
		"ClickHouseProfileEvents_CompressedReadBufferBlocks":                       "counter",
		"ClickHouseProfileEvents_CompressedReadBufferBytes":                        "counter",
		"ClickHouseProfileEvents_UncompressedCacheHits":                            "counter",
		"ClickHouseProfileEvents_UncompressedCacheMisses":                          "counter",
		"ClickHouseProfileEvents_UncompressedCacheWeightLost":                      "counter",
		"ClickHouseProfileEvents_MMappedFileCacheHits":                             "counter",
		"ClickHouseProfileEvents_MMappedFileCacheMisses":                           "counter",
		"ClickHouseProfileEvents_OpenedFileCacheHits":                              "counter",
		"ClickHouseProfileEvents_OpenedFileCacheMisses":                            "counter",
		"ClickHouseProfileEvents_AIOWrite":                                         "counter",
		"ClickHouseProfileEvents_AIOWriteBytes":                                    "counter",
		"ClickHouseProfileEvents_AIORead":                                          "counter",
		"ClickHouseProfileEvents_AIOReadBytes":                                     "counter",
		"ClickHouseProfileEvents_IOBufferAllocs":                                   "counter",
		"ClickHouseProfileEvents_IOBufferAllocBytes":                               "counter",
		"ClickHouseProfileEvents_ArenaAllocChunks":                                 "counter",
		"ClickHouseProfileEvents_ArenaAllocBytes":                                  "counter",
		"ClickHouseProfileEvents_FunctionExecute":                                  "counter",
		"ClickHouseProfileEvents_TableFunctionExecute":                             "counter",
		"ClickHouseProfileEvents_MarkCacheHits":                                    "counter",
		"ClickHouseProfileEvents_MarkCacheMisses":                                  "counter",
		"ClickHouseProfileEvents_CreatedReadBufferOrdinary":                        "counter",
		"ClickHouseProfileEvents_CreatedReadBufferDirectIO":                        "counter",
		"ClickHouseProfileEvents_CreatedReadBufferDirectIOFailed":                  "counter",
		"ClickHouseProfileEvents_CreatedReadBufferMMap":                            "counter",
		"ClickHouseProfileEvents_CreatedReadBufferMMapFailed":                      "counter",
		"ClickHouseProfileEvents_DiskReadElapsedMicroseconds":                      "counter",
		"ClickHouseProfileEvents_DiskWriteElapsedMicroseconds":                     "counter",
		"ClickHouseProfileEvents_NetworkReceiveElapsedMicroseconds":                "counter",
		"ClickHouseProfileEvents_NetworkSendElapsedMicroseconds":                   "counter",
		"ClickHouseProfileEvents_NetworkReceiveBytes":                              "counter",
		"ClickHouseProfileEvents_NetworkSendBytes":                                 "counter",
		"ClickHouseProfileEvents_ThrottlerSleepMicroseconds":                       "counter",
		"ClickHouseProfileEvents_QueryMaskingRulesMatch":                           "counter",
		"ClickHouseProfileEvents_ReplicatedPartFetches":                            "counter",
		"ClickHouseProfileEvents_ReplicatedPartFailedFetches":                      "counter",
		"ClickHouseProfileEvents_ObsoleteReplicatedParts":                          "counter",
		"ClickHouseProfileEvents_ReplicatedPartMerges":                             "counter",
		"ClickHouseProfileEvents_ReplicatedPartFetchesOfMerged":                    "counter",
		"ClickHouseProfileEvents_ReplicatedPartMutations":                          "counter",
		"ClickHouseProfileEvents_ReplicatedPartChecks":                             "counter",
		"ClickHouseProfileEvents_ReplicatedPartChecksFailed":                       "counter",
		"ClickHouseProfileEvents_ReplicatedDataLoss":                               "counter",
		"ClickHouseProfileEvents_InsertedRows":                                     "counter",
		"ClickHouseProfileEvents_InsertedBytes":                                    "counter",
		"ClickHouseProfileEvents_DelayedInserts":                                   "counter",
		"ClickHouseProfileEvents_RejectedInserts":                                  "counter",
		"ClickHouseProfileEvents_DelayedInsertsMilliseconds":                       "counter",
		"ClickHouseProfileEvents_DistributedDelayedInserts":                        "counter",
		"ClickHouseProfileEvents_DistributedRejectedInserts":                       "counter",
		"ClickHouseProfileEvents_DistributedDelayedInsertsMilliseconds":            "counter",
		"ClickHouseProfileEvents_DuplicatedInsertedBlocks":                         "counter",
		"ClickHouseProfileEvents_ZooKeeperInit":                                    "counter",
		"ClickHouseProfileEvents_ZooKeeperTransactions":                            "counter",
		"ClickHouseProfileEvents_ZooKeeperList":                                    "counter",
		"ClickHouseProfileEvents_ZooKeeperCreate":                                  "counter",
		"ClickHouseProfileEvents_ZooKeeperRemove":                                  "counter",
		"ClickHouseProfileEvents_ZooKeeperExists":                                  "counter",
		"ClickHouseProfileEvents_ZooKeeperGet":                                     "counter",
		"ClickHouseProfileEvents_ZooKeeperSet":                                     "counter",
		"ClickHouseProfileEvents_ZooKeeperMulti":                                   "counter",
		"ClickHouseProfileEvents_ZooKeeperCheck":                                   "counter",
		"ClickHouseProfileEvents_ZooKeeperSync":                                    "counter",
		"ClickHouseProfileEvents_ZooKeeperClose":                                   "counter",
		"ClickHouseProfileEvents_ZooKeeperWatchResponse":                           "counter",
		"ClickHouseProfileEvents_ZooKeeperUserExceptions":                          "counter",
		"ClickHouseProfileEvents_ZooKeeperHardwareExceptions":                      "counter",
		"ClickHouseProfileEvents_ZooKeeperOtherExceptions":                         "counter",
		"ClickHouseProfileEvents_ZooKeeperWaitMicroseconds":                        "counter",
		"ClickHouseProfileEvents_ZooKeeperBytesSent":                               "counter",
		"ClickHouseProfileEvents_ZooKeeperBytesReceived":                           "counter",
		"ClickHouseProfileEvents_DistributedConnectionFailTry":                     "counter",
		"ClickHouseProfileEvents_DistributedConnectionMissingTable":                "counter",
		"ClickHouseProfileEvents_DistributedConnectionStaleReplica":                "counter",
		"ClickHouseProfileEvents_DistributedConnectionFailAtAll":                   "counter",
		"ClickHouseProfileEvents_HedgedRequestsChangeReplica":                      "counter",
		"ClickHouseProfileEvents_CompileFunction":                                  "counter",
		"ClickHouseProfileEvents_CompiledFunctionExecute":                          "counter",
		"ClickHouseProfileEvents_CompileExpressionsMicroseconds":                   "counter",
		"ClickHouseProfileEvents_CompileExpressionsBytes":                          "counter",
		"ClickHouseProfileEvents_ExecuteShellCommand":                              "counter",
		"ClickHouseProfileEvents_ExternalSortWritePart":                            "counter",
		"ClickHouseProfileEvents_ExternalSortMerge":                                "counter",
		"ClickHouseProfileEvents_ExternalAggregationWritePart":                     "counter",
		"ClickHouseProfileEvents_ExternalAggregationMerge":                         "counter",
		"ClickHouseProfileEvents_ExternalAggregationCompressedBytes":               "counter",
		"ClickHouseProfileEvents_ExternalAggregationUncompressedBytes":             "counter",
		"ClickHouseProfileEvents_SlowRead":                                         "counter",
		"ClickHouseProfileEvents_ReadBackoff":                                      "counter",
		"ClickHouseProfileEvents_ReplicaPartialShutdown":                           "counter",
		"ClickHouseProfileEvents_SelectedParts":                                    "counter",
		"ClickHouseProfileEvents_SelectedRanges":                                   "counter",
		"ClickHouseProfileEvents_SelectedMarks":                                    "counter",
		"ClickHouseProfileEvents_SelectedRows":                                     "counter",
		"ClickHouseProfileEvents_SelectedBytes":                                    "counter",
		"ClickHouseProfileEvents_Merge":                                            "counter",
		"ClickHouseProfileEvents_MergedRows":                                       "counter",
		"ClickHouseProfileEvents_MergedUncompressedBytes":                          "counter",
		"ClickHouseProfileEvents_MergesTimeMilliseconds":                           "counter",
		"ClickHouseProfileEvents_MergeTreeDataWriterRows":                          "counter",
		"ClickHouseProfileEvents_MergeTreeDataWriterUncompressedBytes":             "counter",
		"ClickHouseProfileEvents_MergeTreeDataWriterCompressedBytes":               "counter",
		"ClickHouseProfileEvents_MergeTreeDataWriterBlocks":                        "counter",
		"ClickHouseProfileEvents_MergeTreeDataWriterBlocksAlreadySorted":           "counter",
		"ClickHouseProfileEvents_InsertedWideParts":                                "counter",
		"ClickHouseProfileEvents_InsertedCompactParts":                             "counter",
		"ClickHouseProfileEvents_InsertedInMemoryParts":                            "counter",
		"ClickHouseProfileEvents_MergedIntoWideParts":                              "counter",
		"ClickHouseProfileEvents_MergedIntoCompactParts":                           "counter",
		"ClickHouseProfileEvents_MergedIntoInMemoryParts":                          "counter",
		"ClickHouseProfileEvents_MergeTreeDataProjectionWriterRows":                "counter",
		"ClickHouseProfileEvents_MergeTreeDataProjectionWriterUncompressedBytes":   "counter",
		"ClickHouseProfileEvents_MergeTreeDataProjectionWriterCompressedBytes":     "counter",
		"ClickHouseProfileEvents_MergeTreeDataProjectionWriterBlocks":              "counter",
		"ClickHouseProfileEvents_MergeTreeDataProjectionWriterBlocksAlreadySorted": "counter",
		"ClickHouseProfileEvents_CannotRemoveEphemeralNode":                        "counter",
		"ClickHouseProfileEvents_RegexpCreated":                                    "counter",
		"ClickHouseProfileEvents_ContextLock":                                      "counter",
		"ClickHouseProfileEvents_StorageBufferFlush":                               "counter",
		"ClickHouseProfileEvents_StorageBufferErrorOnFlush":                        "counter",
		"ClickHouseProfileEvents_StorageBufferPassedAllMinThresholds":              "counter",
		"ClickHouseProfileEvents_StorageBufferPassedTimeMaxThreshold":              "counter",
		"ClickHouseProfileEvents_StorageBufferPassedRowsMaxThreshold":              "counter",
		"ClickHouseProfileEvents_StorageBufferPassedBytesMaxThreshold":             "counter",
		"ClickHouseProfileEvents_StorageBufferPassedTimeFlushThreshold":            "counter",
		"ClickHouseProfileEvents_StorageBufferPassedRowsFlushThreshold":            "counter",
		"ClickHouseProfileEvents_StorageBufferPassedBytesFlushThreshold":           "counter",
		"ClickHouseProfileEvents_StorageBufferLayerLockReadersWaitMilliseconds":    "counter",
		"ClickHouseProfileEvents_StorageBufferLayerLockWritersWaitMilliseconds":    "counter",
		"ClickHouseProfileEvents_DictCacheKeysRequested":                           "counter",
		"ClickHouseProfileEvents_DictCacheKeysRequestedMiss":                       "counter",
		"ClickHouseProfileEvents_DictCacheKeysRequestedFound":                      "counter",
		"ClickHouseProfileEvents_DictCacheKeysExpired":                             "counter",
		"ClickHouseProfileEvents_DictCacheKeysNotFound":                            "counter",
		"ClickHouseProfileEvents_DictCacheKeysHit":                                 "counter",
		"ClickHouseProfileEvents_DictCacheRequestTimeNs":                           "counter",
		"ClickHouseProfileEvents_DictCacheRequests":                                "counter",
		"ClickHouseProfileEvents_DictCacheLockWriteNs":                             "counter",
		"ClickHouseProfileEvents_DictCacheLockReadNs":                              "counter",
		"ClickHouseProfileEvents_DistributedSyncInsertionTimeoutExceeded":          "counter",
		"ClickHouseProfileEvents_DataAfterMergeDiffersFromReplica":                 "counter",
		"ClickHouseProfileEvents_DataAfterMutationDiffersFromReplica":              "counter",
		"ClickHouseProfileEvents_PolygonsAddedToPool":                              "counter",
		"ClickHouseProfileEvents_PolygonsInPoolAllocatedBytes":                     "counter",
		"ClickHouseProfileEvents_RWLockAcquiredReadLocks":                          "counter",
		"ClickHouseProfileEvents_RWLockAcquiredWriteLocks":                         "counter",
		"ClickHouseProfileEvents_RWLockReadersWaitMilliseconds":                    "counter",
		"ClickHouseProfileEvents_RWLockWritersWaitMilliseconds":                    "counter",
		"ClickHouseProfileEvents_DNSError":                                         "counter",
		"ClickHouseProfileEvents_RealTimeMicroseconds":                             "counter",
		"ClickHouseProfileEvents_UserTimeMicroseconds":                             "counter",
		"ClickHouseProfileEvents_SystemTimeMicroseconds":                           "counter",
		"ClickHouseProfileEvents_MemoryOvercommitWaitTimeMicroseconds":             "counter",
		"ClickHouseProfileEvents_SoftPageFaults":                                   "counter",
		"ClickHouseProfileEvents_HardPageFaults":                                   "counter",
		"ClickHouseProfileEvents_OSIOWaitMicroseconds":                             "counter",
		"ClickHouseProfileEvents_OSCPUWaitMicroseconds":                            "counter",
		"ClickHouseProfileEvents_OSCPUVirtualTimeMicroseconds":                     "counter",
		"ClickHouseProfileEvents_OSReadBytes":                                      "counter",
		"ClickHouseProfileEvents_OSWriteBytes":                                     "counter",
		"ClickHouseProfileEvents_OSReadChars":                                      "counter",
		"ClickHouseProfileEvents_OSWriteChars":                                     "counter",
		"ClickHouseProfileEvents_PerfCpuCycles":                                    "counter",
		"ClickHouseProfileEvents_PerfInstructions":                                 "counter",
		"ClickHouseProfileEvents_PerfCacheReferences":                              "counter",
		"ClickHouseProfileEvents_PerfCacheMisses":                                  "counter",
		"ClickHouseProfileEvents_PerfBranchInstructions":                           "counter",
		"ClickHouseProfileEvents_PerfBranchMisses":                                 "counter",
		"ClickHouseProfileEvents_PerfBusCycles":                                    "counter",
		"ClickHouseProfileEvents_PerfStalledCyclesFrontend":                        "counter",
		"ClickHouseProfileEvents_PerfStalledCyclesBackend":                         "counter",
		"ClickHouseProfileEvents_PerfRefCpuCycles":                                 "counter",
		"ClickHouseProfileEvents_PerfCpuClock":                                     "counter",
		"ClickHouseProfileEvents_PerfTaskClock":                                    "counter",
		"ClickHouseProfileEvents_PerfContextSwitches":                              "counter",
		"ClickHouseProfileEvents_PerfCpuMigrations":                                "counter",
		"ClickHouseProfileEvents_PerfAlignmentFaults":                              "counter",
		"ClickHouseProfileEvents_PerfEmulationFaults":                              "counter",
		"ClickHouseProfileEvents_PerfMinEnabledTime":                               "counter",
		"ClickHouseProfileEvents_PerfMinEnabledRunningTime":                        "counter",
		"ClickHouseProfileEvents_PerfDataTLBReferences":                            "counter",
		"ClickHouseProfileEvents_PerfDataTLBMisses":                                "counter",
		"ClickHouseProfileEvents_PerfInstructionTLBReferences":                     "counter",
		"ClickHouseProfileEvents_PerfInstructionTLBMisses":                         "counter",
		"ClickHouseProfileEvents_PerfLocalMemoryReferences":                        "counter",
		"ClickHouseProfileEvents_PerfLocalMemoryMisses":                            "counter",
		"ClickHouseProfileEvents_CreatedHTTPConnections":                           "counter",
		"ClickHouseProfileEvents_CannotWriteToWriteBufferDiscard":                  "counter",
		"ClickHouseProfileEvents_QueryProfilerSignalOverruns":                      "counter",
		"ClickHouseProfileEvents_QueryProfilerRuns":                                "counter",
		"ClickHouseProfileEvents_CreatedLogEntryForMerge":                          "counter",
		"ClickHouseProfileEvents_NotCreatedLogEntryForMerge":                       "counter",
		"ClickHouseProfileEvents_CreatedLogEntryForMutation":                       "counter",
		"ClickHouseProfileEvents_NotCreatedLogEntryForMutation":                    "counter",
		"ClickHouseProfileEvents_S3ReadMicroseconds":                               "counter",
		"ClickHouseProfileEvents_S3ReadRequestsCount":                              "counter",
		"ClickHouseProfileEvents_S3ReadRequestsErrors":                             "counter",
		"ClickHouseProfileEvents_S3ReadRequestsThrottling":                         "counter",
		"ClickHouseProfileEvents_S3ReadRequestsRedirects":                          "counter",
		"ClickHouseProfileEvents_S3WriteMicroseconds":                              "counter",
		"ClickHouseProfileEvents_S3WriteRequestsCount":                             "counter",
		"ClickHouseProfileEvents_S3WriteRequestsErrors":                            "counter",
		"ClickHouseProfileEvents_S3WriteRequestsThrottling":                        "counter",
		"ClickHouseProfileEvents_S3WriteRequestsRedirects":                         "counter",
		"ClickHouseProfileEvents_ReadBufferFromS3Microseconds":                     "counter",
		"ClickHouseProfileEvents_ReadBufferFromS3Bytes":                            "counter",
		"ClickHouseProfileEvents_ReadBufferFromS3RequestsErrors":                   "counter",
		"ClickHouseProfileEvents_WriteBufferFromS3Bytes":                           "counter",
		"ClickHouseProfileEvents_QueryMemoryLimitExceeded":                         "counter",
		"ClickHouseProfileEvents_CachedReadBufferReadFromSourceMicroseconds":       "counter",
		"ClickHouseProfileEvents_CachedReadBufferReadFromCacheMicroseconds":        "counter",
		"ClickHouseProfileEvents_CachedReadBufferReadFromSourceBytes":              "counter",
		"ClickHouseProfileEvents_CachedReadBufferReadFromCacheBytes":               "counter",
		"ClickHouseProfileEvents_CachedReadBufferCacheWriteBytes":                  "counter",
		"ClickHouseProfileEvents_CachedReadBufferCacheWriteMicroseconds":           "counter",
		"ClickHouseProfileEvents_CachedWriteBufferCacheWriteBytes":                 "counter",
		"ClickHouseProfileEvents_CachedWriteBufferCacheWriteMicroseconds":          "counter",
		"ClickHouseProfileEvents_RemoteFSSeeks":                                    "counter",
		"ClickHouseProfileEvents_RemoteFSPrefetches":                               "counter",
		"ClickHouseProfileEvents_RemoteFSCancelledPrefetches":                      "counter",
		"ClickHouseProfileEvents_RemoteFSUnusedPrefetches":                         "counter",
		"ClickHouseProfileEvents_RemoteFSPrefetchedReads":                          "counter",
		"ClickHouseProfileEvents_RemoteFSUnprefetchedReads":                        "counter",
		"ClickHouseProfileEvents_RemoteFSLazySeeks":                                "counter",
		"ClickHouseProfileEvents_RemoteFSSeeksWithReset":                           "counter",
		"ClickHouseProfileEvents_RemoteFSBuffers":                                  "counter",
		"ClickHouseProfileEvents_ThreadpoolReaderTaskMicroseconds":                 "counter",
		"ClickHouseProfileEvents_ThreadpoolReaderReadBytes":                        "counter",
		"ClickHouseProfileEvents_FileSegmentWaitReadBufferMicroseconds":            "counter",
		"ClickHouseProfileEvents_FileSegmentReadMicroseconds":                      "counter",
		"ClickHouseProfileEvents_FileSegmentCacheWriteMicroseconds":                "counter",
		"ClickHouseProfileEvents_FileSegmentPredownloadMicroseconds":               "counter",
		"ClickHouseProfileEvents_FileSegmentUsedBytes":                             "counter",
		"ClickHouseProfileEvents_ReadBufferSeekCancelConnection":                   "counter",
		"ClickHouseProfileEvents_SleepFunctionCalls":                               "counter",
		"ClickHouseProfileEvents_SleepFunctionMicroseconds":                        "counter",
		"ClickHouseProfileEvents_ThreadPoolReaderPageCacheHit":                     "counter",
		"ClickHouseProfileEvents_ThreadPoolReaderPageCacheHitBytes":                "counter",
		"ClickHouseProfileEvents_ThreadPoolReaderPageCacheHitElapsedMicroseconds":  "counter",
		"ClickHouseProfileEvents_ThreadPoolReaderPageCacheMiss":                    "counter",
		"ClickHouseProfileEvents_ThreadPoolReaderPageCacheMissBytes":               "counter",
		"ClickHouseProfileEvents_ThreadPoolReaderPageCacheMissElapsedMicroseconds": "counter",
		"ClickHouseProfileEvents_AsynchronousReadWaitMicroseconds":                 "counter",
		"ClickHouseProfileEvents_ExternalDataSourceLocalCacheReadBytes":            "counter",
		"ClickHouseProfileEvents_MainConfigLoads":                                  "counter",
		"ClickHouseProfileEvents_AggregationPreallocatedElementsInHashTables":      "counter",
		"ClickHouseProfileEvents_AggregationHashTablesInitializedAsTwoLevel":       "counter",
		"ClickHouseProfileEvents_MergeTreeMetadataCacheGet":                        "counter",
		"ClickHouseProfileEvents_MergeTreeMetadataCachePut":                        "counter",
		"ClickHouseProfileEvents_MergeTreeMetadataCacheDelete":                     "counter",
		"ClickHouseProfileEvents_MergeTreeMetadataCacheSeek":                       "counter",
		"ClickHouseProfileEvents_MergeTreeMetadataCacheHit":                        "counter",
		"ClickHouseProfileEvents_MergeTreeMetadataCacheMiss":                       "counter",
		"ClickHouseProfileEvents_KafkaRebalanceRevocations":                        "counter",
		"ClickHouseProfileEvents_KafkaRebalanceAssignments":                        "counter",
		"ClickHouseProfileEvents_KafkaRebalanceErrors":                             "counter",
		"ClickHouseProfileEvents_KafkaMessagesPolled":                              "counter",
		"ClickHouseProfileEvents_KafkaMessagesRead":                                "counter",
		"ClickHouseProfileEvents_KafkaMessagesFailed":                              "counter",
		"ClickHouseProfileEvents_KafkaRowsRead":                                    "counter",
		"ClickHouseProfileEvents_KafkaRowsRejected":                                "counter",
		"ClickHouseProfileEvents_KafkaDirectReads":                                 "counter",
		"ClickHouseProfileEvents_KafkaBackgroundReads":                             "counter",
		"ClickHouseProfileEvents_KafkaCommits":                                     "counter",
		"ClickHouseProfileEvents_KafkaCommitFailures":                              "counter",
		"ClickHouseProfileEvents_KafkaConsumerErrors":                              "counter",
		"ClickHouseProfileEvents_KafkaWrites":                                      "counter",
		"ClickHouseProfileEvents_KafkaRowsWritten":                                 "counter",
		"ClickHouseProfileEvents_KafkaProducerFlushes":                             "counter",
		"ClickHouseProfileEvents_KafkaMessagesProduced":                            "counter",
		"ClickHouseProfileEvents_KafkaProducerErrors":                              "counter",
		"ClickHouseProfileEvents_ScalarSubqueriesGlobalCacheHit":                   "counter",
		"ClickHouseProfileEvents_ScalarSubqueriesLocalCacheHit":                    "counter",
		"ClickHouseProfileEvents_ScalarSubqueriesCacheMiss":                        "counter",
		"ClickHouseProfileEvents_SchemaInferenceCacheHits":                         "counter",
		"ClickHouseProfileEvents_SchemaInferenceCacheMisses":                       "counter",
		"ClickHouseProfileEvents_SchemaInferenceCacheEvictions":                    "counter",
		"ClickHouseProfileEvents_SchemaInferenceCacheInvalidations":                "counter",
		"ClickHouseProfileEvents_KeeperPacketsSent":                                "counter",
		"ClickHouseProfileEvents_KeeperPacketsReceived":                            "counter",
		"ClickHouseProfileEvents_KeeperRequestTotal":                               "counter",
		"ClickHouseProfileEvents_KeeperLatency":                                    "counter",
		"ClickHouseProfileEvents_KeeperCommits":                                    "counter",
		"ClickHouseProfileEvents_KeeperCommitsFailed":                              "counter",
		"ClickHouseProfileEvents_KeeperSnapshotCreations":                          "counter",
		"ClickHouseProfileEvents_KeeperSnapshotCreationsFailed":                    "counter",
		"ClickHouseProfileEvents_KeeperSnapshotApplys":                             "counter",
		"ClickHouseProfileEvents_KeeperSnapshotApplysFailed":                       "counter",
		"ClickHouseProfileEvents_KeeperReadSnapshot":                               "counter",
		"ClickHouseProfileEvents_KeeperSaveSnapshot":                               "counter",
		"ClickHouseProfileEvents_OverflowBreak":                                    "counter",
		"ClickHouseProfileEvents_OverflowThrow":                                    "counter",
		"ClickHouseProfileEvents_OverflowAny":                                      "counter",
		"ClickHouseMetrics_Query":                                                  "gauge",
		"ClickHouseMetrics_Merge":                                                  "gauge",
		"ClickHouseMetrics_PartMutation":                                           "gauge",
		"ClickHouseMetrics_ReplicatedFetch":                                        "gauge",
		"ClickHouseMetrics_ReplicatedSend":                                         "gauge",
		"ClickHouseMetrics_ReplicatedChecks":                                       "gauge",
		"ClickHouseMetrics_BackgroundMergesAndMutationsPoolTask":                   "gauge",
		"ClickHouseMetrics_BackgroundFetchesPoolTask":                              "gauge",
		"ClickHouseMetrics_BackgroundCommonPoolTask":                               "gauge",
		"ClickHouseMetrics_BackgroundMovePoolTask":                                 "gauge",
		"ClickHouseMetrics_BackgroundSchedulePoolTask":                             "gauge",
		"ClickHouseMetrics_BackgroundBufferFlushSchedulePoolTask":                  "gauge",
		"ClickHouseMetrics_BackgroundDistributedSchedulePoolTask":                  "gauge",
		"ClickHouseMetrics_BackgroundMessageBrokerSchedulePoolTask":                "gauge",
		"ClickHouseMetrics_CacheDictionaryUpdateQueueBatches":                      "gauge",
		"ClickHouseMetrics_CacheDictionaryUpdateQueueKeys":                         "gauge",
		"ClickHouseMetrics_DiskSpaceReservedForMerge":                              "gauge",
		"ClickHouseMetrics_DistributedSend":                                        "gauge",
		"ClickHouseMetrics_QueryPreempted":                                         "gauge",
		"ClickHouseMetrics_TCPConnection":                                          "gauge",
		"ClickHouseMetrics_MySQLConnection":                                        "gauge",
		"ClickHouseMetrics_HTTPConnection":                                         "gauge",
		"ClickHouseMetrics_InterserverConnection":                                  "gauge",
		"ClickHouseMetrics_PostgreSQLConnection":                                   "gauge",
		"ClickHouseMetrics_OpenFileForRead":                                        "gauge",
		"ClickHouseMetrics_OpenFileForWrite":                                       "gauge",
		"ClickHouseMetrics_Read":                                                   "gauge",
		"ClickHouseMetrics_Write":                                                  "gauge",
		"ClickHouseMetrics_NetworkReceive":                                         "gauge",
		"ClickHouseMetrics_NetworkSend":                                            "gauge",
		"ClickHouseMetrics_SendScalars":                                            "gauge",
		"ClickHouseMetrics_SendExternalTables":                                     "gauge",
		"ClickHouseMetrics_QueryThread":                                            "gauge",
		"ClickHouseMetrics_ReadonlyReplica":                                        "gauge",
		"ClickHouseMetrics_MemoryTracking":                                         "gauge",
		"ClickHouseMetrics_EphemeralNode":                                          "gauge",
		"ClickHouseMetrics_ZooKeeperSession":                                       "gauge",
		"ClickHouseMetrics_ZooKeeperWatch":                                         "gauge",
		"ClickHouseMetrics_ZooKeeperRequest":                                       "gauge",
		"ClickHouseMetrics_DelayedInserts":                                         "gauge",
		"ClickHouseMetrics_ContextLockWait":                                        "gauge",
		"ClickHouseMetrics_StorageBufferRows":                                      "gauge",
		"ClickHouseMetrics_StorageBufferBytes":                                     "gauge",
		"ClickHouseMetrics_DictCacheRequests":                                      "gauge",
		"ClickHouseMetrics_Revision":                                               "gauge",
		"ClickHouseMetrics_VersionInteger":                                         "gauge",
		"ClickHouseMetrics_RWLockWaitingReaders":                                   "gauge",
		"ClickHouseMetrics_RWLockWaitingWriters":                                   "gauge",
		"ClickHouseMetrics_RWLockActiveReaders":                                    "gauge",
		"ClickHouseMetrics_RWLockActiveWriters":                                    "gauge",
		"ClickHouseMetrics_GlobalThread":                                           "gauge",
		"ClickHouseMetrics_GlobalThreadActive":                                     "gauge",
		"ClickHouseMetrics_LocalThread":                                            "gauge",
		"ClickHouseMetrics_LocalThreadActive":                                      "gauge",
		"ClickHouseMetrics_DistributedFilesToInsert":                               "gauge",
		"ClickHouseMetrics_BrokenDistributedFilesToInsert":                         "gauge",
		"ClickHouseMetrics_TablesToDropQueueSize":                                  "gauge",
		"ClickHouseMetrics_MaxDDLEntryID":                                          "gauge",
		"ClickHouseMetrics_MaxPushedDDLEntryID":                                    "gauge",
		"ClickHouseMetrics_PartsTemporary":                                         "gauge",
		"ClickHouseMetrics_PartsPreCommitted":                                      "gauge",
		"ClickHouseMetrics_PartsCommitted":                                         "gauge",
		"ClickHouseMetrics_PartsPreActive":                                         "gauge",
		"ClickHouseMetrics_PartsActive":                                            "gauge",
		"ClickHouseMetrics_PartsOutdated":                                          "gauge",
		"ClickHouseMetrics_PartsDeleting":                                          "gauge",
		"ClickHouseMetrics_PartsDeleteOnDestroy":                                   "gauge",
		"ClickHouseMetrics_PartsWide":                                              "gauge",
		"ClickHouseMetrics_PartsCompact":                                           "gauge",
		"ClickHouseMetrics_PartsInMemory":                                          "gauge",
		"ClickHouseMetrics_MMappedFiles":                                           "gauge",
		"ClickHouseMetrics_MMappedFileBytes":                                       "gauge",
		"ClickHouseMetrics_AsyncDrainedConnections":                                "gauge",
		"ClickHouseMetrics_ActiveAsyncDrainedConnections":                          "gauge",
		"ClickHouseMetrics_SyncDrainedConnections":                                 "gauge",
		"ClickHouseMetrics_ActiveSyncDrainedConnections":                           "gauge",
		"ClickHouseMetrics_AsynchronousReadWait":                                   "gauge",
		"ClickHouseMetrics_PendingAsyncInsert":                                     "gauge",
		"ClickHouseMetrics_KafkaConsumers":                                         "gauge",
		"ClickHouseMetrics_KafkaConsumersWithAssignment":                           "gauge",
		"ClickHouseMetrics_KafkaProducers":                                         "gauge",
		"ClickHouseMetrics_KafkaLibrdkafkaThreads":                                 "gauge",
		"ClickHouseMetrics_KafkaBackgroundReads":                                   "gauge",
		"ClickHouseMetrics_KafkaConsumersInUse":                                    "gauge",
		"ClickHouseMetrics_KafkaWrites":                                            "gauge",
		"ClickHouseMetrics_KafkaAssignedPartitions":                                "gauge",
		"ClickHouseMetrics_FilesystemCacheReadBuffers":                             "gauge",
		"ClickHouseMetrics_CacheFileSegments":                                      "gauge",
		"ClickHouseMetrics_CacheDetachedFileSegments":                              "gauge",
		"ClickHouseMetrics_FilesystemCacheSize":                                    "gauge",
		"ClickHouseMetrics_FilesystemCacheElements":                                "gauge",
		"ClickHouseMetrics_S3Requests":                                             "gauge",
		"ClickHouseMetrics_KeeperAliveConnections":                                 "gauge",
		"ClickHouseMetrics_KeeperOutstandingRequets":                               "gauge",
		"ClickHouseAsyncMetrics_AsynchronousMetricsCalculationTimeSpent":           "gauge",
		"ClickHouseAsyncMetrics_jemalloc_arenas_all_muzzy_purged":                  "gauge",
		"ClickHouseAsyncMetrics_jemalloc_arenas_all_pactive":                       "gauge",
		"ClickHouseAsyncMetrics_jemalloc_background_thread_run_intervals":          "gauge",
		"ClickHouseAsyncMetrics_jemalloc_metadata":                                 "gauge",
		"ClickHouseAsyncMetrics_HTTPThreads":                                       "gauge",
		"ClickHouseAsyncMetrics_MaxPartCountForPartition":                          "gauge",
		"ClickHouseAsyncMetrics_ReplicasSumInsertsInQueue":                         "gauge",
		"ClickHouseAsyncMetrics_ReplicasSumQueueSize":                              "gauge",
		"ClickHouseAsyncMetrics_ReplicasMaxMergesInQueue":                          "gauge",
		"ClickHouseAsyncMetrics_ReplicasMaxInsertsInQueue":                         "gauge",
		"ClickHouseAsyncMetrics_DiskAvailable_default":                             "gauge",
		"ClickHouseAsyncMetrics_DiskUsed_default":                                  "gauge",
		"ClickHouseAsyncMetrics_FilesystemLogsPathAvailableINodes":                 "gauge",
		"ClickHouseAsyncMetrics_FilesystemLogsPathTotalINodes":                     "gauge",
		"ClickHouseAsyncMetrics_FilesystemLogsPathUsedBytes":                       "gauge",
		"ClickHouseAsyncMetrics_FilesystemLogsPathAvailableBytes":                  "gauge",
		"ClickHouseAsyncMetrics_FilesystemLogsPathTotalBytes":                      "gauge",
		"ClickHouseAsyncMetrics_FilesystemMainPathTotalINodes":                     "gauge",
		"ClickHouseAsyncMetrics_EDAC2_Uncorrectable":                               "gauge",
		"ClickHouseAsyncMetrics_EDAC1_Uncorrectable":                               "gauge",
		"ClickHouseAsyncMetrics_EDAC0_Correctable":                                 "gauge",
		"ClickHouseAsyncMetrics_Temperature_coretemp_Core_3":                       "gauge",
		"ClickHouseAsyncMetrics_Temperature_coretemp_Core_6":                       "gauge",
		"ClickHouseAsyncMetrics_Temperature_coretemp_Core_0":                       "gauge",
		"ClickHouseAsyncMetrics_Temperature0":                                      "gauge",
		"ClickHouseAsyncMetrics_NetworkReceiveDrop_eth0":                           "gauge",
		"ClickHouseAsyncMetrics_NetworkReceivePackets_eth0":                        "gauge",
		"ClickHouseAsyncMetrics_NetworkSendDrop_eth1":                              "gauge",
		"ClickHouseAsyncMetrics_NetworkSendBytes_eth1":                             "gauge",
		"ClickHouseAsyncMetrics_BlockActiveTime_md11":                              "gauge",
		"ClickHouseAsyncMetrics_BlockReadTime_md11":                                "gauge",
		"ClickHouseAsyncMetrics_BlockDiscardBytes_md11":                            "gauge",
		"ClickHouseAsyncMetrics_BlockWriteMerges_md11":                             "gauge",
		"ClickHouseAsyncMetrics_BlockReadOps_md11":                                 "gauge",
		"ClickHouseAsyncMetrics_NumberOfDatabases":                                 "gauge",
		"ClickHouseAsyncMetrics_BlockActiveTime_md0":                               "gauge",
		"ClickHouseAsyncMetrics_BlockDiscardTime_md0":                              "gauge",
		"ClickHouseAsyncMetrics_PrometheusThreads":                                 "gauge",
		"ClickHouseAsyncMetrics_BlockWriteTime_md0":                                "gauge",
		"ClickHouseAsyncMetrics_BlockReadTime_md0":                                 "gauge",
		"ClickHouseAsyncMetrics_BlockWriteBytes_md0":                               "gauge",
		"ClickHouseAsyncMetrics_BlockDiscardMerges_md0":                            "gauge",
		"ClickHouseAsyncMetrics_BlockReadMerges_md0":                               "gauge",
		"ClickHouseAsyncMetrics_BlockDiscardOps_md0":                               "gauge",
		"ClickHouseAsyncMetrics_BlockDiscardTime_nvme0n1":                          "gauge",
		"ClickHouseAsyncMetrics_BlockWriteTime_nvme0n1":                            "gauge",
		"ClickHouseAsyncMetrics_BlockReadBytes_nvme0n1":                            "gauge",
		"ClickHouseAsyncMetrics_EDAC3_Uncorrectable":                               "gauge",
		"ClickHouseAsyncMetrics_BlockDiscardMerges_nvme0n1":                        "gauge",
		"ClickHouseAsyncMetrics_BlockWriteMerges_nvme0n1":                          "gauge",
		"ClickHouseAsyncMetrics_BlockWriteOps_nvme0n1":                             "gauge",
		"ClickHouseAsyncMetrics_BlockReadOps_nvme0n1":                              "gauge",
		"ClickHouseAsyncMetrics_BlockActiveTime_nvme2n1":                           "gauge",
		"ClickHouseAsyncMetrics_BlockInFlightOps_nvme2n1":                          "gauge",
		"ClickHouseAsyncMetrics_BlockDiscardTime_nvme2n1":                          "gauge",
		"ClickHouseAsyncMetrics_BlockDiscardBytes_nvme2n1":                         "gauge",
		"ClickHouseAsyncMetrics_BlockReadBytes_nvme2n1":                            "gauge",
		"ClickHouseAsyncMetrics_BlockReadMerges_nvme2n1":                           "gauge",
		"ClickHouseAsyncMetrics_BlockReadOps_nvme2n1":                              "gauge",
		"ClickHouseAsyncMetrics_BlockWriteTime_nvme3n1":                            "gauge",
		"ClickHouseAsyncMetrics_BlockDiscardBytes_nvme3n1":                         "gauge",
		"ClickHouseAsyncMetrics_BlockDiscardMerges_nvme3n1":                        "gauge",
		"ClickHouseAsyncMetrics_BlockInFlightOps_nvme0n1":                          "gauge",
		"ClickHouseAsyncMetrics_BlockWriteMerges_nvme3n1":                          "gauge",
		"ClickHouseAsyncMetrics_BlockWriteOps_nvme3n1":                             "gauge",
		"ClickHouseAsyncMetrics_BlockReadOps_nvme3n1":                              "gauge",
		"ClickHouseAsyncMetrics_BlockInFlightOps_nvme1n1":                          "gauge",
		"ClickHouseAsyncMetrics_jemalloc_allocated":                                "gauge",
		"ClickHouseAsyncMetrics_BlockWriteTime_nvme1n1":                            "gauge",
		"ClickHouseAsyncMetrics_BlockReadTime_nvme1n1":                             "gauge",
		"ClickHouseAsyncMetrics_NetworkSendBytes_eth0":                             "gauge",
		"ClickHouseAsyncMetrics_BlockDiscardBytes_nvme1n1":                         "gauge",
		"ClickHouseAsyncMetrics_BlockReadBytes_nvme1n1":                            "gauge",
		"ClickHouseAsyncMetrics_BlockDiscardMerges_nvme1n1":                        "gauge",
		"ClickHouseAsyncMetrics_jemalloc_epoch":                                    "gauge",
		"ClickHouseAsyncMetrics_BlockDiscardOps_nvme1n1":                           "gauge",
		"ClickHouseAsyncMetrics_BlockWriteOps_nvme1n1":                             "gauge",
		"ClickHouseAsyncMetrics_BlockReadOps_nvme1n1":                              "gauge",
		"ClickHouseAsyncMetrics_BlockQueueTime_sdb":                                "gauge",
		"ClickHouseAsyncMetrics_BlockInFlightOps_sdb":                              "gauge",
		"ClickHouseAsyncMetrics_TotalPartsOfMergeTreeTables":                       "gauge",
		"ClickHouseAsyncMetrics_BlockReadTime_sdb":                                 "gauge",
		"ClickHouseAsyncMetrics_BlockDiscardOps_sdb":                               "gauge",
		"ClickHouseAsyncMetrics_BlockReadOps_sdb":                                  "gauge",
		"ClickHouseAsyncMetrics_BlockActiveTime_sda":                               "gauge",
		"ClickHouseAsyncMetrics_BlockWriteTime_sda":                                "gauge",
		"ClickHouseAsyncMetrics_BlockWriteBytes_sda":                               "gauge",
		"ClickHouseAsyncMetrics_BlockWriteMerges_sda":                              "gauge",
		"ClickHouseAsyncMetrics_BlockWriteOps_sda":                                 "gauge",
		"ClickHouseAsyncMetrics_BlockReadOps_sda":                                  "gauge",
		"ClickHouseAsyncMetrics_BlockQueueTime_nvme5n1":                            "gauge",
		"ClickHouseAsyncMetrics_BlockActiveTime_nvme5n1":                           "gauge",
		"ClickHouseAsyncMetrics_BlockWriteTime_nvme5n1":                            "gauge",
		"ClickHouseAsyncMetrics_BlockWriteBytes_nvme5n1":                           "gauge",
		"ClickHouseAsyncMetrics_BlockWriteMerges_nvme5n1":                          "gauge",
		"ClickHouseAsyncMetrics_BlockReadMerges_nvme5n1":                           "gauge",
		"ClickHouseAsyncMetrics_BlockReadOps_nvme5n1":                              "gauge",
		"ClickHouseAsyncMetrics_BlockWriteTime_nvme4n1":                            "gauge",
		"ClickHouseAsyncMetrics_BlockReadTime_nvme4n1":                             "gauge",
		"ClickHouseAsyncMetrics_BlockDiscardMerges_nvme4n1":                        "gauge",
		"ClickHouseAsyncMetrics_BlockWriteMerges_nvme4n1":                          "gauge",
		"ClickHouseAsyncMetrics_BlockReadMerges_nvme4n1":                           "gauge",
		"ClickHouseAsyncMetrics_BlockWriteOps_nvme4n1":                             "gauge",
		"ClickHouseAsyncMetrics_BlockReadOps_nvme4n1":                              "gauge",
		"ClickHouseAsyncMetrics_BlockWriteTime_md1":                                "gauge",
		"ClickHouseAsyncMetrics_BlockWriteBytes_md1":                               "gauge",
		"ClickHouseAsyncMetrics_BlockWriteOps_md1":                                 "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_106":                               "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_105":                               "gauge",
		"ClickHouseAsyncMetrics_BlockWriteBytes_nvme1n1":                           "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_104":                               "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_100":                               "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_98":                                "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_95":                                "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_92":                                "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_91":                                "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_90":                                "gauge",
		"ClickHouseAsyncMetrics_BlockInFlightOps_sda":                              "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_88":                                "gauge",
		"ClickHouseAsyncMetrics_BlockReadBytes_nvme4n1":                            "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_86":                                "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_84":                                "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_83":                                "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_82":                                "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_81":                                "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_79":                                "gauge",
		"ClickHouseAsyncMetrics_BlockWriteMerges_nvme2n1":                          "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_78":                                "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_77":                                "gauge",
		"ClickHouseAsyncMetrics_BlockDiscardMerges_md1":                            "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_76":                                "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_75":                                "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_74":                                "gauge",
		"ClickHouseAsyncMetrics_BlockDiscardTime_nvme5n1":                          "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_70":                                "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_68":                                "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_65":                                "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_64":                                "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_63":                                "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_62":                                "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_61":                                "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_59":                                "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_57":                                "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_56":                                "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_54":                                "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_53":                                "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_52":                                "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_50":                                "gauge",
		"ClickHouseAsyncMetrics_ReplicasMaxAbsoluteDelay":                          "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_47":                                "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_44":                                "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_43":                                "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_41":                                "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_40":                                "gauge",
		"ClickHouseAsyncMetrics_BlockDiscardMerges_nvme2n1":                        "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_38":                                "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_37":                                "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_36":                                "gauge",
		"ClickHouseAsyncMetrics_BlockReadTime_md1":                                 "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_33":                                "gauge",
		"ClickHouseAsyncMetrics_BlockActiveTime_sdb":                               "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_31":                                "gauge",
		"ClickHouseAsyncMetrics_BlockInFlightOps_md0":                              "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_30":                                "gauge",
		"ClickHouseAsyncMetrics_Temperature1":                                      "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_25":                                "gauge",
		"ClickHouseAsyncMetrics_BlockReadTime_nvme0n1":                             "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_24":                                "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_22":                                "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_21":                                "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_20":                                "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_16":                                "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_14":                                "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_9":                                 "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_6":                                 "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_4":                                 "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_0":                                 "gauge",
		"ClickHouseAsyncMetrics_OSMemoryCached":                                    "gauge",
		"ClickHouseAsyncMetrics_OSMemoryBuffers":                                   "gauge",
		"ClickHouseAsyncMetrics_BlockReadMerges_nvme1n1":                           "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeNormalized":                             "gauge",
		"ClickHouseAsyncMetrics_BlockWriteTime_sdb":                                "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeNormalized":                              "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeNormalized":                            "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeNormalized":                              "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_85":                                "gauge",
		"ClickHouseAsyncMetrics_OSProcessesCreated":                                "gauge",
		"ClickHouseAsyncMetrics_OSContextSwitches":                                 "gauge",
		"ClickHouseAsyncMetrics_OSInterrupts":                                      "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU111":                             "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_60":                                "gauge",
		"ClickHouseAsyncMetrics_BlockQueueTime_md11":                               "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU111":                                 "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU111":                                 "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU111":                               "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_55":                                "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU111":                                  "gauge",
		"ClickHouseAsyncMetrics_jemalloc_arenas_all_dirty_purged":                  "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU111":                                "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU111":                                  "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU110":                                 "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU110":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU110":                                "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU110":                                "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU109":                             "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU109":                                 "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU109":                               "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU109":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU109":                                "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU109":                                "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU109":                                  "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU108":                                 "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU108":                               "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU108":                                  "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_13":                                "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU108":                                  "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU107":                                 "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU107":                               "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU107":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU107":                                  "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU107":                                "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU107":                                  "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU106":                             "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU106":                                 "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU106":                               "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU106":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU106":                                "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU106":                                  "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU105":                             "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU105":                                 "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU105":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU105":                                "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU105":                                  "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU105":                                  "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU104":                             "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU104":                                 "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU104":                               "gauge",
		"ClickHouseAsyncMetrics_Temperature_coretemp_Core_2":                       "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU104":                                  "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU104":                                "gauge",
		"ClickHouseAsyncMetrics_BlockInFlightOps_md11":                             "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU103":                                 "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU108":                             "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU103":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU103":                                "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU110":                                  "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU102":                                 "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_32":                                "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU102":                               "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU102":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU102":                                "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU102":                                "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU102":                                  "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU102":                                  "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU101":                                 "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU101":                                 "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU101":                               "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_29":                                "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU101":                                  "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU101":                                  "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU100":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU100":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU100":                                "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU106":                                  "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_51":                                "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU100":                                  "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU100":                                "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU100":                                  "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU100":                                  "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU99":                                  "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU99":                                "gauge",
		"ClickHouseAsyncMetrics_BlockWriteTime_nvme2n1":                            "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU99":                                   "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU99":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU98":                                  "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU98":                                 "gauge",
		"ClickHouseAsyncMetrics_EDAC3_Correctable":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU98":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU103":                               "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU98":                                 "gauge",
		"ClickHouseAsyncMetrics_BlockWriteBytes_nvme2n1":                           "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU98":                                   "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU97":                                  "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU97":                                "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU97":                                 "gauge",
		"ClickHouseAsyncMetrics_NetworkReceiveBytes_eth1":                          "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU96":                                "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU96":                                    "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU96":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU96":                                   "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU96":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU95":                              "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU95":                                  "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU110":                               "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU95":                                  "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU110":                                  "gauge",
		"ClickHouseAsyncMetrics_BlockWriteMerges_nvme1n1":                          "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU95":                                "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU95":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU95":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU95":                                 "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU94":                                  "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU94":                                  "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU94":                                "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU94":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU94":                                   "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU94":                                   "gauge",
		"ClickHouseAsyncMetrics_BlockActiveTime_nvme1n1":                           "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU93":                                  "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU93":                                    "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU93":                                   "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_17":                                "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU93":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU92":                                  "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU92":                                "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU92":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU91":                              "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU91":                                    "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU91":                                 "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU93":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU91":                                   "gauge",
		"ClickHouseAsyncMetrics_BlockReadTime_nvme2n1":                             "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU91":                                 "gauge",
		"ClickHouseAsyncMetrics_jemalloc_background_thread_num_threads":            "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU90":                                  "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU90":                                "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU90":                                    "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU90":                                   "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU90":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU89":                              "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU89":                                  "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU89":                                    "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU89":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU88":                                  "gauge",
		"ClickHouseAsyncMetrics_BlockDiscardTime_nvme4n1":                          "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU88":                                  "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU88":                                "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU88":                                 "gauge",
		"ClickHouseAsyncMetrics_Temperature_coretemp_Package_id_1":                 "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU88":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU88":                                 "gauge",
		"ClickHouseAsyncMetrics_BlockDiscardBytes_md1":                             "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU87":                              "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU87":                                  "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU101":                                  "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU87":                                  "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU104":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU87":                                    "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_23":                                "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU87":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU86":                                "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU86":                                    "gauge",
		"ClickHouseAsyncMetrics_OSMemoryFreePlusCached":                            "gauge",
		"ClickHouseAsyncMetrics_BlockReadMerges_sdb":                               "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU86":                                 "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_66":                                "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU86":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU86":                                 "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_46":                                "gauge",
		"ClickHouseAsyncMetrics_BlockDiscardMerges_sdb":                            "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU85":                              "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU107":                                  "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU85":                                  "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU85":                                  "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU85":                                    "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU85":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU95":                                    "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU85":                                 "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU85":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU84":                                  "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU111":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU84":                                "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU84":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU84":                                   "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU84":                                   "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU84":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU83":                              "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU111":                                "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU83":                                  "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU83":                                  "gauge",
		"ClickHouseAsyncMetrics_BlockDiscardTime_md1":                              "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU83":                                "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU83":                                    "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU98":                              "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU83":                                   "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU82":                                  "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU82":                                    "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_19":                                "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU82":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU82":                                   "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU82":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU81":                              "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU81":                                "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU81":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU81":                                 "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU81":                                   "gauge",
		"ClickHouseAsyncMetrics_BlockDiscardBytes_sda":                             "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU80":                              "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU101":                                "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU80":                                  "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU80":                                  "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU80":                                "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU92":                                  "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU80":                                 "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU80":                                 "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU80":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU108":                                "gauge",
		"ClickHouseAsyncMetrics_BlockDiscardBytes_nvme4n1":                         "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU80":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeNormalized":                               "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU79":                              "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU79":                                  "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_1":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU79":                                    "gauge",
		"ClickHouseAsyncMetrics_OSProcessesBlocked":                                "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU79":                                 "gauge",
		"ClickHouseAsyncMetrics_NetworkSendPackets_eth1":                           "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU79":                                 "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU79":                                   "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU78":                                  "gauge",
		"ClickHouseAsyncMetrics_BlockDiscardOps_nvme0n1":                           "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU78":                                "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU78":                                    "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_11":                                "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU78":                                 "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU78":                                 "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU83":                                   "gauge",
		"ClickHouseAsyncMetrics_BlockReadBytes_md0":                                "gauge",
		"ClickHouseAsyncMetrics_jemalloc_mapped":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU77":                              "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU90":                                 "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU77":                                "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU103":                             "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU77":                                    "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU77":                                 "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU91":                                  "gauge",
		"ClickHouseAsyncMetrics_BlockWriteOps_md0":                                 "gauge",
		"ClickHouseAsyncMetrics_EDAC1_Correctable":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU77":                                   "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU77":                                   "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU77":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU76":                              "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU76":                                  "gauge",
		"ClickHouseAsyncMetrics_BlockInFlightOps_nvme3n1":                          "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU76":                                  "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU104":                                  "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU76":                                    "gauge",
		"ClickHouseAsyncMetrics_BlockWriteOps_nvme2n1":                             "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU76":                                 "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU76":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU25":                                "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU76":                                 "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU98":                                  "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU74":                              "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU74":                                  "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU108":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU74":                                    "gauge",
		"ClickHouseAsyncMetrics_OSOpenFiles":                                       "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU74":                                   "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU58":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU20":                                 "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU74":                                 "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU73":                              "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU92":                                    "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeNormalized":                             "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU73":                                  "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU24":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU73":                                    "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU73":                                 "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU48":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU75":                                    "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU73":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU58":                                  "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU76":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU20":                                "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU47":                              "gauge",
		"ClickHouseAsyncMetrics_BlockQueueTime_sda":                                "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU73":                                 "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU73":                                   "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU73":                                   "gauge",
		"ClickHouseAsyncMetrics_LoadAverage5":                                      "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU72":                                  "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU72":                                 "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU72":                                 "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU52":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU106":                                "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU71":                                  "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU71":                                    "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU72":                                  "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU52":                                  "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU71":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU71":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU56":                                  "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU71":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU70":                              "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU40":                                 "gauge",
		"ClickHouseAsyncMetrics_BlockDiscardTime_md11":                             "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU70":                                  "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU36":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU6":                                  "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU5":                                    "gauge",
		"ClickHouseAsyncMetrics_OSStealTime":                                       "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU70":                                    "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU70":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU70":                                 "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU69":                              "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU25":                                  "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU63":                                "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU69":                                  "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU68":                              "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU68":                                "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU100":                             "gauge",
		"ClickHouseAsyncMetrics_NetworkSendPackets_eth0":                           "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU68":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU67":                              "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU67":                                "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU10":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU79":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU67":                                 "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU66":                                  "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU66":                                  "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU8":                                   "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU18":                                   "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU82":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU66":                                "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU66":                                    "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU66":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU80":                                    "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU66":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU69":                                 "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU65":                                  "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU86":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU65":                                "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU83":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU65":                                    "gauge",
		"ClickHouseAsyncMetrics_Uptime":                                            "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU85":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU65":                                 "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU65":                                 "gauge",
		"ClickHouseAsyncMetrics_BlockActiveTime_nvme4n1":                           "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU65":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU96":                                 "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU65":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU64":                                  "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU9":                                   "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU70":                                  "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU105":                               "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU64":                                    "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU74":                                  "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU5":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU63":                              "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU63":                                  "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_89":                                "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU68":                                 "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU3":                                   "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU63":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU61":                                 "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU63":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU44":                                  "gauge",
		"ClickHouseAsyncMetrics_NetworkSendDrop_eth0":                              "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU62":                              "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU49":                                   "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU21":                                   "gauge",
		"ClickHouseAsyncMetrics_BlockReadMerges_nvme3n1":                           "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU62":                                    "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU72":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU67":                                    "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU23":                                    "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU62":                                 "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU62":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU61":                              "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU61":                                  "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_26":                                "gauge",
		"ClickHouseAsyncMetrics_NetworkReceivePackets_eth1":                        "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU61":                                  "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU81":                                  "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU92":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU105":                                "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU61":                                "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU61":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU54":                              "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU61":                                   "gauge",
		"ClickHouseAsyncMetrics_NetworkReceiveErrors_eth1":                         "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU60":                              "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU83":                                   "gauge",
		"ClickHouseAsyncMetrics_FilesystemMainPathUsedINodes":                      "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU60":                                  "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_67":                                "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU60":                                 "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU68":                                 "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU60":                                 "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU67":                                   "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU59":                                  "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU59":                                 "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU70":                                   "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU59":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU58":                              "gauge",
		"ClickHouseAsyncMetrics_BlockDiscardMerges_sda":                            "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU58":                                "gauge",
		"ClickHouseAsyncMetrics_BlockWriteOps_sdb":                                 "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU61":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU58":                                 "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU58":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU38":                                    "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU58":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU53":                                    "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU89":                                "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU57":                                  "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU57":                                "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU57":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU105":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU57":                                 "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU57":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU56":                              "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU56":                                    "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU60":                                "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU17":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU31":                                "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_108":                               "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU56":                                 "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU56":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU92":                                 "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU67":                                  "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU103":                                  "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU55":                              "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU55":                                  "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_93":                                "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU55":                                "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU55":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU54":                                  "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU32":                                    "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU75":                                  "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_101":                               "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU55":                                   "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU89":                                  "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU54":                                  "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU54":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU46":                                 "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU54":                                   "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU72":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU53":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU0":                                     "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU11":                                   "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU53":                                   "gauge",
		"ClickHouseAsyncMetrics_BlockDiscardTime_nvme3n1":                          "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU52":                                  "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU52":                                "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU52":                                    "gauge",
		"ClickHouseAsyncMetrics_BlockQueueTime_nvme2n1":                            "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU52":                                 "gauge",
		"ClickHouseAsyncMetrics_Temperature_coretemp_Core_5":                       "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU52":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU51":                              "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeNormalized":                           "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU51":                                  "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU51":                                    "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU27":                                "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU108":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU51":                                 "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU42":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU29":                                   "gauge",
		"ClickHouseAsyncMetrics_BlockQueueTime_md1":                                "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU51":                                 "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU62":                                   "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU51":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU45":                                 "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU37":                                  "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU52":                              "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU51":                                   "gauge",
		"ClickHouseAsyncMetrics_BlockWriteBytes_nvme3n1":                           "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU64":                              "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU50":                                  "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU50":                                "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU96":                                  "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU50":                                    "gauge",
		"ClickHouseAsyncMetrics_BlockDiscardOps_md11":                              "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU50":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU89":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU50":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU50":                                 "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU50":                                   "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU60":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU49":                                "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU49":                                 "gauge",
		"ClickHouseAsyncMetrics_jemalloc_metadata_thp":                             "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU70":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU16":                                 "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_87":                                "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_99":                                "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU71":                                   "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_109":                               "gauge",
		"ClickHouseAsyncMetrics_NumberOfTables":                                    "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU60":                                  "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU48":                                  "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU69":                                "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU48":                                    "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU44":                                "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU48":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU48":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU32":                                "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU10":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU48":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU99":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU99":                                    "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU58":                                    "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU64":                                  "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU47":                                  "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU75":                                 "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU4":                                    "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU47":                                "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU87":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU97":                                    "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU52":                                   "gauge",
		"ClickHouseAsyncMetrics_NetworkReceiveDrop_eth1":                           "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU47":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU34":                                 "gauge",
		"ClickHouseAsyncMetrics_MemoryDataAndStack":                                "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU47":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU23":                                   "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU111":                                  "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU47":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU39":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU94":                                    "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU46":                                  "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU3":                                    "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU46":                                  "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU34":                              "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU4":                                     "gauge",
		"ClickHouseAsyncMetrics_BlockQueueTime_nvme0n1":                            "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU46":                                "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU53":                                  "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU93":                              "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_71":                                "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU46":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU46":                                 "gauge",
		"ClickHouseAsyncMetrics_BlockDiscardMerges_md11":                           "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU46":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU45":                                   "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU45":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU57":                              "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU44":                              "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU100":                                 "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU44":                                  "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU14":                                  "gauge",
		"ClickHouseAsyncMetrics_BlockDiscardBytes_nvme5n1":                         "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU44":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU24":                                  "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU44":                                 "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU43":                                  "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU43":                                    "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU76":                                "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU78":                              "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU84":                                 "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU54":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU43":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU36":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU33":                                    "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU43":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU42":                              "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU82":                                  "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU42":                                  "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU56":                                  "gauge",
		"ClickHouseAsyncMetrics_BlockReadBytes_nvme5n1":                            "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU45":                                    "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU87":                                 "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU42":                                  "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU41":                              "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU52":                                 "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU41":                                  "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU21":                                "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU41":                                  "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU26":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU72":                                    "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU6":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU41":                                    "gauge",
		"ClickHouseAsyncMetrics_BlockWriteBytes_md11":                              "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU42":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU19":                                 "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU84":                              "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_35":                                "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU41":                                 "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU53":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU41":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU108":                                  "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU54":                                    "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU37":                                  "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU47":                                  "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU41":                                   "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU66":                                   "gauge",
		"ClickHouseAsyncMetrics_BlockReadOps_md1":                                  "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU41":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU40":                                "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU42":                                "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU86":                                  "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU40":                                 "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_73":                                "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU40":                                   "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU40":                                   "gauge",
		"ClickHouseAsyncMetrics_BlockDiscardBytes_md0":                             "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU48":                                  "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU39":                              "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU106":                                 "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU39":                                  "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU68":                                    "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU39":                                  "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU39":                                "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU39":                                    "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU23":                                "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU7":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU39":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeNormalized":                            "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU42":                                   "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU39":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU19":                                  "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU38":                              "gauge",
		"ClickHouseAsyncMetrics_BlockWriteMerges_md0":                              "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU38":                                  "gauge",
		"ClickHouseAsyncMetrics_BlockDiscardOps_sda":                               "gauge",
		"ClickHouseAsyncMetrics_DiskUnreserved_default":                            "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU40":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU89":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU38":                                "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU67":                                  "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU19":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU62":                                "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_7":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU54":                                   "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_48":                                "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU38":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU38":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU24":                                 "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU38":                                 "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU107":                             "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU45":                              "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU23":                                  "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU37":                              "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU49":                              "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU25":                              "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU37":                                "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU68":                                   "gauge",
		"ClickHouseAsyncMetrics_BlockDiscardBytes_sdb":                             "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU37":                                 "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU7":                                 "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU90":                              "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU37":                                   "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU55":                                  "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU37":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU73":                                "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU36":                              "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU103":                                  "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU36":                                  "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU94":                              "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU96":                                   "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_103":                               "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU36":                                "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU36":                                 "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU13":                                  "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU27":                                   "gauge",
		"ClickHouseAsyncMetrics_BlockWriteBytes_nvme4n1":                           "gauge",
		"ClickHouseAsyncMetrics_TotalRowsOfMergeTreeTables":                        "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU43":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU60":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU81":                                 "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_49":                                "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU51":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU3":                                     "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU32":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU35":                                    "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU4":                                    "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU35":                                 "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU35":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU102":                             "gauge",
		"ClickHouseAsyncMetrics_OSMemoryTotal":                                     "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU15":                                "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU27":                                  "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU35":                                   "gauge",
		"ClickHouseAsyncMetrics_BlockDiscardOps_nvme2n1":                           "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU34":                                 "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU39":                                   "gauge",
		"ClickHouseAsyncMetrics_NetworkReceiveErrors_eth0":                         "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU18":                                 "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU82":                                "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU20":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU33":                                 "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU62":                                  "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU40":                                  "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU108":                                "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU9":                                   "gauge",
		"ClickHouseAsyncMetrics_jemalloc_arenas_all_pmuzzy":                        "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU33":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIdleTime":                                        "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU38":                                   "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU35":                                  "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU33":                                   "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU33":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU32":                                 "gauge",
		"ClickHouseAsyncMetrics_BlockReadBytes_md1":                                "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU63":                                 "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU105":                                  "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU19":                                  "gauge",
		"ClickHouseAsyncMetrics_TCPThreads":                                        "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU32":                                   "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU31":                                  "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU31":                                 "gauge",
		"ClickHouseAsyncMetrics_BlockWriteTime_md11":                               "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU69":                                   "gauge",
		"ClickHouseAsyncMetrics_BlockReadBytes_sda":                                "gauge",
		"ClickHouseAsyncMetrics_jemalloc_resident":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU43":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU31":                                   "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_5":                                 "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU31":                                   "gauge",
		"ClickHouseAsyncMetrics_MemoryShared":                                      "gauge",
		"ClickHouseAsyncMetrics_BlockReadTime_nvme3n1":                             "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU5":                                    "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU31":                                   "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU1":                                    "gauge",
		"ClickHouseAsyncMetrics_BlockReadOps_md0":                                  "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU53":                                "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU93":                                  "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU30":                              "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU30":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU62":                                 "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU90":                                  "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU107":                                 "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU29":                                  "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU27":                                  "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTime":                                      "gauge",
		"ClickHouseAsyncMetrics_BlockReadMerges_sda":                               "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU36":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU103":                                "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU106":                                  "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU30":                                 "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU28":                                   "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_45":                                "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU29":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU59":                                    "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU29":                                   "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU59":                                   "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_96":                                "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU68":                                  "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU99":                                 "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU29":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU22":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU67":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU12":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU64":                                "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU69":                                   "gauge",
		"ClickHouseAsyncMetrics_TotalBytesOfMergeTreeTables":                       "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU28":                              "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU99":                              "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU28":                                 "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU28":                                 "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU17":                                 "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_27":                                "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU30":                                  "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU13":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU27":                              "gauge",
		"ClickHouseAsyncMetrics_ReplicasSumMergesInQueue":                          "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU8":                                    "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU28":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU42":                                    "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_10":                                "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU53":                              "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU88":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU27":                                    "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_111":                               "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU53":                                  "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU36":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU6":                                   "gauge",
		"ClickHouseAsyncMetrics_DiskTotal_default":                                 "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU27":                                 "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU35":                                "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU30":                                   "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU90":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU29":                                "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU17":                                  "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU27":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU110":                             "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU53":                                 "gauge",
		"ClickHouseAsyncMetrics_OSMemoryFreeWithoutCached":                         "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU27":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU62":                                   "gauge",
		"ClickHouseAsyncMetrics_Temperature_coretemp_Core_1":                       "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU1":                                 "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_42":                                "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU26":                                  "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU26":                                  "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU75":                                "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU70":                                 "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU7":                                    "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU5":                                 "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU4":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU87":                                 "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU96":                                  "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU26":                                "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU64":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU59":                                  "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU57":                                    "gauge",
		"ClickHouseAsyncMetrics_BlockDiscardTime_sdb":                              "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU23":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU26":                                    "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU26":                                 "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU0":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU26":                                   "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU85":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU33":                              "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU6":                               "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU26":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU19":                              "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU15":                                  "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU22":                              "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU26":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU69":                                    "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU21":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU25":                                  "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU98":                                   "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU25":                                   "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU92":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU33":                                  "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU24":                                "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU3":                               "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU79":                                   "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU84":                                  "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU88":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU25":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU46":                                    "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU10":                                    "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU34":                                "gauge",
		"ClickHouseAsyncMetrics_FilesystemMainPathTotalBytes":                      "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU24":                                 "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU24":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU23":                              "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU23":                                  "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU27":                                 "gauge",
		"ClickHouseAsyncMetrics_BlockReadMerges_md11":                              "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU41":                                "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU4":                                    "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU33":                                "gauge",
		"ClickHouseAsyncMetrics_BlockQueueTime_nvme4n1":                            "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU1":                                   "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU2":                                   "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_8":                                 "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU10":                                  "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU7":                                    "gauge",
		"ClickHouseAsyncMetrics_LoadAverage1":                                      "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU15":                                    "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU23":                                   "gauge",
		"ClickHouseAsyncMetrics_FilesystemMainPathUsedBytes":                       "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU44":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU77":                                  "gauge",
		"ClickHouseAsyncMetrics_BlockWriteMerges_sdb":                              "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU22":                                  "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_28":                                "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU15":                                 "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU20":                              "gauge",
		"ClickHouseAsyncMetrics_BlockInFlightOps_nvme4n1":                          "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU22":                                "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU22":                                    "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU78":                                   "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_80":                                "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU38":                                  "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU22":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU72":                                "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU66":                                   "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU72":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU69":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU24":                                    "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_69":                                "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU46":                              "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU22":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU21":                              "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU91":                                "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU97":                                   "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU74":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU30":                                    "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU37":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU2":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU21":                                  "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU14":                                "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU30":                                "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU104":                                  "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU58":                                  "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU21":                                    "gauge",
		"ClickHouseAsyncMetrics_OSMemoryAvailable":                                 "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU16":                                  "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU21":                                 "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU31":                              "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU21":                                   "gauge",
		"ClickHouseAsyncMetrics_BlockWriteBytes_sdb":                               "gauge",
		"ClickHouseAsyncMetrics_jemalloc_active":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU22":                                  "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU11":                                  "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU21":                                  "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU74":                                "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU20":                                  "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU49":                                    "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU20":                                  "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU0":                                 "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU38":                                   "gauge",
		"ClickHouseAsyncMetrics_BlockDiscardOps_nvme4n1":                           "gauge",
		"ClickHouseAsyncMetrics_Temperature_coretemp_Package_id_0":                 "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU69":                                 "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU99":                                   "gauge",
		"ClickHouseAsyncMetrics_BlockWriteBytes_nvme0n1":                           "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU36":                                    "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU64":                                   "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU15":                                  "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU94":                                 "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU19":                                "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU19":                                    "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU47":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU35":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU19":                                   "gauge",
		"ClickHouseAsyncMetrics_jemalloc_retained":                                 "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU19":                                 "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU19":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU18":                              "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU59":                                "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU9":                               "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU22":                                   "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_107":                               "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU18":                                  "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU18":                                 "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU63":                                  "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU18":                                   "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU6":                                    "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU17":                              "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU0":                                  "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU67":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU97":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU17":                                "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU85":                                "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU86":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU32":                                 "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU16":                                  "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU12":                                "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU16":                                "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU20":                                    "gauge",
		"ClickHouseAsyncMetrics_jemalloc_background_thread_num_runs":               "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU16":                                   "gauge",
		"ClickHouseAsyncMetrics_BlockDiscardOps_nvme3n1":                           "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU49":                                 "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU28":                                "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU54":                                   "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU15":                                   "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU78":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU92":                                 "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU97":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU17":                                    "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU15":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU59":                                   "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_12":                                "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU34":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU32":                              "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU8":                               "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_97":                                "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU23":                                 "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU12":                              "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU1":                                    "gauge",
		"ClickHouseAsyncMetrics_BlockReadBytes_sdb":                                "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU25":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU14":                                   "gauge",
		"ClickHouseAsyncMetrics_BlockActiveTime_nvme0n1":                           "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU45":                                  "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTime":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU13":                              "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU46":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU43":                                  "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU88":                                    "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU37":                                    "gauge",
		"ClickHouseAsyncMetrics_InterserverThreads":                                "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU17":                                 "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU2":                                    "gauge",
		"ClickHouseAsyncMetrics_BlockDiscardTime_sda":                              "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU13":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU14":                                 "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU78":                                  "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU89":                                 "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU13":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU34":                                  "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_15":                                "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU65":                                  "gauge",
		"ClickHouseAsyncMetrics_OSProcessesRunning":                                "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU15":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU5":                               "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU63":                                    "gauge",
		"ClickHouseAsyncMetrics_OSIrqTime":                                         "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU24":                                  "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU28":                                   "gauge",
		"ClickHouseAsyncMetrics_BlockWriteMerges_md1":                              "gauge",
		"ClickHouseAsyncMetrics_BlockReadTime_nvme5n1":                             "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU12":                                  "gauge",
		"ClickHouseAsyncMetrics_HashTableStatsCacheMisses":                         "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU2":                                    "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU110":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU12":                                 "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU10":                                 "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU11":                                  "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU11":                                "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU101":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU35":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU11":                                 "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU48":                              "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU93":                                "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU56":                                "gauge",
		"ClickHouseAsyncMetrics_BlockReadBytes_nvme3n1":                            "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU56":                                   "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU8":                                    "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU20":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU8":                                     "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU10":                                  "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU14":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU35":                                  "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU10":                                "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU31":                                  "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU29":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU42":                                   "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU68":                                  "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU20":                                   "gauge",
		"ClickHouseAsyncMetrics_BlockQueueTime_md0":                                "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU11":                                    "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU61":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU66":                              "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU59":                                 "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU51":                                  "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU16":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU22":                                 "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU14":                                   "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU49":                                  "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU33":                                  "gauge",
		"ClickHouseAsyncMetrics_BlockDiscardOps_md1":                               "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU43":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU3":                                  "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU1":                                     "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU81":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU9":                                     "gauge",
		"ClickHouseAsyncMetrics_MMapCacheCells":                                    "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU96":                              "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU14":                                    "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU87":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU13":                                    "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU14":                                  "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_39":                                "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU65":                              "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU72":                              "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_34":                                "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU25":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU100":                               "gauge",
		"ClickHouseAsyncMetrics_NetworkSendErrors_eth0":                            "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU49":                                   "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU10":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU87":                                "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeNormalized":                              "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU70":                                "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU11":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU63":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU84":                                    "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_110":                               "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU45":                                 "gauge",
		"ClickHouseAsyncMetrics_BlockReadMerges_nvme0n1":                           "gauge",
		"ClickHouseAsyncMetrics_FilesystemMainPathAvailableBytes":                  "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU13":                                "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_94":                                "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU12":                                  "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU30":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU91":                                  "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeNormalized":                         "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU49":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU7":                                  "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU29":                                  "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU86":                              "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU94":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU9":                                 "gauge",
		"ClickHouseAsyncMetrics_BlockActiveTime_nvme3n1":                           "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU47":                                   "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU55":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU43":                                "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU18":                                    "gauge",
		"ClickHouseAsyncMetrics_NetworkSendErrors_eth1":                            "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU9":                                  "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU60":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU3":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU9":                                    "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU28":                                    "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU13":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU109":                                  "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU13":                                 "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU77":                                  "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU9":                                    "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU44":                                 "gauge",
		"ClickHouseAsyncMetrics_BlockDiscardBytes_nvme0n1":                         "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU12":                                 "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU31":                                 "gauge",
		"ClickHouseAsyncMetrics_BlockWriteOps_nvme5n1":                             "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU24":                              "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU36":                                  "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU77":                                 "gauge",
		"ClickHouseAsyncMetrics_EDAC0_Uncorrectable":                               "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU0":                                    "gauge",
		"ClickHouseAsyncMetrics_HashTableStatsCacheEntries":                        "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU103":                                  "gauge",
		"ClickHouseAsyncMetrics_BlockQueueTime_nvme3n1":                            "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU8":                                   "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_18":                                "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU34":                                  "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU8":                                    "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU47":                                    "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU30":                                 "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU4":                               "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU71":                              "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU8":                                  "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU44":                                    "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU17":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU25":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU5":                                  "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU83":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU60":                                    "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU69":                                  "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU9":                                  "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU7":                               "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU57":                                   "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU89":                                   "gauge",
		"ClickHouseAsyncMetrics_BlockDiscardTime_nvme1n1":                          "gauge",
		"ClickHouseAsyncMetrics_FilesystemMainPathAvailableINodes":                 "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU7":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU28":                                  "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU18":                                  "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU2":                                  "gauge",
		"ClickHouseAsyncMetrics_BlockReadMerges_md1":                               "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU71":                                 "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU40":                              "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU7":                                  "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU90":                                 "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU75":                              "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU81":                                  "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU104":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU61":                                    "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU88":                              "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU8":                                 "gauge",
		"ClickHouseAsyncMetrics_UncompressedCacheCells":                            "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU7":                                    "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU6":                                  "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU8":                                  "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU39":                                 "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU11":                              "gauge",
		"ClickHouseAsyncMetrics_BlockWriteOps_md11":                                "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU75":                                 "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU6":                                   "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU103":                                 "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_2":                                 "gauge",
		"ClickHouseAsyncMetrics_EDAC2_Correctable":                                 "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU35":                              "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU11":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU93":                                 "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU2":                                 "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU2":                               "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU12":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU24":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSystemTime":                                      "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU57":                                  "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU45":                                  "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU9":                                    "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU10":                              "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU6":                                     "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU79":                                  "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU7":                                     "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU0":                                    "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU80":                                   "gauge",
		"ClickHouseAsyncMetrics_HashTableStatsCacheHits":                           "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU45":                                "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU102":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU6":                                    "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU79":                                "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU34":                                    "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU2":                                    "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU82":                              "gauge",
		"ClickHouseAsyncMetrics_BlockDiscardMerges_nvme5n1":                        "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU6":                                    "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU51":                                "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU76":                                   "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU50":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU98":                                "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU64":                                 "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU17":                                   "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_3":                                 "gauge",
		"ClickHouseAsyncMetrics_OSUptime":                                          "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU95":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU5":                                  "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU29":                                    "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU63":                                 "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU26":                              "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU110":                                  "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU0":                               "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU107":                                "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU71":                                  "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU4":                                 "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU34":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU54":                                "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU104":                                "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU3":                                    "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU75":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU71":                                "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU4":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU49":                                  "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_72":                                "gauge",
		"ClickHouseAsyncMetrics_BlockDiscardOps_nvme5n1":                           "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU15":                              "gauge",
		"ClickHouseAsyncMetrics_UncompressedCacheBytes":                            "gauge",
		"ClickHouseAsyncMetrics_NetworkReceiveBytes_eth0":                          "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU15":                                 "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU16":                              "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU34":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU55":                                 "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_102":                               "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU50":                              "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU18":                                   "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU109":                                  "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU20":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU11":                                   "gauge",
		"ClickHouseAsyncMetrics_OSUserTime":                                        "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU55":                                 "gauge",
		"ClickHouseAsyncMetrics_OSGuestTime":                                       "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU53":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU5":                                     "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU10":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU14":                              "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU67":                                 "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU43":                              "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU2":                                     "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU2":                                  "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU68":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU12":                                    "gauge",
		"ClickHouseAsyncMetrics_MemoryCode":                                        "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU92":                              "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU74":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU56":                                   "gauge",
		"ClickHouseAsyncMetrics_BlockActiveTime_md1":                               "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU5":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU1":                               "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU0":                                  "gauge",
		"ClickHouseAsyncMetrics_BlockInFlightOps_nvme5n1":                          "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU73":                                  "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU1":                                   "gauge",
		"ClickHouseAsyncMetrics_ReplicasMaxQueueSize":                              "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU50":                                  "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU91":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU75":                                  "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU82":                                 "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU95":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU42":                                 "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU1":                                  "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU3":                                 "gauge",
		"ClickHouseAsyncMetrics_BlockReadTime_sda":                                 "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU12":                                   "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU45":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU64":                                 "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU32":                                  "gauge",
		"ClickHouseAsyncMetrics_BlockQueueTime_nvme1n1":                            "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU74":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU14":                                 "gauge",
		"ClickHouseAsyncMetrics_MemoryResident":                                    "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU75":                                   "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU97":                                  "gauge",
		"ClickHouseAsyncMetrics_ReplicasMaxRelativeDelay":                          "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU44":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU31":                                    "gauge",
		"ClickHouseAsyncMetrics_MemoryVirtual":                                     "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU81":                                    "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU101":                                "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU3":                                  "gauge",
		"ClickHouseAsyncMetrics_OSThreadsTotal":                                    "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU3":                                    "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTime":                                     "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU66":                                 "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU28":                                  "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU56":                                   "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU18":                                "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU1":                                    "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU75":                                   "gauge",
		"ClickHouseAsyncMetrics_CPUFrequencyMHz_58":                                "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU59":                              "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU55":                                    "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU40":                                    "gauge",
		"ClickHouseAsyncMetrics_OSNiceTime":                                        "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU62":                                  "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU40":                                  "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU16":                                 "gauge",
		"ClickHouseAsyncMetrics_Temperature_coretemp_Core_4":                       "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU41":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU78":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU93":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU98":                                    "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU0":                                    "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU101":                             "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU48":                                   "gauge",
		"ClickHouseAsyncMetrics_LoadAverage15":                                     "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU4":                                  "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU29":                              "gauge",
		"ClickHouseAsyncMetrics_OSGuestNiceTimeCPU97":                              "gauge",
		"ClickHouseAsyncMetrics_FilesystemLogsPathUsedINodes":                      "gauge",
		"ClickHouseAsyncMetrics_OSSoftIrqTimeCPU48":                                "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU4":                                  "gauge",
		"ClickHouseAsyncMetrics_MarkCacheFiles":                                    "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU32":                                  "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU64":                                   "gauge",
		"ClickHouseAsyncMetrics_BlockInFlightOps_md1":                              "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU5":                                    "gauge",
		"ClickHouseAsyncMetrics_CompiledExpressionCacheBytes":                      "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU99":                                  "gauge",
		"ClickHouseAsyncMetrics_OSIOWaitTimeCPU1":                                  "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU37":                                 "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU109":                                 "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU13":                                  "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU97":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU16":                                   "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU23":                                   "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU91":                                   "gauge",
		"ClickHouseAsyncMetrics_OSUserTimeCPU32":                                   "gauge",
		"ClickHouseAsyncMetrics_OSThreadsRunnable":                                 "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU17":                                  "gauge",
		"ClickHouseAsyncMetrics_MarkCacheBytes":                                    "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU33":                                 "gauge",
		"ClickHouseAsyncMetrics_OSSystemTimeCPU21":                                 "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU65":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU25":                                    "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU30":                                  "gauge",
		"ClickHouseAsyncMetrics_OSNiceTimeCPU57":                                   "gauge",
		"ClickHouseAsyncMetrics_OSIrqTimeCPU16":                                    "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU102":                                  "gauge",
		"ClickHouseAsyncMetrics_CompiledExpressionCacheCount":                      "gauge",
		"ClickHouseAsyncMetrics_jemalloc_arenas_all_pdirty":                        "gauge",
		"ClickHouseAsyncMetrics_OSIdleTimeCPU58":                                   "gauge",
		"ClickHouseAsyncMetrics_OSStealTimeCPU86":                                  "gauge",
		"ClickHouseAsyncMetrics_OSGuestTimeCPU0":                                   "gauge",
		"ClickHouseAsyncMetrics_BlockReadBytes_md11":                               "gauge",
		"ClickHouseAsyncMetrics_Jitter":                                            "gauge",
		"ClickHouseStatusInfo_DictionaryStatus":                                    "gauge",
	}
	var (
		metadataNet     string
		metadataAddr    string
		metadataActorID int64
	)
	flag.Int64Var(&metadataActorID, "metadata-actor-id", 0, "")
	flag.StringVar(&metadataAddr, "metadata-addr", "127.0.0.1:2442", "")
	flag.StringVar(&metadataNet, "metadata-net", "tcp4", "")
	flag.StringVar(&argv.aesPwdFile, "aes-pwd-file", "", "path to AES password file, will try to read "+defaultPathToPwd+" if not set")
	build.FlagParseShowVersionHelp()
	flag.Parse()
	client := tlmetadata.Client{
		Client: rpc.NewClient(
			rpc.ClientWithCryptoKey(readAESPwd()),
			rpc.ClientWithTrustedSubnetGroups(build.TrustedSubnetGroups())),
		Network: metadataNet,
		Address: metadataAddr,
		ActorID: metadataActorID,
	}
	loader := metajournal.NewMetricMetaLoader(&client, metajournal.DefaultMetaTimeout)
	var (
		namespaceName = "scrape"
		namespaceID   int32
		storage       *metajournal.MetricsStorage
		workMu        sync.Mutex
		work          = make(map[int32]map[int32]format.MetricMetaValue)
		workCond      = sync.NewCond(&workMu)
	)
	storage = metajournal.MakeMetricsStorage("", nil, nil, func(newEntries []tlmetadata.Event) {
		var n int
		for _, e := range newEntries {
			switch e.EventType {
			case format.NamespaceEvent:
				meta := format.NamespaceMeta{}
				err := json.Unmarshal([]byte(e.Data), &meta)
				if err != nil {
					log.Printf("Cannot marshal metric group %s: %v", meta.Name, err)
					continue
				}
				if meta.Name == namespaceName {
					fmt.Printf("%q namespace ID: %d\n", meta.Name, meta.ID)
					workCond.L.Lock()
					namespaceID = meta.ID
					workCond.L.Unlock()
					n++
				}
			case format.MetricEvent:
				meta := format.MetricMetaValue{}
				err := meta.UnmarshalBinary([]byte(e.Data))
				if err != nil {
					fmt.Println(e.Data)
					fmt.Println(err)
					continue
				}
				if meta.NamespaceID == 0 || meta.NamespaceID == format.BuiltinNamespaceIDDefault {
					continue
				}
				if meta.Kind != format.MetricKindValue ||
					strings.HasSuffix(meta.Name, "_bucket") || strings.HasSuffix(meta.Name, "_sum") ||
					metricType[strings.TrimPrefix(meta.Name, "scrape:")] != "counter" {
					// fmt.Println("skip ", meta.Name)
					continue
				}
				workCond.L.Lock()
				if m := work[meta.NamespaceID]; m != nil {
					m[meta.MetricID] = meta
				} else {
					work[meta.NamespaceID] = map[int32]format.MetricMetaValue{meta.MetricID: meta}
				}
				workCond.L.Unlock()
				n++
			}
		}
		if n != 0 {
			workCond.Signal()
		}
	})
	storage.Journal().Start(nil, nil, loader.LoadJournal)
	fmt.Println("Press <Enter> to start")
	bufio.NewReader(os.Stdin).ReadString('\n')
	fmt.Println("Running")
	for {
		var meta format.MetricMetaValue
		workCond.L.Lock()
	outer:
		for {
			if namespaceID == 0 {
				workCond.Wait()
				continue
			}
			ns := work[namespaceID]
			if len(ns) == 0 {
				workCond.Wait()
				continue
			}
			meta = format.MetricMetaValue{}
			for k, v := range ns {
				if v.Kind != format.MetricKindValue ||
					strings.HasSuffix(v.Name, "_bucket") || strings.HasSuffix(meta.Name, "_sum") ||
					metricType[strings.TrimPrefix(v.Name, "scrape:")] != "counter" {
					delete(ns, k)
					continue
				}
				meta = v
				delete(ns, k)
				break outer
			}
		}
		workCond.L.Unlock()
		v := storage.GetMetaMetric(meta.MetricID)
		if v == nil {
			fmt.Printf("Failed to get metric %q\n", meta.Name)
			continue
		}
		meta = *v
		meta.Kind = format.MetricKindCounter
		fmt.Println(meta.Name, meta.Version)
		var err error
		meta, err = loader.SaveMetric(context.Background(), meta, "")
		if err != nil {
			fmt.Println(err)
			continue
		}
		err = storage.Journal().WaitVersion(context.Background(), meta.Version)
		if err != nil {
			log.Fatal(err)
		}
		time.Sleep(time.Second)
	}
}
