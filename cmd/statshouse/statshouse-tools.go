// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package main

import (
	"bufio"
	"context"
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

	u, err := receiver.ListenUDP("udp", argv.listenAddr, argv.bufferSizeUDP, false, nil, nil, log.Printf)
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
		if testFastPath && h.Key.Tags[1] != 1 {
			wrongTag1.Inc()
			return
		}
		goodMetric.Inc()
	}
	metricStorage := metajournal.MakeMetricsStorage("", nil, nil)
	metricStorage.Journal().Start(nil, nil, dolphinLoader)
	mapper := mapping.NewMapper("", pmcLoader, nil, nil, 1000, handleMappedMetric)

	recv, err := receiver.ListenUDP("udp", listenAddr, receiver.DefaultConnBufSize, true, nil, nil, nil)
	if err != nil {
		log.Fatalf("%v", err)
	}
	recv2, err := receiver.ListenUDP("udp", listenAddr, receiver.DefaultConnBufSize, true, nil, nil, nil)
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
			w = args.WriteBoxed(w)
		}
		// _ = args.WriteBoxed(&w)
		for {
			if !testFastPath {
				w = w[:0]
				args.Metrics[0].Tags[0].Value = append(args.Metrics[0].Tags[0].Value[:0], keyPrefix...)
				args.Metrics[0].Tags[0].Value = strconv.AppendInt(args.Metrics[0].Tags[0].Value, int64(rand.New().Int31()), 10)
				w = args.WriteBoxed(w)
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
				done = mapper.Map(data_model.HandlerArgs{MetricBytes: m, MapCallback: cb}, metricStorage.GetMetaMetricByNameBytes(m.Name), &h)
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
	extra := rpc.InvokeReqExtra{FailIfNoConnection: true}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5) // TODO - option to set timeout
	defer cancel()
	if err := tlclient.AddMetricsBatchBytes(ctx, batch, &extra, &ret); err != nil {
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
		config   aggregator.KnownTags
		storage  *metajournal.MetricsStorage
		workMu   sync.Mutex
		work     = make(map[int32]map[int32]format.MetricMetaValue)
		workCond = sync.NewCond(&workMu)
	)
	storage = metajournal.MakeMetricsStorage("", nil, nil, func(newEntries []tlmetadata.Event) {
		var n int
		for _, e := range newEntries {
			switch e.EventType {
			case format.MetricEvent:
				meta := format.MetricMetaValue{}
				err := meta.UnmarshalBinary([]byte(e.Data))
				if err != nil {
					fmt.Fprintln(os.Stderr, e.Data)
					fmt.Fprintln(os.Stderr, err)
					continue
				}
				if meta.NamespaceID == 0 || meta.NamespaceID == format.BuiltinNamespaceIDDefault {
					continue
				}
				if len(meta.TagsDraft) == 0 {
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
			case format.PromConfigEvent:
				v, err := aggregator.ParseKnownTags([]byte(e.Data), storage)
				fmt.Fprintln(os.Stderr, e.Data)
				if err != nil {
					fmt.Fprintln(os.Stderr, err)
					continue
				}
				workCond.L.Lock()
				config = v
				workCond.L.Unlock()
				n++
			}
		}
		if n != 0 {
			workCond.Signal()
		}
	})
	storage.Journal().Start(nil, nil, loader.LoadJournal)
	fmt.Println("Press <Enter> to start publishing tag drafts")
	bufio.NewReader(os.Stdin).ReadString('\n')
	fmt.Println("Publishing tag drafts")
	for {
		var meta format.MetricMetaValue
		workCond.L.Lock()
	outer:
		for {
			var ns map[int32]format.MetricMetaValue
			for _, v := range work {
				if len(v) != 0 {
					ns = v
					break
				}
			}
			if len(ns) == 0 {
				workCond.Wait()
				continue
			}
			for k, v := range ns {
				meta = v
				delete(ns, k)
				break outer
			}
		}
		workCond.L.Unlock()
		v := storage.GetMetaMetric(meta.MetricID)
		if v == nil {
			fmt.Fprintf(os.Stderr, "Failed to get metric %q\n", meta.Name)
			continue
		}
		meta = *v
		workCond.L.Lock()
		n := config.PublishDraftTags(&meta)
		workCond.L.Unlock()
		if n == 0 {
			continue
		}
		fmt.Println(meta.NamespaceID, meta.Name, meta.Version)
		var err error
		meta, err = loader.SaveMetric(context.Background(), meta, "")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			continue
		}
		err = storage.Journal().WaitVersion(context.Background(), meta.Version)
		if err != nil {
			log.Fatal(err)
		}
	}
}
