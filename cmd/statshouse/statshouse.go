// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/vkcom/statshouse/internal/agent"
	"github.com/vkcom/statshouse/internal/aggregator"
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/env"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/mapping"
	"github.com/vkcom/statshouse/internal/metajournal"
	"github.com/vkcom/statshouse/internal/pcache"
	"github.com/vkcom/statshouse/internal/receiver"
	"github.com/vkcom/statshouse/internal/stats"
	"github.com/vkcom/statshouse/internal/util"
	"github.com/vkcom/statshouse/internal/vkgo/build"
	"github.com/vkcom/statshouse/internal/vkgo/platform"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
	"github.com/vkcom/statshouse/internal/vkgo/srvfunc"
)

var (
	logOk  *log.Logger
	logErr *log.Logger
	logFd  *os.File
)

func reopenLog() {
	var err error
	logFd, err = srvfunc.LogRotate(logFd, argv.logFile)
	if err != nil {
		_, _ = os.Stderr.WriteString(fmt.Sprintf("Cannot log to file %q: %v", argv.logFile, err))
		return
	}

	logOk.SetOutput(logFd)
	logErr.SetOutput(logFd)
}

var globalStartTime = time.Now() // good enough for us

func main() {
	// data_model.PrintLinearMaxHostProbabilities()
	os.Exit(runMain())
}

func runMain() int {
	pidStr := strconv.Itoa(os.Getpid())
	logOk = log.New(os.Stdout, "LOG "+pidStr+" ", log.LstdFlags|log.Lshortfile|log.Lmicroseconds)
	logErr = log.New(os.Stderr, "ERR "+pidStr+" ", log.LstdFlags|log.Lshortfile|log.Lmicroseconds)

	var verb string
	legacyVerb := false
	if len(os.Args) < 2 {
		printVerbUsage()
		return 1
	}
	// Motivation - some engine infrastructure cannot add options without dash. so wi allow both
	// $> statshouse agent -a -b -c
	// and
	// $> statshouse -agent -a -b -c
	if os.Args[1] != "" && os.Args[1][0] == '-' &&
		os.Args[1] != "-benchmark" && os.Args[1] != "--benchmark" &&
		os.Args[1] != "-test_map" && os.Args[1] != "--test_map" &&
		os.Args[1] != "-test_longpoll" && os.Args[1] != "--test_longpoll" &&
		os.Args[1] != "-simple_fsync" && os.Args[1] != "--simple_fsync" &&
		os.Args[1] != "-tlclient.api" && os.Args[1] != "--tlclient.api" &&
		os.Args[1] != "-tlclient" && os.Args[1] != "--tlclient" &&
		os.Args[1] != "-simulator" && os.Args[1] != "--simulator" &&
		os.Args[1] != "-agent" && os.Args[1] != "--agent" &&
		os.Args[1] != "-aggregator" && os.Args[1] != "--aggregator" &&
		os.Args[1] != "-ingress_proxy" && os.Args[1] != "--ingress_proxy" { // legacy flags mode
		// TODO - remove this path when all statshouses command lines are updated
		legacyVerb = true

		var newConveyor string
		flag.StringVar(&newConveyor, "new-conveyor", "agent", "'aggregator', 'agent' (default), 'ingress_proxy'")

		argvAddDeprecatedFlags()
		argvAddCommonFlags()
		argvAddAgentFlags(true)
		argvAddAggregatorFlags(true)
		argvAddIngressProxyFlags()
		build.FlagParseShowVersionHelp()
		switch newConveyor {
		case "aggregate", "aggregator": // old name
			verb = "aggregator"
		case "agent", "duplicate_map":
			verb = "agent"
		case "ingress_proxy":
			verb = newConveyor
		default:
			logErr.Printf("Wrong value for -new-conveyor argument %q, see --help for valid values", newConveyor)
			return 1
		}
	} else {
		verb = os.Args[1]
		copy(os.Args[1:], os.Args[2:])
		os.Args = os.Args[:len(os.Args)-1]
		switch verb {
		case "test_parser", "-test_parser", "--test_parser":
			return mainTestParser()
		case "benchmark", "-benchmark", "--benchmark":
			mainBenchmarks()
			return 0
		case "test_map", "-test_map", "--test_map":
			mainTestMap()
			return 0
		case "test_longpoll", "-test_longpoll", "--test_longpoll":
			mainTestLongpoll()
			return 0
		case "simple_fsync", "-simple_fsync", "--simple_fsync":
			mainSimpleFSyncTest()
			return 0
		case "tlclient.api", "-tlclient.api", "--tlclient.api":
			mainTLClientAPI()
			return 0
		case "tlclient", "-tlclient", "--tlclient":
			return mainTLClient()
		case "simulator", "-simulator", "--simulator":
			mainSimulator()
			return 0
		case "agent", "-agent", "--agent":
			argvAddCommonFlags()
			argvAddAgentFlags(false)
			build.FlagParseShowVersionHelp()
		case "aggregator", "-aggregator", "--aggregator":
			argvAddCommonFlags()
			argvAddAggregatorFlags(false)
			build.FlagParseShowVersionHelp()
		case "ingress_proxy", "-ingress_proxy", "--ingress_proxy":
			argvAddCommonFlags()
			argvAddAgentFlags(false)
			argvAddIngressProxyFlags()
			argv.configAgent = agent.DefaultConfig()
			build.FlagParseShowVersionHelp()
		case "tag_mapping", "-tag_mapping", "--tag_mapping":
			mainTagMapping()
			return 0
		case "publish_tag_drafts", "-publish_tag_drafts", "--publish_tag_drafts":
			mainPublishTagDrafts()
			return 0
		default:
			_, _ = fmt.Fprintf(os.Stderr, "Unknown verb %q:\n", verb)
			printVerbUsage()
			return 1
		}
	}

	if _, err := srvfunc.SetHardRLimitNoFile(argv.maxOpenFiles); err != nil {
		logErr.Printf("Could not set new rlimit: %v", err)
	}

	aesPwd := readAESPwd()

	if argv.ingressPwdDir != "" {
		if err := argv.configIngress.ReadIngressKeys(argv.ingressPwdDir); err != nil {
			logErr.Printf("could not read ingress keys: %v", err)
			return 1
		}
	}

	if err := platform.ChangeUserGroup(argv.userLogin, argv.userGroup); err != nil {
		logErr.Printf("Could not change user/group to %q/%q: %v", argv.userLogin, argv.userGroup, err)
		return 1
	}

	reopenLog()

	if argv.cacheDir != "" {
		_ = os.Mkdir(argv.cacheDir, os.ModePerm) // create dir, but not parent dirs
	}

	var dc *pcache.DiskCache                                  // We support working without touching disk (on readonly filesystems)
	if argv.cacheDir == "" && argv.historicStorageDir != "" { // legacy mode option. TODO - remove
		argv.cacheDir = argv.historicStorageDir
	}
	if argv.cacheDir == "" && argv.diskCacheFilename != "" { // legacy mode option. TODO - remove
		argv.cacheDir = filepath.Dir(argv.diskCacheFilename)
	}
	if argv.cacheDir != "" {
		var err error
		if dc, err = pcache.OpenDiskCache(filepath.Join(argv.cacheDir, "mapping_cache.sqlite3"), pcache.DefaultTxDuration); err != nil {
			logErr.Printf("failed to open disk cache: %v", err)
			return 1
		}
		// we do not want to delay shutdown for saving cache
		// defer func() {
		//	if err := dc.Close(); err != nil {
		//		logErr.Printf("failed to close disk cache: %v", err)
		//	}
		// }()
	}

	argv.configAgent.AggregatorAddresses = strings.Split(argv.aggAddr, ",")

	if _, err := strconv.Atoi(argv.listenAddr); err == nil { // old convention of using port
		argv.listenAddr = ":" + argv.listenAddr // convert to addr
	}

	if argv.customHostName == "" {
		argv.customHostName = srvfunc.HostnameForStatshouse()
		logOk.Printf("detected statshouse hostname as %q from OS hostname %q\n", argv.customHostName, srvfunc.Hostname())
	}

	switch verb {
	case "agent", "-agent", "--agent":
		if !legacyVerb && len(argv.configAgent.AggregatorAddresses) != 3 {
			logErr.Printf("-agg-addr must contain comma-separated list of 3 aggregators (1 shard is recommended)")
			return 1
		}
		mainAgent(aesPwd, dc)
	case "aggregator", "-aggregator", "--aggregator":
		mainAggregator(aesPwd, dc)
	case "ingress_proxy", "-ingress_proxy", "--ingress_proxy":
		if len(argv.configAgent.AggregatorAddresses) != 3 {
			logErr.Printf("-agg-addr must contain comma-separated list of 3 aggregators (1 shard is recommended)")
			return 1
		}
		mainIngressProxy(aesPwd)
	default:
		logErr.Printf("Wrong command line verb or -new-conveyor argument %q, see --help for valid values", verb)
	}
	return 0
}

func mainAgent(aesPwd string, dc *pcache.DiskCache) int {
	startDiscCacheTime := time.Now() // we only have disk cache before. Be carefull when redesigning
	argv.configAgent.Cluster = argv.cluster
	if err := argv.configAgent.ValidateConfigSource(); err != nil {
		logErr.Printf("%s", err)
		return 1
	}

	if argv.coresUDP < 0 {
		logErr.Printf("--cores-udp must be set to at least 0")
		return 1
	}
	if argv.maxCores < 0 {
		argv.maxCores = 1 + argv.coresUDP*3/2
	}
	if argv.maxCores > 0 {
		runtime.GOMAXPROCS(argv.maxCores)
	}

	runPprof()

	var (
		receiversUDP  []*receiver.UDP
		metricStorage = metajournal.MakeMetricsStorage(argv.configAgent.Cluster, dc, nil)
	)
	envLoader, _ := env.ListenEnvFile(argv.envFilePath)

	sh2, err := agent.MakeAgent("tcp",
		argv.cacheDir,
		aesPwd,
		argv.configAgent,
		argv.customHostName,
		format.TagValueIDComponentAgent,
		metricStorage,
		dc,
		log.Printf,
		func(a *agent.Agent, unixNow uint32) {
			k := data_model.Key{
				Timestamp: unixNow,
				Metric:    format.BuiltinMetricIDAgentUDPReceiveBufferSize,
			}
			for _, r := range receiversUDP {
				v := float64(r.ReceiveBufferSize())
				a.AddValueCounter(k, v, 1, format.BuiltinMetricMetaAgentUDPReceiveBufferSize)
			}
			if dc != nil {
				s, err := dc.DiskSizeBytes()
				if err == nil {
					a.AddValueCounter(data_model.Key{Timestamp: unixNow, Metric: format.BuiltinMetricIDAgentDiskCacheSize, Tags: [16]int32{0, 0, 0}}, float64(s), 1, format.BuiltinMetricMetaAgentDiskCacheSize)
				}
			}
		},
		nil,
		envLoader)
	if err != nil {
		logErr.Printf("error creating Agent instance: %v", err)
		return 1
	}

	var logPackets func(format string, args ...interface{})

	switch argv.logLevel {
	case "info", "":
		break
	case "trace":
		logPackets = logOk.Printf
	default:
		logErr.Printf("--log-level should be either 'trace', 'info' or empty (which is synonym for 'info')")
		return 1
	}
	var mirrorUdpConn net.Conn
	if argv.mirrorUdpAddr != "" {
		logOk.Printf("mirror UDP addr %q", argv.mirrorUdpAddr)
		var err error
		mirrorUdpConn, err = net.Dial("udp", argv.mirrorUdpAddr)
		if err != nil {
			logErr.Printf("failed to connect to mirror UDP addr %q: %v", argv.mirrorUdpAddr, err)
			// not fatal, we can continue without mirror
		} else {
			defer func() { _ = mirrorUdpConn.Close() }()
		}
	}
	listenUDP := func(network string, addr string) error {
		if argv.coresUDP == 0 || addr == "" {
			return nil
		}
		u, err := receiver.ListenUDP(network, addr, argv.bufferSizeUDP, false, sh2, mirrorUdpConn, logPackets)
		if err != nil {
			logErr.Printf("listen %q failed: %v", network, err)
			return err
		}
		receiversUDP = append(receiversUDP, u)
		for i := 1; i < argv.coresUDP; i++ {
			dup, err := u.Duplicate()
			if err != nil {
				logErr.Printf("duplicate listen socket failed: %v", err)
				return err
			}
			receiversUDP = append(receiversUDP, dup)
		}
		logOk.Printf("listen %q addr %q by %d cores", network, addr, argv.coresUDP)
		return nil
	}
	if err := listenUDP("udp4", argv.listenAddr); err != nil {
		return 1
	}
	if err := listenUDP("udp6", argv.listenAddrIPv6); err != nil {
		return 1
	}
	if err := listenUDP("unixgram", argv.listenAddrUnix); err != nil {
		return 1
	}
	sh2.Run(0, 0, 0)
	metricStorage.Journal().Start(sh2, nil, sh2.LoadMetaMetricJournal)

	var ac *mapping.AutoCreate
	if argv.configAgent.AutoCreate {
		ac = mapping.NewAutoCreate(metricStorage, sh2.AutoCreateMetric)
		defer ac.Shutdown()
	}

	w := startWorker(sh2,
		metricStorage,
		sh2.LoadOrCreateMapping,
		dc,
		ac,
		argv.configAgent.Cluster,
		logPackets,
	)
	//code to populate cache for test below
	//for i := 0; i != 10000000; i++ {
	//	v := "hren_test_" + strconv.Itoa(i)
	//	if err := dc.Set(ns, v, []byte(v), slowNow, time.Hour); err != nil {
	//		log.Fatalf("hren")
	//	}
	//}
	//dc.Close()
	//code to test that cache cleanup does not affect normal cache look ups
	//go func() {
	//	ns := data_model.TagValueDiskNamespace + "default"
	//	for {
	//		time.Sleep(time.Second)
	//		slowNow := time.Now()
	//		_, _, _, err, _ := dc.Get(ns, "hren")
	//		log.Printf("Get() took %v error %v", time.Since(slowNow), err)
	//	}
	//}()
	tagsCacheEmpty := w.mapper.TagValueDiskCacheEmpty()
	if !tagsCacheEmpty {
		logOk.Printf("Tag Value cache not empty")
	} else {
		logOk.Printf("Tag Value cache empty, loading boostrap...")
		mappings, ttl, err := sh2.GetTagMappingBootstrap(context.Background())
		if err != nil {
			logErr.Printf("failed to load boostrap mappings: %v", err)
		} else {
			now := time.Now()
			for _, ma := range mappings {
				if err := w.mapper.SetBootstrapValue(now, ma.Str, pcache.Int32ToValue(ma.Value), ttl); err != nil {
					logErr.Printf("failed to set boostrap mapping %q <-> %d: %v", ma.Str, ma.Value, err)
				}
			}
			logOk.Printf("Loaded and set %d boostrap mappings", len(mappings))
		}
	}

	receiversWG := sync.WaitGroup{}
	for i, u := range receiversUDP {
		receiversWG.Add(1)
		go func(u *receiver.UDP, num int) {
			err := u.Serve(w)
			if err != nil {
				logErr.Fatalf("Serve: %v", err)
			}
			log.Printf("UDP listener %d finished", num)
			receiversWG.Done()
		}(u, i)
	}
	// UDP receivers are receiving data, so we consider agent started (not losing UDP packets already)
	shutdownInfoReport(sh2, format.TagValueIDComponentAgent, argv.cacheDir, startDiscCacheTime)

	// Open TCP ports
	listeners := make([]net.Listener, 0, 2)
	listeners = append(listeners, listen("tcp4", argv.listenAddr))
	if argv.listenAddrIPv6 != "" {
		listeners = append(listeners, listen("tcp6", argv.listenAddrIPv6))
	}

	// Run pprof server
	hijackTCP := rpc.NewHijackListener(listeners[0].Addr())
	hijackHTTP := rpc.NewHijackListener(listeners[0].Addr())
	defer func() { _ = hijackTCP.Close() }()
	defer func() { _ = hijackHTTP.Close() }()

	receiverTCP := receiver.NewTCPReceiver(sh2, logPackets)
	go func() {
		if err := receiverTCP.Serve(w, hijackTCP); err != nil {
			logErr.Printf("error serving TCP: %v", err)
		}
	}()

	receiverHTTP := receiver.NewHTTPReceiver(sh2, logPackets)
	go func() {
		if err := receiverHTTP.Serve(w, hijackHTTP); err != nil {
			logErr.Printf("error serving HTTP: %v", err)
		}
	}()

	// Run RPC server
	receiverRPC := receiver.MakeRPCReceiver(sh2, w)
	handlerRPC := &tlstatshouse.Handler{
		RawAddMetricsBatch: receiverRPC.RawAddMetricsBatch,
	}
	metrics := util.NewRPCServerMetrics("statshouse_agent")
	options := []rpc.ServerOptionsFunc{
		rpc.ServerWithLogf(logErr.Printf),
		rpc.ServerWithVersion(build.Info()),
		rpc.ServerWithCryptoKeys([]string{aesPwd}),
		rpc.ServerWithTrustedSubnetGroups(build.TrustedSubnetGroups()),
		rpc.ServerWithHandler(handlerRPC.Handle),
		rpc.ServerWithStatsHandler(statsHandler{receiversUDP: receiversUDP, receiverRPC: receiverRPC, sh2: sh2, metricsStorage: metricStorage}.handleStats),
		metrics.ServerWithMetrics,
	}
	options = append(options, rpc.ServerWithSocketHijackHandler(func(conn *rpc.HijackConnection) {
		if strings.HasPrefix(string(conn.Magic), receiver.TCPPrefix) {
			conn.Magic = conn.Magic[len(receiver.TCPPrefix):]
			hijackTCP.AddConnection(conn)
			return
		}
		hijackHTTP.AddConnection(conn)
	}))
	srv := rpc.NewServer(options...)
	defer metrics.Run(srv)()
	for _, ln := range listeners {
		go serveRPC(ln, srv)
	}

	// Run scrape
	receiver.RunScrape(sh2, w)
	if !argv.hardwareMetricScrapeDisable {
		m, err := stats.NewCollectorManager(stats.CollectorManagerOptions{ScrapeInterval: argv.hardwareMetricScrapeInterval, HostName: argv.customHostName}, w, envLoader, logErr)
		if err != nil {
			logErr.Println("failed to init hardware collector", err.Error())
		} else {
			go func() {
				err := m.RunCollector()
				if err != nil {
					logErr.Println("failed to run hardware collector", err.Error())
				}
			}()
			defer m.StopCollector()
		}
	}
	chSignal := make(chan os.Signal, 1)
	signal.Notify(chSignal, syscall.SIGINT, syscall.SIGUSR1)

loop:
	for {
		sig := <-chSignal
		switch sig {
		case syscall.SIGINT:
			break loop
		case syscall.SIGUSR1:
			logOk.Printf("Logrotate")
			reopenLog()
		}
	}
	shutdownInfo := tlstatshouse.ShutdownInfo{}
	now := time.Now()
	shutdownInfo.StartShutdownTime = now.UnixNano()

	logOk.Printf("Shutting down...")
	logOk.Printf("1. Disabling sending new data to aggregators...")
	sh2.DisableNewSends()
	logOk.Printf("2. Waiting recent senders to finish sending data...")
	sh2.WaitRecentSenders(time.Second * data_model.InsertDelay)
	shutdownInfo.StopRecentSenders = shutdownInfoDuration(&now).Nanoseconds()
	logOk.Printf("3. Closing UDP/unixdgram/RPC server and flusher...")
	for _, u := range receiversUDP {
		_ = u.Close()
	}
	sh2.ShutdownFlusher()
	srv.Shutdown()
	receiversWG.Wait()
	shutdownInfo.StopReceivers = shutdownInfoDuration(&now).Nanoseconds()
	logOk.Printf("4. All UDP readers finished...")
	/// TODO - we receive almost no metrics via RPC, we do not want risk waiting here for the long time
	/// _ = srv.Close()
	/// logOk.Printf("5. RPC server stopped...")
	sh2.WaitFlusher()
	shutdownInfo.StopFlusher = shutdownInfoDuration(&now).Nanoseconds()
	logOk.Printf("6. Flusher stopped, flushing remainig data to preprocessors...")
	nonEmpty := sh2.FlushAllData()
	shutdownInfo.StopFlushing = shutdownInfoDuration(&now).Nanoseconds()
	logOk.Printf("7. Waiting preprocessor to save %d buckets of historic data...", nonEmpty)
	sh2.WaitPreprocessor()
	shutdownInfo.StopPreprocessor = shutdownInfoDuration(&now).Nanoseconds()
	shutdownInfo.FinishShutdownTime = now.UnixNano()
	shutdownInfoSave(argv.cacheDir, shutdownInfo)
	logOk.Printf("Bye")
	return 0
}

func mainAggregator(aesPwd string, dc *pcache.DiskCache) int {
	startDiscCacheTime := time.Now() // we only have disk cache before. Be carefull when redesigning
	if err := aggregator.ValidateConfigAggregator(argv.configAggregator); err != nil {
		logErr.Printf("%s", err)
		return 1
	}

	argv.configAggregator.Cluster = argv.cluster

	if len(argv.aggAddr) == 0 {
		logErr.Printf("--agg-addr to listen must be specified")
		return 1
	}
	agg, err := aggregator.MakeAggregator(dc, argv.cacheDir, argv.aggAddr, aesPwd, argv.configAggregator, argv.customHostName, argv.logLevel == "trace")
	if err != nil {
		logErr.Printf("%v", err)
		return 1
	}
	shutdownInfoReport(agg.Agent(), format.TagValueIDComponentAggregator, argv.cacheDir, startDiscCacheTime)

	chSignal := make(chan os.Signal, 1)
	signal.Notify(chSignal, syscall.SIGINT, syscall.SIGUSR1)

loop:
	for {
		sig := <-chSignal
		switch sig {
		case syscall.SIGINT:
			break loop
		case syscall.SIGUSR1:
			logOk.Printf("Aggregators do not support log rotation") // admin might expect rotation, tell them.
		}
	}
	shutdownInfo := tlstatshouse.ShutdownInfo{}
	now := time.Now()
	shutdownInfo.StartShutdownTime = now.UnixNano()

	logOk.Printf("Shutting down...")
	logOk.Printf("1. Disabling inserting new data to clickhouses...")
	agg.DisableNewInsert()
	logOk.Printf("2. Waiting all inserts to finish...")
	agg.WaitInsertsFinish(data_model.ClickHouseTimeoutShutdown)
	shutdownInfo.StopInserters = shutdownInfoDuration(&now).Nanoseconds()
	// Now when inserts are finished and responses are in send queues of RPC connections,
	// we can initiate shutdown by sending LetsFIN packets and waiting to actual FINs.
	logOk.Printf("3. Starting gracefull RPC shutdown...")
	agg.ShutdownRPCServer()
	logOk.Printf("4. Waiting RPC clients to receive responses and disconnect...")
	agg.WaitRPCServer(10 * time.Second)
	shutdownInfo.StopRPCServer = shutdownInfoDuration(&now).Nanoseconds()
	shutdownInfo.FinishShutdownTime = now.UnixNano()
	shutdownInfoSave(argv.cacheDir, shutdownInfo)
	logOk.Printf("Bye")
	return 0
}

func mainIngressProxy(aesPwd string) {
	// Ensure proxy configuration is valid
	config := argv.configIngress
	config.Network = "tcp"
	config.Cluster = argv.cluster
	config.ExternalAddresses = strings.Split(argv.ingressExtAddr, ",")
	config.ExternalAddressesIPv6 = strings.Split(argv.ingressExtAddrIPv6, ",")

	// Ensure agent configuration is valid
	if err := argv.configAgent.ValidateConfigSource(); err != nil {
		logErr.Fatalf("%v", err)
	}

	runPprof()

	// Run agent (we use agent instance for ingress proxy built-in metrics)
	argv.configAgent.Cluster = argv.cluster
	sh2, err := agent.MakeAgent("tcp", argv.cacheDir, aesPwd, argv.configAgent, argv.customHostName,
		format.TagValueIDComponentIngressProxy, nil, nil, log.Printf, nil, nil, nil)
	if err != nil {
		logErr.Fatalf("error creating Agent instance: %v", err)
	}
	sh2.Run(0, 0, 0)
	if argv.ingressVersion == "2" {
		ctx, cancel := context.WithCancel(context.Background())
		exit := make(chan error, 1)
		go func() {
			exit <- aggregator.RunIngressProxy2(ctx, sh2, config, aesPwd)
		}()
		sigint := make(chan os.Signal, 1)
		signal.Notify(sigint, syscall.SIGINT)
		select {
		case <-sigint:
			cancel()
			select {
			case <-exit:
			case <-time.After(5 * time.Second):
			}
			logOk.Println("Buy")
		case err := <-exit:
			logErr.Println(err)
			cancel()
		}
		return
	}

	// Ensure proxy configuration is valid
	if len(config.ExternalAddresses) != 3 {
		logErr.Fatalf("--ingress-external-addr must contain exactly 3 comma-separated addresses of ingress proxies, contains '%q'", strings.Join(config.ExternalAddresses, ","))
	}
	if len(config.IngressKeys) == 0 {
		logErr.Fatalf("ingress proxy must have non-empty list of ingress crypto keys")
	}

	// Run ingress proxy
	ln, err := rpc.Listen(config.Network, config.ListenAddr, false)
	if err != nil {
		logErr.Fatalf("Failed to listen on %s %s: %v", config.Network, config.ListenAddr, err)
	}
	err = aggregator.RunIngressProxy(ln, sh2, aesPwd, config)
	if err != nil {
		logErr.Fatalf("error running ingress proxy: %v", err)
	}
}

func listen(network, address string) net.Listener {
	ln, err := net.Listen(network, address)
	if err != nil {
		logErr.Fatalf("Failed to listen on %s %s: %v", network, address, err)
	}
	return ln
}

func serveRPC(ln net.Listener, server *rpc.Server) {
	err := server.Serve(ln)
	if err != nil {
		logErr.Fatalf("RPC server failed to serve on %s: %v", ln.Addr(), err)
	}
}

func runPprof() {
	if argv.pprofHTTP {
		logErr.Printf("warning: --pprof-http option deprecated due to security reasons. Please use explicit --pprof=127.0.0.1:11123 option")
	}
	if argv.pprofListenAddr != "" {
		go func() {
			if err := http.ListenAndServe(argv.pprofListenAddr, nil); err != nil {
				logErr.Printf("failed to listen pprof on %q: %v", argv.pprofListenAddr, err)
			}
		}()
	}
}
