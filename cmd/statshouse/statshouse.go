// Copyright 2025 V Kontakte LLC
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
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/env"
	"github.com/vkcom/statshouse/internal/format"
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

const defaultPathToPwd = `/etc/engine/pass`

var argv struct {
	logFile                      string
	logLevel                     string
	userLogin                    string // логин для setuid
	userGroup                    string // логин для setguid
	maxOpenFiles                 uint64
	pprofListenAddr              string
	pprofHTTP                    bool
	aesPwdFile                   string
	cacheDir                     string
	customHostName               string // useful for testing and in some environments
	aggAddr                      string // common, different meaning
	maxCores                     int
	listenAddr                   string
	listenAddrIPv6               string
	listenAddrUnix               string
	mirrorUdpAddr                string
	coresUDP                     int
	bufferSizeUDP                int
	promRemoteMod                bool
	hardwareMetricScrapeInterval time.Duration
	hardwareMetricScrapeDisable  bool
	envFilePath                  string

	// test_map mode
	mapString string

	// tlclient mode
	statshouseAddr string
	statshouseNet  string

	// tag_mapping mode
	metric          string
	tags            string
	budget          int
	metadataNet     string
	metadataAddr    string
	metadataActorID int64

	// publish_tag_drafts mode
	maxUpdates int
	dryRun     bool

	// for old mode
	historicStorageDir string
	diskCacheFilename  string

	agent.Config
}

type mainAgent struct {
	agent           *agent.Agent
	diskCache       *pcache.DiskCache
	worker          *worker
	mirrorUdpConn   net.Conn
	receiverHTTP    *receiver.HTTP
	receiverTCP     *receiver.TCP
	receiversUDP    []*receiver.UDP
	hijackHTTP      *rpc.HijackListener
	hijackTCP       *rpc.HijackListener
	receiversWG     sync.WaitGroup
	logPackets      func(format string, args ...interface{})
	hardwareMetrics *stats.CollectorManager
}

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

func main() {
	// runtime.MemProfileRate = 1024 // sometimes useful for local pprof

	// helpfull when refactoring flags in meta metrics
	// data, _ := json.Marshal(format.BuiltinMetrics)
	// _ = os.WriteFile("builtin2.json", data, 0644)

	pidStr := strconv.Itoa(os.Getpid())
	logOk = log.New(os.Stdout, "LOG "+pidStr+" ", log.LstdFlags|log.Lshortfile|log.Lmicroseconds)
	logErr = log.New(os.Stderr, "ERR "+pidStr+" ", log.LstdFlags|log.Lshortfile|log.Lmicroseconds)
	// data_model.PrintLinearMaxHostProbabilities()
	if entrypoint, err := parseCommandLine(); err != nil {
		log.Fatalln(err)
	} else if entrypoint != nil {
		code := entrypoint()
		fmt.Println() // ensure command prompt starts at new line, it's annoying when not
		os.Exit(code)
	}
}

func run() int {
	if _, err := srvfunc.SetHardRLimitNoFile(argv.maxOpenFiles); err != nil {
		logErr.Printf("Could not set new rlimit: %v", err)
	}

	aesPwd := readAESPwd()
	if err := platform.ChangeUserGroup(argv.userLogin, argv.userGroup); err != nil {
		logErr.Printf("Could not change user/group to %q/%q: %v", argv.userLogin, argv.userGroup, err)
		return 1
	}

	reopenLog()

	if argv.cacheDir != "" {
		_ = os.Mkdir(argv.cacheDir, os.ModePerm) // create dir, but not parent dirs
	}

	var main mainAgent
	var fpmc *os.File
	if argv.cacheDir != "" { // we support working without touching disk (on readonly filesystems, in stateless containers, etc)
		var err error
		if main.diskCache, err = pcache.OpenDiskCache(filepath.Join(argv.cacheDir, "mapping_cache.sqlite3"), pcache.DefaultTxDuration); err != nil {
			logErr.Printf("failed to open disk cache: %v", err)
			return 1
		}
		// we do not want to delay shutdown for saving cache
		// defer func() {
		//	if err := dc.Close(); err != nil {
		//		logErr.Printf("failed to close disk cache: %v", err)
		//	}
		// }()

		// we do not want to confuse mappings from different clusters, this would be a disaster
		fpmc, err = os.OpenFile(filepath.Join(argv.cacheDir, fmt.Sprintf("mappings-%s.cache", argv.Cluster)), os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			logErr.Printf("failed to open agent mappings cache: %v", err)
			return 1
		}
		defer fpmc.Close()
	}
	mappingsCache, _ := pcache.LoadMappingsCacheFile(fpmc, argv.MappingCacheSize, argv.MappingCacheTTL) // we ignore error because cache can be damaged

	startDiscCacheTime := time.Now() // we only have disk cache before. Be carefull when redesigning
	if argv.maxCores > 0 {
		runtime.GOMAXPROCS(argv.maxCores)
	}

	if argv.pprofListenAddr != "" {
		go main.runPprof()
	}

	metricStorage := metajournal.MakeMetricsStorage(nil)
	var fj *os.File
	if argv.cacheDir != "" {
		// we do not want to confuse journal from different clusters, this would be a disaster
		var err error
		fj, err = os.OpenFile(filepath.Join(argv.cacheDir, fmt.Sprintf("journal-compact-%s.cache", argv.Cluster)), os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			logErr.Printf("failed to open journal cache: %v", err)
			return 1
		}
		defer fj.Close()
	}

	// we ignore error because cache can be damaged
	// we do not pass 'compact' to agent journal because journal is compacted on aggregator
	journalFast, _ := metajournal.LoadJournalFastFile(fj, data_model.JournalDDOSProtectionAgentTimeout, false,
		[]metajournal.ApplyEvent{metricStorage.ApplyEvent})
	if argv.cacheDir != "" {
		journalFast.SetDumpPathPrefix(filepath.Join(argv.cacheDir, fmt.Sprintf("journal-compact-%s", argv.Cluster)))
	}
	// This code is used to investigate journal loading efficiency.
	//if err := http.ListenAndServe(":9999", nil); err != nil {
	//	panic(err)
	//}

	envLoader, _ := env.ListenEnvFile(argv.envFilePath)

	var err error
	main.agent, err = agent.MakeAgent("tcp",
		argv.cacheDir,
		aesPwd,
		argv.Config,
		argv.customHostName,
		format.TagValueIDComponentAgent,
		metricStorage, mappingsCache,
		nil, nil, journalFast.VersionHash,
		log.Printf,
		main.beforeFlushBucket,
		nil,
		envLoader)
	if err != nil {
		logErr.Printf("error creating Agent instance: %v", err)
		return 1
	}

	switch argv.logLevel {
	case "info", "":
		break
	case "trace":
		main.logPackets = logOk.Printf
	default:
		logErr.Printf("--log-level should be either 'trace', 'info' or empty (which is synonym for 'info')")
		return 1
	}
	if argv.mirrorUdpAddr != "" {
		logOk.Printf("mirror UDP addr %q", argv.mirrorUdpAddr)
		var err error
		main.mirrorUdpConn, err = net.Dial("udp", argv.mirrorUdpAddr)
		if err != nil {
			logErr.Printf("failed to connect to mirror UDP addr %q: %v", argv.mirrorUdpAddr, err)
			// not fatal, we can continue without mirror
		} else {
			defer func() { _ = main.mirrorUdpConn.Close() }()
		}
	}
	if err := main.listenUDP("udp4", argv.listenAddr); err != nil {
		return 1
	}
	if err := main.listenUDP("udp6", argv.listenAddrIPv6); err != nil {
		return 1
	}
	if err := main.listenUDP("unixgram", argv.listenAddrUnix); err != nil {
		return 1
	}
	chSignal := make(chan os.Signal, 1)
	go func() {
		main.agent.GoGetConfig() // if terminates, agent must restart
		chSignal <- syscall.SIGINT
	}()
	main.agent.Run(0, 0, 0)

	journalFast.Start(main.agent, nil, main.agent.LoadMetaMetricJournal)

	var ac *data_model.AutoCreate
	if argv.AutoCreate {
		ac = data_model.NewAutoCreate(metricStorage, main.agent.AutoCreateMetric)
		defer ac.Shutdown()
	}

	main.worker = startWorker(main.agent,
		metricStorage,
		main.agent.LoadOrCreateMapping,
		main.diskCache,
		ac,
		argv.Cluster,
		main.logPackets,
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
	tagsCacheEmpty := main.worker.mapper.TagValueDiskCacheEmpty()
	if !tagsCacheEmpty {
		logOk.Printf("Tag Value cache not empty")
	} else {
		logOk.Printf("Tag Value cache empty, loading boostrap...")
		mappings, ttl, err := main.agent.GetTagMappingBootstrap(context.Background())
		if err != nil {
			logErr.Printf("failed to load boostrap mappings: %v", err)
		} else {
			now := time.Now()
			for _, ma := range mappings {
				if err := main.worker.mapper.SetBootstrapValue(now, ma.Str, pcache.Int32ToValue(ma.Value), ttl); err != nil {
					logErr.Printf("failed to set boostrap mapping %q <-> %d: %v", ma.Str, ma.Value, err)
				}
			}
			logOk.Printf("Loaded and set %d boostrap mappings", len(mappings))
		}
	}

	for i, u := range main.receiversUDP {
		main.receiversWG.Add(1)
		go main.serve(u, i)
	}
	// UDP receivers are receiving data, so we consider agent started (not losing UDP packets already)
	agent.ShutdownInfoReport(main.agent, format.TagValueIDComponentAgent, argv.cacheDir, startDiscCacheTime)

	// Open TCP ports
	listeners := make([]net.Listener, 0, 2)
	listeners = append(listeners, listen("tcp4", argv.listenAddr))
	if argv.listenAddrIPv6 != "" {
		listeners = append(listeners, listen("tcp6", argv.listenAddrIPv6))
	}

	// Run pprof server
	main.hijackTCP = rpc.NewHijackListener(listeners[0].Addr())
	main.hijackHTTP = rpc.NewHijackListener(listeners[0].Addr())
	defer func() { _ = main.hijackTCP.Close() }()
	defer func() { _ = main.hijackHTTP.Close() }()

	main.receiverTCP = receiver.NewTCPReceiver(main.agent, main.logPackets)
	go main.serveTCP()

	main.receiverHTTP = receiver.NewHTTPReceiver(main.agent, main.logPackets)
	go main.serveHTTP()

	// Run RPC server
	receiverRPC := receiver.MakeRPCReceiver(main.agent, main.worker)
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
		rpc.ServerWithStatsHandler(statsHandler{receiversUDP: main.receiversUDP, receiverRPC: receiverRPC, sh2: main.agent, journal: journalFast}.handleStats),
		metrics.ServerWithMetrics,
	}
	options = append(options, rpc.ServerWithSocketHijackHandler(main.hijackConnection))
	srv := rpc.NewServer(options...)
	defer metrics.Run(srv)()
	for _, ln := range listeners {
		go serveRPC(ln, srv)
	}

	// Run scrape
	receiver.RunScrape(main.agent, main.worker)
	if !argv.hardwareMetricScrapeDisable {
		main.hardwareMetrics, err = stats.NewCollectorManager(stats.CollectorManagerOptions{ScrapeInterval: argv.hardwareMetricScrapeInterval, HostName: argv.customHostName}, main.worker, envLoader, logErr)
		if err != nil {
			logErr.Println("failed to init hardware collector", err.Error())
		} else {
			go main.startHardwareMetricsCollector()
			defer main.hardwareMetrics.StopCollector()
		}
	}
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
	main.agent.DisableNewSends()
	logOk.Printf("2. Waiting recent senders to finish sending data...")
	main.agent.WaitRecentSenders(time.Second * data_model.InsertDelay)
	shutdownInfo.StopRecentSenders = agent.ShutdownInfoDuration(&now).Nanoseconds()
	logOk.Printf("3. Closing UDP/unixdgram/RPC server and flusher...")
	for _, u := range main.receiversUDP {
		_ = u.Close()
	}
	main.agent.ShutdownFlusher()
	srv.Shutdown()
	main.receiversWG.Wait()
	shutdownInfo.StopReceivers = agent.ShutdownInfoDuration(&now).Nanoseconds()
	logOk.Printf("4. All UDP readers finished...")
	/// TODO - we receive almost no metrics via RPC, we do not want risk waiting here for the long time
	/// _ = srv.Close()
	/// logOk.Printf("5. RPC server stopped...")
	main.agent.WaitFlusher()
	shutdownInfo.StopFlusher = agent.ShutdownInfoDuration(&now).Nanoseconds()
	logOk.Printf("6. Flusher stopped, flushing remainig data to preprocessors...")
	nonEmpty := main.agent.FlushAllData()
	shutdownInfo.StopFlushing = agent.ShutdownInfoDuration(&now).Nanoseconds()
	logOk.Printf("7. Waiting preprocessor to save %d buckets of historic data...", nonEmpty)
	main.agent.WaitPreprocessor()
	shutdownInfo.StopPreprocessor = agent.ShutdownInfoDuration(&now).Nanoseconds()
	logOk.Printf("8. Saving mappings...")
	_ = mappingsCache.Save()
	shutdownInfo.SaveMappings = agent.ShutdownInfoDuration(&now).Nanoseconds()
	logOk.Printf("9. Saving journal...")
	_ = journalFast.Save()
	shutdownInfo.SaveJournal = agent.ShutdownInfoDuration(&now).Nanoseconds()
	shutdownInfo.FinishShutdownTime = now.UnixNano()
	agent.ShutdownInfoSave(argv.cacheDir, shutdownInfo)
	logOk.Printf("Bye")

	return 0
}

func (main *mainAgent) listenUDP(network string, addr string) error {
	if argv.coresUDP == 0 || addr == "" {
		return nil
	}
	reusePort := argv.coresUDP > 1 && network != "unixgram"
	u, err := receiver.ListenUDP(network, addr, argv.bufferSizeUDP, reusePort, main.agent, main.mirrorUdpConn, main.logPackets)
	if err != nil {
		logErr.Printf("listen %q failed: %v", network, err)
		return err
	}
	main.receiversUDP = append(main.receiversUDP, u)
	for i := 1; i < argv.coresUDP; i++ {
		var dup *receiver.UDP
		if network == "unixgram" {
			dup, err = u.Duplicate()
		} else {
			dup, err = receiver.ListenUDP(network, addr, argv.bufferSizeUDP, true, main.agent, main.mirrorUdpConn, main.logPackets)
		}
		if err != nil {
			logErr.Printf("duplicate listen socket failed: %v", err)
			return err
		}
		main.receiversUDP = append(main.receiversUDP, dup)
	}
	logOk.Printf("listen %q addr %q by %d cores", network, addr, argv.coresUDP)
	return nil
}

func (main *mainAgent) serve(u *receiver.UDP, num int) {
	defer main.receiversWG.Done()
	err := u.Serve(main.worker)
	if err != nil {
		logErr.Fatalf("Serve: %v", err)
	}
	log.Printf("UDP listener %d finished", num)
}

func (main *mainAgent) serveHTTP() {
	if err := main.receiverHTTP.Serve(main.worker, main.hijackHTTP); err != nil {
		logErr.Printf("error serving HTTP: %v", err)
	}
}

func (main *mainAgent) serveTCP() {
	if err := main.receiverTCP.Serve(main.worker, main.hijackTCP); err != nil {
		logErr.Printf("error serving TCP: %v", err)
	}
}

func (main *mainAgent) hijackConnection(conn *rpc.HijackConnection) {
	if strings.HasPrefix(string(conn.Magic), receiver.TCPPrefix) {
		conn.Magic = conn.Magic[len(receiver.TCPPrefix):]
		main.hijackTCP.AddConnection(conn)
		return
	}
	main.hijackHTTP.AddConnection(conn)
}

func (main *mainAgent) beforeFlushBucket(a *agent.Agent, unixNow uint32) {
	for _, r := range main.receiversUDP {
		v := float64(r.ReceiveBufferSize())
		a.AddValueCounter(unixNow, format.BuiltinMetricMetaAgentUDPReceiveBufferSize,
			[]int32{}, v, 1)
	}
	if main.diskCache != nil {
		s, err := main.diskCache.DiskSizeBytes()
		if err == nil {
			a.AddValueCounter(unixNow, format.BuiltinMetricMetaAgentDiskCacheSize,
				[]int32{0, 0, 0, 0, a.ComponentTag()}, float64(s), 1)
		}
	}
}

func (main *mainAgent) startHardwareMetricsCollector() {
	err := main.hardwareMetrics.RunCollector()
	if err != nil {
		logErr.Println("failed to run hardware collector", err.Error())
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

func (main *mainAgent) runPprof() {
	if err := http.ListenAndServe(argv.pprofListenAddr, nil); err != nil {
		logErr.Printf("failed to listen pprof on %q: %v", argv.pprofListenAddr, err)
	}
}

func readAESPwd() string {
	var aesPwd []byte
	var err error
	if argv.aesPwdFile == "" {
		aesPwd, _ = os.ReadFile(defaultPathToPwd)
	} else {
		aesPwd, err = os.ReadFile(argv.aesPwdFile)
		if err != nil {
			log.Fatalf("Could not read AES password file %s: %s", argv.aesPwdFile, err)
		}
	}
	return string(aesPwd)
}

func argvCreateClient() (*rpc.Client, string) {
	cryptoKey := readAESPwd()
	return rpc.NewClient(
		rpc.ClientWithLogf(logErr.Printf), rpc.ClientWithCryptoKey(cryptoKey), rpc.ClientWithTrustedSubnetGroups(build.TrustedSubnetGroups())), cryptoKey
}

func parseCommandLine() (entrypoint func() int, _ error) {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Daemons usage:\n")
		fmt.Fprintf(os.Stderr, "statshouse agent <options>             daemon receiving data from clients and sending to aggregators\n")
		fmt.Fprintf(os.Stderr, "Tools usage:\n")
		fmt.Fprintf(os.Stderr, "statshouse tlclient <options>          use as TL client to send JSON metrics to another statshouse\n")
		fmt.Fprintf(os.Stderr, "statshouse test_map <options>          test key mapping pipeline\n")
		fmt.Fprintf(os.Stderr, "statshouse test_parser <options>       parse and print packets received by UDP\n")
		fmt.Fprintf(os.Stderr, "statshouse test_longpoll <options>     test longpoll journal\n")
		fmt.Fprintf(os.Stderr, "statshouse simple_fsync <options>      simple SSD benchmark\n")
		fmt.Fprintf(os.Stderr, "statshouse tlclient.api <options>      test API\n")
		fmt.Fprintf(os.Stderr, "statshouse benchmark <options>         some benchmarks\n")
		return nil, nil
	}

	var verb string
	const conveyorArgPrefix = "-new-conveyor="
	for i := 0; i < len(os.Args) && verb == ""; i++ {
		if !strings.HasPrefix(os.Args[i], conveyorArgPrefix) {
			continue
		}
		s := os.Args[i][len(conveyorArgPrefix):]
		switch s {
		case "agent", "duplicate_map":
			verb = "agent"
			log.Printf("-new-conveyor argument is deprecated, instead of 'statshouse ... %s ...' run 'statshouse agent ...' or 'statshouse -agent ...'", os.Args[i])
			os.Args = append(os.Args[:i], os.Args[i+1:]...)
		default:
			return nil, fmt.Errorf("wrong value for -new-conveyor argument %s, must be 'agent', 'duplicate_map' (also means agent)", s)
		}
	}
	if verb == "" {
		verb = strings.TrimPrefix(strings.TrimPrefix(os.Args[1], "-"), "-")
		os.Args = append(os.Args[:1], os.Args[2:]...)
	}

	switch verb {
	case "agent":
		flag.StringVar(&argv.aesPwdFile, "aes-pwd-file", "", "path to AES password file, will try to read "+defaultPathToPwd+" if not set")
		flag.StringVar(&argv.logFile, "l", "/dev/stdout", "log file")
		flag.StringVar(&argv.logLevel, "log-level", "info", "log level. can be 'info' or 'trace' for now. 'trace' will print all incoming packets")
		flag.StringVar(&argv.userLogin, "u", "kitten", "sets user name to make setuid")
		flag.StringVar(&argv.userGroup, "g", "kitten", "sets user group to make setguid")
		flag.StringVar(&argv.pprofListenAddr, "pprof", "", "HTTP pprof listen address")
		flag.BoolVar(&argv.pprofHTTP, "pprof-http", false, "Serve Go pprof HTTP on RPC port (deprecated due to security reasons)")
		flag.StringVar(&argv.cacheDir, "cache-dir", "", "Data that cannot be immediately sent will be stored here together with metric metadata cache.")
		flag.Uint64Var(&argv.maxOpenFiles, "max-open-files", 131072, "open files limit")
		flag.StringVar(&argv.aggAddr, "agg-addr", "", "Comma-separated list of 3 aggregator addresses (shard 1 is recommended). For aggregator, listen addr.")
		flag.StringVar(&argv.Cluster, "cluster", "statlogs2", "clickhouse cluster name to autodetect configuration, local shard and replica")
		flag.StringVar(&argv.customHostName, "hostname", "", "override auto detected hostname")
		flag.StringVar(&argv.listenAddr, "p", ":13337", "RAW UDP & RPC TCP listen address")
		flag.StringVar(&argv.listenAddrIPv6, "listen-addr-ipv6", "", "RAW UDP & RPC TCP listen address (IPv6)")
		flag.StringVar(&argv.listenAddrUnix, "listen-addr-unix", "", "Unix datagram listen address.")
		flag.StringVar(&argv.mirrorUdpAddr, "mirror-udp", "", "mirrors UDP datagrams to the given address")
		flag.IntVar(&argv.coresUDP, "cores-udp", 1, "CPU cores to use for udp receiving. 0 switches UDP off")
		flag.IntVar(&argv.bufferSizeUDP, "buffer-size-udp", receiver.DefaultConnBufSize, "UDP receiving buffer size")
		flag.IntVar(&argv.maxCores, "cores", -1, "CPU cores usage limit. 0 all available, <0 use (cores-udp*3/2 + 1)")
		flag.BoolVar(&argv.promRemoteMod, "prometheus-push-remote", false, "use remote pusher for prom metrics")
		flag.DurationVar(&argv.hardwareMetricScrapeInterval, "hardware-metric-scrape-interval", time.Second, "how often hardware metrics will be scraped")
		flag.BoolVar(&argv.hardwareMetricScrapeDisable, "hardware-metric-scrape-disable", false, "disable hardware metric scraping")
		flag.StringVar(&argv.envFilePath, "env-file-path", "/etc/statshouse_env.yml", "statshouse environment file path")
		argv.Config.Bind(flag.CommandLine, agent.DefaultConfig())
		// DEPRECATED but still in use
		var sampleFactor int
		var maxMemLimit uint64
		flag.IntVar(&sampleFactor, "sample-factor", 1, "Deprecated - If 2, 50% of stats will be throw away, if 10, 90% of stats will be thrown away. If <= 1, keep all stats.")
		flag.Uint64Var(&maxMemLimit, "m", 0, "Deprecated - max memory usage limit")
		flag.StringVar(&argv.historicStorageDir, "historic-storage", "", "Data that cannot be immediately sent will be stored here together with metric cache.")
		flag.StringVar(&argv.diskCacheFilename, "disk-cache-filename", "", "disk cache file name")

		build.FlagParseShowVersionHelp()
		parseListenAddress()

		// TODO: legacy mode options, to be removed
		if argv.cacheDir == "" && argv.historicStorageDir != "" {
			argv.cacheDir = argv.historicStorageDir
		}
		if argv.cacheDir == "" && argv.diskCacheFilename != "" {
			argv.cacheDir = filepath.Dir(argv.diskCacheFilename)
		}

		argv.AggregatorAddresses = strings.Split(argv.aggAddr, ",")
		if len(argv.AggregatorAddresses) != 3 {
			return nil, fmt.Errorf("-agg-addr must contain comma-separated list of 3 aggregators (1 shard is recommended)")
		}
		if argv.coresUDP < 0 {
			return nil, fmt.Errorf("--cores-udp must be set to at least 0")
		}
		if argv.maxCores < 0 {
			argv.maxCores = 1 + argv.coresUDP*3/2
		}
		if argv.customHostName == "" {
			argv.customHostName = srvfunc.HostnameForStatshouse()
			logOk.Printf("detected statshouse hostname as %q from OS hostname %q\n", argv.customHostName, srvfunc.Hostname())
		}
		if argv.pprofHTTP {
			logErr.Printf("warning: --pprof-http option deprecated due to security reasons. Please use explicit --pprof=127.0.0.1:11123 option")
		}

		return run, argv.Config.ValidateConfigSource()
	case "test_parser":
		flag.IntVar(&argv.bufferSizeUDP, "buffer-size-udp", receiver.DefaultConnBufSize, "UDP receiving buffer size")
		flag.StringVar(&argv.listenAddr, "p", ":13337", "RAW UDP & RPC TCP listen address")
		build.FlagParseShowVersionHelp()
		parseListenAddress()
		return mainTestParser, nil
	case "test_map":
		flag.StringVar(&argv.aesPwdFile, "aes-pwd-file", "", "path to AES password file, will try to read "+defaultPathToPwd+" if not set")
		flag.StringVar(&argv.aggAddr, "agg-addr", "", "comma-separated list of aggregator addresses to test.")
		flag.StringVar(&argv.mapString, "string", "production", "string to map.")
		build.FlagParseShowVersionHelp()
		argv.AggregatorAddresses = strings.Split(argv.aggAddr, ",")
		if len(argv.AggregatorAddresses) == 0 {
			return nil, fmt.Errorf("--agg-addr must not be empty")
		}
		return mainTestMap, nil
	case "test_longpoll":
		flag.StringVar(&argv.aesPwdFile, "aes-pwd-file", "", "path to AES password file, will try to read "+defaultPathToPwd+" if not set")
		flag.StringVar(&argv.aggAddr, "agg-addr", "", "comma-separated list of aggregator addresses to test.")
		build.FlagParseShowVersionHelp()
		argv.AggregatorAddresses = strings.Split(argv.aggAddr, ",")
		if len(argv.AggregatorAddresses) == 0 {
			return nil, fmt.Errorf("--agg-addr must not be empty")
		}
		return mainTestLongpoll, nil
	case "modules":
		return mainModules, nil
	case "tlclient":
		flag.StringVar(&argv.aesPwdFile, "aes-pwd-file", "", "path to AES password file, will try to read "+defaultPathToPwd+" if not set")
		flag.StringVar(&argv.statshouseAddr, "statshouse-addr", "127.0.0.1:13337", "statshouse address for tlclient")
		flag.StringVar(&argv.statshouseNet, "statshouse-net", "tcp4", "statshouse network for tlclient")
		build.FlagParseShowVersionHelp()
		return mainTLClient, nil
	case "tlclient.api":
		flag.StringVar(&argv.aesPwdFile, "aes-pwd-file", "", "path to AES password file, will try to read "+defaultPathToPwd+" if not set")
		build.FlagParseShowVersionHelp()
		return mainTLClientAPI, nil
	case "tag_mapping":
		flag.Int64Var(&argv.metadataActorID, "metadata-actor-id", 0, "")
		flag.IntVar(&argv.budget, "budget", 0, "mapping budget to set")
		flag.StringVar(&argv.aesPwdFile, "aes-pwd-file", "", "path to AES password file, will try to read "+defaultPathToPwd+" if not set")
		flag.StringVar(&argv.metadataAddr, "metadata-addr", "127.0.0.1:2442", "")
		flag.StringVar(&argv.metadataNet, "metadata-net", "tcp4", "")
		flag.StringVar(&argv.metric, "metric", "", "metric name, if specified then strings are considered metric tags")
		flag.StringVar(&argv.tags, "tag", "", "string to be searched for a int32 mapping")
		build.FlagParseShowVersionHelp()
		return mainTagMapping, nil
	case "publish_tag_drafts":
		flag.BoolVar(&argv.dryRun, "dry-run", true, "do not publish changes")
		flag.Int64Var(&argv.metadataActorID, "metadata-actor-id", 0, "")
		flag.StringVar(&argv.aesPwdFile, "aes-pwd-file", "", "path to AES password file, will try to read "+defaultPathToPwd+" if not set")
		flag.StringVar(&argv.metadataAddr, "metadata-addr", "127.0.0.1:2442", "")
		flag.StringVar(&argv.metadataNet, "metadata-net", "tcp4", "")
		build.FlagParseShowVersionHelp()
		return mainPublishTagDrafts, nil
	case "mass_update_metadata":
		flag.BoolVar(&argv.dryRun, "dry-run", true, "do not publish changes")
		flag.IntVar(&argv.maxUpdates, "max-updates", 0, "make no more than this # of modifications")
		flag.Int64Var(&argv.metadataActorID, "metadata-actor-id", 0, "")
		flag.StringVar(&argv.aesPwdFile, "aes-pwd-file", "", "path to AES password file, will try to read "+defaultPathToPwd+" if not set")
		flag.StringVar(&argv.metadataAddr, "metadata-addr", "127.0.0.1:2442", "")
		flag.StringVar(&argv.metadataNet, "metadata-net", "tcp4", "")
		build.FlagParseShowVersionHelp()
		return massUpdateMetadata, nil
	case "simple_fsync":
		return mainSimpleFSyncTest, nil
	case "benchmark":
		flag.StringVar(&argv.listenAddr, "p", "127.0.0.1:13337", "RAW UDP & RPC TCP write/listen port")
		build.FlagParseShowVersionHelp()
		return mainBenchmarks, nil
	default:
		return nil, fmt.Errorf("unknown verb %q", verb)
	}
}

func parseListenAddress() {
	if _, err := strconv.Atoi(argv.listenAddr); err == nil { // old convention of using port
		argv.listenAddr = ":" + argv.listenAddr // convert to addr
	}
}
