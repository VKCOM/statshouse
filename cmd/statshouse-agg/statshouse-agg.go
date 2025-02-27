// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	_ "github.com/prometheus/prometheus/discovery/consul" // spawns service discovery goroutines
	"github.com/vkcom/statshouse/internal/agent"
	"github.com/vkcom/statshouse/internal/aggregator"
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/pcache"
	"github.com/vkcom/statshouse/internal/vkgo/build"
	"github.com/vkcom/statshouse/internal/vkgo/platform"
	"github.com/vkcom/statshouse/internal/vkgo/srvfunc"
)

const defaultPathToPwd = `/etc/engine/pass`

var argv struct {
	logFile         string
	logLevel        string
	userLogin       string // логин для setuid
	userGroup       string // логин для setguid
	maxOpenFiles    uint64
	aesPwdFile      string
	cacheDir        string // different default
	customHostName  string // useful for testing and in some environments
	aggAddr         string
	configAgent     agent.Config
	listenAddr      string
	pprofListenAddr string

	aggregator.ConfigAggregator
}

var logFile *os.File

func logRotate() {
	var err error
	logFile, err = srvfunc.LogRotate(logFile, argv.logFile)
	if err != nil {
		log.Printf("logrotate %s error: %v", argv.logFile, err)
	}
}

func main() {
	os.Exit(mainAggregator())
}

func mainAggregator() int {
	if err := parseCommandLine(); err != nil {
		log.Println(err)
		return 1
	}
	logRotate()

	// Read AES password
	var aesPwd string
	if argv.aesPwdFile == "" {
		// ignore error if file path wasn't explicitly specified
		if v, err := os.ReadFile(defaultPathToPwd); err != nil {
			log.Printf("Could not read AES password file %s: %s (ignored because no file was specified)", defaultPathToPwd, err)
		} else {
			aesPwd = string(v)
		}
	} else if v, err := os.ReadFile(argv.aesPwdFile); err != nil {
		// fatal if could not read file at path specified explicitly
		log.Printf("Could not read AES password file %s: %s", argv.aesPwdFile, err)
		return 1
	} else {
		aesPwd = string(v)
	}
	if _, err := srvfunc.SetHardRLimitNoFile(argv.maxOpenFiles); err != nil {
		log.Printf("Could not set new rlimit: %v", err)
	}
	// we need elevated rights to SetHardRLimitNoFile and read AES password
	if err := platform.ChangeUserGroup(argv.userLogin, argv.userGroup); err != nil {
		log.Printf("Could not change user/group to %q/%q: %v", argv.userLogin, argv.userGroup, err)
		return 1
	}

	if argv.pprofListenAddr != "" {
		go runPprof()
	}

	if argv.cacheDir == "" {
		log.Printf("aggregator cannot run without -cache-dir for now")
		return 1
	}
	_ = os.Mkdir(argv.cacheDir, os.ModePerm) // create dir, but not parent dirs
	dc, err := pcache.OpenDiskCache(filepath.Join(argv.cacheDir, "mapping_cache.sqlite3"), pcache.DefaultTxDuration)
	if err != nil {
		log.Printf("failed to open disk cache: %v", err)
		return 1
	}
	// we do not want to confuse mappings from different clusters, this would be a disaster
	fpmc, err := os.OpenFile(filepath.Join(argv.cacheDir, fmt.Sprintf("mappings-%s.cache", argv.Cluster)), os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		log.Printf("failed to open aggregator mappings cache: %v", err)
		return 1
	}
	defer fpmc.Close()
	// we do not want to confuse journal from different clusters, this would be a disaster
	fj, err := os.OpenFile(filepath.Join(argv.cacheDir, fmt.Sprintf("journal-%s.cache", argv.Cluster)), os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		log.Printf("failed to open journal cache: %v", err)
		return 1
	}
	defer fj.Close()
	fjCompact, err := os.OpenFile(filepath.Join(argv.cacheDir, fmt.Sprintf("journal-compact-%s.cache", argv.Cluster)), os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		log.Printf("failed to open journal cache: %v", err)
		return 1
	}
	defer fjCompact.Close()

	mappingsCache, _ := pcache.LoadMappingsCacheFile(fpmc, argv.MappingCacheSize, argv.MappingCacheTTL) // we ignore error because cache can be damaged
	startDiscCacheTime := time.Now()                                                                    // we only have disk cache before. Be carefull when redesigning
	agg, err := aggregator.MakeAggregator(dc, fj, fjCompact, mappingsCache, argv.cacheDir, argv.aggAddr, aesPwd, argv.ConfigAggregator, argv.customHostName, argv.logLevel == "trace")
	if err != nil {
		log.Println(err)
		return 1
	}

	// Run
	agent.ShutdownInfoReport(agg.Agent(), format.TagValueIDComponentAggregator, argv.cacheDir, startDiscCacheTime)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGUSR1, syscall.SIGINT)
main_loop:
	for sig := range signals {
		switch sig {
		case syscall.SIGUSR1:
			log.Println("logRotate", argv.logFile)
			logRotate()
		case syscall.SIGINT:
			break main_loop
		}
	}

	// Shutdown
	shutdownInfo := tlstatshouse.ShutdownInfo{}
	now := time.Now()
	shutdownInfo.StartShutdownTime = now.UnixNano()
	log.Printf("Shutting down...")
	log.Printf("1. Disabling inserting new data to clickhouses...")
	agg.DisableNewInsert()
	log.Printf("2. Waiting all inserts to finish...")
	agg.WaitInsertsFinish(data_model.ClickHouseTimeoutShutdown)
	shutdownInfo.StopInserters = agent.ShutdownInfoDuration(&now).Nanoseconds()
	// Now when inserts are finished and responses are in send queues of RPC connections,
	// we can initiate shutdown by sending LetsFIN packets and waiting to actual FINs.
	log.Printf("3. Starting gracefull RPC shutdown...")
	agg.ShutdownRPCServer()
	log.Printf("4. Waiting RPC clients to receive responses and disconnect...")
	agg.WaitRPCServer(10 * time.Second)
	shutdownInfo.StopRPCServer = agent.ShutdownInfoDuration(&now).Nanoseconds()
	log.Printf("5. Saving mappings...")
	_ = mappingsCache.Save()
	shutdownInfo.SaveMappings = agent.ShutdownInfoDuration(&now).Nanoseconds()
	log.Printf("6. Saving journals...")
	agg.SaveJournals()
	shutdownInfo.SaveJournal = agent.ShutdownInfoDuration(&now).Nanoseconds()
	shutdownInfo.FinishShutdownTime = now.UnixNano()
	agent.ShutdownInfoSave(argv.cacheDir, shutdownInfo)
	log.Printf("Bye")
	return 0
}

func parseCommandLine() error {
	if len(os.Args) > 1 {
		const conveyorName = "aggregator"
		switch os.Args[1] {
		case conveyorName, "-" + conveyorName, "--" + conveyorName:
			log.Printf("positional argument %q is deprecated, it is safe to remove it", os.Args[1])
			os.Args = append(os.Args[:1], os.Args[2:]...)
		}
		const conveyorArgPrefix = "-new-conveyor="
		for i, v := range os.Args {
			if strings.HasPrefix(v, conveyorArgPrefix) {
				if s := v[len(conveyorArgPrefix):]; s == conveyorName {
					log.Printf("option %q is deprecated, it is safe to remove it", v)
					os.Args = append(os.Args[:i], os.Args[i+1:]...)
					break
				} else {
					return fmt.Errorf("wrong value for -new-conveyor option %s, must be %q", s, conveyorName)
				}
			}
		}
	}

	flag.StringVar(&argv.aesPwdFile, "aes-pwd-file", "", "path to AES password file, will try to read "+defaultPathToPwd+" if not set")
	flag.StringVar(&argv.logFile, "l", "/dev/stdout", "log file")
	flag.StringVar(&argv.logLevel, "log-level", "info", "log level. can be 'info' or 'trace' for now. 'trace' will print all incoming packets")
	flag.StringVar(&argv.userLogin, "u", "kitten", "sets user name to make setuid")
	flag.StringVar(&argv.userGroup, "g", "kitten", "sets user group to make setguid")
	flag.StringVar(&argv.cacheDir, "cache-dir", "", "Data that cannot be immediately sent will be stored here together with metric metadata cache.")
	flag.Uint64Var(&argv.maxOpenFiles, "max-open-files", 131072, "open files limit")
	flag.StringVar(&argv.aggAddr, "agg-addr", "", "Comma separated list of aggregator listen addresses")
	flag.StringVar(&argv.Cluster, "cluster", aggregator.DefaultConfigAggregator().Cluster, "clickhouse cluster name to autodetect configuration, local shard and replica")
	flag.StringVar(&argv.customHostName, "hostname", "", "override auto detected hostname")
	flag.IntVar(&argv.ShortWindow, "short-window", aggregator.DefaultConfigAggregator().ShortWindow, "Short admission window. Shorter window reduces latency, but also reduces recent stats quality as more agents come too late")
	flag.IntVar(&argv.RecentInserters, "recent-inserters", aggregator.DefaultConfigAggregator().RecentInserters, "How many parallel inserts to make for recent data")
	flag.IntVar(&argv.HistoricInserters, "historic-inserters", aggregator.DefaultConfigAggregator().HistoricInserters, "How many parallel inserts to make for historic data")
	flag.IntVar(&argv.InsertHistoricWhen, "insert-historic-when", aggregator.DefaultConfigAggregator().InsertHistoricWhen, "Aggregator will insert historic data when # of ongoing recent data inserts is this number or less")
	flag.IntVar(&argv.CardinalityWindow, "cardinality-window", aggregator.DefaultConfigAggregator().CardinalityWindow, "Aggregator will use this window (seconds) to estimate cardinality")
	flag.IntVar(&argv.MaxCardinality, "max-cardinality", aggregator.DefaultConfigAggregator().MaxCardinality, "Aggregator will sample metrics which cardinality estimates are higher")
	argv.Bind(flag.CommandLine, aggregator.DefaultConfigAggregator().ConfigAggregatorRemote, false)
	flag.Float64Var(&argv.SimulateRandomErrors, "simulate-errors-random", aggregator.DefaultConfigAggregator().SimulateRandomErrors, "Probability of errors for recent buckets from 0.0 (no errors) to 1.0 (all errors)")
	flag.BoolVar(&argv.AutoCreate, "auto-create", aggregator.DefaultConfigAggregator().AutoCreate, "Enable metric auto-create.")
	flag.BoolVar(&argv.AutoCreateDefaultNamespace, "auto-create-default-namespace", false, "Auto-create metrics with no namespace specified.")
	flag.BoolVar(&argv.DisableRemoteConfig, "disable-remote-config", aggregator.DefaultConfigAggregator().DisableRemoteConfig, "disable remote configuration")
	flag.StringVar(&argv.ExternalPort, "agg-external-port", aggregator.DefaultConfigAggregator().ExternalPort, "external port for aggregator autoconfiguration if different from port set in agg-addr")
	flag.IntVar(&argv.PreviousNumShards, "previous-shards", aggregator.DefaultConfigAggregator().PreviousNumShards, "Previous number of shard*replicas in cluster. During transition, clients with previous configuration are also allowed to send data.")
	flag.IntVar(&argv.ShardByMetricShards, "shard-by-metric-shards", aggregator.DefaultConfigAggregator().ShardByMetricShards, "When increasing cluster size, we want to pin metrics without explicit shards to their former shards.")
	flag.IntVar(&argv.LocalReplica, "local-replica", aggregator.DefaultConfigAggregator().LocalReplica, "Replica number for local test cluster [1..3]")
	flag.Int64Var(&argv.MetadataActorID, "metadata-actor-id", aggregator.DefaultConfigAggregator().MetadataActorID, "")
	flag.StringVar(&argv.MetadataAddr, "metadata-addr", aggregator.DefaultConfigAggregator().MetadataAddr, "")
	flag.StringVar(&argv.MetadataNet, "metadata-net", aggregator.DefaultConfigAggregator().MetadataNet, "")
	flag.StringVar(&argv.KHAddr, "kh", "127.0.0.1:13338,127.0.0.1:13339", "clickhouse HTTP address:port")
	flag.StringVar(&argv.KHUser, "kh-user", "", "clickhouse user")
	flag.StringVar(&argv.KHPasswordFile, "kh-password-file", "", "file with clickhouse password")
	flag.StringVar(&argv.pprofListenAddr, "pprof", "", "HTTP pprof listen address")
	build.FlagParseShowVersionHelp()

	if len(argv.aggAddr) == 0 {
		return fmt.Errorf("--agg-addr to listen must be specified")
	}
	if _, err := strconv.Atoi(argv.listenAddr); err == nil { // old convention of using port
		argv.listenAddr = ":" + argv.listenAddr // convert to addr
	}
	if argv.customHostName == "" {
		argv.customHostName = srvfunc.HostnameForStatshouse()
		log.Printf("detected statshouse hostname as %q from OS hostname %q\n", argv.customHostName, srvfunc.Hostname())
	}
	if argv.KHPasswordFile != "" {
		if p, err := os.ReadFile(argv.KHPasswordFile); err == nil {
			p = bytes.TrimSpace(p)
			argv.KHPassword = string(p)
			log.Printf("got dpassword '%s' from file %s", argv.KHPassword, argv.KHPasswordFile)
		} else {
			return fmt.Errorf("failed to open --kh-password-file %q: %v", argv.KHPasswordFile, err)
		}
	}

	return aggregator.ValidateConfigAggregator(argv.ConfigAggregator)
}

func runPprof() {
	if err := http.ListenAndServe(argv.pprofListenAddr, nil); err != nil {
		log.Printf("failed to listen pprof on %q: %v", argv.pprofListenAddr, err)
	}
}
