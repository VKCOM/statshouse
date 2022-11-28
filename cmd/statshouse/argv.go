// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/vkcom/statshouse/internal/vkgo/rpc"

	"github.com/vkcom/statshouse/internal/agent"
	"github.com/vkcom/statshouse/internal/aggregator"
	"github.com/vkcom/statshouse/internal/receiver"
)

const (
	defaultPathToPwd = `/etc/engine/pass`
	defaultUser      = `kitten`
	defaultGroup     = `kitten`
)

var (
	argv struct {
		// common
		logFile         string
		logLevel        string
		userLogin       string // логин для setuid
		userGroup       string // логин для setguid
		maxOpenFiles    uint64
		pprofListenAddr string
		aesPwdFile      string
		cacheDir        string // different default
		customHostName  string // useful for testing and in some environments

		aggAddr string // common, different meaning

		cluster string // common for agent and ingress proxy

		configAgent   agent.Config
		maxCores      int
		listenAddr    string
		coresUDP      int
		bufferSizeUDP int
		promRemoteMod bool

		configAggregator aggregator.ConfigAggregator

		configIngress  aggregator.ConfigIngressProxy
		ingressExtAddr string
		ingressPwdDir  string

		// for old mode
		historicStorageDir string
		diskCacheFilename  string
	}
)

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

func argvCreateClient() *rpc.Client {
	return &rpc.Client{
		Logf:                logErr.Printf,
		CryptoKey:           readAESPwd(),
		TrustedSubnetGroups: nil,
	}
}

func argvAddDeprecatedFlags() {
	// Deprecated args still used by statshouses in prod
	var (
		sampleFactor int
		maxMemLimit  uint64
	)
	flag.IntVar(&sampleFactor, "sample-factor", 1, "Deprecated - If 2, 50% of stats will be throw away, if 10, 90% of stats will be thrown away. If <= 1, keep all stats.")
	flag.Uint64Var(&maxMemLimit, "m", 0, "Deprecated - max memory usage limit")
	flag.StringVar(&argv.historicStorageDir, "historic-storage", "", "Data that cannot be immediately sent will be stored here together with metric cache.")
	flag.StringVar(&argv.diskCacheFilename, "disk-cache-filename", "", "disk cache file name")
}

func argvAddCommonFlags() {
	// common flags
	flag.StringVar(&argv.aesPwdFile, "aes-pwd-file", "", "path to AES password file, will try to read "+defaultPathToPwd+" if not set")

	flag.StringVar(&argv.logFile, "l", "/dev/stdout", "log file")
	flag.StringVar(&argv.logLevel, "log-level", "info", "log level. can be 'info' or 'trace' for now. 'trace' will print all incoming packets")

	flag.StringVar(&argv.userLogin, "u", defaultUser, "sets user name to make setuid")
	flag.StringVar(&argv.userGroup, "g", defaultGroup, "sets user group to make setguid")

	flag.StringVar(&argv.pprofListenAddr, "pprof", "", "HTTP pprof listen address (like :13336)")

	flag.StringVar(&argv.cacheDir, "cache-dir", "", "Data that cannot be immediately sent will be stored here together with metric metadata cache.")

	flag.Uint64Var(&argv.maxOpenFiles, "max-open-files", 131072, "open files limit")

	flag.StringVar(&argv.aggAddr, "agg-addr", "", "Comma-separated list of 3 aggregator addresses (shard 1 is recommended). For aggregator, listen addr.")

	flag.StringVar(&argv.cluster, "cluster", aggregator.DefaultConfigAggregator().Cluster, "clickhouse cluster name to autodetect configuration, local shard and replica")

	flag.StringVar(&argv.customHostName, "hostname", "", "override auto detected hostname")
}

func argvAddAgentFlags(legacyVerb bool) {
	flag.IntVar(&argv.configAgent.SampleBudget, "sample-budget", agent.DefaultConfig().SampleBudget, "Statshouse will sample all buckets to contain max this number of bytes.")
	flag.BoolVar(&argv.configAgent.SampleGroups, "sample-groups", false, "Statshouse will first spread resources between groups, then inside each group.")
	flag.Int64Var(&argv.configAgent.MaxHistoricDiskSize, "max-disk-size", agent.DefaultConfig().MaxHistoricDiskSize, "Statshouse will use no more than this amount of disk space for storing historic data.")
	flag.IntVar(&argv.configAgent.SkipFirstNShards, "skip-shards", agent.DefaultConfig().SkipFirstNShards, "Skip first shard*replicas during sharding. When extending cluster, helps prevent filling disks of already full shards.")

	flag.IntVar(&argv.configAgent.StringTopCapacity, "string-top-capacity", agent.DefaultConfig().StringTopCapacity, "How many different strings per key is stored in string tops.")
	flag.IntVar(&argv.configAgent.StringTopCountSend, "string-top-send", agent.DefaultConfig().StringTopCountSend, "How many different strings per key is sent in string tops.")

	flag.IntVar(&argv.configAgent.LivenessResponsesWindowLength, "liveness-window", agent.DefaultConfig().LivenessResponsesWindowLength, "windows size (seconds) to use for liveness checks. Aggregator is live again if all keepalives in window are successes.")
	flag.IntVar(&argv.configAgent.LivenessResponsesWindowSuccesses, "liveness-success", agent.DefaultConfig().LivenessResponsesWindowSuccesses, "For liveness checks. Aggregator is dead if less responses in window are successes.")
	flag.DurationVar(&argv.configAgent.KeepAliveSuccessTimeout, "keep-alive-timeout", agent.DefaultConfig().KeepAliveSuccessTimeout, "For liveness checks. Successful keepalive must take less.")

	flag.BoolVar(&argv.configAgent.SaveSecondsImmediately, "save-seconds-immediately", agent.DefaultConfig().SaveSecondsImmediately, "Save data to disk as soon as second is ready. When false, data is saved after first unsuccessful send.")
	flag.StringVar(&argv.configAgent.StatsHouseEnv, "statshouse-env", agent.DefaultConfig().StatsHouseEnv, "Fill key0 with this value in built-in statistics. Only 'production' and 'staging' values are allowed.")

	flag.BoolVar(&argv.configAgent.RemoteWriteEnabled, "remote-write-enabled", agent.DefaultConfig().RemoteWriteEnabled, "Serve prometheus remote write endpoint.")
	flag.StringVar(&argv.configAgent.RemoteWriteAddr, "remote-write-addr", agent.DefaultConfig().RemoteWriteAddr, "Prometheus remote write listen address.")
	flag.StringVar(&argv.configAgent.RemoteWritePath, "remote-write-path", agent.DefaultConfig().RemoteWritePath, "Prometheus remote write path.")
	if !legacyVerb { // TODO - remove
		flag.BoolVar(&argv.configAgent.AutoCreate, "auto-create", agent.DefaultConfig().AutoCreate, "Enable metric auto-create.")
	}

	flag.StringVar(&argv.listenAddr, "p", ":13337", "RAW UDP & RPC TCP listen address")

	flag.IntVar(&argv.coresUDP, "cores-udp", 1, "CPU cores to use for udp receiving. 0 switches UDP off")
	flag.IntVar(&argv.bufferSizeUDP, "buffer-size-udp", receiver.DefaultConnBufSize, "UDP receiving buffer size")

	flag.IntVar(&argv.maxCores, "cores", -1, "CPU cores usage limit. 0 all available, <0 use (cores-udp*3/2 + 1)")

	flag.BoolVar(&argv.promRemoteMod, "prometheus-push-remote", false, "use remote pusher for prom metrics")
}

func argvAddAggregatorFlags(legacyVerb bool) {
	flag.IntVar(&argv.configAggregator.ShortWindow, "short-window", aggregator.DefaultConfigAggregator().ShortWindow, "Short admission window. Shorter window reduces latency, but also reduces recent stats quality as more agents come too late")
	flag.IntVar(&argv.configAggregator.RecentInserters, "recent-inserters", aggregator.DefaultConfigAggregator().RecentInserters, "How many parallel inserts to make for recent data")
	flag.IntVar(&argv.configAggregator.HistoricInserters, "historic-inserters", aggregator.DefaultConfigAggregator().HistoricInserters, "How many parallel inserts to make for historic data")
	flag.IntVar(&argv.configAggregator.InsertHistoricWhen, "insert-historic-when", aggregator.DefaultConfigAggregator().InsertHistoricWhen, "Aggregator will insert historic data when # of ongoing recent data inserts is this number or less")

	flag.IntVar(&argv.configAggregator.InsertBudget, "insert-budget", aggregator.DefaultConfigAggregator().InsertBudget, "Aggregator will sample data before inserting into clickhouse. Bytes per contributor when # >> 100.")
	flag.IntVar(&argv.configAggregator.InsertBudget100, "insert-budget-100", aggregator.DefaultConfigAggregator().InsertBudget100, "Aggregator will sample data before inserting into clickhouse. Bytes per contributor when # ~ 100.")
	flag.IntVar(&argv.configAggregator.CardinalityWindow, "cardinality-window", aggregator.DefaultConfigAggregator().CardinalityWindow, "Aggregator will use this window (seconds) to estimate cardinality")
	flag.IntVar(&argv.configAggregator.MaxCardinality, "max-cardinality", aggregator.DefaultConfigAggregator().MaxCardinality, "Aggregator will sample metrics which cardinality estimates are higher")

	flag.IntVar(&argv.configAggregator.StringTopCountInsert, "string-top-insert", aggregator.DefaultConfigAggregator().StringTopCountInsert, "How many different strings per key is inserted by aggregator in string tops.")

	flag.Float64Var(&argv.configAggregator.SimulateRandomErrors, "simulate-errors-random", aggregator.DefaultConfigAggregator().SimulateRandomErrors, "Probability of errors for recent buckets from 0.0 (no errors) to 1.0 (all errors)")

	if legacyVerb { // TODO - remove
		var unused string
		var unused1 uint64
		flag.StringVar(&unused, "rpc-proxy-net", "", "rpc-proxy listen network")
		flag.StringVar(&unused, "rpc-proxy-addr", "", "rpc-proxy listen address")
		flag.StringVar(&unused, "dolphin-net", "", "dolphin listen network")
		flag.StringVar(&unused, "dolphin-addr", "", "dolphin listen address")
		flag.StringVar(&unused, "dolphin-table", "", "dolphin table with meta metrics")
		flag.Uint64Var(&unused1, "pmc-mapping-actor-id", 0, "actor ID of PMC mapping cluster")
	} else {
		flag.BoolVar(&argv.configAggregator.AutoCreate, "auto-create", aggregator.DefaultConfigAggregator().AutoCreate, "Enable metric auto-create.")
	}

	flag.StringVar(&argv.configAggregator.ExternalPort, "agg-external-port", aggregator.DefaultConfigAggregator().ExternalPort, "external port for aggregator autoconfiguration if different from port set in agg-addr")
	flag.IntVar(&argv.configAggregator.PreviousNumShards, "previous-shards", aggregator.DefaultConfigAggregator().PreviousNumShards, "Previous number of shard*replicas in cluster. During transition, clients with previous configuration are also allowed to send data.")

	flag.Uint64Var(&argv.configAggregator.MetadataActorID, "metadata-actor-id", aggregator.DefaultConfigAggregator().MetadataActorID, "")
	flag.StringVar(&argv.configAggregator.MetadataAddr, "metadata-addr", aggregator.DefaultConfigAggregator().MetadataAddr, "")
	flag.StringVar(&argv.configAggregator.MetadataNet, "metadata-net", aggregator.DefaultConfigAggregator().MetadataNet, "")

	flag.StringVar(&argv.configAggregator.KHAddr, "kh", "127.0.0.1:13338,127.0.0.1:13339", "clickhouse HTTP address:port")
}

func argvAddIngressProxyFlags() {
	flag.StringVar(&argv.configIngress.ListenAddr, "ingress-addr", "", "Listen address of ingress proxy")
	flag.StringVar(&argv.ingressExtAddr, "ingress-external-addr", "", "Comma-separate list of 3 external addresses of ingress proxies.")
	flag.StringVar(&argv.ingressPwdDir, "ingress-pwd-dir", "", "path to AES passwords dir for clients of ingress proxy.")
}

func printVerbUsage() {
	_, _ = fmt.Fprintf(os.Stderr, "Daemons usage:\n")
	_, _ = fmt.Fprintf(os.Stderr, "statshouse agent <options>             daemon receiving data from clients and sending to aggregators\n")
	_, _ = fmt.Fprintf(os.Stderr, "statshouse aggregator <options>        daemon receiving data from agents and inserting into clickhouse\n")
	_, _ = fmt.Fprintf(os.Stderr, "statshouse ingress_proxy <options>     proxy between agents in unprotected and aggregators in protected environment\n")
	_, _ = fmt.Fprintf(os.Stderr, "Tools usage:\n")
	_, _ = fmt.Fprintf(os.Stderr, "statshouse tlclient <options>          use as TL client to send JSON metrics to another statshouse\n")
	_, _ = fmt.Fprintf(os.Stderr, "statshouse test_map <options>          test key mapping pipeline\n")
	_, _ = fmt.Fprintf(os.Stderr, "statshouse test_parser <options>       parse and print packets received by UDP\n")
	_, _ = fmt.Fprintf(os.Stderr, "statshouse test_longpoll <options>     test longpoll journal\n")
	_, _ = fmt.Fprintf(os.Stderr, "statshouse simple_fsync <options>      simple SSD benchmark\n")
	_, _ = fmt.Fprintf(os.Stderr, "statshouse tlclient.api <options>      test API\n")
	_, _ = fmt.Fprintf(os.Stderr, "statshouse simulator <options>         simulate 10 agents sending data\n")
	_, _ = fmt.Fprintf(os.Stderr, "statshouse benchmark <options>         some brnchmark\n")
}
