// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/VKCOM/statshouse-go"
	"github.com/cloudflare/tableflip"
	"github.com/gorilla/handlers"

	"github.com/VKCOM/statshouse/internal/api"
	"github.com/VKCOM/statshouse/internal/chutil"
	"github.com/VKCOM/statshouse/internal/config"
	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/VKCOM/statshouse/internal/format"
	"github.com/VKCOM/statshouse/internal/pcache/sqlitecache"
	"github.com/VKCOM/statshouse/internal/util"
	"github.com/VKCOM/statshouse/internal/vkgo/build"
	"github.com/VKCOM/statshouse/internal/vkgo/rpc"
	"github.com/VKCOM/statshouse/internal/vkgo/srvfunc"
	"github.com/VKCOM/statshouse/internal/vkgo/vkuth"
)

const (
	shutdownTimeout = 30 * time.Second
	exitTimeout     = 45 * time.Second
	upgradeTimeout  = 60 * time.Second

	httpReadHeaderTimeout = 10 * time.Second
	httpReadTimeout       = 30 * time.Second
	httpIdleTimeout       = 5 * time.Minute

	chDialTimeout = 5 * time.Second

	diskCacheTxDuration = 5 * time.Second
)

var argv struct {
	accessLog                bool
	rpcCryptoKeyPath         string
	brsMaxChunksCount        int
	chV1Addrs                []string
	chV1Debug                bool
	chV1MaxConns             int
	chV1Password             string
	chV1User                 string
	chV2Addrs                []string
	chV2Debug                bool
	chV2MaxLightFastConns    int
	chV2MaxHeavyFastConns    int
	chV2MaxHeavySlowConns    int
	chV2MaxLightSlowConns    int
	chV2MaxHardwareFastConns int
	chV2MaxHardwareSlowConns int

	chV2Password             string
	chV2PasswordFile         string
	chV2User                 string
	defaultMetric            string
	defaultMetricFilterIn    []string
	defaultMetricFilterNotIn []string
	defaultMetricWhat        []string
	defaultMetricGroupBy     []string
	adminDash                int
	eventPreset              []string
	defaultNumSeries         int
	diskCache                string // TODO: remove, use "cacheDir"
	cacheDir                 string
	help                     bool
	listenHTTPAddr           string
	listenRPCAddr            string
	pidFile                  string
	pprofAddr                string
	pprofHTTP                bool
	showInvisible            bool
	slow                     time.Duration
	staticDir                string
	statsHouseNetwork        string
	statsHouseAddr           string
	statsHouseEnv            string
	utcOffsetHours           int // we can't support offsets not divisible by hour because we aggregate the data by hour
	version                  bool
	vkuthAppName             string
	vkuthPublicKeysArg       []string
	vkuthPublicKeys          map[string][]byte
	metadataActorID          int64
	metadataAddr             string
	metadataNet              string

	api.HandlerOptions
	api.Config
}

func main() {
	log.SetPrefix("[statshouse-api] ")
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lmsgprefix)
	os.Exit(run())
}

func run() int {
	if err := parseCommandLine(); err != nil {
		log.Println(err)
		return 1
	}
	if argv.help {
		flag.Usage()
		return 0
	}
	if argv.version {
		log.Println(build.Info())
		return 0
	}

	tf, err := tableflip.New(tableflip.Options{
		PIDFile:        argv.pidFile,
		UpgradeTimeout: upgradeTimeout,
	})
	if err != nil {
		log.Printf("failed to init tableflip: %v", err)
		return 1
	}
	defer tf.Stop()

	go func() {
		ch := make(chan os.Signal, 3)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
		for sig := range ch {
			switch sig {
			case syscall.SIGINT, syscall.SIGTERM:
				log.Printf("got %v, exiting...", sig)
				tf.Stop()
			case syscall.SIGHUP:
				log.Printf("got %v, upgrading...", sig)
				err := tf.Upgrade()
				if err != nil {
					log.Printf("upgrade failed: %v", err)
				}
			}
		}
	}()

	httpLn, err := tf.Listen("tcp", argv.listenHTTPAddr)
	if err != nil {
		log.Printf("failed to listen on %q: %v", argv.listenHTTPAddr, err)
		return 1
	}

	var chV1 *chutil.ClickHouse
	if len(argv.chV1Addrs) > 0 {
		// argv.chV1MaxConns, argv.chV2MaxHeavyConns, argv.chV1Addrs, argv.chV1User, argv.chV1Password, argv.chV1Debug, chDialTimeout
		chV1, err = chutil.OpenClickHouse(chutil.ChConnOptions{
			Addrs:       argv.chV1Addrs,
			User:        argv.chV1User,
			Password:    argv.chV1Password,
			DialTimeout: chDialTimeout,
			ConnLimits: chutil.ConnLimits{
				FastLightMaxConns: argv.chV1MaxConns,
				FastHeavyMaxConns: argv.chV1MaxConns,
				SlowLightMaxConns: argv.chV1MaxConns,
				SlowHeavyMaxConns: argv.chV1MaxConns,
			},
		})
		if err != nil {
			log.Printf("failed to open ClickHouse-v1: %v", err)
			return 1
		}
		defer func() { chV1.Close() }()
	}
	// argv.chV2MaxLightFastConns, argv.chV2MaxHeavyConns, , , argv.chV2Password, argv.chV2Debug, chDialTimeout
	chV2, err := chutil.OpenClickHouse(chutil.ChConnOptions{
		Addrs:       argv.chV2Addrs,
		User:        argv.chV2User,
		Password:    argv.chV2Password,
		DialTimeout: chDialTimeout,
		ConnLimits: chutil.ConnLimits{
			FastLightMaxConns:    argv.chV2MaxLightFastConns,
			FastHeavyMaxConns:    argv.chV2MaxHeavyFastConns,
			SlowLightMaxConns:    argv.chV2MaxLightSlowConns,
			SlowHeavyMaxConns:    argv.chV2MaxHeavySlowConns,
			FastHardwareMaxConns: argv.chV2MaxHardwareFastConns,
			SlowHardwareMaxConns: argv.chV2MaxHardwareSlowConns,
		},
	})
	if err != nil {
		log.Printf("failed to open ClickHouse-v2: %v", err)
		return 1
	}
	defer func() { chV2.Close() }()
	c := rpc.NewClient(rpc.ClientWithLogf(log.Printf), rpc.ClientWithTrustedSubnetGroups(build.TrustedSubnetGroups()))
	defer func() { _ = c.Close() }()

	dc, err := sqlitecache.OpenSqliteDiskCache(argv.diskCache, diskCacheTxDuration)
	if err != nil {
		log.Printf("failed to open disk cache: %v", err)
		return 1
	}
	defer func() {
		err := dc.Close()
		if err != nil {
			log.Printf("failed to close disk cache: %v", err)
		}
	}()

	statshouse.ConfigureNetwork(log.Printf, argv.statsHouseNetwork, argv.statsHouseAddr, argv.statsHouseEnv)
	defer func() { _ = statshouse.Close() }()
	var rpcCryptoKeys []string
	if argv.rpcCryptoKeyPath != "" {
		cryptoKey, err := os.ReadFile(argv.rpcCryptoKeyPath)
		if err != nil {
			log.Printf("could not read RPC crypto key file %q: %v", argv.rpcCryptoKeyPath, err)
			return 1
		}
		rpcCryptoKeys = append(rpcCryptoKeys, string(cryptoKey))
	}
	rpcCryptoKey := ""
	if len(rpcCryptoKeys) > 0 {
		rpcCryptoKey = rpcCryptoKeys[0]
	}

	if staticFS == nil {
		staticFS = os.DirFS(argv.staticDir)
	}

	jwtHelper := vkuth.NewJWTHelper(argv.vkuthPublicKeys, argv.vkuthAppName)
	defaultMetricFilterIn := map[string][]string{}
	defaultMetricFilterNotIn := map[string][]string{}
	for _, s := range argv.defaultMetricFilterIn {
		kv := strings.Split(s, ":")
		if len(kv) != 2 {
			log.Printf("[error] default-metric-filter-in invalid format: %s", s)
		}
		f := defaultMetricFilterIn[kv[0]]
		defaultMetricFilterIn[kv[0]] = append(f, kv[1])
	}
	for _, s := range argv.defaultMetricFilterNotIn {
		kv := strings.Split(s, ":")
		if len(kv) != 2 {
			log.Printf("[error] default-metric-filter-not-in invalid format: %s", s)
		}
		f := defaultMetricFilterNotIn[kv[0]]
		defaultMetricFilterNotIn[kv[0]] = append(f, kv[1])
	}
	jsSettings := api.JSSettings{
		VkuthAppName:             argv.vkuthAppName,
		DefaultMetric:            argv.defaultMetric,
		DefaultMetricGroupBy:     argv.defaultMetricGroupBy,
		DefaultMetricWhat:        argv.defaultMetricWhat,
		DefaultMetricFilterIn:    defaultMetricFilterIn,
		DefaultMetricFilterNotIn: defaultMetricFilterNotIn,
		EventPreset:              argv.eventPreset,
		DefaultNumSeries:         argv.defaultNumSeries,
		DisableV1:                len(argv.chV1Addrs) == 0,
		AdminDash:                argv.adminDash,
	}
	if argv.LocalMode {
		jsSettings.VkuthAppName = ""
	}
	f, err := api.NewHandler(
		staticFS,
		jsSettings,
		argv.showInvisible,
		chV1,
		chV2,
		&tlmetadata.Client{
			Client:  rpc.NewClient(rpc.ClientWithLogf(log.Printf), rpc.ClientWithCryptoKey(rpcCryptoKey), rpc.ClientWithTrustedSubnetGroups(build.TrustedSubnetGroups())),
			Network: argv.metadataNet,
			Address: argv.metadataAddr,
			ActorID: argv.metadataActorID,
		},
		dc,
		jwtHelper,
		argv.HandlerOptions,
		&argv.Config,
	)
	if err != nil {
		log.Printf("failed to create handler: %v", err)
		return 1
	}
	defer func() { _ = f.Close() }()

	m := api.NewHTTPRouter(f)
	a := m.PathPrefix(api.RoutePrefix).Subrouter()
	a.Router.Path("/"+api.EndpointLegacyRedirect).Methods("GET", "HEAD", "POST").HandlerFunc(f.HandleLegacyRedirect)
	a.Path("/" + api.EndpointMetricList).Methods("GET").HandlerFunc(api.HandleGetMetricsList)
	a.Path("/" + api.EndpointMetricTagValues).Methods("GET").HandlerFunc(api.HandleGetMetricTagValues)
	a.Path("/proxy").Methods("GET", "POST").HandlerFunc(api.HandleProxy)
	a.Path("/" + api.EndpointMetric).Methods("GET").HandlerFunc(api.HandleGetMetric)
	a.Path("/" + api.EndpointMetric).Methods("POST").HandlerFunc(api.HandlePostMetric)
	a.Path("/" + api.EndpointResetFlood).Methods("POST").HandlerFunc(api.HandlePostResetFlood)
	a.Path("/" + api.EndpointQuery).Methods("GET").HandlerFunc(api.HandleSeriesQuery)
	a.Path("/badges").Methods("GET", "POST").HandlerFunc(api.HandleBadgesQuery)
	a.Path("/" + api.EndpointPoint).Methods("GET").HandlerFunc(api.HandlePointQuery)
	a.Path("/" + api.EndpointPoint).Methods("POST").HandlerFunc(api.HandlePointQuery)
	a.Path("/" + api.EndpointTable).Methods("GET").HandlerFunc(api.HandleGetTable)
	a.Path("/" + api.EndpointQuery).Methods("POST").HandlerFunc(api.HandleSeriesQuery)
	a.Path("/" + api.EndpointRender).Methods("GET").HandlerFunc(api.HandleGetRender)
	a.Path("/" + api.EndpointDashboard).Methods("GET").HandlerFunc(api.HandleGetDashboard)
	a.Path("/" + api.EndpointDashboardList).Methods("GET").HandlerFunc(api.HandleGetDashboardList)
	a.Path("/"+api.EndpointDashboard).Methods("POST", "PUT").HandlerFunc(api.HandlePutPostDashboard)
	a.Path("/" + api.EndpointGroup).Methods("GET").HandlerFunc(api.HandleGetGroup)
	a.Path("/" + api.EndpointGroupList).Methods("GET").HandlerFunc(api.HandleGetGroupsList)
	a.Path("/"+api.EndpointGroup).Methods("POST", "PUT").HandlerFunc(api.HandlePutPostGroup)
	a.Path("/"+api.EndpointNamespace).Methods("POST", "PUT").HandlerFunc(api.HandlePostNamespace)
	a.Path("/" + api.EndpointNamespace).Methods("GET").HandlerFunc(api.HandleGetNamespace)
	a.Path("/" + api.EndpointNamespaceList).Methods("GET").HandlerFunc(api.HandleGetNamespaceList)
	a.Path("/" + api.EndpointPrometheus).Methods("GET").HandlerFunc(api.HandleGetPromConfig)
	a.Path("/" + api.EndpointPrometheus).Methods("POST").HandlerFunc(api.HandlePostPromConfig)
	a.Path("/" + api.EndpointPrometheusGenerated).Methods("GET").HandlerFunc(api.HandleGetPromConfigGenerated)
	a.Path("/" + api.EndpointKnownTags).Methods("POST").HandlerFunc(api.HandlePostKnownTags)
	a.Path("/" + api.EndpointKnownTags).Methods("GET").HandlerFunc(api.HandleGetKnownTags)
	a.Path("/" + api.EndpointStatistics).Methods("POST").HandlerFunc(api.HandleFrontendStat)
	a.Path("/" + api.EndpointHistory).Methods("GET").HandlerFunc(api.HandleGetHistory)
	a.Path("/" + api.EndpointHealthcheck).Methods("GET").HandlerFunc(api.HandleGetHealthcheck)
	m.Path("/prom/api/v1/query").Methods("GET").HandlerFunc(api.HandleInstantQuery)
	m.Path("/prom/api/v1/query").Methods("POST").HandlerFunc(api.HandleInstantQuery)
	m.Path("/prom/api/v1/query_range").Methods("GET").HandlerFunc(api.HandleRangeQuery)
	m.Path("/prom/api/v1/query_range").Methods("POST").HandlerFunc(api.HandleRangeQuery)
	m.Path("/prom/api/v1/labels").Methods("GET", "POST").HandlerFunc(api.HandlePromLabelsQuery)
	m.Path("/prom/api/v1/label/{name}/values").Methods("GET", "POST").HandlerFunc(api.HandlePromLabelValuesQuery)
	m.Path("/prom/api/v1/series").Methods("GET").HandlerFunc(api.HandlePromSeriesQuery)
	m.Path("/prom/api/v1/series").Methods("POST").HandlerFunc(api.HandlePromSeriesQuery)
	m.Path("/debug/pprof/").Methods("GET").HandlerFunc(api.HandleProf)
	m.Path("/debug/pprof/allocs").Methods("GET").HandlerFunc(api.HandleProf)
	m.Path("/debug/pprof/block").Methods("GET").HandlerFunc(api.HandleProf)
	m.Path("/debug/pprof/cmdline").Methods("GET").HandlerFunc(api.HandleProfCmdline)
	m.Path("/debug/pprof/goroutine").Methods("GET").HandlerFunc(api.HandleProf)
	m.Path("/debug/pprof/heap").Methods("GET").HandlerFunc(api.HandleProf)
	m.Path("/debug/pprof/mutex").Methods("GET").HandlerFunc(api.HandleProf)
	m.Path("/debug/pprof/profile").Methods("GET").HandlerFunc(api.HandleProfProfile)
	m.Path("/debug/pprof/threadcreate").Methods("GET").HandlerFunc(api.HandleProf)
	m.Path("/debug/pprof/trace").Methods("GET").HandlerFunc(api.HandleProfTrace)
	m.Path("/debug/pprof/symbol").Methods("GET").HandlerFunc(api.HandleProfSymbol)
	m.Path("/debug/500").Methods("GET").HandlerFunc(api.DumpInternalServerErrors)
	m.Path("/debug/top/mem").Methods("GET").HandlerFunc(api.DumpQueryTopMemUsage)
	m.Path("/debug/top/time").Methods("GET").HandlerFunc(api.DumpQueryTopDuration)
	m.Path("/debug/tag/draft").Methods("GET").HandlerFunc(api.HandleTagDraftList)
	m.Path("/debug/cache/log").Methods("GET").HandlerFunc(api.DebugCacheLog)
	m.Path("/debug/cache/reset").Methods("GET").HandlerFunc(api.DebugCacheReset)
	m.Path("/debug/cache/init").Methods("GET").HandlerFunc(api.DebugCacheCreateMetrics)
	m.Path("/debug/cache/info").Methods("GET").HandlerFunc(api.DebugCacheInfo)
	m.Router.PathPrefix("/").Methods("GET", "HEAD").HandlerFunc(f.HandleStatic)

	h := http.Handler(m)
	h = handlers.RecoveryHandler(handlers.PrintRecoveryStack(true))(h)
	h = handlers.CompressHandler(h)
	if argv.accessLog {
		h = handlers.CombinedLoggingHandler(os.Stdout, h)
	}
	h = handlers.ProxyHeaders(h)
	if argv.slow > 0 {
		prev := h
		h = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(argv.slow)
			prev.ServeHTTP(w, r)
		})
	}

	s := &http.Server{
		Handler:           h,
		ReadHeaderTimeout: httpReadHeaderTimeout,
		ReadTimeout:       httpReadTimeout,
		IdleTimeout:       httpIdleTimeout,
	}

	go func() {
		err := s.Serve(httpLn)
		if err != http.ErrServerClosed {
			log.Printf("serving HTTP: %v", err)
		}
	}()

	brs := api.NewBigResponseStorage(argv.brsMaxChunksCount, time.Second)
	defer brs.Close()

	chunksCountMeasurementID := statshouse.StartRegularMeasurement(api.CurrentChunksCount(brs))
	defer statshouse.StopRegularMeasurement(chunksCountMeasurementID)

	startTimestamp := time.Now().Unix()
	heartbeatTags := statshouse.Tags{
		1: "4",
		2: fmt.Sprint(format.TagValueIDHeartbeatEventStart),
		6: fmt.Sprint(build.CommitTimestamp()),
		7: srvfunc.HostnameForStatshouse(),
	}
	if build.Commit() != "?" {
		commitRaw, err := hex.DecodeString(build.Commit())
		if err == nil && len(commitRaw) >= 4 {
			heartbeatTags[4] = fmt.Sprint(int32(binary.BigEndian.Uint32(commitRaw)))
		}
	}
	statshouse.Value(format.BuiltinMetricMetaHeartbeatVersion.Name, heartbeatTags, 0)

	heartbeatTags[2] = fmt.Sprint(format.TagValueIDHeartbeatEventHeartbeat)
	defer statshouse.StopRegularMeasurement(statshouse.StartRegularMeasurement(func(c *statshouse.Client) {
		uptime := float64(time.Now().Unix() - startTimestamp)
		c.Value(format.BuiltinMetricMetaHeartbeatVersion.Name, heartbeatTags, uptime)
	}))

	handlerRPC := api.NewRPCRouter(f, brs)
	var hijackListener *rpc.HijackListener
	metrics := util.NewRPCServerMetrics("statshouse_api")
	srv := rpc.NewServer(
		rpc.ServerWithSocketHijackHandler(func(conn *rpc.HijackConnection) {
			hijackListener.AddConnection(conn)
		}),
		rpc.ServerWithLogf(log.Printf),
		rpc.ServerWithTrustedSubnetGroups(build.TrustedSubnetGroups()),
		rpc.ServerWithHandler(handlerRPC.Handle),
		rpc.ServerWithCryptoKeys(rpcCryptoKeys),
		metrics.ServerWithMetrics,
	)
	defer metrics.Run(srv)()
	defer func() { _ = srv.Close() }()

	rpcLn, err := tf.Listen("tcp4", argv.listenRPCAddr)
	if err != nil {
		log.Printf("could not listen RPC: %v", err)
		return 1
	}

	hijackListener = rpc.NewHijackListener(rpcLn.Addr())
	defer func() { _ = hijackListener.Close() }()
	go func() {
		err := srv.Serve(rpcLn)
		if err != nil {
			log.Fatalln("RPC server failed:", err)
		}
	}()
	if argv.pprofHTTP {
		go func() { // serve pprof on RPC port
			m := http.NewServeMux()
			m.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
				http.DefaultServeMux.ServeHTTP(w, r)
			})
			log.Printf("serving Go pprof at %q", argv.listenRPCAddr)
			s := http.Server{Handler: m}
			_ = s.Serve(hijackListener)
		}()
	} else {
		_ = hijackListener.Close() // will close all incoming connections
	}
	err = tf.Ready()
	if err != nil {
		log.Printf("failed to become ready: %v", err)
		return 1
	}

	log.Printf("version %v listening HTTP at %q listening RPC at %q", build.Version(), httpLn.Addr().String(), rpcLn.Addr().String())
	<-tf.Exit()

	time.AfterFunc(exitTimeout, func() {
		log.Printf("graceful shutdown timeout; exiting")
		os.Exit(1)
	})
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	s.Shutdown(ctx)

	return 0
}

func parseCommandLine() (err error) {
	flag.BoolVar(&argv.accessLog, "access-log", false, "write HTTP access log to stdout")
	flag.StringVar(&argv.rpcCryptoKeyPath, "rpc-crypto-path", "", "path to RPC crypto key")
	flag.IntVar(&argv.brsMaxChunksCount, "max-chunks-count", 1000, "in memory data chunks count limit for RPC server")
	var chMaxQueries int // not used any more, TODO - remove?
	flag.IntVar(&chMaxQueries, "clickhouse-max-queries", 32, "maximum number of concurrent ClickHouse queries")
	config.StringSliceVar(flag.CommandLine, &argv.chV1Addrs, "clickhouse-v1-addrs", "", "comma-separated list of ClickHouse-v1 addresses")
	flag.BoolVar(&argv.chV1Debug, "clickhouse-v1-debug", false, "ClickHouse-v1 debug mode")
	flag.IntVar(&argv.chV1MaxConns, "clickhouse-v1-max-conns", 16, "maximum number of ClickHouse-v1 connections (fast and slow)")
	flag.StringVar(&argv.chV1Password, "clickhouse-v1-password", "", "ClickHouse-v1 password")
	flag.StringVar(&argv.chV1User, "clickhouse-v1-user", "", "ClickHouse-v1 user")
	config.StringSliceVar(flag.CommandLine, &argv.chV2Addrs, "clickhouse-v2-addrs", "", "comma-separated list of ClickHouse-v2 addresses")
	flag.BoolVar(&argv.chV2Debug, "clickhouse-v2-debug", false, "ClickHouse-v2 debug mode")
	flag.IntVar(&argv.chV2MaxLightFastConns, "clickhouse-v2-max-conns", 40, "maximum number of ClickHouse-v2 connections (light fast)")
	flag.IntVar(&argv.chV2MaxLightSlowConns, "clickhouse-v2-max-light-slow-conns", 12, "maximum number of ClickHouse-v2 connections (light slow)")
	flag.IntVar(&argv.chV2MaxHeavyFastConns, "clickhouse-v2-max-heavy-conns", 5, "maximum number of ClickHouse-v2 connections (heavy fast)")
	flag.IntVar(&argv.chV2MaxHeavySlowConns, "clickhouse-v2-max-heavy-slow-conns", 1, "maximum number of ClickHouse-v2 connections (heavy slow)")
	flag.IntVar(&argv.chV2MaxHardwareFastConns, "clickhouse-v2-max-hardware-fast-conns", 8, "maximum number of ClickHouse-v2 connections (hardware fast)")
	flag.IntVar(&argv.chV2MaxHardwareSlowConns, "clickhouse-v2-max-hardware-slow-conns", 4, "maximum number of ClickHouse-v2 connections (hardware slow)")

	flag.StringVar(&argv.chV2Password, "clickhouse-v2-password", "", "ClickHouse-v2 password")
	flag.StringVar(&argv.chV2PasswordFile, "clickhouse-v2-password-file", "", "file with ClickHouse-v2 password")
	flag.StringVar(&argv.chV2User, "clickhouse-v2-user", "", "ClickHouse-v2 user")
	flag.StringVar(&argv.defaultMetric, "default-metric", format.BuiltinMetricMetaAggBucketReceiveDelaySec.Name, "default metric to show")
	config.StringSliceVar(flag.CommandLine, &argv.defaultMetricFilterIn, "default-metric-filter-in", "", "default metric filter in <key0>:value")
	config.StringSliceVar(flag.CommandLine, &argv.defaultMetricFilterNotIn, "default-metric-filter-not-in", "", "default metric filter not in <key0>:value")
	config.StringSliceVar(flag.CommandLine, &argv.defaultMetricWhat, "default-metric-filter-what", "", "default metric function")
	config.StringSliceVar(flag.CommandLine, &argv.defaultMetricGroupBy, "default-metric-group-by", "1", "default metric group by tags")
	flag.IntVar(&argv.adminDash, "admin-dash-id", 0, "hardware metric dashboard")
	config.StringSliceVar(flag.CommandLine, &argv.eventPreset, "event-preset", "", "event preset")
	flag.IntVar(&argv.defaultNumSeries, "default-num-series", 5, "default series number to request")
	flag.StringVar(&argv.diskCache, "disk-cache", "statshouse_api_cache.db", "disk cache filename")
	flag.StringVar(&argv.cacheDir, "cache-dir", "", "Directory to store metric metadata cache.")
	flag.BoolVar(&argv.help, "help", false, "print usage instructions and exit")
	flag.StringVar(&argv.listenHTTPAddr, "listen-addr", "localhost:8080", "web server listen address")
	flag.StringVar(&argv.listenRPCAddr, "listen-rpc-addr", "localhost:13347", "RPC server listen address")
	flag.StringVar(&argv.pidFile, "pid-file", "statshouse_api.pid", "path to PID file") // fpr table flip

	flag.StringVar(&argv.pprofAddr, "pprof-addr", "", "Go pprof HTTP listen address (deprecated)")
	flag.BoolVar(&argv.pprofHTTP, "pprof-http", true, "Serve Go pprof HTTP on RPC port")
	flag.BoolVar(&argv.showInvisible, "show-invisible", false, "show invisible metrics as well")
	flag.DurationVar(&argv.slow, "slow", 0, "slow down all HTTP requests by this much")
	flag.StringVar(&argv.staticDir, "static-dir", "", "directory with static assets")
	flag.StringVar(&argv.statsHouseNetwork, "statshouse-network", statshouse.DefaultNetwork, "udp or unixgram")
	flag.StringVar(&argv.statsHouseAddr, "statshouse-addr", statshouse.DefaultAddr, "address of udp socket or path to unix socket")
	flag.StringVar(&argv.statsHouseEnv, "statshouse-env", "dev", "fill key0/environment with this value in StatHouse statistics")
	flag.IntVar(&argv.utcOffsetHours, "utc-offset", 0, "UTC offset for aggregation, in hours")
	flag.BoolVar(&argv.version, "version", false, "show version information and exit")
	flag.StringVar(&argv.vkuthAppName, "vkuth-app-name", "statshouse-api", "vkuth application name (access bits namespace)")
	config.StringSliceVar(flag.CommandLine, &argv.vkuthPublicKeysArg, "vkuth-public-keys", "", "comma-separated list of trusted vkuth public keys; empty list disables token-based access control")

	flag.Int64Var(&argv.metadataActorID, "metadata-actor-id", 0, "metadata engine actor id")
	flag.StringVar(&argv.metadataAddr, "metadata-addr", "127.0.0.1:2442", "metadata engine address")
	flag.StringVar(&argv.metadataNet, "metadata-net", "tcp4", "metadata engine network")
	argv.HandlerOptions.Bind(flag.CommandLine)
	argv.Config.Bind(flag.CommandLine, api.DefaultConfig())
	flag.Parse()

	if len(flag.Args()) != 0 {
		return fmt.Errorf("unexpected command line arguments, check command line for typos: %q", flag.Args())
	}
	if len(argv.chV2Addrs) == 0 {
		return fmt.Errorf("--clickhouse-v2-addrs must be specified")
	}

	if math.Abs(float64(argv.utcOffsetHours)) > 168 { // hours in week (24*7=168)
		return fmt.Errorf("invalid --utc-offset value")
	}
	if staticFS != nil && argv.staticDir != "" {
		return fmt.Errorf("--static-dir must not be specified when static is embedded into the binary")
	}
	if argv.vkuthPublicKeys, err = vkuth.ParseVkuthKeys(argv.vkuthPublicKeysArg); err != nil {
		return err
	}
	if argv.chV2PasswordFile != "" {
		if p, err := os.ReadFile(argv.chV2PasswordFile); err == nil {
			p = bytes.TrimSpace(p)
			argv.chV2Password = string(p)
		} else {
			return fmt.Errorf("failed to read --clickhouse-v2-password-file: %w", err)
		}

	}
	if err = argv.Config.ValidateConfig(); err != nil {
		return err
	}

	return argv.HandlerOptions.Parse()
}
