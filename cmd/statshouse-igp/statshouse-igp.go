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
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/vkcom/statshouse/internal/agent"
	"github.com/vkcom/statshouse/internal/pcache"
	"github.com/vkcom/statshouse/internal/vkgo/build"
	"github.com/vkcom/statshouse/internal/vkgo/platform"
	"github.com/vkcom/statshouse/internal/vkgo/srvfunc"
)

const defaultPathToPwd = `/etc/engine/pass`

var argv struct {
	logFile            string
	userLogin          string // логин для setuid
	userGroup          string // логин для setguid
	maxOpenFiles       uint64
	pprofListenAddr    string
	pprofHTTP          bool
	aesPwdFile         string
	cacheDir           string
	aggAddr            string
	ingressExtAddr     string
	ingressExtAddrIPv6 string
	ingressPwdDir      string
	ConfigIngressProxy
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
	os.Exit(mainIngressProxy())
}

func mainIngressProxy() int {
	if err := parseCommandLine(); err != nil {
		log.Println(err)
		return 1
	}
	logRotate()
	// Read AES password
	var aesPwd string
	if argv.aesPwdFile == "" {
		// ignore error if file path wasn't explicitly specified
		if v, err := os.ReadFile(defaultPathToPwd); err == nil {
			aesPwd = string(v)
		}
	} else if v, err := os.ReadFile(argv.aesPwdFile); err != nil {
		// fatal if could not read file at path specified explicitly
		log.Printf("Could not read AES password file %s: %s", argv.aesPwdFile, err)
		return 1
	} else {
		aesPwd = string(v)
	}

	if argv.ingressPwdDir != "" {
		if err := argv.ReadIngressKeys(argv.ingressPwdDir); err != nil {
			log.Printf("could not read ingress keys: %v", err)
			return 1
		}
	}
	if _, err := srvfunc.SetHardRLimitNoFile(argv.maxOpenFiles); err != nil {
		log.Printf("Could not set new rlimit: %v", err)
	}
	if err := platform.ChangeUserGroup(argv.userLogin, argv.userGroup); err != nil {
		log.Printf("Could not change user/group to %q/%q: %v", argv.userLogin, argv.userGroup, err)
		return 1
	}
	if argv.pprofListenAddr != "" {
		go func() {
			if err := http.ListenAndServe(argv.pprofListenAddr, nil); err != nil {
				log.Printf("failed to listen pprof on %q: %v", argv.pprofListenAddr, err)
			}
		}()
	}
	// we support working without touching disk (on readonly filesystems, in stateless containers, etc)
	var fpmc *os.File
	if argv.cacheDir != "" {
		// do not want to confuse mappings from different clusters, this would be a disaster
		var err error
		if fpmc, err = os.OpenFile(filepath.Join(argv.cacheDir, fmt.Sprintf("mappings-%s.cache", argv.ConfigAgent.Cluster)), os.O_CREATE|os.O_RDWR, 0666); err == nil {
			defer fpmc.Close()
		} else {
			log.Printf("failed to open mappings cache: %v", err)
		}
	}
	mappingsCache, _ := pcache.LoadMappingsCacheFile(fpmc, argv.ConfigAgent.MappingCacheSize, argv.ConfigAgent.MappingCacheTTL)
	defer mappingsCache.Save()
	ctx, cancel := context.WithCancel(context.Background())
	exit := make(chan error, 1)
	go func() {
		exit <- RunIngressProxy(ctx, argv.ConfigIngressProxy, aesPwd, mappingsCache)
	}()
	signalC := make(chan os.Signal, 1)
	signal.Notify(signalC, syscall.SIGINT, syscall.SIGUSR1)
main_loop:
	select {
	case v := <-signalC:
		switch v {
		case syscall.SIGUSR1:
			logRotate()
			goto main_loop
		default: // syscall.SIGINT
			cancel()
			select {
			case <-exit:
			case <-time.After(5 * time.Second):
			}
			log.Println("Buy")
		}
	case err := <-exit:
		log.Println(err)
		cancel()
	}
	return 0
}

func parseCommandLine() error {
	const conveyorName = "ingress_proxy"
	if len(os.Args) > 1 {
		if os.Args[1] == conveyorName {
			log.Printf("positional argument %q is deprecated, you can safely remote it", conveyorName)
			os.Args = append(os.Args[:1], os.Args[2:]...)
		}
	}
	var dummyVerb bool
	flag.BoolVar(&dummyVerb, conveyorName, false, "not used, you can safely remote it")
	var dummyVersion, dummyConveyor, dummyHostname, dummyMetadataAddr string
	flag.StringVar(&dummyVersion, "ingress-version", "", "not used, you can safely remote it")
	flag.StringVar(&dummyConveyor, "new-conveyor", "", "not used, you can safely remote it")
	flag.StringVar(&dummyHostname, "hostname", "", "not used, you can safely remote it")
	flag.StringVar(&dummyMetadataAddr, "metadata-addr", "", "not used, you can safely remote it")
	var dummyMaxResponseMem int
	flag.IntVar(&dummyMaxResponseMem, "max-response-mem", 0, "not used, you can safely remote it")

	flag.StringVar(&argv.logFile, "l", "/dev/stdout", "log file")
	flag.StringVar(&argv.userLogin, "u", "kitten", "sets user name to make setuid")
	flag.StringVar(&argv.userGroup, "g", "kitten", "sets user group to make setguid")
	flag.Uint64Var(&argv.maxOpenFiles, "max-open-files", 131072, "open files limit")
	flag.StringVar(&argv.pprofListenAddr, "pprof", "", "HTTP pprof listen address")
	flag.BoolVar(&argv.pprofHTTP, "pprof-http", false, "Serve Go pprof HTTP on RPC port (deprecated due to security reasons)")
	flag.StringVar(&argv.aesPwdFile, "aes-pwd-file", "", "path to AES password file, will try to read "+defaultPathToPwd+" if not set")
	flag.StringVar(&argv.cacheDir, "cache-dir", "", "Data that cannot be immediately sent will be stored here together with metric metadata cache.")
	flag.StringVar(&argv.aggAddr, "agg-addr", "", "Comma-separated list of 3 aggregator addresses (shard 1 is recommended).")
	flag.StringVar(&argv.ConfigAgent.Cluster, "cluster", "statlogs2", "clickhouse cluster name to autodetect configuration, local shard and replica")
	flag.StringVar(&argv.ListenAddr, "ingress-addr", "", "Listen address of ingress proxy")
	flag.StringVar(&argv.ListenAddrIPV6, "ingress-addr-ipv6", "", "IPv6 listen address of ingress proxy")
	flag.StringVar(&argv.ingressExtAddr, "ingress-external-addr", "", "Comma-separate list of 3 external addresses of ingress proxies.")
	flag.StringVar(&argv.ingressExtAddrIPv6, "ingress-external-addr-ipv6", "", "Comma-separate list of IPv6 external addresses of ingress proxies.")
	flag.StringVar(&argv.ingressPwdDir, "ingress-pwd-dir", "", "path to AES passwords dir for clients of ingress proxy.")
	flag.StringVar(&argv.UpstreamAddr, "ingress-upstream-addr", "", "Upstream server address (for debug purpose, do not use in production).")
	argv.ConfigAgent.Bind(flag.CommandLine, agent.DefaultConfig())
	build.FlagParseShowVersionHelp()

	switch dummyConveyor {
	case "": // ok
	case conveyorName:
		log.Printf("new-conveyor option is deprecated, you can safely remote it")
	default:
		return fmt.Errorf("wrong value for -new-conveyor option %s, must be %q", dummyConveyor, conveyorName)
	}
	argv.ConfigAgent.AggregatorAddresses = strings.Split(argv.aggAddr, ",")
	argv.ExternalAddresses = strings.Split(argv.ingressExtAddr, ",")
	argv.ExternalAddressesIPv6 = strings.Split(argv.ingressExtAddrIPv6, ",")

	if len(argv.ConfigAgent.AggregatorAddresses) != 3 {
		return fmt.Errorf("-agg-addr must contain comma-separated list of 3 aggregators (1 shard is recommended)")
	}
	if argv.pprofHTTP {
		log.Printf("warning: --pprof-http option deprecated due to security reasons. Please use explicit --pprof=127.0.0.1:11123 option")
	}

	return argv.ConfigAgent.ValidateConfigSource()
}
