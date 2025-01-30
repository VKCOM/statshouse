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

func main() {
	if err := parseCommandLine(); err != nil {
		log.Fatal(err)
	}
	if err := platform.ChangeUserGroup(argv.userLogin, argv.userGroup); err != nil {
		log.Fatalf("Could not change user/group to %q/%q: %v", argv.userLogin, argv.userGroup, err)
	}

	// Read AES password
	var aesPwd string
	if argv.aesPwdFile == "" {
		// ignore error if file path wasn't explicitly specified
		if v, err := os.ReadFile(defaultPathToPwd); err == nil {
			aesPwd = string(v)
		}
	} else if v, err := os.ReadFile(argv.aesPwdFile); err != nil {
		// fatal if could not read file at path specified explicitly
		log.Fatalf("Could not read AES password file %s: %s", argv.aesPwdFile, err)
	} else {
		aesPwd = string(v)
	}

	if argv.ingressPwdDir != "" {
		if err := argv.ReadIngressKeys(argv.ingressPwdDir); err != nil {
			log.Fatalf("could not read ingress keys: %v", err)
		}
	}
	if argv.pprofListenAddr != "" {
		go func() {
			if err := http.ListenAndServe(argv.pprofListenAddr, nil); err != nil {
				log.Printf("failed to listen pprof on %q: %v", argv.pprofListenAddr, err)
			}
		}()
	}
	if _, err := srvfunc.SetHardRLimitNoFile(argv.maxOpenFiles); err != nil {
		log.Printf("Could not set new rlimit: %v", err)
	}
	// we support working without touching disk (on readonly filesystems, in stateless containers, etc)
	var mappingsCache *pcache.MappingsCache
	if argv.cacheDir != "" {
		// do not want to confuse mappings from different clusters, this would be a disaster
		if fpmc, err := os.OpenFile(filepath.Join(argv.cacheDir, fmt.Sprintf("mappings-%s.cache", argv.Cluster)), os.O_CREATE|os.O_RDWR, 0666); err == nil {
			defer fpmc.Close()
			mappingsCache, err = pcache.LoadMappingsCacheFile(fpmc, argv.ConfigAgent.MappingCacheSize, argv.ConfigAgent.MappingCacheTTL)
			if err == nil {
				defer mappingsCache.Save()
			} else {
				// ignore error because cache can be damaged
				log.Printf("failed to load mappings cache: %v", err)
			}
		} else {
			log.Printf("failed to open mappings cache: %v", err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	exit := make(chan error, 1)
	go func() {
		exit <- RunIngressProxy(ctx, argv.ConfigIngressProxy, aesPwd, mappingsCache)
	}()
	signalC := make(chan os.Signal, 1)
	signal.Notify(signalC, syscall.SIGINT)
	select {
	case <-signalC:
		cancel()
		select {
		case <-exit:
		case <-time.After(5 * time.Second):
		}
		log.Println("Buy")
	case err := <-exit:
		log.Println(err)
		cancel()
	}
}

func parseCommandLine() error {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "ingress_proxy", "-ingress_proxy":
			log.Printf("positional argument %q is deprecated, it is safe to remove it", os.Args[1])
			os.Args = append(os.Args[:1], os.Args[2:]...)
		}
		const conveyorArgPrefix = "-new-conveyor="
		for i, v := range os.Args {
			if strings.HasPrefix(v, conveyorArgPrefix) {
				if s := v[len(conveyorArgPrefix):]; s == "ingress_proxy" {
					log.Printf("option %q is deprecated, it is safe to remove it", v)
					os.Args = append(os.Args[:i], os.Args[i+1:]...)
					break
				} else {
					return fmt.Errorf("wrong value for -new-conveyor option %s, must be 'ingress_proxy'", s)
				}
			}
		}
	}

	flag.StringVar(&argv.userLogin, "u", "kitten", "sets user name to make setuid")
	flag.StringVar(&argv.userGroup, "g", "kitten", "sets user group to make setguid")
	flag.Uint64Var(&argv.maxOpenFiles, "max-open-files", 131072, "open files limit")
	flag.StringVar(&argv.pprofListenAddr, "pprof", "", "HTTP pprof listen address")
	flag.BoolVar(&argv.pprofHTTP, "pprof-http", false, "Serve Go pprof HTTP on RPC port (deprecated due to security reasons)")
	flag.StringVar(&argv.aesPwdFile, "aes-pwd-file", "", "path to AES password file, will try to read "+defaultPathToPwd+" if not set")
	flag.StringVar(&argv.cacheDir, "cache-dir", "", "Data that cannot be immediately sent will be stored here together with metric metadata cache.")
	flag.StringVar(&argv.aggAddr, "agg-addr", "", "Comma-separated list of 3 aggregator addresses (shard 1 is recommended).")
	flag.StringVar(&argv.Cluster, "cluster", "statlogs2", "clickhouse cluster name to autodetect configuration, local shard and replica")
	flag.StringVar(&argv.ListenAddr, "ingress-addr", "", "Listen address of ingress proxy")
	flag.StringVar(&argv.ListenAddrIPV6, "ingress-addr-ipv6", "", "IPv6 listen address of ingress proxy")
	flag.StringVar(&argv.ingressExtAddr, "ingress-external-addr", "", "Comma-separate list of 3 external addresses of ingress proxies.")
	flag.StringVar(&argv.ingressExtAddrIPv6, "ingress-external-addr-ipv6", "", "Comma-separate list of IPv6 external addresses of ingress proxies.")
	flag.StringVar(&argv.ingressPwdDir, "ingress-pwd-dir", "", "path to AES passwords dir for clients of ingress proxy.")
	flag.StringVar(&argv.Version, "ingress-version", "", "")
	flag.StringVar(&argv.UpstreamAddr, "ingress-upstream-addr", "", "Upstream server address (for debug purpose, do not use in production).")

	argv.ConfigAgent.Bind(flag.CommandLine, agent.DefaultConfig(), false)
	build.FlagParseShowVersionHelp()
	argv.ConfigAgent.AggregatorAddresses = strings.Split(argv.aggAddr, ",")
	argv.ExternalAddresses = strings.Split(argv.ingressExtAddr, ",")
	argv.ExternalAddressesIPv6 = strings.Split(argv.ingressExtAddrIPv6, ",")

	if len(argv.ConfigAgent.AggregatorAddresses) != 3 {
		return fmt.Errorf("-agg-addr must contain comma-separated list of 3 aggregators (1 shard is recommended)")
	}
	if err := argv.ConfigAgent.ValidateConfigSource(); err != nil {
		return err
	}
	if argv.pprofHTTP {
		log.Printf("warning: --pprof-http option deprecated due to security reasons. Please use explicit --pprof=127.0.0.1:11123 option")
	}
	return nil
}
