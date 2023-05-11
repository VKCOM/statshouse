// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package prometheus

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"log"
	"net/netip"
	"os"
	"sort"
	"sync"

	"gopkg.in/yaml.v2"

	promlog "github.com/go-kit/log"

	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/scrape"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/discovery/targetgroup"

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tl"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
)

type PromGroupsClient struct {
	args tlstatshouse.GetTargets2Bytes // only fields_masks for WriteResult
	host string
	hash string
	addr netip.Addr
}

type Updater struct {
	promMx  sync.Mutex
	cfg     *config.Config
	version int64 // use only one goroutine
	groups  map[string][]*targetgroup.Group

	// host -> hostInfo
	mx    sync.RWMutex
	hosts map[netip.Addr]hostInfo

	groupsClientsMu   sync.Mutex // Taken after mx
	promGroupsClients map[*rpc.HandlerContext]PromGroupsClient

	ctx    context.Context
	cancel func()

	manager  *discoveryManager
	logTrace func(format string, a ...interface{})

	hostName string
}

type hostInfo struct {
	hash   string
	result tlstatshouse.GetTargetsResultBytes
}

var emptyTrace = func(format string, a ...interface{}) {}
var hashFunc = sha256.Sum256
var hashStringRepr = func(hash []byte) string {
	return string(hash)
}

func RunPromUpdaterAsync(hostName string, logTrace bool) (*Updater, error) {
	var logTraceFunc func(format string, a ...interface{})
	if logTrace {
		logTraceFunc = log.Printf
	}
	cfg := &config.DefaultConfig
	return runUpdater(hostName, cfg, 0, logTraceFunc), nil
}

func runUpdater(hostName string, cfg *config.Config, configVersion int64, logTrace func(format string, a ...interface{})) *Updater {
	log.Println("creating prom discovery manager")
	ctx, cancelFunc := context.WithCancel(context.Background())
	dm := newDiscoveryManager(ctx, cfg)
	log.Println("running prom discovery manager")
	go func() {
		defer func() {
			err := recover()
			if err != nil {
				log.Printf("Error from prometheus: %v", err)
			}
		}()
		err := dm.goRun()
		if err != nil {
			log.Println("failed to run discovery manager")
		}
	}()
	if logTrace == nil {
		logTrace = emptyTrace
	}
	u := &Updater{
		hosts:             map[netip.Addr]hostInfo{},
		cfg:               cfg,
		ctx:               ctx,
		cancel:            cancelFunc,
		manager:           dm,
		logTrace:          logTrace,
		version:           configVersion,
		hostName:          hostName,
		promGroupsClients: map[*rpc.HandlerContext]PromGroupsClient{},
	}
	go func(u *Updater) {
	loop:
		for {
			select {
			case <-ctx.Done():
				break loop
			case groups := <-dm.SyncCh():
				u.UpdateGroups(groups)
			}
		}
	}(u)
	return u
}

func (u *Updater) UpdateGroups(m map[string][]*targetgroup.Group) {
	u.promMx.Lock()
	u.groups = m
	newHosts := u.onPromUpdatedLocked()
	u.promMx.Unlock()

	u.mx.Lock()
	defer u.mx.Unlock()
	u.hosts = newHosts
	u.logTrace("new targets: %s", newHosts)
	u.broadcastGroups()
}

func (u *Updater) ApplyConfigFromJournal(configString string, version int64) {
	fmt.Println("new version:", version)
	cfg, err := config.Load(configString, false, promlog.NewLogfmtLogger(os.Stdout))
	if err != nil {
		u.logTrace("failed to load prom config: %s", err)
		return
	}
	u.ApplyConfig(cfg)
}

func (u *Updater) ApplyConfig(cfg *config.Config) {
	u.promMx.Lock()
	u.cfg = cfg
	err := u.manager.applyConfig(cfg)
	if err != nil {
		log.Println(err.Error())
	}
	u.promMx.Unlock()
}

func (u *Updater) HandleGetTargets(_ context.Context, hctx *rpc.HandlerContext, args tlstatshouse.GetTargets2Bytes) (err error) {
	u.logTrace("handle get groups request host: '%s', hashJobToLabels: '%s'", args.PromHostName, args.OldHash)
	var ap netip.AddrPort
	ap, err = netip.ParseAddrPort(hctx.RemoteAddr().String())
	if err != nil {
		return rpc.Error{
			Code:        data_model.RPCErrorScrapeAgentIP,
			Description: "scrape agent must have an IP address",
		}
	}
	userHash := string(args.OldHash)
	userHost := string(args.PromHostName)
	u.mx.RLock()
	defer u.mx.RUnlock()
	info := u.hosts[ap.Addr()]
	if userHash != info.hash {
		u.logTrace("handle get groups request host: '%s', hashJobToLabels: '%s' was responsed", args.PromHostName, args.OldHash)
		hctx.Response, err = args.WriteResult(hctx.Response, info.result)
		return err
	}
	u.groupsClientsMu.Lock()
	defer u.groupsClientsMu.Unlock()
	u.promGroupsClients[hctx] = PromGroupsClient{
		args: args,
		host: userHost,
		hash: userHash,
		addr: ap.Addr(),
	}
	u.logTrace("handle get groups request host: '%s', hashJobToLabels: '%s' was hijacked", args.PromHostName, args.OldHash)
	return hctx.HijackResponse(u)
}

func (h *Updater) CancelHijack(hctx *rpc.HandlerContext) {
	h.groupsClientsMu.Lock()
	defer h.groupsClientsMu.Unlock()
	delete(h.promGroupsClients, hctx)
}

func (u *Updater) broadcastGroups() {
	u.groupsClientsMu.Lock()
	defer u.groupsClientsMu.Unlock()
	log.Println("starting broadcast groups")
	u.logTrace("updates: %s", u.hosts)
	sendLongPollSuccessCount := 0
	sendLongPollFailedCount := 0

	for hctx, client := range u.promGroupsClients {
		userHash := client.args.OldHash
		info := u.hosts[client.addr]
		if string(userHash) == info.hash {
			continue
		}
		delete(u.promGroupsClients, hctx)
		var err error
		hctx.Response, err = client.args.WriteResult(hctx.Response, info.result)
		if err == nil {
			sendLongPollSuccessCount++
		} else {
			sendLongPollFailedCount++
		}
		hctx.SendHijackedResponse(err)
	}
	u.logMap(map[string]interface{}{
		"send_long_poll_groups_success_count": sendLongPollSuccessCount,
		"send_long_poll_groups_failed_count":  sendLongPollFailedCount,
		"groups_long_poll_queue":              len(u.promGroupsClients),
	})
}

func statshousePromTargetBytesFromScrapeConfig(scfg *config.ScrapeConfig) tlstatshouse.PromTargetBytes {
	scrapeInterval := scfg.ScrapeInterval
	honorTimestamps := scfg.HonorTimestamps
	honorLabels := scfg.HonorLabels
	scrapeTimeout := scfg.ScrapeTimeout
	bodySizeLimit := int64(scfg.BodySizeLimit)
	labelLimit := int64(scfg.LabelLimit)
	httpConfigStr, _ := yaml.Marshal(scfg.HTTPClientConfig)
	if labelLimit < 0 {
		labelLimit = 0
	}
	labelNameLengthLimit := int64(scfg.LabelNameLengthLimit)
	if labelNameLengthLimit < 0 {
		labelNameLengthLimit = 0
	}
	labelValueLengthLimit := int64(scfg.LabelValueLengthLimit)
	if labelValueLengthLimit < 0 {
		labelValueLengthLimit = 0
	}
	target := tlstatshouse.PromTargetBytes{
		JobName:               []byte(scfg.JobName),
		Url:                   nil,
		Labels:                nil,
		ScrapeInterval:        int64(scrapeInterval),
		ScrapeTimeout:         int64(scrapeTimeout),
		BodySizeLimit:         bodySizeLimit,
		LabelLimit:            labelLimit,
		LabelNameLengthLimit:  labelNameLengthLimit,
		LabelValueLengthLimit: labelValueLengthLimit,
		HttpClientConfig:      httpConfigStr,
	}
	target.SetHonorTimestamps(honorTimestamps)
	target.SetHonorLabels(honorLabels)
	return target
}

// todo make test
func (u *Updater) onPromUpdatedLocked() map[netip.Addr]hostInfo {
	m := u.groups
	cfg := u.cfg
	cfgs := map[string]*config.ScrapeConfig{}
	for _, sc := range cfg.ScrapeConfigs {
		cfgs[sc.JobName] = sc
	}
	hosts := make(map[netip.Addr][]tlstatshouse.PromTargetBytes)
	for jobName, groups := range m {
		scfg, ok := cfgs[jobName]
		if !ok {
			continue
		}
		for _, group := range groups {
			sortedLabels := sortedLabels(group.Labels)
			targets, _ := scrape.TargetsFromGroup(group, scfg)

			for _, target := range targets {
				url := target.URL()
				host, err := netip.ParseAddr(url.Hostname())
				if err != nil {
					u.logTrace("scrape target must have an IP address: %v", err)
					continue
				}
				target := statshousePromTargetBytesFromScrapeConfig(scfg)
				target.Labels = sortedLabels
				target.Url = []byte(url.String())
				hosts[host] = append(hosts[host], target)
			}
		}
	}
	result := make(map[netip.Addr]hostInfo)
	var buf []byte
	for host, info := range hosts {
		info := sortedTargets(info)
		hastStr, hashBytes := hash(info, &buf)
		resBytes := tlstatshouse.GetTargetsResultBytes{
			Targets: info,
			Hash:    hashBytes,
		}
		result[host] = hostInfo{
			hash:   hastStr,
			result: resBytes,
		}
	}
	return result
}

func sortedTargets(targets []tlstatshouse.PromTargetBytes) []tlstatshouse.PromTargetBytes {
	sort.Slice(targets, func(i, j int) bool {
		compareResult := bytes.Compare(targets[i].JobName, targets[j].JobName)
		if compareResult != 0 {
			return compareResult < 0
		}
		return bytes.Compare(targets[i].Url, targets[j].Url) < 0
	})
	return targets
}

func sortedLabels(labels model.LabelSet) (labelsPairs []tl.DictionaryFieldStringBytes) {
	for _, key := range labels {
		labelsPairs = append(labelsPairs, tl.DictionaryFieldStringBytes{Key: []byte(key), Value: []byte(labels[model.LabelName(key)])})
	}
	sort.Slice(labelsPairs, func(i, j int) bool {
		return bytes.Compare(labelsPairs[i].Key, labelsPairs[j].Key) < 0
	})
	return labelsPairs
}

func (u *Updater) logMap(m map[string]interface{}) {
	u.logTrace("%s", m)
}

func hash(groups []tlstatshouse.PromTargetBytes, buf *[]byte) (str string, bytes []byte) {
	result := tlstatshouse.GetTargetsResultBytes{
		Targets: groups,
	}
	var err error
	*buf, err = result.WriteBoxed((*buf)[:0], 0)
	if err != nil {
		return
	}
	hashBytes := hashFunc(*buf)
	return hashStringRepr(hashBytes[:]), hashBytes[:]
}
