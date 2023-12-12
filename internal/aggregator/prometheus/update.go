// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package prometheus

import (
	"context"
	"fmt"
	"log"
	"net/netip"
	"os"
	"sync"

	promlog "github.com/go-kit/log"

	"github.com/prometheus/prometheus/config"

	"github.com/vkcom/statshouse/internal/data_model"
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

	// host -> hostInfo
	mx    sync.RWMutex
	hosts map[netip.Addr]hostInfo

	groupsClientsMu   sync.Mutex // Taken after mx
	promGroupsClients map[*rpc.HandlerContext]PromGroupsClient

	logTrace func(format string, a ...interface{})

	hostName string
}

type hostInfo struct {
	hash   string
	result tlstatshouse.GetTargetsResultBytes
}

var emptyTrace = func(format string, a ...interface{}) {}

func RunPromUpdaterAsync(hostName string, logTrace bool) (*Updater, error) {
	var logTraceFunc func(format string, a ...interface{})
	if logTrace {
		logTraceFunc = log.Printf
	}
	cfg := &config.DefaultConfig
	return runUpdater(hostName, cfg, 0, logTraceFunc), nil
}

func runUpdater(hostName string, cfg *config.Config, configVersion int64, logTrace func(format string, a ...interface{})) *Updater {
	if logTrace == nil {
		logTrace = emptyTrace
	}
	u := &Updater{
		hosts:             map[netip.Addr]hostInfo{},
		cfg:               cfg,
		logTrace:          logTrace,
		version:           configVersion,
		hostName:          hostName,
		promGroupsClients: map[*rpc.HandlerContext]PromGroupsClient{},
	}
	return u
}

func (u *Updater) ApplyConfigFromJournal(configString string, version int64) {
	fmt.Println("new version:", version)
	defer func() {
		if r := recover(); r != nil {
			log.Println("panic:", r)
		}
	}()
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
