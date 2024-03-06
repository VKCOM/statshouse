// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package aggregator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"log"
	"net/netip"
	"sort"
	"strings"
	"sync"

	klog "github.com/go-kit/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/discovery"
	"github.com/prometheus/prometheus/discovery/targetgroup"
	"github.com/prometheus/prometheus/scrape"
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tl"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
	"gopkg.in/yaml.v2"
)

type ScrapeServer struct {
	ctx context.Context
	man *discovery.Manager

	// updated on "applyConfig"
	config   map[string]*config.ScrapeConfig
	configMu sync.Mutex

	// current targets, updated on discovery events
	targets   map[netip.Addr]tlstatshouse.GetTargetsResultBytes
	targetsMu sync.RWMutex

	// long poll requests
	requests   map[*rpc.HandlerContext]scrapeRequest
	requestsMu sync.Mutex
}

type scrapeRequest struct {
	hctx *rpc.HandlerContext
	addr netip.Addr
	args tlstatshouse.GetTargets2Bytes
}

type ScrapeConfig struct {
	GlobalConfig  scrapeGlobalConfig     `yaml:"global"`
	ScrapeConfigs []*config.ScrapeConfig `yaml:"scrape_configs,omitempty"`
}

type scrapeGlobalConfig struct {
	// copied from Prometheus "config.GlobalConfig"
	ScrapeInterval model.Duration `yaml:"scrape_interval,omitempty"`
	ScrapeTimeout  model.Duration `yaml:"scrape_timeout,omitempty"`

	// scrape target namespace
	Namespace string `yaml:"statshouse_namespace"`
}

func LoadScrapeConfig(configS string) ([]ScrapeConfig, error) {
	var res []ScrapeConfig
	err := yaml.UnmarshalStrict([]byte(configS), &res)
	if err != nil {
		res = make([]ScrapeConfig, 1)
		err = yaml.UnmarshalStrict([]byte(configS), &res[0])
		if err != nil {
			return nil, err
		}
	}
	for i := range res {
		gc := res[i].GlobalConfig
		if gc.ScrapeInterval == 0 {
			gc.ScrapeInterval = config.DefaultGlobalConfig.ScrapeInterval
		}
		if gc.ScrapeTimeout == 0 {
			gc.ScrapeTimeout = config.DefaultGlobalConfig.ScrapeTimeout
		}
		for j := range res[i].ScrapeConfigs {
			sc := res[i].ScrapeConfigs[j]
			if sc.ScrapeInterval == 0 {
				sc.ScrapeInterval = gc.ScrapeInterval
			}
			if sc.ScrapeTimeout == 0 {
				if gc.ScrapeTimeout > sc.ScrapeInterval {
					sc.ScrapeTimeout = sc.ScrapeInterval
				} else {
					sc.ScrapeTimeout = gc.ScrapeTimeout
				}
			}
		}
	}
	return res, err
}

func newScrapeServer() *ScrapeServer {
	ctx, cancel := context.WithCancel(context.Background())
	res := &ScrapeServer{
		ctx:      ctx,
		man:      discovery.NewManager(ctx, klog.NewLogfmtLogger(log.Writer())),
		targets:  make(map[netip.Addr]tlstatshouse.GetTargetsResultBytes),
		requests: make(map[*rpc.HandlerContext]scrapeRequest),
	}
	log.Println("running discovery server")
	go func() {
		err := res.man.Run()
		if ctx.Err() == nil {
			cancel()
			if err != nil {
				log.Printf("error running discovery manager: %v\n", err)
			}
		}
	}()
	go func() {
		for {
			select {
			case t := <-res.man.SyncCh():
				res.applyTargets(t)
			case <-ctx.Done():
				return
			}
		}
	}()
	return res
}

func (s *ScrapeServer) applyConfig(configS string) {
	if s.ctx.Err() != nil {
		return
	}
	cs, err := LoadScrapeConfig(configS)
	if err != nil {
		log.Printf("error loading scrape config: %v\n", err)
		return
	}
	sc := make(map[string]*config.ScrapeConfig)
	dc := make(map[string]discovery.Configs)
	for _, c := range cs {
		if c.GlobalConfig.Namespace == "" {
			continue
		}
		namespace := model.LabelValue(c.GlobalConfig.Namespace)
		for _, v := range c.ScrapeConfigs {
			namespaceJobName := fmt.Sprintf("%s:%s", namespace, v.JobName)
			sc[namespaceJobName] = v
			dc[namespaceJobName] = v.ServiceDiscoveryConfigs
		}
	}
	s.configMu.Lock()
	defer s.configMu.Unlock()
	if err = s.man.ApplyConfig(dc); err != nil {
		log.Printf("error applying scrape config: %v\n", err)
		return
	}
	s.config = sc
}

func (s *ScrapeServer) handleGetTargets(_ context.Context, hctx *rpc.HandlerContext, args tlstatshouse.GetTargets2Bytes) error {
	if s.ctx.Err() != nil {
		return rpc.Error{
			Code:        data_model.RPCErrorScrapeAgentIP,
			Description: "service discovery is not running",
		}
	}
	ipp, err := netip.ParseAddrPort(hctx.RemoteAddr().String())
	if err != nil {
		return rpc.Error{
			Code:        data_model.RPCErrorScrapeAgentIP,
			Description: "scrape agent must have an IP address",
		}
	}
	// fast path, try respond without taking "requestsMu"
	req := scrapeRequest{
		hctx: hctx,
		addr: ipp.Addr(),
		args: args,
	}
	done, err := s.tryGetNewTargetsAndWriteResult(req)
	if done || err != nil {
		return err
	}
	// slow path, take "requestsMu" and try again
	s.requestsMu.Lock()
	defer s.requestsMu.Unlock()
	done, err = s.tryGetNewTargetsAndWriteResult(req)
	if done || err != nil {
		return err
	}
	// long poll, scrape targets will be sent once ready
	err = hctx.HijackResponse(s)
	if rpc.IsHijackedResponse(err) {
		s.requests[hctx] = req
	}
	return err
}

func (s *ScrapeServer) tryGetNewTargetsAndWriteResult(req scrapeRequest) (done bool, err error) {
	s.targetsMu.RLock()
	res, ok := s.targets[req.addr]
	s.targetsMu.RUnlock()
	if !ok || bytes.Equal(req.args.OldHash, res.Hash) {
		return false, nil
	}
	req.hctx.Response, err = req.args.WriteResult(req.hctx.Response, res)
	return true, err
}

func (s *ScrapeServer) CancelHijack(hctx *rpc.HandlerContext) {
	s.requestsMu.Lock()
	defer s.requestsMu.Unlock()
	delete(s.requests, hctx)
}

func (s *ScrapeServer) applyTargets(jobs map[string][]*targetgroup.Group) {
	// get config
	var cfg map[string]*config.ScrapeConfig
	s.configMu.Lock()
	cfg = s.config
	s.configMu.Unlock()
	// build targets
	m := make(map[netip.Addr]*tlstatshouse.GetTargetsResultBytes)
	for namespaceJobName, groups := range jobs {
		scfg, ok := cfg[namespaceJobName]
		if !ok {
			continue
		}
		var namespace string
		if i := strings.Index(namespaceJobName, ":"); i != -1 {
			namespace = namespaceJobName[:i]
		}
		if namespace == "" {
			continue
		}
		for _, group := range groups {
			ls := make([]tl.DictionaryFieldStringBytes, 0, len(group.Labels)+1)
			ls = append(ls, tl.DictionaryFieldStringBytes{Key: []byte(format.ScrapeNamespaceTagName), Value: []byte(namespace)})
			for k, v := range group.Labels {
				if k != format.ScrapeNamespaceTagName {
					ls = append(ls, tl.DictionaryFieldStringBytes{Key: []byte(k), Value: []byte(v)})
				}
			}
			sort.Slice(ls, func(i, j int) bool {
				return bytes.Compare(ls[i].Key, ls[j].Key) < 0
			})
			ts, _ := scrape.TargetsFromGroup(group, scfg)
			for _, t := range ts {
				url := t.URL()
				ip, err := netip.ParseAddr(url.Hostname())
				if err != nil {
					log.Printf("scrape target must have an IP address: %v\n", err)
					continue
				}
				v := m[ip]
				if v == nil {
					v = &tlstatshouse.GetTargetsResultBytes{}
					m[ip] = v
				}
				v.Targets = append(v.Targets, newPromTargetBytes(scfg, []byte(url.String()), ls))
			}
		}
	}
	var buf []byte
	targets := make(map[netip.Addr]tlstatshouse.GetTargetsResultBytes, len(m))
	for k, v := range m {
		sort.Slice(v.Targets, func(i, j int) bool {
			lhs, rhs := v.Targets[i], v.Targets[j]
			compareResult := bytes.Compare(lhs.JobName, rhs.JobName)
			if compareResult != 0 {
				return compareResult < 0
			}
			return bytes.Compare(lhs.Url, rhs.Url) < 0
		})
		var err error
		t := tlstatshouse.GetTargetsResultBytes{Targets: v.Targets}
		buf, err = t.WriteBoxed(buf[:0], 0)
		if err != nil {
			continue
		}
		sum := sha256.Sum256(buf)
		v.Hash = sum[:]
		targets[k] = *v
	}
	// publish targets
	s.targetsMu.Lock()
	s.targets = targets
	s.targetsMu.Unlock()
	// serve long poll requests
	pendingRequests := func() []scrapeRequest {
		s.requestsMu.Lock()
		defer s.requestsMu.Unlock()
		res := make([]scrapeRequest, 0, len(s.requests))
		for _, v := range s.requests {
			res = append(res, v)
		}
		return res
	}()
	for _, v := range pendingRequests {
		done, err := s.tryGetNewTargetsAndWriteResult(v)
		if done {
			v.hctx.SendHijackedResponse(err)
			s.CancelHijack(v.hctx)
		}
	}
}

func newPromTargetBytes(cfg *config.ScrapeConfig, url []byte, labels []tl.DictionaryFieldStringBytes) tlstatshouse.PromTargetBytes {
	scrapeInterval := cfg.ScrapeInterval
	honorTimestamps := cfg.HonorTimestamps
	honorLabels := cfg.HonorLabels
	scrapeTimeout := cfg.ScrapeTimeout
	bodySizeLimit := int64(cfg.BodySizeLimit)
	labelLimit := int64(cfg.LabelLimit)
	httpConfigStr, _ := yaml.Marshal(cfg.HTTPClientConfig)
	if labelLimit < 0 {
		labelLimit = 0
	}
	labelNameLengthLimit := int64(cfg.LabelNameLengthLimit)
	if labelNameLengthLimit < 0 {
		labelNameLengthLimit = 0
	}
	labelValueLengthLimit := int64(cfg.LabelValueLengthLimit)
	if labelValueLengthLimit < 0 {
		labelValueLengthLimit = 0
	}
	res := tlstatshouse.PromTargetBytes{
		JobName:               []byte(cfg.JobName),
		Url:                   url,
		Labels:                labels,
		ScrapeInterval:        int64(scrapeInterval),
		ScrapeTimeout:         int64(scrapeTimeout),
		BodySizeLimit:         bodySizeLimit,
		LabelLimit:            labelLimit,
		LabelNameLengthLimit:  labelNameLengthLimit,
		LabelValueLengthLimit: labelValueLengthLimit,
		HttpClientConfig:      httpConfigStr,
	}
	res.SetHonorTimestamps(honorTimestamps)
	res.SetHonorLabels(honorLabels)
	return res
}
