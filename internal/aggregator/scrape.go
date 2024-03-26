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
	_ "github.com/prometheus/prometheus/discovery/consul"
	"github.com/prometheus/prometheus/discovery/targetgroup"
	"github.com/prometheus/prometheus/scrape"
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tl"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
	"gopkg.in/yaml.v2"
)

type scrapeServer struct {
	ctx  context.Context
	man  *discovery.Manager
	meta format.MetaStorageInterface

	// updated on "applyConfig"
	config   ScrapeConfig
	configJ  map[string]*config.ScrapeConfig
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

type ScrapeConfig map[int32]*namespaceScrapeConfig // by namespace ID

type namespaceScrapeConfig struct {
	items     []namespaceScrapeConfigItem
	knownTags map[string]string
}
type namespaceScrapeConfigItem struct {
	Options scrapeOptions          `yaml:"statshouse"`
	Globals *config.GlobalConfig   `yaml:"global,omitempty"`
	Items   []*config.ScrapeConfig `yaml:"scrape_configs,omitempty"`
}

type scrapeOptions struct {
	Namespace string            `yaml:"namespace"`
	KnownTags map[string]string `yaml:"known_tags,omitempty"` // tag name -> tag index
}

func LoadScrapeConfig(configS string, meta format.MetaStorageInterface) (ScrapeConfig, error) {
	var s []namespaceScrapeConfigItem
	err := yaml.UnmarshalStrict([]byte(configS), &s)
	if err != nil {
		s = make([]namespaceScrapeConfigItem, 1)
		err = yaml.UnmarshalStrict([]byte(configS), &s[0])
		if err != nil {
			return nil, err
		}
	}
	res := make(map[int32]*namespaceScrapeConfig)
	for _, v := range s {
		if v.Options.Namespace == "" {
			return nil, fmt.Errorf("scrape namespace not set")
		}
		namespace := meta.GetNamespaceByName(v.Options.Namespace)
		if namespace == nil {
			return nil, fmt.Errorf("scrape namespace not found %q", v.Options.Namespace)
		}
		if namespace.ID == format.BuiltinNamespaceIDDefault {
			return nil, fmt.Errorf("scrape namespace can not be __default")
		}
		gc := v.Globals
		if gc.ScrapeInterval == 0 {
			gc.ScrapeInterval = config.DefaultGlobalConfig.ScrapeInterval
		}
		if gc.ScrapeTimeout == 0 {
			gc.ScrapeTimeout = config.DefaultGlobalConfig.ScrapeTimeout
		}
		for _, sc := range v.Items {
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
		if ns := res[namespace.ID]; ns != nil {
			if len(ns.knownTags) != 0 && len(v.Options.KnownTags) != 0 {
				return nil, fmt.Errorf("known tags set twice for namespace %q", v.Options.Namespace)
			}
			ns.items = append(ns.items, v)
		} else {
			ns := &namespaceScrapeConfig{
				knownTags: v.Options.KnownTags,
				items:     []namespaceScrapeConfigItem{v},
			}
			res[namespace.ID] = ns
		}
	}
	return res, err
}

func newScrapeServer(meta format.MetaStorageInterface) *scrapeServer {
	ctx, cancel := context.WithCancel(context.Background())
	res := &scrapeServer{
		ctx:      ctx,
		man:      discovery.NewManager(ctx, klog.NewLogfmtLogger(log.Writer())),
		meta:     meta,
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

func (s *scrapeServer) applyConfig(configS string) {
	if s.ctx.Err() != nil {
		return
	}
	if s.meta == nil {
		log.Println("error loading scrape config: MetaStorageInterface not set")
		return
	}
	cs, err := LoadScrapeConfig(configS, s.meta)
	if err != nil {
		log.Printf("error loading scrape config: %v\n", err)
		return
	}
	sc := make(map[string]*config.ScrapeConfig)
	dc := make(map[string]discovery.Configs)
	for _, v := range cs {
		if len(v.items) == 0 {
			continue
		}
		namespace := model.LabelValue(v.items[0].Options.Namespace)
		for _, v1 := range v.items {
			for _, v2 := range v1.Items {
				namespaceJobName := fmt.Sprintf("%s:%s", namespace, v2.JobName)
				sc[namespaceJobName] = v2
				dc[namespaceJobName] = v2.ServiceDiscoveryConfigs
			}
		}
	}
	func() { // to use "defer"
		s.configMu.Lock()
		defer s.configMu.Unlock()
		s.config = cs
		s.configJ = sc
		if err = s.man.ApplyConfig(dc); err != nil {
			log.Printf("error applying scrape config: %v\n", err)
		}
	}()
	if len(dc) == 0 {
		// "discovery.Manager" does not send updates on empty config, trigger manually
		s.applyTargets(nil)
	}
}

func (s *scrapeServer) getConfig() ScrapeConfig {
	s.configMu.Lock()
	defer s.configMu.Unlock()
	return s.config
}

func (s *scrapeServer) handleGetTargets(_ context.Context, hctx *rpc.HandlerContext, args tlstatshouse.GetTargets2Bytes) error {
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

func (s *scrapeServer) tryGetNewTargetsAndWriteResult(req scrapeRequest) (done bool, err error) {
	s.targetsMu.RLock()
	res, ok := s.targets[req.addr]
	s.targetsMu.RUnlock()
	if !ok || bytes.Equal(req.args.OldHash, res.Hash) {
		return false, nil
	}
	req.hctx.Response, err = req.args.WriteResult(req.hctx.Response, res)
	return true, err
}

func (s *scrapeServer) CancelHijack(hctx *rpc.HandlerContext) {
	s.requestsMu.Lock()
	defer s.requestsMu.Unlock()
	delete(s.requests, hctx)
}

func (s *scrapeServer) applyTargets(jobs map[string][]*targetgroup.Group) {
	// get config
	var cfg map[string]*config.ScrapeConfig
	s.configMu.Lock()
	cfg = s.configJ
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
	for k := range s.targets {
		s.targets[k] = tlstatshouse.GetTargetsResultBytes{}
	}
	for k, v := range targets {
		s.targets[k] = v
	}
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

func (c ScrapeConfig) PublishDraftTags(meta *format.MetricMetaValue) int {
	if meta.NamespaceID == format.BuiltinNamespaceIDDefault || meta.NamespaceID == format.BuiltinNamespaceIDMissing {
		return 0
	}
	config := c[meta.NamespaceID]
	if config == nil {
		return 0
	}
	var n int
	for k, v := range meta.TagsDraft {
		tagID, ok := config.knownTags[k]
		if !ok || tagID == "" {
			continue
		}
		if tagID == format.StringTopTagID {
			if meta.StringTopName == "" {
				meta.StringTopName = v.Name
				meta.StringTopDescription = v.Description
				n++
			}
		} else if x := format.TagIndex(tagID); 0 <= x && x < format.MaxTags {
			meta.Tags[x] = v
			delete(meta.TagsDraft, k)
			n++
		}
	}
	return n
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
