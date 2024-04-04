// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package aggregator

import (
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/binary"
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
	"github.com/vkcom/statshouse/internal/agent"
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tl"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
	"gopkg.in/yaml.v2"
)

type scrapeServer struct {
	ctx context.Context
	man *discovery.Manager

	// set to not nil some time after launching aggregator
	sh2  *agent.Agent
	meta format.MetaStorageInterface

	// updated on "applyConfig"
	config   ScrapeConfig
	configJ  map[string]*config.ScrapeConfig
	configH  int32 // config SHA1 hash
	configMu sync.RWMutex

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
	items      []namespaceScrapeConfigItem
	knownTags  map[string]string
	knownTagsG map[int32]map[string]string // group ID -> known tags
}
type namespaceScrapeConfigItem struct {
	Options scrapeOptions          `yaml:"statshouse"`
	Globals *config.GlobalConfig   `yaml:"global,omitempty"`
	Items   []*config.ScrapeConfig `yaml:"scrape_configs,omitempty"`
}

type scrapeOptions struct {
	Namespace scrapeOptionsG   `yaml:"namespace"`
	Groups    []scrapeOptionsG `yaml:"groups,omitempty"`
}

type scrapeOptionsG struct {
	Name      string            `yaml:"name"`
	KnownTags map[string]string `yaml:"known_tags,omitempty"` // tag name -> tag ID
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
		namespaceName := v.Options.Namespace.Name
		if namespaceName == "" {
			return nil, fmt.Errorf("scrape namespace not set")
		}
		namespace := meta.GetNamespaceByName(namespaceName)
		if namespace == nil {
			return nil, fmt.Errorf("scrape namespace not found %q", v.Options.Namespace)
		}
		if namespace.ID == format.BuiltinNamespaceIDDefault {
			return nil, fmt.Errorf("scrape namespace can not be __default")
		}
		var knownTagsG map[int32]map[string]string
		for _, g := range v.Options.Groups {
			groupName := namespaceName + format.NamespaceSeparator + g.Name
			group := meta.GetGroupByName(groupName)
			if group == nil {
				return nil, fmt.Errorf("group not found %q", groupName)
			}
			if group.ID == format.BuiltinGroupIDDefault {
				return nil, fmt.Errorf("scrape group can not be __default")
			}
			if len(g.KnownTags) != 0 {
				if knownTagsG == nil {
					knownTagsG = make(map[int32]map[string]string, len(v.Options.Groups))
				}
				knownTagsG[group.ID] = g.KnownTags
			}
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
			if len(v.Options.Namespace.KnownTags) != 0 {
				if len(ns.knownTags) != 0 {
					return nil, fmt.Errorf("known tags set twice for namespace %q", v.Options.Namespace)
				}
				ns.knownTags = v.Options.Namespace.KnownTags
			}
			if len(knownTagsG) != 0 {
				if len(ns.knownTagsG) != 0 {
					return nil, fmt.Errorf("group known tags set twice for namespace %q", v.Options.Namespace)
				}
				ns.knownTagsG = knownTagsG
			}
			ns.items = append(ns.items, v)
		} else {
			ns := &namespaceScrapeConfig{
				knownTags:  v.Options.Namespace.KnownTags,
				knownTagsG: knownTagsG,
				items:      []namespaceScrapeConfigItem{v},
			}
			res[namespace.ID] = ns
		}
	}
	return res, err
}

func newScrapeServer() *scrapeServer {
	ctx, cancel := context.WithCancel(context.Background())
	res := &scrapeServer{
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
		namespace := model.LabelValue(v.items[0].Options.Namespace.Name)
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
			return
		}
		// config applied successfully, update hash
		h := sha1.Sum([]byte(configS))
		s.configH = int32(binary.BigEndian.Uint32(h[:]))
	}()
	if len(dc) == 0 {
		// "discovery.Manager" does not send updates on empty config, trigger manually
		s.applyTargets(nil)
	}
}

func (s *scrapeServer) getConfig() ScrapeConfig {
	s.configMu.RLock()
	defer s.configMu.RUnlock()
	return s.config
}

func (s *scrapeServer) reportConfigHash() {
	var v int32
	s.configMu.RLock()
	v = s.configH
	s.configMu.RUnlock()
	s.sh2.AddCounterHostStringBytes(
		s.sh2.AggKey(0, format.BuiltinMetricIDAggScrapeConfigHash, [format.MaxTags]int32{0, v}),
		nil, 1, 0, nil)
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
	if s.sh2 != nil {
		if err != nil {
			// failure, targets_sent
			s.sh2.AddCounterHostStringBytes(
				s.sh2.AggKey(0, format.BuiltinMetricIDAggScrapeTargetDispatch, [format.MaxTags]int32{0, 1, 2}),
				[]byte(req.addr.String()), 1, 0, nil)
		} else {
			// success, targets_sent
			s.sh2.AddCounterHostStringBytes(
				s.sh2.AggKey(0, format.BuiltinMetricIDAggScrapeTargetDispatch, [format.MaxTags]int32{0, 0, 2}),
				[]byte(req.addr.String()), 1, 0, nil)
		}
	}
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
	s.configMu.RLock()
	cfg = s.configJ
	s.configMu.RUnlock()
	// build targets
	m := make(map[netip.Addr]*tlstatshouse.GetTargetsResultBytes)
	for namespaceJobName, groups := range jobs {
		if s.sh2 != nil {
			for _, g := range groups {
				if g == nil || len(g.Targets) == 0 {
					log.Printf("scrape group is empty %q\n", namespaceJobName)
					continue
				}
				for _, t := range g.Targets {
					if len(t) == 0 {
						log.Printf("scrape target is empty %q\n", namespaceJobName)
						continue
					}
					if addr, ok := t[model.AddressLabel]; ok {
						s.sh2.AddCounterHostStringBytes(
							s.sh2.AggKey(0, format.BuiltinMetricIDAggScrapeTargetDiscovery, [format.MaxTags]int32{}),
							[]byte(addr), 1, 0, nil)
					}
				}
			}
		}
		scfg, ok := cfg[namespaceJobName]
		if !ok {
			log.Printf("scrape configuration not found %q\n", namespaceJobName)
			continue
		}
		var namespace string
		if i := strings.Index(namespaceJobName, ":"); i != -1 {
			namespace = namespaceJobName[:i]
		}
		if namespace == "" {
			log.Printf("scrape namespace is empty %q\n", namespaceJobName)
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
					if s.sh2 != nil {
						// failure, targets_ready
						s.sh2.AddCounterHostStringBytes(
							s.sh2.AggKey(0, format.BuiltinMetricIDAggScrapeTargetDispatch, [format.MaxTags]int32{0, 1, 1}),
							[]byte(url.Hostname()), 1, 0, nil)
					}
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
		if s.sh2 != nil {
			// success, targets_ready
			s.sh2.AddCounterHostStringBytes(
				s.sh2.AggKey(0, format.BuiltinMetricIDAggScrapeTargetDispatch, [format.MaxTags]int32{0, 0, 1}),
				[]byte(k.String()), 1, 0, nil)
		}
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
	if meta.NamespaceID == 0 ||
		meta.NamespaceID == format.BuiltinNamespaceIDDefault ||
		meta.NamespaceID == format.BuiltinNamespaceIDMissing {
		return 0
	}
	config := c[meta.NamespaceID]
	if config == nil {
		return 0
	}
	var n int
	if len(config.knownTags) != 0 {
		n = publishDraftTags(meta, config.knownTags)
	}
	if len(config.knownTagsG) == 0 ||
		meta.GroupID == 0 ||
		meta.GroupID == format.BuiltinGroupIDDefault {
		return n
	}
	if v := config.knownTagsG[meta.GroupID]; len(v) != 0 {
		return n + publishDraftTags(meta, v)
	}
	return n
}

func publishDraftTags(meta *format.MetricMetaValue, knownTags map[string]string) int {
	var n int
	for k, v := range meta.TagsDraft {
		tagID, ok := knownTags[k]
		if !ok || tagID == "" {
			continue
		}
		if tagID == format.StringTopTagID {
			if meta.StringTopName == "" {
				meta.StringTopName = v.Name
				meta.StringTopDescription = v.Description
				n++
			}
		} else if x := format.TagIndex(tagID); 0 <= x && x < format.MaxTags && meta.Tags[x].Name == "" {
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
