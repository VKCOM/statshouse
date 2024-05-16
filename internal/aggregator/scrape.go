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
	"encoding/json"
	"fmt"
	"log"
	"net/netip"
	"net/url"
	"sort"
	"sync"

	klog "github.com/go-kit/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/discovery"
	_ "github.com/prometheus/prometheus/discovery/consul"
	"github.com/prometheus/prometheus/discovery/targetgroup"
	"github.com/vkcom/statshouse/internal/agent"
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tl"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/metajournal"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
	"go.uber.org/atomic"
	"gopkg.in/yaml.v2"
)

// region scrapeServer

type scrapeServer struct {
	config  *scrapeStaticConfigGenerator // runs on a single aggretator (shard 1, replica 1)
	configH atomic.Int32                 // configuration string SHA1 hash

	// set on "run"
	sh2     *agent.Agent // used to report statistics
	running bool         // guard against double "run"

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

func newScrapeServer(shard, replica int32) scrapeServer {
	var config *scrapeStaticConfigGenerator
	if shard == 1 && replica == 1 {
		config = newScrapeConfigService()
	}
	return scrapeServer{
		config:   config,
		targets:  make(map[netip.Addr]tlstatshouse.GetTargetsResultBytes),
		requests: make(map[*rpc.HandlerContext]scrapeRequest),
	}
}

func (s *scrapeServer) run(meta *metajournal.MetricsStorage, journal *metajournal.MetricMetaLoader, sh2 *agent.Agent) {
	if s.running {
		return
	}
	if s.config != nil {
		s.config.run(meta, journal, sh2)
	}
	s.sh2 = sh2
	s.running = true
}

func (s *scrapeServer) applyConfig(configID int32, configS string) {
	switch configID {
	case format.PrometheusConfigID:
		if s.config != nil {
			s.config.applyConfig(configS)
		}
		return
	case format.PrometheusGeneratedConfigID:
		break
	default:
		return
	}
	// build targets
	cs, err := DeserializeScrapeStaticConfig([]byte(configS))
	if err != nil {
		return
	}
	targets := make(map[netip.Addr]*tlstatshouse.GetTargetsResultBytes)
	for _, c := range cs {
		var gaugeMetrics [][]byte
		if len(c.Options.GaugeMetrics) != 0 {
			gaugeMetrics = make([][]byte, len(c.Options.GaugeMetrics))
			for i, v := range c.Options.GaugeMetrics {
				gaugeMetrics[i] = []byte(v)
			}
		}
		tags := []tl.DictionaryFieldStringBytes{{
			Key:   []byte(format.ScrapeNamespaceTagName),
			Value: []byte(c.Options.Namespace),
		}}
		for _, j := range c.Jobs {
			c2 := config.ScrapeConfig{
				JobName:        j.JobName,
				Scheme:         j.Scheme,
				MetricsPath:    j.MetricsPath,
				ScrapeInterval: j.ScrapeInterval,
				ScrapeTimeout:  j.ScrapeTimeout,
			}
			for _, g := range j.Groups {
				for _, t := range g.Targets {
					ipp, err := netip.ParseAddrPort(t)
					if err != nil {
						if s.sh2 != nil {
							// failure, targets_ready
							s.sh2.AddCounterHostStringBytes(
								s.sh2.AggKey(0, format.BuiltinMetricIDAggScrapeTargetDispatch, [format.MaxTags]int32{0, 1, 1}),
								[]byte(t), 1, 0, nil)
						}
						log.Printf("scrape target must have an IP address: %v\n", err)
						continue
					}
					v := targets[ipp.Addr()]
					if v == nil {
						v = &tlstatshouse.GetTargetsResultBytes{GaugeMetrics: gaugeMetrics}
						targets[ipp.Addr()] = v
					}
					v.Targets = append(v.Targets, newPromTargetBytes(&c2, t, tags))
				}
			}
		}
	}
	var buf []byte
	for _, v := range targets {
		sort.Slice(v.Targets, func(i, j int) bool {
			lhs, rhs := v.Targets[i], v.Targets[j]
			compareResult := bytes.Compare(lhs.JobName, rhs.JobName)
			if compareResult != 0 {
				return compareResult < 0
			}
			return bytes.Compare(lhs.Url, rhs.Url) < 0
		})
		v.Hash = nil
		buf = v.WriteBoxed(buf[:0], 0xffffffff)
		sum := sha256.Sum256(buf)
		v.Hash = sum[:]
	}
	// publish targets
	s.targetsMu.Lock()
	for k := range s.targets {
		s.targets[k] = tlstatshouse.GetTargetsResultBytes{}
	}
	for k, v := range targets {
		s.targets[k] = *v
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
	// config applied successfully, update hash
	h := sha1.Sum([]byte(configS))
	s.configH.Store(int32(binary.BigEndian.Uint32(h[:])))
}

func (s *scrapeServer) reportConfigHash() {
	v := s.configH.Load()
	s.sh2.AddCounterHostStringBytes(
		s.sh2.AggKey(0, format.BuiltinMetricIDAggScrapeConfigHash, [format.MaxTags]int32{0, v}),
		nil, 1, 0, nil)
}

func (s *scrapeServer) handleGetTargets(_ context.Context, hctx *rpc.HandlerContext, args tlstatshouse.GetTargets2Bytes) error {
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
	var ok bool
	var res tlstatshouse.GetTargetsResultBytes
	s.targetsMu.RLock()
	if s.targets != nil {
		res, ok = s.targets[req.addr]
	}
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

func newPromTargetBytes(cfg *config.ScrapeConfig, addr string, labels []tl.DictionaryFieldStringBytes) tlstatshouse.PromTargetBytes {
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
	l := url.URL{
		Scheme: cfg.Scheme,
		Host:   addr,
		Path:   cfg.MetricsPath,
	}
	res := tlstatshouse.PromTargetBytes{
		JobName:               []byte(cfg.JobName),
		Url:                   []byte(l.String()),
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

// endregion

// region scrapeStaticConfigGenerator

type scrapeStaticConfigGenerator struct {
	ctx     context.Context
	cancel  func()
	manager *discovery.Manager

	// initialized when "run" called
	storage *metajournal.MetricsStorage
	meta    *metajournal.MetricMetaLoader
	sh2     *agent.Agent

	// updated on "applyConfig"
	config   []scrapeConfigYAML
	configMu sync.RWMutex
}

type scrapeConfigYAML struct {
	Options scrapeStatshouseOptionsYAML `yaml:"statshouse,omitempty"`
	Globals config.GlobalConfig         `yaml:"global,omitempty"`
	Jobs    []*config.ScrapeConfig      `yaml:"scrape_configs,omitempty"`
}

type scrapeStatshouseOptionsYAML struct {
	Namespace    string   `yaml:"namespace"`
	GaugeMetrics []string `yaml:"gauge_metrics,omitempty"`
}

type ScrapeStaticConfig struct {
	Options scrapeStatshouseOptions   `json:"statshouse,omitempty"`
	Globals scrapeStaticConfigGlobals `json:"global,omitempty"`
	Jobs    []scrapeStaticConfigJob   `json:"scrape_configs,omitempty"`
}

type scrapeStatshouseOptions struct {
	Namespace    string   `json:"namespace"`
	GaugeMetrics []string `json:"gauge_metrics,omitempty"`
}

type scrapeStaticConfigGlobals struct {
	ScrapeInterval model.Duration `json:"scrape_interval,omitempty"`
	ScrapeTimeout  model.Duration `json:"scrape_timeout,omitempty"`
}

type scrapeStaticConfigJob struct {
	JobName        string                `json:"job_name"`
	ScrapeInterval model.Duration        `json:"scrape_interval,omitempty"`
	ScrapeTimeout  model.Duration        `json:"scrape_timeout,omitempty"`
	MetricsPath    string                `json:"metrics_path,omitempty"`
	Scheme         string                `json:"scheme,omitempty"`
	Groups         []scrapeStaticConfigG `json:"static_configs,omitempty"`
}

func (j *scrapeStaticConfigJob) UnmarshalJSON(s []byte) error {
	type alias scrapeStaticConfigJob
	j.MetricsPath = config.DefaultScrapeConfig.MetricsPath
	j.Scheme = config.DefaultScrapeConfig.Scheme
	return json.Unmarshal(s, (*alias)(j))
}

type scrapeStaticConfigG struct {
	Targets []string `json:"targets"`
}

func newScrapeConfigService() *scrapeStaticConfigGenerator {
	ctx, cancel := context.WithCancel(context.Background())
	return &scrapeStaticConfigGenerator{
		ctx:     ctx,
		cancel:  cancel,
		manager: discovery.NewManager(ctx, klog.NewLogfmtLogger(log.Writer())),
	}
}

func (s *scrapeStaticConfigGenerator) run(storage *metajournal.MetricsStorage, meta *metajournal.MetricMetaLoader, sh2 *agent.Agent) {
	if s.storage != nil {
		return
	}
	s.storage = storage
	s.meta = meta
	s.sh2 = sh2
	go func() {
		err := s.manager.Run()
		if s.ctx.Err() == nil {
			s.cancel()
			if err != nil {
				log.Printf("error running discovery manager: %v\n", err)
			}
		}
	}()
	go func() {
		for {
			select {
			case t := <-s.manager.SyncCh():
				s.applyTargets(t)
			case <-s.ctx.Done():
				return
			}
		}
	}()
	log.Println("running service discovery")
}

func (s *scrapeStaticConfigGenerator) getConfig() []scrapeConfigYAML {
	s.configMu.RLock()
	defer s.configMu.RUnlock()
	return s.config
}

func (s *scrapeStaticConfigGenerator) applyConfig(configStr string) error {
	if s.ctx.Err() != nil {
		return s.ctx.Err()
	}
	if s.storage == nil {
		return fmt.Errorf("error loading scrape config: MetaStorageInterface not set")
	}
	config, err := DeserializeScrapeConfig([]byte(configStr), s.storage)
	if err != nil {
		return fmt.Errorf("error loading scrape config: %v", err)
	}
	m := make(map[string]discovery.Configs)
	for _, c := range config {
		for _, item := range c.Jobs {
			k := fmt.Sprintf("%s:%s", c.Options.Namespace, item.JobName)
			m[k] = item.ServiceDiscoveryConfigs
		}
	}
	err = func() error { // to use "defer"
		s.configMu.Lock()
		defer s.configMu.Unlock()
		s.config = config
		return s.manager.ApplyConfig(m)
	}()
	if err != nil {
		return err
	}
	if len(m) == 0 {
		// "discovery.Manager" does not send updates on empty config, trigger manually
		s.applyTargets(nil)
	}
	return nil
}

func (s *scrapeStaticConfigGenerator) applyTargets(jobsM map[string][]*targetgroup.Group) {
	// report targets discovered
	if s.sh2 != nil {
		for k, j := range jobsM {
			for _, g := range j {
				if g == nil || len(g.Targets) == 0 {
					log.Printf("scrape group is empty %q\n", k)
					continue
				}
				for _, t := range g.Targets {
					if len(t) == 0 {
						log.Printf("scrape target is empty %q\n", k)
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
	}
	// build static config
	cs := s.getConfig()
	res := make([]ScrapeStaticConfig, 0, len(cs))
	for _, c := range cs {
		var jobs []scrapeStaticConfigJob
		for _, j := range c.Jobs {
			k := c.Options.Namespace + ":" + j.JobName
			sourceTargets := jobsM[k]
			if len(sourceTargets) == 0 {
				continue
			}
			var targets []string
			for _, sourceGroup := range sourceTargets {
				for _, sourceTarget := range sourceGroup.Targets {
					addr := string(sourceTarget[model.AddressLabel])
					if addr != "" {
						targets = append(targets, addr)
					}
				}
			}
			if len(targets) != 0 {
				sort.Strings(targets)
				jobs = append(jobs, scrapeStaticConfigJob{
					JobName:        j.JobName,
					ScrapeInterval: j.ScrapeInterval,
					ScrapeTimeout:  j.ScrapeTimeout,
					MetricsPath:    j.MetricsPath,
					Scheme:         j.Scheme,
					Groups: []scrapeStaticConfigG{{
						Targets: targets,
					}},
				})
			}
		}
		// normalize gauge metric list
		var gaugeMetrics []string
		if len(c.Options.GaugeMetrics) != 0 {
			m := make(map[string]int, len(c.Options.GaugeMetrics))
			for _, v := range c.Options.GaugeMetrics {
				if v != "" {
					m[v]++
				}
			}
			if len(m) != 0 {
				gaugeMetrics = make([]string, 0, len(m))
				for k := range m {
					gaugeMetrics = append(gaugeMetrics, k)
				}
				sort.Strings(gaugeMetrics)
			}
		}
		//--
		res = append(res, ScrapeStaticConfig{
			Options: scrapeStatshouseOptions{
				Namespace:    c.Options.Namespace,
				GaugeMetrics: gaugeMetrics,
			},
			Globals: scrapeStaticConfigGlobals{
				ScrapeInterval: c.Globals.ScrapeInterval,
				ScrapeTimeout:  c.Globals.ScrapeTimeout,
			},
			Jobs: jobs,
		})
	}
	// write static config
	bytes, err := json.Marshal(res)
	if err != nil {
		log.Printf("failed to serialize scrape static config: %v\n", err)
		return
	}
	newData := string(bytes)
	current := s.storage.PromConfigGenerated()
	if newData == current.Data {
		log.Println("scrape static config remains unchanged")
		return
	}
	_, err = s.meta.SaveScrapeStaticConfig(s.ctx, current.Version, newData)
	if err != nil {
		log.Printf("failed to save scrape static config: %v\n", err)
	}
}

func DeserializeScrapeConfig(configBytes []byte, meta format.MetaStorageInterface) ([]scrapeConfigYAML, error) {
	if len(configBytes) == 0 {
		return nil, nil
	}
	var res []scrapeConfigYAML
	err := yaml.UnmarshalStrict(configBytes, &res)
	if err != nil {
		res = make([]scrapeConfigYAML, 1)
		err = yaml.UnmarshalStrict(configBytes, &res[0])
		if err != nil {
			return nil, err
		}
	}
	for _, c := range res {
		if c.Options.Namespace == "" {
			return nil, fmt.Errorf("scrape namespace not set")
		}
		namespace := meta.GetNamespaceByName(c.Options.Namespace)
		if namespace == nil {
			return nil, fmt.Errorf("scrape namespace not found %q", c.Options.Namespace)
		}
		if namespace.ID == format.BuiltinNamespaceIDDefault {
			return nil, fmt.Errorf("scrape namespace can not be __default")
		}
		if c.Globals.ScrapeInterval == 0 {
			c.Globals.ScrapeInterval = config.DefaultGlobalConfig.ScrapeInterval
		}
		if c.Globals.ScrapeTimeout == 0 {
			c.Globals.ScrapeTimeout = config.DefaultGlobalConfig.ScrapeTimeout
		}
		for _, item := range c.Jobs {
			if item.ScrapeInterval == 0 {
				item.ScrapeInterval = c.Globals.ScrapeInterval
			}
			if item.ScrapeTimeout == 0 {
				if c.Globals.ScrapeTimeout > item.ScrapeInterval {
					item.ScrapeTimeout = item.ScrapeInterval
				} else {
					item.ScrapeTimeout = c.Globals.ScrapeTimeout
				}
			}
		}
	}
	return res, nil
}

func DeserializeScrapeStaticConfig(configBytes []byte) ([]ScrapeStaticConfig, error) {
	if len(configBytes) == 0 {
		return nil, nil
	}
	var res []ScrapeStaticConfig
	err := json.Unmarshal(configBytes, &res)
	if err != nil {
		return nil, err
	}
	for _, c := range res {
		if c.Options.Namespace == "" {
			return nil, fmt.Errorf("scrape namespace not set")
		}
		if c.Globals.ScrapeInterval == 0 {
			c.Globals.ScrapeInterval = config.DefaultGlobalConfig.ScrapeInterval
		}
		if c.Globals.ScrapeTimeout == 0 {
			c.Globals.ScrapeTimeout = config.DefaultGlobalConfig.ScrapeTimeout
		}
		for _, item := range c.Jobs {
			if item.ScrapeInterval == 0 {
				item.ScrapeInterval = c.Globals.ScrapeInterval
			}
			if item.ScrapeTimeout == 0 {
				if c.Globals.ScrapeTimeout > item.ScrapeInterval {
					item.ScrapeTimeout = item.ScrapeInterval
				} else {
					item.ScrapeTimeout = c.Globals.ScrapeTimeout
				}
			}
		}
	}
	return res, nil
}

// endregion
