// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package aggregator

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"sync"

	klog "github.com/go-kit/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/discovery"
	"github.com/prometheus/prometheus/discovery/targetgroup"
	"github.com/vkcom/statshouse/internal/agent"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/metajournal"
	"gopkg.in/yaml.v2"
)

type scrapeConfigService struct {
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
	Namespace string `yaml:"namespace"`
}

type ScrapeStaticConfig struct {
	Options scrapeStatshouseOptions   `json:"statshouse,omitempty"`
	Globals scrapeStaticConfigGlobals `json:"global,omitempty"`
	Jobs    []scrapeStaticConfigJob   `json:"scrape_configs,omitempty"`
}

type scrapeStatshouseOptions struct {
	Namespace string `json:"namespace"`
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

func newScrapeConfigService() *scrapeConfigService {
	ctx, cancel := context.WithCancel(context.Background())
	return &scrapeConfigService{
		ctx:     ctx,
		cancel:  cancel,
		manager: discovery.NewManager(ctx, klog.NewLogfmtLogger(log.Writer())),
	}
}

func (s *scrapeConfigService) run(storage *metajournal.MetricsStorage, meta *metajournal.MetricMetaLoader, sh2 *agent.Agent) {
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

func (s *scrapeConfigService) getConfig() []scrapeConfigYAML {
	s.configMu.RLock()
	defer s.configMu.RUnlock()
	return s.config
}

func (s *scrapeConfigService) applyConfig(configStr string) error {
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

func (s *scrapeConfigService) applyTargets(jobsM map[string][]*targetgroup.Group) {
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
		res = append(res, ScrapeStaticConfig{
			Options: scrapeStatshouseOptions{
				Namespace: c.Options.Namespace,
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
