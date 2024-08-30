package aggregator

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"

	klog "github.com/go-kit/log"
	prometheus "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/discovery"
	"github.com/prometheus/prometheus/discovery/consul"
	"github.com/prometheus/prometheus/discovery/targetgroup"

	"github.com/vkcom/statshouse/internal/agent"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/metajournal"
)

type scrapeDiscovery struct {
	ctx     context.Context
	cancel  func()
	manager *discovery.Manager

	// initialized when "run" called
	storage *metajournal.MetricsStorage
	meta    *metajournal.MetricMetaLoader
	sh2     *agent.Agent

	// updated on "applyConfig"
	config   []ScrapeConfig
	configMu sync.RWMutex

	// receives config generated
	cb scrapeDiscoveryCallback
}

type scrapeDiscoveryCallback func([]ScrapeConfig)

func newScrapeDiscovery(cb scrapeDiscoveryCallback) scrapeDiscovery {
	ctx, cancel := context.WithCancel(context.Background())
	return scrapeDiscovery{
		ctx:     ctx,
		cancel:  cancel,
		manager: discovery.NewManager(ctx, klog.NewLogfmtLogger(log.Writer())),
		cb:      cb,
	}
}

func (s *scrapeDiscovery) run(storage *metajournal.MetricsStorage, meta *metajournal.MetricMetaLoader, sh2 *agent.Agent) {
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

func (s *scrapeDiscovery) getConfig() []ScrapeConfig {
	s.configMu.RLock()
	defer s.configMu.RUnlock()
	return s.config
}

func (s *scrapeDiscovery) applyConfig(configStr string) error {
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
		for _, j := range c.ScrapeConfigs {
			var dcs discovery.Configs
			for _, cc := range j.ConsulConfigs {
				dc := consul.DefaultSDConfig // copy
				dc.Server = cc.Server
				dc.Token = prometheus.Secret(cc.Token)
				dc.Datacenter = cc.Datacenter
				dcs = append(dcs, &dc)
			}
			if len(dcs) != 0 {
				k := fmt.Sprintf("%s:%s", c.Options.Namespace, j.JobName)
				m[k] = dcs
			}
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

func (s *scrapeDiscovery) applyTargets(targets map[string][]*targetgroup.Group) {
	// report targets discovered
	if s.sh2 != nil {
		for job, targetGroups := range targets {
			for _, targetGroup := range targetGroups {
				if targetGroup == nil || len(targetGroup.Targets) == 0 {
					log.Printf("scrape group is empty %q\n", job)
					continue
				}
				for _, ls := range targetGroup.Targets {
					if len(ls) == 0 {
						log.Printf("scrape target is empty %q\n", job)
						continue
					}
					if addr, ok := ls[model.AddressLabel]; ok {
						s.sh2.AddCounterHostStringBytes(
							s.sh2.AggKey(0, format.BuiltinMetricIDAggScrapeTargetDiscovery, [format.MaxTags]int32{}),
							[]byte(addr), 1, 0, format.BuiltinMetricMetaAggScrapeTargetDiscovery)
					}
				}
			}
		}
	}
	// build static config
	src := s.getConfig()
	res := make([]ScrapeConfig, 0, len(src))
	for i := range src {
		var jobs []scrapeJobConfig
		for _, job := range src[i].ScrapeConfigs {
			staticConfigs := make([]scrapeGroupConfig, 0, len(job.StaticConfigs))
			staticConfigs = append(staticConfigs, job.StaticConfigs...)
			k := src[i].Options.Namespace + ":" + job.JobName
			for _, group := range targets[k] {
				for _, lset := range group.Targets {
					if _, ok := lset[model.AddressLabel]; !ok {
						continue
					}
					// remove empty labels
					for k, v := range lset {
						if v == "" {
							delete(lset, k)
						}
					}
					// add group labels
					for k, v := range group.Labels {
						if v != "" {
							lset[k] = v
						}
					}
					// add target
					if len(lset) != 0 {
						staticConfigs = append(staticConfigs, scrapeGroupConfig{Labels: lset})
					}
				}
			}
			if len(staticConfigs) != 0 {
				jobs = append(jobs, scrapeJobConfig{
					JobName:              job.JobName,
					ScrapeInterval:       job.ScrapeInterval,
					ScrapeTimeout:        job.ScrapeTimeout,
					MetricsPath:          job.MetricsPath,
					Scheme:               job.Scheme,
					RelabelConfigs:       job.RelabelConfigs,
					MetricRelabelConfigs: job.MetricRelabelConfigs,
					StaticConfigs:        staticConfigs,
				})
			}
		}
		// normalize gauge metric list
		var gaugeMetrics []string
		if len(src[i].Options.GaugeMetrics) != 0 {
			m := make(map[string]int, len(src[i].Options.GaugeMetrics))
			for _, v := range src[i].Options.GaugeMetrics {
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
		res = append(res, ScrapeConfig{
			Options: scrapeOptions{
				Namespace:    src[i].Options.Namespace,
				GaugeMetrics: gaugeMetrics,
			},
			GlobalConfig:  src[i].GlobalConfig,
			ScrapeConfigs: jobs,
		})
	}
	// --
	s.cb(res)
}
