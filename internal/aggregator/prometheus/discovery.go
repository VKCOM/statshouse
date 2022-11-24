// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package prometheus

import (
	"os"
	"sync"

	"github.com/prometheus/prometheus/discovery/targetgroup"

	"context"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/discovery"
)

type discoveryManager struct {
	mx      sync.Mutex
	cfg     *config.Config
	manager *discovery.Manager
}

func newDiscoveryManager(parentCtx context.Context, cfg *config.Config) *discoveryManager {
	// todo fix logs
	discoveryManagerScrape := discovery.NewManager(parentCtx,
		log.NewLogfmtLogger(os.Stdout),
		discovery.Name("scrape"))

	return &discoveryManager{
		cfg:     cfg,
		manager: discoveryManagerScrape,
	}
}

func (m *discoveryManager) goRun() error {
	err := m.applyConfig(m.cfg)
	if err != nil {
		return err
	}
	return m.manager.Run()
}

func (m *discoveryManager) applyConfig(cfg *config.Config) error {
	m.mx.Lock()
	m.cfg = cfg
	m.mx.Unlock()
	configs := make(map[string]discovery.Configs)
	for _, scrapeConfig := range cfg.ScrapeConfigs {
		configs[scrapeConfig.JobName] = scrapeConfig.ServiceDiscoveryConfigs
	}
	err := m.manager.ApplyConfig(configs)
	if err != nil {
		return err
	}
	return nil
}

func (m *discoveryManager) SyncCh() <-chan map[string][]*targetgroup.Group {
	return m.manager.SyncCh()
}
