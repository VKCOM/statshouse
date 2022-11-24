// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package main

import (
	"encoding/hex"
	"sync"
	"time"

	"github.com/vkcom/statshouse/internal/agent"
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/mapping"
	"github.com/vkcom/statshouse/internal/metajournal"
	"github.com/vkcom/statshouse/internal/pcache"
)

const (
	metricMapQueueSize = 1000
)

type worker struct {
	sh2           *agent.Agent
	metricStorage *metajournal.MetricsStorage
	mapper        *mapping.Mapper
	logPackets    func(format string, args ...interface{})

	floodTimeMu            sync.Mutex
	floodTimeHandlePkgFail time.Time
}

func startWorker(sh2 *agent.Agent, metricStorage *metajournal.MetricsStorage, pmcLoader pcache.LoaderFunc, dc *pcache.DiskCache, ac *mapping.AutoCreate, suffix string, logPackets func(format string, args ...interface{})) *worker {
	w := &worker{
		sh2:           sh2,
		metricStorage: metricStorage,
		logPackets:    logPackets,
	}
	w.mapper = mapping.NewMapper(suffix, pmcLoader, dc, ac, metricMapQueueSize, w.handleMappedMetricUnlocked)
	return w
}

func (w *worker) wait() {
	w.mapper.Stop()
}

func (w *worker) HandleMetrics(m *tlstatshouse.MetricBytes, cb mapping.MapCallbackFunc) (h data_model.MappedMetricHeader, done bool) {
	if w.logPackets != nil {
		w.logPackets("Parsed metric: %s\n", m.String())
	}
	h, done = w.mapper.Map(m, w.metricStorage.GetMetaMetricByNameBytes(m.Name), cb)
	if done {
		if w.logPackets != nil {
			w.printMetric("cached", *m, h)
		}
		w.sh2.ApplyMetric(*m, h, format.TagValueIDSrcIngestionStatusOKCached)
	}
	return h, done
}

func (w *worker) handleMappedMetricUnlocked(m tlstatshouse.MetricBytes, h data_model.MappedMetricHeader) {
	if w.logPackets != nil {
		w.printMetric("uncached", m, h)
	}
	w.sh2.ApplyMetric(m, h, format.TagValueIDSrcIngestionStatusOKUncached)
}

func (w *worker) HandleParseError(pkt []byte, err error) {
	if w.logPackets != nil {
		w.logPackets("Error parsing packet: %v", err)
		return
	}
	w.floodTimeMu.Lock()
	defer w.floodTimeMu.Unlock()

	if now := time.Now(); now.Sub(w.floodTimeHandlePkgFail) > 60*time.Second {
		w.floodTimeHandlePkgFail = now

		logErr.Printf("Error parsing packet %x: %v", hex.EncodeToString(pkt), err)
	}
}

func (w *worker) printMetric(cachedString string, m tlstatshouse.MetricBytes, h data_model.MappedMetricHeader) {
	if w.logPackets != nil {
		if err := mapping.MapErrorFromHeader(m, h); err != nil {
			w.logPackets("Error mapping metric (%s): %v\n    %#s\n    %#v\n", cachedString, err, m.String(), h)
		} else {
			w.logPackets("Mapped metric (%s): %#s\n    %#v\n", cachedString, m, h)
		}
	}
}
