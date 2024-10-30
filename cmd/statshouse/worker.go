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

	"go4.org/mem"

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
	autoCreate    *mapping.AutoCreate
	logPackets    func(format string, args ...interface{})

	floodTimeMu            sync.Mutex
	floodTimeHandlePkgFail time.Time
}

func startWorker(sh2 *agent.Agent, metricStorage *metajournal.MetricsStorage, pmcLoader pcache.LoaderFunc, dc *pcache.DiskCache, ac *mapping.AutoCreate, suffix string, logPackets func(format string, args ...interface{})) *worker {
	w := &worker{
		sh2:           sh2,
		metricStorage: metricStorage,
		autoCreate:    ac,
		logPackets:    logPackets,
	}
	w.mapper = mapping.NewMapper(suffix, pmcLoader, dc, ac, metricMapQueueSize, w.handleMappedMetricUnlocked)
	return w
}

// func (w *worker) wait() {
//	w.mapper.Stop()
// }

func (w *worker) HandleMetrics(args data_model.HandlerArgs) (h data_model.MappedMetricHeader, done bool) {
	if w.logPackets != nil {
		w.logPackets("Parsed metric: %s\n", args.MetricBytes.String())
	}
	w.fillTime(args, &h)
	metaOk := w.fillMetricMeta(args, &h)
	if metaOk {
		h.Key.Metric = h.MetricMeta.MetricID
		if !h.MetricMeta.Visible {
			h.IngestionStatus = format.TagValueIDSrcIngestionStatusErrMetricInvisible
		}
		done = w.mapper.Map(args, h.MetricMeta, &h)
	} else {
		w.mapper.MapEnvironment(args.MetricBytes, &h)
		done = true
	}

	if done {
		if w.logPackets != nil {
			w.printMetric("cached", *args.MetricBytes, h)
		}
		w.sh2.ApplyMetric(*args.MetricBytes, h, format.TagValueIDSrcIngestionStatusOKCached)
	}
	return h, done
}

func (w *worker) fillTime(args data_model.HandlerArgs, h *data_model.MappedMetricHeader) {
	h.ReceiveTime = time.Now() // receive time is set once for all functions
	// We do not check fields mask in code below, only field value, because
	// sending 0 instead of manipulating field mask is more convenient for many clients
	if args.MetricBytes.Ts != 0 {
		h.Key.Timestamp = args.MetricBytes.Ts
	} else { // newer clients will mark events with explicit timestamp, so this branch must be rare
		h.Key.Timestamp = uint32(h.ReceiveTime.Unix())
	}
}

func (w *worker) fillMetricMeta(args data_model.HandlerArgs, h *data_model.MappedMetricHeader) (ok bool) {
	metric := args.MetricBytes
	metricMeta := w.metricStorage.GetMetaMetricByNameBytes(metric.Name)
	if metricMeta != nil {
		h.MetricMeta = metricMeta
		return true
	}
	metricMeta = format.BuiltinMetricAllowedToReceive[string(metric.Name)]
	if metricMeta != nil {
		h.MetricMeta = metricMeta
		return true
	}

	// TODO: we use possibly invalid and non-normalized string in AutoCreate, which is strange
	if w.autoCreate != nil && format.ValidMetricName(mem.B(metric.Name)) {
		// before normalizing metric.Name so we do not fill auto create data structures with invalid metric names
		_ = w.autoCreate.AutoCreateMetric(metric, args.Description, args.ScrapeInterval, h.ReceiveTime)
	}
	validName, err := format.AppendValidStringValue(metric.Name[:0], metric.Name)
	if err == nil {
		metric.Name = validName
		h.InvalidString = metric.Name
		h.IngestionStatus = format.TagValueIDSrcIngestionStatusErrMetricNotFound
		return false
	}
	metric.Name = format.AppendHexStringValue(metric.Name[:0], metric.Name)
	h.InvalidString = metric.Name
	h.IngestionStatus = format.TagValueIDSrcIngestionStatusErrMetricNameEncoding
	return false
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
