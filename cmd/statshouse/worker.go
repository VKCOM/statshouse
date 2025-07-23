// Copyright 2025 V Kontakte LLC
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

	"github.com/VKCOM/statshouse/internal/agent"
	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/VKCOM/statshouse/internal/format"
	"github.com/VKCOM/statshouse/internal/mapping"
	"github.com/VKCOM/statshouse/internal/metajournal"
)

type worker struct {
	sh2           *agent.Agent
	metricStorage *metajournal.MetricsStorage
	autoCreate    *data_model.AutoCreate
	logPackets    func(format string, args ...interface{})

	floodTimeMu            sync.Mutex
	floodTimeHandlePkgFail time.Time
}

func startWorker(sh2 *agent.Agent, metricStorage *metajournal.MetricsStorage, ac *data_model.AutoCreate, logPackets func(format string, args ...interface{})) *worker {
	w := &worker{
		sh2:           sh2,
		metricStorage: metricStorage,
		autoCreate:    ac,
		logPackets:    logPackets,
	}
	return w
}

func (w *worker) HandleMetrics(args data_model.HandlerArgs) (h data_model.MappedMetricHeader) {
	if w.logPackets != nil {
		w.logPackets("Parsed metric: %s\n", args.MetricBytes.String())
	}
	{ // duplication code - TODO - remove after conveyor v3 fully works
		const dupSuffix = "_statshouse_dup"
		originalNameLen := len(args.MetricBytes.Name)
		dupName := append(args.MetricBytes.Name, dupSuffix...)
		args.MetricBytes.Name = dupName[:originalNameLen] // reuse, if allocated above
		if dupMeta := w.metricStorage.GetMetaMetricByNameBytes(dupName); dupMeta != nil && !dupMeta.Disable {
			w.fillTime(args, &h)
			h.MetricMeta = dupMeta
			h.Key.Metric = dupMeta.MetricID
			// mapping tags code below either changes args but in a way second call is NOP, or returns error
			w.sh2.Map(args, &h, nil, true) // no autocreate for duplicates
			w.sh2.ApplyMetric(*args.MetricBytes, h, format.TagValueIDSrcIngestionStatusOKDup, args.Scratch)
			h = data_model.MappedMetricHeader{}
		}
	}
	w.fillTime(args, &h)
	metaOk := w.fillMetricMeta(args, &h)
	if metaOk {
		w.sh2.Map(args, &h, w.autoCreate, false)
	} else {
		w.sh2.MapEnvironment(args.MetricBytes, &h)
	}
	if w.logPackets != nil {
		w.printMetric("cached", *args.MetricBytes, h)
	}
	w.sh2.TimingsMapping.AddValueCounter(time.Since(h.ReceiveTime).Seconds(), 1)
	w.sh2.ApplyMetric(*args.MetricBytes, h, format.TagValueIDSrcIngestionStatusOKCached, args.Scratch)
	return h
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
		h.Key.Metric = metricMeta.MetricID
		if metricMeta.Disable {
			h.IngestionStatus = format.TagValueIDSrcIngestionStatusErrMetricDisabled
			return false
		}
		return true
	}
	metricMeta = format.BuiltinMetricByName[string(metric.Name)]
	if metricMeta != nil {
		h.MetricMeta = metricMeta
		h.Key.Metric = metricMeta.MetricID
		if !metricMeta.BuiltinAllowedToReceive {
			h.IngestionStatus = format.TagValueIDSrcIngestionStatusErrMetricBuiltin
			return false
		}
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
