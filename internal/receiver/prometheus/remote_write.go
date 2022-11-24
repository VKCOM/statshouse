// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package prometheus

import (
	"io"
	"log"
	"net/http"
	"sync"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/gorilla/mux"
	"github.com/prometheus/prometheus/prompb"

	"github.com/vkcom/statshouse/internal/agent"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/receiver"
)

const (
	_bucket = "_bucket"
	_sum    = "_sum"
	_count  = "_count"
)

type remoteWrite struct {
	h       receiver.Handler
	cache   metricCache
	cacheMu sync.RWMutex
}

func ServeRemoteWrite(cfg agent.Config, h receiver.Handler) func() {
	rw := &remoteWrite{h: h, cache: metricCache{}}
	router := mux.NewRouter()
	router.Path(cfg.RemoteWritePath).Methods("POST").HandlerFunc(rw.handleWriteResponse)
	s := &http.Server{
		Addr:    cfg.RemoteWriteAddr,
		Handler: http.Handler(router),
	}
	go func() {
		err := s.ListenAndServe()
		if err != http.ErrServerClosed {
			log.Printf("prometheus remote write terminated with %v", err)
		}
	}()
	return func() { _ = s.Close() }
}

func (rw *remoteWrite) handleWriteResponse(w http.ResponseWriter, r *http.Request) {
	metrics, err := rw.readMetrics(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	for _, metric := range metrics {
		rw.h.HandleMetrics(&metric, nil)
	}
	w.WriteHeader(http.StatusOK)
}

func (rw *remoteWrite) readMetrics(r *http.Request) ([]tlstatshouse.MetricBytes, error) {
	req, err := readRemoteWriteRequest(r)
	if err != nil {
		return nil, err
	}
	rw.cacheMu.Lock()
	defer rw.cacheMu.Unlock()
	// metadata (metric type in particular) is required because metrics are handled differently,
	// e.g. we store absolute value for gauges but delta for counters
	if len(rw.cache) == 0 && len(req.Metadata) == 0 {
		return []tlstatshouse.MetricBytes{}, nil
	}
	// read metadata
	for _, meta := range req.Metadata {
		rw.cache.processProtobufMeta(meta)
	}
	// read series
	ret := make([]tlstatshouse.MetricBytes, 0, len(req.Timeseries))
	for _, ts := range req.Timeseries {
		metric, ok := rw.cache.getMetric(metricKeyFromProtobufLabels(ts.Labels))
		if ok {
			for _, sample := range ts.Samples {
				ret = metric.processSample(sample.Value, sample.Timestamp, ret)
			}
		}
	}
	return ret, nil
}

func readRemoteWriteRequest(r *http.Request) (prompb.WriteRequest, error) {
	wr := prompb.WriteRequest{}
	compressed, err := io.ReadAll(r.Body)
	if err != nil {
		return wr, err
	}
	var uncompressed []byte
	uncompressed, err = snappy.Decode(uncompressed, compressed)
	if err != nil {
		return wr, err
	}
	err = proto.Unmarshal(uncompressed, &wr)
	return wr, err
}
