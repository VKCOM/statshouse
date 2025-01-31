// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package receiver

import (
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"

	"github.com/vkcom/statshouse/internal/agent"
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/mapping"
)

const StatshouseHTTPV1Endpoint = "/api/statshousev1"

type HTTP struct {
	parser
}

func NewHTTPReceiver(sh2 *agent.Agent, logPacket func(format string, args ...interface{})) *HTTP {
	result := &HTTP{
		parser: parser{logPacket: logPacket, sh2: sh2, network: "http"},
	}
	result.parser.createMetrics()
	return result
}

// TODO - heavily optimize, as this endpoint will lure hordes of pythonists/javists
// maybe even use fasthttp with sane limits?
func (s *HTTP) httpFunction(h Handler, r *http.Request) error {
	reader := io.LimitReader(r.Body, math.MaxUint16)
	body, err := io.ReadAll(reader)
	if err != nil {
		if len(body) == math.MaxUint16 {
			return fmt.Errorf("error reading HTTP body: must be <= 64KB")
		}
		return fmt.Errorf("error reading HTTP body: %w", err)
	}
	var firstError error
	notDoneCount := 0
	// TODO - store both channel and callback in UserData to prevent 2 allocations
	ch := make(chan error, 1024) // TODO - use semaphore instead
	cb := func(m tlstatshouse.MetricBytes, h data_model.MappedMetricHeader) {
		ch <- mapping.MapErrorFromHeader(m, h)
	}
	var batch tlstatshouse.AddMetricsBatchBytes
	if err := s.parse(h, cb, &notDoneCount, &firstError, body, &batch, nil); err != nil {
		return fmt.Errorf("error parsing HTTP body: %w", err)
	}
	for i := 0; i < notDoneCount; i++ {
		err := <-ch
		if firstError == nil {
			firstError = err
		}
	}
	return firstError
}

func (s *HTTP) Serve(h Handler, ln net.Listener) error {
	handler := http.NewServeMux()
	handler.HandleFunc(StatshouseHTTPV1Endpoint, func(w http.ResponseWriter, r *http.Request) {
		if err := s.httpFunction(h, r); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(err.Error()))
			return
		}
		_, _ = w.Write([]byte("OK"))
	})
	server := http.Server{Handler: handler}
	log.Printf("Serve HTTP on %s", ln.Addr())
	err := server.Serve(ln)
	if err != nil {
		log.Printf("HTTP server failed to serve on %s: %v", ln.Addr(), err)
	}
	return err
}
