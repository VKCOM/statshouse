// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/vkcom/statshouse/internal/format"
)

func (h *Handler) HandlePromInstantQuery(w http.ResponseWriter, r *http.Request) {
	ai, ok := h.parseAccessToken(w, r, nil)
	if !ok {
		return
	}

	q, err := parsePromInstantQuery(r)
	if err != nil {
		promAPIRespondError(w, promAPIErrorBadData, err)
		return
	}

	ng, close, err := newPromInstantQueryEngine(q, h, ai)
	if err != nil {
		promAPIRespondError(w, promAPIErrorExec, err)
		return
	}
	defer close()

	ctx, cancel := context.WithTimeout(r.Context(), querySelectTimeout)
	defer cancel()
	defer func() {
		if err := recover(); err != nil {
			promAPIRespondError(w, promAPIErrorExec, fmt.Errorf("unexpected error: %v", err))
		}
	}()

	res := ng.Exec(ctx)
	if res.Err != nil {
		promAPIRespondError(w, promAPIErrorExec, res.Err)
		return
	}

	promAPIRespond(w, promAPIResponseData{ResultType: res.Value.Type(), Result: res.Value})
}

func (h *Handler) HandlePromRangeQuery(w http.ResponseWriter, r *http.Request) {
	ai, ok := h.parseAccessToken(w, r, nil)
	if !ok {
		return
	}

	q, err := parsePromRangeQuery(r)
	if err != nil {
		promAPIRespondError(w, promAPIErrorBadData, err)
		return
	}

	ng, close, err := newPromRangeQueryEngine(q, h, ai)
	if err != nil {
		promAPIRespondError(w, promAPIErrorExec, err)
		return
	}
	defer close()

	ctx, cancel := context.WithTimeout(r.Context(), querySelectTimeout)
	defer cancel()
	defer func() {
		if err := recover(); err != nil {
			promAPIRespondError(w, promAPIErrorExec, fmt.Errorf("unexpected error: %v", err))
		}
	}()

	res := ng.Exec(ctx)
	if res.Err != nil {
		promAPIRespondError(w, promAPIErrorExec, res.Err)
		return
	}

	promAPIRespond(w, promAPIResponseData{ResultType: res.Value.Type(), Result: res.Value})
}

func (h *Handler) HandlePromLabelValuesQuery(w http.ResponseWriter, r *http.Request) {
	ai, ok := h.parseAccessToken(w, r, nil)
	if !ok {
		return
	}

	name := mux.Vars(r)["name"]
	if name != "__name__" {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	vals := make([]string, 0)
	for _, m := range format.BuiltinMetricsList() {
		vals = append(vals, m.Name)
	}
	for _, v := range h.metricsStorage.GetMetaMetricList(h.showInvisible) {
		if ai.canViewMetric(v.Name) {
			vals = append(vals, v.Name)
		}
	}
	promAPIRespond(w, vals)
}

// region Request

func parsePromRangeQuery(r *http.Request) (*promAPIQuery, error) {
	start, err := parseTime(r.FormValue("start"))
	if err != nil {
		return nil, fmt.Errorf("invalid parameter start: %w", err)
	}

	end, err := parseTime(r.FormValue("end"))
	if err != nil {
		return nil, fmt.Errorf("invalid parameter end: %w", err)
	}
	if end.Before(start) {
		return nil, fmt.Errorf("invalid parameter end: end timestamp must not be before start time")
	}

	step, err := parseDuration(r.FormValue("step"))
	if err != nil {
		return nil, fmt.Errorf("invalid parameter step: %w", err)
	}
	if step <= 0 {
		return nil, fmt.Errorf("invalid parameter step: zero or negative handleQuery resolution step widths are not accepted. Try a positive integer")
	}

	// For safety, limit the number of returned points per timeseries.
	// This is sufficient for 60s resolution for a week or 1h resolution for a year.
	if end.Sub(start)/step > 11000 {
		return nil, fmt.Errorf("exceeded maximum resolution of 11,000 points per timeseries. Try decreasing the handleQuery resolution (?step=XX)")
	}

	qs := r.FormValue("query")
	return &promAPIQuery{qs, start, end, step}, nil
}

func parsePromInstantQuery(r *http.Request) (*promAPIQuery, error) {
	t, err := parseTimeParam(r, "time", time.Now())
	if err != nil {
		return nil, fmt.Errorf("invalid parameter time: %w", err)
	}
	qs := r.FormValue("query")
	return &promAPIQuery{qs, t, t, 0}, nil
}

func parseDuration(s string) (time.Duration, error) {
	if d, err := strconv.ParseFloat(s, 64); err == nil {
		ts := d * float64(time.Second)
		if ts > float64(math.MaxInt64) || ts < float64(math.MinInt64) {
			return 0, fmt.Errorf("cannot parse %qs to a valid duration. It overflows int64", s)
		}
		return time.Duration(ts), nil
	}
	if d, err := model.ParseDuration(s); err == nil {
		return time.Duration(d), nil
	}
	return 0, fmt.Errorf("cannot parse %qs to a valid duration", s)
}

// endregion Request

// region Response

const (
	promAPIStatusSuccess promApiStatus = "success"
	promAPIStatusError   promApiStatus = "error"
)

type promApiStatus string

const (
	promAPIErrorTimeout  promAPIErrorType = "timeout"
	promAPIErrorCanceled promAPIErrorType = "canceled"
	promAPIErrorExec     promAPIErrorType = "execution"
	promAPIErrorBadData  promAPIErrorType = "bad_data"
	promAPIErrorInternal promAPIErrorType = "internal"
	promAPIErrorNotFound promAPIErrorType = "not_found"
)

type promAPIErrorType string

type promAPIResponse struct {
	Status    promApiStatus    `json:"status"`
	Data      interface{}      `json:"data,omitempty"`
	ErrorType promAPIErrorType `json:"errorType,omitempty"`
	Error     string           `json:"error,omitempty"`
	Warnings  []string         `json:"warnings,omitempty"`
}

type promAPIResponseData struct {
	ResultType parser.ValueType `json:"resultType"`
	Result     interface{}      `json:"result"`
}

func promAPIRespond(w http.ResponseWriter, data interface{}) {
	statusMessage := promAPIStatusSuccess
	var warningStrings []string
	b, err := json.Marshal(&promAPIResponse{
		Status:   statusMessage,
		Data:     data,
		Warnings: warningStrings,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if n, err := w.Write(b); err != nil {
		log.Printf("[error] error writing prometheus API response (%v bytes written): %v", n, err)
	}
}

func promAPIRespondError(w http.ResponseWriter, typ promAPIErrorType, err error) {
	b, err := json.Marshal(&promAPIResponse{
		Status:    promAPIStatusError,
		ErrorType: typ,
		Error:     err.Error(),
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	var code int
	switch typ {
	case promAPIErrorBadData:
		code = http.StatusBadRequest
	case promAPIErrorExec:
		code = http.StatusUnprocessableEntity
	case promAPIErrorCanceled, promAPIErrorTimeout:
		code = http.StatusServiceUnavailable
	case promAPIErrorInternal:
		code = http.StatusInternalServerError
	case promAPIErrorNotFound:
		code = http.StatusNotFound
	default:
		code = http.StatusInternalServerError
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if n, err := w.Write(b); err != nil {
		log.Printf("[error] error writing prometheus API response (%v bytes written): %v", n, err)
	}
}

// endregion Response
