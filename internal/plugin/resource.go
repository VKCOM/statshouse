// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package plugin

import (
	"net/http"
	"net/url"
	"strconv"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/resource/httpadapter"
	"github.com/mailru/easyjson"
	"github.com/vkcom/statshouse/internal/api"
	"github.com/vkcom/statshouse/internal/format"
)

//go:generate easyjson -no_std_marshalers resource.go

const (
	endpointMetric          = "/metric"
	endpointMetricNames     = "/metric-names"
	endpointMetricTagValues = "/metric-tag-values"

	paramsMetricName = "metric_name"
	paramsTagID      = "tag_id"
	paramsFunction   = "function"
	paramsWhat       = "what"
	paramsFilters    = "filters"
	paramsTimeFrom   = "time_from"
	paramsTimeTo     = "time_to"
	paramsQuery      = "query"
)

var (
	availableFunctions = map[string][]string{
		format.MetricKindCounter: {
			api.ParamQueryFnCountNorm,
			api.ParamQueryFnCount,
			api.ParamQueryFnCumulCount,
			api.ParamQueryFnMaxCountHost,
			api.ParamQueryFnDerivativeCount,
			api.ParamQueryFnDerivativeCountNorm,
		},
		format.MetricKindValue: {
			api.ParamQueryFnAvg,
			api.ParamQueryFnMin,
			api.ParamQueryFnMax,
			api.ParamQueryFnSumNorm,
			api.ParamQueryFnSum,
			api.ParamQueryFnStddev,
			api.ParamQueryFnCountNorm,
			api.ParamQueryFnCount,
			api.ParamQueryFnCumulAvg,
			api.ParamQueryFnCumulSum,
			api.ParamQueryFnCumulCount,
			api.ParamQueryFnMaxHost,
			api.ParamQueryFnDerivativeCount,
			api.ParamQueryFnDerivativeCountNorm,
			api.ParamQueryFnDerivativeSum,
			api.ParamQueryFnDerivativeSumNorm,
			api.ParamQueryFnDerivativeAvg,
			api.ParamQueryFnDerivativeMin,
			api.ParamQueryFnDerivativeMax,
		},
		format.MetricKindValuePercentiles: {
			api.ParamQueryFnAvg,
			api.ParamQueryFnMin,
			api.ParamQueryFnMax,
			api.ParamQueryFnSumNorm,
			api.ParamQueryFnSum,
			api.ParamQueryFnStddev,
			api.ParamQueryFnCountNorm,
			api.ParamQueryFnCount,
			api.ParamQueryFnCumulAvg,
			api.ParamQueryFnCumulSum,
			api.ParamQueryFnCumulCount,
			api.ParamQueryFnP25,
			api.ParamQueryFnP50,
			api.ParamQueryFnP75,
			api.ParamQueryFnP90,
			api.ParamQueryFnP95,
			api.ParamQueryFnP99,
			api.ParamQueryFnP999,
			api.ParamQueryFnMaxHost,
			api.ParamQueryFnDerivativeCount,
			api.ParamQueryFnDerivativeCountNorm,
			api.ParamQueryFnDerivativeSum,
			api.ParamQueryFnDerivativeSumNorm,
			api.ParamQueryFnDerivativeAvg,
			api.ParamQueryFnDerivativeMin,
			api.ParamQueryFnDerivativeMax,
		},
		format.MetricKindUnique: {
			api.ParamQueryFnUniqueNorm,
			api.ParamQueryFnUnique,
			api.ParamQueryFnCountNorm,
			api.ParamQueryFnCount,
			api.ParamQueryFnCumulCount,
			api.ParamQueryFnAvg,
			api.ParamQueryFnMin,
			api.ParamQueryFnMax,
			api.ParamQueryFnStddev,
			api.ParamQueryFnMaxCountHost,
			api.ParamQueryFnDerivativeCount,
			api.ParamQueryFnDerivativeCountNorm,
			api.ParamQueryFnDerivativeAvg,
			api.ParamQueryFnDerivativeMin,
			api.ParamQueryFnDerivativeMax,
			api.ParamQueryFnDerivativeUnique,
			api.ParamQueryFnDerivativeUniqueNorm,
		},
		format.MetricKindMixed: {
			api.ParamQueryFnCountNorm,
			api.ParamQueryFnCount,
			api.ParamQueryFnAvg,
			api.ParamQueryFnMin,
			api.ParamQueryFnMax,
			api.ParamQueryFnSumNorm,
			api.ParamQueryFnSum,
			api.ParamQueryFnStddev,
			api.ParamQueryFnUniqueNorm,
			api.ParamQueryFnUnique,
			api.ParamQueryFnCumulCount,
			api.ParamQueryFnCumulAvg,
			api.ParamQueryFnCumulSum,
			api.ParamQueryFnMaxHost,
			api.ParamQueryFnMaxCountHost,
			api.ParamQueryFnDerivativeCount,
			api.ParamQueryFnDerivativeCountNorm,
			api.ParamQueryFnDerivativeSum,
			api.ParamQueryFnDerivativeSumNorm,
			api.ParamQueryFnDerivativeAvg,
			api.ParamQueryFnDerivativeMin,
			api.ParamQueryFnDerivativeMax,
			api.ParamQueryFnDerivativeUnique,
			api.ParamQueryFnDerivativeUniqueNorm,
		},
		format.MetricKindMixedPercentiles: {
			api.ParamQueryFnCountNorm,
			api.ParamQueryFnCount,
			api.ParamQueryFnAvg,
			api.ParamQueryFnMin,
			api.ParamQueryFnMax,
			api.ParamQueryFnSumNorm,
			api.ParamQueryFnSum,
			api.ParamQueryFnStddev,
			api.ParamQueryFnUniqueNorm,
			api.ParamQueryFnUnique,
			api.ParamQueryFnCumulCount,
			api.ParamQueryFnCumulAvg,
			api.ParamQueryFnCumulSum,
			api.ParamQueryFnP25,
			api.ParamQueryFnP50,
			api.ParamQueryFnP75,
			api.ParamQueryFnP90,
			api.ParamQueryFnP95,
			api.ParamQueryFnP99,
			api.ParamQueryFnP999,
			api.ParamQueryFnMaxHost,
			api.ParamQueryFnMaxCountHost,
			api.ParamQueryFnDerivativeCount,
			api.ParamQueryFnDerivativeCountNorm,
			api.ParamQueryFnDerivativeSum,
			api.ParamQueryFnDerivativeSumNorm,
			api.ParamQueryFnDerivativeAvg,
			api.ParamQueryFnDerivativeMin,
			api.ParamQueryFnDerivativeMax,
			api.ParamQueryFnDerivativeUnique,
			api.ParamQueryFnDerivativeUniqueNorm,
		},
	}
)

type ResourceHandler struct {
	mux *http.ServeMux
	api *StatsHouseAPIHTTPClient
}

func newResourceHandler(api *StatsHouseAPIHTTPClient) backend.CallResourceHandler {
	mux := http.NewServeMux()
	h := ResourceHandler{mux: mux, api: api}

	mux.HandleFunc(endpointMetricTagValues, h.handleMetricTagValues)
	mux.HandleFunc(endpointMetricNames, h.handleMetricNames)
	mux.HandleFunc(endpointMetric, h.handleMetric)

	return httpadapter.New(mux)
}

//easyjson:json
type metricNamesResponse struct {
	MetricNames []string `json:"metric_names"`
}

//easyjson:json
type metricResourceResponse struct {
	Functions []string `json:"functions"`
	Tags      []Tag    `json:"tags"`
}

//easyjson:json
type metricTagValuesResponse struct {
	TagValues []api.MetricTagValueInfo `json:"tag_values"`
}

type Tag struct {
	ID          string `json:"id"`
	Description string `json:"description,omitempty"`
	IsRaw       bool   `json:"is_raw"`
}

func (h *ResourceHandler) handleMetric(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}

	metricName := r.FormValue("metric_name")
	response, err := h.api.GetMetric(metricName)
	if err != nil {
		writeAPIErrorJSON(w, err)
		return
	}

	tags := make([]Tag, 0, len(response.Metric.Tags))
	for i, tag := range response.Metric.Tags {
		t := Tag{
			ID:          "key" + strconv.Itoa(i),
			Description: tag.Name,
			IsRaw:       tag.Raw,
		}
		if tag.Description != "" {
			t.Description = tag.Description
		}
		tags = append(tags, t)
	}
	if response.Metric.StringTopDescription != "" {
		t := Tag{
			ID:          "skey",
			Description: response.Metric.StringTopName,
			IsRaw:       false,
		}
		if response.Metric.StringTopDescription != "" {
			t.Description = response.Metric.StringTopDescription
		}
		tags = append(tags, t)
	}

	result := &metricResourceResponse{
		Functions: availableFunctions[response.Metric.Kind],
		Tags:      tags,
	}
	writeResponseJSON(w, result)
}

func (h *ResourceHandler) handleMetricNames(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}

	response, err := h.api.GetMetricsList()
	if err != nil {
		writeAPIErrorJSON(w, err)
		return
	}

	if len(response.Metrics) == 0 {
		http.Error(w, "empty metrics", http.StatusInternalServerError)
		return
	}

	names := make([]string, 0, len(response.Metrics))
	for _, m := range response.Metrics {
		names = append(names, m.Name)
	}

	metricNames := &metricNamesResponse{
		MetricNames: names,
	}

	writeResponseJSON(w, metricNames)
}

func (h *ResourceHandler) handleMetricTagValues(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}

	_ = r.ParseForm()

	var (
		err    error
		params = url.Values{}
	)

	if r.FormValue(paramsQuery) != "" {
		params, err = parseURLQuery(r.FormValue(paramsQuery))
		if err != nil {
			writeAPIErrorJSON(w, err)
			return
		}
		if params.Get(api.ParamVersion) == "" {
			params.Set(api.ParamVersion, api.Version2)
		}
		if params.Get(api.ParamNumResults) == "" {
			params.Set(api.ParamNumResults, "1000")
		}
		if params.Get(api.ParamQueryWhat) == "" {
			params.Set(api.ParamQueryWhat, api.ParamQueryFnCountNorm)
		}
		if params.Get(api.ParamFromTime) == "" {
			params.Set(api.ParamFromTime, r.FormValue(paramsTimeFrom))
		}
		if params.Get(api.ParamToTime) == "" {
			params.Set(api.ParamToTime, r.FormValue(paramsTimeTo))
		}
	} else {
		params.Add(api.ParamVersion, api.Version2)
		params.Add(api.ParamMetric, r.FormValue(paramsMetricName))
		params.Add(api.ParamTagID, r.FormValue(paramsTagID))
		params.Add(api.ParamNumResults, "1000")
		params.Add(api.ParamFromTime, r.FormValue(paramsTimeFrom))
		params.Add(api.ParamToTime, r.FormValue(paramsTimeTo))
		for _, f := range r.Form[paramsFilters] {
			params.Add(api.ParamQueryFilter, f)
		}
		if len(r.Form[paramsWhat]) > 0 {
			for _, w := range r.Form[paramsWhat] {
				params.Add(api.ParamQueryWhat, w)
			}
		} else {
			params.Add(api.ParamQueryWhat, r.FormValue(paramsFunction))
		}
	}

	response, err := h.api.GetMetricTagValues(params)
	if err != nil {
		writeAPIErrorJSON(w, err)
		return
	}

	metricTagValues := &metricTagValuesResponse{response.TagValues}
	writeResponseJSON(w, metricTagValues)
}

func writeResponseJSON(w http.ResponseWriter, data easyjson.Marshaler) {
	j, err := easyjson.Marshal(data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	_, err = w.Write(j)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func writeAPIErrorJSON(w http.ResponseWriter, err error) {
	http.Error(w, "StatsHouse API error: "+err.Error(), http.StatusInternalServerError)
}
