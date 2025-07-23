// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package plugin

import (
	"context"
	"encoding/json"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/mailru/easyjson"

	"github.com/VKCOM/statshouse/internal/api"
	"github.com/VKCOM/statshouse/internal/format"
)

//go:generate easyjson -no_std_marshalers datasource.go

// Make sure Datasource implements required interfaces. This is important to do
// since otherwise we will only get a not implemented error response from plugin in
// runtime. In this example datasource instance implements backend.QueryDataHandler,
// backend.CheckHealthHandler, backend.StreamHandler interfaces. Plugin should not
// implement all these interfaces - only those which are required for a particular task.
// For example if plugin does not need streaming functionality then you are free to remove
// methods that implement backend.StreamHandler. Implementing instancemgmt.InstanceDisposer
// is useful to clean up resources used by previous datasource instance when a new datasource
// instance created upon datasource settings changed.
var (
	_ backend.QueryDataHandler   = (*Datasource)(nil)
	_ backend.CheckHealthHandler = (*Datasource)(nil)
)

//easyjson:json
type pluginProperties struct {
	ApiURL string `json:"apiURL"`
}

// NewDatasource creates a new datasource instance.
func NewDatasource(_ context.Context, settings backend.DataSourceInstanceSettings) (instancemgmt.Instance, error) {
	props := &pluginProperties{}
	err := easyjson.Unmarshal(settings.JSONData, props)
	if err != nil {
		return nil, err
	}

	u, err := url.ParseRequestURI(props.ApiURL)
	if err != nil {
		return nil, err
	}

	apiClient := NewHTTPClient(u.String(), settings.DecryptedSecureJSONData["apiKey"], time.Minute)
	return &Datasource{
		newResourceHandler(&apiClient),
		&apiClient,
	}, nil
}

// Datasource is an example datasource which can respond to data queries, reports
// its health and has streaming skills.
type Datasource struct {
	backend.CallResourceHandler
	api *StatsHouseAPIHTTPClient
}

// QueryData handles multiple queries and returns multiple responses.
// req contains the queries []DataQuery (where each query contains RefID as a unique identifier).
// The QueryDataResponse contains a map of RefID to the response for each query, and each response
// contains Frames ([]*Frame).
func (d *Datasource) QueryData(ctx context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
	log.DefaultLogger.Info("QueryData called", "request", req)

	// create response struct
	response := backend.NewQueryDataResponse()

	// loop over queries and execute them individually.
	for _, q := range req.Queries {
		res := d.query(ctx, req.PluginContext, q)

		// save the response in a hashmap
		// based on with RefID as identifier
		response.Responses[q.RefID] = res
	}

	return response, nil
}

type queryModel struct {
	MetricName string   `json:"metricName"`
	Function   string   `json:"func"`
	What       []string `json:"what"`
	Keys       map[string]struct {
		Values  []string `json:"values"`
		GroupBy bool     `json:"groupBy"`
		NotIn   bool     `json:"notIn"`
	} `json:"keys"`
	TopN   int64   `json:"topN"`
	Shifts []int64 `json:"shifts"`
	Mode   string  `json:"mode"`
	URL    string  `json:"url"`
	Alias  string  `json:"alias"`
}

func (d *Datasource) query(_ context.Context, _ backend.PluginContext, query backend.DataQuery) backend.DataResponse {
	response := backend.DataResponse{}

	// Unmarshal the JSON into our queryModel.
	var qm queryModel

	response.Error = json.Unmarshal(query.JSON, &qm)
	if response.Error != nil {
		return response
	}

	var params url.Values

	switch qm.Mode {
	case "url":
		params, response.Error = parseURLQuery(qm.URL)
		if response.Error != nil {
			return response
		}

		params.Set(api.ParamFromTime, strconv.FormatInt(query.TimeRange.From.Unix(), 10))
		params.Set(api.ParamToTime, strconv.FormatInt(query.TimeRange.To.Unix(), 10))
		if params.Get(api.ParamWidth) == "" && params.Get(api.ParamWidthAgg) == "" {
			params.Set(api.ParamWidth, strconv.FormatInt(int64(query.Interval)/int64(time.Second), 10)+"s")
		}
		if params.Get(api.ParamVersion) == "" {
			params.Set(api.ParamVersion, api.Version2)
		}
		if params.Get(api.ParamNumResults) == "" {
			params.Set(api.ParamNumResults, "5")
		}
		if params.Get(api.ParamQueryWhat) == "" {
			params.Set(api.ParamQueryWhat, format.ParamQueryFnCountNorm)
		}
	default:
		aq := Query{
			MetricName: qm.MetricName,
			TopN:       qm.TopN,
			Function:   qm.Function,
			What:       qm.What,
			TimeFrom:   query.TimeRange.From.Unix(),
			TimeTo:     query.TimeRange.To.Unix(),
			Interval:   strconv.FormatInt(int64(query.Interval)/int64(time.Second), 10) + "s",
			Shifts:     qm.Shifts,
		}
		for tagID, key := range qm.Keys {
			if key.GroupBy {
				aq.GroupBy = append(aq.GroupBy, tagID)
			}
			for _, v := range key.Values {
				in := "-"
				if key.NotIn {
					in = "~"
				}
				aq.Filters = append(aq.Filters, tagID+in+v)
			}
		}
		params = aq.GetURLParams()
	}

	ar, err := d.api.GetQuery(params)
	if err != nil {
		response.Error = err
		return response
	}

	// create data frame response.
	frame := data.NewFrame(params.Get(api.ParamMetric))
	alias := strings.TrimSpace(qm.Alias)
	if alias != "" {
		frame.Name = alias
	}

	st := make([]time.Time, 0, len(ar.Series.Time))
	for _, t := range ar.Series.Time {
		st = append(st, time.Unix(t, 0))
	}
	frame.Fields = append(frame.Fields, data.NewField("time", nil, st))

	uniqueWhat := make(map[string]struct{})
	for _, meta := range ar.Series.SeriesMeta {
		uniqueWhat[api.WhatToWhatDesc(meta.What)] = struct{}{}
	}

	for index, sd := range ar.Series.SeriesData {
		name := api.MetaToLabel(ar.Series.SeriesMeta[index], len(uniqueWhat), 0)
		frame.Fields = append(frame.Fields, data.NewField(name, nil, sd))
	}

	response.Frames = append(response.Frames, frame)

	return response
}

// CheckHealth handles health checks sent from Grafana to the plugin.
// The main use case for these health checks is the test button on the
// datasource configuration page which allows users to verify that
// a datasource is working as expected.
func (d *Datasource) CheckHealth(_ context.Context, req *backend.CheckHealthRequest) (*backend.CheckHealthResult, error) {
	log.DefaultLogger.Info("CheckHealth called", "request", req)

	status := backend.HealthStatusOk
	message := "Data source is working"

	_, err := d.api.GetMetricsList()
	if err != nil {
		status = backend.HealthStatusError
		message = "StatsHouse API error: " + err.Error()
	}

	return &backend.CheckHealthResult{
		Status:  status,
		Message: message,
	}, nil
}
