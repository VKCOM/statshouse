// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package plugin

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/mailru/easyjson"

	"github.com/vkcom/statshouse/internal/api"
	"github.com/vkcom/statshouse/internal/vkgo/vkuth"
)

//go:generate easyjson -no_std_marshalers httpclient.go

var (
	reg = regexp.MustCompile(`^(key\d[-|~])\{(.+)\}$`)
)

type StatsHouseAPIHTTPClient struct {
	URL         string
	accessToken string
	client      http.Client
}

func NewHTTPClient(url string, accessToken string, timeout time.Duration) StatsHouseAPIHTTPClient {
	return StatsHouseAPIHTTPClient{url, accessToken, http.Client{Timeout: timeout}}
}

//easyjson:json
type GetQueryResponse struct {
	Series struct {
		Time       []int64                 `json:"time"`
		SeriesMeta []api.QuerySeriesMetaV2 `json:"series_meta"`
		SeriesData [][]*float64            `json:"series_data"`
	} `json:"series"`
}

func (c StatsHouseAPIHTTPClient) sendRequest(endpoint string, response easyjson.Unmarshaler) error {
	req, err := http.NewRequest(http.MethodGet, c.URL+"/"+endpoint, nil)
	if err != nil {
		return err
	}

	req.Header.Add(vkuth.VkuthKeyHeader, c.accessToken)
	req.Header.Add("Content-Type", "application/json")

	res, err := c.client.Do(req)
	if err != nil {
		return err
	}

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("wrong HTTP status: %s", res.Status)
	}

	defer func() { _ = res.Body.Close() }()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}

	ar := &api.Response{Data: response}
	err = easyjson.Unmarshal(body, ar)
	if err != nil {
		return err
	}

	if ar.Error != "" {
		return fmt.Errorf("api error: %s", ar.Error)
	}

	return nil
}

func (c StatsHouseAPIHTTPClient) GetMetric(metricName string) (*api.MetricInfo, error) {
	params := url.Values{}
	params.Add(api.ParamMetric, metricName)
	response := &api.MetricInfo{}
	err := c.sendRequest(api.EndpointMetric+"?"+params.Encode(), response)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (c StatsHouseAPIHTTPClient) GetMetricsList() (*api.GetMetricsListResp, error) {
	response := &api.GetMetricsListResp{}
	err := c.sendRequest(api.EndpointMetricList, response)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (c StatsHouseAPIHTTPClient) GetMetricTagValues(params url.Values) (*api.GetMetricTagValuesResp, error) {
	response := &api.GetMetricTagValuesResp{}
	err := c.sendRequest(api.EndpointMetricTagValues+"?"+formatValues(params).Encode(), response)
	if err != nil {
		return nil, err
	}

	return response, nil
}

type Query struct {
	TopN       int64
	MetricName string
	TimeFrom   int64
	TimeTo     int64
	Interval   string
	Function   string
	What       []string
	GroupBy    []string
	Filters    []string
	Shifts     []int64
}

func (q Query) GetURLParams() url.Values {
	params := url.Values{}
	params.Add(api.ParamVersion, api.Version2)
	params.Add(api.ParamNumResults, strconv.FormatInt(q.TopN, 10))
	params.Add(api.ParamMetric, q.MetricName)
	params.Add(api.ParamFromTime, strconv.FormatInt(q.TimeFrom, 10))
	params.Add(api.ParamToTime, strconv.FormatInt(q.TimeTo, 10))
	params.Add(api.ParamWidth, q.Interval)

	if len(q.What) > 0 {
		for _, w := range q.What {
			params.Add(api.ParamQueryWhat, w)
		}
	} else {
		params.Add(api.ParamQueryWhat, q.Function)
	}

	for _, gb := range q.GroupBy {
		params.Add(api.ParamQueryBy, gb)
	}
	for _, filter := range q.Filters {
		params.Add(api.ParamQueryFilter, filter)
	}
	for _, shift := range q.Shifts {
		params.Add(api.ParamTimeShift, strconv.FormatInt(shift, 10))
	}

	return params
}

func (c StatsHouseAPIHTTPClient) GetQuery(params url.Values) (*GetQueryResponse, error) {
	response := &GetQueryResponse{}
	err := c.sendRequest(api.EndpointQuery+"?"+formatValues(params).Encode(), response)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func formatValues(orig url.Values) url.Values {
	params := url.Values{}
	for key, values := range orig {
		for _, v := range values {
			switch {
			case reg.MatchString(v):
				match := reg.FindStringSubmatch(v)
				for _, exp := range strings.Split(match[2], ",") {
					params.Add(key, match[1]+exp)
				}
			default:
				params.Add(key, v)
			}
		}
	}
	return params
}

func parseURLQuery(query string) (url.Values, error) {
	values, err := url.ParseQuery(query)
	if err != nil {
		u, err := url.Parse(query)
		if err != nil {
			return values, err
		}
		values = u.Query()
	}
	return values, nil
}
