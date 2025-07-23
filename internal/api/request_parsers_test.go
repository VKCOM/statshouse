// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"net/http"
	"testing"
	"time"

	"github.com/VKCOM/statshouse/internal/promql"
	"github.com/stretchr/testify/require"
)

func TestParseApiQuery(t *testing.T) {
	location := time.FixedZone("MSK", 3)
	req, _ := http.NewRequest("GET", "https://statshouse.mvk.com/api/query?n=5&v=2&s=__src_ingestion_status&f=1725623910&t=1725627511&w=60s&qw=count_norm&ts=-86400&qb=2&qf=0-production&qf=1~+0&qf=2~+10&qf=2~+11&mh=1&ep=1&qv=1", nil)
	p := httpRequestHandler{Request: req, requestHandler: requestHandler{Handler: &Handler{HandlerOptions: HandlerOptions{location: location}}}}
	seriesRequest, err := p.parseSeriesRequest()
	require.NoError(t, err)
	t.Log(seriesRequest)
	// n=5 - get top 5 metircs
	require.Equal(t, int(5), seriesRequest.numResults)
	// s=__src_ingestion_status - metric name
	require.Equal(t, "__src_ingestion_status", seriesRequest.metricName)
	// f=1725623910 - from
	require.Equal(t, int64(1725623910), seriesRequest.from.Unix())
	// t=1725627511 - to
	require.Equal(t, int64(1725627511), seriesRequest.to.Unix())
	// w=60s - request with minute resolution
	require.Equal(t, int64(0), seriesRequest.screenWidth)
	require.Equal(t, int64(60), seriesRequest.step)
	// qw=count_norm&mh=1 - query function in this case "count/sec" with max host enabled
	require.Len(t, seriesRequest.what, 1)
	maxHost := true
	queryFunc, ok := promql.ParseQueryFunc("count_norm", &maxHost)
	require.True(t, ok)
	require.Equal(t,
		queryFunc,
		seriesRequest.what[0],
	)
	// ts=-86400 - show plot with 24 hour(86400 seconds) shift in to the past in addition to a real time
	require.Equal(t,
		[]time.Duration{-time.Hour * 24, 0},
		seriesRequest.shifts,
	)
	// qb=2 - group by tag 2
	require.Len(t, seriesRequest.by, 1)
	require.Equal(t,
		"2",
		seriesRequest.by[0],
	)
	// qf=0-production - filter by tag 0 with value "production"
	require.Len(t, seriesRequest.filterIn, 1)
	require.Equal(t,
		[]string{"production"},
		seriesRequest.filterIn["0"],
	)
	// qf=1~+0&qf=2~+10&qf=2~+11 - tag 1 not in ["0"] and tag 2 not in ["10", "11"]
	require.Len(t, seriesRequest.filterNotIn, 2)
	require.Equal(t,
		[]string{" 0"},
		seriesRequest.filterNotIn["1"],
	)
	require.Equal(t,
		[]string{" 10", " 11"},
		seriesRequest.filterNotIn["2"],
	)
	// ep=1 - extend request with additional points before from and after to
	require.True(t, seriesRequest.excessPoints)
	// qv=1 - request __badges for given metric
	require.True(t, seriesRequest.verbose)
}
