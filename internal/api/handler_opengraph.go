// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/vkcom/statshouse/internal/promql"
)

type openGraphInfo struct {
	Title       string
	Image       string
	ImageWidth  int
	ImageHeight int
}

func getOpenGraphInfo(r *http.Request, origPath string) *openGraphInfo {
	if origPath != viewPath {
		return nil // path does not generate image
	}
	var (
		metrics       []string
		whats         [][]string
		width, height = plotSize(dataFormatPNG, true, defaultRenderWidth)
		tab           = 0
		dashboardID   string
	)
	for ; ; tab++ {
		var p string
		if tab == 0 {
			p = ""
		} else {
			p = fmt.Sprintf("t%d.", tab)
		}
		paramMetric := p + ParamMetric
		// Parse metric
		metric := r.FormValue(paramMetric)
		if metric == "" {
			break
		}
		var (
			paramQueryWhat = p + ParamQueryWhat
		)
		//-- what
		what := r.Form[paramQueryWhat]
		if len(what) == 0 {
			what = []string{"count_norm"}
		}
		// SaveMetric to build title later
		metrics = append(metrics, metric)
		whats = append(whats, what)
	}
	// Active tab number, total image height
	tn, err := strconv.Atoi(r.FormValue(paramTabNumber))
	if err != nil {
		tn = 0
	} else {
		if tn == -1 && 1 < tab {
			height = int(float32(height) / 2 * float32((tab+1)/2))
		}
	}
	var (
		u = url.URL{
			Path:     RoutePrefix + EndpointRender,
			RawQuery: r.URL.RawQuery,
		}
		title string
	)
	if 0 <= tn && tn < len(metrics) {
		what := whats[tn]
		for i, w := range what {
			if _, ok := promql.ParseQueryFunc(w, nil); ok {
				what[i] = WhatToWhatDesc(w)
			}
		}
		title = fmt.Sprintf("%s: %s", metrics[tn], strings.Join(what, ", "))
	} else if len(metrics) != 0 {
		title = strings.Join(metrics, ", ")
	} else if len(dashboardID) != 0 {
		title = "Dashboard — StatsHouse"
	}
	return &openGraphInfo{
		Title:       title,
		Image:       u.String(),
		ImageWidth:  width,
		ImageHeight: height,
	}
}
