// Copyright 2022 V Kontakte LLC
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
)

type openGraphInfo struct {
	Title       string
	Image       string
	ImageWidth  int
	ImageHeight int
}

func value(r *http.Request, v string, d string) string {
	values := r.FormValue(v)
	if values == "" {
		values = d
	}

	return values
}

func getOpenGraphInfo(r *http.Request, origPath string) *openGraphInfo {
	if origPath != viewPath {
		return nil // path does not generate image
	}

	var (
		metrics       []string
		whats         [][]string
		v             = url.Values{}
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
			paramVersion     = p + ParamVersion
			paramNumResults  = p + ParamNumResults
			paramQueryBy     = p + ParamQueryBy
			paramQueryFilter = p + ParamQueryFilter
			paramTimeShift   = p + ParamTimeShift
			paramWidthAgg    = p + ParamWidthAgg
			paramWidth       = p + ParamWidth
			paramQueryWhat   = p + ParamQueryWhat
			paramPromQuery   = p + paramPromQuery
			paramYL          = p + paramYL
			paramYH          = p + paramYH
		)

		// Build query
		v.Set(paramMetric, metric)
		v.Set(paramVersion, value(r, paramVersion, "2"))
		v[paramNumResults] = r.Form[paramNumResults]
		v[paramQueryBy] = r.Form[paramQueryBy]
		v[paramQueryFilter] = r.Form[paramQueryFilter]
		v[paramTimeShift] = r.Form[paramTimeShift]
		v[paramPromQuery] = r.Form[paramPromQuery]
		v[paramYL] = r.Form[paramYL]
		v[paramYH] = r.Form[paramYH]
		//-- width
		widthAgg := r.FormValue(paramWidthAgg)
		if widthAgg != "" {
			v.Set(paramWidth, fmt.Sprintf("%ss", widthAgg))
		} else {
			v.Set(paramWidth, strconv.Itoa(width))
		}
		//-- what
		what := r.Form[paramQueryWhat]
		if len(what) == 0 {
			what = []string{"count_norm"}
		}
		v[paramQueryWhat] = what

		// SaveMetric to build title later
		metrics = append(metrics, metric)
		whats = append(whats, what)
	}
	if dashboardID = r.FormValue(paramDashboardID); len(dashboardID) != 0 {
		v.Set(paramDashboardID, dashboardID)
	}
	if len(v) == 0 {
		return nil
	}
	if t := r.FormValue(ParamFromTime); len(t) != 0 {
		v.Set(ParamFromTime, t)
	}
	if t := r.FormValue(ParamToTime); len(t) != 0 {
		v.Set(ParamToTime, t)
	}
	// forward variables as is
	for k, s := range r.Form {
		if strings.HasPrefix(k, "v") {
			for _, vv := range s {
				v.Add(k, vv)
			}
		}
	}
	// Active tab number, total image height
	tn, err := strconv.Atoi(r.FormValue(paramTabNumber))
	if err != nil {
		tn = 0
	} else {
		if tn == -1 && 1 < tab {
			height = int(float32(height) / 2 * float32((tab+1)/2))
		}
		v.Set(paramTabNumber, strconv.Itoa(tn))
	}

	var (
		u = url.URL{
			Path:     RoutePrefix + EndpointRender,
			RawQuery: v.Encode(),
		}
		title string
	)
	if 0 <= tn && tn < len(metrics) {
		what := whats[tn]
		for i, w := range what {
			if _, ok := ParseQueryFunc(w, nil); ok {
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
