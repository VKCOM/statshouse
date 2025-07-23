// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/VKCOM/statshouse/internal/format"
	"github.com/VKCOM/statshouse/internal/promql"
	"github.com/mailru/easyjson"
)

func (r *httpRequestHandler) parseSeriesRequest() (seriesRequest, error) {
	res, err := r.parseSeriesRequestS(1)
	if err != nil {
		return seriesRequest{}, err
	}
	if len(res) == 0 {
		return seriesRequest{}, httpErr(http.StatusBadRequest, fmt.Errorf("request is empty"))
	}
	return res[0], nil
}

func (r *httpRequestHandler) parseSeriesRequestS(maxTabs int) (res []seriesRequest, err error) {
	defer func() {
		var dummy httpError
		if err != nil && !errors.As(err, &dummy) {
			err = httpErr(http.StatusBadRequest, err)
		}
	}()
	type seriesRequestEx struct {
		seriesRequest
		x             string
		strFrom       string
		strTo         string
		strWidth      string
		strWidthAgg   string
		strNumResults string
		strType       string
		width         int
		widthKind     int

		sourceOfWhat int
		sourceOfBy   int
	}
	var (
		source int
		dash   DashboardData
		first  = func(s []string) string {
			if len(s) != 0 {
				return s[0]
			}
			return ""
		}
		env   = make(map[string]promql.Variable)
		tabs  = make(map[string]*seriesRequestEx)
		tabX  string
		ord   []string
		tabAt = func(i string) *seriesRequestEx {
			if t := tabs[i]; t != nil {
				return t
			}
			t := &seriesRequestEx{
				x: i,
				seriesRequest: seriesRequest{
					vars: env,
				},
			}
			ord = append(ord, i)
			tabs[i] = t
			return t
		}
		tab0 = tabAt("0")
	)
	// parse dashboard
	if id, err := strconv.Atoi(first(r.Form[paramDashboardID])); err == nil {
		var v *format.DashboardMeta
		if v = format.BuiltinDashboardByID[int32(id)]; v == nil {
			v = r.metricsStorage.GetDashboardMeta(int32(id))
		}
		if v != nil {
			// Ugly, but there is no other way because "metricsStorage" stores partially parsed dashboard!
			// TODO: either fully parse and validate dashboard JSON or store JSON string blindly.
			if bs, err := json.Marshal(v.JSONData); err == nil {
				easyjson.Unmarshal(bs, &dash)
			}
		}
	}
	var n int
	for i, v := range dash.Plots {
		id := v.ID
		if id == "" {
			id = strconv.Itoa(i)
		}
		tab := tabAt(id)
		if v.UseV2 {
			tab.version = Version2
		}
		tab.numResults = v.NumSeries
		tab.metricName = v.MetricName
		tab.customMetricName = v.CustomName
		if v.Width > 0 {
			tab.strWidth = fmt.Sprintf("%ds", v.Width)
		}
		if v.Width > 0 {
			tab.width = v.Width
		} else {
			tab.width = 1
		}
		tab.widthKind = widthLODRes
		tab.promQL = v.PromQL
		for _, v := range v.What {
			if fn, _ := promql.ParseQueryFunc(v, &tab.maxHost); fn.Digest != promql.DigestUnspecified {
				tab.what = append(tab.what, fn)
			}
		}
		tab.strType = strconv.Itoa(v.Type)
		for _, v := range v.GroupBy {
			if tid, err := parseTagID(v); err == nil {
				tab.by = append(tab.by, tid)
			}
		}
		if len(v.FilterIn) != 0 {
			tab.filterIn = make(map[string][]string)
			for k, v := range v.FilterIn {
				if tid, err := parseTagID(k); err == nil {
					tab.filterIn[tid] = v
				}
			}
		}
		if len(v.FilterNotIn) != 0 {
			tab.filterNotIn = make(map[string][]string)
			for k, v := range v.FilterNotIn {
				if tid, err := parseTagID(k); err == nil {
					tab.filterNotIn[tid] = v
				}
			}
		}
		tab.maxHost = v.MaxHost
		n++
	}
	for _, v := range dash.Vars {
		env[v.Name] = promql.Variable{
			Value:  v.Vals,
			Group:  v.Args.Group,
			Negate: v.Args.Negate,
		}
		for _, link := range v.Link {
			if len(link) != 2 {
				continue
			}
			tagX, _ := strconv.Atoi(link[1])
			var (
				tagID string
			)
			if tagX < 0 {
				tagID = format.StringTopTagID
			} else if 0 <= tagX && tagX < format.MaxTags {
				tagID = format.TagID(tagX)
			} else {
				continue
			}
			tab := tabs[link[0]]
			if v.Args.Group {
				tab.by = append(tab.by, tagID)
			}
			if tab.filterIn != nil {
				delete(tab.filterIn, tagID)
			}
			if tab.filterNotIn != nil {
				delete(tab.filterNotIn, tagID)
			}
			if len(v.Vals) != 0 {
				if v.Args.Negate {
					if tab.filterNotIn == nil {
						tab.filterNotIn = make(map[string][]string)
					}
					tab.filterNotIn[tagID] = v.Vals
				} else {
					if tab.filterIn == nil {
						tab.filterIn = make(map[string][]string)
					}
					tab.filterIn[tagID] = v.Vals
				}
			}
		}
	}
	if n != 0 {
		switch dash.TimeRange.To {
		case "ed": // end of day
			year, month, day := time.Now().In(r.location).Date()
			tab0.to = time.Date(year, month, day, 0, 0, 0, 0, r.location).Add(24 * time.Hour).UTC()
			tab0.strTo = strconv.FormatInt(tab0.to.Unix(), 10)
		case "ew": // end of week
			var (
				year, month, day = time.Now().In(r.location).Date()
				dateNow          = time.Date(year, month, day, 0, 0, 0, 0, r.location)
				offset           = time.Duration(((time.Sunday - dateNow.Weekday() + 7) % 7) + 1)
			)
			tab0.to = dateNow.Add(offset * 24 * time.Hour).UTC()
			tab0.strTo = strconv.FormatInt(tab0.to.Unix(), 10)
		default:
			if n, err := strconv.ParseInt(dash.TimeRange.To, 10, 64); err == nil {
				if to, err := parseUnixTimeTo(n); err == nil {
					tab0.to = to
					tab0.strTo = dash.TimeRange.To
				}
			}
		}
		if from, err := parseUnixTimeFrom(dash.TimeRange.From, tab0.to); err == nil {
			tab0.from = from
			tab0.strFrom = strconv.FormatInt(dash.TimeRange.From, 10)
		}
		tab0.shifts, _ = parseTimeShifts(dash.TimeShifts)
		for _, tab := range tabs {
			tab.shifts = tab0.shifts
		}
	}
	// parse URL
	_ = r.ParseForm() // (*http.Request).FormValue ignores parse errors
	type (
		dashboardVar struct {
			name string
			link [][2]string
		}
		dashboardVarM struct {
			val    []string
			group  string
			negate string
		}
	)
	var (
		vars  []dashboardVar
		varM  = make(map[string]*dashboardVarM)
		varAt = func(i int) *dashboardVar {
			for j := len(vars) - 1; j < i; j++ {
				vars = append(vars, dashboardVar{})
			}
			return &vars[i]
		}
		varByName = func(s string) (v *dashboardVarM) {
			if v = varM[s]; v == nil {
				v = &dashboardVarM{}
				varM[s] = v
			}
			return v
		}
		sourceOfVars           int
		ensureSameSourceOfVars = func() {
			if sourceOfVars == source {
				return
			}
			for _, t := range tabs {
				t.filterIn = nil
				t.filterNotIn = nil
				t.by = t.by[:0]
				t.sourceOfBy = source
			}
			vars = vars[:0]
			for k := range varM {
				delete(varM, k)
			}
			for k := range env {
				delete(env, k)
			}
			sourceOfVars = source
		}
	)
	for i, v := range dash.Vars {
		vv := varAt(i)
		vv.name = v.Name
		vv.link = append(vv.link, v.Link...)
	}
	searchParams := make(url.Values, len(dash.SearchParams))
	for _, v := range dash.SearchParams {
		if _, ok := r.Form[v[0]]; !ok {
			searchParams[v[0]] = append(searchParams[v[0]], v[1])
		}
	}
	for k, v := range searchParams {
		r.Form[k] = v
	}
	source++
	for k, v := range r.Form {
		i := "0"
		if strings.HasPrefix(k, "t") {
			var dotX int
			if dotX = strings.Index(k, "."); dotX != -1 {
				i = k[1:dotX]
				k = k[dotX+1:]
			}
		} else if len(k) > 1 && k[0] == 'v' { // variables, not version
			ensureSameSourceOfVars()
			var dotX int
			if dotX = strings.Index(k, "."); dotX != -1 {
				switch dotX {
				case 1: // e.g. "v.environment.g=1"
					s := strings.Split(k[dotX+1:], ".")
					switch len(s) {
					case 1:
						vv := varByName(s[0])
						vv.val = append(vv.val, v...)
					case 2:
						switch s[1] {
						case "g":
							varByName(s[0]).group = first(v)
						case "nv":
							varByName(s[0]).negate = first(v)
						}
					}
				default: // e.g. "v0.n=environment" or "v0.l=0.0-1.0"
					if varX, err := strconv.Atoi(k[1:dotX]); err == nil {
						vv := varAt(varX)
						switch k[dotX+1:] {
						case "n":
							vv.name = first(v)
						case "l":
							for _, s1 := range strings.Split(first(v), "-") {
								if s2 := strings.Split(s1, "."); len(s2) == 2 {
									vv.link = append(vv.link, [2]string{s2[0], s2[1]})
								}
							}
						}
					}
				}
			}
			continue
		}
		t := tabAt(i)
		if t == nil {
			continue
		}
		switch k {
		case paramTabNumber:
			tabX = first(v)
		case ParamAvoidCache:
			t.avoidCache = true
		case ParamFromTime:
			t.strFrom = first(v)
		case ParamMetric:
			name := first(v)
			ns := r.FormValue(ParamNamespace)
			t.metricName = mergeMetricNamespace(ns, name)
		case ParamNumResults:
			t.strNumResults = first(v)
		case ParamQueryBy:
			for _, s := range v {
				var tid string
				tid, err = parseTagID(s)
				if err != nil {
					return nil, err
				}
				ensureSameSourceOfVars()
				t.by = append(t.by, tid)
			}
		case ParamQueryFilter:
			ensureSameSourceOfVars()
			t.filterIn, t.filterNotIn, err = parseQueryFilter(v)
		case ParamQueryVerbose:
			t.verbose = first(v) == "1"
		case ParamQueryWhat:
			for _, what := range v {
				if fn, _ := promql.ParseQueryFunc(what, &t.maxHost); fn.Digest != promql.DigestUnspecified {
					if t.sourceOfWhat != source {
						t.what = t.what[:0]
						t.sourceOfWhat = source
					}
					t.what = append(t.what, fn)
				}
			}
		case ParamTimeShift:
			t.shifts, err = parseTimeShifts(v)
		case ParamToTime:
			t.strTo = first(v)
		case ParamVersion:
			s := first(v)
			switch s {
			case Version1, Version2, Version3:
				t.version = s
			default:
				return nil, fmt.Errorf("invalid version: %q", s)
			}
		case ParamWidth:
			t.strWidth = first(v)
		case ParamWidthAgg:
			t.strWidthAgg = first(v)
		case paramMaxHost:
			t.maxHost = true
		case paramPromQuery:
			t.promQL = first(v)
		case paramDataFormat:
			t.format = first(v)
		case paramQueryType:
			t.strType = first(v)
		case paramExcessPoints:
			t.excessPoints = true
		case paramFromEnd:
			t.fromEnd = true
		case paramFromRow:
			t.fromRow, err = parseFromRows(first(v))
		case paramToRow:
			t.toRow, err = parseFromRows(first(v))
		case paramYL:
			t.yl = first(v)
		case paramYH:
			t.yh = first(v)
		case paramCompat:
			t.compat = first(v) == "1"
		case "op":
			ord = strings.Split(first(v), ".")
		case "live":
			t.play, _ = strconv.Atoi(first(v))
		}
		if err != nil {
			return nil, err
		}
	}
	if len(tabs) == 0 {
		return nil, nil
	}
	for _, v := range vars {
		vv := varM[v.name]
		if vv == nil {
			continue
		}
		env[v.name] = promql.Variable{
			Value:  vv.val,
			Group:  vv.group == "1",
			Negate: vv.negate == "1",
		}
		for _, link := range v.link {
			if len(link) != 2 {
				continue
			}
			tabX := link[0]
			tagX, _ := strconv.Atoi(link[1])
			var (
				tagID string
			)
			if tagX < 0 {
				tagID = format.StringTopTagID
			} else if 0 <= tagX && tagX < format.MaxTags {
				tagID = format.TagID(tagX)
			} else {
				continue
			}
			tab := tabs[tabX]
			if tab == nil {
				continue
			}
			if vv.group == "1" {
				tab.by = append(tab.by, tagID)
			}
			if tab.filterIn != nil {
				delete(tab.filterIn, tagID)
			}
			if tab.filterNotIn != nil {
				delete(tab.filterNotIn, tagID)
			}
			if len(vv.val) != 0 {
				if vv.negate == "1" {
					if tab.filterNotIn == nil {
						tab.filterNotIn = make(map[string][]string)
					}
					tab.filterNotIn[tagID] = vv.val
				} else {
					if tab.filterIn == nil {
						tab.filterIn = make(map[string][]string)
					}
					tab.filterIn[tagID] = vv.val
				}
			}
		}
	}
	// parse dependent paramemeters
	var (
		finalize = func(t *seriesRequestEx) error {
			t.version = r.version
			numResultsMax := maxSeries
			if len(t.shifts) != 0 {
				numResultsMax /= len(t.shifts)
			}
			if t.strNumResults != "" {
				if t.numResults, err = parseNumResults(t.strNumResults, numResultsMax); err != nil {
					return err
				}
				if t.numResults == 0 {
					t.numResults = math.MaxInt
				}
			}
			if len(t.strWidth) == 0 {
				if n, err := strconv.Atoi(t.strWidthAgg); err == nil {
					if n < 0 {
						t.width = 60 // a minute for "Auto (low)" resolution
					} else {
						t.width = n
					}
				} else {
					t.width = 1 // a second for "Auto" resolution
				}
				t.widthKind = widthLODRes
			} else {
				t.width, t.widthKind, err = parseWidth(t.strWidth, t.strWidthAgg)
				if err != nil {
					return err
				}
			}
			if t.widthKind == widthAutoRes {
				t.screenWidth = int64(t.width)
			} else {
				t.step = int64(t.width)
			}
			if len(t.what) == 0 {
				t.what = append(t.what, promql.SelectorWhat{
					QueryF: format.ParamQueryFnCountNorm,
					Digest: promql.DigestCountSec,
				})
			}
			return nil
		}
	)
	if len(tab0.strFrom) != 0 || len(tab0.strTo) != 0 {
		tab0.from, tab0.to, err = parseFromTo(tab0.strFrom, tab0.strTo)
		if err != nil {
			return nil, err
		}
	}
	for _, t := range tabs {
		t.from = tab0.from
		t.to = tab0.to
		err = finalize(t)
		if err != nil {
			return nil, err
		}
	}
	// build resulting slice
	if tab := tabs[tabX]; tab != nil {
		if tab.strType == "1" {
			return nil, nil
		}
		return []seriesRequest{tab.seriesRequest}, nil
	}
	res = make([]seriesRequest, 0, len(tabs))
	for i := 0; i < len(ord) && len(res) < maxTabs; i++ {
		tab := tabs[ord[i]]
		if tab.strType == "1" {
			continue
		}
		if len(tab.metricName) != 0 || len(tab.promQL) != 0 {
			res = append(res, tab.seriesRequest)
		}
	}
	return res, nil
}
