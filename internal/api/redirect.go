// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/vkcom/statshouse/internal/format"
)

const (
	viewPath         = "/view"
	uiParamCustomAgg = "g"
)

type legacyRedirectReq struct {
	name        string
	topN        string
	date        string
	interval    string
	shift       string
	groupBy     []string
	function    string
	percentiles string
	devStaging  string
	keys        map[string]string
}

func (h *Handler) HandleLegacyRedirect(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	req := legacyRedirectReq{
		name:        strings.TrimSpace(r.FormValue("name")),
		topN:        strings.TrimSpace(r.FormValue("topn")),
		date:        strings.TrimSpace(r.FormValue("date")),
		interval:    strings.TrimSpace(r.FormValue("interval")),
		shift:       strings.TrimSpace(r.FormValue("shift")),
		groupBy:     r.Form["group_by"],
		function:    strings.TrimSpace(r.FormValue("function")),
		percentiles: strings.TrimSpace(r.FormValue("percentiles")),
		devStaging:  strings.TrimSpace(r.FormValue("devstaging")),
		keys:        map[string]string{},
	}
	for i := 1; i < format.MaxTags; i++ {
		req.keys[format.TagID(i)] = strings.TrimSpace(r.FormValue(format.TagIDLegacy(i)))
	}

	u := h.handleLegacyRedirect(req)

	w.Header().Set("Location", u.String())
	w.WriteHeader(http.StatusSeeOther)
}

func (h *Handler) handleLegacyRedirect(req legacyRedirectReq) *url.URL {
	values := url.Values{}
	values.Set(ParamMetric, req.name)
	if req.topN != "" {
		values.Set(ParamNumResults, req.topN)
	}

	from, to, ts := fromLegacyDateShift(req.date, req.shift, req.interval)
	values.Set(ParamFromTime, strconv.Itoa(int(from.Unix())))
	values.Set(ParamToTime, strconv.Itoa(int(to.Unix())))
	for _, s := range ts {
		values.Add(ParamTimeShift, strconv.Itoa(int(toSec(s))))
	}

	for _, gb := range req.groupBy {
		by := strings.TrimSpace(gb)
		if by != "shift" {
			values.Add(ParamQueryBy, by)
		}
	}

	for key, keyValues := range req.keys {
		for _, value := range strings.Split(keyValues, ",") {
			if tr := strings.TrimSpace(value); tr != "" {
				if tr == format.TagValueNullLegacy {
					// hacky workaround: for compatibility, "" from PHP is stored as "null" in old cluster and 0 in new cluster
					// here, we are hoping that case of writing literal "null" to new cluster is rare,
					// and redirect "null" to 0
					values.Add(ParamQueryFilter, key+queryFilterInSep+format.CodeTagValue(format.TagValueIDUnspecified))
				} else {
					values.Add(ParamQueryFilter, key+queryFilterInSep+tr)
				}
			}
		}
	}
	if req.devStaging != "" {
		values.Add(ParamQueryFilter, format.TagID(0)+queryFilterInSep+"staging")
	}
	// do not add 'production' because legacy production shows sum of staging and production

	switch req.function {
	case "count":
		values.Set(ParamQueryWhat, format.ParamQueryFnCount)
	case "unique":
		values.Set(ParamQueryWhat, format.ParamQueryFnUnique)
	case "accumulate":
		values.Set(ParamQueryWhat, format.ParamQueryFnCumulCount)
	case "avg":
		values.Set(ParamQueryWhat, format.ParamQueryFnAvg)
	case "max":
		values.Set(ParamQueryWhat, format.ParamQueryFnMax)
	case "min":
		values.Set(ParamQueryWhat, format.ParamQueryFnMin)
	case "sum":
		values.Set(ParamQueryWhat, format.ParamQueryFnSum)
	case "sum_accum":
		values.Set(ParamQueryWhat, format.ParamQueryFnCumulSum)
	}

	switch req.percentiles {
	case "p500":
		values.Set(ParamQueryWhat, format.ParamQueryFnP50)
	case "p750":
		values.Set(ParamQueryWhat, format.ParamQueryFnP75)
	case "p900":
		values.Set(ParamQueryWhat, format.ParamQueryFnP90)
	case "p950":
		values.Set(ParamQueryWhat, format.ParamQueryFnP95)
	case "p990":
		values.Set(ParamQueryWhat, format.ParamQueryFnP99)
	case "p999":
		values.Set(ParamQueryWhat, format.ParamQueryFnP999)
	}

	switch req.interval {
	case "1m":
		values.Set(uiParamCustomAgg, strconv.Itoa(int(toSec(1*time.Minute))))
	case "5m":
		values.Set(uiParamCustomAgg, strconv.Itoa(int(toSec(5*time.Minute))))
	case "15m":
		values.Set(uiParamCustomAgg, strconv.Itoa(int(toSec(15*time.Minute))))
	case "1h":
		values.Set(uiParamCustomAgg, strconv.Itoa(int(toSec(1*time.Hour))))
	case "24h":
		values.Set(uiParamCustomAgg, strconv.Itoa(int(toSec(24*time.Hour))))
	}

	return &url.URL{
		Path:     viewPath,
		RawQuery: values.Encode(),
	}
}

func fromLegacyDateShift(date string, shift string, interval string) (time.Time, time.Time, []time.Duration) {
	const dateFormat = "2006-1-2"
	day, err := time.Parse(dateFormat, date)
	if err != nil {
		todayDate := time.Now().Format(dateFormat)
		day, _ = time.Parse(dateFormat, todayDate)
	}

	y, m, d := day.Date()
	to := time.Date(y, m, d+1, 0, 0, 0, 0, day.Location())
	if now := time.Now(); to.After(now) {
		to = now
	}

	var (
		shiftIsTimeRange = strings.HasPrefix(shift, "all_")
		shiftParts       = strings.Split(shift, ",")

		from time.Time
		ts   []time.Duration
	)
	if interval == "24h" {
		from = to.Add(-30 * 24 * time.Hour)
		if shiftIsTimeRange {
			switch shift {
			case "all_1":
				from = to.Add(-30 * 24 * time.Hour) // close enough
			case "all_2":
				from = to.Add(-90 * 24 * time.Hour) // close enough
			case "all_6":
				from = to.Add(-180 * 24 * time.Hour) // close enough
			case "all_12":
				from = to.Add(-365 * 24 * time.Hour)
			case "all_24":
				from = to.Add(-2 * 365 * 24 * time.Hour)
			}
		} else {
			for _, s := range shiftParts {
				switch strings.TrimSpace(s) {
				case "0":
					// do nothing
				case "1":
					ts = append(ts, -28*24*time.Hour) // close enough
				}
			}
		}
	} else {
		from = to.Add(-1 * 24 * time.Hour)
		if shiftIsTimeRange {
			switch shift {
			case "all_3":
				from = to.Add(-3 * 24 * time.Hour)
			case "all_7":
				from = to.Add(-7 * 24 * time.Hour)
			case "all_14":
				from = to.Add(-14 * 24 * time.Hour)
			case "all_28":
				from = to.Add(-30 * 24 * time.Hour) // close enough
			case "all_42", "all_56":
				from = to.Add(-90 * 24 * time.Hour) // close enough
			}
		} else {
			for _, s := range shiftParts {
				switch strings.TrimSpace(s) {
				case "0":
					// do nothing
				case "1":
					ts = append(ts, -1*24*time.Hour)
				case "2":
					ts = append(ts, -2*24*time.Hour)
				case "7":
					ts = append(ts, -7*24*time.Hour)
				case "14":
					ts = append(ts, -14*24*time.Hour)
				case "21":
					ts = append(ts, -21*24*time.Hour)
				case "28":
					ts = append(ts, -28*24*time.Hour)
				}
			}
		}
	}

	return from, to, ts
}
