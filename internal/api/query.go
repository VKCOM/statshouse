// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"

	"github.com/vkcom/statshouse/internal/api/model"
	"github.com/vkcom/statshouse/internal/format"
)

const (
	// not valid characters in tag names
	queryFilterInSep    = "-"
	queryFilterNotInSep = "~"
)

const (
	ParamQueryFnCount                = "count"
	ParamQueryFnCountNorm            = "count_norm"
	ParamQueryFnCumulCount           = "cu_count"
	ParamQueryFnCardinality          = "cardinality"
	ParamQueryFnCardinalityNorm      = "cardinality_norm"
	ParamQueryFnCumulCardinality     = "cu_cardinality"
	ParamQueryFnMin                  = "min"
	ParamQueryFnMax                  = "max"
	ParamQueryFnAvg                  = "avg"
	ParamQueryFnCumulAvg             = "cu_avg"
	ParamQueryFnSum                  = "sum"
	ParamQueryFnSumNorm              = "sum_norm"
	ParamQueryFnCumulSum             = "cu_sum"
	ParamQueryFnStddev               = "stddev"
	ParamQueryFnP0_1                 = "p0_1"
	ParamQueryFnP1                   = "p1"
	ParamQueryFnP5                   = "p5"
	ParamQueryFnP10                  = "p10"
	ParamQueryFnP25                  = "p25"
	ParamQueryFnP50                  = "p50"
	ParamQueryFnP75                  = "p75"
	ParamQueryFnP90                  = "p90"
	ParamQueryFnP95                  = "p95"
	ParamQueryFnP99                  = "p99"
	ParamQueryFnP999                 = "p999"
	ParamQueryFnUnique               = "unique"
	ParamQueryFnUniqueNorm           = "unique_norm"
	ParamQueryFnMaxHost              = "max_host"
	ParamQueryFnMaxCountHost         = "max_count_host"
	ParamQueryFnDerivativeCount      = "dv_count"
	ParamQueryFnDerivativeCountNorm  = "dv_count_norm"
	ParamQueryFnDerivativeSum        = "dv_sum"
	ParamQueryFnDerivativeSumNorm    = "dv_sum_norm"
	ParamQueryFnDerivativeAvg        = "dv_avg"
	ParamQueryFnDerivativeMin        = "dv_min"
	ParamQueryFnDerivativeMax        = "dv_max"
	ParamQueryFnDerivativeUnique     = "dv_unique"
	ParamQueryFnDerivativeUniqueNorm = "dv_unique_norm"
)

func normalizedQueryString(
	metricWithNamespace string,
	kind model.QueryFnKind,
	by []string,
	filterIn map[string][]string,
	filterNoIn map[string][]string,
	orderBy bool,
) string {
	sortedBy := append([]string(nil), by...)
	sort.Strings(sortedBy)

	var sortedFilter []string
	for k, vv := range filterIn {
		for _, v := range vv {
			sortedFilter = append(sortedFilter, k+queryFilterInSep+v)
		}
	}
	for k, vv := range filterNoIn {
		for _, v := range vv {
			sortedFilter = append(sortedFilter, k+queryFilterNotInSep+v)
		}
	}
	sort.Strings(sortedFilter)

	var buf strings.Builder
	buf.WriteString(ParamMetric)
	buf.WriteByte('=')
	buf.WriteString(url.QueryEscape(metricWithNamespace))
	buf.WriteByte('&')
	buf.WriteString(ParamQueryWhat)
	buf.WriteByte('=')
	buf.WriteString(url.QueryEscape(string(kind)))
	for _, b := range sortedBy {
		buf.WriteByte('&')
		buf.WriteString(ParamQueryBy)
		buf.WriteByte('=')
		buf.WriteString(url.QueryEscape(b))
	}
	for _, f := range sortedFilter {
		buf.WriteByte('&')
		buf.WriteString(ParamQueryFilter)
		buf.WriteByte('=')
		buf.WriteString(url.QueryEscape(f))
	}

	if orderBy {
		buf.WriteByte('&')
		buf.WriteString("order_by")
	}

	return buf.String()
}

type query struct {
	what     model.QueryFn
	whatKind model.QueryFnKind
	by       []string
}

func parseQueries(version string, whats, by []string, maxHost bool) ([]*query, error) {
	qq := make([]*query, 0, len(whats))
	for _, what := range whats {
		fn, kind, err := parseQueryWhat(what, maxHost)
		if err != nil {
			return nil, err
		}

		q := &query{
			what:     fn,
			whatKind: kind,
		}

		for _, b := range by {
			k, err := parseTagID(b)
			if err != nil {
				return nil, err
			}
			if version == Version1 && b == format.EnvTagID {
				continue // we only support production tables for v1
			}
			q.by = append(q.by, k)
		}

		qq = append(qq, q)

		if version == Version1 && len(qq) > 1 {
			return nil, model.HttpErr(http.StatusBadRequest, fmt.Errorf("version 1 doesn't support multiple functions, %d given", len(whats)))
		}

		if len(qq) > maxFunctions {
			return nil, model.HttpErr(http.StatusBadRequest, fmt.Errorf("too many functions specified (%v, max=%v)", len(whats), maxFunctions))
		}
	}

	return qq, nil
}

func parseFromRows(fromRows string) (model.RowMarker, error) {
	res := model.RowMarker{}
	if fromRows == "" {
		return res, nil
	}
	var buf []byte
	if len(buf) < len(fromRows) {
		buf = make([]byte, len(fromRows))
	}
	n, err := base64.RawURLEncoding.Decode(buf, []byte(fromRows))
	if err != nil {
		return res, err
	}
	err = json.Unmarshal(buf[:n], &res)
	if err != nil {
		return res, err
	}
	return res, nil
}

func encodeFromRows(row *RowMarker) (string, error) {
	jsonBytes, err := json.Marshal(row)
	if err != nil {
		return "", err
	}

	return base64.RawURLEncoding.EncodeToString(jsonBytes), nil
}

func parseQueryWhat(what string, maxHost bool) (model.QueryFn, model.QueryFnKind, error) {
	fn, ok := model.ValidQueryFn(what)
	if !ok {
		return 0, "", model.HttpErr(http.StatusBadRequest, fmt.Errorf("invalid %q value: %q", ParamQueryWhat, what))
	}
	return fn, model.QueryFnToQueryFnKind(fn, maxHost), nil
}

func validateQuery(metricMeta *format.MetricMetaValue, version string) error {
	if _, ok := format.BuiltinMetrics[metricMeta.MetricID]; ok && version != Version2 {
		return model.HttpErr(http.StatusBadRequest, fmt.Errorf("can't use builtin metric %q with version %q", metricMeta.Name, version))
	}
	return nil
}

func parseQueryFilter(filter []string) (map[string][]string, map[string][]string, error) {
	filterIn := map[string][]string{}
	filterNotIn := map[string][]string{}

	for _, f := range filter {
		inIx := strings.Index(f, queryFilterInSep)
		notInIx := strings.Index(f, queryFilterNotInSep)
		switch {
		case inIx == -1 && notInIx == -1:
			return nil, nil, model.HttpErr(http.StatusBadRequest, fmt.Errorf("invalid %q value: %q", ParamQueryFilter, f))
		case inIx != -1 && (notInIx == -1 || inIx < notInIx):
			ks := f[:inIx]
			k, err := parseTagID(ks)
			if err != nil {
				return nil, nil, err
			}
			v := f[inIx+1:]
			if !format.ValidTagValueForAPI(v) {
				return nil, nil, model.HttpErr(http.StatusBadRequest, fmt.Errorf("invalid %q filter: %q", k, v))
			}
			filterIn[k] = append(filterIn[k], v)
		default:
			ks := f[:notInIx]
			k, err := parseTagID(ks)
			if err != nil {
				return nil, nil, err
			}
			v := f[notInIx+1:]
			if !format.ValidTagValueForAPI(v) {
				return nil, nil, model.HttpErr(http.StatusBadRequest, fmt.Errorf("invalid %q not-filter: %q", k, v))
			}
			filterNotIn[k] = append(filterNotIn[k], v)
		}
	}

	return filterIn, filterNotIn, nil
}
