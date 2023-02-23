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
	"sort"
	"strings"

	"github.com/mailru/easyjson/jlexer"
	"github.com/mailru/easyjson/jwriter"

	"github.com/vkcom/statshouse/internal/format"
)

type (
	queryFn     int
	queryFnKind string
)

const (
	// not valid characters in tag names
	queryFilterInSep    = "-"
	queryFilterNotInSep = "~"

	queryFnKindCount       = queryFnKind("count")
	queryFnKindValue       = queryFnKind("value")
	queryFnKindPercentiles = queryFnKind("percentiles")
	queryFnKindUnique      = queryFnKind("unique")
)

const (
	// consecutive integer values for fast selectTSValue
	queryFnCount = queryFn(iota)
	queryFnCountNorm
	queryFnCumulCount
	queryFnCardinality
	queryFnCardinalityNorm
	queryFnCumulCardinality
	queryFnMin
	queryFnMax
	queryFnAvg
	queryFnCumulAvg
	queryFnSum
	queryFnSumNorm
	queryFnCumulSum
	queryFnStddev
	queryFnP25
	queryFnP50
	queryFnP75
	queryFnP90
	queryFnP95
	queryFnP99
	queryFnP999
	queryFnUnique
	queryFnUniqueNorm
	queryFnMaxHost
	queryFnMaxCountHost
	queryFnDerivativeCount
	queryFnDerivativeCountNorm
	queryFnDerivativeSum
	queryFnDerivativeSumNorm
	queryFnDerivativeAvg
	queryFnDerivativeMin
	queryFnDerivativeMax
	queryFnDerivativeUnique
	queryFnDerivativeUniqueNorm

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

func validQueryFn(fn string) (queryFn, bool) {
	switch fn {
	case ParamQueryFnCount:
		return queryFnCount, true
	case ParamQueryFnCountNorm:
		return queryFnCountNorm, true
	case ParamQueryFnCumulCount:
		return queryFnCumulCount, true
	case ParamQueryFnCardinality:
		return queryFnCardinality, true
	case ParamQueryFnCardinalityNorm:
		return queryFnCardinalityNorm, true
	case ParamQueryFnCumulCardinality:
		return queryFnCumulCardinality, true
	case ParamQueryFnMin:
		return queryFnMin, true
	case ParamQueryFnMax:
		return queryFnMax, true
	case ParamQueryFnAvg:
		return queryFnAvg, true
	case ParamQueryFnCumulAvg:
		return queryFnCumulAvg, true
	case ParamQueryFnSum:
		return queryFnSum, true
	case ParamQueryFnSumNorm:
		return queryFnSumNorm, true
	case ParamQueryFnCumulSum:
		return queryFnCumulSum, true
	case ParamQueryFnStddev:
		return queryFnStddev, true
	case ParamQueryFnP25:
		return queryFnP25, true
	case ParamQueryFnP50:
		return queryFnP50, true
	case ParamQueryFnP75:
		return queryFnP75, true
	case ParamQueryFnP90:
		return queryFnP90, true
	case ParamQueryFnP95:
		return queryFnP95, true
	case ParamQueryFnP99:
		return queryFnP99, true
	case ParamQueryFnP999:
		return queryFnP999, true
	case ParamQueryFnUnique:
		return queryFnUnique, true
	case ParamQueryFnUniqueNorm:
		return queryFnUniqueNorm, true
	case ParamQueryFnMaxHost:
		return queryFnMaxHost, true
	case ParamQueryFnMaxCountHost:
		return queryFnMaxCountHost, true
	case ParamQueryFnDerivativeCount:
		return queryFnDerivativeCount, true
	case ParamQueryFnDerivativeSum:
		return queryFnDerivativeSum, true
	case ParamQueryFnDerivativeAvg:
		return queryFnDerivativeAvg, true
	case ParamQueryFnDerivativeCountNorm:
		return queryFnDerivativeCountNorm, true
	case ParamQueryFnDerivativeSumNorm:
		return queryFnDerivativeSumNorm, true
	case ParamQueryFnDerivativeMin:
		return queryFnDerivativeMin, true
	case ParamQueryFnDerivativeMax:
		return queryFnDerivativeMax, true
	case ParamQueryFnDerivativeUnique:
		return queryFnDerivativeUnique, true
	case ParamQueryFnDerivativeUniqueNorm:
		return queryFnDerivativeUniqueNorm, true
	default:
		return 0, false
	}
}

func queryFnToQueryFnKind(fn queryFn, maxHost bool) queryFnKind {
	switch fn {
	case queryFnCount, queryFnCountNorm, queryFnCumulCount, queryFnDerivativeCount, queryFnDerivativeCountNorm,
		queryFnCardinality, queryFnCardinalityNorm, queryFnCumulCardinality:
		if maxHost {
			return queryFnKindValue
		}
		return queryFnKindCount
	case queryFnMin, queryFnMax, queryFnDerivativeMin, queryFnDerivativeMax,
		queryFnAvg, queryFnCumulAvg, queryFnDerivativeAvg,
		queryFnSum, queryFnSumNorm, queryFnCumulSum, queryFnDerivativeSum, queryFnDerivativeSumNorm,
		queryFnStddev, queryFnMaxCountHost, queryFnMaxHost:
		return queryFnKindValue
	case queryFnP25, queryFnP50, queryFnP75, queryFnP90, queryFnP95, queryFnP99, queryFnP999:
		return queryFnKindPercentiles
	case queryFnUnique, queryFnUniqueNorm, queryFnDerivativeUnique, queryFnDerivativeUniqueNorm:
		return queryFnKindUnique
	default:
		return queryFnKindCount
	}
}

func normalizedQueryString(
	metricWithNamespace string,
	kind queryFnKind,
	by []string,
	filterIn map[string][]string,
	filterNoIn map[string][]string,
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

	return buf.String()
}

type query struct {
	what     queryFn
	whatKind queryFnKind
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
			return nil, httpErr(http.StatusBadRequest, fmt.Errorf("version 1 doesn't support multiple functions, %d given", len(whats)))
		}

		if len(qq) > maxFunctions {
			return nil, httpErr(http.StatusBadRequest, fmt.Errorf("too many functions specified (%v, max=%v)", len(whats), maxFunctions))
		}
	}

	return qq, nil
}

func parseQueryWhat(what string, maxHost bool) (queryFn, queryFnKind, error) {
	fn, ok := validQueryFn(what)
	if !ok {
		return 0, "", httpErr(http.StatusBadRequest, fmt.Errorf("invalid %q value: %q", ParamQueryWhat, what))
	}
	return fn, queryFnToQueryFnKind(fn, maxHost), nil
}

func validateQuery(metricMeta *format.MetricMetaValue, version string) error {
	if _, ok := format.BuiltinMetrics[metricMeta.MetricID]; ok && version != Version2 {
		return httpErr(http.StatusBadRequest, fmt.Errorf("can't use builtin metric %q with version %q", metricMeta.Name, version))
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
			return nil, nil, httpErr(http.StatusBadRequest, fmt.Errorf("invalid %q value: %q", ParamQueryFilter, f))
		case inIx != -1 && (notInIx == -1 || inIx < notInIx):
			ks := f[:inIx]
			k, err := parseTagID(ks)
			if err != nil {
				return nil, nil, err
			}
			v := f[inIx+1:]
			if !format.ValidTagValueForAPI(v) {
				return nil, nil, httpErr(http.StatusBadRequest, fmt.Errorf("invalid %q filter: %q", k, v))
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
				return nil, nil, httpErr(http.StatusBadRequest, fmt.Errorf("invalid %q not-filter: %q", k, v))
			}
			filterNotIn[k] = append(filterNotIn[k], v)
		}
	}

	return filterIn, filterNotIn, nil
}

func (fn queryFn) String() string {
	switch fn {
	case queryFnCount:
		return ParamQueryFnCount
	case queryFnCountNorm:
		return ParamQueryFnCountNorm
	case queryFnCumulCount:
		return ParamQueryFnCumulCount
	case queryFnCardinality:
		return ParamQueryFnCardinality
	case queryFnCardinalityNorm:
		return ParamQueryFnCardinalityNorm
	case queryFnCumulCardinality:
		return ParamQueryFnCumulCardinality
	case queryFnMin:
		return ParamQueryFnMin
	case queryFnMax:
		return ParamQueryFnMax
	case queryFnAvg:
		return ParamQueryFnAvg
	case queryFnCumulAvg:
		return ParamQueryFnCumulAvg
	case queryFnSum:
		return ParamQueryFnSum
	case queryFnSumNorm:
		return ParamQueryFnSumNorm
	case queryFnCumulSum:
		return ParamQueryFnCumulSum
	case queryFnStddev:
		return ParamQueryFnStddev
	case queryFnP25:
		return ParamQueryFnP25
	case queryFnP50:
		return ParamQueryFnP50
	case queryFnP75:
		return ParamQueryFnP75
	case queryFnP90:
		return ParamQueryFnP90
	case queryFnP95:
		return ParamQueryFnP95
	case queryFnP99:
		return ParamQueryFnP99
	case queryFnP999:
		return ParamQueryFnP999
	case queryFnUnique:
		return ParamQueryFnUnique
	case queryFnUniqueNorm:
		return ParamQueryFnUniqueNorm
	case queryFnMaxHost:
		return ParamQueryFnMaxHost
	case queryFnMaxCountHost:
		return ParamQueryFnMaxCountHost
	case queryFnDerivativeCount:
		return ParamQueryFnDerivativeCount
	case queryFnDerivativeSum:
		return ParamQueryFnDerivativeSum
	case queryFnDerivativeAvg:
		return ParamQueryFnDerivativeAvg
	case queryFnDerivativeMin:
		return ParamQueryFnDerivativeMin
	case queryFnDerivativeMax:
		return ParamQueryFnDerivativeMax
	case queryFnDerivativeUnique:
		return ParamQueryFnDerivativeUnique
	case queryFnDerivativeUniqueNorm:
		return ParamQueryFnDerivativeUniqueNorm
	case queryFnDerivativeCountNorm:
		return ParamQueryFnDerivativeCountNorm
	case queryFnDerivativeSumNorm:
		return ParamQueryFnDerivativeSumNorm
	default:
		return fmt.Sprintf("fn-%d", fn)
	}
}

func (fn queryFn) MarshalEasyJSON(w *jwriter.Writer) {
	w.String(fn.String())
}

func (fn *queryFn) UnmarshalEasyJSON(w *jlexer.Lexer) {
	var err error
	*fn, _, err = parseQueryWhat(w.String(), false)
	if err != nil {
		w.AddError(err)
	}
}
