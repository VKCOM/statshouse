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

	jlexer "github.com/mailru/easyjson/jlexer"
	jwriter "github.com/mailru/easyjson/jwriter"
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouseApi"
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

type QueryFunc struct {
	Name  string
	What  data_model.DigestWhat
	Cumul bool
	Deriv bool
}

func ParseQueryFunc(str string, maxhost *bool) (QueryFunc, bool) {
	res := QueryFunc{Name: str}
	switch str {
	case ParamQueryFnCount:
		res.What = data_model.DigestCount
	case ParamQueryFnCountNorm:
		res.What = data_model.DigestCountSec
	case ParamQueryFnCumulCount:
		res.What = data_model.DigestCountRaw
		res.Cumul = true
	case ParamQueryFnCardinality:
		res.What = data_model.DigestCardinality
	case ParamQueryFnCardinalityNorm:
		res.What = data_model.DigestCardinalitySec
	case ParamQueryFnCumulCardinality:
		res.What = data_model.DigestCardinalityRaw
		res.Cumul = true
	case ParamQueryFnMin:
		res.What = data_model.DigestMin
	case ParamQueryFnMax:
		res.What = data_model.DigestMax
	case ParamQueryFnAvg:
		res.What = data_model.DigestAvg
	case ParamQueryFnCumulAvg:
		res.What = data_model.DigestAvg
		res.Cumul = true
	case ParamQueryFnSum:
		res.What = data_model.DigestSum
	case ParamQueryFnSumNorm:
		res.What = data_model.DigestSumSec
	case ParamQueryFnCumulSum:
		res.What = data_model.DigestSumRaw
		res.Cumul = true
	case ParamQueryFnStddev:
		res.What = data_model.DigestStdDev
	case ParamQueryFnP0_1:
		res.What = data_model.DigestP0_1
	case ParamQueryFnP1:
		res.What = data_model.DigestP1
	case ParamQueryFnP5:
		res.What = data_model.DigestP5
	case ParamQueryFnP10:
		res.What = data_model.DigestP10
	case ParamQueryFnP25:
		res.What = data_model.DigestP25
	case ParamQueryFnP50:
		res.What = data_model.DigestP50
	case ParamQueryFnP75:
		res.What = data_model.DigestP75
	case ParamQueryFnP90:
		res.What = data_model.DigestP90
	case ParamQueryFnP95:
		res.What = data_model.DigestP95
	case ParamQueryFnP99:
		res.What = data_model.DigestP99
	case ParamQueryFnP999:
		res.What = data_model.DigestP999
	case ParamQueryFnUnique:
		res.What = data_model.DigestUnique
	case ParamQueryFnUniqueNorm:
		res.What = data_model.DigestUniqueSec
	case ParamQueryFnMaxHost:
		if maxhost != nil {
			*maxhost = true
		}
	case ParamQueryFnMaxCountHost:
		res.What = data_model.DigestMax
		if maxhost != nil {
			*maxhost = true
		}
	case ParamQueryFnDerivativeCount:
		res.What = data_model.DigestCount
		res.Deriv = true
	case ParamQueryFnDerivativeSum:
		res.What = data_model.DigestSum
		res.Deriv = true
	case ParamQueryFnDerivativeAvg:
		res.What = data_model.DigestAvg
		res.Deriv = true
	case ParamQueryFnDerivativeCountNorm:
		res.What = data_model.DigestCountSec
		res.Deriv = true
	case ParamQueryFnDerivativeSumNorm:
		res.What = data_model.DigestSumSec
		res.Deriv = true
	case ParamQueryFnDerivativeMin:
		res.What = data_model.DigestMin
		res.Deriv = true
	case ParamQueryFnDerivativeMax:
		res.What = data_model.DigestMax
		res.Deriv = true
	case ParamQueryFnDerivativeUnique:
		res.What = data_model.DigestUnique
		res.Deriv = true
	case ParamQueryFnDerivativeUniqueNorm:
		res.What = data_model.DigestUniqueSec
		res.Deriv = true
	default:
		return QueryFunc{}, false
	}
	return res, true
}

func QueryFuncFromTLFunc(f tlstatshouseApi.Function, maxhost *bool) QueryFunc {
	var res QueryFunc
	switch f {
	case tlstatshouseApi.FnCount():
		res.Name = ParamQueryFnCount
		res.What = data_model.DigestCount
	case tlstatshouseApi.FnCountNorm():
		res.Name = ParamQueryFnCountNorm
		res.What = data_model.DigestCountSec
	case tlstatshouseApi.FnCumulCount():
		res.Name = ParamQueryFnCumulCount
		res.What = data_model.DigestCountRaw
		res.Cumul = true
	case tlstatshouseApi.FnMin():
		res.Name = ParamQueryFnMin
		res.What = data_model.DigestMin
	case tlstatshouseApi.FnMax():
		res.Name = ParamQueryFnMax
		res.What = data_model.DigestMax
	case tlstatshouseApi.FnAvg():
		res.Name = ParamQueryFnAvg
		res.What = data_model.DigestAvg
	case tlstatshouseApi.FnCumulAvg():
		res.Name = ParamQueryFnCumulAvg
		res.What = data_model.DigestAvg
		res.Cumul = true
	case tlstatshouseApi.FnSum():
		res.Name = ParamQueryFnSum
		res.What = data_model.DigestSum
	case tlstatshouseApi.FnSumNorm():
		res.Name = ParamQueryFnSumNorm
		res.What = data_model.DigestSumSec
	case tlstatshouseApi.FnCumulSum():
		res.Name = ParamQueryFnCumulSum
		res.What = data_model.DigestSumRaw
		res.Cumul = true
	case tlstatshouseApi.FnStddev():
		res.Name = ParamQueryFnStddev
		res.What = data_model.DigestStdDev
	case tlstatshouseApi.FnP01():
		res.Name = ParamQueryFnP0_1
		res.What = data_model.DigestP0_1
	case tlstatshouseApi.FnP1():
		res.Name = ParamQueryFnP1
		res.What = data_model.DigestP1
	case tlstatshouseApi.FnP5():
		res.Name = ParamQueryFnP5
		res.What = data_model.DigestP5
	case tlstatshouseApi.FnP10():
		res.Name = ParamQueryFnP10
		res.What = data_model.DigestP10
	case tlstatshouseApi.FnP25():
		res.Name = ParamQueryFnP25
		res.What = data_model.DigestP25
	case tlstatshouseApi.FnP50():
		res.Name = ParamQueryFnP50
		res.What = data_model.DigestP50
	case tlstatshouseApi.FnP75():
		res.Name = ParamQueryFnP75
		res.What = data_model.DigestP75
	case tlstatshouseApi.FnP90():
		res.Name = ParamQueryFnP90
		res.What = data_model.DigestP90
	case tlstatshouseApi.FnP95():
		res.Name = ParamQueryFnP95
		res.What = data_model.DigestP95
	case tlstatshouseApi.FnP99():
		res.Name = ParamQueryFnP99
		res.What = data_model.DigestP99
	case tlstatshouseApi.FnP999():
		res.Name = ParamQueryFnP999
		res.What = data_model.DigestP999
	case tlstatshouseApi.FnUnique():
		res.Name = ParamQueryFnUnique
		res.What = data_model.DigestUnique
	case tlstatshouseApi.FnUniqueNorm():
		res.Name = ParamQueryFnUniqueNorm
		res.What = data_model.DigestUniqueSec
	case tlstatshouseApi.FnMaxHost():
		res.Name = ParamQueryFnMaxHost
		if maxhost != nil {
			*maxhost = true
		}
	case tlstatshouseApi.FnMaxCountHost():
		res.Name = ParamQueryFnMaxCountHost
		res.What = data_model.DigestMax
		if maxhost != nil {
			*maxhost = true
		}
	case tlstatshouseApi.FnDerivativeCount():
		res.Name = ParamQueryFnDerivativeCount
		res.What = data_model.DigestCount
		res.Deriv = true
	case tlstatshouseApi.FnDerivativeSum():
		res.Name = ParamQueryFnDerivativeSum
		res.What = data_model.DigestSum
		res.Deriv = true
	case tlstatshouseApi.FnDerivativeAvg():
		res.Name = ParamQueryFnDerivativeAvg
		res.What = data_model.DigestAvg
		res.Deriv = true
	case tlstatshouseApi.FnDerivativeCountNorm():
		res.Name = ParamQueryFnDerivativeCountNorm
		res.What = data_model.DigestCountSec
		res.Deriv = true
	case tlstatshouseApi.FnDerivativeSumNorm():
		res.Name = ParamQueryFnDerivativeSumNorm
		res.What = data_model.DigestSumSec
		res.Deriv = true
	case tlstatshouseApi.FnDerivativeMin():
		res.Name = ParamQueryFnDerivativeMin
		res.What = data_model.DigestMin
		res.Deriv = true
	case tlstatshouseApi.FnDerivativeMax():
		res.Name = ParamQueryFnDerivativeMax
		res.What = data_model.DigestMax
		res.Deriv = true
	case tlstatshouseApi.FnDerivativeUnique():
		res.Name = ParamQueryFnDerivativeUnique
		res.What = data_model.DigestUnique
		res.Deriv = true
	case tlstatshouseApi.FnDerivativeUniqueNorm():
		res.Name = ParamQueryFnDerivativeUniqueNorm
		res.What = data_model.DigestUniqueSec
		res.Deriv = true
	}
	return res
}

func ParseTLFunc(str string) (tlstatshouseApi.Function, bool) {
	var res tlstatshouseApi.Function
	switch str {
	case ParamQueryFnCount:
		res = tlstatshouseApi.FnCount()
	case ParamQueryFnCountNorm:
		res = tlstatshouseApi.FnCountNorm()
	case ParamQueryFnCumulCount:
		res = tlstatshouseApi.FnCumulCount()
	case ParamQueryFnMin:
		res = tlstatshouseApi.FnMin()
	case ParamQueryFnMax:
		res = tlstatshouseApi.FnMax()
	case ParamQueryFnAvg:
		res = tlstatshouseApi.FnAvg()
	case ParamQueryFnCumulAvg:
		res = tlstatshouseApi.FnCumulAvg()
	case ParamQueryFnSum:
		res = tlstatshouseApi.FnSum()
	case ParamQueryFnSumNorm:
		res = tlstatshouseApi.FnSumNorm()
	case ParamQueryFnStddev:
		res = tlstatshouseApi.FnStddev()
	case ParamQueryFnP0_1:
		res = tlstatshouseApi.FnP01()
	case ParamQueryFnP1:
		res = tlstatshouseApi.FnP1()
	case ParamQueryFnP5:
		res = tlstatshouseApi.FnP5()
	case ParamQueryFnP10:
		res = tlstatshouseApi.FnP10()
	case ParamQueryFnP25:
		res = tlstatshouseApi.FnP25()
	case ParamQueryFnP50:
		res = tlstatshouseApi.FnP50()
	case ParamQueryFnP75:
		res = tlstatshouseApi.FnP75()
	case ParamQueryFnP90:
		res = tlstatshouseApi.FnP90()
	case ParamQueryFnP95:
		res = tlstatshouseApi.FnP95()
	case ParamQueryFnP99:
		res = tlstatshouseApi.FnP99()
	case ParamQueryFnP999:
		res = tlstatshouseApi.FnP999()
	case ParamQueryFnUnique:
		res = tlstatshouseApi.FnUnique()
	case ParamQueryFnUniqueNorm:
		res = tlstatshouseApi.FnUniqueNorm()
	case ParamQueryFnMaxHost:
		res = tlstatshouseApi.FnMaxHost()
	case ParamQueryFnMaxCountHost:
		res = tlstatshouseApi.FnMaxCountHost()
	case ParamQueryFnCumulSum:
		res = tlstatshouseApi.FnCumulSum()
	case ParamQueryFnDerivativeCount:
		res = tlstatshouseApi.FnDerivativeCount()
	case ParamQueryFnDerivativeCountNorm:
		res = tlstatshouseApi.FnDerivativeCountNorm()
	case ParamQueryFnDerivativeSum:
		res = tlstatshouseApi.FnDerivativeSum()
	case ParamQueryFnDerivativeSumNorm:
		res = tlstatshouseApi.FnDerivativeSumNorm()
	case ParamQueryFnDerivativeMin:
		res = tlstatshouseApi.FnDerivativeMin()
	case ParamQueryFnDerivativeMax:
		res = tlstatshouseApi.FnDerivativeMax()
	case ParamQueryFnDerivativeAvg:
		res = tlstatshouseApi.FnDerivativeAvg()
	case ParamQueryFnDerivativeUnique:
		res = tlstatshouseApi.FnDerivativeUnique()
	case ParamQueryFnDerivativeUniqueNorm:
		res = tlstatshouseApi.FnDerivativeUniqueNorm()
	default:
		return tlstatshouseApi.Function{}, false
	}
	return res, true
}

func (fn QueryFunc) MarshalEasyJSON(w *jwriter.Writer) {
	w.String(fn.Name)
}

func (fn *QueryFunc) UnmarshalEasyJSON(w *jlexer.Lexer) {
	s := w.String()
	var ok bool
	*fn, ok = ParseQueryFunc(s, nil)
	if !ok {
		w.AddError(fmt.Errorf("unrecognized query function: %q", s))
	}
}

func normalizedQueryString(
	metricName string,
	kind data_model.DigestKind,
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
	buf.WriteString(url.QueryEscape(metricName))
	buf.WriteByte('&')
	buf.WriteString(ParamQueryWhat)
	buf.WriteByte('=')
	buf.WriteString(url.QueryEscape(kind.String()))
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

func parseFromRows(fromRows string) (RowMarker, error) {
	res := RowMarker{}
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

func validateQuery(metricMeta *format.MetricMetaValue, version string) error {
	if _, ok := format.BuiltinMetrics[metricMeta.MetricID]; ok && version == Version1 {
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
