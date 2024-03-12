// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"fmt"
	"net/http"

	jlexer "github.com/mailru/easyjson/jlexer"
	"github.com/mailru/easyjson/jwriter"
)

const (
	QueryFnUnspecified = QueryFn(iota)
	QueryFnCount
	QueryFnCountNorm
	QueryFnCumulCount
	QueryFnCardinality
	QueryFnCardinalityNorm
	QueryFnCumulCardinality
	QueryFnMin
	QueryFnMax
	QueryFnAvg
	QueryFnCumulAvg
	QueryFnSum
	QueryFnSumNorm
	QueryFnCumulSum
	QueryFnStddev
	QueryFnStdvar
	QueryFnP0_1
	QueryFnP1
	QueryFnP5
	QueryFnP10
	QueryFnP25
	QueryFnP50
	QueryFnP75
	QueryFnP90
	QueryFnP95
	QueryFnP99
	QueryFnP999
	QueryFnUnique
	QueryFnUniqueNorm
	QueryFnMaxHost
	QueryFnMaxCountHost
	QueryFnDerivativeCount
	QueryFnDerivativeCountNorm
	QueryFnDerivativeSum
	QueryFnDerivativeSumNorm
	QueryFnDerivativeAvg
	QueryFnDerivativeMin
	QueryFnDerivativeMax
	QueryFnDerivativeUnique
	QueryFnDerivativeUniqueNorm

	QueryFnKindCount          = QueryFnKind("count")
	QueryFnKindValue          = QueryFnKind("value")
	QueryFnKindPercentiles    = QueryFnKind("percentiles")
	QueryFnKindPercentilesLow = QueryFnKind("percentiles_low")
	QueryFnKindUnique         = QueryFnKind("unique")

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

	MaxTimeShifts = 10
)

type QueryFn int
type QueryFnKind string

type Metadata struct {
	UserEmail string `json:"user_email"`
	UserName  string `json:"user_name"`
	UserRef   string `json:"user_ref"`
}

type RowMarker struct {
	Time int64    `json:"time"`
	Tags []RawTag `json:"tags"`
	SKey string   `json:"skey"`
}

type RawTag struct {
	Index int   `json:"index"`
	Value int32 `json:"value"`
}

func ValidQueryFn(fn string) (QueryFn, bool) {
	switch fn {
	case ParamQueryFnCount:
		return QueryFnCount, true
	case ParamQueryFnCountNorm:
		return QueryFnCountNorm, true
	case ParamQueryFnCumulCount:
		return QueryFnCumulCount, true
	case ParamQueryFnCardinality:
		return QueryFnCardinality, true
	case ParamQueryFnCardinalityNorm:
		return QueryFnCardinalityNorm, true
	case ParamQueryFnCumulCardinality:
		return QueryFnCumulCardinality, true
	case ParamQueryFnMin:
		return QueryFnMin, true
	case ParamQueryFnMax:
		return QueryFnMax, true
	case ParamQueryFnAvg:
		return QueryFnAvg, true
	case ParamQueryFnCumulAvg:
		return QueryFnCumulAvg, true
	case ParamQueryFnSum:
		return QueryFnSum, true
	case ParamQueryFnSumNorm:
		return QueryFnSumNorm, true
	case ParamQueryFnCumulSum:
		return QueryFnCumulSum, true
	case ParamQueryFnStddev:
		return QueryFnStddev, true
	case ParamQueryFnP0_1:
		return QueryFnP0_1, true
	case ParamQueryFnP1:
		return QueryFnP1, true
	case ParamQueryFnP5:
		return QueryFnP5, true
	case ParamQueryFnP10:
		return QueryFnP10, true
	case ParamQueryFnP25:
		return QueryFnP25, true
	case ParamQueryFnP50:
		return QueryFnP50, true
	case ParamQueryFnP75:
		return QueryFnP75, true
	case ParamQueryFnP90:
		return QueryFnP90, true
	case ParamQueryFnP95:
		return QueryFnP95, true
	case ParamQueryFnP99:
		return QueryFnP99, true
	case ParamQueryFnP999:
		return QueryFnP999, true
	case ParamQueryFnUnique:
		return QueryFnUnique, true
	case ParamQueryFnUniqueNorm:
		return QueryFnUniqueNorm, true
	case ParamQueryFnMaxHost:
		return QueryFnMaxHost, true
	case ParamQueryFnMaxCountHost:
		return QueryFnMaxCountHost, true
	case ParamQueryFnDerivativeCount:
		return QueryFnDerivativeCount, true
	case ParamQueryFnDerivativeSum:
		return QueryFnDerivativeSum, true
	case ParamQueryFnDerivativeAvg:
		return QueryFnDerivativeAvg, true
	case ParamQueryFnDerivativeCountNorm:
		return QueryFnDerivativeCountNorm, true
	case ParamQueryFnDerivativeSumNorm:
		return QueryFnDerivativeSumNorm, true
	case ParamQueryFnDerivativeMin:
		return QueryFnDerivativeMin, true
	case ParamQueryFnDerivativeMax:
		return QueryFnDerivativeMax, true
	case ParamQueryFnDerivativeUnique:
		return QueryFnDerivativeUnique, true
	case ParamQueryFnDerivativeUniqueNorm:
		return QueryFnDerivativeUniqueNorm, true
	default:
		return QueryFnUnspecified, false
	}
}

func QueryFnToQueryFnKind(fn QueryFn, maxHost bool) QueryFnKind {
	switch fn {
	case QueryFnCount, QueryFnCountNorm, QueryFnCumulCount, QueryFnDerivativeCount, QueryFnDerivativeCountNorm,
		QueryFnCardinality, QueryFnCardinalityNorm, QueryFnCumulCardinality:
		if maxHost {
			return QueryFnKindValue
		}
		return QueryFnKindCount
	case QueryFnMin, QueryFnMax, QueryFnDerivativeMin, QueryFnDerivativeMax,
		QueryFnAvg, QueryFnCumulAvg, QueryFnDerivativeAvg,
		QueryFnSum, QueryFnSumNorm, QueryFnCumulSum, QueryFnDerivativeSum, QueryFnDerivativeSumNorm,
		QueryFnStddev, QueryFnMaxCountHost, QueryFnMaxHost:
		return QueryFnKindValue
	case QueryFnP0_1, QueryFnP1, QueryFnP5, QueryFnP10:
		return QueryFnKindPercentilesLow
	case QueryFnP25, QueryFnP50, QueryFnP75, QueryFnP90, QueryFnP95, QueryFnP99, QueryFnP999:
		return QueryFnKindPercentiles
	case QueryFnUnique, QueryFnUniqueNorm, QueryFnDerivativeUnique, QueryFnDerivativeUniqueNorm:
		return QueryFnKindUnique
	default:
		return QueryFnKindCount
	}
}

func (fn QueryFn) MarshalEasyJSON(w *jwriter.Writer) {
	w.String(fn.String())
}

func (fn *QueryFn) UnmarshalEasyJSON(w *jlexer.Lexer) {
	var err error
	*fn, _, err = parseQueryWhat(w.String(), false)
	if err != nil {
		w.AddError(err)
	}
}

func parseQueryWhat(what string, maxHost bool) (QueryFn, QueryFnKind, error) {
	fn, ok := ValidQueryFn(what)
	if !ok {
		return 0, "", HttpErr(http.StatusBadRequest, fmt.Errorf("invalid query function value: %q", what))
	}
	return fn, QueryFnToQueryFnKind(fn, maxHost), nil
}

func (fn QueryFn) String() string {
	switch fn {
	case QueryFnUnspecified:
		return ""
	case QueryFnCount:
		return ParamQueryFnCount
	case QueryFnCountNorm:
		return ParamQueryFnCountNorm
	case QueryFnCumulCount:
		return ParamQueryFnCumulCount
	case QueryFnCardinality:
		return ParamQueryFnCardinality
	case QueryFnCardinalityNorm:
		return ParamQueryFnCardinalityNorm
	case QueryFnCumulCardinality:
		return ParamQueryFnCumulCardinality
	case QueryFnMin:
		return ParamQueryFnMin
	case QueryFnMax:
		return ParamQueryFnMax
	case QueryFnAvg:
		return ParamQueryFnAvg
	case QueryFnCumulAvg:
		return ParamQueryFnCumulAvg
	case QueryFnSum:
		return ParamQueryFnSum
	case QueryFnSumNorm:
		return ParamQueryFnSumNorm
	case QueryFnCumulSum:
		return ParamQueryFnCumulSum
	case QueryFnStddev:
		return ParamQueryFnStddev
	case QueryFnP0_1:
		return "p0.1"
	case QueryFnP1:
		return ParamQueryFnP1
	case QueryFnP5:
		return ParamQueryFnP5
	case QueryFnP10:
		return ParamQueryFnP10
	case QueryFnP25:
		return ParamQueryFnP25
	case QueryFnP50:
		return ParamQueryFnP50
	case QueryFnP75:
		return ParamQueryFnP75
	case QueryFnP90:
		return ParamQueryFnP90
	case QueryFnP95:
		return ParamQueryFnP95
	case QueryFnP99:
		return ParamQueryFnP99
	case QueryFnP999:
		return ParamQueryFnP999
	case QueryFnUnique:
		return ParamQueryFnUnique
	case QueryFnUniqueNorm:
		return ParamQueryFnUniqueNorm
	case QueryFnMaxHost:
		return ParamQueryFnMaxHost
	case QueryFnMaxCountHost:
		return ParamQueryFnMaxCountHost
	case QueryFnDerivativeCount:
		return ParamQueryFnDerivativeCount
	case QueryFnDerivativeSum:
		return ParamQueryFnDerivativeSum
	case QueryFnDerivativeAvg:
		return ParamQueryFnDerivativeAvg
	case QueryFnDerivativeMin:
		return ParamQueryFnDerivativeMin
	case QueryFnDerivativeMax:
		return ParamQueryFnDerivativeMax
	case QueryFnDerivativeUnique:
		return ParamQueryFnDerivativeUnique
	case QueryFnDerivativeUniqueNorm:
		return ParamQueryFnDerivativeUniqueNorm
	case QueryFnDerivativeCountNorm:
		return ParamQueryFnDerivativeCountNorm
	case QueryFnDerivativeSumNorm:
		return ParamQueryFnDerivativeSumNorm
	default:
		return fmt.Sprintf("fn-%d", fn)
	}
}
