// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package promql

import (
	"context"
	"fmt"
	"math"

	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouseApi"
	"github.com/VKCOM/statshouse/internal/format"
	"github.com/mailru/easyjson/jlexer"
	"github.com/mailru/easyjson/jwriter"
)

const (
	Avg            = "avg"
	Count          = "count"
	CountSec       = "countsec"
	CountRaw       = "countraw"
	Max            = "max"
	Min            = "min"
	Sum            = "sum"
	SumSec         = "sumsec"
	SumRaw         = "sumraw"
	StdDev         = "stddev"
	StdVar         = "stdvar"
	P0_1           = "p0_1"
	P1             = "p1"
	P5             = "p5"
	P10            = "p10"
	P25            = "p25"
	P50            = "p50"
	P75            = "p75"
	P90            = "p90"
	P95            = "p95"
	P99            = "p99"
	P999           = "p999"
	Cardinality    = "cardinality"
	CardinalitySec = "cardinalitysec"
	CardinalityRaw = "cardinalityraw"
	Unique         = "unique"
	UniqueSec      = "uniquesec"
	MinHost        = "minhost"
	MaxHost        = "maxhost"

	NilValueBits = 0x7ff0000000000002
)

const (
	DigestUnspecified DigestWhat = iota
	DigestCount
	DigestCountSec
	DigestCountRaw
	DigestSum
	DigestSumSec
	DigestSumRaw
	DigestAvg
	DigestMin
	DigestMax
	DigestP0_1
	DigestP1
	DigestP5
	DigestP10
	DigestP25
	DigestP50
	DigestP75
	DigestP90
	DigestP95
	DigestP99
	DigestP999
	DigestStdDev
	DigestStdVar
	DigestCardinality
	DigestCardinalitySec
	DigestCardinalityRaw
	DigestUnique
	DigestUniqueSec
	DigestUniqueRaw
)

var NilValue = math.Float64frombits(NilValueBits)

type DigestWhat int

type SeriesQuery struct {
	// What
	Metric     *format.MetricMetaValue
	Whats      []SelectorWhat
	GroupBy    []int
	MinMaxHost [2]bool // "min" at [0], "max" at [1]

	// When
	Timescale data_model.Timescale
	Offset    int64

	// Filtering
	data_model.QueryFilter

	// Transformations
	Range     int64
	prefixSum bool

	Options Options
}

type SelectorWhat struct {
	Digest DigestWhat
	QueryF string
}

func ParseQueryFunc(str string, maxhost *bool) (SelectorWhat, bool) {
	res := SelectorWhat{QueryF: str}
	switch str {
	case format.ParamQueryFnCount:
		res.Digest = DigestCount
	case format.ParamQueryFnCountNorm:
		res.Digest = DigestCountSec
	case format.ParamQueryFnCumulCount:
		res.Digest = DigestCountRaw
	case format.ParamQueryFnCardinality:
		res.Digest = DigestCardinality
	case format.ParamQueryFnCardinalityNorm:
		res.Digest = DigestCardinalitySec
	case format.ParamQueryFnCumulCardinality:
		res.Digest = DigestCardinalityRaw
	case format.ParamQueryFnMin:
		res.Digest = DigestMin
	case format.ParamQueryFnMax:
		res.Digest = DigestMax
	case format.ParamQueryFnAvg:
		res.Digest = DigestAvg
	case format.ParamQueryFnCumulAvg:
		res.Digest = DigestAvg
	case format.ParamQueryFnSum:
		res.Digest = DigestSum
	case format.ParamQueryFnSumNorm:
		res.Digest = DigestSumSec
	case format.ParamQueryFnCumulSum:
		res.Digest = DigestSumRaw
	case format.ParamQueryFnStddev:
		res.Digest = DigestStdDev
	case format.ParamQueryFnP0_1:
		res.Digest = DigestP0_1
	case format.ParamQueryFnP1:
		res.Digest = DigestP1
	case format.ParamQueryFnP5:
		res.Digest = DigestP5
	case format.ParamQueryFnP10:
		res.Digest = DigestP10
	case format.ParamQueryFnP25:
		res.Digest = DigestP25
	case format.ParamQueryFnP50:
		res.Digest = DigestP50
	case format.ParamQueryFnP75:
		res.Digest = DigestP75
	case format.ParamQueryFnP90:
		res.Digest = DigestP90
	case format.ParamQueryFnP95:
		res.Digest = DigestP95
	case format.ParamQueryFnP99:
		res.Digest = DigestP99
	case format.ParamQueryFnP999:
		res.Digest = DigestP999
	case format.ParamQueryFnUnique:
		res.Digest = DigestUnique
	case format.ParamQueryFnUniqueNorm:
		res.Digest = DigestUniqueSec
	case format.ParamQueryFnMaxHost:
		if maxhost != nil {
			*maxhost = true
		}
	case format.ParamQueryFnMaxCountHost:
		res.Digest = DigestMax
		if maxhost != nil {
			*maxhost = true
		}
	case format.ParamQueryFnDerivativeCount:
		res.Digest = DigestCount
	case format.ParamQueryFnDerivativeSum:
		res.Digest = DigestSum
	case format.ParamQueryFnDerivativeAvg:
		res.Digest = DigestAvg
	case format.ParamQueryFnDerivativeCountNorm:
		res.Digest = DigestCountSec
	case format.ParamQueryFnDerivativeSumNorm:
		res.Digest = DigestSumSec
	case format.ParamQueryFnDerivativeMin:
		res.Digest = DigestMin
	case format.ParamQueryFnDerivativeMax:
		res.Digest = DigestMax
	case format.ParamQueryFnDerivativeUnique:
		res.Digest = DigestUnique
	case format.ParamQueryFnDerivativeUniqueNorm:
		res.Digest = DigestUniqueSec
	default:
		return SelectorWhat{}, false
	}
	return res, true
}

func QueryFuncFromTLFunc(f tlstatshouseApi.Function, maxhost *bool) SelectorWhat {
	var res SelectorWhat
	switch f {
	case tlstatshouseApi.FnCount():
		res.QueryF = format.ParamQueryFnCount
		res.Digest = DigestCount
	case tlstatshouseApi.FnCountNorm():
		res.QueryF = format.ParamQueryFnCountNorm
		res.Digest = DigestCountSec
	case tlstatshouseApi.FnCumulCount():
		res.QueryF = format.ParamQueryFnCumulCount
		res.Digest = DigestCountRaw
	case tlstatshouseApi.FnMin():
		res.QueryF = format.ParamQueryFnMin
		res.Digest = DigestMin
	case tlstatshouseApi.FnMax():
		res.QueryF = format.ParamQueryFnMax
		res.Digest = DigestMax
	case tlstatshouseApi.FnAvg():
		res.QueryF = format.ParamQueryFnAvg
		res.Digest = DigestAvg
	case tlstatshouseApi.FnCumulAvg():
		res.QueryF = format.ParamQueryFnCumulAvg
		res.Digest = DigestAvg
	case tlstatshouseApi.FnSum():
		res.QueryF = format.ParamQueryFnSum
		res.Digest = DigestSum
	case tlstatshouseApi.FnSumNorm():
		res.QueryF = format.ParamQueryFnSumNorm
		res.Digest = DigestSumSec
	case tlstatshouseApi.FnCumulSum():
		res.QueryF = format.ParamQueryFnCumulSum
		res.Digest = DigestSumRaw
	case tlstatshouseApi.FnStddev():
		res.QueryF = format.ParamQueryFnStddev
		res.Digest = DigestStdDev
	case tlstatshouseApi.FnP01():
		res.QueryF = format.ParamQueryFnP0_1
		res.Digest = DigestP0_1
	case tlstatshouseApi.FnP1():
		res.QueryF = format.ParamQueryFnP1
		res.Digest = DigestP1
	case tlstatshouseApi.FnP5():
		res.QueryF = format.ParamQueryFnP5
		res.Digest = DigestP5
	case tlstatshouseApi.FnP10():
		res.QueryF = format.ParamQueryFnP10
		res.Digest = DigestP10
	case tlstatshouseApi.FnP25():
		res.QueryF = format.ParamQueryFnP25
		res.Digest = DigestP25
	case tlstatshouseApi.FnP50():
		res.QueryF = format.ParamQueryFnP50
		res.Digest = DigestP50
	case tlstatshouseApi.FnP75():
		res.QueryF = format.ParamQueryFnP75
		res.Digest = DigestP75
	case tlstatshouseApi.FnP90():
		res.QueryF = format.ParamQueryFnP90
		res.Digest = DigestP90
	case tlstatshouseApi.FnP95():
		res.QueryF = format.ParamQueryFnP95
		res.Digest = DigestP95
	case tlstatshouseApi.FnP99():
		res.QueryF = format.ParamQueryFnP99
		res.Digest = DigestP99
	case tlstatshouseApi.FnP999():
		res.QueryF = format.ParamQueryFnP999
		res.Digest = DigestP999
	case tlstatshouseApi.FnUnique():
		res.QueryF = format.ParamQueryFnUnique
		res.Digest = DigestUnique
	case tlstatshouseApi.FnUniqueNorm():
		res.QueryF = format.ParamQueryFnUniqueNorm
		res.Digest = DigestUniqueSec
	case tlstatshouseApi.FnMaxHost():
		res.QueryF = format.ParamQueryFnMaxHost
		if maxhost != nil {
			*maxhost = true
		}
	case tlstatshouseApi.FnMaxCountHost():
		res.QueryF = format.ParamQueryFnMaxCountHost
		res.Digest = DigestMax
		if maxhost != nil {
			*maxhost = true
		}
	case tlstatshouseApi.FnDerivativeCount():
		res.QueryF = format.ParamQueryFnDerivativeCount
		res.Digest = DigestCount
	case tlstatshouseApi.FnDerivativeSum():
		res.QueryF = format.ParamQueryFnDerivativeSum
		res.Digest = DigestSum
	case tlstatshouseApi.FnDerivativeAvg():
		res.QueryF = format.ParamQueryFnDerivativeAvg
		res.Digest = DigestAvg
	case tlstatshouseApi.FnDerivativeCountNorm():
		res.QueryF = format.ParamQueryFnDerivativeCountNorm
		res.Digest = DigestCountSec
	case tlstatshouseApi.FnDerivativeSumNorm():
		res.QueryF = format.ParamQueryFnDerivativeSumNorm
		res.Digest = DigestSumSec
	case tlstatshouseApi.FnDerivativeMin():
		res.QueryF = format.ParamQueryFnDerivativeMin
		res.Digest = DigestMin
	case tlstatshouseApi.FnDerivativeMax():
		res.QueryF = format.ParamQueryFnDerivativeMax
		res.Digest = DigestMax
	case tlstatshouseApi.FnDerivativeUnique():
		res.QueryF = format.ParamQueryFnDerivativeUnique
		res.Digest = DigestUnique
	case tlstatshouseApi.FnDerivativeUniqueNorm():
		res.QueryF = format.ParamQueryFnDerivativeUniqueNorm
		res.Digest = DigestUniqueSec
	}
	return res
}

func (fn SelectorWhat) MarshalEasyJSON(w *jwriter.Writer) {
	w.String(fn.QueryF)
}

func (fn *SelectorWhat) UnmarshalEasyJSON(w *jlexer.Lexer) {
	s := w.String()
	var ok bool
	*fn, ok = ParseQueryFunc(s, nil)
	if !ok {
		w.AddError(fmt.Errorf("unrecognized query function: %q", s))
	}
}

type TagValueQuery struct {
	Version    string
	Metric     *format.MetricMetaValue
	TagIndex   int
	TagID      string
	TagValueID int64
}

type TagValueIDQuery struct {
	Version  string
	Tag      format.MetricMetaTag
	TagValue string
}

type TagValuesQuery struct {
	Metric    *format.MetricMetaValue
	Tag       format.MetricMetaTag
	Timescale data_model.Timescale
	Offset    int64
	Options   Options
}

type Handler interface {
	//
	// # Tag mapping
	//

	GetHostName(hostID int32) string
	GetHostName64(hostID int64) string
	GetTagValue(qry TagValueQuery) string
	GetTagValueID(qry TagValueIDQuery) (int64, error)
	GetTagFilter(metric *format.MetricMetaValue, tagIndex int, tagValue string) (data_model.TagValue, error)

	//
	// # Metric Metadata
	//

	MatchMetrics(f *data_model.QueryFilter) error

	//
	// # Storage
	//

	QuerySeries(ctx context.Context, qry *SeriesQuery) (Series, func(), error)
	QueryTagValueIDs(ctx context.Context, qry TagValuesQuery) ([]int64, error)

	//
	// # Allocator
	//

	Alloc(int) *[]float64
	Free(*[]float64)

	//
	// # Trace
	//

	Tracef(format string, a ...any)
}

// Used by 'Handler' implementation to signal that entity requested was just not found
var ErrNotFound = fmt.Errorf("not found")

// Wrapper for errors returned by 'Exec'
type Error struct {
	what  any
	panic bool
}

func (e Error) Error() string {
	return fmt.Sprintf("%v", e.what)
}

func (e Error) Unwrap() error {
	if err, ok := e.what.(error); ok {
		return err
	}
	return nil
}

func (e Error) EngineFailure() bool {
	return e.panic
}

func (what DigestWhat) String() string {
	var res string
	switch what {
	case DigestAvg:
		res = Avg
	case DigestCount:
		res = Count
	case DigestCountSec:
		res = CountSec
	case DigestCountRaw:
		res = CountRaw
	case DigestMax:
		res = Max
	case DigestMin:
		res = Min
	case DigestSum:
		res = Sum
	case DigestSumSec:
		res = SumSec
	case DigestSumRaw:
		res = SumRaw
	case DigestStdDev:
		res = StdDev
	case DigestStdVar:
		res = StdVar
	case DigestP0_1:
		res = P0_1
	case DigestP1:
		res = P1
	case DigestP5:
		res = P5
	case DigestP10:
		res = P10
	case DigestP25:
		res = P25
	case DigestP50:
		res = P50
	case DigestP75:
		res = P75
	case DigestP90:
		res = P90
	case DigestP95:
		res = P95
	case DigestP99:
		res = P99
	case DigestP999:
		res = P999
	case DigestCardinality:
		res = Cardinality
	case DigestCardinalitySec:
		res = CardinalitySec
	case DigestCardinalityRaw:
		res = CardinalityRaw
	case DigestUnique:
		res = Unique
	case DigestUniqueSec:
		res = UniqueSec
	}
	return res
}

func (what DigestWhat) DataModelDigestWhat() data_model.DigestWhat {
	var w data_model.DigestWhat
	switch what {
	case DigestAvg:
		w = data_model.DigestAvg
	case DigestCount, DigestCountSec, DigestCountRaw:
		w = data_model.DigestCount
	case DigestMax:
		w = data_model.DigestMax
	case DigestMin:
		w = data_model.DigestMin
	case DigestSum, DigestSumSec, DigestSumRaw:
		w = data_model.DigestSum
	case DigestStdDev, DigestStdVar:
		w = data_model.DigestStdDev
	case DigestP0_1, DigestP1, DigestP5, DigestP10, DigestP25, DigestP50, DigestP75, DigestP90, DigestP95, DigestP99, DigestP999:
		w = data_model.DigestPercentile
	case DigestCardinality, DigestCardinalitySec, DigestCardinalityRaw:
		w = data_model.DigestCardinality
	case DigestUnique, DigestUniqueSec:
		w = data_model.DigestUnique
	}
	return w
}

func (what DigestWhat) Selector() data_model.DigestSelector {
	var w data_model.DigestWhat
	var f float64
	switch what {
	case DigestAvg:
		w = data_model.DigestAvg
	case DigestCount, DigestCountSec, DigestCountRaw:
		w = data_model.DigestCount
	case DigestMax:
		w = data_model.DigestMax
	case DigestMin:
		w = data_model.DigestMin
	case DigestSum, DigestSumSec, DigestSumRaw:
		w = data_model.DigestSum
	case DigestStdDev, DigestStdVar:
		w = data_model.DigestStdDev
	case DigestP0_1:
		w = data_model.DigestPercentile
		f = 0.001
	case DigestP1:
		w = data_model.DigestPercentile
		f = 0.01
	case DigestP5:
		w = data_model.DigestPercentile
		f = 0.05
	case DigestP10:
		w = data_model.DigestPercentile
		f = 0.1
	case DigestP25:
		w = data_model.DigestPercentile
		f = 0.25
	case DigestP50:
		w = data_model.DigestPercentile
		f = 0.5
	case DigestP75:
		w = data_model.DigestPercentile
		f = 0.75
	case DigestP90:
		w = data_model.DigestPercentile
		f = 0.9
	case DigestP95:
		w = data_model.DigestPercentile
		f = 0.95
	case DigestP99:
		w = data_model.DigestPercentile
		f = 0.99
	case DigestP999:
		w = data_model.DigestPercentile
		f = 0.999
	case DigestCardinality, DigestCardinalitySec, DigestCardinalityRaw:
		w = data_model.DigestCardinality
	case DigestUnique, DigestUniqueSec:
		w = data_model.DigestUnique
	}
	return data_model.DigestSelector{What: w, Argument: f}
}
