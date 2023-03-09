// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package promql

import (
	"context"
	"math"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/vkcom/statshouse/internal/format"
)

const (
	Avg      = "avg"
	AvgAcc   = "avgacc"
	Count    = "count"
	CountSec = "countsec"
	CountAcc = "countacc"
	Max      = "max"
	Min      = "min"
	Sum      = "sum"
	SumSec   = "sumsec"
	SumAcc   = "sumacc"
	StdDev   = "stddev"
	StdVar   = "stdvar"
	P25      = "p25"
	P50      = "p50"
	P75      = "p75"
	P90      = "p90"
	P95      = "p95"
	P99      = "p99"
	P999     = "p999"
	MaxHost  = "maxhost"

	NilValueBits = 0x7ff0000000000002
)

type DigestWhat int

const (
	DigestAvg DigestWhat = iota + 1
	DigestCount
	DigestMax
	DigestMin
	DigestSum
	DigestP25
	DigestP50
	DigestP75
	DigestP90
	DigestP95
	DigestP99
	DigestP999
	DigestStdDev
	DigestStdVar
)

var NilValue = math.Float64frombits(NilValueBits)

type LOD struct {
	Len, Step int64
}

type SeriesQuery struct {
	// what
	Meta    *format.MetricMetaValue
	What    DigestWhat
	MaxHost bool

	// when
	From int64
	LODs []LOD

	// grouping
	GroupBy []string

	// filtering
	FilterIn   [format.MaxTags]map[int32]string // tagIx -> tagValueID -> tagValue
	FilterOut  [format.MaxTags]map[int32]string // as above
	SFilterIn  []string
	SFilterOut []string

	// transformations
	Factor     int64
	Accumulate bool
}

type DataAccess interface {
	MatchMetrics(ctx context.Context, matcher *labels.Matcher) ([]*format.MetricMetaValue, []string, error)
	GetQueryLODs(qry Query, maxOffset map[*format.MetricMetaValue]int64, now int64) ([]LOD, error)

	GetTagValue(id int32) string
	GetTagValueID(val string) (int32, error)

	QuerySeries(ctx context.Context, qry *SeriesQuery) (*SeriesBag, func(), error)
	QueryTagValues(ctx context.Context, meta *format.MetricMetaValue, tagIx int, from, to int64) ([]int32, error)
	QuerySTagValues(ctx context.Context, meta *format.MetricMetaValue, from, to int64) ([]string, error)
}

type Allocator interface {
	Alloc(int) *[]float64
	Free(*[]float64)
}
