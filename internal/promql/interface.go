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
	Avg         = "avg"
	Count       = "count"
	Max         = "max"
	Min         = "min"
	Sum         = "sum"
	StdDev      = "stddev"
	StdVar      = "stdvar"
	P25         = "p25"
	P50         = "p50"
	P75         = "p75"
	P90         = "p90"
	P95         = "p95"
	P99         = "p99"
	P999        = "p999"
	Cardinality = "cardinality"
	Unique      = "unique"
	MaxHost     = "maxhost"

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
	DigestCardinality
	DigestUnique
)

var NilValue = math.Float64frombits(NilValueBits)

type LOD struct {
	Len, Step int64
}

type SeriesQuery struct {
	// What
	Meta    *format.MetricMetaValue
	What    DigestWhat
	MaxHost bool

	// When
	From int64
	LODs []LOD

	// Grouping
	GroupBy []string

	// Filtering
	FilterIn   [format.MaxTags]map[int32]string // tagIx -> tagValueID -> tagValue
	FilterOut  [format.MaxTags]map[int32]string // as above
	SFilterIn  []string
	SFilterOut []string

	// Transformations
	Factor     int64
	Accumulate bool

	Options Options
}

type RichTagValueQuery struct {
	Version    string
	Meta       *format.MetricMetaValue
	TagID      string
	TagValueID int32
}

type TagValuesQuery struct {
	Version string
	Meta    *format.MetricMetaValue
	Start   int64
	End     int64
	TagX    int
}

type Handler interface {
	//
	// # Tag mapping
	//

	GetTagValue(tagValueID int32) string
	GetRichTagValue(qry RichTagValueQuery) string
	GetTagValueID(tagValue string) (int32, error)

	//
	// # Metric Metadata
	//

	MatchMetrics(ctx context.Context, matcher *labels.Matcher) ([]*format.MetricMetaValue, []string, error)
	GetQueryLODs(qry Query, maxOffset map[*format.MetricMetaValue]int64) ([]LOD, int64)

	//
	// # Storage
	//

	QuerySeries(ctx context.Context, qry *SeriesQuery) (SeriesBag, func(), error)
	QueryTagValues(ctx context.Context, qry TagValuesQuery) ([]int32, error)
	QuerySTagValues(ctx context.Context, qry TagValuesQuery) ([]string, error)

	//
	// # Allocator
	//

	Alloc(int) *[]float64
	Free(*[]float64)
}
