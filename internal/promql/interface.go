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

type What int

const (
	Unspecified What = iota
	DigestAvg
	DigestCnt
	DigestMax
	DigestMin
	DigestSum
	DigestStdDev
	DigestCntAgg // DigestCnt aggregated over query interval
	CntPerSecond // DigestCnt divided by digest interval
	SumPerSecond // DigestSum divided by digest interval

	NilValueBits = 0x7ff0000000000002
)

var NilValue = math.Float64frombits(NilValueBits)

type LOD struct {
	Len, Step int64
}

type SeriesQuery struct {
	// what
	Meta *format.MetricMetaValue
	What What

	// when
	From  int64
	Range int64
	LODs  []LOD

	// grouping
	GroupBy []string

	// filtering
	FilterIn   [format.MaxTags]map[int32]string // tagIx -> tagValueID -> tagValue
	FilterOut  [format.MaxTags]map[int32]string // as above
	SFilterIn  []string
	SFilterOut []string

	OmitNameTag bool
}

type DataAccess interface {
	MatchMetrics(ctx context.Context, matcher *labels.Matcher) ([]*format.MetricMetaValue, error)
	GetQueryLODs(qry Query, maxOffset map[*format.MetricMetaValue]int64, now int64) ([]LOD, error)

	GetTagValue(id int32) string
	GetTagValueID(val string) (int32, error)

	QuerySeries(ctx context.Context, qry *SeriesQuery) (SeriesBag, func(), error)
	QueryTagValues(ctx context.Context, meta *format.MetricMetaValue, tagIx int, from, to int64) ([]int32, error)
	QuerySTagValues(ctx context.Context, meta *format.MetricMetaValue, from, to int64) ([]string, error)
}

type Allocator interface {
	Alloc(int) *[]float64
	Free(*[]float64)
}
