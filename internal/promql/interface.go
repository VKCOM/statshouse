// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package promql

import (
	"context"
	"fmt"
	"math"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/format"
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

var NilValue = math.Float64frombits(NilValueBits)

type SeriesQuery struct {
	// What
	Metric     *format.MetricMetaValue
	Whats      []SelectorWhat
	GroupBy    []string
	MinMaxHost [2]bool // "min" at [0], "max" at [1]

	// When
	Timescale data_model.Timescale
	Offset    int64

	// Filtering
	FilterIn   [format.MaxTags]map[string]int32 // tag index -> tag value -> tag value ID
	FilterOut  [format.MaxTags]map[string]int32 // as above
	SFilterIn  []string
	SFilterOut []string

	// Transformations
	Range     int64
	prefixSum bool

	Options Options
}

type SelectorWhat struct {
	Digest data_model.DigestWhat
	QueryF string
}

type TagValueQuery struct {
	Version    string
	Metric     *format.MetricMetaValue
	TagIndex   int
	TagID      string
	TagValueID int32
}

type TagValueIDQuery struct {
	Version  string
	Metric   *format.MetricMetaValue
	TagIndex int
	TagValue string
}

type TagValuesQuery struct {
	Metric    *format.MetricMetaValue
	TagIndex  int
	Timescale data_model.Timescale
	Offset    int64
	Options   Options
}

type Handler interface {
	//
	// # Tag mapping
	//

	GetHostName(hostID int32) string
	GetTagValue(qry TagValueQuery) string
	GetTagValueID(qry TagValueIDQuery) (int32, error)

	//
	// # Metric Metadata
	//

	MatchMetrics(ctx context.Context, matcher *labels.Matcher, namespace string) ([]*format.MetricMetaValue, error)

	//
	// # Storage
	//

	QuerySeries(ctx context.Context, qry *SeriesQuery) (Series, func(), error)
	QueryTagValueIDs(ctx context.Context, qry TagValuesQuery) ([]int32, error)
	QueryStringTop(ctx context.Context, qry TagValuesQuery) ([]string, error)

	//
	// # Allocator
	//

	Alloc(int) *[]float64
	Free(*[]float64)
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

func DigestWhatString(what data_model.DigestWhat) string {
	var res string
	switch what {
	case data_model.DigestAvg:
		res = Avg
	case data_model.DigestCount:
		res = Count
	case data_model.DigestCountSec:
		res = CountSec
	case data_model.DigestCountRaw:
		res = CountRaw
	case data_model.DigestMax:
		res = Max
	case data_model.DigestMin:
		res = Min
	case data_model.DigestSum:
		res = Sum
	case data_model.DigestSumSec:
		res = SumSec
	case data_model.DigestSumRaw:
		res = SumRaw
	case data_model.DigestStdDev:
		res = StdDev
	case data_model.DigestStdVar:
		res = StdVar
	case data_model.DigestP0_1:
		res = P0_1
	case data_model.DigestP1:
		res = P1
	case data_model.DigestP5:
		res = P5
	case data_model.DigestP10:
		res = P10
	case data_model.DigestP25:
		res = P25
	case data_model.DigestP50:
		res = P50
	case data_model.DigestP75:
		res = P75
	case data_model.DigestP90:
		res = P90
	case data_model.DigestP95:
		res = P95
	case data_model.DigestP99:
		res = P99
	case data_model.DigestP999:
		res = P999
	case data_model.DigestCardinality:
		res = Cardinality
	case data_model.DigestCardinalitySec:
		res = CardinalitySec
	case data_model.DigestCardinalityRaw:
		res = CardinalityRaw
	case data_model.DigestUnique:
		res = Unique
	case data_model.DigestUniqueSec:
		res = UniqueSec
	}
	return res
}
