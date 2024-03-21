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
	"strconv"

	"github.com/prometheus/prometheus/model/labels"
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

type DigestWhat int

const (
	DigestAvg DigestWhat = iota + 1
	DigestCount
	DigestCountSec
	DigestCountRaw
	DigestMax
	DigestMin
	DigestSum
	DigestSumSec
	DigestSumRaw
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
)

var NilValue = math.Float64frombits(NilValueBits)

type Timescale struct {
	Time       []int64
	LODs       []LOD
	Step       int64 // aggregation interval requested (former "desiredStepMul")
	StartX     int   // requested time interval starts at "Time[StartX]"
	ViewStartX int
	ViewEndX   int
}

type LOD struct {
	Step int64
	Len  int // number of elements LOD occupies in time array
}

type SeriesQuery struct {
	// What
	Metric     *format.MetricMetaValue
	Whats      []DigestWhat
	GroupBy    []string
	MinMaxHost [2]bool // "min" at [0], "max" at [1]

	// When
	Timescale Timescale
	Offset    int64

	// Filtering
	FilterIn   [format.MaxTags]map[int32]string // tag index -> tag value ID -> tag value
	FilterOut  [format.MaxTags]map[int32]string // as above
	SFilterIn  []string
	SFilterOut []string

	// Transformations
	Range int64

	Options Options
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
	Timescale Timescale
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

	MatchMetrics(ctx context.Context, matcher *labels.Matcher, namespace string) ([]*format.MetricMetaValue, []string, error)
	GetTimescale(qry Query, offsets map[*format.MetricMetaValue]int64) (Timescale, error)

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

func (t *Timescale) empty() bool {
	return t.StartX == len(t.Time)
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

func (w DigestWhat) String() string {
	switch w {
	case DigestAvg:
		return "avg"
	case DigestCount:
		return "count"
	case DigestCountSec:
		return "count_norm"
	case DigestCountRaw:
		return "count_raw"
	case DigestMax:
		return "max"
	case DigestMin:
		return "min"
	case DigestSum:
		return "sum"
	case DigestSumSec:
		return "sum_norm"
	case DigestSumRaw:
		return "sum_raw"
	case DigestP0_1:
		return "p0_1"
	case DigestP1:
		return "p1"
	case DigestP5:
		return "p5"
	case DigestP10:
		return "p10"
	case DigestP25:
		return "p25"
	case DigestP50:
		return "p50"
	case DigestP75:
		return "p75"
	case DigestP90:
		return "p90"
	case DigestP95:
		return "p95"
	case DigestP99:
		return "p99"
	case DigestP999:
		return "p999"
	case DigestStdDev:
		return "stddev"
	case DigestStdVar:
		return "stdvar"
	case DigestCardinality:
		return "cardinality"
	case DigestCardinalitySec:
		return "cardinality_norm"
	case DigestCardinalityRaw:
		return "cardinality_raw"
	case DigestUnique:
		return "unique"
	case DigestUniqueSec:
		return "unique_norm"
	default:
		return strconv.Itoa(int(w))
	}
}
