// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"github.com/vkcom/statshouse/internal/format"
	"pgregory.net/rand"
)

type Query struct {
	Start int64 // inclusive
	End   int64 // exclusive
	Step  int64
	Expr  string

	Options Options // StatsHouse specific
}

// NB! If you add an option make sure that default Options{} corresponds to Prometheus behavior.
type Options struct {
	Version          string
	AvoidCache       bool
	TimeNow          int64
	ScreenWidth      int64
	Collapse         bool // aka "point" query
	Extend           bool
	TagWhat          bool
	TagOffset        bool
	TagTotal         bool
	ExplicitGrouping bool
	MinHost          bool
	MaxHost          bool
	QuerySequential  bool
	Offsets          []int64
	Limit            int
	Rand             *rand.Rand
	Vars             map[string]Variable

	ExprQueriesSingleMetricCallback MetricMetaValueCallback
}

type (
	MetricMetaValueCallback func(*format.MetricMetaValue)
	SeriesQueryCallback     func(version string, key string, pq any, lod any, avoidCache bool)
)

type Variable struct {
	Value  []string
	Group  bool
	Negate bool
}
