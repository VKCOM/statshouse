// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package promql

import (
	"context"
	"fmt"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/promql/model"
)

type Handler interface {
	model.Allocator
	model.TagMapper

	//
	// # Metric Metadata
	//

	MatchMetrics(ctx context.Context, matcher *labels.Matcher) ([]*format.MetricMetaValue, []string, error)
	GetTimescale(qry model.Query, offsets map[*format.MetricMetaValue]int64) (model.Timescale, error)

	//
	// # Storage
	//

	QuerySeries(ctx context.Context, qry *model.SeriesQuery) (model.Series, error)
	QueryTagValueIDs(ctx context.Context, qry model.TagValuesQuery) ([]int32, error)
	QueryStringTop(ctx context.Context, qry model.TagValuesQuery) ([]string, error)
}

// Used by 'Handler' implementation to signal that entity requested was just not found
var ErrNotFound = fmt.Errorf("not found")
