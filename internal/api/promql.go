// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"time"

	"github.com/prometheus/prometheus/promql/parser"

	"github.com/vkcom/statshouse/internal/promql"
)

type promAPIQuery struct {
	qs         string // original query string
	start, end time.Time
	step       time.Duration
}

func newPromInstantQueryEngine(q *promAPIQuery, h *Handler, ai accessInfo) (promql.Query, func(), error) {
	opt := promql.EngineOpts{
		Timeout:              querySelectTimeout,
		EnableAtModifier:     true,
		EnableNegativeOffset: true,
	}
	ev := promQueryable{
		ng:       promql.NewEngine(opt),
		h:        h,
		ai:       ai,
		now:      time.Now().Unix(),
		topNodes: map[parser.Node]*promQuery{},
	}
	ret, err := ev.ng.NewInstantQuery(&ev, q.qs, q.start)
	if err != nil {
		return nil, nil, err
	}
	ev.stmt = ret.Statement().(*parser.EvalStmt)
	return ret, ret.Close, nil
}

func newPromRangeQueryEngine(q *promAPIQuery, h *Handler, ai accessInfo) (promql.Query, func(), error) {
	opt := promql.EngineOpts{
		Timeout:              querySelectTimeout,
		EnableAtModifier:     true,
		EnableNegativeOffset: true,
	}
	ev := promQueryable{
		ng:       promql.NewEngine(opt),
		h:        h,
		ai:       ai,
		now:      time.Now().Unix(),
		topNodes: map[parser.Node]*promQuery{},
	}
	ret, err := ev.ng.NewRangeQuery(&ev, q.qs, q.start, q.end, q.step)
	if err != nil {
		return nil, nil, err
	}
	ev.stmt = ret.Statement().(*parser.EvalStmt)
	return ret, ret.Close, nil
}
