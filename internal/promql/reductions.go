// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package promql

import (
	"github.com/vkcom/statshouse/internal/promql/model"
	"github.com/vkcom/statshouse/internal/promql/parser"
)

type reduction struct {
	rule         int
	expr         parser.Expr
	what         string
	step         int64 // matrix selector range
	grouped      bool
	groupBy      []string
	groupWithout bool
}

type reductionRuleFunc func(*reduction, parser.Expr, int64) bool

var reductionRules = [][]reductionRuleFunc{
	// #0 sum(demo_memory_usage_bytes)
	{reduceAggregateExpr},
	// #1 sum_over_time(demo_memory_usage_bytes[5s])
	{reduceMatrixSelector, reduceOverTimeCall},
	// #2 sum(sum_over_time(demo_memory_usage_bytes[5s]))
	{reduceMatrixSelector, reduceOverTimeCall, reduceAggregateExpr},
	// #3 sum_over_time(sum(demo_memory_usage_bytes)[5s:])
	{reduceAggregateExpr, reduceSubQueryExpr, reduceOverTimeCall},
}

func evalReductionRules(sel *parser.VectorSelector, nodes []parser.Node, step int64) (res reduction, ok bool) {
	var (
		depth int
		curr  = make([]reduction, len(reductionRules))
		next  = make([]reduction, len(reductionRules))
	)
	for i := range curr {
		curr[i].rule = i
		if len(sel.What) != 0 {
			curr[i].what = sel.What
		}
	}
	for i := len(nodes); i != 0 && len(curr) != 0; i-- {
		// skip parentheses
		var e parser.Expr
		for ; i != 0; i-- {
			if _, p := nodes[i-1].(*parser.ParenExpr); !p {
				e = nodes[i-1].(parser.Expr)
				break
			}
		}
		if e == nil {
			break
		}
		// eval reduction rules
		next = next[:0]
		for _, r := range curr {
			s := reductionRules[r.rule]
			if !s[depth](&r, e, step) {
				continue
			}
			r.expr = e
			if len(s) == depth+1 {
				// reduction found, continue search (maybe there is a longer one)
				res = r
				ok = true
			} else {
				next = append(next, r)
			}
		}
		depth++
		curr, next = next, curr
	}
	return res, ok
}

func reduceMatrixSelector(r *reduction, e parser.Expr, step int64) bool {
	sel, ok := e.(*parser.MatrixSelector)
	if !ok || sel.Range > step {
		return false
	}
	r.step = sel.Range
	return true
}

func reduceSubQueryExpr(r *reduction, e parser.Expr, step int64) bool {
	sel, ok := e.(*parser.SubqueryExpr)
	if !ok || sel.Range > step {
		return false
	}
	r.step = sel.Range
	return true
}

func reduceOverTimeCall(r *reduction, e parser.Expr, step int64) bool {
	call, ok := e.(*parser.Call)
	if !ok {
		return false
	}
	var what string
	switch call.Func.Name {
	case "avg_over_time":
		what = model.Avg
	case "min_over_time":
		what = model.Min
	case "max_over_time":
		what = model.Max
	case "sum_over_time":
		what = model.Sum
	case "count_over_time":
		what = model.Count
	case "stddev_over_time":
		if r.step != step {
			return false
		}
		what = model.StdDev
	case "stdvar_over_time":
		if r.step != step {
			return false
		}
		what = model.StdVar
	default:
		return false
	}
	r.what, ok = reduceWhat(r.what, what)
	return ok
}

func reduceAggregateExpr(r *reduction, e parser.Expr, _ int64) bool {
	agg, ok := e.(*parser.AggregateExpr)
	if !ok {
		return false
	}
	var what string
	switch agg.Op {
	case parser.AVG:
		what = model.Avg
	case parser.MIN:
		what = model.Min
	case parser.MAX:
		what = model.Max
	case parser.SUM:
		what = model.SumSec
	case parser.COUNT:
		what = model.CountSec
	default:
		return false
	}
	if r.what, ok = reduceWhat(r.what, what); !ok {
		return false
	}
	r.grouped = true
	r.groupBy = agg.Grouping
	r.groupWithout = agg.Without
	return true
}

func reduceWhat(a, b string) (string, bool) {
	if a == "" || a == model.SumSec && b == model.Sum || a == model.CountSec && b == model.Count {
		return b, true
	}
	if a == b || a == model.Sum && b == model.SumSec || a == model.Count && b == model.CountSec {
		return a, true
	}
	return a, false
}
