package promql

import "github.com/vkcom/statshouse/internal/promql/parser"

type reduction struct {
	rule         int
	expr         parser.Expr
	what         What
	selRange     int64 // matrix selector range
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
	{reduceAggregateExpr, reduceSubqueryExpr, reduceOverTimeCall},
}

func evalReductionRules(nodes []parser.Node, step int64) (res reduction, ok bool) {
	var (
		depth int
		curr  = make([]reduction, len(reductionRules))
		next  = make([]reduction, len(reductionRules))
	)
	for i := range curr {
		curr[i].rule = i
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
	r.selRange = sel.Range
	return true
}

func reduceSubqueryExpr(r *reduction, e parser.Expr, step int64) bool {
	sel, ok := e.(*parser.SubqueryExpr)
	if !ok || sel.Range > step {
		return false
	}
	r.selRange = sel.Range
	return true
}

func reduceOverTimeCall(r *reduction, e parser.Expr, _ int64) bool {
	call, ok := e.(*parser.Call)
	if !ok {
		return false
	}
	switch call.Func.Name {
	case "avg_over_time":
		r.what = DigestAvg
	case "min_over_time":
		r.what = DigestMin
	case "max_over_time":
		r.what = DigestMax
	case "sum_over_time":
		r.what = DigestSum
	case "count_over_time":
		r.what = DigestCnt
	case "stddev_over_time":
		r.what = DigestStdDev
	default:
		return false
	}
	return true
}

func reduceAggregateExpr(r *reduction, e parser.Expr, _ int64) bool {
	agg, ok := e.(*parser.AggregateExpr)
	if !ok {
		return false
	}
	var what What
	switch agg.Op {
	case parser.AVG:
		what = DigestAvg
	case parser.MIN:
		what = DigestMin
	case parser.MAX:
		what = DigestMax
	case parser.SUM:
		what = SumPerSecond
	case parser.COUNT:
		what = CntPerSecond
	default:
		return false
	}
	switch {
	case r.what == Unspecified:
		r.what = what
	case r.what == what:
	case r.what == DigestSum && what == SumPerSecond:
	case r.what == DigestCnt && what == CntPerSecond:
		// do nothing
	default:
		return false
	}
	r.grouped = true
	r.groupBy = agg.Grouping
	r.groupWithout = agg.Without
	return true
}
