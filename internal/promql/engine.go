// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package promql

import (
	"context"
	"errors"
	"fmt"
	"hash"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gogo/protobuf/sortkeys"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/promql/parser"
	"github.com/vkcom/statshouse/internal/receiver/prometheus"

	"pgregory.net/rand"
)

const (
	labelWhat  = "__what__"
	labelBy    = "__by__"
	labelBind  = "__bind__"
	LabelShard = "__shard__"
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
	Version             string
	AvoidCache          bool
	TimeNow             int64
	StepAuto            bool
	ExpandToLODBoundary bool
	TagOffset           bool
	TagTotal            bool
	ExplicitGrouping    bool
	MaxHost             bool
	Offsets             []int64
	Rand                *rand.Rand
	Vars                map[string]Variable

	ExprQueriesSingleMetricCallback MetricMetaValueCallback
	SeriesQueryCallback             SeriesQueryCallback
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

type Engine struct {
	h   Handler
	loc *time.Location
}

type evaluator struct {
	Engine

	ctx context.Context
	opt Options
	ast parser.Expr
	ars map[parser.Expr]parser.Expr // ast reductions
	t   Timescale
	r   int64 // matrix selector range
	hh  hash.Hash64

	// metric -> tag index -> offset -> tag value id -> tag value
	tags map[*format.MetricMetaValue][]map[int64]map[int32]string
	// metric -> offset -> String TOP
	stop map[*format.MetricMetaValue]map[int64][]string

	// memory management
	allocMap         map[*[]float64]bool
	freeList         []*[]float64
	reuseList        []*[]float64
	cancellationList []func()

	// diagnostics
	trace *[]string
	debug bool
}

type seriesQueryX struct { // SeriesQuery extended
	SeriesQuery
	prefixSum bool
	histogram histogramQuery
}

type histogramQuery struct {
	restore bool
	filter  bool
	compare bool // "==" if true, "!=" otherwise
	le      float32
}

type contextKey int

type traceContext struct {
	s *[]string // sink
	v bool      // verbose
}

const (
	offsetContextKey contextKey = iota
	traceContextKey
)

func NewEngine(h Handler, loc *time.Location) Engine {
	return Engine{h, loc}
}

func (ng Engine) Exec(ctx context.Context, qry Query) (res parser.Value, cancel func(), err error) {
	if qry.Options.TimeNow == 0 {
		// fix the time "now"
		qry.Options.TimeNow = time.Now().Unix()
	}
	if qry.Options.TimeNow < qry.Start {
		// the future is unknown
		return &TimeSeries{Time: []int64{}}, func() {}, nil
	}
	// register panic handler
	var ev evaluator
	defer func() {
		if r := recover(); r != nil {
			err = Error{what: r, panic: true}
			ev.cancel()
		}
	}()
	// parse query
	ev, err = ng.newEvaluator(ctx, qry)
	if err != nil {
		return nil, nil, Error{what: err}
	}
	// evaluate query
	if t, ok := ctx.Value(traceContextKey).(*traceContext); ok {
		*t.s = append(*t.s, ev.ast.String())
	}
	if ev.trace != nil && ev.debug {
		ev.tracef("requested from %d to %d, timescale from %d to %d", qry.Start, qry.End, ev.t.Start, ev.t.End)
	}
	switch e := ev.ast.(type) {
	case *parser.StringLiteral:
		return String{T: qry.Start, V: e.Val}, func() {}, nil
	default:
		var res TimeSeries
		res, err = ev.exec()
		if err != nil {
			ev.cancel()
			return nil, nil, Error{what: err}
		}
		// resolve int32 tag values into strings
		for _, s := range res.Series.Data {
			for _, v := range s.Tags.ID2Tag {
				v.stringify(&ev)
			}
		}
		if ev.trace != nil {
			ev.tracef(ev.ast.String())
			ev.tracef("buffers alloc #%d, reuse #%d, %s", len(ev.allocMap)+len(ev.freeList), len(ev.reuseList), res.String())
		}
		return &res, ev.cancel, nil
	}
}

func (ng Engine) newEvaluator(ctx context.Context, qry Query) (evaluator, error) {
	ev := evaluator{
		Engine: ng,
		ctx:    ctx,
		opt:    qry.Options,
	}
	// init diagnostics
	if v, ok := ctx.Value(traceContextKey).(*traceContext); ok {
		ev.trace = v.s
		ev.debug = v.v
	}
	// parse PromQL expression
	var err error
	ev.ast, err = parser.ParseExpr(qry.Expr)
	if err != nil {
		return evaluator{}, err
	}
	if v, ok := evalLiteral(ev.ast); ok {
		ev.ast = v
	}
	// ensure zero offset present, sort descending, remove duplicates
	s := make([]int64, 0, 1+len(ev.opt.Offsets))
	s = append(s, 0)
	s = append(s, ev.opt.Offsets...)
	sort.Sort(sort.Reverse(sortkeys.Int64Slice(s)))
	var i int
	for j := 1; j < len(s); j++ {
		if s[i] != s[j] {
			i++
			s[i] = s[j]
		}
	}
	ev.opt.Offsets = s[:i+1]
	// match metrics
	var (
		maxRange     int64
		metricOffset = make(map[*format.MetricMetaValue]int64)
	)
	parser.Inspect(ev.ast, func(node parser.Node, path []parser.Node) error {
		switch e := node.(type) {
		case *parser.VectorSelector:
			if err = ev.bindVariables(e); err == nil {
				err = ev.matchMetrics(e, path, metricOffset, s[0])
			}
		case *parser.MatrixSelector:
			if maxRange < e.Range {
				maxRange = e.Range
			}
		case *parser.SubqueryExpr:
			if maxRange < e.Range {
				maxRange = e.Range
			}
		}
		return err
	})
	if err != nil {
		return evaluator{}, err
	}
	// init timescale
	qry.Start -= maxRange // widen time range to accommodate range selectors
	if qry.Step <= 0 {    // instant query case
		qry.Step = 1
	}
	ev.t, err = ng.h.GetTimescale(qry, metricOffset)
	if err != nil {
		return evaluator{}, err
	}
	// evaluate reduction rules
	ev.ars = make(map[parser.Expr]parser.Expr)
	stepMin := ev.t.LODs[len(ev.t.LODs)-1].Step
	parser.Inspect(ev.ast, func(node parser.Node, nodes []parser.Node) error {
		switch s := node.(type) {
		case *parser.VectorSelector:
			var grouped bool
			if s.GroupBy != nil {
				grouped = true
			}
			if !grouped && len(s.What) <= 1 {
				if ar, ok := evalReductionRules(s, nodes, stepMin); ok {
					s.What = ar.what
					s.GroupBy = ar.groupBy
					s.GroupWithout = ar.groupWithout
					s.Range = ar.step
					s.OmitNameTag = true
					ev.ars[ar.expr] = s
					grouped = ar.grouped
				}
			}
			if !grouped && !ev.opt.ExplicitGrouping {
				s.GroupByAll = true
			}
		}
		return nil
	})
	// callback
	if ev.opt.ExprQueriesSingleMetricCallback != nil && len(metricOffset) == 1 {
		for metric := range metricOffset {
			ev.opt.ExprQueriesSingleMetricCallback(metric)
			break
		}
	}
	// final touch
	ev.tags = make(map[*format.MetricMetaValue][]map[int64]map[int32]string)
	ev.stop = make(map[*format.MetricMetaValue]map[int64][]string)
	return ev, nil
}

func (ev *evaluator) bindVariables(sel *parser.VectorSelector) error {
	var s []*labels.Matcher
	for _, matcher := range sel.LabelMatchers {
		if matcher.Name == labelBind {
			if matcher.Type != labels.MatchEqual {
				return fmt.Errorf("%s supports only strict equality", matcher.Name)
			}
			s = append(s, matcher)
		}
	}
	for _, matcher := range s {
		for _, bind := range strings.Split(matcher.Value, ",") {
			s := strings.Split(bind, ":")
			if len(s) != 2 || len(s[0]) == 0 || len(s[1]) == 0 {
				return fmt.Errorf("%s invalid value format: expected \"tag:var\", got %q", matcher.Name, bind)
			}
			var (
				vn = s[1]   // variable name
				vv Variable // variable value
				ok bool
			)
			if vv, ok = ev.opt.Vars[vn]; !ok {
				if ev.trace != nil && ev.debug {
					ev.tracef("variable %q not specified", vn)
				}
				continue
			}
			var mt labels.MatchType
			if vv.Negate {
				mt = labels.MatchNotEqual
			} else {
				mt = labels.MatchEqual
			}
			var (
				tn  = s[0] // tag name
				m   *labels.Matcher
				err error
			)
			for _, v := range vv.Value {
				if m, err = labels.NewMatcher(mt, tn, v); err != nil {
					return err
				}
				sel.LabelMatchers = append(sel.LabelMatchers, m)
			}
			if vv.Group {
				sel.GroupBy = append(sel.GroupBy, tn)
			}
		}
	}
	return nil
}

func (ev *evaluator) matchMetrics(sel *parser.VectorSelector, path []parser.Node, metricOffset map[*format.MetricMetaValue]int64, offset int64) error {
	for _, matcher := range sel.LabelMatchers {
		if len(sel.MatchingMetrics) != 0 && len(sel.What) != 0 {
			break
		}
		switch matcher.Name {
		case labels.MetricName:
			metrics, names, err := ev.h.MatchMetrics(ev.ctx, matcher)
			if err != nil {
				return err
			}
			if len(metrics) == 0 {
				if ev.trace != nil && ev.debug {
					ev.tracef("no metric matches %v", matcher)
				}
				return nil // metric does not exist, not an error
			}
			if ev.trace != nil && ev.debug {
				ev.tracef("found %d metrics for %v", len(metrics), matcher)
			}
			for i, m := range metrics {
				var (
					curOffset, ok = metricOffset[m]
					newOffset     = sel.OriginalOffset + offset
				)
				if !ok || curOffset < newOffset {
					metricOffset[m] = newOffset
				}
				sel.MatchingMetrics = append(sel.MatchingMetrics, m)
				sel.MatchingNames = append(sel.MatchingNames, names[i])
			}
		case labelWhat:
			if matcher.Type != labels.MatchEqual {
				return fmt.Errorf("%s supports only strict equality", labelWhat)
			}
			for _, what := range strings.Split(matcher.Value, ",") {
				switch what {
				case "":
					// ignore empty "what" value
				case MaxHost:
					sel.MaxHost = true
				default:
					sel.What = what
				}
			}
		case labelBy:
			if matcher.Type != labels.MatchEqual {
				return fmt.Errorf("%s supports only strict equality", labelBy)
			}
			if len(matcher.Value) != 0 {
				sel.GroupBy = strings.Split(matcher.Value, ",")
			} else {
				sel.GroupBy = make([]string, 0)
			}
		}
	}
	for i := len(path); len(sel.MetricKindHint) == 0 && i != 0; i-- {
		switch e := path[i-1].(type) {
		case *parser.Call:
			switch e.Func.Name {
			case "delta", "deriv", "holt_winters", "idelta", "predict_linear":
				sel.MetricKindHint = format.MetricKindValue
			case "increase", "irate", "rate", "resets":
				sel.MetricKindHint = format.MetricKindCounter
			}
		}
	}
	return nil
}

func (ev *evaluator) time() []int64 {
	return ev.t.Time
}

func (ev *evaluator) exec() (TimeSeries, error) {
	ss, err := ev.eval(ev.ast)
	if err != nil {
		return TimeSeries{}, err
	}
	var (
		lo int
		hi = len(ev.t.Time)
	)
	for lo < hi && ev.t.Time[lo] < ev.t.Start {
		lo++
	}
	for lo < hi-1 && ev.t.End <= ev.t.Time[hi-1] {
		hi--
	}
	res := TimeSeries{Time: ev.t.Time[lo:hi]}
	for _, s := range ss {
		// remove series with no data within [start, end), update total
		if s.Meta.Total == 0 {
			s.Meta.Total = len(s.Data)
		}
		ss := ev.newSeries(len(s.Data))
		for i := range s.Data {
			var keep bool
			for j := lo; j < hi; j++ {
				if !math.IsNaN((*s.Data[i].Values)[j]) {
					keep = true
					break
				}
			}
			if keep {
				ss.appendX(s, i)
			} else {
				s.Meta.Total--
			}
		}
		// trim time outside [start, end)
		for i := range ss.Data {
			s := (*ss.Data[i].Values)[lo:hi]
			ss.Data[i].Values = &s
			if len(ss.Data[i].MaxHost) != 0 {
				ss.Data[i].MaxHost = ss.Data[i].MaxHost[lo:hi]
			}
		}
		res.Series.append(ss)
	}
	return res, nil
}

func (ev *evaluator) eval(expr parser.Expr) (res []Series, err error) {
	if ev.ctx.Err() != nil {
		return nil, ev.ctx.Err()
	}
	if e, ok := ev.ars[expr]; ok {
		if ev.trace != nil && ev.debug {
			ev.tracef("replace %s with %s", string(expr.Type()), string(e.Type()))
		}
		return ev.eval(e)
	}
	switch e := expr.(type) {
	case *parser.AggregateExpr:
		fn := aggregates[e.Op]
		if fn == nil {
			return nil, fmt.Errorf("not implemented aggregate %q", e.Op)
		}
		if res, err = fn(ev, e); err != nil {
			return nil, err
		}
		if e.Op != parser.TOPK && e.Op != parser.BOTTOMK {
			removeMetricName(res)
		}
	case *parser.BinaryExpr:
		switch l := e.LHS.(type) {
		case *parser.NumberLiteral:
			res, err = ev.eval(e.RHS)
			if err != nil {
				return nil, err
			}
			fn := getBinaryFunc(scalarSliceFuncM, e.Op, e.ReturnBool)
			if fn == nil {
				return nil, fmt.Errorf("binary operator %q is not defined on (%q, %q) pair", e.Op, e.LHS.Type(), e.RHS.Type())
			}
			for i := range res {
				for j := range res[i].Data {
					fn(l.Val, *res[i].Data[j].Values)
				}
			}
		default:
			switch r := e.RHS.(type) {
			case *parser.NumberLiteral:
				if res, err = ev.eval(e.LHS); err != nil {
					return nil, err
				}
				fn := getBinaryFunc(sliceScalarFuncM, e.Op, e.ReturnBool)
				if fn == nil {
					return nil, fmt.Errorf("binary operator %q is not defined on (%q, %q) pair", e.Op, e.LHS.Type(), e.RHS.Type())
				}
				for i := range res {
					for j := range res[i].Data {
						fn(*res[i].Data[j].Values, r.Val)
					}
				}
			default:
				if res, err = ev.evalBinary(e); err != nil {
					return nil, err
				}
				for i := range res {
					res[i].Meta.Total = len(res[i].Data)
				}
			}
		}
		if e.ReturnBool || len(e.VectorMatching.MatchingLabels) != 0 || shouldDropMetricName(e.Op) {
			removeMetricName(res)
		}
	case *parser.Call:
		fn, ok := calls[e.Func.Name]
		if !ok {
			return nil, fmt.Errorf("not implemented function %q", e.Func.Name)
		}
		if res, err = fn(ev, e.Args); err != nil {
			return nil, err
		}
		ev.r = 0
		switch e.Func.Name {
		case "label_join", "label_replace", "last_over_time":
			break // keep metric name
		default:
			removeMetricName(res)
		}
	case *parser.MatrixSelector:
		if res, err = ev.eval(e.VectorSelector); err != nil {
			return nil, err
		}
		ev.r = e.Range
	case *parser.NumberLiteral:
		res = make([]Series, len(ev.opt.Offsets))
		for i := range res {
			res[i].Data = []SeriesData{ev.alloc()}
			s := *res[i].Data[0].Values
			for j := range s {
				s[j] = e.Val
			}
		}
	case *parser.ParenExpr:
		res, err = ev.eval(e.Expr)
	case *parser.SubqueryExpr:
		if res, err = ev.eval(e.Expr); err != nil {
			return nil, err
		}
		ev.r = e.Range
	case *parser.UnaryExpr:
		if res, err = ev.eval(e.Expr); err != nil {
			return nil, err
		}
		if e.Op == parser.SUB {
			for i := range res {
				for j := range res[i].Data {
					s := *res[i].Data[j].Values
					for k := range s {
						s[k] = -s[k]
					}
				}
				res[i].removeMetricName()
			}
		}
	case *parser.VectorSelector:
		res, err = ev.querySeries(e)
	default:
		return nil, fmt.Errorf("not implemented %T", expr)
	}
	return res, err
}

func (ev *evaluator) evalBinary(expr *parser.BinaryExpr) ([]Series, error) {
	if expr.Op.IsSetOperator() {
		expr.VectorMatching.Card = parser.CardManyToMany
	}
	lhs, err := ev.eval(expr.LHS)
	if err != nil {
		return nil, err
	}
	rhs, err := ev.eval(expr.RHS)
	if err != nil {
		return nil, err
	}
	if len(lhs) != len(rhs) {
		panic("LHS length isn't equal to RHS length")
	}
	fn := getBinaryFunc(sliceBinaryFuncM, expr.Op, expr.ReturnBool)
	res := make([]Series, len(lhs))
	for x, lhs := range lhs {
		rhs := rhs[x]
		switch expr.VectorMatching.Card {
		case parser.CardOneToOne:
			if rhs.scalar() {
				for i := range lhs.Data {
					fn(*lhs.Data[i].Values, *lhs.Data[i].Values, *rhs.Data[0].Values)
				}
				res[x].append(lhs)
				rhs.free(ev)
			} else if lhs.scalar() {
				op := expr.Op
				var swapArgs bool
				switch expr.Op {
				case parser.EQLC:
					swapArgs = true
				case parser.GTE:
					swapArgs = true
					op = parser.LSS
				case parser.GTR:
					swapArgs = true
					op = parser.LTE
				case parser.LSS:
					swapArgs = true
					op = parser.GTE
				case parser.LTE:
					swapArgs = true
					op = parser.GTR
				case parser.NEQ:
					swapArgs = true
				}
				fn = getBinaryFunc(sliceBinaryFuncM, op, expr.ReturnBool)
				if swapArgs {
					for i := range rhs.Data {
						fn(*rhs.Data[i].Values, *rhs.Data[i].Values, *lhs.Data[0].Values)
					}
				} else {
					for i := range rhs.Data {
						fn(*rhs.Data[i].Values, *lhs.Data[0].Values, *rhs.Data[i].Values)
					}
				}
				res[x].append(rhs)
				lhs.Data[0].free(ev)
			} else {
				var lhsM map[uint64]int
				lhsM, err = lhs.hash(ev, hashOptions{
					on:    expr.VectorMatching.On,
					tags:  expr.VectorMatching.MatchingLabels,
					stags: rhs.Meta.STags,
				})
				if err != nil {
					return nil, err
				}
				var rhsM map[uint64]int
				rhsM, err = rhs.hash(ev, hashOptions{
					on:    expr.VectorMatching.On,
					tags:  expr.VectorMatching.MatchingLabels,
					stags: lhs.Meta.STags,
				})
				if err != nil {
					return nil, err
				}
				res[x] = ev.newSeries(len(lhsM))
				for lhsH, lhsX := range lhsM {
					if rhsX, ok := rhsM[lhsH]; ok {
						fn(*lhs.Data[lhsX].Values, *lhs.Data[lhsX].Values, *rhs.Data[rhsX].Values)
						res[x].appendX(lhs, lhsX)
					} else {
						lhs.Data[lhsX].free(ev)
					}
				}
				rhs.free(ev)
			}
		case parser.CardManyToOne:
			var lhsG []seriesGroup
			lhsG, err = lhs.group(ev, hashOptions{
				on:    expr.VectorMatching.On,
				tags:  expr.VectorMatching.MatchingLabels,
				stags: rhs.Meta.STags,
			})
			if err != nil {
				return nil, err
			}
			var rhsM map[uint64]int
			rhsM, err = rhs.hash(ev, hashOptions{
				stags: lhs.Meta.STags,
			})
			if err != nil {
				return nil, err
			}
			res[x] = ev.newSeries(len(lhsG))
			for _, lhsG := range lhsG {
				if rhsX, ok := rhsM[lhsG.hash]; ok {
					for lhsX, lhsS := range lhsG.Data {
						fn(*lhsS.Values, *lhsS.Values, *rhs.Data[rhsX].Values)
						for _, v := range expr.VectorMatching.Include {
							if t, ok := rhs.Data[rhsX].Tags.get(v); ok {
								lhsG.AddTagAt(lhsX, t)
							}
						}
						res[x].appendX(lhsG.Series, lhsX)
					}
				} else {
					lhsG.free(ev)
				}
			}
			rhs.free(ev)
		case parser.CardOneToMany:
			var lhsM map[uint64]int
			lhsM, err = lhs.hash(ev, hashOptions{
				stags: rhs.Meta.STags,
			})
			if err != nil {
				return nil, err
			}
			var rhsG []seriesGroup
			rhsG, err = rhs.group(ev, hashOptions{
				tags:  expr.VectorMatching.MatchingLabels,
				on:    expr.VectorMatching.On,
				stags: lhs.Meta.STags,
			})
			if err != nil {
				return nil, err
			}
			res[x] = ev.newSeries(len(rhsG))
			for _, rhsG := range rhsG {
				if lhsX, ok := lhsM[rhsG.hash]; ok {
					for rhsX, rhsS := range rhsG.Data {
						fn(*rhsS.Values, *rhsS.Values, *lhs.Data[lhsX].Values)
						for _, v := range expr.VectorMatching.Include {
							if tag, ok := lhs.Data[lhsX].Tags.get(v); ok {
								rhsG.AddTagAt(rhsX, tag)
							}
						}
						res[x].appendX(rhsG.Series, rhsX)
					}
				} else {
					rhsG.free(ev)
				}
			}
			lhs.free(ev)
		case parser.CardManyToMany:
			var lhsM map[uint64]int
			lhsM, err = lhs.hash(ev, hashOptions{
				on:    expr.VectorMatching.On,
				tags:  expr.VectorMatching.MatchingLabels,
				stags: rhs.Meta.STags,
			})
			if err != nil {
				return nil, err
			}
			var rhsM map[uint64]int
			rhsM, err = rhs.hash(ev, hashOptions{
				on:    expr.VectorMatching.On,
				tags:  expr.VectorMatching.MatchingLabels,
				stags: lhs.Meta.STags,
			})
			if err != nil {
				return nil, err
			}
			switch expr.Op {
			case parser.LAND:
				res[x] = ev.newSeries(len(lhsM))
				for lhsH, lhsX := range lhsM {
					if _, ok := rhsM[lhsH]; ok {
						res[x].appendX(lhs, lhsX)
					} else {
						ev.freeAt(lhs.Data, lhsX)
					}
				}
				rhs.free(ev)
			case parser.LDEFAULT:
				res[x] = lhs
				for lhsH, lhsX := range lhsM {
					if rhsX, ok := rhsM[lhsH]; ok {
						sliceDefault(*res[x].Data[lhsX].Values, *lhs.Data[lhsX].Values, *rhs.Data[rhsX].Values)
					}
				}
				rhs.free(ev)
			case parser.LOR:
				res[x] = lhs
				for rhsH, rhsX := range rhsM {
					if _, ok := lhsM[rhsH]; !ok {
						res[x].appendX(rhs, rhsX)
					} else {
						ev.freeAt(rhs.Data, rhsX)
					}
				}
			case parser.LUNLESS:
				res[x] = ev.newSeries(len(lhsM))
				for lhsH, lhsX := range lhsM {
					if _, ok := rhsM[lhsH]; !ok {
						res[x].appendX(lhs, lhsX)
					} else {
						ev.freeAt(lhs.Data, lhsX)
					}
				}
				rhs.free(ev)
			default:
				err = fmt.Errorf("not implemented binary operator %q", expr.Op)
			}
		}
		if err != nil {
			return nil, err
		}
	}
	return res, nil
}

func (ev *evaluator) querySeries(sel *parser.VectorSelector) ([]Series, error) {
	res := make([]Series, len(ev.opt.Offsets))
	for i, metric := range sel.MatchingMetrics {
		if ev.trace != nil && ev.debug {
			ev.tracef("#%d request %s: %s", i, metric.Name, sel.What)
		}
		for x, offset := range ev.opt.Offsets {
			qry, err := ev.buildSeriesQuery(ev.ctx, sel, metric, sel.What, sel.OriginalOffset+offset)
			if err != nil {
				return nil, err
			}
			if qry.empty() {
				if ev.trace != nil && ev.debug {
					ev.tracef("#%d query is empty", i)
				}
				continue
			}
			series, cancel, err := ev.h.QuerySeries(ev.ctx, &qry.SeriesQuery)
			if err != nil {
				return nil, err
			}
			ev.cancellationList = append(ev.cancellationList, cancel)
			if ev.trace != nil && ev.debug {
				ev.tracef("#%d series count %d", i, len(series.Data))
			}
			if qry.prefixSum {
				series = ev.funcPrefixSum(series)
			}
			for k := range series.Data {
				if !sel.OmitNameTag {
					series.AddTagAt(k, &SeriesTag{
						ID:     labels.MetricName,
						SValue: sel.MatchingNames[i]})
				}
				series.Data[k].Offset = offset
				series.Data[k].What = series.Meta.What
			}
			if qry.histogram.restore {
				series, err = ev.restoreHistogram(series, qry)
				if err != nil {
					return nil, err
				}
			}
			res[x].append(series)
		}
	}
	return res, nil
}

func (ev *evaluator) restoreHistogram(bag Series, qry seriesQueryX) (Series, error) {
	s, err := bag.histograms(ev)
	if err != nil {
		return Series{}, err
	}
	for _, h := range s {
		for i := 1; i < len(h.buckets); i++ {
			for j := 0; j < len(ev.time()); j++ {
				(*h.group.Data[h.buckets[i].x].Values)[j] += (*h.group.Data[h.buckets[i-1].x].Values)[j]
			}
		}
	}
	if !qry.histogram.filter {
		return bag, nil
	}
	res := ev.newSeries(len(bag.Data))
	for _, h := range s {
		for _, b := range h.buckets {
			if qry.histogram.le == b.le == qry.histogram.compare {
				res.append(h.group.at(b.x))
				break
			}
		}
	}
	return res, nil
}

func (ev *evaluator) buildSeriesQuery(ctx context.Context, sel *parser.VectorSelector, metric *format.MetricMetaValue, selWhat string, offset int64) (seriesQueryX, error) {
	// what
	var (
		what      DigestWhat
		prefixSum bool
	)
	switch selWhat {
	case Count:
		what = DigestCount
	case CountSec:
		what = DigestCountSec
	case CountRaw:
		what = DigestCountRaw
	case Min:
		what = DigestMin
	case Max:
		what = DigestMax
	case Sum:
		what = DigestSum
	case SumSec:
		what = DigestSumSec
	case SumRaw:
		what = DigestSumRaw
	case Avg:
		what = DigestAvg
	case StdDev:
		what = DigestStdDev
	case StdVar:
		what = DigestStdVar
	case P0_1:
		what = DigestP0_1
	case P1:
		what = DigestP1
	case P5:
		what = DigestP5
	case P10:
		what = DigestP10
	case P25:
		what = DigestP25
	case P50:
		what = DigestP50
	case P75:
		what = DigestP75
	case P90:
		what = DigestP90
	case P95:
		what = DigestP95
	case P99:
		what = DigestP99
	case P999:
		what = DigestP999
	case Cardinality:
		what = DigestCardinality
	case CardinalitySec:
		what = DigestCardinalitySec
	case CardinalityRaw:
		what = DigestCardinalityRaw
	case Unique:
		what = DigestUnique
	case UniqueSec:
		what = DigestUniqueSec
	case "":
		if metric.Kind == format.MetricKindCounter || sel.MetricKindHint == format.MetricKindCounter {
			what = DigestCountRaw
			prefixSum = true
		} else {
			what = DigestAvg
		}
	default:
		return seriesQueryX{}, fmt.Errorf("unrecognized %s value %q", labelWhat, sel.What)
	}
	// grouping
	var (
		groupBy    []string
		metricH    = what == DigestCount && metric.Name2Tag[format.LETagName].Raw
		histogramQ histogramQuery
		addGroupBy = func(t format.MetricMetaTag) {
			groupBy = append(groupBy, format.TagID(t.Index))
			if t.Name == format.LETagName {
				histogramQ.restore = true
			}
		}
	)
	if sel.GroupByAll {
		if !sel.GroupWithout {
			for _, t := range metric.Tags {
				if len(t.Name) != 0 {
					addGroupBy(t)
				}
			}
		}
	} else if sel.GroupWithout {
		skip := make(map[int]bool)
		for _, name := range sel.GroupBy {
			t, ok, _ := metric.APICompatGetTag(name)
			if ok {
				skip[t.Index] = true
			}
		}
		for _, t := range metric.Tags {
			if !skip[t.Index] {
				addGroupBy(t)
			}
		}
	} else if len(sel.GroupBy) != 0 {
		groupBy = make([]string, 0, len(sel.GroupBy))
		for _, k := range sel.GroupBy {
			if k == LabelShard {
				groupBy = append(groupBy, format.ShardTagID)
			} else if t, ok, _ := metric.APICompatGetTag(k); ok {
				if t.Index == format.StringTopTagIndex {
					groupBy = append(groupBy, format.StringTopTagID)
				} else {
					addGroupBy(t)
				}
			}
		}
	}
	// filtering
	var (
		filterIn   [format.MaxTags]map[int32]string // tag index -> tag value ID -> tag value
		filterOut  [format.MaxTags]map[int32]string // as above
		sFilterIn  []string
		sFilterOut []string
		emptyCount [format.MaxTags + 1]int // number of "MatchEqual" or "MatchRegexp" filters which are guaranteed to yield empty response
	)
	for _, matcher := range sel.LabelMatchers {
		if strings.HasPrefix(matcher.Name, "__") {
			continue
		}
		tag, ok, _ := metric.APICompatGetTag(matcher.Name)
		if !ok {
			return seriesQueryX{}, fmt.Errorf("not found tag %q", matcher.Name)
		}
		if tag.Index == format.StringTopTagIndex {
			switch matcher.Type {
			case labels.MatchEqual:
				sFilterIn = append(sFilterIn, matcher.Value)
			case labels.MatchNotEqual:
				sFilterOut = append(sFilterOut, matcher.Value)
			case labels.MatchRegexp:
				stop, err := ev.getStringTop(ctx, metric, offset)
				if err != nil {
					return seriesQueryX{}, err
				}
				var n int
				for _, v := range stop {
					if matcher.Matches(v) {
						sFilterIn = append(sFilterIn, v)
						n++
					}
				}
				if n == 0 {
					// there no data satisfying the filter
					emptyCount[format.MaxTags]++
					continue
				}
			case labels.MatchNotRegexp:
				stop, err := ev.getStringTop(ctx, metric, offset)
				if err != nil {
					return seriesQueryX{}, err
				}
				for _, v := range stop {
					if !matcher.Matches(v) {
						sFilterOut = append(sFilterOut, v)
					}
				}
			}
		} else {
			i := tag.Index
			switch matcher.Type {
			case labels.MatchEqual:
				id, err := ev.getTagValueID(metric, i, matcher.Value)
				if err != nil {
					if errors.Is(err, ErrNotFound) {
						// string is not mapped, result is guaranteed to be empty
						emptyCount[i]++
						continue
					} else {
						return seriesQueryX{}, fmt.Errorf("failed to map string %q: %v", matcher.Value, err)
					}
				}
				if metricH && !histogramQ.restore && matcher.Name == format.LETagName {
					histogramQ.filter = true
					histogramQ.compare = true
					histogramQ.le = prometheus.LexDecode(id)
				} else if filterIn[i] != nil {
					filterIn[i][id] = matcher.Value
				} else {
					filterIn[i] = map[int32]string{id: matcher.Value}
				}
			case labels.MatchNotEqual:
				id, err := ev.getTagValueID(metric, i, matcher.Value)
				if err != nil {
					if errors.Is(err, ErrNotFound) {
						continue // ignore values with no mapping
					}
					return seriesQueryX{}, err
				}
				if metricH && !histogramQ.restore && matcher.Name == format.LETagName {
					histogramQ.filter = true
					histogramQ.compare = false
					histogramQ.le = prometheus.LexDecode(id)
				} else if filterOut[i] != nil {
					filterOut[i][id] = matcher.Value
				} else {
					filterOut[i] = map[int32]string{id: matcher.Value}
				}
			case labels.MatchRegexp:
				m, err := ev.getTagValues(ctx, metric, i, offset)
				if err != nil {
					return seriesQueryX{}, err
				}
				in := make(map[int32]string)
				for id, str := range m {
					if matcher.Matches(str) {
						in[id] = str
					}
				}
				if len(in) == 0 {
					// there no data satisfying the filter
					emptyCount[i]++
					continue
				}
				filterIn[i] = in
			case labels.MatchNotRegexp:
				m, err := ev.getTagValues(ctx, metric, i, offset)
				if err != nil {
					return seriesQueryX{}, err
				}
				out := make(map[int32]string)
				for id, str := range m {
					if !matcher.Matches(str) {
						out[id] = str
					}
				}
				filterOut[i] = out
			}
		}
	}
	for i, n := range emptyCount {
		if n == 0 {
			continue
		}
		var m int
		if i == format.MaxTags {
			m = len(sFilterIn)
		} else {
			m = len(filterIn[i])
		}
		if m == 0 {
			// All "MatchEqual" and "MatchRegexp" filters give an empty result and
			// there are no other such filters, overall result is guaranteed to be empty
			return seriesQueryX{}, nil
		}
	}
	if histogramQ.filter && !histogramQ.restore {
		groupBy = append(groupBy, format.TagID(format.LETagIndex))
		histogramQ.restore = true
	}
	return seriesQueryX{
			SeriesQuery{
				Metric:     metric,
				What:       what,
				Timescale:  ev.t,
				Offset:     offset,
				Range:      sel.Range,
				GroupBy:    groupBy,
				FilterIn:   filterIn,
				FilterOut:  filterOut,
				SFilterIn:  sFilterIn,
				SFilterOut: sFilterOut,
				MaxHost:    sel.MaxHost || ev.opt.MaxHost,
				Options:    ev.opt,
			},
			prefixSum,
			histogramQ},
		nil
}

func (ev *evaluator) newSeries(capacity int) Series {
	if capacity == 0 {
		return Series{}
	}
	return Series{Data: make([]SeriesData, 0, capacity)}
}

func (ev *evaluator) getTagValues(ctx context.Context, metric *format.MetricMetaValue, tagX int, offset int64) (map[int32]string, error) {
	m, ok := ev.tags[metric]
	if !ok {
		// tag index -> offset -> tag value ID -> tag value
		m = make([]map[int64]map[int32]string, format.MaxTags)
		ev.tags[metric] = m
	}
	m2 := m[tagX]
	if m2 == nil {
		// offset -> tag value ID -> tag value
		m2 = make(map[int64]map[int32]string)
		m[tagX] = m2
	}
	var res map[int32]string
	if res, ok = m2[offset]; ok {
		return res, nil
	}
	ids, err := ev.h.QueryTagValueIDs(ctx, TagValuesQuery{
		Metric:    metric,
		TagIndex:  tagX,
		Timescale: ev.t,
		Offset:    offset,
		Options:   ev.opt,
	})
	if err != nil {
		return nil, err
	}
	// tag value ID -> tag value
	res = make(map[int32]string, len(ids))
	for _, id := range ids {
		res[id] = ev.h.GetTagValue(TagValueQuery{
			Version:    ev.opt.Version,
			Metric:     metric,
			TagIndex:   tagX,
			TagValueID: id,
		})
	}
	m2[offset] = res
	return res, nil
}

func (ev *evaluator) getTagValueID(metric *format.MetricMetaValue, tagX int, tagV string) (int32, error) {
	if format.HasRawValuePrefix(tagV) {
		return format.ParseCodeTagValue(tagV)
	}
	if tagX < 0 || len(metric.Tags) <= tagX {
		return 0, ErrNotFound
	}
	t := metric.Tags[tagX]
	if t.Name == labels.BucketLabel && t.Raw {
		if v, err := strconv.ParseFloat(tagV, 32); err == nil {
			return prometheus.LexEncode(float32(v))
		}
	}
	return ev.h.GetTagValueID(TagValueIDQuery{
		Version:  ev.opt.Version,
		Metric:   metric,
		TagIndex: tagX,
		TagValue: tagV,
	})
}

func (ev *evaluator) getStringTop(ctx context.Context, metric *format.MetricMetaValue, offset int64) ([]string, error) {
	m, ok := ev.stop[metric]
	if !ok {
		// offset -> tag values
		m = make(map[int64][]string)
		ev.stop[metric] = m
	}
	var res []string
	if res, ok = m[offset]; ok {
		return res, nil
	}
	var err error
	res, err = ev.h.QueryStringTop(ctx, TagValuesQuery{
		Metric:    metric,
		Timescale: ev.t,
		Offset:    offset,
		Options:   ev.opt,
	})
	if err == nil {
		m[offset] = res
	}
	return res, err
}

func (ev *evaluator) alloc() SeriesData {
	var s *[]float64
	if len(ev.reuseList) != 0 {
		ev.reuseList, s = removeLast(ev.reuseList)
	} else if len(ev.freeList) != 0 {
		ev.freeList, s = removeLast(ev.freeList)
	} else {
		if ev.allocMap == nil {
			ev.allocMap = make(map[*[]float64]bool)
		}
		s = ev.h.Alloc(len(ev.time()))
		ev.allocMap[s] = true
	}
	return SeriesData{Values: s}
}

func (s *SeriesData) free(ev *evaluator) {
	if ev.allocMap != nil && ev.allocMap[s.Values] {
		delete(ev.allocMap, s.Values)
		ev.freeList = append(ev.freeList, s.Values)
	} else {
		ev.reuseList = append(ev.reuseList, s.Values)
	}
	s.Values = nil
}

func (ss *Series) free(ev *evaluator) {
	ev.free(ss.Data)
}

func (ev *evaluator) free(s []SeriesData) {
	for x := range s {
		s[x].free(ev)
	}
}

func (ev *evaluator) freeAt(s []SeriesData, x ...int) {
	for _, i := range x {
		s[i].free(ev)
	}
}

func (ev *evaluator) cancel() {
	if ev.allocMap != nil {
		for s := range ev.allocMap {
			ev.h.Free(s)
		}
		ev.allocMap = nil
	}
	for _, s := range ev.freeList {
		ev.h.Free(s)
	}
	ev.freeList = nil
	for _, cancel := range ev.cancellationList {
		cancel()
	}
	ev.reuseList = nil
	ev.cancellationList = nil
}

func (ev *evaluator) newWindow(v []float64, s bool) window {
	return newWindow(ev.time(), v, ev.r, s)
}

func (q *seriesQueryX) empty() bool {
	return q.Metric == nil
}

func TraceContext(ctx context.Context, s *[]string) context.Context {
	return context.WithValue(ctx, traceContextKey, &traceContext{s, false})
}

func DebugContext(ctx context.Context, s *[]string) context.Context {
	return context.WithValue(ctx, traceContextKey, &traceContext{s, true})
}

func (ev *evaluator) tracef(format string, a ...any) {
	*ev.trace = append(*ev.trace, fmt.Sprintf(format, a...))
}

func evalLiteral(expr parser.Expr) (*parser.NumberLiteral, bool) {
	switch e := expr.(type) {
	case *parser.BinaryExpr:
		l, okL := evalLiteral(e.LHS)
		r, okR := evalLiteral(e.RHS)
		if okL && okR {
			return &parser.NumberLiteral{Val: getBinaryFunc(scalarBinaryFuncM, e.Op, e.ReturnBool)(l.Val, r.Val)}, true
		} else if okL {
			e.LHS = l
		} else if okR {
			e.RHS = r
		}
	case *parser.NumberLiteral:
		return e, true
	case *parser.ParenExpr:
		return evalLiteral(e.Expr)
	case *parser.UnaryExpr:
		if l, ok := evalLiteral(e.Expr); ok {
			if e.Op == parser.SUB {
				l.Val = -l.Val
			}
			return l, true
		}
	default:
		for _, node := range parser.Children(expr) {
			if e2, ok := node.(parser.Expr); ok {
				evalLiteral(e2)
			}
		}
	}
	return nil, false
}

func removeLast[V any](s []V) ([]V, V) {
	return s[:len(s)-1], s[len(s)-1]
}
