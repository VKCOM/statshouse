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
	labelWhat   = "__what__"
	labelBy     = "__by__"
	labelBind   = "__bind__"
	labelOffset = "__offset__"
	labelTotal  = "__total__"
	LabelShard  = "__shard__"
	LabelFn     = "__fn__"
)

type Query struct {
	Start int64 // inclusive
	End   int64 // exclusive
	Step  int64
	Expr  string

	Options Options // StatsHouse specific
}

// NB! If you add an option make sure that default Options{} corresponds to prometheus behavior.
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

	// metric -> tag index -> offset -> tag value id -> tag value
	tags map[*format.MetricMetaValue][]map[int64]map[int32]string
	// metric -> offset -> tag values
	stags map[*format.MetricMetaValue]map[int64][]string

	ba map[*[]float64]bool // buffers allocated
	br map[*[]float64]bool // buffers reused

	cancellationList []func()
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
		return &SeriesBag{Time: []int64{}}, func() {}, nil
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
	traceExpr(ctx, &ev)
	debugTracef(ctx, "requested from %d to %d, timescale from %d to %d", qry.Start, qry.End, ev.t.Start, ev.t.End)
	switch e := ev.ast.(type) {
	case *parser.StringLiteral:
		return String{T: qry.Start, V: e.Val}, func() {}, nil
	default:
		var bag SeriesBag
		bag, err = ev.exec()
		if err != nil {
			ev.cancel()
			return nil, nil, Error{what: err}
		}
		bag.stringify(&ev)
		debugTracef(ctx, "%v", &bag)
		return &bag, ev.cancel, nil
	}
}

func (ng Engine) newEvaluator(ctx context.Context, qry Query) (evaluator, error) {
	ast, err := parser.ParseExpr(qry.Expr)
	if err != nil {
		return evaluator{}, err
	}
	if l, ok := evalLiteral(ast); ok {
		ast = l
	}
	// ensure zero offset present, sort descending, remove duplicates
	offsets := make([]int64, 0, 1+len(qry.Options.Offsets))
	offsets = append(offsets, 0)
	offsets = append(offsets, qry.Options.Offsets...)
	sort.Sort(sort.Reverse(sortkeys.Int64Slice(offsets)))
	var i int
	for j := 1; j < len(offsets); j++ {
		if offsets[i] != offsets[j] {
			i++
			offsets[i] = offsets[j]
		}
	}
	qry.Options.Offsets = offsets[:i+1]
	// match metrics
	var (
		maxRange     int64
		metricOffset = make(map[*format.MetricMetaValue]int64)
	)
	parser.Inspect(ast, func(node parser.Node, path []parser.Node) error {
		switch e := node.(type) {
		case *parser.VectorSelector:
			if err = ng.bindVariables(ctx, e, qry.Options.Vars); err == nil {
				err = ng.matchMetrics(ctx, e, path, metricOffset, offsets[0])
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
	// get timescale
	qry.Start -= maxRange // widen time range to accommodate range selectors
	if qry.Step <= 0 {    // instant query case
		qry.Step = 1
	}
	t, err := ng.h.GetTimescale(qry, metricOffset)
	if err != nil {
		return evaluator{}, err
	}
	// evaluate reduction rules
	var (
		ars     = make(map[parser.Expr]parser.Expr)
		stepMin = t.LODs[len(t.LODs)-1].Step
	)
	parser.Inspect(ast, func(node parser.Node, nodes []parser.Node) error {
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
					ars[ar.expr] = s
					grouped = ar.grouped
				}
			}
			if !grouped && !qry.Options.ExplicitGrouping {
				s.GroupByAll = true
			}
		}
		return nil
	})
	// callback
	if qry.Options.ExprQueriesSingleMetricCallback != nil && len(metricOffset) == 1 {
		for metric := range metricOffset {
			qry.Options.ExprQueriesSingleMetricCallback(metric)
			break
		}
	}
	return evaluator{
		Engine: ng,
		ctx:    ctx,
		opt:    qry.Options,
		ast:    ast,
		ars:    ars,
		t:      t,
		tags:   make(map[*format.MetricMetaValue][]map[int64]map[int32]string),
		stags:  make(map[*format.MetricMetaValue]map[int64][]string),
		ba:     make(map[*[]float64]bool),
		br:     make(map[*[]float64]bool),
	}, nil
}

func (ng Engine) bindVariables(ctx context.Context, sel *parser.VectorSelector, vars map[string]Variable) error {
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
			if vv, ok = vars[vn]; !ok {
				debugTracef(ctx, "variable %q not specified", vn)
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

func (ng Engine) matchMetrics(ctx context.Context, sel *parser.VectorSelector, path []parser.Node, metricOffset map[*format.MetricMetaValue]int64, offset int64) error {
	for _, matcher := range sel.LabelMatchers {
		if len(sel.MatchingMetrics) != 0 && len(sel.What) != 0 {
			break
		}
		switch matcher.Name {
		case labels.MetricName:
			metrics, names, err := ng.h.MatchMetrics(ctx, matcher)
			if err != nil {
				return err
			}
			if len(metrics) == 0 {
				debugTracef(ctx, "no metric matches %v", matcher)
				return nil // metric does not exist, not an error
			}
			debugTracef(ctx, "found %d metrics for %v", len(metrics), matcher)
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

func (ev *evaluator) exec() (SeriesBag, error) {
	bag, err := ev.eval(ev.ast)
	if err != nil {
		return SeriesBag{}, err
	}
	bag[0].Time = ev.time()
	bag[0].trim(ev.t.Start, ev.t.End)
	for x := 1; x < len(bag); x++ {
		bag[x].Time = ev.time()
		bag[x].trim(ev.t.Start, ev.t.End)
		bag[0].append(bag[x])
	}
	return bag[0], nil
}

func (ev *evaluator) eval(expr parser.Expr) (res []SeriesBag, err error) {
	if ev.ctx.Err() != nil {
		return nil, ev.ctx.Err()
	}
	if e, ok := ev.ars[expr]; ok {
		debugTracef(ev.ctx, "replace %s with %s", string(expr.Type()), string(e.Type()))
		return ev.eval(e)
	}
	switch e := expr.(type) {
	case *parser.AggregateExpr:
		if fn := aggregates[e.Op]; fn != nil {
			res, err = fn(ev, e)
			if err == nil && e.Op != parser.TOPK && e.Op != parser.BOTTOMK {
				dropMetricName(res)
			}
		} else {
			err = fmt.Errorf("not implemented aggregate %q", e.Op)
		}
	case *parser.BinaryExpr:
		switch l := e.LHS.(type) {
		case *parser.NumberLiteral:
			res, err = ev.eval(e.RHS)
			if err == nil {
				fn := getBinaryFunc(scalarSliceFuncM, e.Op, e.ReturnBool)
				if fn == nil {
					return nil, fmt.Errorf("binary operator %q is not defined on (%q, %q) pair", e.Op, e.LHS.Type(), e.RHS.Type())
				}
				for x := range res {
					for _, row := range res[x].Data {
						fn(l.Val, *row)
					}
				}
			}
		default:
			switch r := e.RHS.(type) {
			case *parser.NumberLiteral:
				res, err = ev.eval(e.LHS)
				if err == nil {
					fn := getBinaryFunc(sliceScalarFuncM, e.Op, e.ReturnBool)
					if fn == nil {
						return nil, fmt.Errorf("binary operator %q is not defined on (%q, %q) pair", e.Op, e.LHS.Type(), e.RHS.Type())
					}
					for x := range res {
						for _, row := range res[x].Data {
							fn(*row, r.Val)
						}
					}
				}
			default:
				res, err = ev.evalBinary(e)
			}
		}
		if err == nil && (e.ReturnBool || len(e.VectorMatching.MatchingLabels) != 0 || shouldDropMetricName(e.Op)) {
			dropMetricName(res)
		}
	case *parser.Call:
		if fn, ok := calls[e.Func.Name]; ok {
			res, err = fn(ev, e.Args)
			if err == nil {
				ev.r = 0
				switch e.Func.Name {
				case "label_join", "label_replace", "last_over_time":
					break // keep metric name
				default:
					dropMetricName(res)
				}
			}
		} else {
			err = fmt.Errorf("not implemented function %q", e.Func.Name)
		}
	case *parser.MatrixSelector:
		res, err = ev.eval(e.VectorSelector)
		if err == nil {
			ev.r = e.Range
		}
	case *parser.NumberLiteral:
		res = make([]SeriesBag, len(ev.opt.Offsets))
		for x := range res {
			row := ev.alloc()
			for i := range *row {
				(*row)[i] = e.Val
			}
			res[x] = SeriesBag{Data: []*[]float64{row}}
		}
	case *parser.ParenExpr:
		res, err = ev.eval(e.Expr)
	case *parser.SubqueryExpr:
		res, err = ev.eval(e.Expr)
		if err == nil {
			ev.r = e.Range
		}
	case *parser.UnaryExpr:
		res, err = ev.eval(e.Expr)
		if err == nil && e.Op == parser.SUB {
			for x := range res {
				for _, row := range res[x].Data {
					for i := range *row {
						(*row)[i] = -(*row)[i]
					}
				}
				res[x].dropMetricName()
			}
		}
	case *parser.VectorSelector:
		res, err = ev.querySeries(e)
	default:
		err = fmt.Errorf("not implemented %T", expr)
	}
	return res, err
}

func (ev *evaluator) evalBinary(expr *parser.BinaryExpr) ([]SeriesBag, error) {
	if expr.Op.IsSetOperator() {
		expr.VectorMatching.Card = parser.CardManyToMany
	}
	var (
		bags [2][]SeriesBag
		args = [2]parser.Expr{expr.LHS, expr.RHS}
		err  error
	)
	for i := range args {
		bags[i], err = ev.eval(args[i])
		if err != nil {
			return nil, err
		}
		if len(bags[i]) != len(ev.opt.Offsets) {
			panic("the number of bags does not match the number of offsets")
		}
	}
	res := make([]SeriesBag, len(ev.opt.Offsets))
	fn := getBinaryFunc(sliceBinaryFuncM, expr.Op, expr.ReturnBool)
	switch expr.VectorMatching.Card {
	case parser.CardOneToOne:
		for x := range ev.opt.Offsets {
			l := bags[0][x]
			r := bags[1][x]
			if r.scalar() {
				for i := range l.Data {
					fn(*l.Data[i], *l.Data[i], *r.Data[0])
				}
				res[x] = l
				ev.free(r.Data[0])
			} else if l.scalar() {
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
					for i := range r.Data {
						fn(*r.Data[i], *r.Data[i], *l.Data[0])
					}
				} else {
					for i := range r.Data {
						fn(*r.Data[i], *l.Data[0], *r.Data[i])
					}
				}
				res[x] = r
				ev.free(l.Data[0])
			} else {
				var mappingL map[uint64]int
				mappingL, err = l.hash(!expr.VectorMatching.On, expr.VectorMatching.MatchingLabels)
				if err != nil {
					return nil, err
				}
				var mappingR map[uint64]int
				mappingR, err = r.hash(!expr.VectorMatching.On, expr.VectorMatching.MatchingLabels)
				if err != nil {
					return nil, err
				}
				res[x] = ev.newSeriesBag(len(mappingL))
				for h, xl := range mappingL {
					if xr, ok := mappingR[h]; ok {
						fn(*l.Data[xl], *l.Data[xl], *r.Data[xr])
						res[x].appendX(l, xl)
					} else {
						ev.free(l.Data[xl])
					}
				}
				for _, xr := range mappingR {
					ev.free(r.Data[xr])
				}
			}
		}
	case parser.CardManyToOne:
		for x := range ev.opt.Offsets {
			var groups []seriesGroup
			groups, err = bags[0][x].group(ev, !expr.VectorMatching.On, expr.VectorMatching.MatchingLabels)
			if err != nil {
				return nil, err
			}
			one := bags[1][x]
			var oneM map[uint64]int
			oneM, err = one.hash(!expr.VectorMatching.On, expr.VectorMatching.MatchingLabels)
			if err != nil {
				return nil, err
			}
			res[x] = ev.newSeriesBag(len(groups))
			for _, many := range groups {
				if oneX, ok := oneM[many.hash]; ok {
					for manyX, manyRow := range many.bag.Data {
						fn(*manyRow, *manyRow, *one.Data[oneX])
						for _, tagName := range expr.VectorMatching.Include {
							if t, ok := one.getTagAt(oneX, tagName); ok {
								many.bag.setTagAt(manyX, t)
							}
						}
						res[x].appendX(many.bag, manyX)
					}
				}
			}
		}
	case parser.CardOneToMany:
		for x := range ev.opt.Offsets {
			one := bags[0][x]
			var oneM map[uint64]int
			oneM, err = one.hash(!expr.VectorMatching.On, expr.VectorMatching.MatchingLabels)
			if err != nil {
				return nil, err
			}
			var groups []seriesGroup
			groups, err = bags[1][x].group(ev, !expr.VectorMatching.On, expr.VectorMatching.MatchingLabels)
			if err != nil {
				return nil, err
			}
			res[x] = ev.newSeriesBag(len(groups))
			for _, many := range groups {
				if oneX, ok := oneM[many.hash]; ok {
					for manyX, manyRow := range many.bag.Data {
						fn(*manyRow, *manyRow, *one.Data[oneX])
						for _, tagName := range expr.VectorMatching.Include {
							if t, ok := one.getTagAt(oneX, tagName); ok {
								many.bag.setTagAt(manyX, t)
							}
						}
						res[x].appendX(many.bag, manyX)
					}
				}
			}
		}
	case parser.CardManyToMany:
		for x := range ev.opt.Offsets {
			l := bags[0][x]
			var mappingL map[uint64]int
			mappingL, err = l.hash(!expr.VectorMatching.On, expr.VectorMatching.MatchingLabels)
			if err != nil {
				return nil, err
			}
			r := bags[1][x]
			var mappingR map[uint64]int
			mappingR, err = r.hash(!expr.VectorMatching.On, expr.VectorMatching.MatchingLabels)
			if err != nil {
				return nil, err
			}
			switch expr.Op {
			case parser.LAND:
				res[x] = ev.newSeriesBag(len(mappingL))
				for h, xl := range mappingL {
					if _, ok := mappingR[h]; ok {
						res[x].appendX(l, xl)
					}
				}
			case parser.LDEFAULT:
				res[x] = l
				for h, xl := range mappingL {
					if xr, ok := mappingR[h]; ok {
						sliceDefault(*res[x].Data[xl], *l.Data[xl], *r.Data[xr])
					}
				}
			case parser.LOR:
				res[x] = l
				for h, xr := range mappingR {
					if _, ok := mappingL[h]; !ok {
						res[x].appendX(r, xr)
					}
				}
			case parser.LUNLESS:
				res[x] = ev.newSeriesBag(len(mappingL))
				for h, xl := range mappingL {
					if _, ok := mappingR[h]; !ok {
						res[x].appendX(l, xl)
					}
				}
			default:
				err = fmt.Errorf("not implemented binary operator %q", expr.Op)
			}
		}
	}
	return res, err
}

func (ev *evaluator) querySeries(sel *parser.VectorSelector) ([]SeriesBag, error) {
	res := make([]SeriesBag, len(ev.opt.Offsets))
	for i, metric := range sel.MatchingMetrics {
		debugTracef(ev.ctx, "#%d request %s: %s", i, metric.Name, sel.What)
		for x, offset := range ev.opt.Offsets {
			qry, err := ev.buildSeriesQuery(ev.ctx, sel, metric, sel.What, sel.OriginalOffset+offset)
			if err != nil {
				return nil, err
			}
			if qry.empty() {
				debugTracef(ev.ctx, "#%d query is empty", i)
				continue
			}
			bag, cancel, err := ev.h.QuerySeries(ev.ctx, &qry.SeriesQuery)
			if err != nil {
				return nil, err
			}
			ev.cancellationList = append(ev.cancellationList, cancel)
			debugTracef(ev.ctx, "#%d series count %d", i, len(bag.Meta))
			if qry.prefixSum {
				bag = ev.funcPrefixSum(bag)
			}
			if !sel.OmitNameTag {
				bag.setSTag(labels.MetricName, sel.MatchingNames[i])
			}
			if ev.opt.TagOffset && offset != 0 {
				bag.setTag(labelOffset, int32(offset))
			}
			if qry.histogram.restore {
				bag, err = ev.restoreHistogram(&bag, &qry)
				if err != nil {
					return nil, err
				}
			}
			for _, v := range sel.LabelMatchers {
				if v.Name == LabelFn {
					bag.setSTag(LabelFn, v.Value)
					break
				}
			}
			res[x].append(bag)
		}
	}
	if ev.opt.TagTotal {
		for x := range res {
			res[x].setTag(labelTotal, int32(len(res[x].Data)))
		}
	}
	return res, nil
}

func (ev *evaluator) restoreHistogram(bag *SeriesBag, qry *seriesQueryX) (SeriesBag, error) {
	s, err := bag.histograms(ev)
	if err != nil {
		return SeriesBag{}, err
	}
	for _, h := range s {
		for i := 1; i < len(h.buckets); i++ {
			for j := 0; j < len(ev.time()); j++ {
				(*h.group.bag.Data[h.buckets[i].x])[j] += (*h.group.bag.Data[h.buckets[i-1].x])[j]
			}
		}
	}
	if !qry.histogram.filter {
		return *bag, nil
	}
	res := ev.newSeriesBag(len(bag.Meta))
	for _, h := range s {
		for _, b := range h.buckets {
			if qry.histogram.le == b.le == qry.histogram.compare {
				res.append(h.group.bag.at(b.x))
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
				strTop, err := ev.getSTagValues(ctx, metric, offset)
				if err != nil {
					return seriesQueryX{}, err
				}
				var n int
				for _, str := range strTop {
					if matcher.Matches(str) {
						sFilterIn = append(sFilterIn, str)
						n++
					}
				}
				if n == 0 {
					// there no data satisfying the filter
					emptyCount[format.MaxTags]++
					continue
				}
			case labels.MatchNotRegexp:
				strTop, err := ev.getSTagValues(ctx, metric, offset)
				if err != nil {
					return seriesQueryX{}, err
				}
				for _, str := range strTop {
					if !matcher.Matches(str) {
						sFilterOut = append(sFilterOut, str)
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

func (ev *evaluator) newSeriesBag(capacity int) SeriesBag {
	if capacity == 0 {
		return SeriesBag{}
	}
	return SeriesBag{
		Data: make([]*[]float64, 0, capacity),
		Meta: make([]SeriesMeta, 0, capacity),
	}
}

func (ev *evaluator) weightData(data []*[]float64) []float64 {
	var (
		w      = make([]float64, len(data))
		nodecN int // number of non-decreasing series
	)
	for i, data := range data {
		var (
			j     int
			acc   float64
			prev  = -math.MaxFloat64
			nodec = true // non-decreasing
		)
		for _, lod := range ev.t.LODs {
			for m := 0; m < lod.Len; m++ {
				k := j + m
				if ev.t.Time[k] < ev.t.Start {
					continue // skip points before requested interval start
				}
				if ev.t.End <= ev.t.Time[k] {
					break // discard points after requested interval end
				}
				v := (*data)[k]
				if !math.IsNaN(v) {
					acc += v * v * float64(lod.Step)
					if v < prev {
						nodec = false
					}
					prev = v
				}
			}
			j += lod.Len
		}
		w[i] = acc
		if nodec {
			nodecN++
		}
	}
	if nodecN == len(w) {
		// all series are non-decreasing, weight is a last value
		for i, data := range data {
			last := -math.MaxFloat64
			for i := len(*data); i != 0; i-- {
				v := (*data)[i-1]
				if !math.IsNaN(v) {
					last = v
					break
				}
			}
			w[i] = last
		}
	}
	return w
}

func (ev *evaluator) getTagValue(metric *format.MetricMetaValue, tagName string, tagValueID int32) string {
	return ev.h.GetTagValue(TagValueQuery{
		Version:    ev.opt.Version,
		Metric:     metric,
		TagID:      tagName,
		TagValueID: tagValueID,
	})
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

func (ev *evaluator) getSTagValues(ctx context.Context, metric *format.MetricMetaValue, offset int64) ([]string, error) {
	m, ok := ev.stags[metric]
	if !ok {
		// offset -> tag values
		m = make(map[int64][]string)
		ev.stags[metric] = m
	}
	var res []string
	if res, ok = m[offset]; ok {
		return res, nil
	}
	var err error
	res, err = ev.h.QuerySTagValues(ctx, TagValuesQuery{
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

func (ev *evaluator) alloc() *[]float64 {
	for s, free := range ev.br {
		if free {
			ev.br[s] = false
			return s
		}
	}
	s := ev.h.Alloc(len(ev.time()))
	ev.ba[s] = true
	return s
}

func (ev *evaluator) free(s *[]float64) {
	if ev.ba[s] {
		ev.h.Free(s)
		delete(ev.ba, s)
	} else {
		ev.br[s] = true
	}
}

func (ev *evaluator) freeSeriesBagData(data []*[]float64, k int) {
	if k < 0 {
		return
	}
	for ; k < len(data); k++ {
		ev.free(data[k])
		data[k] = nil
	}
}

func (ev *evaluator) freeSeriesBagDataX(data []*[]float64, x ...int) {
	for _, i := range x {
		ev.free(data[i])
		data[i] = nil
	}
}

func (ev *evaluator) cancel() {
	for s := range ev.ba {
		ev.h.Free(s)
	}
	ev.ba = nil
	for _, cancel := range ev.cancellationList {
		cancel()
	}
	ev.cancellationList = nil
}

func (ev *evaluator) newWindow(v []float64, s bool) window {
	return newWindow(ev.time(), v, ev.r, s)
}

func (q *seriesQueryX) empty() bool {
	return q.Metric == nil
}

func TraceExprContext(ctx context.Context, s *[]string) context.Context {
	return context.WithValue(ctx, traceContextKey, &traceContext{s, false})
}

func DebugTraceContext(ctx context.Context, s *[]string) context.Context {
	return context.WithValue(ctx, traceContextKey, &traceContext{s, true})
}

func debugTracef(ctx context.Context, format string, a ...any) {
	if t, ok := ctx.Value(traceContextKey).(*traceContext); ok && t.v {
		*t.s = append(*t.s, fmt.Sprintf(format, a...))
	}
}

func traceExpr(ctx context.Context, ev *evaluator) {
	if t, ok := ctx.Value(traceContextKey).(*traceContext); ok {
		*t.s = append(*t.s, ev.ast.String())
	}
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
