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
	labelOffset = "__offset__"
	labelTotal  = "__total__"
	LabelShard  = "__shard__"
	LabelFn     = "__fn__"
)

type Query struct {
	Start int64
	End   int64
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

	ExprQueriesSingleMetricCallback MetricMetaValueCallback
	SeriesQueryCallback             SeriesQueryCallback
}

type (
	MetricMetaValueCallback func(*format.MetricMetaValue)
	SeriesQueryCallback     func(version string, key string, pq any, lod any, avoidCache bool)
)

type Engine struct {
	h   Handler
	loc *time.Location
}

type evaluator struct {
	Engine
	Options Options

	ast parser.Expr
	ars map[parser.Expr]parser.Expr // ast reductions
	t   Timescale

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
		bag, err = ev.exec(ctx)
		if err != nil {
			ev.cancel()
			return nil, nil, Error{what: err}
		}
		if qry.Options.ExpandToLODBoundary {
			bag.trim(ev.t.Start, ev.t.End)
		} else {
			bag.trim(qry.Start, qry.End)
		}
		bag.stringify(&ev)
		debugTracef(ctx, "%v", &bag)
		return &bag, ev.cancel, nil
	}
}

func (ng Engine) newEvaluator(ctx context.Context, qry Query) (evaluator, error) {
	if qry.Options.TimeNow == 0 {
		qry.Options.TimeNow = time.Now().Unix()
	}
	ast, err := parser.ParseExpr(qry.Expr)
	if err != nil {
		return evaluator{}, err
	}
	if l, ok := evalLiteral(ast); ok {
		ast = l
	}
	// offsets
	offsets := make([]int64, 0, 1+len(qry.Options.Offsets))
	offsets = append(offsets, 0)
	offsets = append(offsets, qry.Options.Offsets...)
	sort.Sort(sort.Reverse(sortkeys.Int64Slice(offsets)))
	qry.Options.Offsets = offsets
	// match metrics
	var (
		maxRange     int64
		metricOffset = make(map[*format.MetricMetaValue]int64)
	)
	parser.Inspect(ast, func(node parser.Node, path []parser.Node) error {
		switch e := node.(type) {
		case *parser.VectorSelector:
			err = ng.matchMetrics(ctx, e, path, metricOffset, offsets[0])
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
		Engine:  ng,
		Options: qry.Options,
		ast:     ast,
		ars:     ars,
		t:       t,
		tags:    make(map[*format.MetricMetaValue][]map[int64]map[int32]string),
		stags:   make(map[*format.MetricMetaValue]map[int64][]string),
		ba:      make(map[*[]float64]bool),
		br:      make(map[*[]float64]bool),
	}, nil
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

func (ev *evaluator) exec(ctx context.Context) (SeriesBag, error) {
	var (
		res        = ev.newSeriesBag(0)
		lastOffset = int64(math.MinInt64)
	)
	for _, offset := range ev.Options.Offsets {
		if lastOffset == offset {
			continue
		}
		lastOffset = offset
		bag, err := ev.eval(withOffset(ctx, offset), ev.ast)
		if err != nil {
			return SeriesBag{}, err
		}
		res.append(bag)
	}
	return res, nil
}

func (ev *evaluator) eval(ctx context.Context, expr parser.Expr) (res SeriesBag, err error) {
	if ctx.Err() != nil {
		return SeriesBag{}, ctx.Err()
	}
	if e, ok := ev.ars[expr]; ok {
		debugTracef(ctx, "replace %s with %s", string(expr.Type()), string(e.Type()))
		return ev.eval(ctx, e)
	}
	switch e := expr.(type) {
	case *parser.AggregateExpr:
		res, err = ev.evalAggregate(ctx, e)
		if err == nil && e.Op != parser.TOPK && e.Op != parser.BOTTOMK {
			res.dropMetricName()
		}
	case *parser.BinaryExpr:
		switch l := e.LHS.(type) {
		case *parser.NumberLiteral:
			res, err = ev.eval(ctx, e.RHS)
			if err == nil {
				fn := getBinaryFunc(scalarSliceFuncM, e.Op, e.ReturnBool)
				if fn == nil {
					return SeriesBag{}, fmt.Errorf("binary operator %q is not defined on (%q, %q) pair", e.Op, e.LHS.Type(), e.RHS.Type())
				}
				for _, row := range res.Data {
					fn(l.Val, *row)
				}
			}
		default:
			switch r := e.RHS.(type) {
			case *parser.NumberLiteral:
				res, err = ev.eval(ctx, e.LHS)
				if err == nil {
					fn := getBinaryFunc(sliceScalarFuncM, e.Op, e.ReturnBool)
					if fn == nil {
						return SeriesBag{}, fmt.Errorf("binary operator %q is not defined on (%q, %q) pair", e.Op, e.LHS.Type(), e.RHS.Type())
					}
					for _, row := range res.Data {
						fn(*row, r.Val)
					}
				}
			default:
				res, err = ev.evalBinary(ctx, e)
			}
		}
		if err == nil && (e.ReturnBool || len(e.VectorMatching.MatchingLabels) != 0 || shouldDropMetricName(e.Op)) {
			res.dropMetricName()
		}
	case *parser.Call:
		if fn, ok := calls[e.Func.Name]; ok {
			res, err = fn(ctx, ev, e.Args)
			if err == nil {
				res.Range = 0
				switch e.Func.Name {
				case "label_join", "label_replace", "last_over_time":
					break // keep metric name
				default:
					res.dropMetricName()
				}
			}
		} else {
			err = fmt.Errorf("not implemented function %q", e.Func.Name)
		}
	case *parser.MatrixSelector:
		res, err = ev.eval(ctx, e.VectorSelector)
		if err == nil {
			res.Range = e.Range
		}
	case *parser.NumberLiteral:
		row := ev.alloc()
		for i := range *row {
			(*row)[i] = e.Val
		}
		res = SeriesBag{Time: ev.time(), Data: []*[]float64{row}}
	case *parser.ParenExpr:
		res, err = ev.eval(ctx, e.Expr)
	case *parser.SubqueryExpr:
		res, err = ev.eval(ctx, e.Expr)
		if err == nil {
			res.Range = e.Range
		}
	case *parser.UnaryExpr:
		res, err = ev.eval(ctx, e.Expr)
		if err == nil && e.Op == parser.SUB {
			for _, row := range res.Data {
				for i := range *row {
					(*row)[i] = -(*row)[i]
				}
			}
			res.dropMetricName()
		}
	case *parser.VectorSelector:
		res, err = ev.querySeries(ctx, e)
	default:
		err = fmt.Errorf("not implemented %T", expr)
	}
	return res, err
}

func (ev *evaluator) evalAggregate(ctx context.Context, expr *parser.AggregateExpr) (SeriesBag, error) {
	bag, err := ev.eval(ctx, expr.Expr)
	if err != nil {
		return SeriesBag{}, err
	}
	if len(bag.Data) == 0 {
		return bag, nil
	}
	var fn aggregateFunc
	if fn = aggregates[expr.Op]; fn == nil {
		return SeriesBag{}, fmt.Errorf("not implemented aggregate %q", expr.Op)
	}
	var groups []seriesGroup
	groups, err = bag.group(ev, expr.Without, expr.Grouping)
	if err != nil {
		return SeriesBag{}, err
	}
	res := ev.newSeriesBag(len(groups))
	for _, g := range groups {
		res.append(fn(ev, g, expr.Param))
	}
	return res, nil
}

func (ev *evaluator) evalBinary(ctx context.Context, expr *parser.BinaryExpr) (res SeriesBag, err error) {
	if expr.Op.IsSetOperator() {
		expr.VectorMatching.Card = parser.CardManyToMany
	}
	var (
		bags [2]SeriesBag
		args = [2]parser.Expr{expr.LHS, expr.RHS}
	)
	for i := range args {
		bags[i], err = ev.eval(ctx, args[i])
		if err != nil {
			return SeriesBag{}, err
		}
	}
	fn := getBinaryFunc(sliceBinaryFuncM, expr.Op, expr.ReturnBool)
	switch expr.VectorMatching.Card {
	case parser.CardOneToOne:
		l := bags[0]
		r := bags[1]
		if r.scalar() {
			for i := range l.Data {
				fn(*l.Data[i], *l.Data[i], *r.Data[0])
			}
			res = l
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
			res = r
			ev.free(l.Data[0])
		} else {
			var mappingL map[uint64]int
			mappingL, err = l.hash(!expr.VectorMatching.On, expr.VectorMatching.MatchingLabels)
			if err != nil {
				return SeriesBag{}, err
			}
			var mappingR map[uint64]int
			mappingR, err = r.hash(!expr.VectorMatching.On, expr.VectorMatching.MatchingLabels)
			if err != nil {
				return SeriesBag{}, err
			}
			res = ev.newSeriesBag(len(mappingL))
			for h, xl := range mappingL {
				if xr, ok := mappingR[h]; ok {
					fn(*l.Data[xl], *l.Data[xl], *r.Data[xr])
					res.appendX(l, xl)
				} else {
					ev.free(l.Data[xl])
				}
			}
			for _, xr := range mappingR {
				ev.free(r.Data[xr])
			}
		}
	case parser.CardManyToOne:
		var groups []seriesGroup
		groups, err = bags[0].group(ev, !expr.VectorMatching.On, expr.VectorMatching.MatchingLabels)
		if err != nil {
			return SeriesBag{}, err
		}
		one := bags[1]
		var oneM map[uint64]int
		oneM, err = one.hash(!expr.VectorMatching.On, expr.VectorMatching.MatchingLabels)
		if err != nil {
			return SeriesBag{}, err
		}
		res = ev.newSeriesBag(len(groups))
		for _, many := range groups {
			if oneX, ok := oneM[many.hash]; ok {
				for manyX, manyRow := range many.bag.Data {
					fn(*manyRow, *manyRow, *one.Data[oneX])
					for _, tagName := range expr.VectorMatching.Include {
						if t, ok := one.getTagAt(oneX, tagName); ok {
							many.bag.setTagAt(manyX, t)
						}
					}
					res.appendX(many.bag, manyX)
				}
			}
		}
	case parser.CardOneToMany:
		one := bags[0]
		var oneM map[uint64]int
		oneM, err = one.hash(!expr.VectorMatching.On, expr.VectorMatching.MatchingLabels)
		if err != nil {
			return SeriesBag{}, err
		}
		var groups []seriesGroup
		groups, err = bags[1].group(ev, !expr.VectorMatching.On, expr.VectorMatching.MatchingLabels)
		if err != nil {
			return SeriesBag{}, err
		}
		res = ev.newSeriesBag(len(groups))
		for _, many := range groups {
			if oneX, ok := oneM[many.hash]; ok {
				for manyX, manyRow := range many.bag.Data {
					fn(*manyRow, *manyRow, *one.Data[oneX])
					for _, tagName := range expr.VectorMatching.Include {
						if t, ok := one.getTagAt(oneX, tagName); ok {
							many.bag.setTagAt(manyX, t)
						}
					}
					res.appendX(many.bag, manyX)
				}
			}
		}
	case parser.CardManyToMany:
		l := bags[0]
		var mappingL map[uint64]int
		mappingL, err = l.hash(!expr.VectorMatching.On, expr.VectorMatching.MatchingLabels)
		if err != nil {
			return SeriesBag{}, err
		}
		r := bags[1]
		var mappingR map[uint64]int
		mappingR, err = r.hash(!expr.VectorMatching.On, expr.VectorMatching.MatchingLabels)
		if err != nil {
			return SeriesBag{}, err
		}
		switch expr.Op {
		case parser.LAND:
			res = ev.newSeriesBag(len(mappingL))
			for h, xl := range mappingL {
				if _, ok := mappingR[h]; ok {
					res.appendX(l, xl)
				}
			}
		case parser.LDEFAULT:
			res = l
			for h, xl := range mappingL {
				if xr, ok := mappingR[h]; ok {
					sliceDefault(*res.Data[xl], *l.Data[xl], *r.Data[xr])
				}
			}
		case parser.LOR:
			res = l
			for h, xr := range mappingR {
				if _, ok := mappingL[h]; !ok {
					res.appendX(r, xr)
				}
			}
		case parser.LUNLESS:
			res = ev.newSeriesBag(len(mappingL))
			for h, xl := range mappingL {
				if _, ok := mappingR[h]; !ok {
					res.appendX(l, xl)
				}
			}
		default:
			err = fmt.Errorf("not implemented binary operator %q", expr.Op)
		}
	}
	return res, err
}

func (ev *evaluator) querySeries(ctx context.Context, sel *parser.VectorSelector) (SeriesBag, error) {
	res := ev.newSeriesBag(0)
	for i, metric := range sel.MatchingMetrics {
		debugTracef(ctx, "#%d request %s: %s", i, metric.Name, sel.What)
		qry, err := ev.buildSeriesQuery(ctx, sel, metric, sel.What)
		if err != nil {
			return SeriesBag{}, err
		}
		if qry.empty() {
			debugTracef(ctx, "#%d query is empty", i)
			continue
		}
		bag, cancel, err := ev.h.QuerySeries(ctx, &qry.SeriesQuery)
		if err != nil {
			return SeriesBag{}, err
		}
		ev.cancellationList = append(ev.cancellationList, cancel)
		debugTracef(ctx, "#%d series count %d", i, len(bag.Meta))
		if qry.prefixSum {
			bag = ev.funcPrefixSum(bag)
		}
		if !sel.OmitNameTag {
			bag.setSTag(labels.MetricName, sel.MatchingNames[i])
		}
		if ev.Options.TagOffset && qry.Offset != 0 {
			bag.setTag(labelOffset, int32(qry.Offset))
		}
		if qry.histogram.restore {
			bag, err = ev.restoreHistogram(&bag, &qry)
			if err != nil {
				return SeriesBag{}, err
			}
		}
		for _, v := range sel.LabelMatchers {
			if v.Name == LabelFn {
				bag.setSTag(LabelFn, v.Value)
				break
			}
		}
		res.append(bag)
	}
	if ev.Options.TagTotal {
		res.setTag(labelTotal, int32(len(res.Data)))
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

func (ev *evaluator) buildSeriesQuery(ctx context.Context, sel *parser.VectorSelector, metric *format.MetricMetaValue, selWhat string) (seriesQueryX, error) {
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
			groupBy = append(groupBy, format.TagIDLegacy(t.Index))
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
			t, ok := metric.Name2Tag[name]
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
			switch k {
			case format.StringTopTagID, format.NewStringTopTagID:
				groupBy = append(groupBy, format.StringTopTagID)
			case LabelShard:
				groupBy = append(groupBy, format.ShardTagID)
			default:
				if t, ok := metric.Name2Tag[k]; ok && 0 <= t.Index && t.Index < format.MaxTags {
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
		emptyCount int // number of "MatchEqual" or "MatchRegexp" filters which are guaranteed to yield empty response
	)
	for _, matcher := range sel.LabelMatchers {
		if strings.HasPrefix(matcher.Name, "__") {
			continue
		}
		switch matcher.Name {
		case format.StringTopTagID, format.NewStringTopTagID:
			switch matcher.Type {
			case labels.MatchEqual:
				sFilterIn = append(sFilterIn, matcher.Value)
			case labels.MatchNotEqual:
				sFilterOut = append(sFilterOut, matcher.Value)
			case labels.MatchRegexp:
				strTop, err := ev.getSTagValues(ctx, metric, ev.getOffset(ctx, sel))
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
					emptyCount++
					continue
				}
			case labels.MatchNotRegexp:
				strTop, err := ev.getSTagValues(ctx, metric, ev.getOffset(ctx, sel))
				if err != nil {
					return seriesQueryX{}, err
				}
				for _, str := range strTop {
					if !matcher.Matches(str) {
						sFilterOut = append(sFilterOut, str)
					}
				}
			}
		default:
			i := metric.Name2Tag[matcher.Name].Index
			switch matcher.Type {
			case labels.MatchEqual:
				id, err := ev.getTagValueID(metric, i, matcher.Value)
				if err != nil {
					if errors.Is(err, ErrNotFound) {
						// string is not mapped, result is guaranteed to be empty
						emptyCount++
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
				m, err := ev.getTagValues(ctx, metric, i, ev.getOffset(ctx, sel))
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
					emptyCount++
					continue
				}
				filterIn[i] = in
			case labels.MatchNotRegexp:
				m, err := ev.getTagValues(ctx, metric, i, ev.getOffset(ctx, sel))
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
	if emptyCount != 0 {
		var filterInSet bool
		for _, v := range filterIn {
			if len(v) != 0 {
				filterInSet = true
				break
			}
		}
		if !filterInSet && len(sFilterIn) == 0 {
			// All "MatchEqual" and "MatchRegexp" filters give an empty result and
			// there are no other such filters, overall result is guaranteed to be empty
			return seriesQueryX{}, nil
		}
	}
	if histogramQ.filter && !histogramQ.restore {
		groupBy = append(groupBy, format.TagIDLegacy(metric.Name2Tag[format.LETagName].Index))
		histogramQ.restore = true
	}
	return seriesQueryX{
			SeriesQuery{
				Metric:     metric,
				What:       what,
				Timescale:  ev.t,
				Offset:     ev.getOffset(ctx, sel),
				Range:      sel.Range,
				GroupBy:    groupBy,
				FilterIn:   filterIn,
				FilterOut:  filterOut,
				SFilterIn:  sFilterIn,
				SFilterOut: sFilterOut,
				MaxHost:    sel.MaxHost || ev.Options.MaxHost,
				Options:    ev.Options,
			},
			prefixSum,
			histogramQ},
		nil
}

func (ev *evaluator) newSeriesBag(capacity int) SeriesBag {
	if capacity == 0 {
		return SeriesBag{Time: ev.time()}
	}
	return SeriesBag{
		Time: ev.time(),
		Data: make([]*[]float64, 0, capacity),
		Meta: make([]SeriesMeta, 0, capacity),
	}
}

func (ev *evaluator) getTagValue(metric *format.MetricMetaValue, tagName string, tagValueID int32) string {
	return ev.h.GetTagValue(TagValueQuery{
		Version:    ev.Options.Version,
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
		Version:   ev.Options.Version,
		Metric:    metric,
		TagIndex:  tagX,
		Timescale: ev.t,
		Offset:    offset,
		Options:   ev.Options,
	})
	if err != nil {
		return nil, err
	}
	// tag value ID -> tag value
	res = make(map[int32]string, len(ids))
	for _, id := range ids {
		res[id] = ev.h.GetTagValue(TagValueQuery{
			Version:    ev.Options.Version,
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
		Version:  ev.Options.Version,
		Metric:   metric,
		TagIndex: tagX,
		TagValue: tagV,
	})
}

func (ev *evaluator) getOffset(ctx context.Context, sel *parser.VectorSelector) int64 {
	return sel.OriginalOffset + getOffset(ctx)
}

func withOffset(ctx context.Context, offset int64) context.Context {
	debugTracef(ctx, "offset %d", offset)
	return context.WithValue(ctx, offsetContextKey, offset)
}

func getOffset(ctx context.Context) int64 {
	if offset, ok := ctx.Value(offsetContextKey).(int64); ok {
		return offset
	}
	return 0
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
		Version:   ev.Options.Version,
		Metric:    metric,
		Timescale: ev.t,
		Offset:    offset,
		Options:   ev.Options,
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
