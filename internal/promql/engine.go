// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package promql

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/promql/parser"
	"github.com/vkcom/statshouse/internal/receiver/prometheus"
)

const (
	labelWhat   = "__what__"
	labelBy     = "__by__"
	labelOffset = "__offset__"
	labelTotal  = "__total__"
)

type Query struct {
	Start   int64
	End     int64
	Step    int64
	Expr    string
	Options Options // StatsHouse specific
}

type Options struct {
	TimeNow             int64
	StepAuto            bool
	ExpandToLODBoundary bool
	TagOffset           bool
	TagTotal            bool
	CanonicalTagNames   bool

	ExprQueriesSingleMetricCallback func(*format.MetricMetaValue)
}

type Engine struct {
	h   Handler
	loc *time.Location
}

type evaluator struct {
	Engine

	now int64
	qry Query
	ast parser.Expr
	ars map[parser.Expr]parser.Expr // ast reductions

	lods []LOD
	time []int64

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

func NewEngine(h Handler, loc *time.Location) Engine {
	return Engine{h, loc}
}

func (ng Engine) Exec(ctx context.Context, qry Query) (res parser.Value, cancel func(), err error) {
	var ev evaluator
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("engine panic: %v", r)
			ev.cancel()
		}
	}()
	// parse query
	ev, err = ng.newEvaluator(ctx, qry)
	if err != nil {
		return nil, nil, err
	}
	// evaluate query
	switch e := ev.ast.(type) {
	case *parser.StringLiteral:
		return String{T: qry.Start, V: e.Val}, func() {}, nil
	default:
		var bag SeriesBag
		bag, err = ev.eval(ctx, ev.ast)
		if err != nil {
			ev.cancel()
			return nil, nil, err
		}
		if !qry.Options.ExpandToLODBoundary {
			bag.trim(qry.Start, qry.End)
		}
		ev.stringify(&bag)
		return &bag, ev.cancel, nil
	}
}

func (ng Engine) newEvaluator(ctx context.Context, qry Query) (ev evaluator, err error) {
	ev = evaluator{
		Engine: ng,
		ars:    make(map[parser.Expr]parser.Expr),
		tags:   make(map[*format.MetricMetaValue][]map[int64]map[int32]string),
		stags:  make(map[*format.MetricMetaValue]map[int64][]string),
		ba:     make(map[*[]float64]bool),
		br:     make(map[*[]float64]bool),
	}
	if qry.Options.TimeNow != 0 {
		ev.now = qry.Options.TimeNow
	} else {
		ev.now = time.Now().Unix()
	}
	ev.ast, err = parser.ParseExpr(qry.Expr)
	if err != nil {
		return ev, err
	}
	if l, ok := evalLiteral(ev.ast); ok {
		ev.ast = l
	}
	// match metrics
	maxOffset := make(map[*format.MetricMetaValue]int64)
	var maxRange int64
	parser.Inspect(ev.ast, func(node parser.Node, _ []parser.Node) error {
		switch e := node.(type) {
		case *parser.VectorSelector:
			err = ng.matchMetrics(ctx, e, maxOffset)
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
		return ev, err
	}
	// lods, from and time
	qry.Start -= maxRange // widen time range to accommodate range selectors
	if qry.Step <= 0 {    // instant query case
		qry.Step = 1
	}
	ev.lods, err = ng.h.GetQueryLODs(qry, maxOffset, ev.now)
	if err != nil {
		return ev, err
	}
	if len(ev.lods) == 0 {
		ev.lods = []LOD{{Len: (qry.End-qry.Start)/qry.Step + 1, Step: qry.Step}}
	}
	timeLen := int64(0)
	for _, v := range ev.lods {
		timeLen += v.Len
	}
	var (
		stepMax = ev.lods[0].Step
		stepMin = ev.lods[len(ev.lods)-1].Step
		from    = qry.Start / stepMax * stepMax
		to      = from
	)
	ev.time = make([]int64, 0, timeLen)
	for _, v := range ev.lods {
		for i := 0; i < int(v.Len); i++ {
			ev.time = append(ev.time, to)
			to += v.Step
		}
	}
	qry.Start = from
	qry.End = to
	// evaluate reduction rules, align selectors offsets
	parser.Inspect(ev.ast, func(node parser.Node, nodes []parser.Node) error {
		switch s := node.(type) {
		case *parser.VectorSelector:
			var grouped bool
			if s.GroupBy != nil {
				grouped = true
			}
			if !grouped {
				if ar, ok := evalReductionRules(s, nodes, stepMin); ok {
					s.What = ar.what
					s.GroupBy = ar.groupBy
					s.GroupWithout = ar.groupWithout
					s.Factor = ar.factor
					s.OmitNameTag = true
					ev.ars[ar.expr] = s
					grouped = ar.grouped
				}
			}
			if !grouped { // then group by all
				for i := 0; i < format.MaxTags; i++ {
					s.GroupBy = append(s.GroupBy, format.TagID(i))
				}
			}
			s.Offset = s.OriginalOffset / stepMax * stepMax
		case *parser.SubqueryExpr:
			s.Offset = s.OriginalOffset / stepMax * stepMax
		}
		return nil
	})
	// save effective query
	ev.qry = qry
	// callback
	if qry.Options.ExprQueriesSingleMetricCallback != nil && len(maxOffset) == 1 {
		var metric *format.MetricMetaValue
		for metric = range maxOffset {
			break
		}
		qry.Options.ExprQueriesSingleMetricCallback(metric)
	}
	return ev, nil
}

func (ng Engine) matchMetrics(ctx context.Context, sel *parser.VectorSelector, maxOffset map[*format.MetricMetaValue]int64) error {
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
				return nil // metric does not exist, not an error
			}
			for i, m := range metrics {
				offset, ok := maxOffset[m]
				if !ok || offset < sel.OriginalOffset {
					maxOffset[m] = sel.OriginalOffset
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
	return nil
}

func (ev *evaluator) eval(ctx context.Context, expr parser.Expr) (res SeriesBag, err error) {
	if e, ok := ev.ars[expr]; ok {
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
		res = SeriesBag{Time: ev.time, Data: []*[]float64{row}}
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
	var fn aggregateFunc
	if fn = aggregates[expr.Op]; fn == nil {
		return SeriesBag{}, fmt.Errorf("not implemented aggregate %q", expr.Op)
	}
	var groups []seriesGroup
	groups, err = bag.group(expr.Without, expr.Grouping)
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
	var bags [2]SeriesBag
	args := [2]parser.Expr{expr.LHS, expr.RHS}
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
		groups, err = bags[0].group(!expr.VectorMatching.On, expr.VectorMatching.MatchingLabels)
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
						var tag int32
						var tagValue string
						if tag, ok = one.getTag(oneX, tagName); ok {
							many.bag.setTag(manyX, tagName, tag)
						} else if tagValue, ok = one.getSTag(oneX, tagName); ok {
							many.bag.setSTag(manyX, tagName, tagValue)
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
		groups, err = bags[1].group(!expr.VectorMatching.On, expr.VectorMatching.MatchingLabels)
		if err != nil {
			return SeriesBag{}, err
		}
		res = ev.newSeriesBag(len(groups))
		for _, many := range groups {
			if oneX, ok := oneM[many.hash]; ok {
				for manyX, manyRow := range many.bag.Data {
					fn(*manyRow, *manyRow, *one.Data[oneX])
					for _, tagName := range expr.VectorMatching.Include {
						var tag int32
						var tagValue string
						if tag, ok = one.getTag(oneX, tagName); ok {
							many.bag.setTag(manyX, tagName, tag)
						} else if tagValue, ok = one.getSTag(oneX, tagName); ok {
							many.bag.setSTag(manyX, tagName, tagValue)
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
	for i, m := range sel.MatchingMetrics {
		qry, err := ev.buildSeriesQuery(ctx, sel, m)
		if err != nil {
			return SeriesBag{}, err
		}
		bag, cancel, err := ev.h.QuerySeries(ctx, &qry.SeriesQuery)
		if err != nil {
			return SeriesBag{}, err
		}
		ev.cancellationList = append(ev.cancellationList, cancel)
		if qry.prefixSum {
			bag = funcPrefixSum(bag)
		}
		if !sel.OmitNameTag {
			for j := range bag.Meta {
				bag.Meta[j].SetSTag(labels.MetricName, sel.MatchingNames[i])
			}
		}
		if ev.qry.Options.TagOffset && sel.OriginalOffset != 0 {
			bag.tagOffset(sel.OriginalOffset)
		}
		if qry.histogram.restore {
			bag, err = ev.restoreHistogram(&bag, &qry)
			if err != nil {
				return SeriesBag{}, err
			}
		}
		res.append(bag)
	}
	if ev.qry.Options.TagTotal {
		res.tagTotal(len(res.Data))
	}
	return res, nil
}

func (ev *evaluator) restoreHistogram(bag *SeriesBag, qry *seriesQueryX) (SeriesBag, error) {
	s, err := bag.histograms()
	if err != nil {
		return SeriesBag{}, err
	}
	for _, h := range s {
		for i := 1; i < len(h.buckets); i++ {
			for j := 0; j < len(ev.time); j++ {
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

func (ev *evaluator) buildSeriesQuery(ctx context.Context, sel *parser.VectorSelector, metric *format.MetricMetaValue) (seriesQueryX, error) {
	// what
	var (
		what      DigestWhat
		prefixSum bool
	)
	switch sel.What {
	case Count:
		what = DigestCount
	case Min:
		what = DigestMin
	case Max:
		what = DigestMax
	case Sum:
		what = DigestSum
	case Avg:
		what = DigestAvg
	case StdDev:
		what = DigestStdDev
	case StdVar:
		what = DigestStdVar
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
	case Unique:
		what = DigestUnique
	case "":
		if metric.Kind == format.MetricKindCounter {
			what = DigestCount
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
	)
	if sel.GroupWithout {
		skip := make(map[int]bool)
		for _, name := range sel.GroupBy {
			tag, ok := metric.Name2Tag[name]
			if ok {
				skip[tag.Index] = true
			}
		}
		for _, tag := range metric.Tags {
			if !skip[tag.Index] {
				groupBy = append(groupBy, format.TagID(tag.Index))
				if tag.Name == format.LETagName {
					histogramQ.restore = true
				}
			}
		}
	} else {
		groupBy = make([]string, 0, len(sel.GroupBy))
		for _, name := range sel.GroupBy {
			if tag, ok := metric.Name2Tag[name]; ok {
				groupBy = append(groupBy, format.TagID(tag.Index))
				if tag.Name == format.LETagName {
					histogramQ.restore = true
				}
			}
		}
	}
	// filtering
	var (
		filterIn   [format.MaxTags]map[int32]string // tagX -> tagValueID -> tagValue
		filterOut  [format.MaxTags]map[int32]string // as above
		sFilterIn  []string
		sFilterOut []string
	)
	for _, matcher := range sel.LabelMatchers {
		if strings.HasPrefix(matcher.Name, "__") {
			continue
		}
		if matcher.Name == format.StringTopTagID {
			switch matcher.Type {
			case labels.MatchEqual:
				sFilterIn = append(sFilterIn, matcher.Value)
			case labels.MatchNotEqual:
				sFilterOut = append(sFilterOut, matcher.Value)
			case labels.MatchRegexp:
				fallthrough
			case labels.MatchNotRegexp:
				strTop, err := ev.getSTagValues(ctx, metric, sel.Offset)
				if err != nil {
					return seriesQueryX{}, err
				}
				for _, str := range strTop {
					if matcher.Matches(str) {
						sFilterIn = append(sFilterIn, str)
					} else {
						sFilterOut = append(sFilterIn, str)
					}
				}
			}
		} else {
			i := metric.Name2Tag[matcher.Name].Index
			switch matcher.Type {
			case labels.MatchEqual:
				id, err := ev.getTagValueID(metric, i, matcher.Value)
				if err != nil {
					return seriesQueryX{}, err
				}
				if metricH && !histogramQ.restore && matcher.Name == format.LETagName {
					histogramQ.filter = true
					histogramQ.compare = true
					histogramQ.le = prometheus.LexDecode(id)
				} else {
					filterIn[i] = map[int32]string{id: matcher.Value}
				}
			case labels.MatchNotEqual:
				id, err := ev.getTagValueID(metric, i, matcher.Value)
				if err != nil {
					return seriesQueryX{}, err
				}
				if metricH && !histogramQ.restore && matcher.Name == format.LETagName {
					histogramQ.filter = true
					histogramQ.compare = false
					histogramQ.le = prometheus.LexDecode(id)
				} else {
					filterOut[i] = map[int32]string{id: matcher.Value}
				}
			case labels.MatchRegexp:
				fallthrough
			case labels.MatchNotRegexp:
				m, err := ev.getTagValues(ctx, metric, i, sel.Offset)
				if err != nil {
					return seriesQueryX{}, err
				}
				var (
					in  = make(map[int32]string)
					out = make(map[int32]string)
				)
				for id, str := range m {
					if matcher.Matches(str) {
						in[id] = str
					} else {
						out[id] = str
					}
				}
				filterIn[i] = in
				filterOut[i] = out
			}
		}
	}
	if histogramQ.filter && !histogramQ.restore {
		groupBy = append(groupBy, format.TagID(metric.Name2Tag[format.LETagName].Index))
		histogramQ.restore = true
	}
	return seriesQueryX{
			SeriesQuery{
				Meta:       metric,
				What:       what,
				From:       ev.qry.Start - sel.Offset,
				Factor:     sel.Factor,
				LODs:       ev.lods,
				GroupBy:    groupBy,
				FilterIn:   filterIn,
				FilterOut:  filterOut,
				SFilterIn:  sFilterIn,
				SFilterOut: sFilterOut,
				MaxHost:    sel.MaxHost,
				Options:    ev.qry.Options,
			},
			prefixSum,
			histogramQ},
		nil
}

func (ev *evaluator) newSeriesBag(capacity int) SeriesBag {
	if capacity == 0 {
		return SeriesBag{Time: ev.time}
	}
	return SeriesBag{
		Time: ev.time,
		Data: make([]*[]float64, 0, capacity),
		Meta: make([]SeriesMeta, 0, capacity),
	}
}

func (ev *evaluator) stringify(bag *SeriesBag) {
	for i := range bag.Meta {
		for name, value := range bag.Meta[i].Tags {
			if strings.HasPrefix(name, "__") {
				continue
			}
			bag.setSTag(i, name, ev.getTagValue(bag.Meta[i].Metric, name, value))
			delete(bag.Meta[i].Tags, name)
		}
	}
}

func (ev *evaluator) getTagValue(metric *format.MetricMetaValue, tagName string, tagValueID int32) string {
	var raw bool
	if metric != nil {
		raw = metric.Name2Tag[tagName].Raw
	}
	var res string
	switch {
	case raw:
		if tagName == format.LETagName {
			res = strconv.FormatFloat(float64(prometheus.LexDecode(tagValueID)), 'f', -1, 32)
		} else {
			res = strconv.FormatInt(int64(tagValueID), 10)
		}
	case tagValueID != 0:
		res = ev.h.GetTagValue(tagValueID)
	}
	return res
}

func (ev *evaluator) getTagValues(ctx context.Context, metric *format.MetricMetaValue, tagX int, offset int64) (map[int32]string, error) {
	m1, ok := ev.tags[metric]
	if !ok {
		// tag index -> offset -> tag value ID -> tag value
		m1 = make([]map[int64]map[int32]string, format.MaxTags)
		ev.tags[metric] = m1
	}
	m2 := m1[tagX]
	if m2 == nil {
		// offset -> tag value ID -> tag value
		m2 = make(map[int64]map[int32]string)
		m1[tagX] = m2
	}
	var res map[int32]string
	if res, ok = m2[offset]; ok {
		return res, nil
	}
	ids, err := ev.h.QueryTagValues(ctx, metric, tagX, ev.qry.Start, ev.qry.End)
	if err != nil {
		return nil, err
	}
	// tag value ID -> tag value
	res = make(map[int32]string, len(ids))
	for _, id := range ids {
		res[id] = ev.h.GetTagValue(id)
	}
	m2[offset] = res
	return res, nil
}

func (ev *evaluator) getTagValueID(metric *format.MetricMetaValue, tagX int, tagV string) (int32, error) {
	if metric.Tags[tagX].Raw {
		return getRawTagValue(tagV)
	}
	return ev.h.GetTagValueID(tagV)
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
	res, err = ev.h.QuerySTagValues(ctx, metric, ev.qry.Start, ev.qry.End)
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
	s := ev.h.Alloc(len(ev.time))
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

func getRawTagValue(tagV string) (int32, error) {
	n, err := strconv.ParseInt(tagV, 10, 32)
	if err == nil {
		return int32(n), nil
	}
	var f float64
	f, err = strconv.ParseFloat(tagV, 32)
	if err != nil {
		return 0, err
	}
	return prometheus.LexEncode(float32(f))
}
