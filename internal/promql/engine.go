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
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/promql/parser"
	"github.com/vkcom/statshouse/internal/receiver/prometheus"
)

const labelWhat = "__what__"

type Query struct {
	Start int64
	End   int64
	Step  int64
	Expr  string
}

type Engine struct {
	al  Allocator
	da  DataAccess
	loc *time.Location
}

type evaluator struct {
	al  Allocator
	da  DataAccess
	loc *time.Location
	now int64

	qry Query
	ast parser.Expr
	ars map[parser.Expr]parser.Expr // ast reductions

	lods []LOD
	from int64
	time []int64

	tagM tagMap    // tag value id <-> tag value
	tagV tagValues // tag (id,value)s for interval

	alBuffers          map[*[]float64]bool // buffers allocated with Allocator
	daBuffers          map[*[]float64]bool // reused DataAccess buffers
	daCancellationList []func()
}

type seriesQueryX struct { // SeriesQuery extended
	SeriesQuery
	histogram histogramQuery
}

type histogramQuery struct {
	restore bool
	filter  bool
	leEQ    bool  // compare "==" if true, "!=" otherwise
	leID    int32 // encoded "le" tag
}

func NewEngine(al Allocator, da DataAccess, loc *time.Location) Engine {
	return Engine{al, da, loc}
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
		var bag *SeriesBag
		bag, err = ev.eval(ctx, ev.ast)
		if err != nil {
			ev.cancel()
			return nil, nil, err
		}
		bag.Time = ev.time
		bag.Start = qry.Start
		ev.stringify(bag)
		return bag, ev.cancel, nil
	}
}

func (ng Engine) newEvaluator(ctx context.Context, qry Query) (ev evaluator, err error) {
	ev = evaluator{
		al:        ng.al,
		da:        ng.da,
		loc:       ng.loc,
		now:       time.Now().Unix(),
		ars:       make(map[parser.Expr]parser.Expr),
		alBuffers: make(map[*[]float64]bool),
		daBuffers: make(map[*[]float64]bool),
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
	ev.lods, err = ng.da.GetQueryLODs(qry, maxOffset, ev.now)
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
		step = ev.lods[0].Step
		from = qry.Start / step * step
		to   = from
	)
	ev.from = from
	ev.time = make([]int64, 0, timeLen)
	for _, v := range ev.lods {
		for i := 0; i < int(v.Len); i++ {
			ev.time = append(ev.time, to)
			to += v.Step
		}
	}
	// evaluate reduction rules, align selectors offsets
	parser.Inspect(ev.ast, func(node parser.Node, nodes []parser.Node) error {
		switch s := node.(type) {
		case *parser.VectorSelector:
			var grouped bool
			if ar, ok := evalReductionRules(s, nodes, step); ok {
				s.What = ar.what
				s.GroupBy = ar.groupBy
				s.GroupWithout = ar.groupWithout
				s.Factor = ar.factor
				s.OmitNameTag = true
				ev.ars[ar.expr] = s
				grouped = ar.grouped
			}
			if !grouped { // then group by all
				for i := 0; i < format.MaxTags; i++ {
					s.GroupBy = append(s.GroupBy, format.TagID(i))
				}
			}
			s.Offset = s.OriginalOffset / step * step
		case *parser.SubqueryExpr:
			s.Offset = s.OriginalOffset / step * step
		}
		return nil
	})
	// tag values
	ev.tagM = newTagMap(ng.da)
	ev.tagV = newTagValues(ng.da, from, to, ev.tagM)
	ev.qry = qry
	return ev, nil
}

func (ng Engine) matchMetrics(ctx context.Context, sel *parser.VectorSelector, maxOffset map[*format.MetricMetaValue]int64) error {
	for _, matcher := range sel.LabelMatchers {
		if len(sel.MatchingMetrics) != 0 && len(sel.What) != 0 {
			break
		}
		if matcher.Name == labels.MetricName {
			metrics, names, err := ng.da.MatchMetrics(ctx, matcher)
			if err != nil {
				return err
			}
			if len(metrics) == 0 {
				return nil // metric does not exist, not an error
			}
			for i, meta := range metrics {
				offset, ok := maxOffset[meta]
				if !ok || offset < sel.OriginalOffset {
					maxOffset[meta] = sel.OriginalOffset
				}
				sel.MatchingMetrics = append(sel.MatchingMetrics, meta)
				sel.MatchingNames = append(sel.MatchingNames, names[i])
			}
		} else if matcher.Name == labelWhat {
			if matcher.Type != labels.MatchEqual {
				return fmt.Errorf("%s supports only strict equality", labelWhat)
			}
			switch matcher.Value {
			case MaxHost:
				sel.MaxHost = true
			default:
				sel.What = matcher.Value
			}
		}
	}
	return nil
}

func (ev *evaluator) eval(ctx context.Context, expr parser.Expr) (res *SeriesBag, err error) {
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
		res = &SeriesBag{Time: ev.time, Data: []*[]float64{row}}
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

func (ev *evaluator) evalAggregate(ctx context.Context, expr *parser.AggregateExpr) (*SeriesBag, error) {
	res, err := ev.eval(ctx, expr.Expr)
	if err != nil || len(res.Data) == 0 {
		return res, err
	}
	var fn aggregateFunc
	if fn = aggregates[expr.Op]; fn == nil {
		return res, fmt.Errorf("not implemented aggregate %q", expr.Op)
	}
	var groups []seriesGroup
	groups, err = res.group(expr.Without, expr.Grouping)
	if err != nil {
		return nil, err
	}
	bags := make([]*SeriesBag, 0, len(groups))
	for _, g := range groups {
		bags = append(bags, fn(ev, g, expr.Param))
	}
	res = &SeriesBag{Time: ev.time}
	res.append(bags...)
	return res, nil
}

func (ev *evaluator) evalBinary(ctx context.Context, expr *parser.BinaryExpr) (res *SeriesBag, err error) {
	if expr.Op.IsSetOperator() {
		expr.VectorMatching.Card = parser.CardManyToMany
	}
	var bags [2]*SeriesBag
	args := [2]parser.Expr{expr.LHS, expr.RHS}
	for i := range args {
		bags[i], err = ev.eval(ctx, args[i])
		if err != nil {
			return nil, err
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
				return nil, err
			}
			var mappingR map[uint64]int
			mappingR, err = r.hash(!expr.VectorMatching.On, expr.VectorMatching.MatchingLabels)
			if err != nil {
				return nil, err
			}
			res = &SeriesBag{Time: ev.time}
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
			return nil, err
		}
		one := bags[1]
		var oneM map[uint64]int
		oneM, err = one.hash(!expr.VectorMatching.On, expr.VectorMatching.MatchingLabels)
		if err != nil {
			return nil, err
		}
		res = &SeriesBag{Time: ev.time}
		for _, many := range groups {
			if oneX, ok := oneM[many.hash]; ok {
				for manyX, manyRow := range many.bag.Data {
					fn(*manyRow, *manyRow, *one.Data[oneX])
					for _, tagName := range expr.VectorMatching.Include {
						var tag TagValue
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
			return nil, err
		}
		var groups []seriesGroup
		groups, err = bags[1].group(!expr.VectorMatching.On, expr.VectorMatching.MatchingLabels)
		if err != nil {
			return nil, err
		}
		res = &SeriesBag{Time: ev.time}
		for _, many := range groups {
			if oneX, ok := oneM[many.hash]; ok {
				for manyX, manyRow := range many.bag.Data {
					fn(*manyRow, *manyRow, *one.Data[oneX])
					for _, tagName := range expr.VectorMatching.Include {
						var tag TagValue
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
			return nil, err
		}
		r := bags[1]
		var mappingR map[uint64]int
		mappingR, err = r.hash(!expr.VectorMatching.On, expr.VectorMatching.MatchingLabels)
		if err != nil {
			return nil, err
		}
		res = &SeriesBag{Time: ev.time}
		switch expr.Op {
		case parser.LAND:
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

func (ev *evaluator) querySeries(ctx context.Context, sel *parser.VectorSelector) (*SeriesBag, error) {
	bags := make([]*SeriesBag, len(sel.MatchingMetrics))
	for i, meta := range sel.MatchingMetrics {
		qry, err := ev.buildSeriesQuery(ctx, sel, meta)
		if err != nil {
			return nil, err
		}
		var cancel func()
		bags[i], cancel, err = ev.da.QuerySeries(ctx, &qry.SeriesQuery)
		if err != nil {
			return nil, err
		}
		ev.daCancellationList = append(ev.daCancellationList, cancel)
		if !sel.OmitNameTag {
			for _, tags := range bags[i].STags {
				tags[labels.MetricName] = sel.MatchingNames[i]
			}
		}
		if qry.histogram.restore {
			bags[i], err = ev.restoreHistogram(bags[i], &qry)
			if err != nil {
				return nil, err
			}
		}
	}
	res := SeriesBag{Time: ev.time}
	res.append(bags...)
	return &res, nil
}

func (ev *evaluator) restoreHistogram(bag *SeriesBag, qry *seriesQueryX) (*SeriesBag, error) {
	buckets, err := bag.histogram()
	if err != nil {
		return nil, err
	}
	if len(buckets) <= 1 {
		return bag, nil
	}
	// aggregate bucket counters
	var h1 map[uint64]int
	h1, err = buckets[0].g.bag.hashWithout("le")
	if err != nil {
		return nil, err
	}
	for x1, x2 := 0, 1; x2 < len(buckets); x1, x2 = x2, x2+1 {
		var h2 map[uint64]int
		h2, err = buckets[x2].g.bag.hashWithout("le")
		if err != nil {
			return nil, err
		}
		for h, y2 := range h2 {
			if y1, ok := h1[h]; ok {
				for t := range ev.time {
					(*buckets[x2].g.bag.Data[y2])[t] += (*buckets[x1].g.bag.Data[y1])[t]
				}
			}
		}
		h1 = h2
	}
	if !qry.histogram.filter {
		return bag, nil
	}
	// filter buckets
	res := SeriesBag{Time: ev.time}
	for i, tags := range bag.Tags {
		if qry.histogram.leID == tags[format.LETagName].ID == qry.histogram.leEQ {
			res.appendX(bag, i)
		}
	}
	return &res, nil
}

func (ev *evaluator) buildSeriesQuery(ctx context.Context, sel *parser.VectorSelector, meta *format.MetricMetaValue) (seriesQueryX, error) {
	// what
	var (
		what       DigestWhat
		factor     = sel.Factor
		accumulate bool
	)
	switch sel.What {
	case Count:
		what = DigestCount
	case CountSec:
		what = DigestCount
		factor = 1
	case CountAcc:
		what = DigestCount
		accumulate = true
	case Min:
		what = DigestMin
	case Max:
		what = DigestMax
	case Sum:
		what = DigestSum
	case SumAcc:
		what = DigestSum
		accumulate = true
	case SumSec:
		what = DigestSum
		factor = 1
	case Avg:
		what = DigestAvg
	case AvgAcc:
		what = DigestAvg
		accumulate = true
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
	case "":
		if meta.Kind == format.MetricKindCounter {
			what = DigestCount
			accumulate = true
		} else {
			what = DigestAvg
		}
	default:
		return seriesQueryX{}, fmt.Errorf("unrecognized %s value %q", labelWhat, sel.What)
	}
	// grouping
	var (
		groupBy    []string
		histogram  = what == DigestCount && meta.Name2Tag[format.LETagName].Raw
		histogramQ histogramQuery
	)
	if sel.GroupWithout {
		skip := make(map[int]bool)
		for _, name := range sel.GroupBy {
			tag, ok := meta.Name2Tag[name]
			if ok {
				skip[tag.Index] = true
			}
		}
		for _, tag := range meta.Tags {
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
			if tag, ok := meta.Name2Tag[name]; ok {
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
		if matcher.Name == labels.MetricName || matcher.Name == labelWhat {
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
				strTop, err := ev.tagV.getSTagValues(ctx, meta, sel.Offset)
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
			i := meta.Name2Tag[matcher.Name].Index
			switch matcher.Type {
			case labels.MatchEqual:
				id, err := ev.tagM.getTagValueID(meta, i, matcher.Value)
				if err != nil {
					return seriesQueryX{}, err
				}
				if histogram && !histogramQ.restore && matcher.Name == format.LETagName {
					histogramQ.filter = true
					histogramQ.leEQ = true
					histogramQ.leID = id
				} else {
					filterIn[i] = map[int32]string{id: matcher.Value}
				}
			case labels.MatchNotEqual:
				id, err := ev.tagM.getTagValueID(meta, i, matcher.Value)
				if err != nil {
					return seriesQueryX{}, err
				}
				if histogram && !histogramQ.restore && matcher.Name == format.LETagName {
					histogramQ.filter = true
					histogramQ.leEQ = false
					histogramQ.leID = id
				} else {
					filterOut[i] = map[int32]string{id: matcher.Value}
				}
			case labels.MatchRegexp:
				fallthrough
			case labels.MatchNotRegexp:
				m, err := ev.tagV.getTagValues(ctx, meta, i, sel.Offset)
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
		groupBy = append(groupBy, format.TagID(meta.Name2Tag[format.LETagName].Index))
		histogramQ.restore = true
	}
	return seriesQueryX{SeriesQuery{
		Meta:       meta,
		What:       what,
		From:       ev.from - sel.Offset,
		Factor:     factor,
		LODs:       ev.lods,
		GroupBy:    groupBy,
		FilterIn:   filterIn,
		FilterOut:  filterOut,
		SFilterIn:  sFilterIn,
		SFilterOut: sFilterOut,
		Accumulate: accumulate,
		MaxHost:    sel.MaxHost,
	}, histogramQ}, nil
}

func (ev *evaluator) stringify(bag *SeriesBag) {
	for i, tags := range bag.Tags {
		for name, value := range tags {
			bag.setSTag(i, name, ev.stag(name, value))
		}
	}
	if len(bag.MaxHost) != 0 {
		maxHost := make([][]string, len(bag.MaxHost))
		for i, row := range bag.MaxHost {
			maxHost[i] = make([]string, len(row))
			for j, id := range row {
				if id != 0 {
					maxHost[i][j] = ev.tagM.getTagValue(id)
				}
			}
		}
		bag.SMaxHost = maxHost
	}
}

func (ev *evaluator) stag(name string, value TagValue) string {
	var raw bool
	if value.Meta != nil {
		raw = value.Meta.Name2Tag[name].Raw
	}
	var res string
	switch {
	case raw:
		if name == format.LETagName {
			res = strconv.FormatFloat(float64(prometheus.LexDecode(value.ID)), 'f', -1, 32)
		} else {
			res = strconv.FormatInt(int64(value.ID), 10)
		}
	case value.ID != 0:
		res = ev.tagM.getTagValue(value.ID)
	}
	return res
}

func (ev *evaluator) alloc() *[]float64 {
	for s, free := range ev.daBuffers {
		if free {
			ev.daBuffers[s] = false
			return s
		}
	}
	s := ev.al.Alloc(len(ev.time))
	ev.alBuffers[s] = true
	return s
}

func (ev *evaluator) free(s *[]float64) {
	if ev.alBuffers[s] {
		ev.al.Free(s)
		delete(ev.alBuffers, s)
	} else {
		ev.daBuffers[s] = true
	}
}

func (ev *evaluator) cancel() {
	for s := range ev.alBuffers {
		ev.al.Free(s)
	}
	ev.alBuffers = nil
	for _, cancel := range ev.daCancellationList {
		cancel()
	}
	ev.daCancellationList = nil
}
