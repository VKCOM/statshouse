// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package promql

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/promql/parser"
)

const (
	labelWhat = "__what"
)

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

	qry Query
	ast parser.Expr
	now int64

	lods []LOD
	from int64
	time []int64

	tagM tagMap    // tag value id <-> tag value
	tagV tagValues // tag (id,value)s for interval

	alBuffers          map[*[]float64]bool // buffers allocated with Allocator
	daBuffers          map[*[]float64]bool // reused DataAccess buffers
	daCancellationList []func()
}

type tagMap struct {
	da        DataAccess
	valueToID map[string]int32
	idToValue map[int32]string
}

type tagValues struct {
	da       DataAccess
	from, to int64
	tagM     tagMap
	values   map[*format.MetricMetaValue][]map[int64]map[int32]string // metric -> tagIx -> offset -> id -> value
	strTop   map[*format.MetricMetaValue]map[int64][]string           // metric -> offset -> values
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
		if l, ok := ev.evalLiteral(ev.ast); ok {
			ev.ast = l
		}
		var bag SeriesBag
		bag, err = ev.eval(ctx, ev.ast)
		if err != nil {
			ev.cancel()
			return nil, nil, err
		}
		bag = ev.convertTagsToSTags(bag)
		bag.Start = qry.Start
		return bag, ev.cancel, nil
	}
}

func (ng Engine) newEvaluator(ctx context.Context, qry Query) (ev evaluator, err error) {
	ev = evaluator{
		al:        ng.al,
		da:        ng.da,
		loc:       ng.loc,
		now:       time.Now().Unix(),
		alBuffers: make(map[*[]float64]bool),
		daBuffers: make(map[*[]float64]bool),
	}
	ev.ast, err = parser.ParseExpr(qry.Expr)
	if err != nil {
		return
	}
	// match metrics
	maxOffset := make(map[*format.MetricMetaValue]int64)
	var maxRange int64
	parser.Inspect(ev.ast, func(node parser.Node, nodes []parser.Node) error {
		switch e := node.(type) {
		case *parser.VectorSelector:
			err = ng.matchMetrics(ctx, e, nodes, maxOffset)
		case *parser.MatrixSelector:
			if maxRange < e.Range {
				maxRange = e.Range
			}
		}
		return err
	})
	if err != nil {
		return
	}
	// lods, from and time
	qry.Start -= maxRange // widen time range to accommodate range selectors
	if qry.Step <= 0 {    // instant query case
		qry.Step = 1
	}
	ev.lods, err = ng.da.GetQueryLODs(qry, maxOffset, ev.now)
	if err != nil {
		return
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
	// align selectors offsets
	parser.Inspect(ev.ast, func(node parser.Node, nodes []parser.Node) error {
		switch s := node.(type) {
		case *parser.VectorSelector:
			s.Offset = s.OriginalOffset / step * step
		case *parser.SubqueryExpr:
			s.Offset = s.OriginalOffset / step * step
		}
		return err
	})
	if err != nil {
		return
	}
	// the rest
	ev.tagM = ng.newTagMap()
	ev.tagV = ng.newTagValues(from, to, ev.tagM)
	ev.qry = qry
	return
}

func (ng Engine) matchMetrics(ctx context.Context, sel *parser.VectorSelector, path []parser.Node, maxOffset map[*format.MetricMetaValue]int64) error {
	var (
		nameFound = false
		what      = Unspecified
	)
	for _, matcher := range sel.LabelMatchers {
		if nameFound && what != Unspecified {
			break
		}
		if matcher.Name == labels.MetricName {
			nameFound = true
			matchingMetrics, err := ng.da.MatchMetrics(ctx, matcher)
			if err != nil {
				return err
			}
			if len(matchingMetrics) == 0 {
				return nil // metric does not exist, not an error
			}
			for _, meta := range matchingMetrics {
				offset, ok := maxOffset[meta]
				if !ok || offset < sel.OriginalOffset {
					maxOffset[meta] = sel.OriginalOffset
				}
				sel.MatchingMetrics = append(sel.MatchingMetrics, meta)
			}
		} else if matcher.Name == labelWhat {
			if matcher.Type != labels.MatchEqual {
				return fmt.Errorf("%s supports only strict equality", labelWhat)
			}
			switch matcher.Value {
			case "cnt":
				what = Count
			case "min":
				what = Min
			case "max":
				what = Max
			case "sum":
				what = Sum
			case "avg":
				what = Avg
			default:
				return fmt.Errorf("unrecognized %s value %q", labelWhat, matcher.Value)
			}
		}
	}
	var replacement bool
	for i := len(path); i != 0; i-- {
		if _, ok := path[i-1].(*parser.ParenExpr); ok {
			continue
		}
		if expr, ok := path[i-1].(*parser.AggregateExpr); ok {
			aggregateWhat := Unspecified
			switch expr.Op {
			case parser.AVG:
				aggregateWhat = Avg
			case parser.COUNT:
				aggregateWhat = Count
			case parser.MAX:
				aggregateWhat = Max
			case parser.MIN:
				aggregateWhat = Min
			case parser.SUM:
				aggregateWhat = Sum
			}
			if aggregateWhat != Unspecified && (what == Unspecified || what == aggregateWhat) {
				what = aggregateWhat
				sel.GroupBy = expr.Grouping
				sel.GroupWithout = expr.Without
				expr.ReplacementExpr = sel
				replacement = true
			}
		}
		break
	}
	if len(sel.GroupBy) == 0 && !replacement { // then group by all
		for i := 0; i < format.MaxTags; i++ {
			sel.GroupBy = append(sel.GroupBy, format.TagID(i))
		}
	}
	sel.What = int(what)
	return nil
}

func (ng Engine) newTagMap() tagMap {
	return tagMap{da: ng.da, valueToID: make(map[string]int32), idToValue: make(map[int32]string)}
}

func (ng Engine) newTagValues(from int64, to int64, tagM tagMap) tagValues {
	return tagValues{
		da:     ng.da,
		from:   from,
		to:     to,
		tagM:   tagM,
		values: make(map[*format.MetricMetaValue][]map[int64]map[int32]string),
		strTop: make(map[*format.MetricMetaValue]map[int64][]string),
	}
}

func (ev *evaluator) eval(ctx context.Context, expr parser.Expr) (res SeriesBag, err error) {
	switch e := expr.(type) {
	case *parser.AggregateExpr:
		if e.ReplacementExpr != nil {
			res, err = ev.eval(ctx, e.ReplacementExpr)
		} else {
			res, err = ev.evalAggregate(ctx, e)
		}
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
		res = SeriesBag{
			Time:  ev.time,
			Data:  []*[]float64{row},
			Tags:  make([]map[string]int32, 1),
			STags: make([]map[string]string, 1)}
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
	return
}

func (ev *evaluator) evalLiteral(expr parser.Expr) (*parser.NumberLiteral, bool) {
	switch e := expr.(type) {
	case *parser.BinaryExpr:
		l, okL := ev.evalLiteral(e.LHS)
		r, okR := ev.evalLiteral(e.RHS)
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
		return ev.evalLiteral(e.Expr)
	case *parser.UnaryExpr:
		if l, ok := ev.evalLiteral(e.Expr); ok {
			if e.Op == parser.SUB {
				l.Val = -l.Val
			}
			return l, true
		}
	default:
		for _, node := range parser.Children(expr) {
			if e2, ok := node.(parser.Expr); ok {
				ev.evalLiteral(e2)
			}
		}
	}
	return nil, false
}

func (ev *evaluator) evalAggregate(ctx context.Context, expr *parser.AggregateExpr) (res SeriesBag, err error) {
	res, err = ev.eval(ctx, expr.Expr)
	if err != nil || len(res.Data) == 0 {
		return
	}
	var fn aggregateFunc
	if fn = aggregates[expr.Op]; fn == nil {
		err = fmt.Errorf("not implemented aggregate %q", expr.Op)
		return
	}
	var groups []seriesGroup
	groups, err = res.group(expr.Grouping, expr.Without)
	if err != nil {
		return
	}
	var seriesCount int
	bags := make([]SeriesBag, len(groups))
	for i, g := range groups {
		bags[i] = fn(ev, g, expr.Param)
		seriesCount += len(bags[i].Data)
	}
	res = SeriesBag{
		Time:  ev.time,
		Data:  make([]*[]float64, 0, seriesCount),
		Tags:  make([]map[string]int32, 0, seriesCount),
		STags: make([]map[string]string, 0, seriesCount),
	}
	for _, b := range bags {
		res.Data = append(res.Data, b.Data...)
		res.Tags = append(res.Tags, b.Tags...)
		res.STags = append(res.STags, b.STags...)
	}
	return
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
			return
		}
	}
	res.Time = ev.time
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
			mappingL, err = l.hashTags(expr.VectorMatching.MatchingLabels, !expr.VectorMatching.On)
			if err != nil {
				return
			}
			var mappingR map[uint64]int
			mappingR, err = r.hashTags(expr.VectorMatching.MatchingLabels, !expr.VectorMatching.On)
			if err != nil {
				return
			}
			for h, xl := range mappingL {
				if xr, ok := mappingR[h]; ok {
					fn(*l.Data[xl], *l.Data[xl], *r.Data[xr])
					res.Data = append(res.Data, l.Data[xl])
					res.Tags = append(res.Tags, l.Tags[xl])
					res.STags = append(res.STags, l.STags[xl])
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
		groups, err = bags[0].group(expr.VectorMatching.MatchingLabels, !expr.VectorMatching.On)
		if err != nil {
			return
		}
		one := bags[1]
		var oneM map[uint64]int
		oneM, err = one.hashTags(expr.VectorMatching.MatchingLabels, !expr.VectorMatching.On)
		if err != nil {
			return
		}
		for _, many := range groups {
			if oneX, ok := oneM[many.hash]; ok {
				for manyX, manyRow := range many.bag.Data {
					fn(*manyRow, *manyRow, *one.Data[oneX])
					res.Data = append(res.Data, manyRow)
					for _, tag := range expr.VectorMatching.Include {
						var tagValueID int32
						var tagValue string
						if tagValueID, ok = one.Tags[oneX][tag]; ok {
							many.bag.Tags[manyX][tag] = tagValueID
						} else if tagValue, ok = one.STags[oneX][tag]; ok {
							many.bag.STags[manyX][tag] = tagValue
						}
					}
					res.Tags = append(res.Tags, many.bag.Tags[manyX])
					res.STags = append(res.STags, many.bag.STags[manyX])
				}
			}
		}
	case parser.CardOneToMany:
		one := bags[0]
		var oneM map[uint64]int
		oneM, err = one.hashTags(expr.VectorMatching.MatchingLabels, !expr.VectorMatching.On)
		if err != nil {
			return
		}
		var groups []seriesGroup
		groups, err = bags[1].group(expr.VectorMatching.MatchingLabels, !expr.VectorMatching.On)
		if err != nil {
			return
		}
		for _, many := range groups {
			if oneX, ok := oneM[many.hash]; ok {
				for manyX, manyRow := range many.bag.Data {
					fn(*manyRow, *manyRow, *one.Data[oneX])
					res.Data = append(res.Data, manyRow)
					for _, tag := range expr.VectorMatching.Include {
						var tagValueID int32
						var tagValue string
						if tagValueID, ok = one.Tags[oneX][tag]; ok {
							many.bag.Tags[manyX][tag] = tagValueID
						} else if tagValue, ok = one.STags[oneX][tag]; ok {
							many.bag.STags[manyX][tag] = tagValue
						}
					}
					res.Tags = append(res.Tags, many.bag.Tags[manyX])
					res.STags = append(res.STags, many.bag.STags[manyX])
				}
			}
		}
	case parser.CardManyToMany:
		l := bags[0]
		var mappingL map[uint64]int
		mappingL, err = l.hashTags(expr.VectorMatching.MatchingLabels, !expr.VectorMatching.On)
		if err != nil {
			return
		}
		r := bags[1]
		var mappingR map[uint64]int
		mappingR, err = r.hashTags(expr.VectorMatching.MatchingLabels, !expr.VectorMatching.On)
		if err != nil {
			return
		}
		switch expr.Op {
		case parser.LAND:
			for h, xl := range mappingL {
				if _, ok := mappingR[h]; ok {
					res.Data = append(res.Data, l.Data[xl])
					res.Tags = append(res.Tags, l.Tags[xl])
					res.STags = append(res.STags, l.STags[xl])
				}
			}
		case parser.LOR:
			res = l
			for h, xr := range mappingR {
				if _, ok := mappingL[h]; !ok {
					res.Data = append(res.Data, r.Data[xr])
					res.Tags = append(res.Tags, r.Tags[xr])
					res.STags = append(res.STags, r.STags[xr])
				}
			}
		case parser.LUNLESS:
			for h, xl := range mappingL {
				if _, ok := mappingR[h]; !ok {
					res.Data = append(res.Data, l.Data[xl])
					res.Tags = append(res.Tags, l.Tags[xl])
					res.STags = append(res.STags, l.STags[xl])
				}
			}
		default:
			err = fmt.Errorf("not implemented binary operator %q", expr.Op)
		}
	}
	return
}

func (ev *evaluator) querySeries(ctx context.Context, sel *parser.VectorSelector) (res SeriesBag, err error) {
	var seriesCount = 0
	bags := make([]SeriesBag, len(sel.MatchingMetrics))
	for i, meta := range sel.MatchingMetrics {
		var qry SeriesQuery
		qry, err = ev.buildSeriesQuery(ctx, sel, meta)
		if err != nil {
			return
		}
		var cancel func()
		bags[i], cancel, err = ev.da.QuerySeries(ctx, &qry)
		if err != nil {
			return
		}
		ev.daCancellationList = append(ev.daCancellationList, cancel)
		seriesCount += len(bags[i].Data)
	}
	res = SeriesBag{
		Time:  ev.time,
		Data:  make([]*[]float64, 0, seriesCount),
		Tags:  make([]map[string]int32, 0, seriesCount),
		STags: make([]map[string]string, 0, seriesCount),
	}
	for _, b := range bags {
		res.Data = append(res.Data, b.Data...)
		res.Tags = append(res.Tags, b.Tags...)
		res.STags = append(res.STags, b.STags...)
	}
	return
}

func (ev *evaluator) buildSeriesQuery(ctx context.Context, sel *parser.VectorSelector, meta *format.MetricMetaValue) (SeriesQuery, error) {
	// what
	what := What(sel.What)
	if what == Unspecified {
		if meta.Kind == format.MetricKindCounter {
			what = AggregateCount
		} else {
			what = Avg
		}
	}
	// grouping
	var groupBy []string
	if sel.GroupWithout {
		skip := make(map[int]bool)
		for _, name := range sel.GroupBy {
			tag, ok := meta.Name2Tag[name]
			if ok {
				skip[tag.Index] = true
			}
		}
		for i := 0; i < format.MaxTags; i++ {
			if !skip[i] {
				groupBy = append(groupBy, format.TagID(i))
			}
		}
	} else {
		groupBy = make([]string, 0, len(sel.GroupBy))
		for _, name := range sel.GroupBy {
			if tag, ok := meta.Name2Tag[name]; ok {
				groupBy = append(groupBy, format.TagID(tag.Index))
			}
		}
	}
	// filtering
	var (
		filterIn   [format.MaxTags]map[int32]string // tagIx -> tagValueID -> tagValue
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
				strTop, err := ev.tagV.getStrTop(ctx, meta, sel.Offset)
				if err != nil {
					return SeriesQuery{}, err
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
				id, err := ev.tagM.getTagValueID(matcher.Value)
				if err != nil {
					return SeriesQuery{}, err
				}
				filterIn[i] = map[int32]string{id: matcher.Value}
			case labels.MatchNotEqual:
				id, err := ev.tagM.getTagValueID(matcher.Value)
				if err != nil {
					return SeriesQuery{}, err
				}
				filterOut[i] = map[int32]string{id: matcher.Value}
			case labels.MatchRegexp:
				fallthrough
			case labels.MatchNotRegexp:
				m, err := ev.tagV.getTagValues(ctx, meta, i, sel.Offset)
				if err != nil {
					return SeriesQuery{}, err
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
	return SeriesQuery{
		Meta:       meta,
		What:       what,
		From:       ev.from - sel.Offset,
		LODs:       ev.lods,
		GroupBy:    groupBy,
		FilterIn:   filterIn,
		FilterOut:  filterOut,
		SFilterIn:  sFilterIn,
		SFilterOut: sFilterOut,
	}, nil
}

func (ev *evaluator) convertTagsToSTags(bag SeriesBag) SeriesBag {
	for i, tags := range bag.Tags {
		stags := bag.STags[i]
		for name, id := range tags {
			if id != 0 {
				stags[name] = ev.tagM.getTagValue(id)
			}
		}
	}
	return SeriesBag{Time: bag.Time, Data: bag.Data, STags: bag.STags}
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

func (tagM tagMap) getTagValue(id int32) string {
	v, ok := tagM.idToValue[id]
	if !ok {
		v = tagM.da.GetTagValue(id)
		tagM.idToValue[id] = v
		tagM.valueToID[v] = id
	}
	return v
}

func (tagM tagMap) getTagValueID(val string) (id int32, err error) {
	var ok bool
	id, ok = tagM.valueToID[val]
	if !ok {
		id, err = tagM.da.GetTagValueID(val)
		if err != nil {
			return
		}
		tagM.idToValue[id] = val
		tagM.valueToID[val] = id
	}
	return id, nil
}

func (tagV tagValues) getTagValues(ctx context.Context, meta *format.MetricMetaValue, tagIx int, offset int64) (map[int32]string, error) {
	tov, ok := tagV.values[meta] // tag -> offset -> valueID -> value
	if !ok {
		tov = make([]map[int64]map[int32]string, format.MaxTags)
		tagV.values[meta] = tov
	}
	ov := tov[tagIx] // offset -> valueID -> value
	if ov == nil {
		ov = make(map[int64]map[int32]string)
		tov[tagIx] = ov
	}
	var res map[int32]string
	if res, ok = ov[offset]; ok {
		return res, nil
	}
	ids, err := tagV.da.QueryTagValues(ctx, meta, tagIx, tagV.from, tagV.to)
	if err != nil {
		return nil, err
	}
	res = make(map[int32]string, len(ids))
	for _, id := range ids {
		res[id] = tagV.tagM.getTagValue(id)
	}
	ov[offset] = res
	return res, nil
}

func (tagV tagValues) getStrTop(ctx context.Context, meta *format.MetricMetaValue, offset int64) ([]string, error) {
	ov, ok := tagV.strTop[meta] // offset -> values
	if !ok {
		ov = make(map[int64][]string)
		tagV.strTop[meta] = ov
	}
	var res []string
	if res, ok = ov[offset]; ok {
		return res, nil
	}
	var err error
	res, err = tagV.da.QuerySTagValues(ctx, meta, tagV.from, tagV.to)
	if err == nil {
		ov[offset] = res
	}
	return res, err
}
