// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package promql

import (
	"context"
	"fmt"
	"hash"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/sortkeys"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/vkcom/statshouse-go"
	"github.com/vkcom/statshouse/internal/chutil"
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/promql/parser"
	"github.com/vkcom/statshouse/internal/vkgo/srvfunc"
	"golang.org/x/sync/errgroup"
	"pgregory.net/rand"
)

const (
	LabelWhat    = "__what__"
	labelBy      = "__by__"
	labelBind    = "__bind__"
	LabelShard   = "__shard__"
	LabelOffset  = "__offset__"
	LabelMinHost = "__minhost__"
	LabelMaxHost = "__maxhost__"

	maxSeriesRows = 10_000_000
)

type Query struct {
	Start int64 // inclusive
	End   int64 // exclusive
	Step  int64
	Expr  string

	Options Options // StatsHouse specific
}

type Options struct {
	Version          string
	Version3Start    int64 // timestamp of schema version 3 start, zero means not set
	Namespace        string
	AvoidCache       bool
	TimeNow          int64
	ScreenWidth      int64
	Mode             data_model.QueryMode
	Extend           bool
	TagWhat          bool
	TagOffset        bool
	TagTotal         bool
	RawBucketLabel   bool
	ExplicitGrouping bool
	MinHost          bool
	MaxHost          bool
	QuerySequential  bool
	Compat           bool // Prometheus compatibilty mode
	Debug            bool // trace verbose
	Offsets          []int64
	GroupBy          []string
	Limit            int
	Play             int
	Rand             *rand.Rand
	Vars             map[string]Variable

	ExprQueriesSingleMetricCallback MetricMetaValueCallback
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
	location  *time.Location
	utcOffset int64
}

type evaluator struct {
	Engine
	Handler
	data_model.QueryStat

	ctx context.Context
	opt Options
	ast parser.Expr
	ars map[parser.Expr]parser.Expr // ast reductions
	t   data_model.Timescale
	r   int64 // matrix selector range
	hh  hash.Hash64

	// metric -> tag index -> offset -> tag value id -> tag value
	tags map[*format.MetricMetaValue][]map[int64]data_model.TagValues
	// metric -> offset -> String TOP
	stop map[*format.MetricMetaValue]map[int64][]string

	// memory management
	allocMap         map[*[]float64]bool
	freeList         []*[]float64
	reuseList        []*[]float64
	cancellationList []func()

	// diagnostics
	timeStart          time.Time
	timeQueryParseEnd  time.Time
	dataAccessDuration time.Duration
}

var nilMetric = [1]*format.MetricMetaValue{{}}

func NewEngine(loc *time.Location, utcOffset int64) Engine {
	return Engine{location: loc, utcOffset: utcOffset}
}

func GetMetricNameMatchers(expr string, res []*labels.Matcher) ([]*labels.Matcher, error) {
	res = res[:0]
	ast, err := parser.ParseExpr(expr)
	if err != nil {
		return res, err
	}
	parser.Inspect(ast, func(node parser.Node, _ []parser.Node) error {
		if sel, ok := node.(*parser.VectorSelector); ok {
			for _, matcher := range sel.LabelMatchers {
				if matcher.Name == labels.MetricName {
					res = append(res, matcher)
				}
			}
		}
		return nil
	})
	return res, nil
}

func (ng Engine) Exec(ctx context.Context, h Handler, qry Query) (parser.Value, func(), error) {
	ev, err := ng.NewEvaluator(ctx, h, qry)
	if err != nil {
		return nil, nil, err
	}
	return ev.Run()
}

func (ev *evaluator) Run() (parser.Value, func(), error) {
	if e, ok := ev.ast.(*parser.StringLiteral); ok {
		return String{T: ev.t.Start, V: e.Val}, func() {}, nil
	}
	if ev.t.Empty() {
		return &TimeSeries{Time: []int64{}}, func() {}, nil
	}
	// evaluate query
	ev.Tracef(ev.ast.String())
	if ev.opt.Debug {
		ev.Tracef("requested from %d to %d, timescale from %d to %d", ev.t.Start, ev.t.End, ev.t.Time[ev.t.StartX], ev.t.Time[len(ev.t.Time)-1])
	}
	var ok bool
	defer func() {
		if !ok {
			ev.cancel()
		}
	}()
	res, err := ev.exec()
	if err != nil {
		return nil, nil, Error{what: err}
	}
	// resolve int32 tag values into strings
	for _, dat := range res.Series.Data {
		for _, tg := range dat.Tags.ID2Tag {
			tg.stringify(ev)
		}
	}
	ev.Tracef("buffers alloc #%d, reuse #%d, %s", len(ev.allocMap)+len(ev.freeList), len(ev.reuseList), res.String())
	ev.reportStat(time.Now())
	ok = true // prevents deffered "cancel"
	return &res, ev.cancel, nil
}

func (ev *evaluator) QueryMetric() *format.MetricMetaValue {
	if len(ev.QueryStat.MetricOffset) == 1 {
		for v := range ev.QueryStat.MetricOffset {
			return v
		}
	}
	return nil
}

func (ng Engine) NewEvaluator(ctx context.Context, h Handler, qry Query) (evaluator, error) {
	timeStart := time.Now()
	if qry.Options.TimeNow == 0 {
		// fix the time "now"
		qry.Options.TimeNow = timeStart.Unix()
	}
	if qry.Options.TimeNow < qry.Start {
		// the future is unknown
		return evaluator{}, nil
	}
	ev := evaluator{
		Engine:    ng,
		Handler:   h,
		ctx:       ctx,
		opt:       qry.Options,
		timeStart: timeStart,
	}
	// parse PromQL expression
	var err error
	ev.ast, err = parser.ParseExpr(qry.Expr)
	if err != nil {
		return evaluator{}, Error{what: err}
	}
	if v, ok := evalLiteral(ev.ast); ok {
		ev.ast = v
	}
	ev.opt.Offsets = normalizeOffsets(append(ev.opt.Offsets, 0))
	// match metrics
	var maxRange int64
	parser.Inspect(ev.ast, func(node parser.Node, path []parser.Node) error {
		switch e := node.(type) {
		case *parser.VectorSelector:
			if e.GroupBy != nil || e.What != "" {
				ev.opt.Compat = false // compat mode is disabled when StateHouse extensions are in use
			}
			if len(e.OriginalOffsetEx) != 0 {
				e.Offsets = normalizeOffsets(e.OriginalOffsetEx)
			} else {
				e.Offsets = []int64{e.OriginalOffset}
			}
			if err = ev.bindVariables(e); err != nil {
				return err
			}
			if err = ev.matchMetrics(e, path); err != nil {
				return err
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
		return evaluator{}, Error{what: err}
	}
	// widen time range to accommodate range selectors and ensure instant query won't return empty result
	qry.Start -= maxRange
	if qry.Options.Mode == data_model.InstantQuery {
		maxRange -= 5 * 60 // 5 times larger than maximum metric resolution (and scrape interval)
	}
	// init timescale
	ev.t, err = data_model.GetTimescale(data_model.GetTimescaleArgs{
		QueryStat:     ev.QueryStat,
		Version:       qry.Options.Version,
		Version3Start: qry.Options.Version3Start,
		Start:         qry.Start,
		End:           qry.End,
		Step:          qry.Step,
		TimeNow:       qry.Options.TimeNow,
		ScreenWidth:   qry.Options.ScreenWidth,
		Mode:          qry.Options.Mode,
		Extend:        qry.Options.Extend,
		Location:      ng.location,
		UTCOffset:     ng.utcOffset,
	})
	if err != nil {
		return evaluator{}, Error{what: err}
	}
	if ev.t.Empty() {
		return ev, nil
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
	if ev.opt.ExprQueriesSingleMetricCallback != nil && len(ev.QueryStat.MetricOffset) == 1 {
		for metric := range ev.QueryStat.MetricOffset {
			ev.opt.ExprQueriesSingleMetricCallback(metric)
			break
		}
	}
	// final touch
	ev.tags = make(map[*format.MetricMetaValue][]map[int64]data_model.TagValues)
	ev.stop = make(map[*format.MetricMetaValue]map[int64][]string)
	ev.timeQueryParseEnd = time.Now()
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
				if ev.opt.Debug {
					ev.Tracef("variable %q not specified", vn)
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

func (ev *evaluator) matchMetrics(sel *parser.VectorSelector, path []parser.Node) error {
	sel.MinHost = ev.opt.MinHost
	sel.MaxHost = ev.opt.MaxHost
	for _, matcher := range sel.LabelMatchers {
		switch matcher.Name {
		case labels.MetricName:
			var err error
			f := data_model.QueryFilter{MetricMatcher: matcher, Namespace: ev.opt.Namespace}
			if err = ev.MatchMetrics(&f); err != nil {
				return err
			}
			if ev.opt.Debug {
				ev.Tracef("found %d metrics for %v", len(f.MatchingMetrics), matcher)
			}
			for _, m := range f.MatchingMetrics {
				var selOffset int64
				for _, v := range sel.Offsets {
					if selOffset < v {
						selOffset = v
					}
				}
				ev.QueryStat.Add(m, selOffset+ev.opt.Offsets[0])
			}
			sel.QueryFilter = f
		case LabelWhat:
			if matcher.Type != labels.MatchEqual {
				return fmt.Errorf("%s supports only strict equality", LabelWhat)
			}
			for _, what := range strings.Split(matcher.Value, ",") {
				switch what {
				case "":
					// ignore empty "what" value
				case MinHost:
					sel.MinHost = true
				case MaxHost:
					sel.MaxHost = true
				default:
					sel.Whats = append(sel.Whats, what)
				}
			}
		case labelBy:
			if matcher.Type != labels.MatchEqual {
				return fmt.Errorf("%s supports only strict equality", labelBy)
			}
			if len(matcher.Value) != 0 {
				sel.GroupBy = append(sel.GroupBy, strings.Split(matcher.Value, ",")...)
			} else if sel.GroupBy == nil {
				sel.GroupBy = make([]string, 0)
			}
		case LabelMinHost:
			sel.MinHost = true
			sel.MinHostMatchers = append(sel.MinHostMatchers, matcher)
		case LabelMaxHost:
			sel.MaxHost = true
			sel.MaxHostMatchers = append(sel.MaxHostMatchers, matcher)
		}
	}
	var aggregate, groupByLE bool
	for i := len(path); i != 0 && !(sel.MinHost && sel.MaxHost && aggregate && groupByLE); i-- {
		switch e := path[i-1].(type) {
		case *parser.Call:
			switch e.Func.Name {
			case "label_minhost":
				sel.MinHost = true
			case "label_maxhost":
				sel.MaxHost = true
			case "histogram_quantile":
				groupByLE = true
			}
		case *parser.AggregateExpr:
			if e.Op == parser.AGGREGATE {
				aggregate = true
			}
		}
	}
	if aggregate {
		sel.QueryFilter.MatchingMetrics = nilMetric[:]
	}
	if groupByLE {
		sel.GroupBy = append(sel.GroupBy, format.LETagName)
	}
	return nil
}

func (ev *evaluator) time() []int64 {
	return ev.t.Time
}

func (ev *evaluator) exec() (TimeSeries, error) {
	srs, err := ev.eval(ev.ast)
	if err != nil {
		return TimeSeries{}, err
	}
	ev.stableRemoveEmptySeries(srs)
	startX := ev.t.StartX
	endX := len(ev.t.Time)
	if ev.opt.Mode == data_model.InstantQuery {
	searchLastPoint:
		for startX = endX; startX > ev.t.StartX; startX-- {
			for j := range srs {
				for k := range srs[j].Data {
					for _, v := range *srs[j].Data[k].Values {
						if math.IsNaN(v) {
							continue searchLastPoint
						}
					}
				}
			}
			break
		}
		if startX > ev.t.StartX {
			startX -= 1
		}
		endX = startX + 1
	}
	res := TimeSeries{Time: ev.t.Time[startX:endX]}
	limit := ev.opt.Limit
	if limit == 0 {
		limit = math.MaxInt
	} else if limit < 0 {
		limit = -limit
	}
	for _, sr := range srs {
		if len(res.Series.Data) == 0 {
			// get resulting meta from first time shift
			res.Series.Meta = sr.Meta
		}
		i := 0
		end := len(sr.Data)
		if limit < len(sr.Data) {
			if ev.opt.Limit < 0 {
				i = len(sr.Data) - limit
			} else {
				end = limit
			}
		}
		for ; i < end; i++ {
			if sr.Data[i].Values != nil {
				// trim time outside [startX:endX]
				vs := (*sr.Data[i].Values)[startX:endX]
				sr.Data[i].Values = &vs
				for j := range sr.Data[i].MinMaxHost {
					if len(sr.Data[i].MinMaxHost[j]) != 0 {
						sr.Data[i].MinMaxHost[j] = sr.Data[i].MinMaxHost[j][startX:endX]
					}
				}
			}
			res.Series.appendSome(sr, i)
		}
	}
	res.Series.pruneMinMaxHost()
	return res, nil
}

func (ev *evaluator) eval(expr parser.Expr) (res []Series, err error) {
	if ev.ctx.Err() != nil {
		return nil, ev.ctx.Err()
	}
	if e, ok := ev.ars[expr]; ok {
		if ev.opt.Debug {
			ev.Tracef("replace %s with %s", string(expr.Type()), string(e.Type()))
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
					res[i].Data[j].What = SelectorWhat{}
				}
				res[i].Meta = evalSeriesMeta(e, SeriesMeta{}, res[i].Meta)
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
					if e.Op == parser.LDEFAULT && res[i].empty() {
						res[i] = ev.newVector(r.Val)
					} else {
						for j := range res[i].Data {
							fn(*res[i].Data[j].Values, r.Val)
							res[i].Data[j].What = SelectorWhat{}
						}
						res[i].Meta = evalSeriesMeta(e, res[i].Meta, SeriesMeta{})
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
			res[i] = ev.newVector(e.Val)
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
					lhs.Data[i].What = SelectorWhat{}
				}
				res[x].appendAll(lhs)
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
						rhs.Data[i].What = SelectorWhat{}
					}
				} else {
					for i := range rhs.Data {
						fn(*rhs.Data[i].Values, *lhs.Data[0].Values, *rhs.Data[i].Values)
						rhs.Data[i].What = SelectorWhat{}
					}
				}
				res[x].appendAll(rhs)
				lhs.Data[0].free(ev)
			} else {
				var lhsM map[uint64]hashMeta
				lhsM, err = lhs.hash(ev, hashOptions{
					on:         expr.VectorMatching.On,
					tags:       expr.VectorMatching.MatchingLabels,
					stags:      rhs.Meta.STags,
					listUnused: true,
				})
				if err != nil {
					return nil, err
				}
				var rhsM map[uint64]hashMeta
				rhsM, err = rhs.hash(ev, hashOptions{
					on:    expr.VectorMatching.On,
					tags:  expr.VectorMatching.MatchingLabels,
					stags: lhs.Meta.STags,
				})
				if err != nil {
					return nil, err
				}
				res[x] = ev.newSeries(len(lhsM), evalSeriesMeta(expr, lhs.Meta, rhs.Meta))
				for lhsH, lhsMt := range lhsM {
					if rhsMt, ok := rhsM[lhsH]; ok {
						fn(*lhs.Data[lhsMt.x].Values, *lhs.Data[lhsMt.x].Values, *rhs.Data[rhsMt.x].Values)
						if lhs.Data[lhsMt.x].What != rhs.Data[rhsMt.x].What {
							lhs.Data[lhsMt.x].What = SelectorWhat{}
						}
						for _, v := range lhsMt.unused {
							lhs.Data[lhsMt.x].Tags.remove(v)
						}
						res[x].appendSome(lhs, lhsMt.x)
					} else {
						lhs.Data[lhsMt.x].free(ev)
					}
				}
				rhs.free(ev)
			}
		case parser.CardManyToOne:
			lhs, rhs = rhs, lhs
			fallthrough
		case parser.CardOneToMany:
			var lhsM map[uint64]hashMeta
			lhsM, err = lhs.hash(ev, hashOptions{
				on:    expr.VectorMatching.On,
				tags:  expr.VectorMatching.MatchingLabels,
				stags: rhs.Meta.STags,
			})
			if err != nil {
				return nil, err
			}
			var rhsM map[uint64][]int
			rhsM, _, err = rhs.group(ev, hashOptions{
				on:    expr.VectorMatching.On,
				tags:  expr.VectorMatching.MatchingLabels,
				stags: lhs.Meta.STags,
			})
			if err != nil {
				return nil, err
			}
			res[x] = ev.newSeries(len(rhsM), evalSeriesMeta(expr, lhs.Meta, rhs.Meta))
			for rhsH, rhsXs := range rhsM {
				if lhsMt, ok := lhsM[rhsH]; ok {
					for _, rhsX := range rhsXs {
						if expr.VectorMatching.Card == parser.CardOneToMany {
							fn(*rhs.Data[rhsX].Values, *lhs.Data[lhsMt.x].Values, *rhs.Data[rhsX].Values)
						} else {
							fn(*rhs.Data[rhsX].Values, *rhs.Data[rhsX].Values, *lhs.Data[lhsMt.x].Values)
						}
						if rhs.Data[rhsX].What != lhs.Data[lhsMt.x].What {
							rhs.Data[rhsX].What = SelectorWhat{}
						}
						for _, v := range expr.VectorMatching.Include {
							if tag, ok := lhs.Data[lhsMt.x].Tags.Get(v); ok {
								rhs.AddTagAt(rhsX, tag)
							}
						}
					}
					res[x].appendSome(rhs, rhsXs...)
				} else {
					ev.freeSome(rhs.Data, rhsXs...)
				}
			}
			lhs.free(ev)
		case parser.CardManyToMany:
			var lhsM map[uint64][]int
			lhsM, _, err = lhs.group(ev, hashOptions{
				on:    expr.VectorMatching.On,
				tags:  expr.VectorMatching.MatchingLabels,
				stags: rhs.Meta.STags,
			})
			if err != nil {
				return nil, err
			}
			switch expr.Op {
			case parser.LAND:
				var rhsM map[uint64][]int
				rhsM, _, err = rhs.group(ev, hashOptions{
					on:    expr.VectorMatching.On,
					tags:  expr.VectorMatching.MatchingLabels,
					stags: lhs.Meta.STags,
				})
				if err != nil {
					return nil, err
				}
				res[x] = ev.newSeries(len(lhsM), lhs.Meta)
				for lhsH, lhsX := range lhsM {
					if rhsX, ok := rhsM[lhsH]; ok {
						for _, rx := range rhsX[1:] {
							sliceOr(*rhs.Data[rhsX[0]].Values, *rhs.Data[rhsX[0]].Values, *rhs.Data[rx].Values)
						}
						for _, lx := range lhsX {
							sliceAnd(*lhs.Data[lx].Values, *lhs.Data[lx].Values, *rhs.Data[rhsX[0]].Values)
						}
						res[x].appendSome(lhs, lhsX...)
					} else {
						ev.freeSome(lhs.Data, lhsX...)
					}
				}
				rhs.free(ev)
			case parser.LDEFAULT:
				if lhs.empty() {
					res[x] = rhs
				} else {
					var rhsM map[uint64]hashMeta
					rhsM, err = rhs.hash(ev, hashOptions{
						on:    expr.VectorMatching.On,
						tags:  expr.VectorMatching.MatchingLabels,
						stags: lhs.Meta.STags,
					})
					if err != nil {
						return nil, err
					}
					res[x] = lhs
					res[x].Meta = evalSeriesMeta(expr, lhs.Meta, rhs.Meta)
					for lhsH, lhsXs := range lhsM {
						if rhsMt, ok := rhsM[lhsH]; ok {
							for _, lhsX := range lhsXs {
								sliceOr(*res[x].Data[lhsX].Values, *lhs.Data[lhsX].Values, *rhs.Data[rhsMt.x].Values)
							}
						}
					}
					rhs.free(ev)
				}
			case parser.LOR:
				if lhs.empty() {
					res[x] = rhs
				} else {
					var rhsM map[uint64][]int
					rhsM, _, err = rhs.group(ev, hashOptions{
						on:    expr.VectorMatching.On,
						tags:  expr.VectorMatching.MatchingLabels,
						stags: lhs.Meta.STags,
					})
					if err != nil {
						return nil, err
					}
					for rhsH, rhsXs := range rhsM {
						if lhsXs, ok := lhsM[rhsH]; ok {
							for _, lhsX := range lhsXs {
								for _, rhsX := range rhsXs {
									if tagsEqual(lhs.Data[lhsX].Tags.ID2Tag, rhs.Data[rhsX].Tags.ID2Tag) {
										sliceOr(*lhs.Data[lhsX].Values, *lhs.Data[lhsX].Values, *rhs.Data[rhsX].Values)
										rhs.Data[rhsX].empty = true // exactly matching series on the left found
									} else {
										sliceUnless(*rhs.Data[rhsX].Values, *rhs.Data[rhsX].Values, *lhs.Data[lhsX].Values)
									}
								}
							}
						}
					}
					res[x] = lhs
					// remove all-nil series on the right
					rhs.removeEmpty(ev)
					// add RHS series which are not found on the left
					if !rhs.empty() {
						res[x].appendAll(rhs)
						res[x].Meta = evalSeriesMeta(expr, lhs.Meta, rhs.Meta)
					}
				}
			case parser.LUNLESS:
				var rhsM map[uint64][]int
				rhsM, _, err = rhs.group(ev, hashOptions{
					on:    expr.VectorMatching.On,
					tags:  expr.VectorMatching.MatchingLabels,
					stags: lhs.Meta.STags,
				})
				if err != nil {
					return nil, err
				}
				res[x] = ev.newSeries(len(lhsM), lhs.Meta)
				for lhsH, lhsXs := range lhsM {
					if rhsXs, ok := rhsM[lhsH]; ok {
						for _, rhsX := range rhsXs[1:] {
							sliceOr(*rhs.Data[rhsXs[0]].Values, *rhs.Data[rhsXs[0]].Values, *rhs.Data[rhsX].Values)
						}
						for _, lhsX := range lhsXs {
							sliceUnless(*lhs.Data[lhsX].Values, *lhs.Data[lhsX].Values, *rhs.Data[rhsXs[0]].Values)
						}
					}
					res[x].appendSome(lhs, lhsXs...)
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

func (ev *evaluator) querySeries(sel *parser.VectorSelector) (srs []Series, err error) {
	if len(sel.MatchingMetrics) == 1 && sel.MatchingMetrics[0].MetricID == 0 && ev.t.Duration() > time.Hour {
		return nil, fmt.Errorf("wildcard query range exceeds maximum allowed 1 hour")
	}
	res := make([]Series, len(ev.opt.Offsets))
	if len(sel.MatchingMetrics) > 1 && ev.opt.Mode == data_model.TagsQuery {
		for i := range res {
			res[i].Data = make([]SeriesData, len(sel.MatchingMetrics))
			for j, m := range sel.MatchingMetrics {
				res[i].AddTagAt(j, &SeriesTag{
					Metric: m,
					ID:     labels.MetricName,
					SValue: m.Name,
				})
			}
		}
		return res, nil
	}
	run := func(j int, mu *sync.Mutex) error {
		var locked bool
		if mu != nil {
			mu.Lock()
			locked = true
			defer func() {
				if locked {
					mu.Unlock()
				}
			}()
		}
		offset := ev.opt.Offsets[j]
		for i, metric := range sel.MatchingMetrics {
			if ev.opt.Debug {
				ev.Tracef("#%d request %s: %s", i, metric.Name, sel.What)
			}
			for _, selOffset := range sel.Offsets {
				qry, err := ev.buildSeriesQuery(ev.ctx, sel, metric, sel.Whats, selOffset+offset)
				if err != nil {
					return err
				}
				if qry.empty() {
					if ev.opt.Debug {
						ev.Tracef("#%d query is empty", i)
					}
					continue
				}
				if mu != nil {
					mu.Unlock()
					locked = false
				}
				sr, cancel, err := ev.QuerySeries(ev.ctx, &qry)
				if err != nil {
					return err
				}
				if mu != nil {
					mu.Lock()
					locked = true
				}
				ev.cancellationList = append(ev.cancellationList, cancel)
				if ev.opt.Debug {
					ev.Tracef("#%d series count %d", i, len(sr.Data))
				}
				for k, s := range [2][]*labels.Matcher{sel.MinHostMatchers, sel.MaxHostMatchers} {
					if len(s) != 0 {
						sr.filterMinMaxHost(ev, k, s)
					}
				}
				for k := range sr.Data {
					if !sel.OmitNameTag {
						sr.AddTagAt(k, &SeriesTag{
							ID:     labels.MetricName,
							SValue: sel.MatchingMetrics[i].Name})
					}
					if len(sel.OriginalOffsetEx) != 0 {
						sr.AddTagAt(k, &SeriesTag{
							ID:    LabelOffset,
							Value: selOffset})
					}
					sr.Data[k].Offset = offset
				}
				if qry.prefixSum {
					sr = ev.funcPrefixSum(sr)
				}
				if len(res[j].Data) == 0 {
					res[j].Meta = sr.Meta
				}
				res[j].appendAll(sr)
			}
		}
		return nil // success
	}
	timeStart := time.Now()
	if ev.opt.QuerySequential || len(res) == 1 {
		for i := 0; i < len(res); i++ {
			err = run(i, nil)
			if err != nil {
				return nil, err
			}
		}
	} else {
		var mu sync.Mutex
		for i := 0; i < len(res); {
			// Limit the number of parallel queries to the number of time shifts
			// available in UI (currently 7) plus always present zero offset.
			var g errgroup.Group
			for n := 8; n != 0 && i < len(res); i, n = i+1, n-1 {
				ii := i
				g.Go(func() error {
					return run(ii, &mu)
				})
			}
			err = g.Wait()
			if err != nil {
				return nil, err
			}
		}
	}
	ev.dataAccessDuration += time.Since(timeStart)
	return res, nil
}

func (ev *evaluator) buildSeriesQuery(ctx context.Context, sel *parser.VectorSelector, metric *format.MetricMetaValue, selWhats []string, offset int64) (SeriesQuery, error) {
	// whats
	var whats []SelectorWhat
	for _, selWhat := range selWhats {
		if selWhat == "" {
			continue
		}
		if what, queryFunc, ok := parseSelectorWhat(selWhat); ok {
			whats = append(whats, SelectorWhat{Digest: what, QueryF: queryFunc})
		} else {
			return SeriesQuery{}, fmt.Errorf("unrecognized %s value %q", LabelWhat, selWhat)
		}
	}
	var prefixSum bool
	if len(whats) == 0 {
		var what DigestWhat
		if metric.Kind == format.MetricKindCounter {
			if ev.opt.Compat {
				what = DigestCountRaw
				prefixSum = true
			} else {
				what = DigestCount
			}
		} else {
			what = DigestAvg
		}
		whats = append(whats, SelectorWhat{Digest: what})
	}
	// grouping
	var (
		groupBy    []int
		addGroupBy = func(t *format.MetricMetaTag) {
			groupBy = append(groupBy, int(t.Index))
		}
	)
	if len(ev.opt.GroupBy) != 0 {
		groupBy = metric.GroupBy(ev.opt.GroupBy)
	} else if sel.GroupByAll {
		if !sel.GroupWithout {
			for i := 0; i < format.MaxTags; i++ {
				groupBy = append(groupBy, i)
			}
			groupBy = append(groupBy, format.StringTopTagIndex)
		}
	} else if sel.GroupWithout {
		skip := make(map[int]bool)
		for _, name := range sel.GroupBy {
			t, _ := metric.APICompatGetTag(name)
			if t != nil {
				skip[int(t.Index)] = true
			}
		}
		for i := 0; i < format.MaxTags; i++ {
			if !skip[i] {
				groupBy = append(groupBy, i)
			}
		}
	} else if len(sel.GroupBy) != 0 {
		groupBy = make([]int, 0, len(sel.GroupBy))
		for _, k := range sel.GroupBy {
			if k == LabelShard {
				groupBy = append(groupBy, format.ShardTagIndex)
			} else if t, _ := metric.APICompatGetTag(k); t != nil {
				if t.Index == format.StringTopTagIndex {
					groupBy = append(groupBy, format.StringTopTagIndexV3)
				} else {
					addGroupBy(t)
				}
			}
		}
	}
	// filtering
	var (
		emptyCount [format.NewMaxTags]int // number of "MatchEqual" or "MatchRegexp" filters which are guaranteed to yield empty response
	)
	for _, matcher := range sel.LabelMatchers {
		if strings.HasPrefix(matcher.Name, "__") {
			continue
		}
		tag, _ := metric.APICompatGetTag(matcher.Name)
		if tag == nil {
			return SeriesQuery{}, fmt.Errorf("not found tag %q", matcher.Name)
		}
		i := int(tag.Index)
		if i == format.StringTopTagIndex {
			i = format.StringTopTagIndexV3
		}
		switch matcher.Type {
		case labels.MatchEqual:
			if v, err := ev.getTagValue(metric, i, matcher.Value); err == nil {
				sel.FilterIn.Append(i, v)
			} else {
				return SeriesQuery{}, fmt.Errorf("failed to map string %q: %v", matcher.Value, err)
			}
		case labels.MatchNotEqual:
			if v, err := ev.getTagValue(metric, i, matcher.Value); err == nil {
				sel.FilterNotIn.Append(i, v)
			} else {
				return SeriesQuery{}, fmt.Errorf("failed to map string %q: %v", matcher.Value, err)
			}
		case labels.MatchRegexp:
			m, err := ev.getTagValues(ctx, metric, i, offset)
			if err != nil {
				return SeriesQuery{}, err
			}
			var matchCount int
			for _, tag := range m {
				if matcher.Matches(tag.Value) {
					sel.FilterIn.Append(i, tag)
					matchCount++
				}
			}
			if matchCount == 0 && ev.opt.Version != data_model.Version3 {
				// there no data satisfying the filter
				emptyCount[i]++
				continue
			}
			sel.FilterIn.Tags[i].Re2 = matcher.Value
		case labels.MatchNotRegexp:
			m, err := ev.getTagValues(ctx, metric, i, offset)
			if err != nil {
				return SeriesQuery{}, err
			}
			for _, tag := range m {
				if !matcher.Matches(tag.Value) {
					sel.FilterNotIn.Append(i, tag)
				}
			}
			sel.FilterNotIn.Tags[i].Re2 = matcher.Value
		}
	}
	for i, n := range emptyCount {
		if n == 0 {
			continue
		}
		if m := len(sel.FilterIn.Tags[i].Values); m == 0 {
			// All "MatchEqual" and "MatchRegexp" filters give an empty result and
			// there are no other such filters, overall result is guaranteed to be empty
			return SeriesQuery{}, nil
		}
	}
	// remove dublicates in "groupBy"
	sort.Ints(groupBy)
	for i := 1; i < len(groupBy); i++ {
		j := i
		for j < len(groupBy) && groupBy[i-1] == groupBy[j] {
			j++
		}
		if i != j {
			groupBy = append(groupBy[:i], groupBy[j:]...)
		}
	}
	return SeriesQuery{
			Metric:      metric,
			Whats:       whats,
			Timescale:   ev.t,
			Offset:      offset,
			Range:       sel.Range,
			GroupBy:     groupBy,
			QueryFilter: sel.QueryFilter,
			MinMaxHost:  [2]bool{sel.MinHost, sel.MaxHost},
			Options:     ev.opt,
			prefixSum:   prefixSum,
		},
		nil
}

func (ev *evaluator) newSeries(capacity int, meta SeriesMeta) Series {
	if capacity == 0 {
		return Series{Meta: meta}
	}
	return Series{
		Data: make([]SeriesData, 0, capacity),
		Meta: meta,
	}
}

func (ev *evaluator) newVector(v float64) Series {
	res := Series{
		Data: []SeriesData{{Values: ev.alloc()}},
	}
	for i := range *res.Data[0].Values {
		(*res.Data[0].Values)[i] = v
	}
	return res
}

func (ev *evaluator) getTagValues(ctx context.Context, metric *format.MetricMetaValue, tagX int, offset int64) (data_model.TagValues, error) {
	m, ok := ev.tags[metric]
	if !ok {
		// tag index -> offset -> tag value ID -> tag value
		m = make([]map[int64]data_model.TagValues, format.MaxTags)
		ev.tags[metric] = m
	}
	m2 := m[tagX]
	if m2 == nil {
		// offset -> tag value ID -> tag value
		m2 = make(map[int64]data_model.TagValues)
		m[tagX] = m2
	}
	var tag format.MetricMetaTag
	if tagX < len(metric.Tags) {
		tag = metric.Tags[tagX]
	}
	var res data_model.TagValues
	if res, ok = m2[offset]; ok {
		return res, nil
	}
	ids, err := ev.QueryTagValueIDs(ctx, TagValuesQuery{
		Metric:    metric,
		Tag:       tag,
		Timescale: ev.t,
		Offset:    offset,
		Options:   ev.opt,
	})
	if err != nil {
		return nil, err
	}
	// tag value ID -> tag value
	res = make(data_model.TagValues, 0, len(ids))
	for _, id := range ids {
		s := ev.GetTagValue(TagValueQuery{
			Version:    ev.opt.Version,
			Metric:     metric,
			TagIndex:   tagX,
			TagValueID: int64(id),
		})
		if s == " 0" {
			s = ""
		}
		res = append(res, data_model.NewTagValue(s, id))
	}
	m2[offset] = res
	return res, nil
}

func (ev *evaluator) getTagValue(metric *format.MetricMetaValue, tagX int, tagV string) (data_model.TagValue, error) {
	if tagV == "" {
		return data_model.NewTagValue("", 0), nil
	}
	if format.HasRawValuePrefix(tagV) {
		v, err := format.ParseCodeTagValue(tagV)
		if err != nil {
			return data_model.TagValue{}, err
		}
		if v != 0 {
			return data_model.NewTagValueM(v), nil
		} else {
			return data_model.NewTagValue("", 0), nil
		}
	}
	var t format.MetricMetaTag
	if 0 <= tagX && tagX < len(metric.Tags) {
		t = metric.Tags[tagX]
		if t.Raw {
			// histogram bucket label
			if t.Name == labels.BucketLabel {
				if v, err := strconv.ParseFloat(tagV, 32); err == nil {
					return data_model.NewTagValueM(int64(statshouse.LexEncode(float32(v)))), nil
				}
			}
			// mapping from raw value comments
			var s string
			for k, v := range t.ValueComments {
				if v == tagV {
					if s != "" {
						return data_model.TagValue{}, fmt.Errorf("ambiguous comment to value mapping")
					}
					s = k
				}
			}
			if s != "" {
				v, err := format.ParseCodeTagValue(s)
				if err != nil {
					return data_model.TagValue{}, err
				}
				return data_model.NewTagValueM(v), nil
			}
		}
	}
	v, err := ev.GetTagValueID(TagValueIDQuery{
		Version:  ev.opt.Version,
		Tag:      t,
		TagValue: tagV,
	})
	switch err {
	case nil:
		return data_model.NewTagValue(tagV, v), nil
	case ErrNotFound:
		return data_model.NewTagValue(tagV, format.TagValueIDDoesNotExist), nil
	default:
		return data_model.TagValue{}, err
	}
}

func (ev *evaluator) weight(ds []SeriesData) []float64 {
	var (
		w      = make([]float64, len(ds))
		nodecN int // number of non-decreasing series
	)
	for i, d := range ds {
		var (
			j     int
			acc   float64
			prev  = -math.MaxFloat64
			nodec = true // non-decreasing
		)
		for _, lod := range ev.t.LODs {
			for m := 0; m < lod.Len; m++ {
				k := j + m
				// do not count invisible
				if k < ev.t.ViewStartX {
					continue
				} else if k >= ev.t.ViewEndX {
					break
				}
				v := (*d.Values)[k]
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
		for i, s := range ds {
			last := -math.MaxFloat64
			for i := ev.t.ViewEndX; i > 0; i-- {
				v := (*s.Values)[i-1]
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

func (ev *evaluator) alloc() *[]float64 {
	var s *[]float64
	if len(ev.reuseList) != 0 {
		ev.reuseList, s = removeLast(ev.reuseList)
	} else if len(ev.freeList) != 0 {
		ev.freeList, s = removeLast(ev.freeList)
	} else {
		if ev.allocMap == nil {
			ev.allocMap = make(map[*[]float64]bool)
		}
		s = ev.Alloc(len(ev.time()))
		ev.allocMap[s] = true
	}
	return s
}

func (ev *evaluator) free(s *[]float64) {
	if ev.allocMap != nil && ev.allocMap[s] {
		delete(ev.allocMap, s)
		ev.freeList = append(ev.freeList, s)
	} else {
		ev.reuseList = append(ev.reuseList, s)
	}
}

func (ev *evaluator) freeAll(ds []SeriesData) {
	for x := range ds {
		ds[x].free(ev)
	}
}

func (ev *evaluator) freeSome(ds []SeriesData, xs ...int) {
	for _, x := range xs {
		ds[x].free(ev)
	}
}

func (ev *evaluator) cancel() {
	if ev.allocMap != nil {
		for s := range ev.allocMap {
			ev.Free(s)
		}
		ev.allocMap = nil
	}
	for _, s := range ev.freeList {
		ev.Free(s)
	}
	ev.freeList = nil
	for _, cancel := range ev.cancellationList {
		cancel()
	}
	ev.reuseList = nil
	ev.cancellationList = nil
}

func (ev *evaluator) newWindow(v []float64, s bool) window {
	return newWindow(ev.time(), v, ev.r, ev.t.LODs[len(ev.t.LODs)-1].Step, s)
}

func (ev *evaluator) reportStat(timeEnd time.Time) {
	tags := statshouse.Tags{
		1: srvfunc.HostnameForStatshouse(),
	}
	r := ev.t.End - ev.t.Start
	x := 2
	switch {
	// add one because UI always requests one second more
	case r <= 1+1:
		tags[x] = "1" // "1_second"
	case r <= 300+1:
		tags[x] = "2" // "5_minutes"
	case r <= 900+1:
		tags[x] = "3" // "15_minutes"
	case r <= 3600+1:
		tags[x] = "4" // "1_hour"
	case r <= 7200+1:
		tags[x] = "5" // "2_hours"
	case r <= 21600+1:
		tags[x] = "6" // "6_hours"
	case r <= 43200+1:
		tags[x] = "7" // "12_hours"
	case r <= 86400+1:
		tags[x] = "8" // "1_day"
	case r <= 172800+1:
		tags[x] = "9" // "2_days"
	case r <= 259200+1:
		tags[x] = "10" // "3_days"
	case r <= 604800+1:
		tags[x] = "11" // "1_week"
	case r <= 1209600+1:
		tags[x] = "12" // "2_weeks"
	case r <= 2592000+1:
		tags[x] = "13" // "1_month"
	case r <= 7776000+1:
		tags[x] = "14" // "3_months"
	case r <= 15552000+1:
		tags[x] = "15" // "6_months"
	case r <= 31536000+1:
		tags[x] = "16" // "1_year"
	case r <= 63072000+1:
		tags[x] = "17" // "2_years"
	default:
		tags[x] = "18"
	}
	n := len(ev.t.Time) - ev.t.StartX // number of points
	x = 3
	switch {
	case n <= 1:
		tags[x] = "1"
	case n <= 1024:
		tags[x] = "2" // "1K"
	case n <= 2048:
		tags[x] = "3" // "2K"
	case n <= 3072:
		tags[x] = "4" // "3K"
	case n <= 4096:
		tags[x] = "5" // "4K"
	case n <= 5120:
		tags[x] = "6" // "5K"
	case n <= 6144:
		tags[x] = "7" // "6K"
	case n <= 7168:
		tags[x] = "8" // "7K"
	case n <= 8192:
		tags[x] = "9" // "8K"
	default:
		tags[x] = "10"
	}
	x = 4
	tags[x] = "1" // "query_parsing"
	value := ev.timeQueryParseEnd.Sub(ev.timeStart).Seconds()
	statshouse.Value(format.BuiltinMetricMetaPromQLEngineTime.Name, tags, value)
	tags[x] = "2" // "data_access"
	value = ev.dataAccessDuration.Seconds()
	statshouse.Value(format.BuiltinMetricMetaPromQLEngineTime.Name, tags, value)
	tags[x] = "3" // "data_processing"
	value = (timeEnd.Sub(ev.timeQueryParseEnd) - ev.dataAccessDuration).Seconds()
	statshouse.Value(format.BuiltinMetricMetaPromQLEngineTime.Name, tags, value)
}

func (qry *SeriesQuery) empty() bool {
	return len(qry.MatchingMetrics) == 0
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

func normalizeOffsets(s []int64) []int64 {
	res := make([]int64, 0, len(s))
	res = append(res, s...)
	// sort descending
	sort.Sort(sort.Reverse(sortkeys.Int64Slice(res)))
	// remove duplicates
	var i int
	for j := 1; j < len(res); j++ {
		if res[i] != res[j] {
			i++
			res[i] = res[j]
		}
	}
	return res[:i+1]
}

func parseSelectorWhat(str string) (DigestWhat, string, bool) {
	var digestWhat, queryFunc string
	if i := strings.Index(str, ":"); i != -1 {
		digestWhat = str[:i]
		queryFunc = str[i+1:]
	} else {
		digestWhat = str
	}
	var res DigestWhat
	switch digestWhat {
	case Count:
		res = DigestCount
	case CountSec:
		res = DigestCountSec
	case CountRaw:
		res = DigestCountRaw
	case Min:
		res = DigestMin
	case Max:
		res = DigestMax
	case Sum:
		res = DigestSum
	case SumSec:
		res = DigestSumSec
	case SumRaw:
		res = DigestSumRaw
	case Avg:
		res = DigestAvg
	case StdDev:
		res = DigestStdDev
	case StdVar:
		res = DigestStdVar
	case P0_1:
		res = DigestP0_1
	case P1:
		res = DigestP1
	case P5:
		res = DigestP5
	case P10:
		res = DigestP10
	case P25:
		res = DigestP25
	case P50:
		res = DigestP50
	case P75:
		res = DigestP75
	case P90:
		res = DigestP90
	case P95:
		res = DigestP95
	case P99:
		res = DigestP99
	case P999:
		res = DigestP999
	case Cardinality:
		res = DigestCardinality
	case CardinalitySec:
		res = DigestCardinalitySec
	case CardinalityRaw:
		res = DigestCardinalityRaw
	case Unique:
		res = DigestUnique
	case UniqueSec:
		res = DigestUniqueSec
	default:
		return DigestUnspecified, "", false
	}
	return res, queryFunc, true
}

func getHostName(h Handler, arg chutil.ArgMinMaxStringFloat32) string {
	if arg.Arg != "" {
		return arg.Arg
	} else if arg.AsInt32 != 0 {
		return h.GetHostName(arg.AsInt32)
	}
	return ""
}
