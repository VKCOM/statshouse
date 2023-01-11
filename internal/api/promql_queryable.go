// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"sort"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	value2 "github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"

	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/promql"
)

type promQueryable struct {
	ng               *promql.Engine
	stmt             *parser.EvalStmt
	h                *Handler
	ai               accessInfo                 // to verify client is allowed to access metrics queried
	ctx              context.Context            // evaluation context
	now              int64                      // frozen time.Now().Unix()
	topNodes         map[parser.Node]*promQuery // a promQuery per VectorSelector
	buffersAllocated []*[]float64               // buffers allocated during request handling
}

type promQuery struct {
	node            parser.Node
	from            int64 // unix time
	lods            []lodSlimInfo
	filterEqual     map[string][]string
	filterNotEqual  map[string][]string
	filterRegexp    []*labels.Matcher
	matchingMetrics []*metricQuery
}

type metricQuery struct {
	info        *promQuery
	meta        *format.MetricMetaValue
	fn          queryFn
	fnKind      queryFnKind
	by          map[string]string // key[0-15] to MetricMetaTag name
	omitNameTag bool
}

func (qe *promQueryable) Querier(ctx context.Context, s *parser.EvalStmt, _, _ int64) (promql.Querier, error) {
	if _, err := qe.calcPromQueries(s.Expr, 0, nil); err != nil {
		return nil, err
	}
	qe.ctx = ctx
	return qe, nil
}

func (qe *promQueryable) Select(node parser.Node, _ *storage.SelectHints, _ ...*labels.Matcher) (parser.Node, []storage.Series) {
	vs, ok := qe.topNodes[node]
	if !ok {
		panic(fmt.Errorf("unexpected AST node %v", node))
	}
	ss := make([]storage.Series, 0)
	for _, q := range vs.matchingMetrics {
		err := qe.evalMetricQuery(qe.ctx, q, &ss)
		if err != nil {
			panic(err)
		}
	}
	return vs.node, ss
}

func (qe *promQueryable) Close() {
	for _, s := range qe.buffersAllocated {
		qe.h.putFloatsSlice(s)
	}
	qe.buffersAllocated = nil
}

func (qe *promQueryable) calcPromQueries(node parser.Node, evalRange time.Duration, path []parser.Node) ([]*promQuery, error) {
	switch e := node.(type) {
	case *parser.BinaryExpr:
		if lt, rt := e.LHS.Type(), e.RHS.Type(); lt == parser.ValueTypeVector && rt == parser.ValueTypeVector {
			resL, err := qe.calcPromQueries(e.LHS, evalRange, path)
			if err != nil {
				return nil, err
			}
			resR, err := qe.calcPromQueries(e.RHS, evalRange, path)
			if err != nil {
				return nil, err
			}
			alignQueryLODs(resL, resR)
			return append(resL, resR...), nil
		}
	case *parser.VectorSelector:
		res, err := qe.newPromQuery(e, evalRange, path)
		if err != nil {
			return nil, err
		}
		qe.topNodes[node] = res
		return []*promQuery{res}, nil
	case *parser.MatrixSelector:
		// StatsHouse stores data aggregated for half open (t-range, t] intervals, Prometheus engine
		// calculates *_over_time functions over closed [t-range,t] time intervals. Subtracting a millisecond so
		// Prometheus will calculate *_over_time functions over half open intervals.
		e.Range -= time.Millisecond
		evalRange = e.Range
	}
	var ret []*promQuery
	path = append(path, node)
	for _, e := range parser.Children(node) {
		res, err := qe.calcPromQueries(e, evalRange, path)
		if err != nil {
			return nil, err
		}
		ret = append(ret, res...)
	}
	return ret, nil
}

func (qe *promQueryable) newPromQuery(expr parser.Expr, evalRange time.Duration, path []parser.Node) (*promQuery, error) {
	promql.UnwrapParenExpr(&expr)
	// process LabelMatchers
	var (
		e          = expr.(*parser.VectorSelector)
		start, end = qe.ng.GetTimeRangesForSelector(qe.stmt, e, path, evalRange)
		pq         = promQuery{
			node:           e,
			from:           start,
			filterEqual:    map[string][]string{},
			filterNotEqual: map[string][]string{},
		}
	)
	for _, m := range e.LabelMatchers {
		if m.Name == labels.MetricName {
			for _, meta := range format.BuiltinMetrics {
				if m.Matches(meta.Name) || m.Matches(meta.Name+"_bucket") {
					pq.matchingMetrics = append(pq.matchingMetrics, &metricQuery{info: &pq, meta: meta, fn: -1})
					break
				}
			}
			for _, meta := range qe.h.metricsStorage.GetMetaMetricList(false) {
				if m.Matches(meta.Name) || m.Matches(meta.Name+"_bucket") {
					pq.matchingMetrics = append(pq.matchingMetrics, &metricQuery{info: &pq, meta: meta, fn: -1})
					break
				}
			}
		} else {
			switch m.Type {
			case labels.MatchEqual:
				pq.filterEqual[m.Name] = append(pq.filterEqual[m.Name], m.Value)
			case labels.MatchNotEqual:
				pq.filterNotEqual[m.Name] = append(pq.filterNotEqual[m.Name], m.Value)
			case labels.MatchRegexp:
				fallthrough
			case labels.MatchNotRegexp:
				pq.filterRegexp = append(pq.filterRegexp, m)
			}
		}
	}
	if len(pq.matchingMetrics) == 0 {
		return &pq, nil
	}
	pq.initGroupingAndQueryFn(path)
	// calculate LODs
	var (
		step       = int(qe.stmt.Interval / time.Second)
		resolution = 1 // a second
	)
	for _, metric := range pq.matchingMetrics {
		if resolution < metric.meta.Resolution {
			resolution = metric.meta.Resolution
		}
	}
	if step < resolution {
		step = resolution
	}
	pq.lods = calcSlimLODs(start, end, int64(step), qe.now)
	return &pq, nil
}

func (qe *promQueryable) evalMetricQuery(ctx context.Context, mq *metricQuery, ss *[]storage.Series) error {
	if mq.meta == nil {
		return nil
	}
	lods, length := calcLODs(mq.info.lods, mq.info.from, int64(mq.meta.PreKeyFrom), qe.h.location)
	by := getGroupingTags(mq.meta, &mq.by)
	// build query
	filterEqual, mappedFilterEqual, err := qe.resolveFilter(mq.meta, mq.info.filterEqual)
	if err != nil {
		return err
	}
	filterNotEqual, mappedFilterNotEqual, err := qe.resolveFilter(mq.meta, mq.info.filterNotEqual)
	if err != nil {
		return err
	}
	qs := normalizedQueryString(mq.meta.Name, mq.fnKind, by, filterEqual, filterNotEqual)
	pq := &preparedPointsQuery{
		user:        qe.ai.user,
		version:     Version2,
		metricID:    mq.meta.MetricID,
		preKeyTagID: mq.meta.PreKeyTagID,
		isStringTop: false,
		kind:        mq.fnKind,
		by:          by,
		filterIn:    mappedFilterEqual,
		filterNotIn: mappedFilterNotEqual,
	}
	// query data
	var (
		allTimes   = make([]int64, 0, length)
		meta       = make([]QuerySeriesMeta, 0)
		data       []*[]float64
		tagsToIx   = map[tsTags]int{}
		ixToLE     = map[int]float32{}
		histograms = map[tsTags][]int{}
		i          int
	)
	for _, lod := range lods {
		m, err := qe.h.cache.Get(ctx, Version2, qs, pq, lod, false)
		if err != nil {
			return err
		}
		for j, s := range m {
			if len(s) == 0 {
				allTimes = append(allTimes, (lod.fromSec+int64(j)*lod.stepSec)*1_000)
				i++
				continue
			}
			for _, ss := range s {
				k, ok := tagsToIx[ss.tsTags]
				if !ok {
					k = len(data)
					tagsToIx[ss.tsTags] = k
					data = append(data, qe.h.getFloatsSlice(length))
					qe.buffersAllocated = append(qe.buffersAllocated, data[k])
					for kk := range *data[k] {
						// Prometheus engine might replace regular NaNs with previous data point,
						// this one below is never replaced
						(*data[k])[kk] = math.Float64frombits(value2.StaleNaN)
					}
					qe.resolveTags(ss.tsTags, mq, &meta, histograms, ixToLE)
				}
				(*data[k])[i] = selectTSValue(mq.fn, lod.stepSec, 1, &ss)
			}
			allTimes = append(allTimes, s[0].time*1_000)
			i++
		}
	}
	// filter tags
	var remove []int
	for i, m := range meta {
		for _, matcher := range mq.info.filterRegexp {
			tagValue, ok := m.Tags[matcher.Name]
			if ok && !matcher.Matches(tagValue) {
				remove = append(remove, i)
			}
		}
	}
	sort.Ints(remove)
	for i := len(remove); i != 0; i-- {
		j := remove[i-1]
		meta[j] = meta[len(meta)-1]
		meta = meta[:len(meta)-1]
		data[j] = data[len(data)-1]
		data = data[:len(data)-1]
	}
	// append "__name__" tag
	if !mq.omitNameTag {
		for _, v := range meta {
			if v.Tags == nil {
				v.Tags = make(map[string]string, 1)
			}
			v.Tags[labels.MetricName] = mq.meta.Name
		}
	}
	// restore (accumulate) counters
	if mq.fn == queryFnCumulCount {
		for _, v := range data {
			acc := 0.
			for i, vv := range *v {
				if !math.IsNaN(vv) { // don't override NaNs
					acc += vv
					(*v)[i] = acc
				}
			}
		}
	}
	// restore (accumulate) "le" tags
	for _, ix := range histograms {
		sort.Slice(ix, func(i, j int) bool { return ixToLE[i] < ixToLE[j] })
		for i := 1; i < len(ix); i++ {
			for j := 0; j < len(allTimes); j++ {
				(*data[ix[i]])[j] += (*data[ix[i-1]])[j]
			}
		}
	}
	// generate resulting series
	for i, v := range meta {
		*ss = append(*ss, &promSeries{time: allTimes, data: *data[i], tags: v.Tags})
	}
	return nil
}

func (qe *promQueryable) resolveFilter(meta *format.MetricMetaValue, filter map[string][]string) (map[string][]string, map[string][]interface{}, error) {
	var (
		m1 = map[string][]string{}
		m2 = map[string][]interface{}{}
	)
	for key, values := range filter {
		tag, ok := meta.Name2Tag[key]
		if !ok {
			return nil, nil, fmt.Errorf("tag with name %s not found for metric %s", key, meta.Name)
		}
		var (
			s1 = make([]string, 0, len(values))
			s2 = make([]interface{}, 0, len(values))
		)
		for _, v := range values {
			id, err := qe.h.getRichTagValueID(&tag, Version2, v)
			if err != nil {
				if httpCode(err) == http.StatusNotFound {
					continue // ignore values with no mapping
				}
				return nil, nil, err
			}
			s1 = append(s1, v)
			s2 = append(s2, id)
		}
		tagKey := format.TagID(tag.Index)
		m1[tagKey] = s1
		m2[tagKey] = s2
	}
	return m1, m2, nil
}

func (pq *promQuery) initGroupingAndQueryFn(path []parser.Node) {
	funcReplace := map[*parser.Call]*parser.Function{}
outerFor:
	for i := len(path); i != 0; i-- {
		for _, metric := range pq.matchingMetrics {
			node := path[i-1]
			switch e := node.(type) {
			case *parser.AggregateExpr:
				if metric.by != nil || metric.fn != -1 {
					break outerFor
				}
				switch e.Op {
				case parser.AVG:
					metric.fn = queryFnAvg
				case parser.COUNT:
					metric.fn = queryFnCount
				case parser.MAX:
					metric.fn = queryFnMax
				case parser.MIN:
					metric.fn = queryFnMin
				case parser.SUM:
					metric.fn = queryFnSum
				}
				if metric.fn != -1 {
					metric.initGrouping(e)
					pq.node = node
				}
			case *parser.Call:
				if metric.fn != -1 {
					break outerFor
				}
				switch e.Func.Name {
				case "avg_over_time":
					metric.fn = queryFnAvg
				case "min_over_time":
					metric.fn = queryFnMin
				case "max_over_time":
					metric.fn = queryFnMax
				case "sum_over_time":
					metric.fn = queryFnAvg
				case "count_over_time":
					// Ask Prometheus engine to calculate count by summing counters provided
					funcReplace[e] = parser.Functions["sum_over_time"]
					metric.fn = queryFnCount
				case "histogram_quantile":
					// Prometheus engine will get value from "le" tag
					metric.fn = queryFnCount
				}
				if metric.fn != -1 {
					metric.omitNameTag = true
				}
			}
		}
	}
	for call, f := range funcReplace {
		call.Func = f
	}
	for _, metric := range pq.matchingMetrics {
		if metric.fn < 0 { // wasn't set
			if metric.meta.Kind == format.MetricKindCounter {
				metric.fn = queryFnCumulCount
			} else {
				metric.fn = queryFnAvg
			}
		}
		metric.fnKind = queryFnToQueryFnKind(metric.fn)
	}
}

func (mq *metricQuery) initGrouping(expr *parser.AggregateExpr) {
	mq.by = map[string]string{}
	mq.omitNameTag = true
	if expr.Without {
		for _, tag := range mq.meta.Tags {
			if len(tag.Name) == 0 || (tag.Index == 0 && tag.Name == "env") {
				continue
			}
			key := format.TagID(tag.Index)
			found := false
			for _, name := range expr.Grouping {
				if name == key || name == tag.Name {
					found = true
					break
				}
			}
			if !found {
				mq.by[key] = tag.Name
			}
		}
	} else {
		for _, name := range expr.Grouping {
			if name == labels.MetricName {
				mq.omitNameTag = false
				continue
			}
			for _, tag := range mq.meta.Tags {
				key := format.TagID(tag.Index)
				if name == key || name == tag.Name {
					mq.by[key] = tag.Name
					break
				}
			}
		}
	}
}
