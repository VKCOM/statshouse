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
	h                *Handler
	ai               accessInfo   // to verify client is allowed to access metrics queried
	now              int64        // frozen time.Now().Unix()
	buffersAllocated []*[]float64 // buffers allocated during request handling
}

type promVectorSelector struct {
	node            *parser.VectorSelector
	topNode         parser.Node
	from            int64 // unix time
	step            time.Duration
	lods            []lodSlimInfo
	filterEqual     map[string][]string
	filterNotEqual  map[string][]string
	filterRegexp    []*labels.Matcher
	matchingMetrics []*metricQuery
}

type metricQuery struct {
	selector    *promVectorSelector
	meta        *format.MetricMetaValue
	what        queryFn
	whatKind    queryFnKind
	by          map[string]string // key[0-15] to MetricMetaTag name
	omitNameTag bool
}

func (qe *promQueryable) Query(ctx context.Context, s *parser.EvalStmt) (map[parser.Node]*parser.VectorSelector, error) {
	vss, err := qe.matchMetrics(s, s.Expr, 0, nil)
	if err != nil {
		return nil, err
	}
	terminals := make(map[parser.Node]*parser.VectorSelector, len(vss))
	for _, vs := range vss {
		vs.node.Series = make([]storage.Series, 0)
		for _, mq := range vs.matchingMetrics {
			err := qe.evalQuery(ctx, mq)
			if err != nil {
				return nil, err
			}
		}
		terminals[vs.topNode] = vs.node
	}
	// StatsHouse stores data aggregated for half open (from, to] intervals, Prometheus engine
	// calculates *_over_time functions over closed [from, to] time intervals. Subtracting a millisecond so
	// Prometheus will calculate *_over_time functions over half open intervals too.
	parser.Inspect(s, func(node parser.Node, _ []parser.Node) error {
		if e, ok := node.(*parser.MatrixSelector); ok {
			e.Range -= 1
		}
		return nil
	})
	return terminals, nil
}

func (qe *promQueryable) Close() {
	for _, s := range qe.buffersAllocated {
		qe.h.putFloatsSlice(s)
	}
	qe.buffersAllocated = nil
}

func (qe *promQueryable) matchMetrics(s *parser.EvalStmt, node parser.Node, evalRange time.Duration, path []parser.Node) ([]*promVectorSelector, error) {
	switch e := node.(type) {
	case *parser.BinaryExpr:
		if lt, rt := e.LHS.Type(), e.RHS.Type(); lt == parser.ValueTypeVector && rt == parser.ValueTypeVector {
			resL, err := qe.matchMetrics(s, e.LHS, evalRange, path)
			if err != nil {
				return nil, err
			}
			resR, err := qe.matchMetrics(s, e.RHS, evalRange, path)
			if err != nil {
				return nil, err
			}
			alignQueryLODs(resL, resR)
			return append(resL, resR...), nil
		}
	case *parser.VectorSelector:
		res, err := qe.newVectorSelector(s, e, evalRange, path)
		if err != nil {
			return nil, err
		}
		return []*promVectorSelector{res}, nil
	case *parser.MatrixSelector:
		evalRange = e.Range
	}
	var ret []*promVectorSelector
	path = append(path, node)
	for _, e := range parser.Children(node) {
		res, err := qe.matchMetrics(s, e, evalRange, path)
		if err != nil {
			return nil, err
		}
		ret = append(ret, res...)
	}
	return ret, nil
}

func (qe *promQueryable) newVectorSelector(s *parser.EvalStmt, expr parser.Expr, evalRange time.Duration, path []parser.Node) (*promVectorSelector, error) {
	// match metrics
	promql.UnwrapParenExpr(&expr)
	var (
		e          = expr.(*parser.VectorSelector)
		start, end = qe.h.promEngine.GetTimeRangesForSelector(s, e, path, evalRange)
		vs         = promVectorSelector{
			node:           e,
			topNode:        e,
			from:           start,
			filterEqual:    map[string][]string{},
			filterNotEqual: map[string][]string{},
		}
		fn = queryFn(-1)
	)
	for _, m := range e.LabelMatchers {
		if m.Name == labels.MetricName {
			for _, meta := range format.BuiltinMetrics {
				if m.Matches(meta.Name) || m.Matches(meta.Name+"_bucket") {
					vs.matchingMetrics = append(vs.matchingMetrics, &metricQuery{selector: &vs, meta: meta, what: -1})
					break
				}
			}
			for _, meta := range qe.h.metricsStorage.GetMetaMetricList(qe.h.showInvisible) {
				if m.Matches(meta.Name) || m.Matches(meta.Name+"_bucket") {
					vs.matchingMetrics = append(vs.matchingMetrics, &metricQuery{selector: &vs, meta: meta, what: -1})
					break
				}
			}
		} else if m.Name == "__fn" {
			if m.Type != labels.MatchEqual {
				return nil, fmt.Errorf("__fn supports strict equality operator only")
			}
			var ok bool
			if fn, ok = validQueryFn(m.Value); !ok {
				return nil, fmt.Errorf("bad __fn %q", m.Value)
			}
		} else {
			switch m.Type {
			case labels.MatchEqual:
				vs.filterEqual[m.Name] = append(vs.filterEqual[m.Name], m.Value)
			case labels.MatchNotEqual:
				vs.filterNotEqual[m.Name] = append(vs.filterNotEqual[m.Name], m.Value)
			case labels.MatchRegexp:
				fallthrough
			case labels.MatchNotRegexp:
				vs.filterRegexp = append(vs.filterRegexp, m)
			}
		}
	}
	if len(vs.matchingMetrics) == 0 {
		return &vs, nil
	}
	// calculate LODs
	vs.lods = calcSlimLODs(start, end, int64(vs.step), qe.now)
	resolution := time.Second // minimum a second
	for _, metric := range vs.matchingMetrics {
		v := time.Duration(metric.meta.Resolution) * time.Second
		if resolution < v {
			resolution = v
		}
	}
	vs.step = s.Interval
	if vs.step < resolution {
		vs.step = resolution
	}
	// initialize what and grouping
	vs.initWhat(path, fn)
	vs.initGrouping(path)
	return &vs, nil
}

func (qe *promQueryable) evalQuery(ctx context.Context, mq *metricQuery) error {
	if mq.meta == nil {
		return nil
	}
	lods, length := calcLODs(mq.selector.lods, mq.selector.from, int64(mq.meta.PreKeyFrom), qe.h.location)
	by := getGroupingTags(mq.meta, &mq.by)
	// build query
	filterEqual, mappedFilterEqual, err := qe.resolveFilter(mq.meta, mq.selector.filterEqual)
	if err != nil {
		return err
	}
	filterNotEqual, mappedFilterNotEqual, err := qe.resolveFilter(mq.meta, mq.selector.filterNotEqual)
	if err != nil {
		return err
	}
	qs := normalizedQueryString(mq.meta.Name, mq.whatKind, by, filterEqual, filterNotEqual)
	pq := &preparedPointsQuery{
		user:        qe.ai.user,
		version:     Version2,
		metricID:    mq.meta.MetricID,
		preKeyTagID: mq.meta.PreKeyTagID,
		isStringTop: false,
		kind:        mq.whatKind,
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
		m, err := qe.h.cache.Get(ctx, Version2, qs, pq, lod)
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
						// StaleNaN is never replaced
						(*data[k])[kk] = math.Float64frombits(value2.StaleNaN)
					}
					qe.resolveTags(ss.tsTags, mq, &meta, histograms, ixToLE)
				}
				(*data[k])[i] = selectTSValue(mq.what, lod.stepSec, 1, &ss)
			}
			allTimes = append(allTimes, s[0].Time*1_000)
			i++
		}
	}
	// filter tags
	var remove []int
	for i, m := range meta {
		for _, matcher := range mq.selector.filterRegexp {
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
	if mq.what == queryFnCumulCount {
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
		mq.selector.node.Series = append(mq.selector.node.Series, newPromSeries(allTimes, *data[i], v.Tags))
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
			return nil, nil, fmt.Errorf("not found tag %q for metric %q", key, meta.Name)
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

func (vs *promVectorSelector) initWhat(path []parser.Node, fn queryFn) {
	omitNameTag := false
	defer func() {
		for _, mq := range vs.matchingMetrics {
			if fn != -1 {
				mq.what = fn
			} else if mq.meta.Kind == format.MetricKindCounter {
				mq.what = queryFnCumulCount
			} else {
				mq.what = queryFnAvg
			}
			mq.whatKind = queryFnToQueryFnKind(mq.what)
			mq.omitNameTag = omitNameTag
		}
	}()
	i := len(path) - 1
	if i == -1 {
		return
	}
	node := path[i]
	if e, ok := node.(*parser.AggregateExpr); ok {
		var aggFn queryFn
		switch e.Op {
		case parser.MIN:
			aggFn = queryFnMin
		case parser.MAX:
			aggFn = queryFnMax
		case parser.AVG:
			aggFn = queryFnAvg
		case parser.SUM:
			aggFn = queryFnSum
		case parser.COUNT:
			aggFn = queryFnCount
		}
		if aggFn != fn && fn != -1 {
			return
		}
		// Parent is an aggregate function
		// AND it is mapped directly to one of our precomputed columns
		// AND it does not conflict with user provided one
		// so, we can return result from precomputed column skipping current AST node
		fn = aggFn
		vs.topNode = node
		i--
	}
	if i == -1 {
		return
	}
	switch e := path[i].(type) {
	case *parser.MatrixSelector:
		if i == 0 {
			return
		}
		if e2, ok := path[i-1].(*parser.Call); ok {
			if vs.step == e.Range {
				callFn := queryFn(-1)
				switch e2.Func.Name {
				case "min_over_time":
					callFn = queryFnMin
				case "max_over_time":
					callFn = queryFnMax
				case "avg_over_time":
					callFn = queryFnAvg
				case "sum_over_time":
					callFn = queryFnSum
				case "count_over_time":
					callFn = queryFnCount
				}
				if callFn != -1 && callFn == fn {
					// Parent is a matrix selector
					// AND matrix selector range equals to query step
					// AND matrix selector is followed by *_over_time function call
					// AND *_over_time function call is mapped directly to one of our precomputed columns
					// AND *_over_time function call match current function
					// so, we can return result from precomputed column skipping current AST node
					fn = callFn
					vs.topNode = node
					omitNameTag = true
				}
			}
		}
	case *parser.Call:
		if fn != -1 {
			return
		}
		switch e.Func.Name {
		case "histogram_quantile":
			// Prometheus engine will get value from "le" tag
			fn = queryFnCount
		case "count_over_time":
			// Prometheus engine will calculate count by summing counters provided
			e.Func = parser.Functions["sum_over_time"]
			fn = queryFnCount
		}
	}
}

func (vs *promVectorSelector) initGrouping(path []parser.Node) {
	for i := len(path); i > 0; i-- {
		node := path[i-1]
		if e, ok := node.(*parser.AggregateExpr); ok {
			for _, mq := range vs.matchingMetrics {
				mq.initGrouping(e)
			}
			break
		}
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
