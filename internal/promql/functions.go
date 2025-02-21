// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package promql

import (
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/promql/parser"
	"github.com/vkcom/statshouse/internal/util"
)

// region AggregateExpr

type aggregateFunc func(*evaluator, *parser.AggregateExpr) ([]Series, error)

var aggregates map[parser.ItemType]aggregateFunc

func init() {
	aggregates = map[parser.ItemType]aggregateFunc{
		parser.AVG:               aggregateAt0(funcAvg),
		parser.BOTTOMK:           funcTopK,
		parser.COUNT:             aggregateAt0(funcCount),
		parser.COUNT_VALUES:      funcCountValues,
		parser.DROP_EMPTY_SERIES: funcDropEmptySeries,
		parser.GROUP:             aggregateAt0(funcGroup),
		parser.MAX:               aggregateAt0(funcMax),
		parser.MIN:               aggregateAt0(funcMin),
		parser.QUANTILE:          aggregateAt0(funcQuantile),
		parser.STDDEV:            aggregateAt0(funcStdDev),
		parser.STDVAR:            aggregateAt0(funcStdVar),
		parser.SUM:               aggregateAt0(funcSum),
		parser.TOPK:              funcTopK,
		parser.SORT:              funcTopK,
		parser.SORT_DESC:         funcTopK,
		parser.AGGREGATE:         aggregateNOP,
	}
}

func aggregateNOP(ev *evaluator, expr *parser.AggregateExpr) ([]Series, error) {
	return ev.eval(expr.Expr)
}

func aggregateAt0(fn func([]SeriesData, parser.Expr)) aggregateFunc {
	return func(ev *evaluator, expr *parser.AggregateExpr) ([]Series, error) {
		res, err := ev.eval(expr.Expr)
		if err != nil {
			return nil, err
		}
		for i := range res {
			if len(res[i].Data) == 0 {
				continue
			}
			var m map[uint64][]int
			m, tags, err := res[i].group(ev, hashOptions{
				on:         !expr.Without,
				tags:       expr.Grouping,
				listUnused: true,
			})
			if err != nil {
				return nil, err
			}
			sr := ev.newSeries(len(m), res[i].Meta)
			for _, xs := range m {
				ds := res[i].dataAt(xs...)
				fn(ds, expr.Param)
				for j := range ds[0].MinMaxHost {
					ds[0].MinMaxHost[j] = ev.groupMinMaxHost(ds, j)
				}
				for _, id := range tags[xs[0]].unused {
					ds[0].Tags.remove(id)
				}
				for j := range ds[1:] {
					if ds[0].Offset != ds[j].Offset {
						ds[0].Offset = 0
						break
					}
				}
				sr.Data = append(sr.Data, ds[0])
				ev.freeAll(ds[1:])
			}
			res[i] = sr
		}
		return res, nil
	}
}

func funcAvg(ds []SeriesData, _ parser.Expr) {
	d0 := *ds[0].Values
	for i := 0; i < len(d0); i++ {
		var (
			res float64
			cnt int
		)
		for j := 0; j < len(ds); j++ {
			v := (*ds[j].Values)[i]
			if math.IsNaN(v) {
				continue
			}
			res += v
			cnt++
		}
		if cnt != 0 {
			d0[i] = res / float64(cnt)
		} else {
			d0[i] = math.NaN()
		}
	}
}

func funcCount(ds []SeriesData, _ parser.Expr) {
	d0 := *ds[0].Values
	for i := 0; i < len(d0); i++ {
		var n int
		for _, d := range ds {
			if !math.IsNaN((*d.Values)[i]) {
				n++
			}
		}
		d0[i] = float64(n)
	}
}

func funcCountValues(ev *evaluator, expr *parser.AggregateExpr) ([]Series, error) {
	srs, err := ev.eval(expr.Expr)
	if err != nil {
		return nil, err
	}
	res := make([]Series, len(srs))
	for i := range srs {
		res[i].Meta = srs[i].Meta
		if len(srs[i].Data) == 0 {
			continue
		}
		m, tags, err := srs[i].group(ev, hashOptions{
			on:       !expr.Without,
			tags:     expr.Grouping,
			listUsed: true,
		})
		if err != nil {
			return nil, err
		}
		for _, xs := range m {
			dm := make(map[float64]SeriesData)
			for _, x := range xs {
				src := &srs[i].Data[x]
				for j, v := range *src.Values {
					d, ok := dm[v]
					if !ok {
						d = SeriesData{
							Values: ev.alloc(),
							Tags:   src.Tags.cloneSome(tags[x].used...),
							Offset: src.Offset,
							What:   src.What,
						}
						for k := range *d.Values {
							(*d.Values)[k] = 0
						}
						d.Tags.add(
							&SeriesTag{
								ID:     expr.Param.(*parser.StringLiteral).Val,
								SValue: strconv.FormatFloat(v, 'f', -1, 64)},
							&srs[i].Meta)
						dm[v] = d
					}
					(*d.Values)[j]++
				}
			}
			for _, d := range dm {
				for i := range *d.Values {
					if (*d.Values)[i] == 0 {
						(*d.Values)[i] = NilValue
					}
				}
				res[i].Data = append(res[i].Data, d)
			}
			srs[i].freeSome(ev, xs...)
		}
	}
	return res, nil
}

func funcDropEmptySeries(ev *evaluator, expr *parser.AggregateExpr) ([]Series, error) {
	res, err := ev.eval(expr.Expr)
	if err != nil {
		return nil, err
	}
	ev.stableRemoveEmptySeries(res)
	return res, nil
}

func (ev *evaluator) removeEmptySeries(srs []Series) {
	if ev.opt.Mode == data_model.TagsQuery {
		return
	}
	if ev.t.ViewStartX == ev.t.ViewEndX {
		return
	}
	for i := 0; i < len(srs); i++ {
		srs[i].removeEmpty(ev)
	}
}

func (ev *evaluator) stableRemoveEmptySeries(srs []Series) {
	if ev.opt.Mode == data_model.TagsQuery {
		return
	}
	if ev.t.ViewStartX == ev.t.ViewEndX {
		return
	}
	for i := 0; i < len(srs); i++ {
		if srs[i].Meta.Total == 0 {
			srs[i].Meta.Total = len(srs[i].Data)
		}
		ds := make([]SeriesData, 0, len(srs[i].Data))
		for j := 0; j < len(srs[i].Data); j++ {
			var keep bool
			for _, v := range (*srs[i].Data[j].Values)[ev.t.ViewStartX:ev.t.ViewEndX] {
				if !math.IsNaN(v) {
					keep = true
					break
				}
			}
			if keep {
				ds = append(ds, srs[i].Data[j])
			} else {
				ev.free(srs[i].Data[j].Values)
				srs[i].Meta.Total--
			}
		}
		srs[i].Data = ds
	}
}

func funcGroup(ds []SeriesData, _ parser.Expr) {
	d0 := *ds[0].Values
	for i := 0; i < len(d0); i++ {
		d0[i] = 1
	}
}

func funcMax(ds []SeriesData, _ parser.Expr) {
	d0 := *ds[0].Values
	for i := 0; i < len(d0); i++ {
		var (
			res = math.NaN()
			nan = true
		)
		for _, d := range ds {
			v := (*d.Values)[i]
			if math.IsNaN(v) {
				continue
			}
			if nan || res < v {
				res = v
				nan = false
			}
		}
		d0[i] = res
	}
}

func funcMin(ds []SeriesData, _ parser.Expr) {
	d0 := *ds[0].Values
	for i := 0; i < len(d0); i++ {
		var (
			res = math.NaN()
			nan = true
		)
		for _, d := range ds {
			v := (*d.Values)[i]
			if math.IsNaN(v) {
				continue
			}
			if nan || v < res {
				res = v
				nan = false
			}
		}
		d0[i] = res
	}
}

func funcQuantile(ds []SeriesData, p parser.Expr) {
	var (
		q     = p.(*parser.NumberLiteral).Val
		valid bool
	)
	if math.IsNaN(q) {
		q = math.NaN()
	} else if q < 0 {
		q = math.Inf(-1)
	} else if q > 1 {
		q = math.Inf(1)
	} else {
		valid = true
	}
	d0 := *ds[0].Values
	if !valid {
		for i := 0; i < len(d0); i++ {
			d0[i] = q
		}
	} else {
		var (
			x  = make([]int, len(ds))
			ix = q * (float64(len(ds)) - 1)
			i1 = int(math.Floor(ix))
			i2 = int(math.Min(float64(len(ds)-1), float64(i1+1)))
			w1 = float64(i2) - ix
			w2 = 1 - w1
		)
		for i := range x {
			x[i] = i
		}
		for i := 0; i < len(d0); i++ {
			sort.Slice(x, func(j, k int) bool { return (*ds[x[j]].Values)[i] < (*ds[x[k]].Values)[i] })
			d0[i] = (*ds[x[i1]].Values)[i]*w1 + (*ds[x[i2]].Values)[i]*w2
		}
	}
}

func funcStdDev(ds []SeriesData, p parser.Expr) {
	funcStdVar(ds, p)
	d0 := *ds[0].Values
	for i := 0; i < len(d0); i++ {
		(*ds[0].Values)[i] = math.Sqrt((*ds[0].Values)[i])
	}
}

func funcStdVar(ds []SeriesData, _ parser.Expr) {
	d0 := *ds[0].Values
	for i := 0; i < len(d0); i++ {
		var cnt int
		var sum float64
		for _, d := range ds {
			v := (*d.Values)[i]
			if !math.IsNaN(v) {
				sum += v
				cnt++
			}
		}
		mean := sum / float64(cnt)
		var res float64
		for _, d := range ds {
			v := (*d.Values)[i]
			if !math.IsNaN(v) {
				d := v - mean
				res += d * d / float64(cnt)
			}
		}
		d0[i] = res
	}
}

func funcSum(ds []SeriesData, _ parser.Expr) {
	d0 := *ds[0].Values
	for i := 0; i < len(d0); i++ {
		var (
			res = math.NaN()
			nan = true
		)
		for j := 0; j < len(ds); j++ {
			v := (*ds[j].Values)[i]
			if math.IsNaN(v) {
				continue
			}
			if nan {
				res = v
				nan = false
			} else {
				res += v
			}
		}
		d0[i] = res
	}
}

func funcTopK(ev *evaluator, expr *parser.AggregateExpr) ([]Series, error) {
	k := math.MaxInt
	if expr.Op == parser.TOPK || expr.Op == parser.BOTTOMK {
		k = int(expr.Param.(*parser.NumberLiteral).Val)
		if k <= 0 {
			return make([]Series, len(ev.opt.Offsets)), nil
		}
	}
	res, err := ev.eval(expr.Expr)
	if err != nil {
		return nil, err
	}
	ev.removeEmptySeries(res)
	type (
		sortedSeriesGroup struct {
			ds []SeriesData
			k  int
			ws []float64 // weights, not sorted
			xs []int     // index, sorted according to weights
		}
		bucket struct {
			topK map[uint64][]int
			gs   []sortedSeriesGroup
		}
	)
	var (
		bs   = make(map[uint64]*bucket)
		desc bool
		sort = func(ds []SeriesData) sortedSeriesGroup {
			var (
				w = ev.weight(ds)
				x = make([]int, len(ds))
				n = k
			)
			if len(ds) < n {
				n = len(ds)
			}
			for i := range x {
				x[i] = i
			}
			if desc {
				util.PartialSortIndexByValueDesc(x, w, n, ev.opt.Rand, nil)
			} else {
				util.PartialSortIndexByValueAsc(x, w, n, ev.opt.Rand, nil)
			}
			return sortedSeriesGroup{ds, n, w, x}
		}
		swap = func(g *sortedSeriesGroup, i, j int) {
			g.xs[i], g.xs[j] = g.xs[j], g.xs[i]
		}
		less = func(g *sortedSeriesGroup, i, j int) bool {
			if desc {
				return g.ws[g.xs[i]] > g.ws[g.xs[j]]
			} else {
				return g.ws[g.xs[i]] < g.ws[g.xs[j]]
			}
		}
	)
	if expr.Op == parser.TOPK || expr.Op == parser.SORT_DESC {
		desc = true
	}
	for i := range res {
		if len(res[i].Data) == 0 {
			continue
		}
		var m map[uint64][]int
		m, _, err := res[i].group(ev, hashOptions{
			on:   !expr.Without,
			tags: expr.Grouping,
		})
		if err != nil {
			return nil, err
		}
		for h, g := range m {
			b := bs[h]
			if b == nil {
				b = &bucket{
					topK: make(map[uint64][]int),
					gs:   make([]sortedSeriesGroup, len(res)),
				}
				bs[h] = b
			}
			b.gs[i] = sort(res[i].dataAt(g...))
			for j := 0; j < b.gs[i].k; j++ {
				sum, _, err := b.gs[i].ds[b.gs[i].xs[j]].Tags.hash(ev, hashOptions{})
				if err != nil {
					return nil, err
				}
				y := b.topK[sum]
				if y == nil {
					y = make([]int, len(res))
					b.topK[sum] = y
				}
				y[i]++
			}
		}
	}
	for _, b := range bs {
		if len(b.topK) <= k {
			// top is consistent across shifts
			continue
		}
		for h, xs := range b.topK {
			for i := range b.gs {
				if xs[i] != 0 {
					// top is within shift top
					continue
				}
				for j := b.gs[i].k; b.gs[i].k < len(b.topK) && j < len(b.gs[i].ds); j++ {
					v, _, err := b.gs[i].ds[b.gs[i].xs[j]].Tags.hash(ev, hashOptions{})
					if err != nil {
						return nil, err
					}
					if v == h {
						// found top from another shift
						swap(&b.gs[i], b.gs[i].k, j)
						// preserve order
						for x := b.gs[i].k; x > k && less(&b.gs[i], x, x-1); x-- {
							swap(&b.gs[i], x, x-1)
						}
						// bump number of sorted series
						b.gs[i].k++
					}
				}
			}
		}
	}
	for i := range res {
		res[i] = ev.newSeries(0, res[i].Meta)
	}
	for i := range bs {
		for j, g := range bs[i].gs {
			for _, x := range g.xs[:g.k] {
				res[j].Data = append(res[j].Data, g.ds[x])
			}
			ev.freeSome(g.ds, g.xs[g.k:]...)
		}
	}
	return res, nil
}

// endregion AggregateExpr

// region Call

type callFunc func(*evaluator, parser.Expressions) ([]Series, error)

var calls map[string]callFunc

func init() {
	calls = map[string]callFunc{
		"abs":              simpleCall(math.Abs),
		"absent":           funcAbsent,
		"absent_over_time": funcAbsentOverTime,
		"ceil":             simpleCall(math.Ceil),
		"changes":          overTimeCall(funcChanges, true, 0),
		"clamp":            funcClamp,
		"clamp_max":        funcClampMax,
		"clamp_min":        funcClampMin,
		"day_of_month":     timeCall(time.Time.Day),
		"day_of_week":      timeCall(time.Time.Weekday),
		"day_of_year":      timeCall(time.Time.YearDay),
		"days_in_month":    timeCall(func(t time.Time) int { return 32 - time.Date(t.Year(), t.Month(), 32, 0, 0, 0, 0, t.Location()).Day() }),
		"delta":            seriesCall(funcDelta),
		"deriv":            seriesCall(funcDeriv),
		"exp":              simpleCall(math.Exp),
		"floor":            simpleCall(math.Floor),
		// "histogram_count": ?
		// "histogram_sum": ?
		// "histogram_fraction": ?
		"histogram_quantile": funcHistogramQuantile,
		"holt_winters":       funcHoltWinters,
		"hour":               timeCall(time.Time.Hour),
		"idelta":             seriesCall(funcIdelta),
		"increase":           seriesCall(funcDelta),
		"irate":              seriesCall(funcIrate),
		"label_join":         funcLabelJoin,
		"label_replace":      funcLabelReplace,
		"label_set":          funcLabelSet,
		"label_minhost":      funcLabelMinHost,
		"label_maxhost":      funcLabelMaxHost,
		"ln":                 simpleCall(math.Log),
		"log2":               simpleCall(math.Log2),
		"log10":              simpleCall(math.Log10),
		"lod_step_sec":       funcLODStepSec,
		"minute":             timeCall(time.Time.Minute),
		"month":              timeCall(time.Time.Month),
		"predict_linear":     funcPredictLinear,
		"prefix_sum":         funcPrefixSum,
		"rate":               seriesCall(funcRate),
		"resets":             seriesCall(funcResets),
		"round":              funcRound,
		"scalar":             funcScalar,
		"sgn": simpleCall(func(v float64) float64 {
			if v < 0 {
				return -1
			} else if v > 0 {
				return 1
			}
			return v
		}),
		"sqrt":               simpleCall(math.Sqrt),
		"time":               funcTimestamp,
		"timestamp":          funcTimestamp,
		"vector":             funcVector,
		"year":               timeCall(time.Time.Year),
		"avg_over_time":      overTimeCall(funcAvgOverTime, false, NilValue),
		"min_over_time":      overTimeCall(funcMinOverTime, false, NilValue),
		"max_over_time":      overTimeCall(funcMaxOverTime, false, NilValue),
		"sum_over_time":      overTimeCall(funcSumOverTime, true, NilValue),
		"count_over_time":    overTimeCall(funcCountOverTime, true, 0),
		"quantile_over_time": funcQuantileOverTime,
		"stddev_over_time":   overTimeCall(funcStdDevOverTime, true, NilValue),
		"stdvar_over_time":   overTimeCall(funcStdVarOverTime, true, NilValue),
		"last_over_time":     overTimeCall(funcLastOverTime, false, NilValue),
		"present_over_time":  funcPresentOverTime,
		"acos":               simpleCall(math.Acos),
		"acosh":              simpleCall(math.Acosh),
		"asin":               simpleCall(math.Asin),
		"asinh":              simpleCall(math.Asinh),
		"atan":               simpleCall(math.Atan),
		"atanh":              simpleCall(math.Atanh),
		"cos":                simpleCall(math.Cos),
		"cosh":               simpleCall(math.Cosh),
		"sin":                simpleCall(math.Sin),
		"sinh":               simpleCall(math.Sinh),
		"tan":                simpleCall(math.Tan),
		"tanh":               simpleCall(math.Tanh),
		"deg":                simpleCall(func(v float64) float64 { return v * 180 / math.Pi }),
		"pi":                 funcPi,
		"rad":                simpleCall(func(v float64) float64 { return v * math.Pi / 180 }),
	}
}

type window struct {
	t, ct   []int64   // time, current interval time
	v, cv   []float64 // values, current interval values
	w       int64     // target width
	s       int64     // current value of t[r+1]-t[r]
	l, r, n int       // current [l,r] interval, number of not NaN values inside
	strict  bool      // don't stretch to LOD resolution if set
	done    bool      // next "moveOneLeft" returns false if set
}

func newWindow(t []int64, v []float64, w, step int64, s bool) window {
	return window{t: t, v: v, w: w, s: step, strict: s, l: len(t), r: len(t), done: len(t) == 0}
}

func (wnd *window) moveOneLeft() bool {
	if wnd.done {
		return false
	}
	// shift right boundary
	var l, r, n = wnd.l, wnd.r, wnd.n
	if 0 < n && !math.IsNaN(wnd.v[r]) {
		n--
	}
	r--
	// shift left boundary
	if l > r {
		l = r
		if math.IsNaN(wnd.v[l]) || (wnd.strict && wnd.w < wnd.s) {
			n = 0
		} else {
			n = 1
		}
	}
	found := wnd.w <= 0 && l == r
	for !found && 0 < l {
		found = (wnd.w <= wnd.t[r]-wnd.t[l]+wnd.s) || (wnd.strict && wnd.w < wnd.t[r]-wnd.t[l-1]+wnd.s)
		if found {
			break
		}
		l--
		if !math.IsNaN(wnd.v[l]) {
			n++
		}
	}
	// see what we got
	if l <= 0 {
		wnd.done = true
		if !found {
			return false
		}
	} else {
		wnd.s = wnd.t[r] - wnd.t[r-1]
	}
	wnd.l, wnd.r, wnd.n = l, r, n
	return true
}

func (wnd *window) get() ([]int64, []float64) {
	if wnd.n == 0 {
		return nil, nil
	}
	var (
		l = wnd.l
		r = wnd.r
	)
	for l < r && math.IsNaN(wnd.v[l]) {
		l++
	}
	for l < r && math.IsNaN(wnd.v[r]) {
		r--
	}
	if r-l+1 == wnd.n {
		return wnd.t[l : r+1], wnd.v[l : r+1]
	}
	wnd.ct = wnd.ct[:0]
	wnd.cv = wnd.cv[:0]
	for ; l <= r; l++ {
		if !math.IsNaN(wnd.v[l]) {
			wnd.ct = append(wnd.ct, wnd.t[l])
			wnd.cv = append(wnd.cv, wnd.v[l])
		}
	}
	return wnd.ct, wnd.cv
}

func (wnd *window) getValues() []float64 {
	if wnd.n == 0 {
		return nil
	}
	var (
		l = wnd.l
		r = wnd.r
	)
	for l < r && math.IsNaN(wnd.v[l]) {
		l++
	}
	for l < r && math.IsNaN(wnd.v[r]) {
		r--
	}
	if r-l+1 == wnd.n {
		return wnd.v[l : r+1]
	}
	wnd.cv = wnd.copyValues(wnd.cv[:0], l, r)
	return wnd.cv
}

func (wnd *window) getCopyOfValues() []float64 {
	wnd.cv = wnd.copyValues(wnd.cv[:0], wnd.l, wnd.r)
	return wnd.cv
}

func (wnd *window) copyValues(v []float64, l, r int) []float64 {
	if wnd.n == 0 {
		return v
	}
	for ; l <= r; l++ {
		if !math.IsNaN(wnd.v[l]) {
			v = append(v, wnd.v[l])
		}
	}
	return v
}

func (wnd *window) setValueAtRight(v float64) {
	var (
		wasNaN = math.IsNaN(wnd.v[wnd.r])
		isNaN  = math.IsNaN(v)
	)
	switch {
	case wasNaN && !isNaN:
		wnd.n++
	case !wasNaN && isNaN:
		wnd.n--
	}
	wnd.v[wnd.r] = v
}

func (wnd *window) fillPrefixWith(v float64) {
	for i := 0; i < wnd.r; i++ {
		wnd.v[i] = v
	}
}

func seriesCall(fn func(*evaluator, Series) Series) callFunc {
	return func(ev *evaluator, args parser.Expressions) ([]Series, error) {
		res, err := ev.eval(args[0])
		if err != nil {
			return nil, err
		}
		for i := range res {
			res[i] = fn(ev, res[i])
		}
		return res, nil
	}
}

func overTimeCall(fn func(v []float64) float64, strict bool, nilValue float64) callFunc {
	return func(ev *evaluator, args parser.Expressions) ([]Series, error) {
		res, err := ev.eval(args[0])
		if err != nil {
			return nil, err
		}
		for i := range res {
			for _, s := range res[i].Data {
				wnd := ev.newWindow(*s.Values, strict)
				for wnd.moveOneLeft() {
					if wnd.n != 0 {
						wnd.setValueAtRight(fn((*s.Values)[wnd.l : wnd.r+1]))
					} else {
						wnd.setValueAtRight(nilValue)
					}
				}
				wnd.fillPrefixWith(NilValue)
			}
		}
		return res, nil
	}
}

func simpleCall(fn func(float64) float64) callFunc {
	return func(ev *evaluator, args parser.Expressions) ([]Series, error) {
		res, err := ev.eval(args[0])
		if err != nil {
			return nil, err
		}
		for i := range res {
			for _, s := range res[i].Data {
				for i := range *s.Values {
					(*s.Values)[i] = fn((*s.Values)[i])
				}
			}
		}
		return res, nil
	}
}

func timeCall[V int | time.Weekday | time.Month](fn func(time.Time) V) callFunc {
	return func(ev *evaluator, args parser.Expressions) (res []Series, err error) {
		if len(args) != 0 {
			res, err = ev.eval(args[0])
			if err != nil {
				return nil, err
			}
		} else {
			res = make([]Series, 0, len(ev.opt.Offsets))
			t := ev.time()
			for _, v := range ev.opt.Offsets {
				d := SeriesData{
					Values: ev.alloc(),
					Offset: v,
				}
				for i := range *d.Values {
					(*d.Values)[i] = float64(t[i] - v)
				}
				res = append(res, Series{Data: []SeriesData{d}})
			}
		}
		for i := range res {
			for _, d := range res[i].Data {
				for i, v := range *d.Values {
					(*d.Values)[i] = float64(fn(time.Unix(int64(v), 0).In(ev.location)))
				}
			}
		}
		return res, nil
	}
}

func funcAbsent(ev *evaluator, args parser.Expressions) ([]Series, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("invalid argument count in absent(): expected 1, got %d", len(args))
	}
	res, err := ev.eval(args[0])
	if err != nil {
		return nil, err
	}
	for i := range res {
		var (
			d SeriesData // absent row
			n int        // absent count
		)
		if len(res[i].Data) != 0 {
			d = res[i].Data[0]
			for j := range ev.time() {
				var m int
				for _, s := range res[i].Data {
					if math.Float64bits((*s.Values)[j]) != NilValueBits {
						m++
					}
				}
				if m == 0 {
					(*d.Values)[j] = 1
					n++
				} else {
					(*d.Values)[j] = NilValue
				}
			}
			ev.freeAll(res[i].Data[1:])
		} else {
			d = SeriesData{Values: ev.alloc()}
			n = len(*d.Values)
			for i := range *d.Values {
				(*d.Values)[i] = 1
			}
		}
		res[i] = Series{
			Data: []SeriesData{d},
			Meta: res[i].Meta,
		}
		if sel, ok := args[0].(*parser.VectorSelector); ok && n != 0 {
			for _, m := range sel.LabelMatchers {
				if m.Type == labels.MatchEqual {
					res[i].AddTagAt(0, &SeriesTag{ID: m.Name, SValue: m.Value})
				}
			}
		}
	}
	return res, nil
}

func funcAbsentOverTime(ev *evaluator, args parser.Expressions) ([]Series, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("invalid argument count in absent_over_time(): expected 1, got %d", len(args))
	}
	res, err := ev.eval(args[0])
	if err != nil {
		return nil, err
	}
	for i := range res {
		var (
			d SeriesData // absent row
			n int        // absent count
		)
		if len(res[i].Data) != 0 {
			d = res[i].Data[0]
			lastSeen := int64(math.MinInt64)
			for j, t := range ev.time() {
				var m int
				for _, s := range res[i].Data {
					if math.Float64bits((*s.Values)[j]) != NilValueBits {
						m++
					}
				}
				if m == 0 && lastSeen < t-ev.r {
					(*d.Values)[j] = 1
					n++
				} else {
					(*d.Values)[j] = NilValue
					lastSeen = t
				}
			}
			ev.freeAll(res[i].Data[1:])
		} else {
			d = SeriesData{Values: ev.alloc()}
			n = len(*d.Values)
			for i := range *d.Values {
				(*d.Values)[i] = 1
			}
		}
		res[i] = Series{
			Data: []SeriesData{d},
			Meta: res[i].Meta,
		}
		if sel, ok := args[0].(*parser.VectorSelector); ok && n != 0 {
			for _, m := range sel.LabelMatchers {
				if m.Type == labels.MatchEqual {
					res[i].AddTagAt(0, &SeriesTag{ID: m.Name, SValue: m.Value})
				}
			}
		}
	}
	return res, nil
}

func funcChanges(v []float64) float64 {
	var i, j, res int
	for i < len(v) && math.IsNaN(v[i]) {
		i++
	}
	for ; i < len(v); i = j {
		j = i + 1
		for j < len(v) && math.IsNaN(v[j]) {
			j++
		}
		if j < len(v) && v[i] != v[j] {
			res++
		}
	}
	return float64(res)
}

func funcClamp(ev *evaluator, args parser.Expressions) ([]Series, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("invalid argument count in clamp(): expected 3, got %d", len(args))
	}
	res, err := ev.eval(args[0])
	if err != nil {
		return nil, err
	}
	min := args[1].(*parser.NumberLiteral).Val
	max := args[2].(*parser.NumberLiteral).Val
	// return NaN if min or max is NaN
	if math.IsNaN(min) || math.IsNaN(max) {
		for x := range res {
			for _, s := range res[x].Data {
				for i := range *s.Values {
					(*s.Values)[i] = math.NaN()
				}
			}
		}
		return res, nil
	}
	// return an empty vector if min > max
	if min > max {
		for i := range res {
			res[i].free(ev)
			res[i] = Series{}
		}
		return res, nil
	}
	for i := range res {
		for _, d := range res[i].Data {
			for j := range *d.Values {
				if (*d.Values)[j] < min {
					(*d.Values)[j] = min
				} else if (*d.Values)[j] > max {
					(*d.Values)[j] = max
				}
				// else { min <= (*s.Data)[i] <= max }
			}
		}
	}
	return res, nil
}

func funcClampMax(ev *evaluator, args parser.Expressions) ([]Series, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("invalid argument count in clamp_max(): expected 2, got %d", len(args))
	}
	res, err := ev.eval(args[0])
	if err != nil {
		return nil, err
	}
	max := args[1].(*parser.NumberLiteral).Val
	// return NaN if max is NaN
	if math.IsNaN(max) {
		for x := range res {
			for _, s := range res[x].Data {
				for i := range *s.Values {
					(*s.Values)[i] = math.NaN()
				}
			}
		}
		return res, nil
	}
	for i := range res {
		for _, d := range res[i].Data {
			for j := range *d.Values {
				if (*d.Values)[j] > max {
					(*d.Values)[j] = max
				}
				// else { (*s.Data)[i] <= max }
			}
		}
	}
	return res, nil
}

func funcClampMin(ev *evaluator, args parser.Expressions) ([]Series, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("invalid argument count in clamp_min(): expected 2, got %d", len(args))
	}
	res, err := ev.eval(args[0])
	if err != nil {
		return nil, err
	}
	min := args[1].(*parser.NumberLiteral).Val
	// return NaN if min is NaN
	if math.IsNaN(min) {
		for i := range res {
			for _, d := range res[i].Data {
				for j := range *d.Values {
					(*d.Values)[j] = math.NaN()
				}
			}
		}
		return res, nil
	}
	for i := range res {
		for _, d := range res[i].Data {
			for j := range *d.Values {
				if (*d.Values)[j] < min {
					(*d.Values)[j] = min
				}
				// else { (*s.Data)[i] >= min }
			}
		}
	}
	return res, nil
}

func funcDelta(ev *evaluator, res Series) Series {
	res = funcRate(ev, res)
	if ev.r == 0 {
		return res
	}
	for _, d := range res.Data {
		for i := range *d.Values {
			(*d.Values)[i] *= float64(ev.r)
		}
	}
	return res
}

func funcDeriv(ev *evaluator, sr Series) Series {
	for _, d := range sr.Data {
		wnd := ev.newWindow(*d.Values, false)
		for wnd.moveOneLeft() {
			if wnd.n != 0 {
				slope, _ := linearRegression(wnd.get())
				wnd.setValueAtRight(slope)
			} else {
				wnd.setValueAtRight(NilValue)
			}
		}
		wnd.fillPrefixWith(NilValue)
	}
	return sr
}

func funcIdelta(ev *evaluator, sr Series) Series {
	for _, d := range sr.Data {
		for j := len(*d.Values) - 1; j > 0; j-- {
			(*d.Values)[j] = (*d.Values)[j] - (*d.Values)[j-1]
		}
		(*d.Values)[0] = NilValue
	}
	return sr
}

func funcIrate(ev *evaluator, sr Series) Series {
	t := ev.time()
	for _, s := range sr.Data {
		for i := len(*s.Values) - 1; i > 0; i-- {
			(*s.Values)[i] = ((*s.Values)[i] - (*s.Values)[i-1]) / float64(t[i]-t[i-1])
		}
		(*s.Values)[0] = NilValue
	}
	return sr
}

func funcHistogramQuantile(ev *evaluator, args parser.Expressions) ([]Series, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("invalid argument count in histogram_quantile(): expected 2, got %d", len(args))
	}
	res, err := ev.eval(args[1])
	if err != nil {
		return nil, err
	}
	for i := range res {
		hs, err := res[i].histograms(ev)
		if err != nil {
			return nil, fmt.Errorf("failed to restore histogram: %v", err)
		}
		sr := ev.newSeries(len(hs), res[i].Meta)
		for _, h := range hs {
			d := res[i].Data
			s := *d[h.buckets[0].x].Values
			if len(h.buckets) == 0 {
				for i := range s {
					s[i] = NilValue
				}
			} else {
				q := args[0].(*parser.NumberLiteral).Val // quantile
				for j := range s {
					var total float64 // total count
					for k := 0; k < len(h.buckets); k++ {
						v := (*d[h.buckets[k].x].Values)[j]
						if !math.IsNaN(v) {
							total += v
						}
					}
					if total == 0 {
						s[j] = NilValue
						continue
					}
					var count float64 // bucket count
					var lo float64    // bucket lower bound count
					var hi float64    // bucket upper bound count
					var x int         // bucket upper bound index
					rank := q * total // quantile corresponding count
					buckets := res[i].Meta.Metric.HistogramBuckets
					for k := 0; x < len(buckets) && k < len(h.buckets); x++ {
						if buckets[x] == h.buckets[k].le {
							v := (*d[h.buckets[k].x].Values)[j]
							if !math.IsNaN(v) {
								count = v
								hi += count
								if hi >= rank {
									break
								}
								lo = hi
							}
							k++
						}
					}
					var v float64
					switch {
					case x == 0:
						// lower bound is -Inf
						v = float64(buckets[0])
					case x == len(buckets):
						// upper bound is +Inf
						v = float64(buckets[len(buckets)-2])
					case hi == rank:
						// on bucket border
						v = float64(buckets[x])
					default:
						// inside bucket
						v = float64(buckets[x-1])
						if count != 0 {
							v += float64(buckets[x]-buckets[x-1]) * (rank - lo) / count
						}
					}
					s[j] = v
				}
			}
			ev.freeAll(h.data()[1:])
			sr.appendAll(h.seriesAt(0))
		}
		res[i] = sr
	}
	return res, nil
}

func funcLabelJoin(ev *evaluator, args parser.Expressions) ([]Series, error) {
	if len(args) < 3 {
		return nil, fmt.Errorf("invalid argument count in label_join(): expected at least 3, got %d", len(args))
	}
	res, err := ev.eval(args[0])
	if err != nil {
		return nil, err
	}
	var (
		dst = args[1].(*parser.StringLiteral).Val
		sep = args[2].(*parser.StringLiteral).Val
		src = make(map[string]bool, len(args)-3)
	)
	for i := 3; i < len(args); i++ {
		src[args[i].(*parser.StringLiteral).Val] = true
	}
	for i := range res {
		for j, m := range res[i].Data {
			var s []string
			for k := range src {
				if t, ok := m.Tags.gets(ev, k); ok && len(t.SValue) != 0 {
					s = append(s, t.SValue)
				}
			}
			if len(s) != 0 {
				res[i].AddTagAt(j, &SeriesTag{ID: dst, SValue: strings.Join(s, sep)})
			}
		}
	}
	return res, nil
}

func funcLabelReplace(ev *evaluator, args parser.Expressions) ([]Series, error) {
	if len(args) != 5 {
		return nil, fmt.Errorf("invalid argument count in label_replace(): expected 5, got %d", len(args))
	}
	res, err := ev.eval(args[0])
	if err != nil {
		return nil, err
	}
	var (
		dst = args[1].(*parser.StringLiteral).Val
		tpl = args[2].(*parser.StringLiteral).Val
		src = args[3].(*parser.StringLiteral).Val
		r   *regexp.Regexp
	)
	r, err = regexp.Compile("^(?:" + args[4].(*parser.StringLiteral).Val + ")$")
	if err != nil {
		return nil, err
	}
	if len(dst) == 0 {
		return res, fmt.Errorf("invalid destination label name in label_replace(): %s", dst)
	}
	for i := range res {
		for j := range res[i].Data {
			var v string
			if t, ok := res[i].Data[j].Tags.gets(ev, src); ok {
				v = t.SValue
			}
			match := r.FindStringSubmatchIndex(v)
			if len(match) != 0 {
				res[i].AddTagAt(j, &SeriesTag{
					ID:          dst,
					SValue:      string(r.ExpandString([]byte{}, tpl, v, match)),
					stringified: true,
				})
			}
		}
	}
	return res, nil
}

func funcLabelSet(ev *evaluator, args parser.Expressions) ([]Series, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("invalid argument count in label_set(): expected 3, got %d", len(args))
	}
	k := args[1].(*parser.StringLiteral).Val
	v := args[2].(*parser.StringLiteral).Val
	res, err := ev.eval(args[0])
	if err != nil {
		return nil, err
	}
	for i := range res {
		for j := range res[i].Data {
			res[i].AddTagAt(j, &SeriesTag{ID: k, SValue: v, stringified: true})
		}
	}
	return res, nil
}

func funcLabelMinHost(ev *evaluator, args parser.Expressions) ([]Series, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("invalid argument count in label_minhost(): expected 1, got %d", len(args))
	}
	res, err := ev.eval(args[0])
	if err != nil {
		return nil, err
	}
	for i := range res {
		if err = res[i].labelMinMaxHost(ev, 0, LabelMinHost); err != nil {
			return nil, err
		}
	}
	return res, nil
}

func funcLabelMaxHost(ev *evaluator, args parser.Expressions) ([]Series, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("invalid argument count in label_maxhost(): expected 1, got %d", len(args))
	}
	res, err := ev.eval(args[0])
	if err != nil {
		return nil, err
	}
	for i := range res {
		if err = res[i].labelMinMaxHost(ev, 1, LabelMaxHost); err != nil {
			return nil, err
		}
	}
	return res, nil
}

func funcLODStepSec(ev *evaluator, _ parser.Expressions) ([]Series, error) {
	res := make([]Series, len(ev.opt.Offsets))
	for i := range ev.opt.Offsets {
		var (
			j int
			d = SeriesData{Values: ev.alloc()}
		)
		for _, lod := range ev.t.LODs {
			for k := 0; k < lod.Len; k++ {
				(*d.Values)[j+k] = float64(lod.Step)
			}
			j += lod.Len
		}
		res[i] = Series{Data: []SeriesData{d}}
	}
	return res, nil
}

func funcPredictLinear(ev *evaluator, args parser.Expressions) ([]Series, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("invalid argument count in predict_linear(): expected 2, got %d", len(args))
	}
	res, err := ev.eval(args[0])
	if err != nil {
		return nil, err
	}
	d := args[1].(*parser.NumberLiteral).Val // duration
	for i := range res {
		for _, s := range res[i].Data {
			wnd := ev.newWindow(*s.Values, false)
			for wnd.moveOneLeft() {
				if wnd.n != 0 {
					slope, intercept := linearRegression(wnd.get())
					wnd.setValueAtRight(slope*d + intercept)
				} else {
					wnd.setValueAtRight(NilValue)
				}
			}
			wnd.fillPrefixWith(NilValue)
		}
	}
	return res, nil
}

func funcPrefixSum(ev *evaluator, args parser.Expressions) ([]Series, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("invalid argument count in prefix_sum(): expected 1, got %d", len(args))
	}
	res, err := ev.eval(args[0])
	if err != nil {
		return nil, err
	}
	for i := range res {
		res[i] = ev.funcPrefixSum(res[i])
	}
	return res, nil
}

func (ev *evaluator) funcPrefixSum(sr Series) Series {
	for _, d := range sr.Data {
		// skip values before requested interval start
		j := ev.t.ViewStartX
		// skip NANs
		for j < len(*d.Values) && math.IsNaN((*d.Values)[j]) {
			j++
		}
		// sum the rest
		var sum float64
		for ; j < len(*d.Values); j++ {
			v := (*d.Values)[j]
			if !math.IsNaN(v) {
				sum += v
			}
			if j > 0 {
				for k := range d.MinMaxHost {
					if j < len(d.MinMaxHost[k]) && d.MinMaxHost[k][j].Empty() && !d.MinMaxHost[k][j-1].Empty() {
						d.MinMaxHost[k][j] = d.MinMaxHost[k][j-1]
					}
				}
			}
			(*d.Values)[j] = sum
		}
		// copy first value to the left of requested interval
		for j = ev.t.ViewStartX; j > 0; j-- {
			(*d.Values)[j-1] = (*d.Values)[ev.t.ViewStartX]
		}
	}
	return sr
}

func funcRate(ev *evaluator, sr Series) Series {
	t := ev.time()
	for _, s := range sr.Data {
		wnd := ev.newWindow(*s.Values, false)
		for wnd.moveOneLeft() {
			if dt := t[wnd.r] - t[wnd.l] + wnd.s; dt != 0 {
				dv := (*s.Values)[wnd.r]
				if wnd.l > 0 {
					dv -= (*s.Values)[wnd.l-1]
				}
				wnd.setValueAtRight(dv / float64(dt))
			} else {
				wnd.setValueAtRight(NilValue)
			}
		}
		wnd.fillPrefixWith(NilValue)
	}
	return sr
}

func funcResets(ev *evaluator, sr Series) Series {
	for _, d := range sr.Data {
		for i := range *d.Values {
			(*d.Values)[i] = 0
		}
	}
	return sr
}

func funcScalar(ev *evaluator, args parser.Expressions) ([]Series, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("invalid argument count in scalar(): expected 1, got %d", len(args))
	}
	res, err := ev.eval(args[0])
	if err != nil {
		return nil, err
	}
	for i := range res {
		if len(res[i].Data) == 1 {
			continue
		}
		if len(res[i].Data) == 0 {
			res[i].Data = append(res[i].Data, SeriesData{Values: ev.alloc()})
		}
		for j := range *res[i].Data[0].Values {
			(*res[i].Data[0].Values)[j] = math.NaN()
		}
		ev.freeAll(res[i].Data[1:])
	}
	return res, nil
}

func funcTimestamp(ev *evaluator, args parser.Expressions) (res []Series, err error) {
	if len(args) != 0 {
		res, err = ev.eval(args[0])
		if err != nil {
			return res, err
		}
	} else {
		res = make([]Series, len(ev.opt.Offsets))
		for i := 0; i < len(res); i++ {
			res[i] = Series{
				Data: []SeriesData{{
					Values: ev.alloc(),
					Offset: ev.opt.Offsets[i],
				}},
			}
		}
	}
	for i := 0; i < len(res); i++ {
		for j := 0; j < len(res[i].Data); j++ {
			s := *res[i].Data[j].Values
			v := res[i].Data[j].Offset
			for k := 0; k < len(s); k++ {
				s[k] = float64(ev.t.Time[k] - v)
			}
		}
		res[i].Meta.Units = "second"
	}
	return res, nil
}

func funcVector(ev *evaluator, args parser.Expressions) ([]Series, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("invalid argument count in vector(): expected 1, got %d", len(args))
	}
	return ev.eval(args[0])
}

func funcAvgOverTime(s []float64) float64 {
	var (
		sum float64
		cnt int
	)
	for _, v := range s {
		if math.IsNaN(v) {
			continue
		}
		sum += v
		cnt++
	}
	if cnt == 0 {
		return NilValue
	}
	return sum / float64(cnt)
}

func funcMinOverTime(s []float64) float64 {
	var (
		res = math.MaxFloat64
		cnt int
	)
	for _, v := range s {
		if math.IsNaN(v) {
			continue
		}
		if v < res {
			res = v
		}
		cnt++
	}
	if cnt == 0 {
		return NilValue
	}
	return res
}

func funcMaxOverTime(s []float64) float64 {
	var (
		res = -math.MaxFloat64
		cnt int
	)
	for _, v := range s {
		if math.IsNaN(v) {
			continue
		}
		if res < v {
			res = v
		}
		cnt++
	}
	if cnt == 0 {
		return NilValue
	}
	return res
}

func funcSumOverTime(s []float64) float64 {
	var (
		res float64
		cnt int
	)
	for _, v := range s {
		if math.IsNaN(v) {
			continue
		}
		res += v
		cnt++
	}
	if cnt == 0 {
		return NilValue
	}
	return res
}

func funcCountOverTime(s []float64) float64 {
	var res float64
	for _, v := range s {
		if math.IsNaN(v) {
			continue
		}
		res++
	}
	return res
}

func funcQuantileOverTime(ev *evaluator, args parser.Expressions) ([]Series, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("invalid argument count in quantile_over_time(): expected 1, got %d", len(args))
	}
	res, err := ev.eval(args[1])
	if err != nil {
		return nil, err
	}
	var (
		q = args[0].(*parser.NumberLiteral).Val
		v float64
	)
	if math.IsNaN(q) {
		v = math.NaN()
	} else if q < 0 {
		v = math.Inf(-1)
	} else if q > 1 {
		v = math.Inf(1)
	}
	if v != 0 {
		for i := range res {
			for _, s := range res[i].Data {
				for j := range *s.Values {
					(*s.Values)[j] = v
				}
			}
		}
		return res, nil
	}
	for i := range res {
		for _, s := range res[i].Data {
			wnd := ev.newWindow(*s.Values, true)
			for wnd.moveOneLeft() {
				if wnd.n != 0 {
					vs := wnd.getCopyOfValues()
					sort.Float64s(vs)
					var (
						ix = q * (float64(len(vs)) - 1)
						i1 = int(math.Floor(ix))
						i2 = int(math.Min(float64(len(vs)-1), float64(i1+1)))
						w1 = float64(i2) - ix
						w2 = 1 - w1
					)
					wnd.setValueAtRight(vs[i1]*w1 + vs[i2]*w2)
				} else {
					wnd.setValueAtRight(NilValue)
				}
			}
			wnd.fillPrefixWith(NilValue)
		}
	}
	return res, nil
}

func funcPresentOverTime(ev *evaluator, args parser.Expressions) ([]Series, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("invalid argument count in present_over_time(): expected 1, got %d", len(args))
	}
	res, err := ev.eval(args[0])
	if err != nil {
		return nil, err
	}
	for i := range res {
		for _, s := range res[i].Data {
			lastSeen := int64(math.MinInt64)
			for j, t := range ev.time() {
				p := math.Float64bits((*s.Values)[j]) != NilValueBits
				if p || lastSeen < t-ev.r {
					(*s.Values)[j] = 1
					if p {
						lastSeen = t
					}
				} else {
					(*s.Values)[j] = NilValue
				}
			}
		}
	}
	return res, nil
}

func funcStdDevOverTime(s []float64) float64 {
	return math.Sqrt(funcStdVarOverTime(s))
}

func funcStdVarOverTime(s []float64) float64 {
	var cnt int
	var sum float64
	for _, v := range s {
		if !math.IsNaN(v) {
			sum += v
			cnt++
		}
	}
	mean := sum / float64(cnt)
	var res float64
	for _, v := range s {
		if !math.IsNaN(v) {
			d := v - mean
			res += d * d / float64(cnt)
		}
	}
	return res
}

func funcLastOverTime(s []float64) float64 {
	for i := len(s) - 1; i >= 0; i-- {
		if !math.IsNaN(s[i]) {
			return s[i]
		}
	}
	return NilValue
}

func funcPi(ev *evaluator, _ parser.Expressions) ([]Series, error) {
	res := make([]Series, len(ev.opt.Offsets))
	for i := 0; i < len(res); i++ {
		res[i] = Series{
			Data: []SeriesData{{Values: ev.alloc()}},
		}
		s := *res[i].Data[0].Values
		for j := 0; j < len(s); j++ {
			s[j] = math.Pi
		}
	}
	return res, nil
}

// endregion Call

// region BinaryExpr

type (
	sliceBinaryFunc  func([]float64, []float64, []float64)
	scalarBinaryFunc func(float64, float64) float64
	scalarSliceFunc  func(float64, []float64)
	sliceScalarFunc  func([]float64, float64)
)

var sliceBinaryFuncM = map[parser.ItemType][2]sliceBinaryFunc{
	parser.ADD:   {sliceAdd},
	parser.DIV:   {sliceDiv},
	parser.EQLC:  {sliceFilterEqual, sliceEqual},
	parser.GTE:   {sliceFilterGreaterOrEqual, sliceGreaterOrEqual},
	parser.GTR:   {sliceFilterGreater, sliceGreater},
	parser.LSS:   {sliceFilterLess, sliceLess},
	parser.LTE:   {sliceFilterLessOrEqual, sliceLessOrEqual},
	parser.MOD:   {sliceMod},
	parser.MUL:   {sliceMul},
	parser.NEQ:   {sliceFilterNotEqual, sliceNotEqual},
	parser.POW:   {slicePow},
	parser.SUB:   {sliceSub},
	parser.ATAN2: {sliceAtan2},
}

var scalarBinaryFuncM = map[parser.ItemType][2]scalarBinaryFunc{
	parser.ADD:      {scalarAdd},
	parser.LDEFAULT: {scalarDefault},
	parser.DIV:      {scalarDiv},
	parser.EQLC:     {scalarFilterEqual, scalarEqual},
	parser.GTE:      {scalarFilterGreaterOrEqual, scalarGreaterOrEqual},
	parser.GTR:      {scalarFilterGreater, scalarGreater},
	parser.LSS:      {scalarFilterLess, scalarLess},
	parser.LTE:      {scalarFilterLessOrEqual, scalarLessOrEqual},
	parser.MOD:      {math.Mod},
	parser.MUL:      {scalarMul},
	parser.NEQ:      {scalarFilterNotEqual, scalarNotEqual},
	parser.POW:      {math.Pow},
	parser.SUB:      {scalarSub},
	parser.ATAN2:    {math.Atan2},
}

var scalarSliceFuncM = map[parser.ItemType][2]scalarSliceFunc{
	parser.ADD:   {scalarSliceAdd},
	parser.DIV:   {scalarSliceDiv},
	parser.EQLC:  {scalarSliceFilterEqual, scalarSliceEqual},
	parser.GTE:   {scalarSliceFilterGreaterOrEqual, scalarSliceGreaterOrEqual},
	parser.GTR:   {scalarSliceFilterGreater, scalarSliceGreater},
	parser.LSS:   {scalarSliceFilterLess, scalarSliceLess},
	parser.LTE:   {scalarSliceFilterLessOrEqual, scalarSliceLessOrEqual},
	parser.MOD:   {scalarSliceMod},
	parser.MUL:   {scalarSliceMul},
	parser.NEQ:   {scalarSliceFilterNotEqual, scalarSliceNotEqual},
	parser.POW:   {scalarSlicePow},
	parser.SUB:   {scalarSliceSub},
	parser.ATAN2: {scalarSliceAtan2},
}

var sliceScalarFuncM = map[parser.ItemType][2]sliceScalarFunc{
	parser.ADD:      {sliceScalarAdd},
	parser.LDEFAULT: {sliceScalarDefault},
	parser.DIV:      {sliceScalarDiv},
	parser.EQLC:     {sliceScalarFilterEqual, sliceScalarEqual},
	parser.GTE:      {sliceScalarFilterGreaterOrEqual, sliceScalarGreaterOrEqual},
	parser.GTR:      {sliceScalarFilterGreater, sliceScalarGreater},
	parser.LSS:      {sliceScalarFilterLess, sliceScalarLess},
	parser.LTE:      {sliceScalarFilterLessOrEqual, sliceScalarLessOrEqual},
	parser.MOD:      {sliceScalarMod},
	parser.MUL:      {sliceScalarMul},
	parser.NEQ:      {sliceScalarFilterNotEqual, sliceScalarNotEqual},
	parser.POW:      {sliceScalarPow},
	parser.SUB:      {sliceScalarSub},
	parser.ATAN2:    {sliceScalarAtan2},
}

func getBinaryFunc[F sliceBinaryFunc | scalarBinaryFunc | scalarSliceFunc | sliceScalarFunc](m map[parser.ItemType][2]F, op parser.ItemType, b bool) F {
	f := m[op]
	if b && f[1] != nil {
		return f[1]
	} else {
		return f[0]
	}
}

func sliceAdd(dst, lhs, rhs []float64) {
	for i := range lhs {
		dst[i] = lhs[i] + rhs[i]
	}
}

func sliceOr(dst, lhs, rhs []float64) {
	for i := range lhs {
		if math.IsNaN(lhs[i]) {
			dst[i] = rhs[i]
		} else {
			dst[i] = lhs[i]
		}
	}
}

func sliceAnd(dst, lhs, rhs []float64) {
	for i := range lhs {
		if !math.IsNaN(rhs[i]) {
			dst[i] = lhs[i]
		} else {
			dst[i] = NilValue
		}
	}
}

func sliceUnless(dst, lhs, rhs []float64) {
	for i := range lhs {
		if math.IsNaN(rhs[i]) {
			dst[i] = lhs[i]
		} else {
			dst[i] = NilValue
		}
	}
}

func sliceDiv(dst, lhs, rhs []float64) {
	for i := range lhs {
		dst[i] = lhs[i] / rhs[i]
	}
}

func sliceFilterEqual(dst, lhs, rhs []float64) {
	for i := range lhs {
		if lhs[i] == rhs[i] {
			dst[i] = lhs[i]
		} else {
			dst[i] = NilValue
		}
	}
}

func sliceEqual(dst, lhs, rhs []float64) {
	for i := range lhs {
		if lhs[i] == rhs[i] {
			dst[i] = 1
		} else {
			dst[i] = 0
		}
	}
}

func sliceFilterGreaterOrEqual(dst, lhs, rhs []float64) {
	for i := range lhs {
		if lhs[i] >= rhs[i] {
			dst[i] = lhs[i]
		} else {
			dst[i] = NilValue
		}
	}
}

func sliceGreaterOrEqual(dst, lhs, rhs []float64) {
	for i := range lhs {
		if lhs[i] >= rhs[i] {
			dst[i] = 1
		} else {
			dst[i] = 0
		}
	}
}

func sliceFilterGreater(dst, lhs, rhs []float64) {
	for i := range lhs {
		if lhs[i] > rhs[i] {
			dst[i] = lhs[i]
		} else {
			dst[i] = NilValue
		}
	}
}

func sliceGreater(dst, lhs, rhs []float64) {
	for i := range lhs {
		if lhs[i] > rhs[i] {
			dst[i] = 1
		} else {
			dst[i] = 0
		}
	}
}

func sliceFilterLess(dst, lhs, rhs []float64) {
	for i := range lhs {
		if lhs[i] < rhs[i] {
			dst[i] = lhs[i]
		} else {
			dst[i] = NilValue
		}
	}
}

func sliceLess(dst, lhs, rhs []float64) {
	for i := range lhs {
		if lhs[i] < rhs[i] {
			dst[i] = 1
		} else {
			dst[i] = 0
		}
	}
}

func sliceFilterLessOrEqual(dst, lhs, rhs []float64) {
	for i := range lhs {
		if lhs[i] <= rhs[i] {
			dst[i] = lhs[i]
		} else {
			dst[i] = NilValue
		}
	}
}

func sliceLessOrEqual(dst, lhs, rhs []float64) {
	for i := range lhs {
		if lhs[i] <= rhs[i] {
			dst[i] = 1
		} else {
			dst[i] = 0
		}
	}
}

func sliceMod(dst, lhs, rhs []float64) {
	for i := range lhs {
		dst[i] = math.Mod(lhs[i], rhs[i])
	}
}

func sliceMul(dst, lhs, rhs []float64) {
	for i := range lhs {
		dst[i] = lhs[i] * rhs[i]
	}
}

func sliceFilterNotEqual(dst, lhs, rhs []float64) {
	for i := range lhs {
		if lhs[i] != rhs[i] {
			dst[i] = lhs[i]
		} else {
			dst[i] = NilValue
		}
	}
}

func sliceNotEqual(dst, lhs, rhs []float64) {
	for i := range lhs {
		if lhs[i] != rhs[i] {
			dst[i] = 1
		} else {
			dst[i] = 0
		}
	}
}

func slicePow(dst, lhs, rhs []float64) {
	for i := range lhs {
		dst[i] = math.Pow(lhs[i], rhs[i])
	}
}

func sliceSub(dst, lhs, rhs []float64) {
	for i := range lhs {
		dst[i] = lhs[i] - rhs[i]
	}
}

func sliceAtan2(dst, lhs, rhs []float64) {
	for i := range lhs {
		dst[i] = math.Atan2(lhs[i], rhs[i])
	}
}

func scalarAdd(lhs, rhs float64) float64 {
	return lhs + rhs
}

func scalarDefault(lhs, rhs float64) float64 {
	if math.IsNaN(lhs) {
		return rhs
	}
	return lhs
}

func scalarDiv(lhs, rhs float64) float64 {
	return lhs / rhs
}

func scalarFilterEqual(lhs, rhs float64) float64 {
	if lhs == rhs {
		return lhs
	} else {
		return NilValue
	}
}

func scalarEqual(lhs, rhs float64) float64 {
	if lhs == rhs {
		return 1
	} else {
		return 0
	}
}

func scalarFilterGreaterOrEqual(lhs, rhs float64) float64 {
	if lhs >= rhs {
		return lhs
	} else {
		return NilValue
	}
}

func scalarGreaterOrEqual(lhs, rhs float64) float64 {
	if lhs >= rhs {
		return 1
	} else {
		return 0
	}
}

func scalarFilterGreater(lhs, rhs float64) float64 {
	if lhs > rhs {
		return lhs
	} else {
		return NilValue
	}
}

func scalarGreater(lhs, rhs float64) float64 {
	if lhs > rhs {
		return 1
	} else {
		return 0
	}
}

func scalarFilterLess(lhs, rhs float64) float64 {
	if lhs < rhs {
		return lhs
	} else {
		return NilValue
	}
}

func scalarLess(lhs, rhs float64) float64 {
	if lhs < rhs {
		return 1
	} else {
		return 0
	}
}

func scalarFilterLessOrEqual(lhs, rhs float64) float64 {
	if lhs <= rhs {
		return lhs
	} else {
		return NilValue
	}
}

func scalarLessOrEqual(lhs, rhs float64) float64 {
	if lhs <= rhs {
		return 1
	} else {
		return 0
	}
}

func scalarMul(lhs, rhs float64) float64 {
	return lhs * rhs
}

func scalarFilterNotEqual(lhs, rhs float64) float64 {
	if lhs != rhs {
		return lhs
	} else {
		return NilValue
	}
}

func scalarNotEqual(lhs, rhs float64) float64 {
	if lhs != rhs {
		return 1
	} else {
		return 0
	}
}

func scalarSub(lhs, rhs float64) float64 {
	return lhs - rhs
}

func scalarSliceAdd(lhs float64, rhs []float64) {
	for i := range rhs {
		rhs[i] += lhs
	}
}

func scalarSliceDiv(lhs float64, rhs []float64) {
	for i := range rhs {
		rhs[i] = lhs / rhs[i]
	}
}

func scalarSliceFilterEqual(lhs float64, rhs []float64) {
	for i := range rhs {
		if lhs != rhs[i] {
			rhs[i] = NilValue
		}
	}
}

func scalarSliceEqual(lhs float64, rhs []float64) {
	for i := range rhs {
		if lhs == rhs[i] {
			rhs[i] = 1
		} else {
			rhs[i] = 0
		}
	}
}

func scalarSliceFilterGreaterOrEqual(lhs float64, rhs []float64) {
	for i := range rhs {
		if lhs < rhs[i] {
			rhs[i] = NilValue
		}
	}
}

func scalarSliceGreaterOrEqual(lhs float64, rhs []float64) {
	for i := range rhs {
		if lhs >= rhs[i] {
			rhs[i] = 1
		} else {
			rhs[i] = 0
		}
	}
}

func scalarSliceFilterGreater(lhs float64, rhs []float64) {
	for i := range rhs {
		if lhs <= rhs[i] {
			rhs[i] = NilValue
		}
	}
}

func scalarSliceGreater(lhs float64, rhs []float64) {
	for i := range rhs {
		if lhs > rhs[i] {
			rhs[i] = 1
		} else {
			rhs[i] = 0
		}
	}
}

func scalarSliceFilterLess(lhs float64, rhs []float64) {
	for i := range rhs {
		if lhs >= rhs[i] {
			rhs[i] = NilValue
		}
	}
}

func scalarSliceLess(lhs float64, rhs []float64) {
	for i := range rhs {
		if lhs < rhs[i] {
			rhs[i] = 1
		} else {
			rhs[i] = 0
		}
	}
}

func scalarSliceFilterLessOrEqual(lhs float64, rhs []float64) {
	for i := range rhs {
		if lhs > rhs[i] {
			rhs[i] = NilValue
		}
	}
}

func scalarSliceLessOrEqual(lhs float64, rhs []float64) {
	for i := range rhs {
		if lhs <= rhs[i] {
			rhs[i] = 1
		} else {
			rhs[i] = 0
		}
	}
}

func scalarSliceMod(lhs float64, rhs []float64) {
	for i := range rhs {
		rhs[i] = math.Mod(lhs, rhs[i])
	}
}

func scalarSliceMul(lhs float64, rhs []float64) {
	for i := range rhs {
		rhs[i] *= lhs
	}
}

func scalarSliceFilterNotEqual(lhs float64, rhs []float64) {
	for i := range rhs {
		if lhs == rhs[i] {
			rhs[i] = NilValue
		}
	}
}

func scalarSliceNotEqual(lhs float64, rhs []float64) {
	for i := range rhs {
		if lhs != rhs[i] {
			rhs[i] = 1
		} else {
			rhs[i] = 0
		}
	}
}

func scalarSlicePow(lhs float64, rhs []float64) {
	for i := range rhs {
		rhs[i] = math.Pow(lhs, rhs[i])
	}
}

func scalarSliceSub(lhs float64, rhs []float64) {
	for i := range rhs {
		rhs[i] = lhs - rhs[i]
	}
}

func scalarSliceAtan2(lhs float64, rhs []float64) {
	for i := range rhs {
		rhs[i] = math.Atan2(lhs, rhs[i])
	}
}

func sliceScalarAdd(lhs []float64, rhs float64) {
	for i := range lhs {
		lhs[i] += rhs
	}
}

func sliceScalarDefault(lhs []float64, rhs float64) {
	for i := range lhs {
		if math.IsNaN(lhs[i]) {
			lhs[i] = rhs
		}
	}
}

func sliceScalarDiv(lhs []float64, rhs float64) {
	for i := range lhs {
		lhs[i] /= rhs
	}
}

func sliceScalarFilterEqual(lhs []float64, rhs float64) {
	for i := range lhs {
		if lhs[i] != rhs {
			lhs[i] = NilValue
		}
	}
}

func sliceScalarEqual(lhs []float64, rhs float64) {
	for i := range lhs {
		if lhs[i] == rhs {
			lhs[i] = 1
		} else {
			lhs[i] = 0
		}
	}
}

func sliceScalarFilterGreaterOrEqual(lhs []float64, rhs float64) {
	for i := range lhs {
		if lhs[i] < rhs {
			lhs[i] = NilValue
		}
	}
}

func sliceScalarGreaterOrEqual(lhs []float64, rhs float64) {
	for i := range lhs {
		if lhs[i] >= rhs {
			lhs[i] = 1
		} else {
			lhs[i] = 0
		}
	}
}

func sliceScalarFilterGreater(lhs []float64, rhs float64) {
	for i := range lhs {
		if lhs[i] <= rhs {
			lhs[i] = NilValue
		}
	}
}

func sliceScalarGreater(lhs []float64, rhs float64) {
	for i := range lhs {
		if lhs[i] > rhs {
			lhs[i] = 1
		} else {
			lhs[i] = 0
		}
	}
}

func sliceScalarFilterLess(lhs []float64, rhs float64) {
	for i := range lhs {
		if lhs[i] >= rhs {
			lhs[i] = NilValue
		}
	}
}

func sliceScalarLess(lhs []float64, rhs float64) {
	for i := range lhs {
		if lhs[i] < rhs {
			lhs[i] = 1
		} else {
			lhs[i] = 0
		}
	}
}

func sliceScalarFilterLessOrEqual(lhs []float64, rhs float64) {
	for i := range lhs {
		if lhs[i] > rhs {
			lhs[i] = NilValue
		}
	}
}

func sliceScalarLessOrEqual(lhs []float64, rhs float64) {
	for i := range lhs {
		if lhs[i] <= rhs {
			lhs[i] = 1
		} else {
			lhs[i] = 0
		}
	}
}

func sliceScalarMod(lhs []float64, rhs float64) {
	for i := range lhs {
		lhs[i] = math.Mod(lhs[i], rhs)
	}
}

func sliceScalarMul(lhs []float64, rhs float64) {
	for i := range lhs {
		lhs[i] *= rhs
	}
}

func sliceScalarFilterNotEqual(lhs []float64, rhs float64) {
	for i := range lhs {
		if lhs[i] == rhs {
			lhs[i] = NilValue
		}
	}
}

func sliceScalarNotEqual(lhs []float64, rhs float64) {
	for i := range lhs {
		if lhs[i] != rhs {
			lhs[i] = 1
		} else {
			lhs[i] = 0
		}
	}
}

func sliceScalarPow(lhs []float64, rhs float64) {
	for i := range lhs {
		lhs[i] = math.Pow(lhs[i], rhs)
	}
}

func sliceScalarSub(lhs []float64, rhs float64) {
	for i := range lhs {
		lhs[i] -= rhs
	}
}

func sliceScalarAtan2(lhs []float64, rhs float64) {
	for i := range lhs {
		lhs[i] = math.Atan2(lhs[i], rhs)
	}
}

// endregion BinaryExpr
