// Copyright 2022 V Kontakte LLC
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
	"github.com/vkcom/statshouse/internal/promql/parser"
	"github.com/vkcom/statshouse/internal/util"
)

// region AggregateExpr

type aggregateFunc func(*evaluator, *parser.AggregateExpr) ([]Series, error)
type seriesGroupFunc func(*evaluator, seriesGroup, parser.Expr) Series

var aggregates map[parser.ItemType]aggregateFunc

func init() {
	aggregates = map[parser.ItemType]aggregateFunc{
		parser.AVG:          evalAndGroup(simpleAggregate(funcAvg)),
		parser.BOTTOMK:      funcTopK,
		parser.COUNT:        evalAndGroup(simpleAggregate(funcCount)),
		parser.COUNT_VALUES: evalAndGroup(funcCountValues),
		parser.GROUP:        evalAndGroup(simpleAggregate(funcGroup)),
		parser.MAX:          evalAndGroup(simpleAggregate(funcMax)),
		parser.MIN:          evalAndGroup(simpleAggregate(funcMin)),
		parser.QUANTILE:     evalAndGroup(funcQuantile),
		parser.STDDEV:       evalAndGroup(simpleAggregate(funcStdDev)),
		parser.STDVAR:       evalAndGroup(simpleAggregate(funcStdVar)),
		parser.SUM:          evalAndGroup(simpleAggregate(funcSum)),
		parser.TOPK:         funcTopK,
	}
}

func evalAndGroup(fn seriesGroupFunc) aggregateFunc {
	return func(ev *evaluator, expr *parser.AggregateExpr) ([]Series, error) {
		bag, err := ev.eval(expr.Expr)
		if err != nil {
			return nil, err
		}
		for x := range bag {
			if len(bag[x].Data) == 0 {
				continue
			}
			var groups []seriesGroup
			groups, err := bag[x].group(ev, hashOptions{
				on:   !expr.Without,
				tags: expr.Grouping,
			})
			if err != nil {
				return nil, err
			}
			res := ev.newSeries(len(groups))
			for i, g := range groups {
				res.append(fn(ev, g, expr.Param))
				res.Data[i].MaxHost = g.groupMaxHost(ev)
			}
			bag[x] = res
		}
		return bag, nil
	}
}

func simpleAggregate(fn func([]SeriesData, int)) seriesGroupFunc {
	return func(ev *evaluator, g seriesGroup, _ parser.Expr) Series {
		if len(g.Data) == 0 {
			return ev.newSeries(0)
		}
		fn(g.Data, len(ev.time()))
		ev.free(g.Data[1:])
		return g.at(0)
	}
}

func funcAvg(s []SeriesData, n int) {
	for j := 0; j < n; j++ {
		var (
			res float64
			cnt int
		)
		for i := 0; i < len(s); i++ {
			v := (*s[i].Values)[j]
			if math.IsNaN(v) {
				continue
			}
			res += v
			cnt++
		}
		if cnt != 0 {
			(*s[0].Values)[j] = res / float64(cnt)
		} else {
			(*s[0].Values)[j] = math.NaN()
		}
	}
}

func funcCount(data []SeriesData, n int) {
	for j := 0; j < n; j++ {
		var res int
		for _, s := range data {
			if !math.IsNaN((*s.Values)[j]) {
				res++
			}
		}
		(*data[0].Values)[j] = float64(res)
	}
}

func funcCountValues(ev *evaluator, g seriesGroup, p parser.Expr) Series {
	m := make(map[float64]SeriesData)
	for _, s := range g.Data {
		for i, v := range *s.Values {
			s, ok := m[v]
			if !ok {
				s = ev.alloc()
				m[v] = s
			}
			(*s.Values)[i]++
		}
	}
	res := Series{Meta: g.Meta}
	for v, data := range m {
		for i := range *data.Values {
			if (*data.Values)[i] == 0 {
				(*data.Values)[i] = math.NaN()
			}
		}
		data.Tags.add(
			&SeriesTag{
				ID:     p.(*parser.StringLiteral).Val,
				SValue: strconv.FormatFloat(v, 'f', -1, 64)},
			&res.Meta)
		res.Data = append(res.Data, data)
	}
	g.free(ev)
	return res
}

func funcGroup(s []SeriesData, n int) {
	for j := 0; j < n; j++ {
		(*s[0].Values)[j] = 1
	}
}

func funcMax(data []SeriesData, n int) {
	for j := 0; j < n; j++ {
		var (
			res = math.NaN()
			nan = true
		)
		for _, s := range data {
			v := (*s.Values)[j]
			if math.IsNaN(v) {
				continue
			}
			if nan || res < v {
				res = v
				nan = false
			}
		}
		(*data[0].Values)[j] = res
	}
}

func funcMin(data []SeriesData, n int) {
	for j := 0; j < n; j++ {
		var (
			res = math.NaN()
			nan = true
		)
		for _, s := range data {
			v := (*s.Values)[j]
			if math.IsNaN(v) {
				continue
			}
			if nan || v < res {
				res = v
				nan = false
			}
		}
		(*data[0].Values)[j] = res
	}
}

func funcQuantile(ev *evaluator, g seriesGroup, p parser.Expr) Series {
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
	var (
		res = *g.Data[0].Values
		t   = ev.time()
	)
	if !valid {
		for i := 0; i < len(t); i++ {
			res[i] = q
		}
	} else {
		var (
			x  = make([]int, len(g.Data))
			ix = q * (float64(len(g.Data)) - 1)
			i1 = int(math.Floor(ix))
			i2 = int(math.Min(float64(len(g.Data)-1), float64(i1+1)))
			w1 = float64(i2) - ix
			w2 = 1 - w1
		)
		for i := range x {
			x[i] = i
		}
		for i := 0; i < len(t); i++ {
			sort.Slice(x, func(j, k int) bool { return (*g.Data[x[j]].Values)[i] < (*g.Data[x[k]].Values)[i] })
			res[i] = (*g.Data[x[i1]].Values)[i]*w1 + (*g.Data[x[i2]].Values)[i]*w2
		}
	}
	ev.free(g.Data[1:])
	return g.at(0)
}

func funcStdDev(s []SeriesData, n int) {
	funcStdVar(s, n)
	for j := 0; j < n; j++ {
		(*s[0].Values)[j] = math.Sqrt((*s[0].Values)[j])
	}
}

func funcStdVar(data []SeriesData, n int) {
	for j := 0; j < n; j++ {
		var cnt int
		var sum float64
		for _, s := range data {
			v := (*s.Values)[j]
			if !math.IsNaN(v) {
				sum += v
				cnt++
			}
		}
		mean := sum / float64(cnt)
		var res float64
		for _, s := range data {
			v := (*s.Values)[j]
			if !math.IsNaN(v) {
				d := v - mean
				res += d * d / float64(cnt)
			}
		}
		(*data[0].Values)[j] = res
	}
}

func funcSum(s []SeriesData, n int) {
	for j := 0; j < n; j++ {
		var (
			res = math.NaN()
			nan = true
		)
		for i := 0; i < len(s); i++ {
			v := (*s[i].Values)[j]
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
		(*s[0].Values)[j] = res
	}
}

func funcTopK(ev *evaluator, expr *parser.AggregateExpr) ([]Series, error) {
	k := int(expr.Param.(*parser.NumberLiteral).Val)
	if k <= 0 {
		return make([]Series, len(ev.opt.Offsets)), nil
	}
	bags, err := ev.eval(expr.Expr)
	if err != nil {
		return nil, err
	}
	type (
		sortedSeriesBag struct {
			Series
			k int
			w []float64 // weights, not sorted
			x []int     // index, sorted according to weights
		}
		bucket struct {
			topK map[uint64][]int
			bags []sortedSeriesBag
		}
	)
	var (
		buckets = make(map[uint64]*bucket)
		desc    bool
		sort    = func(bag Series) sortedSeriesBag {
			var (
				w = bag.weight(ev)
				x = make([]int, len(bag.Data))
				n = k
			)
			if len(bag.Data) < n {
				n = len(bag.Data)
			}
			for i := range x {
				x[i] = i
			}
			if desc {
				util.PartialSortIndexByValueDesc(x, w, n, ev.opt.Rand, nil)
			} else {
				util.PartialSortIndexByValueAsc(x, w, n, ev.opt.Rand, nil)
			}
			return sortedSeriesBag{bag, n, w, x}
		}
		swap = func(bag *sortedSeriesBag, i, j int) {
			bag.x[i], bag.x[j] = bag.x[j], bag.x[i]
		}
		less = func(bag *sortedSeriesBag, i, j int) bool {
			if desc {
				return bag.w[bag.x[i]] > bag.w[bag.x[j]]
			} else {
				return bag.w[bag.x[i]] < bag.w[bag.x[j]]
			}
		}
	)
	if expr.Op == parser.TOPK {
		desc = true
	}
	for x := range bags {
		if len(bags[x].Data) == 0 {
			continue
		}
		var groups []seriesGroup
		groups, err := bags[x].group(ev, hashOptions{
			on:   !expr.Without,
			tags: expr.Grouping,
		})
		if err != nil {
			return nil, err
		}
		for _, g := range groups {
			b := buckets[g.hash]
			if b == nil {
				b = &bucket{
					topK: make(map[uint64][]int),
					bags: make([]sortedSeriesBag, len(bags)),
				}
				buckets[g.hash] = b
			}
			b.bags[x] = sort(g.Series)
			for i := 0; i < b.bags[x].k; i++ {
				sum, _, err := b.bags[x].Data[b.bags[x].x[i]].Tags.hash(ev, hashOptions{}, false)
				if err != nil {
					return nil, err
				}
				y := b.topK[sum]
				if y == nil {
					y = make([]int, len(bags))
					b.topK[sum] = y
				}
				y[x]++
			}
		}
	}
	for _, bucket := range buckets {
		if len(bucket.topK) <= k {
			// top is consistent across shifts
			continue
		}
		for sum, xs := range bucket.topK {
			for x := range bucket.bags {
				if xs[x] != 0 {
					// top is within shift top
					continue
				}
				for i := bucket.bags[x].k; bucket.bags[x].k < len(bucket.topK) && i < len(bucket.bags[x].Data); i++ {
					v, _, err := bucket.bags[x].Data[bucket.bags[x].x[i]].Tags.hash(ev, hashOptions{}, false)
					if err != nil {
						return nil, err
					}
					if v == sum {
						// found top from another shift
						swap(&bucket.bags[x], bucket.bags[x].k, i)
						// keep ordering
						for j := bucket.bags[x].k; j > k && less(&bucket.bags[x], j, j-1); j-- {
							swap(&bucket.bags[x], j, j-1)
						}
						// bump number of sorted series
						bucket.bags[x].k++
					}
				}
			}
		}
	}
	for x := range bags {
		bags[x] = ev.newSeries(0)
	}
	for i := range buckets {
		for x, bag := range buckets[i].bags {
			bags[x].appendX(bag.Series, bag.x[:bag.k]...)
			ev.freeAt(bag.Data, bag.x[bag.k:]...)
		}
	}
	return bags, nil
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
		"changes":          overTimeCall(funcChanges, 0),
		"clamp":            funcClamp,
		"clamp_max":        funcClampMax,
		"clamp_min":        funcClampMin,
		"day_of_month":     timeCall(time.Time.Day),
		"day_of_week":      timeCall(time.Time.Weekday),
		"day_of_year":      timeCall(time.Time.YearDay),
		"days_in_month":    timeCall(func(t time.Time) int { return 32 - time.Date(t.Year(), t.Month(), 32, 0, 0, 0, 0, t.Location()).Day() }),
		"delta":            bagCall(funcDelta),
		"deriv":            bagCall(funcDeriv),
		"exp":              simpleCall(math.Exp),
		"floor":            simpleCall(math.Floor),
		// "histogram_count": ?
		// "histogram_sum": ?
		// "histogram_fraction": ?
		"histogram_quantile": funcHistogramQuantile,
		"holt_winters":       funcHoltWinters,
		"hour":               timeCall(time.Time.Hour),
		"idelta":             bagCall(funcIdelta),
		"increase":           bagCall(funcDelta),
		"irate":              bagCall(funcIrate),
		"label_join":         funcLabelJoin,
		"label_replace":      funcLabelReplace,
		"ln":                 simpleCall(math.Log),
		"log2":               simpleCall(math.Log2),
		"log10":              simpleCall(math.Log10),
		"lod_step_sec":       funcLODStepSec,
		"minute":             timeCall(time.Time.Minute),
		"month":              timeCall(time.Time.Month),
		"predict_linear":     funcPredictLinear,
		"prefix_sum":         funcPrefixSum,
		"rate":               bagCall(funcRate),
		"resets":             bagCall(funcResets),
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
		// "sort": ?
		// "sort_desc": ?
		"sqrt":               simpleCall(math.Sqrt),
		"time":               generatorCall(funcTime),
		"timestamp":          bagCall(funcTimestamp),
		"vector":             funcVector,
		"year":               timeCall(time.Time.Year),
		"avg_over_time":      overTimeCall(funcAvgOverTime, NilValue),
		"min_over_time":      overTimeCall(funcMinOverTime, NilValue),
		"max_over_time":      overTimeCall(funcMaxOverTime, NilValue),
		"sum_over_time":      overTimeCall(funcSumOverTime, NilValue),
		"count_over_time":    overTimeCall(funcCountOverTime, 0),
		"quantile_over_time": funcQuantileOverTime,
		"stddev_over_time":   overTimeCall(funcStdDevOverTime, NilValue),
		"stdvar_over_time":   overTimeCall(funcStdVarOverTime, NilValue),
		"last_over_time":     nopCall,
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
		"pi":                 generatorCall(funcPi),
		"rad":                simpleCall(func(v float64) float64 { return v * math.Pi / 180 }),
	}
}

type window struct {
	t, ct   []int64   // time, current interval time
	v, cv   []float64 // values, current interval values
	w       int64     // target width
	l, r, n int       // current [l,r] interval, number of not NaN values inside
	strict  bool      // don't stretch to LOD resolution if set
	done    bool      // next "moveOneLeft" returns false if set
}

func newWindow(t []int64, v []float64, w int64, s bool) window {
	return window{t: t, v: v, w: w, strict: s, l: len(t), r: len(t), done: len(t) == 0}
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
		if math.IsNaN(wnd.v[l]) {
			n = 0
		} else {
			n = 1
		}
	}
	for 0 < l {
		if 0 < wnd.w {
			if wnd.w <= wnd.t[r]-wnd.t[l] {
				break
			}
			if wnd.strict && wnd.w < wnd.t[r]-wnd.t[l-1] {
				break
			}
		} else if l != r {
			break
		}
		l--
		if !math.IsNaN(wnd.v[l]) {
			n++
		}
	}
	if l <= 0 {
		wnd.done = true
		if wnd.w <= 0 {
			if l == r {
				return false
			}
		} else if wnd.t[r]-wnd.t[l] < wnd.w {
			return false
		}
	}
	wnd.l, wnd.r, wnd.n = l, r, n
	return true
}

func (wnd *window) get() ([]int64, []float64) {
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

func bagCall(fn func(*evaluator, Series) Series) callFunc {
	return func(ev *evaluator, args parser.Expressions) ([]Series, error) {
		bag, err := ev.eval(args[0])
		if err != nil {
			return nil, err
		}
		for x := range bag {
			bag[x] = fn(ev, bag[x])
		}
		return bag, nil
	}
}

func generatorCall(fn func(ev *evaluator, args parser.Expressions) Series) callFunc {
	return func(ev *evaluator, args parser.Expressions) ([]Series, error) {
		bag := make([]Series, len(ev.opt.Offsets))
		for x := range ev.opt.Offsets {
			bag[x] = fn(ev, args)
		}
		return bag, nil
	}
}

func nopCall(ev *evaluator, args parser.Expressions) ([]Series, error) {
	return ev.eval(args[0])
}

func overTimeCall(fn func(v []float64) float64, nilValue float64) callFunc {
	return func(ev *evaluator, args parser.Expressions) ([]Series, error) {
		bag, err := ev.eval(args[0])
		if err != nil {
			return nil, err
		}
		for x := range bag {
			for _, s := range bag[x].Data {
				wnd := ev.newWindow(*s.Values, true)
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
		return bag, nil
	}
}

func simpleCall(fn func(float64) float64) callFunc {
	return func(ev *evaluator, args parser.Expressions) ([]Series, error) {
		bag, err := ev.eval(args[0])
		if err != nil {
			return nil, err
		}
		for x := range bag {
			for _, s := range bag[x].Data {
				for i := range *s.Values {
					(*s.Values)[i] = fn((*s.Values)[i])
				}
			}
		}
		return bag, nil
	}
}

func timeCall[V int | time.Weekday | time.Month](fn func(time.Time) V) callFunc {
	return func(ev *evaluator, args parser.Expressions) (bag []Series, err error) {
		if len(args) != 0 {
			bag, err = ev.eval(args[0])
			if err != nil {
				return nil, err
			}
		}
		for x := range bag {
			if bag[x].Data == nil {
				bag[x] = funcTime(ev, args)
			}
			for _, s := range bag[x].Data {
				for i, v := range *s.Values {
					(*s.Values)[i] = float64(fn(time.Unix(int64(v), 0).In(ev.loc)))
				}
			}
		}
		return bag, nil
	}
}

func funcAbsent(ev *evaluator, args parser.Expressions) ([]Series, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("invalid argument count in absent(): expected 1, got %d", len(args))
	}
	bag, err := ev.eval(args[0])
	if err != nil {
		return nil, err
	}
	for x := range bag {
		var (
			s SeriesData // absent row
			n int        // absent count
		)
		if len(bag[x].Data) != 0 {
			s = bag[x].Data[0]
			for i := range ev.time() {
				var m int
				for _, s := range bag[x].Data {
					if math.Float64bits((*s.Values)[i]) != NilValueBits {
						m++
					}
				}
				if m == 0 {
					(*s.Values)[i] = 1
					n++
				} else {
					(*s.Values)[i] = NilValue
				}
			}
			ev.free(bag[x].Data[1:])
		} else {
			s = ev.alloc()
			n = len(*s.Values)
			for i := range *s.Values {
				(*s.Values)[i] = 1
			}
		}
		bag[x] = Series{
			Data: []SeriesData{s},
			Meta: bag[x].Meta,
		}
		if sel, ok := args[0].(*parser.VectorSelector); ok && n != 0 {
			for _, m := range sel.LabelMatchers {
				if m.Type == labels.MatchEqual {
					bag[x].AddTagAt(0, &SeriesTag{ID: m.Name, SValue: m.Value})
				}
			}
		}
	}
	return bag, nil
}

func funcAbsentOverTime(ev *evaluator, args parser.Expressions) ([]Series, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("invalid argument count in absent_over_time(): expected 1, got %d", len(args))
	}
	bag, err := ev.eval(args[0])
	if err != nil {
		return nil, err
	}
	for x := range bag {
		var (
			s SeriesData // absent row
			n int        // absent count
		)
		if len(bag[x].Data) != 0 {
			s = bag[x].Data[0]
			lastSeen := int64(math.MinInt64)
			for i, t := range ev.time() {
				var m int
				for _, s := range bag[x].Data {
					if math.Float64bits((*s.Values)[i]) != NilValueBits {
						m++
					}
				}
				if m == 0 && lastSeen < t-ev.r {
					(*s.Values)[i] = 1
					n++
				} else {
					(*s.Values)[i] = NilValue
					lastSeen = t
				}
			}
			ev.free(bag[x].Data[1:])
		} else {
			s = ev.alloc()
			n = len(*s.Values)
			for i := range *s.Values {
				(*s.Values)[i] = 1
			}
		}
		bag[x] = Series{
			Data: []SeriesData{s},
			Meta: bag[x].Meta,
		}
		if sel, ok := args[0].(*parser.VectorSelector); ok && n != 0 {
			for _, m := range sel.LabelMatchers {
				if m.Type == labels.MatchEqual {
					bag[x].AddTagAt(0, &SeriesTag{ID: m.Name, SValue: m.Value})
				}
			}
		}
	}
	return bag, nil
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
	bag, err := ev.eval(args[0])
	if err != nil {
		return nil, err
	}
	min := args[1].(*parser.NumberLiteral).Val
	max := args[2].(*parser.NumberLiteral).Val
	// return NaN if min or max is NaN
	if math.IsNaN(min) || math.IsNaN(max) {
		for x := range bag {
			for _, s := range bag[x].Data {
				for i := range *s.Values {
					(*s.Values)[i] = math.NaN()
				}
			}
		}
		return bag, nil
	}
	// return an empty vector if min > max
	if min > max {
		for x := range bag {
			bag[x].free(ev)
			bag[x] = Series{}
		}
		return bag, nil
	}
	for x := range bag {
		for _, s := range bag[x].Data {
			for i := range *s.Values {
				if (*s.Values)[i] < min {
					(*s.Values)[i] = min
				} else if (*s.Values)[i] > max {
					(*s.Values)[i] = max
				}
				// else { min <= (*s.Data)[i] <= max }
			}
		}
	}
	return bag, nil
}

func funcClampMax(ev *evaluator, args parser.Expressions) ([]Series, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("invalid argument count in clamp_max(): expected 2, got %d", len(args))
	}
	bag, err := ev.eval(args[0])
	if err != nil {
		return nil, err
	}
	max := args[1].(*parser.NumberLiteral).Val
	// return NaN if max is NaN
	if math.IsNaN(max) {
		for x := range bag {
			for _, s := range bag[x].Data {
				for i := range *s.Values {
					(*s.Values)[i] = math.NaN()
				}
			}
		}
		return bag, nil
	}
	for x := range bag {
		for _, s := range bag[x].Data {
			for i := range *s.Values {
				if (*s.Values)[i] > max {
					(*s.Values)[i] = max
				}
				// else { (*s.Data)[i] <= max }
			}
		}
	}
	return bag, nil
}

func funcClampMin(ev *evaluator, args parser.Expressions) ([]Series, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("invalid argument count in clamp_min(): expected 2, got %d", len(args))
	}
	bag, err := ev.eval(args[0])
	if err != nil {
		return nil, err
	}
	min := args[1].(*parser.NumberLiteral).Val
	// return NaN if min is NaN
	if math.IsNaN(min) {
		for x := range bag {
			for _, s := range bag[x].Data {
				for i := range *s.Values {
					(*s.Values)[i] = math.NaN()
				}
			}
		}
		return bag, nil
	}
	for x := range bag {
		for _, s := range bag[x].Data {
			for i := range *s.Values {
				if (*s.Values)[i] < min {
					(*s.Values)[i] = min
				}
				// else { (*s.Data)[i] >= min }
			}
		}
	}
	return bag, nil
}

func funcDelta(ev *evaluator, bag Series) Series {
	bag = funcRate(ev, bag)
	if ev.r == 0 {
		return bag
	}
	for _, s := range bag.Data {
		for i := range *s.Values {
			(*s.Values)[i] *= float64(ev.r)
		}
	}
	return bag
}

func funcDeriv(ev *evaluator, bag Series) Series {
	for _, s := range bag.Data {
		wnd := ev.newWindow(*s.Values, false)
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
	return bag
}

func funcIdelta(ev *evaluator, bag Series) Series {
	for _, s := range bag.Data {
		for i := len(*s.Values) - 1; i > 0; i-- {
			(*s.Values)[i] = (*s.Values)[i] - (*s.Values)[i-1]
		}
		(*s.Values)[0] = NilValue
	}
	return bag
}

func funcIrate(ev *evaluator, bag Series) Series {
	t := ev.time()
	for _, s := range bag.Data {
		for i := len(*s.Values) - 1; i > 0; i-- {
			(*s.Values)[i] = ((*s.Values)[i] - (*s.Values)[i-1]) / float64(t[i]-t[i-1])
		}
		(*s.Values)[0] = NilValue
	}
	return bag
}

func funcHistogramQuantile(ev *evaluator, args parser.Expressions) ([]Series, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("invalid argument count in histogram_quantile(): expected 2, got %d", len(args))
	}
	bag, err := ev.eval(args[1])
	if err != nil {
		return nil, err
	}
	for x := range bag {
		hs, err := bag[x].histograms(ev)
		if err != nil {
			return nil, err
		}
		res := ev.newSeries(len(hs))
		for _, h := range hs {
			d := h.group.Data
			s := *d[0].Values
			if len(h.buckets) < 2 {
				for i := range s {
					s[i] = NilValue
				}
			} else {
				q := args[0].(*parser.NumberLiteral).Val // quantile
				for i := range s {
					total := (*d[h.buckets[len(h.buckets)-1].x].Values)[i]
					if total == 0 {
						s[i] = NilValue
						continue
					}
					rank := q * total
					var j int // upper bound index
					for j < len(h.buckets)-1 && (*d[h.buckets[j].x].Values)[i] < rank {
						j++
					}
					var v float64
					switch j {
					case 0: // lower bound is -inf
						v = float64(h.buckets[0].le)
					case len(h.buckets) - 1: // upper bound is +inf
						v = float64(h.buckets[len(h.buckets)-2].le)
					default:
						var (
							lo    = h.buckets[j-1].le                // lower bound
							count = (*d[h.buckets[j-1].x].Values)[i] // lower bound count
						)
						if rank == count {
							v = float64(lo)
						} else {
							hi := h.buckets[j].le // upper bound
							v = float64(lo) + float64(hi-lo)/(rank-count)
						}
					}
					s[i] = v
				}
			}
			ev.free(d[1:])
			res.append(h.group.at(0))
		}
		bag[x] = res
	}
	return bag, nil
}

func funcLabelJoin(ev *evaluator, args parser.Expressions) ([]Series, error) {
	if len(args) < 3 {
		return nil, fmt.Errorf("invalid argument count in label_join(): expected at least 3, got %d", len(args))
	}
	bag, err := ev.eval(args[0])
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
	for x := range bag {
		for i, m := range bag[x].Data {
			var s []string
			for k := range src {
				if t, ok := m.Tags.gets(ev, k); ok && len(t.SValue) != 0 {
					s = append(s, t.SValue)
				}
			}
			if len(s) != 0 {
				bag[x].AddTagAt(i, &SeriesTag{ID: dst, SValue: strings.Join(s, sep)})
			}
		}
	}
	return bag, nil
}

func funcLabelReplace(ev *evaluator, args parser.Expressions) ([]Series, error) {
	if len(args) != 5 {
		return nil, fmt.Errorf("invalid argument count in label_replace(): expected 5, got %d", len(args))
	}
	bag, err := ev.eval(args[0])
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
		return bag, fmt.Errorf("invalid destination label name in label_replace(): %s", dst)
	}
	for x := range bag {
		for i := range bag[x].Data {
			var v string
			if t, ok := bag[x].Data[i].Tags.gets(ev, src); ok {
				v = t.SValue
			}
			match := r.FindStringSubmatchIndex(v)
			if len(match) != 0 {
				v = string(r.ExpandString([]byte{}, tpl, v, match))
				if len(v) != 0 {
					bag[x].AddTagAt(i, &SeriesTag{ID: dst, SValue: v})
				} else if i < len(bag[x].Data) {
					bag[x].Data[i].Tags.remove(dst)
				}
			}
		}
	}
	return bag, nil
}

func funcLODStepSec(ev *evaluator, _ parser.Expressions) ([]Series, error) {
	bag := make([]Series, len(ev.opt.Offsets))
	for x := range ev.opt.Offsets {
		var (
			i int
			s = ev.alloc()
		)
		for _, lod := range ev.t.LODs {
			for k := 0; k < lod.Len; k++ {
				(*s.Values)[i+k] = float64(lod.Step)
			}
			i += lod.Len
		}
		bag[x] = Series{Data: []SeriesData{s}}
	}
	return bag, nil
}

func funcPredictLinear(ev *evaluator, args parser.Expressions) ([]Series, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("invalid argument count in predict_linear(): expected 2, got %d", len(args))
	}
	bag, err := ev.eval(args[0])
	if err != nil {
		return nil, err
	}
	d := args[1].(*parser.NumberLiteral).Val // duration
	for x := range bag {
		for _, s := range bag[x].Data {
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
	return bag, nil
}

func funcPrefixSum(ev *evaluator, args parser.Expressions) ([]Series, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("invalid argument count in prefix_sum(): expected 1, got %d", len(args))
	}
	bag, err := ev.eval(args[0])
	if err != nil {
		return nil, err
	}
	for x := range bag {
		bag[x] = ev.funcPrefixSum(bag[x])
	}
	return bag, nil
}

func (ev *evaluator) funcPrefixSum(bag Series) Series {
	var (
		i int
		t = ev.time()
	)
	for t[i] < ev.t.Start {
		i++
	}
	for _, s := range bag.Data {
		var j int
		for ; j < i && j < len(*s.Values); j++ {
			(*s.Values)[j] = 0
		}
		var sum float64
		for ; j < len(*s.Values); j++ {
			v := (*s.Values)[j]
			if !math.IsNaN(v) {
				sum += v
			}
			if 0 < j && j < len(s.MaxHost) && s.MaxHost[j] == 0 && s.MaxHost[j-1] != 0 {
				s.MaxHost[j] = s.MaxHost[j-1]
			}
			(*s.Values)[j] = sum
		}
	}
	return bag
}

func funcRate(ev *evaluator, bag Series) Series {
	t := ev.time()
	for _, s := range bag.Data {
		wnd := ev.newWindow(*s.Values, false)
		for wnd.moveOneLeft() {
			if 1 < wnd.n {
				delta := (*s.Values)[wnd.r] - (*s.Values)[wnd.l]
				wnd.setValueAtRight(delta / float64(t[wnd.r]-t[wnd.l]))
			} else {
				wnd.setValueAtRight(NilValue)
			}
		}
		wnd.fillPrefixWith(NilValue)
	}
	return bag
}

func funcResets(ev *evaluator, bag Series) Series {
	for _, s := range bag.Data {
		for i := range *s.Values {
			(*s.Values)[i] = 0
		}
	}
	return bag
}

func funcScalar(ev *evaluator, args parser.Expressions) ([]Series, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("invalid argument count in scalar(): expected 1, got %d", len(args))
	}
	bag, err := ev.eval(args[0])
	if err != nil {
		return nil, err
	}
	for x := range bag {
		if len(bag[x].Data) == 1 {
			continue
		}
		if len(bag[x].Data) == 0 {
			bag[x].Data = append(bag[x].Data, ev.alloc())
		}
		for i := range *bag[x].Data[0].Values {
			(*bag[x].Data[0].Values)[i] = math.NaN()
		}
		ev.free(bag[x].Data[1:])
	}
	return bag, nil
}

func funcTime(ev *evaluator, _ parser.Expressions) Series {
	var (
		t = ev.time()
		s = ev.alloc()
	)
	for i := range *s.Values {
		(*s.Values)[i] = float64(t[i])
	}
	return Series{Data: []SeriesData{s}}
}

func funcTimestamp(ev *evaluator, bag Series) Series {
	for i := range bag.Data {
		for j, t := range ev.time() {
			(*bag.Data[i].Values)[j] = float64(t)
		}
	}
	return bag
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
	bag, err := ev.eval(args[1])
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
		for x := range bag {
			for _, s := range bag[x].Data {
				for i := range *s.Values {
					(*s.Values)[i] = v
				}
			}
		}
		return bag, nil
	}
	for x := range bag {
		for _, s := range bag[x].Data {
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
	return bag, nil
}

func funcPresentOverTime(ev *evaluator, args parser.Expressions) ([]Series, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("invalid argument count in present_over_time(): expected 1, got %d", len(args))
	}
	bag, err := ev.eval(args[0])
	if err != nil {
		return nil, err
	}
	for x := range bag {
		for _, s := range bag[x].Data {
			lastSeen := int64(math.MinInt64)
			for i, t := range ev.time() {
				p := math.Float64bits((*s.Values)[i]) != NilValueBits
				if p || lastSeen < t-ev.r {
					(*s.Values)[i] = 1
					if p {
						lastSeen = t
					}
				} else {
					(*s.Values)[i] = NilValue
				}
			}
		}
	}
	return bag, nil
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

func funcPi(ev *evaluator, _ parser.Expressions) Series {
	s := ev.alloc()
	for i := range *s.Values {
		(*s.Values)[i] = math.Pi
	}
	return Series{Data: []SeriesData{s}}
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
	parser.ADD:      {sliceAdd},
	parser.LDEFAULT: {sliceDefault},
	parser.DIV:      {sliceDiv},
	parser.EQLC:     {sliceFilterEqual, sliceEqual},
	parser.GTE:      {sliceFilterGreaterOrEqual, sliceGreaterOrEqual},
	parser.GTR:      {sliceFilterGreater, sliceGreater},
	parser.LSS:      {sliceFilterLess, sliceLess},
	parser.LTE:      {sliceFilterLessOrEqual, sliceLessOrEqual},
	parser.MOD:      {sliceMod},
	parser.MUL:      {sliceMul},
	parser.NEQ:      {sliceFilterNotEqual, sliceNotEqual},
	parser.POW:      {slicePow},
	parser.SUB:      {sliceSub},
	parser.ATAN2:    {sliceAtan2},
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

func sliceDefault(dst, lhs, rhs []float64) {
	for i := range lhs {
		if math.IsNaN(lhs[i]) {
			lhs[i] = rhs[i]
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
