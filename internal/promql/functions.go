// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package promql

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/common/model"
	"github.com/vkcom/statshouse/internal/promql/parser"
)

// region AggregateExpr

type aggregateFunc func(*evaluator, seriesGroup, parser.Expr) SeriesBag

var aggregates = map[parser.ItemType]aggregateFunc{
	parser.AVG:          simpleAggregate(funcAvg),
	parser.BOTTOMK:      funcBottomK,
	parser.COUNT:        simpleAggregate(funcCount),
	parser.COUNT_VALUES: funcCountValues,
	parser.GROUP:        simpleAggregate(funcGroup),
	parser.MAX:          simpleAggregate(funcMax),
	parser.MIN:          simpleAggregate(funcMin),
	parser.QUANTILE:     funcQuantile,
	parser.STDDEV:       simpleAggregate(funcStdDev),
	parser.STDVAR:       simpleAggregate(funcStdVar),
	parser.SUM:          simpleAggregate(funcSum),
	parser.TOPK:         funcTopK,
}

func simpleAggregate(fn func([]*[]float64, int)) aggregateFunc {
	return func(ev *evaluator, g seriesGroup, _ parser.Expr) SeriesBag {
		fn(g.bag.Data, len(g.bag.Time))
		for i := 1; i < len(g.bag.Data); i++ {
			ev.free(g.bag.Data[i])
		}
		return SeriesBag{
			Time:  g.bag.Time,
			Data:  []*[]float64{g.bag.Data[0]},
			Tags:  []map[string]int32{g.tags},
			STags: []map[string]string{g.stags},
		}
	}
}

func funcAvg(data []*[]float64, n int) {
	for j := 0; j < n; j++ {
		var res float64
		for i := 0; i < len(data); i++ {
			res += (*data[i])[j]
		}
		(*data[0])[j] = res / float64(len(data))
	}
}

func funcBottomK(ev *evaluator, g seriesGroup, p parser.Expr) SeriesBag {
	return firstK(ev, g, int(p.(*parser.NumberLiteral).Val), false)
}

func funcCount(data []*[]float64, n int) {
	for j := 0; j < n; j++ {
		var res int
		for _, row := range data {
			if !math.IsNaN((*row)[j]) {
				res++
			}
		}
		(*data[0])[j] = float64(res)
	}
}

func funcCountValues(ev *evaluator, g seriesGroup, p parser.Expr) SeriesBag {
	m := make(map[float64]*[]float64)
	for _, row := range g.bag.Data {
		for i, v := range *row {
			s := m[v]
			if s == nil {
				s = ev.alloc()
				m[v] = s
			}
			(*s)[i]++
		}
	}
	res := SeriesBag{
		Time:  g.bag.Time,
		Data:  make([]*[]float64, 0, len(m)),
		Tags:  make([]map[string]int32, len(m)),
		STags: make([]map[string]string, 0, len(m)),
	}
	for v, s := range m {
		for i := range *s {
			if (*s)[i] == 0 {
				(*s)[i] = math.NaN()
			}
		}
		res.Data = append(res.Data, s)
		res.STags = append(res.STags, map[string]string{
			p.(*parser.StringLiteral).Val: strconv.FormatFloat(v, 'f', -1, 64),
		})
	}
	for _, row := range g.bag.Data {
		ev.free(row)
	}
	return res
}

func funcGroup(data []*[]float64, n int) {
	for j := 0; j < n; j++ {
		(*data[0])[j] = 1
	}
}

func funcMax(data []*[]float64, n int) {
	for j := 0; j < n; j++ {
		res := -math.MaxFloat64
		for _, row := range data {
			v := (*row)[j]
			if res < v {
				res = v
			}
		}
		(*data[0])[j] = res
	}
}

func funcMin(data []*[]float64, n int) {
	for j := 0; j < n; j++ {
		res := math.MaxFloat64
		for _, row := range data {
			v := (*row)[j]
			if v < res {
				res = v
			}
		}
		(*data[0])[j] = res
	}
}

func funcQuantile(ev *evaluator, g seriesGroup, p parser.Expr) SeriesBag {
	var (
		q = p.(*parser.NumberLiteral).Val
		v float64
	)
	if math.IsNaN(q) {
		v = math.NaN()
	} else if q < 0 {
		v = math.Inf(-1)
	} else if q > 1 {
		v = math.Inf(1)
	}
	row0 := *g.bag.Data[0]
	if v != 0 {
		for i := 0; i < len(g.bag.Time); i++ {
			row0[i] = v
		}
	} else {
		var (
			x  = make([]int, len(g.bag.Data))
			ix = q * (float64(len(g.bag.Data)) - 1)
			i1 = int(math.Floor(ix))
			i2 = int(math.Min(float64(len(g.bag.Data)-1), float64(i1+1)))
			w1 = float64(i2) - ix
			w2 = 1 - w1
		)
		for i := range x {
			x[i] = i
		}
		for i := 0; i < len(g.bag.Time); i++ {
			sort.Slice(x, func(j, k int) bool { return (*g.bag.Data[x[j]])[i] < (*g.bag.Data[x[k]])[i] })
			row0[i] = (*g.bag.Data[x[i1]])[i]*w1 + (*g.bag.Data[x[i2]])[i]*w2
		}
	}
	for i := 1; i < len(g.bag.Data); i++ {
		ev.free(g.bag.Data[i])
	}
	return SeriesBag{
		Time:  g.bag.Time,
		Data:  []*[]float64{g.bag.Data[0]},
		Tags:  []map[string]int32{g.tags},
		STags: []map[string]string{g.stags},
	}
}

func funcStdDev(data []*[]float64, n int) {
	funcStdVar(data, n)
	for j := 0; j < n; j++ {
		(*data[0])[j] = math.Sqrt((*data[0])[j])
	}
}

func funcStdVar(data []*[]float64, n int) {
	for j := 0; j < n; j++ {
		var cnt int
		var sum float64
		for _, row := range data {
			v := (*row)[j]
			if !math.IsNaN(v) {
				sum += v
				cnt++
			}
		}
		mean := sum / float64(cnt)
		var res float64
		for _, row := range data {
			v := (*row)[j]
			if !math.IsNaN(v) {
				d := v - mean
				res += d * d / float64(cnt)
			}
		}
		(*data[0])[j] = res
	}
}

func funcSum(data []*[]float64, n int) {
	for j := 0; j < n; j++ {
		var res float64
		for i := 0; i < len(data); i++ {
			res += (*data[i])[j]
		}
		(*data[0])[j] = res
	}
}

func funcTopK(ev *evaluator, g seriesGroup, p parser.Expr) SeriesBag {
	return firstK(ev, g, int(p.(*parser.NumberLiteral).Val), true)
}

func firstK(ev *evaluator, g seriesGroup, k int, topDown bool) SeriesBag {
	if k <= 0 {
		return SeriesBag{Time: g.bag.Time}
	}
	if len(g.bag.Data) <= k {
		return g.bag
	}
	w := make([]float64, len(g.bag.Data))
	for i, data := range g.bag.Data {
		var acc float64
		for _, v := range *data {
			acc += v * v
		}
		w[i] = acc
	}
	x := make([]int, len(g.bag.Data))
	for i := range x {
		x[i] = i
	}
	if topDown {
		sort.Slice(x, func(i, j int) bool { return w[x[i]] > w[x[j]] })
	} else {
		sort.Slice(x, func(i, j int) bool { return w[x[i]] < w[x[j]] })
	}
	res := SeriesBag{
		Time:  g.bag.Time,
		Data:  make([]*[]float64, k),
		Tags:  make([]map[string]int32, k),
		STags: make([]map[string]string, k),
	}
	for i := 0; i < k; i++ {
		res.Data[i] = g.bag.Data[x[i]]
		res.Tags[i] = g.bag.Tags[x[i]]
		res.STags[i] = g.bag.STags[x[i]]
	}
	for ; k < len(g.bag.Data); k++ {
		ev.free(g.bag.Data[k])
	}
	return res
}

// endregion AggregateExpr

// region Call

type callFunc func(context.Context, *evaluator, parser.Expressions) (SeriesBag, error)

var calls map[string]callFunc

func init() {
	calls = map[string]callFunc{
		"abs": simpleCall(math.Abs),
		// "absent": ?
		// "absent_over_time": ?
		"ceil": simpleCall(math.Ceil),
		// "changes": ?
		"clamp":         funcClamp,
		"clamp_max":     funcClampMax,
		"clamp_min":     funcClampMin,
		"day_of_month":  timeCall(time.Time.Day),
		"day_of_week":   timeCall(time.Time.Weekday),
		"day_of_year":   timeCall(time.Time.YearDay),
		"days_in_month": timeCall(func(t time.Time) int { return 32 - time.Date(t.Year(), t.Month(), 32, 0, 0, 0, 0, t.Location()).Day() }),
		"delta":         bagCall(funcDelta),
		"deriv":         bagCall(funcDeriv),
		"exp":           simpleCall(math.Exp),
		"floor":         simpleCall(math.Floor),
		// "histogram_count": ?
		// "histogram_sum": ?
		// "histogram_fraction": ?
		// "histogram_quantile": ?
		"holt_winters":   funcHoltWinters,
		"hour":           timeCall(time.Time.Hour),
		"idelta":         bagCall(funcIdelta),
		"increase":       bagCall(funcDelta),
		"irate":          bagCall(funcIrate),
		"label_join":     funcLabelJoin,
		"label_replace":  funcLabelReplace,
		"ln":             simpleCall(math.Log),
		"log2":           simpleCall(math.Log2),
		"log10":          simpleCall(math.Log10),
		"minute":         timeCall(time.Time.Minute),
		"month":          timeCall(time.Time.Month),
		"predict_linear": funcPredictLinear,
		"rate":           bagCall(funcRate),
		"resets":         bagCall(funcResets),
		"round":          funcRound,
		"scalar":         funcScalar,
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
		"avg_over_time":      overTimeCall(funcAvgOverTime),
		"min_over_time":      overTimeCall(funcMinOverTime),
		"max_over_time":      overTimeCall(funcMaxOverTime),
		"sum_over_time":      overTimeCall(funcSumOverTime),
		"count_over_time":    overTimeCall(funcCountOverTime),
		"quantile_over_time": funcQuantileOverTime,
		"stddev_over_time":   overTimeCall(funcStdDevOverTime),
		"stdvar_over_time":   overTimeCall(funcStdVarOverTime),
		"last_over_time":     nopCall,
		// "present_over_time": ?
		"acos":  simpleCall(math.Acos),
		"acosh": simpleCall(math.Acosh),
		"asin":  simpleCall(math.Asin),
		"asinh": simpleCall(math.Asinh),
		"atan":  simpleCall(math.Atan),
		"atanh": simpleCall(math.Atanh),
		"cos":   simpleCall(math.Cos),
		"cosh":  simpleCall(math.Cosh),
		"sin":   simpleCall(math.Sin),
		"sinh":  simpleCall(math.Sinh),
		"tan":   simpleCall(math.Tan),
		"tanh":  simpleCall(math.Tanh),
		"deg":   simpleCall(func(v float64) float64 { return v * 180 / math.Pi }),
		"pi":    generatorCall(funcPi),
		"rad":   simpleCall(func(v float64) float64 { return v * math.Pi / 180 }),
	}
}

type window struct {
	time   []int64
	data   []float64
	minCnt int   // minimum number of points
	minDur int64 // minimum duration
	l, r   int   // current [l, r] interval, cnt <= r-l && dur <= time[r]-time[l]
}

func newWindow(cnt int, dur int64, t []int64, d []float64) window {
	return window{time: t, data: d, minCnt: cnt - 1, minDur: dur, r: len(t)}
}

func (wnd *window) moveOnePointLeft() bool {
	// move right boundary
	for {
		if wnd.r <= 0 {
			return false
		}
		wnd.r--
		if !math.IsNaN(wnd.data[wnd.r]) {
			break
		}
	}
	// move left boundary
	wnd.l = wnd.r
	var cnt int
	var dur int64
	for 0 < wnd.l && (dur < wnd.minDur || cnt < wnd.minCnt) {
		wnd.l--
		if !math.IsNaN(wnd.data[wnd.l]) {
			cnt++
			dur = wnd.time[wnd.r] - wnd.time[wnd.l]
		}
	}
	return 0 <= wnd.l && wnd.minDur <= dur && wnd.minCnt <= cnt
}

func (wnd *window) get(t []int64, v []float64) ([]int64, []float64) {
	for i := wnd.l; i <= wnd.r; i++ {
		if !math.IsNaN(wnd.data[i]) {
			t = append(t, wnd.time[i])
			v = append(v, wnd.data[i])
		}
	}
	return t, v
}

func (wnd *window) getData(s []float64) []float64 {
	for i := wnd.l; i <= wnd.r; i++ {
		if !math.IsNaN(wnd.data[i]) {
			s = append(s, wnd.data[i])
		}
	}
	return s
}

func bagCall(fn func(SeriesBag) SeriesBag) callFunc {
	return func(ctx context.Context, ev *evaluator, args parser.Expressions) (bag SeriesBag, err error) {
		bag, err = ev.eval(ctx, args[0])
		if err != nil {
			return bag, err
		}
		return fn(bag), nil
	}
}

func generatorCall(fn func(ev *evaluator, args parser.Expressions) SeriesBag) callFunc {
	return func(_ context.Context, ev *evaluator, args parser.Expressions) (bag SeriesBag, err error) {
		return fn(ev, args), nil
	}
}

func nopCall(ctx context.Context, ev *evaluator, args parser.Expressions) (bag SeriesBag, err error) {
	return ev.eval(ctx, args[0])
}

func overTimeCall(fn func(v []float64) float64) callFunc {
	return func(ctx context.Context, ev *evaluator, args parser.Expressions) (bag SeriesBag, err error) {
		bag, err = ev.eval(ctx, args[0])
		if err != nil {
			return bag, err
		}
		for _, row := range bag.Data {
			wnd := newWindow(1, bag.Range, bag.Time, *row)
			for wnd.moveOnePointLeft() {
				(*row)[wnd.r] = fn((*row)[wnd.l : wnd.r+1])
			}
			for i := 0; i < wnd.r; i++ {
				(*row)[i] = NilValue
			}
		}
		return bag, nil
	}
}

func simpleCall(fn func(float64) float64) callFunc {
	return func(ctx context.Context, ev *evaluator, args parser.Expressions) (bag SeriesBag, err error) {
		bag, err = ev.eval(ctx, args[0])
		if err != nil {
			return bag, err
		}
		for _, row := range bag.Data {
			for i := range *row {
				(*row)[i] = fn((*row)[i])
			}
		}
		return bag, nil
	}
}

func timeCall[V int | time.Weekday | time.Month](fn func(time.Time) V) callFunc {
	return func(ctx context.Context, ev *evaluator, args parser.Expressions) (bag SeriesBag, err error) {
		if len(args) != 0 {
			bag, err = ev.eval(ctx, args[0])
			if err != nil {
				return bag, err
			}
		}
		if len(bag.Data) == 0 {
			bag = funcTime(ev, args)
		}
		for _, row := range bag.Data {
			for j, v := range *row {
				(*row)[j] = float64(fn(time.Unix(int64(v), 0).In(ev.loc)))
			}
		}
		return bag, nil
	}
}

func funcClamp(ctx context.Context, ev *evaluator, args parser.Expressions) (bag SeriesBag, err error) {
	bag, err = ev.eval(ctx, args[0])
	if err != nil {
		return bag, err
	}
	min := args[1].(*parser.NumberLiteral).Val
	max := args[2].(*parser.NumberLiteral).Val
	// return NaN if min or max is NaN
	if math.IsNaN(min) || math.IsNaN(max) {
		for _, row := range bag.Data {
			for i := range *row {
				(*row)[i] = math.NaN()
			}
		}
		return bag, nil
	}
	// return an empty vector if min > max
	if min > max {
		return SeriesBag{Time: bag.Time}, nil
	}
	for _, row := range bag.Data {
		for i := range *row {
			if (*row)[i] < min {
				(*row)[i] = min
			} else if (*row)[i] > max {
				(*row)[i] = max
			}
			// else { min <= row[i] <= max }
		}
	}
	return bag, nil
}

func funcClampMax(ctx context.Context, ev *evaluator, args parser.Expressions) (bag SeriesBag, err error) {
	bag, err = ev.eval(ctx, args[0])
	if err != nil {
		return bag, err
	}
	max := args[1].(*parser.NumberLiteral).Val
	// return NaN if max is NaN
	if math.IsNaN(max) {
		for _, row := range bag.Data {
			for i := range *row {
				(*row)[i] = math.NaN()
			}
		}
		return bag, nil
	}
	for _, row := range bag.Data {
		for i := range *row {
			if (*row)[i] > max {
				(*row)[i] = max
			}
			// else { row[i] <= max }
		}
	}
	return bag, nil
}

func funcClampMin(ctx context.Context, ev *evaluator, args parser.Expressions) (bag SeriesBag, err error) {
	bag, err = ev.eval(ctx, args[0])
	if err != nil {
		return bag, err
	}
	min := args[1].(*parser.NumberLiteral).Val
	// return NaN if min is NaN
	if math.IsNaN(min) {
		for _, row := range bag.Data {
			for i := range *row {
				(*row)[i] = math.NaN()
			}
		}
		return bag, nil
	}
	for _, row := range bag.Data {
		for i := range *row {
			if (*row)[i] < min {
				(*row)[i] = min
			}
			// else { row[i] >= min }
		}
	}
	return bag, nil
}

func funcDelta(bag SeriesBag) SeriesBag {
	bag = funcRate(bag)
	if bag.Range == 0 {
		return bag
	}
	for _, row := range bag.Data {
		for i := range *row {
			(*row)[i] *= float64(bag.Range)
		}
	}
	return bag
}

func funcDeriv(bag SeriesBag) SeriesBag {
	var (
		t = make([]int64, 0, 2)
		v = make([]float64, 0, 2)
	)
	for _, row := range bag.Data {
		wnd := newWindow(2, bag.Range, bag.Time, *row)
		for wnd.moveOnePointLeft() {
			t, v = wnd.get(t[:0], v[:0])
			slope, _ := linearRegression(t, v)
			(*row)[wnd.r] = slope
		}
		for i := 0; i < wnd.r; i++ {
			(*row)[i] = NilValue
		}
	}
	return bag
}

func funcIdelta(bag SeriesBag) SeriesBag {
	for _, row := range bag.Data {
		for i := len(*row) - 1; i > 0; i-- {
			(*row)[i] = (*row)[i] - (*row)[i-1]
		}
		(*row)[0] = NilValue
	}
	return bag
}

func funcIrate(bag SeriesBag) SeriesBag {
	for _, row := range bag.Data {
		for i := len(*row) - 1; i > 0; i-- {
			(*row)[i] = ((*row)[i] - (*row)[i-1]) / float64(bag.Time[i]-bag.Time[i-1])
		}
		(*row)[0] = NilValue
	}
	return bag
}

func funcLabelJoin(ctx context.Context, ev *evaluator, args parser.Expressions) (bag SeriesBag, err error) {
	bag, err = ev.eval(ctx, args[0])
	if err != nil {
		return bag, err
	}
	var (
		dst = args[1].(*parser.StringLiteral).Val
		sep = args[2].(*parser.StringLiteral).Val
		src = make(map[string]bool, len(args)-3)
	)
	for i := 3; i < len(args); i++ {
		src[args[i].(*parser.StringLiteral).Val] = true
	}
	for i := range bag.Data {
		var s []string
		for tag, tagValueID := range bag.Tags[i] {
			if src[tag] {
				s = append(s, ev.tagM.getTagValue(tagValueID))
			}
		}
		for tag, tagValue := range bag.STags[i] {
			if src[tag] {
				s = append(s, tagValue)
			}
		}
		if len(s) != 0 {
			bag.STags[i][dst] = strings.Join(s, sep)
			delete(bag.Tags[i], dst)
		}
	}
	return bag, nil
}

func funcLabelReplace(ctx context.Context, ev *evaluator, args parser.Expressions) (bag SeriesBag, err error) {
	bag, err = ev.eval(ctx, args[0])
	if err != nil {
		return bag, err
	}
	var (
		dst = args[1].(*parser.StringLiteral).Val
		tpl = args[2].(*parser.StringLiteral).Val
		src = args[3].(*parser.StringLiteral).Val
		r   *regexp.Regexp
	)
	r, err = regexp.Compile("^(?:" + args[4].(*parser.StringLiteral).Val + ")$")
	if err != nil {
		return bag, err
	}
	if !model.LabelNameRE.MatchString(dst) {
		return bag, fmt.Errorf("invalid destination label name in label_replace(): %s", dst)
	}
	for i := range bag.Data {
		var v string
		for tag, tagValueID := range bag.Tags[i] {
			if src == tag {
				v = ev.tagM.getTagValue(tagValueID)
				goto replace
			}
		}
		for tag, tagValue := range bag.STags[i] {
			if src == tag {
				v = tagValue
				goto replace
			}
		}
	replace:
		match := r.FindStringSubmatchIndex(v)
		if len(match) != 0 {
			v = string(r.ExpandString([]byte{}, tpl, v, match))
			if len(v) != 0 {
				bag.STags[i][dst] = v
			} else {
				delete(bag.STags[i], dst)
			}
			delete(bag.Tags[i], dst)
		}
	}
	return bag, nil
}

func funcPredictLinear(ctx context.Context, ev *evaluator, args parser.Expressions) (bag SeriesBag, err error) {
	bag, err = ev.eval(ctx, args[0])
	if err != nil {
		return bag, err
	}
	var (
		d = args[1].(*parser.NumberLiteral).Val // duration
		t = make([]int64, 0, 2)                 // time
		v = make([]float64, 0, 2)               // values
	)
	for _, row := range bag.Data {
		wnd := newWindow(2, bag.Range, bag.Time, *row)
		for wnd.moveOnePointLeft() {
			t, v = wnd.get(t[:0], v[:0])
			slope, intercept := linearRegression(t, v)
			(*row)[wnd.r] = slope*d + intercept
		}
		for i := 0; i < wnd.r; i++ {
			(*row)[i] = NilValue
		}
	}
	return bag, nil
}

func funcRate(bag SeriesBag) SeriesBag {
	for _, row := range bag.Data {
		wnd := newWindow(2, bag.Range, bag.Time, *row)
		for wnd.moveOnePointLeft() {
			delta := (*row)[wnd.r] - (*row)[wnd.l]
			(*row)[wnd.r] = delta / float64(bag.Time[wnd.r]-bag.Time[wnd.l])
		}
		for i := 0; i < wnd.r; i++ {
			(*row)[i] = NilValue
		}
	}
	return bag
}

func funcResets(bag SeriesBag) SeriesBag {
	for _, row := range bag.Data {
		for i := range *row {
			(*row)[i] = 0
		}
	}
	return bag
}

func funcScalar(ctx context.Context, ev *evaluator, args parser.Expressions) (bag SeriesBag, err error) {
	bag, err = ev.eval(ctx, args[0])
	if err != nil {
		return bag, err
	}
	if len(bag.Data) == 1 {
		return bag, nil
	}
	if len(bag.Data) == 0 {
		bag.Data = append(bag.Data, ev.alloc())
		bag.Tags = append(bag.Tags, nil)
		bag.STags = append(bag.STags, nil)
	}
	for i := range *bag.Data[0] {
		(*bag.Data[0])[i] = math.NaN()
	}
	for i := 1; i < len(bag.Data); i++ {
		ev.free(bag.Data[i])
	}
	return bag, nil
}

func funcTime(ev *evaluator, _ parser.Expressions) SeriesBag {
	row := ev.alloc()
	for i := range *row {
		(*row)[i] = float64(ev.time[i])
	}
	return SeriesBag{
		Time:  ev.time,
		Data:  []*[]float64{row},
		Tags:  make([]map[string]int32, 1),
		STags: make([]map[string]string, 1),
	}
}

func funcTimestamp(bag SeriesBag) SeriesBag {
	for i := range bag.Data {
		for j, t := range bag.Time {
			(*bag.Data[i])[j] = float64(t)
		}
	}
	return bag
}

func funcVector(ctx context.Context, ev *evaluator, args parser.Expressions) (SeriesBag, error) {
	return ev.eval(ctx, args[0])
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

func funcQuantileOverTime(ctx context.Context, ev *evaluator, args parser.Expressions) (bag SeriesBag, err error) {
	bag, err = ev.eval(ctx, args[1])
	if err != nil {
		return bag, err
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
		for _, row := range bag.Data {
			for i := range *row {
				(*row)[i] = v
			}
		}
		return bag, nil
	}
	vs := make([]float64, 0, 2)
	for _, row := range bag.Data {
		wnd := newWindow(2, bag.Range, bag.Time, *row)
		for wnd.moveOnePointLeft() {
			vs = wnd.getData(vs[:0])
			sort.Float64s(vs)
			var (
				ix = q * (float64(len(vs)) - 1)
				i1 = int(math.Floor(ix))
				i2 = int(math.Min(float64(len(vs)-1), float64(i1+1)))
				w1 = float64(i2) - ix
				w2 = 1 - w1
			)
			(*row)[wnd.r] = vs[i1]*w1 + vs[i2]*w2
		}
		for i := 0; i < wnd.r; i++ {
			(*row)[i] = NilValue
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

func funcPi(ev *evaluator, _ parser.Expressions) SeriesBag {
	row := ev.alloc()
	for i := range *row {
		(*row)[i] = math.Pi
	}
	return SeriesBag{
		Time:  ev.time,
		Data:  []*[]float64{row},
		Tags:  make([]map[string]int32, 1),
		STags: make([]map[string]string, 1),
	}
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
	parser.ADD:   {scalarAdd},
	parser.DIV:   {scalarDiv},
	parser.EQLC:  {scalarFilterEqual, scalarEqual},
	parser.GTE:   {scalarFilterGreaterOrEqual, scalarGreaterOrEqual},
	parser.GTR:   {scalarFilterGreater, scalarGreater},
	parser.LSS:   {scalarFilterLess, scalarLess},
	parser.LTE:   {scalarFilterLessOrEqual, scalarLessOrEqual},
	parser.MOD:   {math.Mod},
	parser.MUL:   {scalarMul},
	parser.NEQ:   {scalarFilterNotEqual, scalarNotEqual},
	parser.POW:   {math.Pow},
	parser.SUB:   {scalarSub},
	parser.ATAN2: {math.Atan2},
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
	parser.ADD:   {sliceScalarAdd},
	parser.DIV:   {sliceScalarDiv},
	parser.EQLC:  {sliceScalarFilterEqual, sliceScalarEqual},
	parser.GTE:   {sliceScalarFilterGreaterOrEqual, sliceScalarGreaterOrEqual},
	parser.GTR:   {sliceScalarFilterGreater, sliceScalarGreater},
	parser.LSS:   {sliceScalarFilterLess, sliceScalarLess},
	parser.LTE:   {sliceScalarFilterLessOrEqual, sliceScalarLessOrEqual},
	parser.MOD:   {sliceScalarMod},
	parser.MUL:   {sliceScalarMul},
	parser.NEQ:   {sliceScalarFilterNotEqual, sliceScalarNotEqual},
	parser.POW:   {sliceScalarPow},
	parser.SUB:   {sliceScalarSub},
	parser.ATAN2: {sliceScalarAtan2},
}

func getBinaryFunc[F sliceBinaryFunc | scalarBinaryFunc | scalarSliceFunc | sliceScalarFunc](m map[parser.ItemType][2]F, op parser.ItemType, b bool) F {
	if b {
		return m[op][1]
	} else {
		return m[op][0]
	}
}

func sliceAdd(dst, lhs, rhs []float64) {
	for i := range lhs {
		dst[i] = lhs[i] + rhs[i]
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
