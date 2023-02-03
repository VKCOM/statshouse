// Copied (with minor modifications) from https://github.com/prometheus
//
// Copyright 2015 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package promql

import (
	"context"
	"encoding/json"
	"fmt"
	"math"

	"github.com/vkcom/statshouse/internal/promql/parser"
)

// region promql/functions.go

func kahanSumInc(inc, sum, c float64) (newSum, newC float64) {
	t := sum + inc
	// Using Neumaier improvement, swap if next term larger than sum.
	if math.Abs(sum) >= math.Abs(inc) {
		c += (sum - t) + inc
	} else {
		c += (inc - t) + sum
	}
	return t, c
}

// linearRegression performs a least-square linear regression analysis on the
// provided SamplePairs. It returns the slope, and the intercept value at the
// provided time.
func linearRegression(t []int64, v []float64) (slope, intercept float64) {
	var (
		n          float64
		sumX, cX   float64
		sumY, cY   float64
		sumXY, cXY float64
		sumX2, cX2 float64
		initY      float64
		constY     bool
	)
	initY = v[0]
	constY = true
	for i, sample := range v {
		// Set constY to false if any new y values are encountered.
		if constY && i > 0 && sample != initY {
			constY = false
		}
		n += 1.0
		x := float64(t[i] - t[0])
		sumX, cX = kahanSumInc(x, sumX, cX)
		sumY, cY = kahanSumInc(sample, sumY, cY)
		sumXY, cXY = kahanSumInc(x*sample, sumXY, cXY)
		sumX2, cX2 = kahanSumInc(x*x, sumX2, cX2)
	}
	if constY {
		if math.IsInf(initY, 0) {
			return math.NaN(), math.NaN()
		}
		return 0, initY
	}
	sumX = sumX + cX
	sumY = sumY + cY
	sumXY = sumXY + cXY
	sumX2 = sumX2 + cX2

	covXY := sumXY - sumX*sumY/n
	varX := sumX2 - sumX*sumX/n

	slope = covXY / varX
	intercept = sumY/n - slope*sumX/n
	return slope, intercept
}

// Calculate the trend value at the given index i in raw data d.
// This is somewhat analogous to the slope of the trend at the given index.
// The argument "tf" is the trend factor.
// The argument "s0" is the computed smoothed value.
// The argument "s1" is the computed trend factor.
// The argument "b" is the raw input value.
func calcTrendValue(i int, tf, s0, s1, b float64) float64 {
	if i == 0 {
		return b
	}

	x := tf * (s1 - s0)
	y := (1 - tf) * b

	return x + y
}

// Holt-Winters is similar to a weighted moving average, where historical data has exponentially less influence on the current data.
// Holt-Winter also accounts for trends in data. The smoothing factor (0 < sf < 1) affects how historical data will affect the current
// data. A lower smoothing factor increases the influence of historical data. The trend factor (0 < tf < 1) affects
// how trends in historical data will affect the current data. A higher trend factor increases the influence.
// of trends. Algorithm taken from https://en.wikipedia.org/wiki/Exponential_smoothing titled: "Double exponential smoothing".
func funcHoltWinters(ctx context.Context, ev *evaluator, args parser.Expressions) (bag SeriesBag, err error) {
	bag, err = ev.eval(ctx, args[0])
	if err != nil {
		return
	}
	sf := args[1].(*parser.NumberLiteral).Val
	if sf <= 0 || sf >= 1 {
		err = fmt.Errorf("invalid smoothing factor. Expected: 0 < sf < 1, got: %f", sf)
		return
	}
	tf := args[2].(*parser.NumberLiteral).Val
	if tf <= 0 || tf >= 1 {
		err = fmt.Errorf("invalid trend factor. Expected: 0 < tf < 1, got: %f", tf)
		return
	}
	v := make([]float64, 0, 2)
	for _, row := range bag.Data {
		wnd := newWindow(2, bag.Range, bag.Time, *row)
		for wnd.moveOnePointLeft() {
			v = wnd.getData(v[:0])
			var (
				s0, x, y float64
				s1, b    = v[0], v[1] - v[0]
			)
			for i := 1; i < len(v); i++ {
				// Scale the raw value against the smoothing factor.
				x = sf * v[i]

				// Scale the last smoothed value with the trend at this point.
				b = calcTrendValue(i-1, tf, s0, s1, b)
				y = (1 - sf) * (s1 + b)

				s0, s1 = s1, x+y
			}
			(*row)[wnd.r] = s1
		}
		for i := 0; i < wnd.r; i++ {
			(*row)[i] = NilValue
		}
	}
	bag.Range = 0
	return
}

func funcRound(ctx context.Context, ev *evaluator, args parser.Expressions) (bag SeriesBag, err error) {
	bag, err = ev.eval(ctx, args[0])
	if err != nil {
		return
	}

	// round returns a number rounded to toNearest.
	// Ties are solved by rounding up.
	toNearest := float64(1)
	if len(args) >= 2 {
		toNearest = args[1].(*parser.NumberLiteral).Val
	}

	// Invert as it seems to cause fewer floating point accuracy issues.
	toNearestInverse := 1.0 / toNearest

	for _, p := range bag.Data {
		row := *p
		for i := range row {
			row[i] = math.Floor(row[i]*toNearestInverse+0.5) / toNearestInverse
		}
	}
	return
}

// endregion promql/functions.go

// region promql/value.go

// String represents a string value.
type String struct {
	T int64
	V string
}

func (String) Type() parser.ValueType { return parser.ValueTypeString }

func (s String) String() string {
	return s.V
}

func (s String) MarshalJSON() ([]byte, error) {
	return json.Marshal([]any{float64(s.T), s.V})
}

// endregion promql/value.go

// region promql/engine.go

// shouldDropMetricName returns whether the metric name should be dropped in the
// result of the op operation.
func shouldDropMetricName(op parser.ItemType) bool {
	switch op {
	case parser.ADD, parser.SUB, parser.DIV, parser.MUL, parser.POW, parser.MOD:
		return true
	default:
		return false
	}
}

// endregion promql/engine.go
