// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package promql

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

func testWindow(t require.TestingT, values []float64, step, width int64, strict bool) {
	var (
		i    int64
		time = make([]int64, len(values))
	)
	for range time {
		time[i] = i * step
		i++
	}
	var (
		wnd   = newWindow(time, values, width, step, strict)
		lastL = len(time)
		lastR = len(time)
	)
	for wnd.moveOneLeft() {
		// window moves left
		require.LessOrEqual(t, wnd.l, wnd.r)
		require.Less(t, wnd.l, lastL)
		require.Equal(t, wnd.r, lastR-1)
		lastL, lastR = wnd.l, wnd.r
		// number of values in the current interval is updated correctly
		tt, vv := wnd.get()
		require.Equal(t, len(tt), wnd.n)
		require.Equal(t, len(vv), wnd.n)
		require.Equal(t, len(wnd.getValues()), wnd.n)
		require.Equal(t, len(wnd.getCopyOfValues()), wnd.n)
		// ensure window width is correct
		if wnd.w == 0 {
			require.Equal(t, wnd.l, wnd.r)
		} else if wnd.n != 0 {
			var (
				d  int64
				ww = wnd.t[wnd.r] - wnd.t[wnd.l] + step
			)
			if wnd.strict {
				d = wnd.w - ww
			} else {
				d = ww - wnd.w
			}
			require.LessOrEqual(t, int64(0), d)
			require.LessOrEqual(t, d, step)
		}
	}
}

func TestWindowRegression1(t *testing.T) {
	var (
		values = []float64{1}
		step   = int64(1)
		width  = int64(2)
		strict = false
	)
	testWindow(t, values, step, width, strict)
}

func TestWindowRegression2(t *testing.T) {
	var (
		values = []float64{1, 1}
		step   = int64(1)
		width  = int64(3)
		strict = false
	)
	testWindow(t, values, step, width, strict)
}

func TestWindowRegression3(t *testing.T) {
	var (
		values = []float64{1, 1, 1}
		step   = int64(1)
		width  = int64(1)
		strict = false
	)
	testWindow(t, values, step, width, strict)
}

func TestWindowRegression4(t *testing.T) {
	var (
		values = []float64{1, 1, 1}
		step   = int64(1)
		width  = int64(2)
		strict = false
	)
	testWindow(t, values, step, width, strict)
}

func TestWindowRegression5(t *testing.T) {
	var (
		values = []float64{1, 1, 1, 1, 1, 1}
		step   = int64(2)
		width  = int64(6)
		strict = false
	)
	testWindow(t, values, step, width, strict)
}

func TestWindowRegression6(t *testing.T) {
	var (
		values = []float64{1, 1, 1, 1, 1}
		step   = int64(2)
		width  = int64(9223372036854775807)
		strict = false
	)
	testWindow(t, values, step, width, strict)
}

func TestWindowRandom(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		var (
			values      = rapid.SliceOfN(rapid.Float64(), 0, 1_000_000).Draw(t, "values")
			step        = rapid.Int64Min(1).Draw(t, "step")
			width       = rapid.Int64Min(0).Draw(t, "width")
			strict      = rapid.Bool().Draw(t, "strict")
			chanceOfNaN = rapid.UintMax(100).Draw(t, "chance of NaN")
		)
		for i := range values {
			if rapid.UintMax(100).Draw(t, "NaN dice") < chanceOfNaN {
				values[i] = math.NaN()
			}
		}
		testWindow(t, values, step, width, strict)
	})
}
