package promql

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

func testWindow(t require.TestingT, w *window, s int64) {
	var i int64
	for range w.t {
		w.t[i] = i * s
		i++
	}
	var lastL, lastR = math.MaxInt, math.MaxInt
	for i = 0; w.moveOneLeft(); i++ {
		// window moves left
		require.Less(t, w.l, lastL)
		require.Less(t, w.r, lastR)
		lastL, lastR = w.l, w.r
		// number of values in the current interval is updated correctly
		tt, vv := w.get()
		require.Equal(t, len(tt), w.n, "iteration #%d", i)
		require.Equal(t, len(vv), w.n, "iteration #%d", i)
		require.Equal(t, len(w.getValues()), w.n, "iteration #%d", i)
		require.Equal(t, len(w.getCopyOfValues()), w.n, "iteration #%d", i)
		// ensure window width is correct
		if w.w == 0 {
			require.Equal(t, w.l+1, w.r)
		} else {
			var (
				d  int64
				ww = w.t[w.r] - w.t[w.l]
			)
			if w.strict {
				d = w.w - ww
			} else {
				d = ww - w.w
			}
			require.LessOrEqual(t, int64(0), d)
			require.LessOrEqual(t, d, s)
		}
	}
}

func TestWindowRegression1(t *testing.T) {
	var (
		values = []float64{1}
		time   = make([]int64, len(values))
		w      = newWindow(time, values, 2, false)
	)
	testWindow(t, &w, 1)
}

func TestWindowRegression2(t *testing.T) {
	var (
		values = []float64{1, 1}
		time   = make([]int64, len(values))
		w      = newWindow(time, values, 3, false)
	)
	testWindow(t, &w, 1)
}

func TestWindowRegression3(t *testing.T) {
	var (
		values = []float64{1, 1, 1}
		time   = make([]int64, len(values))
		w      = newWindow(time, values, 1, false)
	)
	testWindow(t, &w, 1)
}

func TestWindowRegression4(t *testing.T) {
	var (
		values = []float64{1, 1, 1}
		time   = make([]int64, len(values))
		w      = newWindow(time, values, 2, false)
	)
	testWindow(t, &w, 1)
}

func TestWindowRegression5(t *testing.T) {
	var (
		values = []float64{1, 1, 1, 1, 1, 1}
		time   = make([]int64, len(values))
		w      = newWindow(time, values, 6, false)
	)
	testWindow(t, &w, 2)
}

func TestWindowRegression6(t *testing.T) {
	var (
		values = []float64{1, 1, 1, 1, 1}
		time   = make([]int64, len(values))
		w      = newWindow(time, values, 9223372036854775807, false)
	)
	testWindow(t, &w, 2)
}

func TestWindowRandom(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		var (
			values      = rapid.SliceOfN(rapid.Float64(), 0, 1_000_000).Draw(t, "values")
			time        = make([]int64, len(values))
			chanceOfNaN = rapid.UintMax(100).Draw(t, "chance of NaN")
			w           = newWindow(time, values, rapid.Int64Min(0).Draw(t, "width"), rapid.Bool().Draw(t, "strict"))
		)
		for i := range values {
			if rapid.UintMax(100).Draw(t, "NaN dice") < chanceOfNaN {
				values[i] = math.NaN()
			}
		}
		testWindow(t, &w, rapid.Int64Min(1).Draw(t, "step"))
	})
}
