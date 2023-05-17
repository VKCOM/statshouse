// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAccumulateSeries(t *testing.T) {
	// Does nothing with empty slices
	s := make([]float64, 0)
	accumulateSeries(s)
	require.Empty(t, s)

	// Single element accumulates to itself
	s = []float64{1}
	accumulateSeries(s)
	require.Equal(t, []float64{1}, s)

	// Two elements ascending
	s = []float64{1, 2}
	accumulateSeries(s)
	require.Equal(t, []float64{1, 3}, s)

	// Two negative elements ascending
	s = []float64{-2, -1}
	accumulateSeries(s)
	require.Equal(t, []float64{-2, -3}, s)

	// Three elements ascending, one missing
	s = []float64{1, math.NaN(), 2}
	accumulateSeries(s)
	requireEqual(t, []float64{1, 1, 3}, s)
}

func TestDifferentiateSeries(t *testing.T) {
	// Does nothing with empty slice
	s := make([]float64, 0)
	differentiateSeries(s)
	require.Empty(t, s)

	// One element slice gives NaN
	s = []float64{1}
	differentiateSeries(s)
	requireEqual(t, []float64{math.NaN()}, s)

	// Two elements ascending
	s = []float64{1, 2}
	differentiateSeries(s)
	requireEqual(t, []float64{math.NaN(), 1}, s)

	// Two negative elements ascending
	s = []float64{-2, -1}
	differentiateSeries(s)
	requireEqual(t, []float64{math.NaN(), 1}, s)

	// Three elements ascending, one missing
	s = []float64{1, math.NaN(), 2}
	differentiateSeries(s)
	requireEqual(t, []float64{math.NaN(), math.NaN(), math.NaN()}, s)

	// Skip leading NaNs
	s = []float64{math.NaN(), math.NaN(), math.NaN(), 1, 2}
	differentiateSeries(s)
	requireEqual(t, []float64{math.NaN(), math.NaN(), math.NaN(), math.NaN(), 1}, s)

	// One input NaN gives two output NaNs
	s = []float64{1, 2, math.NaN(), 3, 4}
	differentiateSeries(s)
	requireEqual(t, []float64{math.NaN(), 1, math.NaN(), math.NaN(), 1}, s)
}

func requireEqual(t *testing.T, required, actual []float64) {
	require.Equal(t, len(required), len(actual))
	for i, v := range required {
		if math.IsNaN(v) {
			require.True(t, math.IsNaN(actual[i]))
		} else {
			require.Equal(t, v, actual[i])
		}
	}
}

func Test_limitQueries(t *testing.T) {
	genRow := func(time int64) queryTableRow {
		return queryTableRow{row: tsSelectRow{time: time}}
	}
	q := []queryTableRow{genRow(1), genRow(2), genRow(3), genRow(4), genRow(5), genRow(6), genRow(7), genRow(8)}
	type args struct {
		q       []queryTableRow
		from    RowMarker
		to      RowMarker
		fromEnd bool
		limit   int
	}
	tests := []struct {
		name        string
		args        args
		wantRes     []queryTableRow
		wantHasMore bool
	}{
		{"subslice full", args{q, RowMarker{Time: 2}, RowMarker{Time: 7}, false, 10}, q[2:6], false},
		{"subslice limited", args{q, RowMarker{Time: 2}, RowMarker{Time: 7}, false, 2}, q[2:4], true},
		{"subslice full from end", args{q, RowMarker{Time: 2}, RowMarker{Time: 7}, true, 10}, q[2:6], false},
		{"subslice limited from end", args{q, RowMarker{Time: 2}, RowMarker{Time: 7}, true, 2}, q[4:6], true},
		{"slice full", args{q, RowMarker{Time: 0}, RowMarker{Time: 10}, false, 10}, q, false},
		{"slice full limited", args{q, RowMarker{Time: -1}, RowMarker{Time: -1}, false, 2}, []queryTableRow{}, false},
		{"slice full", args{q, RowMarker{}, RowMarker{}, false, 10}, q, false},
		{"slice full", args{q, RowMarker{}, RowMarker{}, false, 2}, q[:2], true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotRes, gotHasMore := limitQueries(tt.args.q, tt.args.from, tt.args.to, tt.args.fromEnd, tt.args.limit)
			assert.Equalf(t, tt.wantRes, gotRes, "limitQueries(%v, %v, %v, %v, %v)", tt.args.q, tt.args.from, tt.args.to, tt.args.fromEnd, tt.args.limit)
			assert.Equalf(t, tt.wantHasMore, gotHasMore, "limitQueries(%v, %v, %v, %v, %v)", tt.args.q, tt.args.from, tt.args.to, tt.args.fromEnd, tt.args.limit)
		})
	}
}
