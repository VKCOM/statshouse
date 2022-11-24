// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"math"
	"testing"

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
