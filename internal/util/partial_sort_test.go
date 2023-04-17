// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package util

import (
	"math"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"pgregory.net/rand"
	"pgregory.net/rapid"
)

func BenchmarkPartialSortIndexByValue(b *testing.B) {
	var (
		idx = make([]int, 1<<20)
		rnd = rand.New()
	)

	// shuffle index
	for i := 0; i < len(idx); i++ {
		idx[i] = i
	}
	rnd.Shuffle(len(idx), func(i, j int) { idx[i], idx[j] = idx[j], idx[i] })

	// generate random values
	val := make([]float64, len(idx))
	for i := 0; i < len(val); i++ {
		val[i] = rnd.Float64()
	}

	// choose n
	n := 100

	b.ResetTimer()

	b.Run("slice.Sort", func(b *testing.B) {
		unsorted := make([]int, len(idx))
		for i := 0; i < b.N; i++ {
			copy(unsorted, idx)
			sort.Slice(unsorted, func(i, j int) bool { return val[unsorted[i]] > val[unsorted[j]] })
		}
	})

	b.Run("PartialSortIndexByValueDesc", func(b *testing.B) {
		unsorted := make([]int, len(idx))
		for i := 0; i < b.N; i++ {
			copy(unsorted, idx)
			PartialSortIndexByValueDesc(unsorted, val, n, nil)
		}
	})
}

func TestPartialSortIndexByValue(t *testing.T) {
	draw := func(t *rapid.T, n int, min float64, max float64) ([]float64, []int, int) {
		idx := make([]int, rapid.IntRange(0, n).Draw(t, "len"))
		for i := 0; i < len(idx); i++ {
			idx[i] = i
		}

		seed := rapid.Uint64().Draw(t, "seed")
		rand.New(seed).Shuffle(len(idx), func(i, j int) { idx[i], idx[j] = idx[j], idx[i] })

		val := rapid.SliceOfN(rapid.Float64Range(min, max), len(idx), len(idx)).Draw(t, "values")
		return val, idx, rapid.IntRange(0, len(val)).Draw(t, "n")
	}
	rapid.Check(t, func(t *rapid.T) { // that it actually sorts
		val, idx, n := draw(t, 10_000, -math.MaxFloat64, math.MaxFloat64)

		PartialSortIndexByValueDesc(idx, val, n, nil)
		for i := 1; i < n; i++ {
			require.GreaterOrEqual(t, val[idx[i-1]], val[idx[i]])
		}

		PartialSortIndexByValueAsc(idx, val, n, nil)
		for i := 1; i < n; i++ {
			require.LessOrEqual(t, val[idx[i-1]], val[idx[i]])
		}
	})
	rapid.Check(t, func(t *rapid.T) { // that sorting order is determined by seed if duplicates are present
		var (
			// min, max are two consecutive floats to get 50% duplicates
			min      = float64(1)
			max      = math.Float64frombits(math.Float64bits(min) + 1)
			v, x1, n = draw(t, 10_000, min, max)
			x2       = append(make([]int, 0, len(x1)), x1...)
			r1       = rand.New() // r1 is random
			r2       = *r1        // r2 is a copy of r1
		)
		PartialSortIndexByValueAsc(x1, v, n, r1)
		PartialSortIndexByValueAsc(x2, v, n, &r2)
		require.EqualValues(t, x1, x2)
		PartialSortIndexByValueDesc(x1, v, n, r1)
		PartialSortIndexByValueDesc(x2, v, n, &r2)
		require.EqualValues(t, x1, x2)
	})
}
