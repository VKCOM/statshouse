// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
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

	b.Run("partialSortIndexByValueDesc", func(b *testing.B) {
		unsorted := make([]int, len(idx))
		for i := 0; i < b.N; i++ {
			copy(unsorted, idx)
			partialSortIndexByValueDesc(unsorted, val, n)
		}
	})
}

func TestPartialSortIndexByValue(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		idx := make([]int, rapid.IntRange(0, 1<<20).Draw(t, "len"))
		for i := 0; i < len(idx); i++ {
			idx[i] = i
		}

		seed := rapid.Uint64().Draw(t, "seed")
		rand.New(seed).Shuffle(len(idx), func(i, j int) { idx[i], idx[j] = idx[j], idx[i] })

		val := rapid.SliceOfN(rapid.Float64(), len(idx), len(idx)).Draw(t, "idx")
		n := rapid.IntRange(0, len(val)).Draw(t, "n")

		partialSortIndexByValueDesc(idx, val, n)
		for i := 1; i < n; i++ {
			require.GreaterOrEqual(t, val[idx[i-1]], val[idx[i]])
		}

		partialSortIndexByValueAsc(idx, val, n)
		for i := 1; i < n; i++ {
			require.LessOrEqual(t, val[idx[i-1]], val[idx[i]])
		}
	})
}
