// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package util

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

	b.Run("PartialSortIndexByValueDesc", func(b *testing.B) {
		unsorted := make([]int, len(idx))
		for i := 0; i < b.N; i++ {
			copy(unsorted, idx)
			PartialSortIndexByValueDesc(unsorted, val, n, nil, nil)
		}
	})
}

func TestPartialSortIndexByValue(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		var (
			val  = rapid.SliceOf(rapid.Float64()).Draw(t, "values")
			idx  = make([]int, len(val))
			idx2 = make([]int, len(val))
			n    = rapid.IntRange(0, len(val)).Draw(t, "n")
		)
		for i := 0; i < len(val); i++ {
			idx[i] = i
			idx2[i] = i
		}
		PartialSortIndexByValueDesc(idx, val, n, nil, nil)
		for i := 1; i < n; i++ {
			if val[idx[i-1]] == val[idx[i]] {
				require.Less(t, idx2[idx[i-1]], idx2[idx[i]])
			} else {
				require.Greater(t, val[idx[i-1]], val[idx[i]])
			}
		}
		idx2 = append(idx2[:0], idx...)
		PartialSortIndexByValueAsc(idx, val, n, nil, nil)
		for i := 1; i < n; i++ {
			if val[idx[i-1]] == val[idx[i]] {
				require.Less(t, idx2[idx[i-1]], idx2[idx[i]])
			} else {
				require.Less(t, val[idx[i-1]], val[idx[i]])
			}
		}
	})
}
