// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"pgregory.net/rand"
)

func partialSortIndexByValueAsc(idx []int, val []float64, n int) {
	partialQuickSortIndexByValue(idx, val, 0, len(idx), n, -1, rand.New())
}

func partialSortIndexByValueDesc(idx []int, val []float64, n int) {
	partialQuickSortIndexByValue(idx, val, 0, len(idx), n, 1, rand.New())
}

func partialQuickSortIndexByValue(idx []int, val []float64, lo, hi, n int, m float64, rnd *rand.Rand) {
	if n <= lo {
		return
	}

	cnt := hi - lo
	if cnt <= 1 {
		return
	}

	// choose pivot
	p := lo + rnd.Intn(cnt)

	// partition
	idx[lo], idx[p] = idx[p], idx[lo]
	i := lo
	for j := lo + 1; j < hi; j++ {
		if m*val[idx[lo]] < m*val[idx[j]] {
			i++
			idx[i], idx[j] = idx[j], idx[i]
		}
	}

	idx[lo], idx[i] = idx[i], idx[lo]

	// recurse
	partialQuickSortIndexByValue(idx, val, lo, i, n, m, rnd)
	partialQuickSortIndexByValue(idx, val, i+1, hi, n, m, rnd)
}
