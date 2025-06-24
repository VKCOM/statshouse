// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package algo

import "golang.org/x/exp/constraints"

func Clamp[T constraints.Ordered](value T, low T, high T) T {
	if value <= low {
		return low
	}
	if value >= high {
		return high
	}
	return value
}

func MinInt(x int, y int) int {
	if x <= y {
		return x
	}

	return y
}
