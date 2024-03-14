// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package algo

import "golang.org/x/exp/constraints"

func CompareStrings(x, y string) int {
	// TODO: make some unsafe magic to speed up?
	if x < y {
		return -1
	}
	if x > y {
		return 1
	}
	return 0
}

// This is copy/paste from https://cs.opensource.google/go/go/+/refs/tags/go1.21.0:src/cmp/cmp.go
// Use standard library, then use go 1.21

// Compare returns
//
//	-1 if x is less than y,
//	 0 if x equals y,
//	+1 if x is greater than y.
//
// For floating-point types, a NaN is considered less than any non-NaN,
// a NaN is considered equal to a NaN, and -0.0 is equal to 0.0.
func Compare[T constraints.Ordered](x, y T) int {
	xNaN := isNaN(x)
	yNaN := isNaN(y)
	if xNaN && yNaN {
		return 0
	}
	if xNaN || x < y {
		return -1
	}
	if yNaN || x > y {
		return +1
	}
	return 0
}

// isNaN reports whether x is a NaN without requiring the math package.
// This will always return false if T is not floating-point.
func isNaN[T constraints.Ordered](x T) bool {
	return x != x
}
