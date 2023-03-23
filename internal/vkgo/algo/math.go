// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package algo

import "golang.org/x/exp/constraints"

func Min[T constraints.Ordered](x T, y T) T {
	if x <= y {
		return x
	}
	return y
}

func Max[T constraints.Ordered](x T, y T) T {
	if x >= y {
		return x
	}
	return y
}

func MinInt8(x int8, y int8) int8 {
	if x <= y {
		return x
	}

	return y
}

func MinInt32(x int32, y int32) int32 {
	if x <= y {
		return x
	}

	return y
}

func MinInt64(x int64, y int64) int64 {
	if x <= y {
		return x
	}

	return y
}

func MinInt(x int, y int) int {
	if x <= y {
		return x
	}

	return y
}

func MinUInt8(x uint8, y uint8) uint8 {
	if x <= y {
		return x
	}

	return y
}

func MinUInt32(x uint32, y uint32) uint32 {
	if x <= y {
		return x
	}

	return y
}

func MinUInt64(x uint64, y uint64) uint64 {
	if x <= y {
		return x
	}

	return y
}

func MinUInt(x uint, y uint) uint {
	if x <= y {
		return x
	}

	return y
}

func MaxInt(x int, y int) int {
	if x >= y {
		return x
	}

	return y
}
