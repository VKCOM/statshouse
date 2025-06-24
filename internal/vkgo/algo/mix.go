// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package algo

func ValueOrDefault[T comparable](value, def T) T {
	var zero T
	if value != zero {
		return value
	}
	return def
}

func ResizeSlice[T any](s []T, size int) []T {
	if cap(s) < size {
		s = append(s, make([]T, size-len(s))...)
	} else {
		s = s[:size]
	}
	return s
}

func FillSlice[T any](slice []T, val T) {
	for i := range slice {
		slice[i] = val
	}
}
