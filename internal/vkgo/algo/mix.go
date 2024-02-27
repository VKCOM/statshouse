// Copyright 2024 V Kontakte LLC
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

func ResizeSlice(s []byte, size int) []byte {
	if cap(s) < size {
		s = append(s, make([]byte, size-len(s))...)
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
