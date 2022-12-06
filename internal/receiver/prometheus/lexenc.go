// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package prometheus

import (
	"math"
)

func LexEncode(x float32) int32 {
	bits := math.Float32bits(x)
	if bits&0x80000000 != 0 {
		// flip all except signbit so bigger negatives go before smaller ones
		bits ^= 0x7fffffff
	}
	return int32(bits)
}

func LexDecode(x int32) float32 {
	bits := uint32(x)
	if x < 0 {
		bits ^= 0x7fffffff
	}
	return math.Float32frombits(bits)
}
