// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package prometheus

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	"pgregory.net/rapid"

	"github.com/stretchr/testify/require"
)

func TestLexEncode(t *testing.T) {
	float32NoNaN := rapid.Float32Range(float32(math.Inf(-1)), float32(math.Inf(1)))

	rapid.Check(t, func(t *rapid.T) {
		a := float32NoNaN.Draw(t, "a").(float32)
		b := float32NoNaN.Draw(t, "b").(float32)
		ae, be := LexEncode(a), LexEncode(b)
		var ab, bb [4]byte
		binary.BigEndian.PutUint32(ab[:], uint32(ae))
		binary.BigEndian.PutUint32(bb[:], uint32(be))
		switch {
		case a < b:
			if bytes.Compare(ab[:], bb[:]) >= 0 {
				t.Fatalf("%v < %v, but %x >= %x", a, b, ab, bb)
			}
		case a > b:
			if bytes.Compare(ab[:], bb[:]) <= 0 {
				t.Fatalf("%v > %v, but %x <= %x", a, b, ab, bb)
			}
		default:
			if ab != bb {
				t.Fatalf("%v == %v, but %x != %x", a, b, ab, bb)
			}
		}
	})
}

func TestLexEnc(t *testing.T) {
	// from -Inf up towards 0
	lexEncTestRange(t, float32(math.Inf(-1)), 0, 1000)
	// through 0
	lexEncTestSlice(t, []float32{-1, 0, 1}, nil)
	lexEncTestRange(t, -math.SmallestNonzeroFloat32, math.SmallestNonzeroFloat32, math.MaxInt64)
	// from +Inf down towards 0
	lexEncTestRange(t, float32(math.Inf(+1)), 0, 1000)
	// NaN
	n := LexEncode(float32(math.NaN()))
	require.Equal(t, n, LexEncode(LexDecode(n)))
}

func lexEncTestRange(t *testing.T, first, last float32, cnt int64) {
	var (
		src     = []float32{first, math.Nextafter32(first, last)}
		enc     = []int32{0, 0}
		bounded = cnt < math.MaxInt64
	)
	for ; src[0] != src[1] && 0 < cnt; cnt-- {
		lexEncTestSlice(t, src, enc)
		src[0] = src[1]
		src[1] = math.Nextafter32(src[0], last)
	}
	if bounded {
		require.Equal(t, cnt, int64(0))
	}
}

func lexEncTestSlice(t *testing.T, src []float32, enc []int32) {
	if enc == nil {
		enc = make([]int32, len(src))
	}
	for i := range src {
		enc[i] = LexEncode(src[i])
		require.Equal(t, LexDecode(enc[i]), src[i])
	}
	for i := 1; i < len(enc); i++ {
		if src[i-1] < src[i] {
			require.Less(t, enc[i-1], enc[i])
		} else {
			require.Greater(t, enc[i-1], enc[i])
		}
	}
}
