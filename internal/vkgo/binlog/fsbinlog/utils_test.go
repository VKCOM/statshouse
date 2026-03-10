// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package fsbinlog

import (
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"
)

func TestGetAlignedBuffer(t *testing.T) {
	assert.Equal(t, 4, len(getAlignedBuffer(make([]byte, 4))))
	assert.Equal(t, 4, len(getAlignedBuffer(make([]byte, 5))))
	assert.Equal(t, 4, len(getAlignedBuffer(make([]byte, 6))))
	assert.Equal(t, 4, len(getAlignedBuffer(make([]byte, 7))))
	assert.Equal(t, 8, len(getAlignedBuffer(make([]byte, 8))))
	assert.Equal(t, 8, len(getAlignedBuffer(make([]byte, 9))))
}

func TestComputeExpSuffixWithSmallPos(t *testing.T) {
	for pos := int64(0); pos < expSuffixBase; pos++ {
		require.Equal(t, uint32(0), ComputeExpSuffix(pos))
	}
}

func TestComputeExpSuffixWithLargePos(t *testing.T) {
	const iterations = 100
	for i := 0; i < iterations; i++ {
		curExp := uint32(1)
		for pos := expSuffixBase + rand.Int64N(expSuffixBase); pos < 1e17; pos *= 10 {
			require.Equal(t, curExp, ComputeExpSuffix(pos))
			curExp++
		}
	}
}
