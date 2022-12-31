// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package pcache

import (
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

func TestI32SRoundtrip(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		i := rapid.Int32().Draw(t, "i")
		j := si32(i32s(i))
		require.Equal(t, i, j)
	})
}

func TestI64SRoundtrip(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		i := rapid.Int64().Draw(t, "i")
		j := si64(i64s(i))
		require.Equal(t, i, j)
	})
}
