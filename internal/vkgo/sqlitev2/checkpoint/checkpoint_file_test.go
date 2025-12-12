// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package checkpoint

import (
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/VKCOM/statshouse/internal/vkgo/sqlitev2/checkpoint/gen2/tlsqlite"
)

// TODO add more tests

func Test_encode(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		commitOffset := rapid.Int64().Draw(t, "commit_offset")
		m := tlsqlite.MetainfoBytes{Offset: commitOffset}
		metainfo, err := decode(encode(m, nil))
		require.NoError(t, err)
		require.Equal(t, commitOffset, metainfo.Offset)
	})
}
