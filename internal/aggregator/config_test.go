// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package aggregator

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUpdateFromRemoteDescription(t *testing.T) {
	c := &ConfigAggregatorRemote{}
	c.updateFromRemoteDescription(`--insert-budget=500
--shard-insert-budget=1:800
--shard-insert-budget=5:200`)
	require.Equal(t, 500, c.InsertBudget)
	require.Len(t, c.ShardInsertBudget, 2)
	require.Equal(t, 800, c.ShardInsertBudget[1])
	require.Equal(t, 200, c.ShardInsertBudget[5])
}
