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

func TestParseMigrationTimeRange(t *testing.T) {
	tests := []struct {
		name          string
		timeRange     string
		expectedStart uint32
		expectedEnd   uint32
	}{
		{
			name:          "empty string disables migration",
			timeRange:     "",
			expectedStart: 0,
			expectedEnd:   0,
		},
		{
			name:          "invalid format returns zero values",
			timeRange:     "invalid",
			expectedStart: 0,
			expectedEnd:   0,
		},
		{
			name:          "invalid format with single number",
			timeRange:     "1234567890",
			expectedStart: 0,
			expectedEnd:   0,
		},
		{
			name:          "invalid format with multiple dashes",
			timeRange:     "1234567890-9876543210-1111111111",
			expectedStart: 0,
			expectedEnd:   0,
		},
		{
			name:          "start equals end returns zero values",
			timeRange:     "1234567890-1234567890",
			expectedStart: 0,
			expectedEnd:   0,
		},
		{
			name:          "start less than end returns zero values",
			timeRange:     "1-10",
			expectedStart: 0,
			expectedEnd:   0,
		},
		{
			name:          "valid range with start greater than end",
			timeRange:     "10-1",
			expectedStart: 10,
			expectedEnd:   1,
		},
		{
			name:          "valid range with whitespace",
			timeRange:     "  10  -  1  ",
			expectedStart: 10,
			expectedEnd:   1,
		},
		{
			name:          "valid range at boundary",
			timeRange:     "4294967295-1",
			expectedStart: 4294967295,
			expectedEnd:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ConfigAggregatorRemote{
				MigrationTimeRange: tt.timeRange,
			}
			startTs, endTs := c.ParseMigrationTimeRange()
			require.Equal(t, tt.expectedStart, startTs, "start timestamp mismatch")
			require.Equal(t, tt.expectedEnd, endTs, "end timestamp mismatch")
		})
	}
}
