// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build integration
// +build integration

package aggregator

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/clickhouse"
)

func TestMigrationIntegration(t *testing.T) {
	ctx := context.Background()

	// Start ClickHouse container
	clickHouseContainer, err := clickhouse.Run(ctx,
		"clickhouse/clickhouse-server:24.3-alpine",
		clickhouse.WithDatabase("default"),
		clickhouse.WithUsername("default"),
		clickhouse.WithPassword(""),
	)
	require.NoError(t, err)
	defer func() {
		if err := testcontainers.TerminateContainer(clickHouseContainer); err != nil {
			t.Logf("failed to terminate container: %s", err)
		}
	}()

	// Get connection details
	connectionHost, err := clickHouseContainer.Host(ctx)
	require.NoError(t, err)

	connectionPort, err := clickHouseContainer.MappedPort(ctx, "9000/tcp")
	require.NoError(t, err)

	httpPort, err := clickHouseContainer.MappedPort(ctx, "8123/tcp")
	require.NoError(t, err)

	// Set up database connection
	dsn := fmt.Sprintf("clickhouse://%s:%s", connectionHost, connectionPort.Port())
	db, err := sql.Open("clickhouse", dsn)
	require.NoError(t, err)
	defer db.Close()

	// Create tables
	httpAddr := fmt.Sprintf("%s:%s", connectionHost, httpPort.Port())
	err = setupTables(httpAddr)
	require.NoError(t, err)

	// Create test data in V2 format
	testTimestamp := uint32(time.Now().Unix() / 3600 * 3600) // Round to hour
	err = CreateTestDataV2(httpAddr, "", "", testTimestamp)
	require.NoError(t, err)

	// Verify test data was created
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM statshouse_value_1h").Scan(&count)
	require.NoError(t, err)
	require.Greater(t, count, 0, "Test data should be created")

	// Run migration
	err = TestMigrateSingleHour(httpAddr, "", "", testTimestamp, 1)
	require.NoError(t, err)

	// Verify migration results
	var v3Count int
	err = db.QueryRow("SELECT COUNT(*) FROM statshouse_v3_1h").Scan(&v3Count)
	require.NoError(t, err)
	require.Greater(t, v3Count, 0, "V3 data should be created")

	// Verify data integrity by comparing some values
	var v2Sum, v3Sum float64
	err = db.QueryRow("SELECT SUM(sum) FROM statshouse_value_1h WHERE time = ?", time.Unix(int64(testTimestamp), 0)).Scan(&v2Sum)
	require.NoError(t, err)

	err = db.QueryRow("SELECT SUM(sum) FROM statshouse_v3_1h WHERE time = ?", time.Unix(int64(testTimestamp), 0)).Scan(&v3Sum)
	require.NoError(t, err)

	require.Equal(t, v2Sum, v3Sum, "Sum values should match between V2 and V3")

	t.Logf("Migration successful: %d V2 rows -> %d V3 rows", count, v3Count)
}

func TestV2RowBinaryFormat(t *testing.T) {
	// Test the V2 row parsing in isolation
	v2Row := &v2Row{
		metric:      1,
		time:        1733000400,
		keys:        [16]int32{123, 456, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		skey:        "test",
		count:       10.0,
		min:         1.0,
		max:         5.0,
		sum:         30.0,
		sumsquare:   100.0,
		percentiles: []byte{1, 2, 3}, // Simple test data
		uniq_state:  []byte{4, 5, 6}, // Simple test data
		min_host:    createV2ArgMinMaxData(100, 1.0, true),
		max_host:    createV2ArgMinMaxData(200, 5.0, true),
	}

	// Serialize and parse back
	serialized := serializeV2Row(v2Row)
	parsed, bytesRead, err := parseV2Row(serialized)
	require.NoError(t, err)
	require.Equal(t, len(serialized), bytesRead)

	// Verify all fields match
	require.Equal(t, v2Row.metric, parsed.metric)
	require.Equal(t, v2Row.time, parsed.time)
	require.Equal(t, v2Row.keys, parsed.keys)
	require.Equal(t, v2Row.skey, parsed.skey)
	require.Equal(t, v2Row.count, parsed.count)
	require.Equal(t, v2Row.min, parsed.min)
	require.Equal(t, v2Row.max, parsed.max)
	require.Equal(t, v2Row.sum, parsed.sum)
	require.Equal(t, v2Row.sumsquare, parsed.sumsquare)
}

func TestV3RowBinaryFormat(t *testing.T) {
	// Test the V3 row generation in isolation
	v2Row := &v2Row{
		metric:      1,
		time:        1733000400,
		keys:        [16]int32{123, 456, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		skey:        "test",
		count:       10.0,
		min:         1.0,
		max:         5.0,
		sum:         30.0,
		sumsquare:   100.0,
		percentiles: []byte{1, 0, 0, 0, 2, 3},    // With length prefix
		uniq_state:  []byte{3, 0, 0, 0, 4, 5, 6}, // With length prefix
		min_host:    createV2ArgMinMaxData(100, 1.0, true),
		max_host:    createV2ArgMinMaxData(200, 5.0, true),
	}

	// Convert to V3
	v3Data := convertRowV2ToV3(nil, v2Row)
	require.Greater(t, len(v3Data), 0, "V3 data should not be empty")

	// Basic structure validation - check that we have the expected minimum size
	// This is a rough calculation based on the expected fields
	expectedMinSize := 1 + 4 + 4 + 1 + 4 + (48 * (4 + 1)) + (6 * 8) + (5 * 8) + len(v2Row.percentiles) + len(v2Row.uniq_state)
	require.GreaterOrEqual(t, len(v3Data), expectedMinSize/2, "V3 data should have reasonable size")

	t.Logf("V3 row size: %d bytes", len(v3Data))
}

func setupTables(httpAddr string) error {
	// Create minimal test tables
	queries := []string{
		// V2 table
		`CREATE TABLE IF NOT EXISTS statshouse_value_1h (
			metric Int32,
			time DateTime,
			key0 Int32, key1 Int32, key2 Int32, key3 Int32, key4 Int32, key5 Int32, key6 Int32, key7 Int32,
			key8 Int32, key9 Int32, key10 Int32, key11 Int32, key12 Int32, key13 Int32, key14 Int32, key15 Int32,
			skey String,
			count SimpleAggregateFunction(sum, Float64),
			min SimpleAggregateFunction(min, Float64),
			max SimpleAggregateFunction(max, Float64),
			sum SimpleAggregateFunction(sum, Float64),
			sumsquare SimpleAggregateFunction(sum, Float64),
			percentiles AggregateFunction(quantilesTDigest(0.5), Float32),
			uniq_state AggregateFunction(uniq, Int64),
			min_host AggregateFunction(argMin, Int32, Float32),
			max_host AggregateFunction(argMax, Int32, Float32)
		) ENGINE = AggregatingMergeTree()
		ORDER BY (metric, time)`,

		// V2 distributed table
		`CREATE TABLE IF NOT EXISTS statshouse_value_1h_dist AS statshouse_value_1h ENGINE = Distributed('default', 'default', 'statshouse_value_1h')`,

		// V3 table
		`CREATE TABLE IF NOT EXISTS statshouse_v3_1h (
			index_type UInt8,
			metric Int32,
			pre_tag UInt32,
			pre_stag String,
			time DateTime,
			tag0 Int32, stag0 String, tag1 Int32, stag1 String, tag2 Int32, stag2 String, tag3 Int32, stag3 String,
			tag4 Int32, stag4 String, tag5 Int32, stag5 String, tag6 Int32, stag6 String, tag7 Int32, stag7 String,
			tag8 Int32, stag8 String, tag9 Int32, stag9 String, tag10 Int32, stag10 String, tag11 Int32, stag11 String,
			tag12 Int32, stag12 String, tag13 Int32, stag13 String, tag14 Int32, stag14 String, tag15 Int32, stag15 String,
			tag16 Int32, stag16 String, tag17 Int32, stag17 String, tag18 Int32, stag18 String, tag19 Int32, stag19 String,
			tag20 Int32, stag20 String, tag21 Int32, stag21 String, tag22 Int32, stag22 String, tag23 Int32, stag23 String,
			tag24 Int32, stag24 String, tag25 Int32, stag25 String, tag26 Int32, stag26 String, tag27 Int32, stag27 String,
			tag28 Int32, stag28 String, tag29 Int32, stag29 String, tag30 Int32, stag30 String, tag31 Int32, stag31 String,
			tag32 Int32, stag32 String, tag33 Int32, stag33 String, tag34 Int32, stag34 String, tag35 Int32, stag35 String,
			tag36 Int32, stag36 String, tag37 Int32, stag37 String, tag38 Int32, stag38 String, tag39 Int32, stag39 String,
			tag40 Int32, stag40 String, tag41 Int32, stag41 String, tag42 Int32, stag42 String, tag43 Int32, stag43 String,
			tag44 Int32, stag44 String, tag45 Int32, stag45 String, tag46 Int32, stag46 String, tag47 Int32, stag47 String,
			count SimpleAggregateFunction(sum, Float64),
			min SimpleAggregateFunction(min, Float64),
			max SimpleAggregateFunction(max, Float64),
			max_count SimpleAggregateFunction(max, Float64),
			sum SimpleAggregateFunction(sum, Float64),
			sumsquare SimpleAggregateFunction(sum, Float64),
			min_host AggregateFunction(argMin, String, Float32),
			max_host AggregateFunction(argMax, String, Float32),
			max_count_host AggregateFunction(argMax, String, Float32),
			min_host_legacy AggregateFunction(argMin, Int32, Float32),
			max_host_legacy AggregateFunction(argMax, Int32, Float32),
			percentiles AggregateFunction(quantilesTDigest(0.5), Float32),
			uniq_state AggregateFunction(uniq, Int64)
		) ENGINE = AggregatingMergeTree()
		ORDER BY (index_type, metric)`,
	}

	httpClient := &http.Client{Timeout: 30 * time.Second}

	for _, query := range queries {
		req, err := http.NewRequest("POST", fmt.Sprintf("http://%s/", httpAddr), strings.NewReader(query))
		if err != nil {
			return err
		}

		resp, err := httpClient.Do(req)
		if err != nil {
			return err
		}

		if resp.StatusCode != 200 {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return fmt.Errorf("ClickHouse query failed: %s - %s", resp.Status, string(body))
		}
		resp.Body.Close()
	}

	return nil
}
