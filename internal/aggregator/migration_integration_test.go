// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build integration

package aggregator

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/VKCOM/statshouse/internal/chutil"
	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/hrissan/tdigest"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/clickhouse"
)

func TestV2DataParsingIntegration(t *testing.T) {
	ctx := context.Background()

	// Start ClickHouse container
	clickHouseContainer, err := clickhouse.Run(ctx,
		"clickhouse/clickhouse-server:24.3-alpine",
		clickhouse.WithDatabase("default"),
		clickhouse.WithUsername("default"),
		clickhouse.WithPassword("secret"),
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

	httpPort, err := clickHouseContainer.MappedPort(ctx, "8123/tcp")
	require.NoError(t, err)

	httpAddr := fmt.Sprintf("%s:%s", connectionHost, httpPort.Port())
	httpClient := &http.Client{Timeout: 120 * time.Second} // Increased timeout

	// Create V2 table structure
	createV2TableQuery := `CREATE TABLE statshouse_value_1h_dist (
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
	ORDER BY (metric, time, key0, key1, key2, key3, key4, key5, key6, key7, key8, key9, key10, key11, key12, key13, key14, key15, skey)`

	req := chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       httpAddr,
		User:       "default",
		Password:   "secret",
		Query:      createV2TableQuery,
	}
	resp, err := req.Execute(context.Background())
	require.NoError(t, err)
	resp.Close()

	// Step 5: Test with complete parseV2Row function from migration.go
	// Create test data with all fields
	testData := createTestData()

	// Insert test data using SQL with all fields
	err = insertTestData(httpClient, httpAddr, "default", "secret", testData)
	require.NoError(t, err)
	t.Logf("Step 5: Inserted test data with SQL for timestamp %d", testData[0].time)

	// we order by metric to get predictable order of rows, same as in the test data
	selectQuery := fmt.Sprintf(`
	SELECT metric, time, key0, key1, key2, key3, key4, key5, key6, key7, key8, key9, key10, key11, key12, key13, key14, key15, skey,
		count, min, max, sum, sumsquare, min_host, max_host, percentiles, uniq_state
	FROM statshouse_value_1h_dist
	WHERE time = toDateTime(%d) ORDER BY metric`,
		testData[0].time)

	t.Logf("Executing query: %s", selectQuery)

	req = chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       httpAddr,
		User:       "default",
		Password:   "secret",
		Query:      selectQuery,
		Format:     "RowBinary",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err = req.Execute(ctx)
	require.NoError(t, err)
	// defer resp.Close()

	t.Logf("Step 5: Query executed successfully, starting to read response")

	// Parse the RowBinary response using the complete parseV2Row function
	var parsedRows []*v2Row
	bufReader := bufio.NewReader(resp)
	for {
		row, err := parseV2Row(bufReader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err)
		}
		parsedRows = append(parsedRows, row)
		t.Logf("Step 5: Parsed row %d: metric=%d, time=%d, key0=%d, skey=%s, count=%.2f, min=%.2f, max=%.2f, sum=%.2f, sumsquare=%.2f, min_host=(%d,%.2f), max_host=(%d,%.2f)",
			len(parsedRows)-1, row.metric, row.time, row.keys[0], row.skey, row.count, row.min, row.max, row.sum, row.sumsquare,
			row.min_host.Arg, row.min_host.Val, row.max_host.Arg, row.max_host.Val)
	}

	// Validate parsed data matches original
	require.Equal(t, len(testData), len(parsedRows), "Should parse same number of rows")

	for i, expected := range testData {
		actual := parsedRows[i]
		require.Equal(t, expected.metric, actual.metric, "metric mismatch at row %d", i)
		require.Equal(t, expected.time, actual.time, "time mismatch at row %d", i)
		require.Equal(t, expected.keys, actual.keys, "keys mismatch at row %d", i)
		require.Equal(t, expected.skey, actual.skey, "skey mismatch at row %d", i)
		require.Equal(t, expected.count, actual.count, "count mismatch at row %d", i)
		require.Equal(t, expected.min, actual.min, "min mismatch at row %d", i)
		require.Equal(t, expected.max, actual.max, "max mismatch at row %d", i)
		require.Equal(t, expected.sum, actual.sum, "sum mismatch at row %d", i)
		require.Equal(t, expected.sumsquare, actual.sumsquare, "sumsquare mismatch at row %d", i)
		require.Equal(t, expected.min_host.Arg, actual.min_host.Arg, "min_host.Arg mismatch at row %d", i)
		require.Equal(t, expected.min_host.Val, actual.min_host.Val, "min_host.Val mismatch at row %d", i)
		require.Equal(t, expected.max_host.Arg, actual.max_host.Arg, "max_host.Arg mismatch at row %d", i)
		require.Equal(t, expected.max_host.Val, actual.max_host.Val, "max_host.Val mismatch at row %d", i)
		// compare two float64s with 1e-6 precision
		if expected.perc.Digest != nil && actual.perc.Digest != nil {
			require.InDelta(t, expected.perc.Digest.Quantile(0.5), actual.perc.Digest.Quantile(0.5), 1e-6, "percentile 0.5 mismatch at row %d", i)
			require.InDelta(t, expected.perc.Digest.Quantile(0.99), actual.perc.Digest.Quantile(0.99), 1e-6, "percentile 0.99 mismatch at row %d", i)
		} else {
			require.Nil(t, actual.perc.Digest, "percentiles should be nil at row %d", i)
		}
		require.Equal(t, expected.uniq.ItemsCount(), actual.uniq.ItemsCount(), "uniq_state mismatch at row %d", i)
	}

	t.Logf("Step 5 SUCCESS: Validated %d rows with complete parseV2Row function", len(parsedRows))
}

// TestV2ToV3Conversion tests that convertRowV2ToV3 works correctly and generates data that can be inserted into V3 table
func TestV2ToV3ConversionIntegration(t *testing.T) {
	ctx := context.Background()

	// Start ClickHouse container
	clickHouseContainer, err := clickhouse.Run(ctx,
		"clickhouse/clickhouse-server:24.3-alpine",
		clickhouse.WithDatabase("default"),
		clickhouse.WithUsername("default"),
		clickhouse.WithPassword("secret"),
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

	httpPort, err := clickHouseContainer.MappedPort(ctx, "8123/tcp")
	require.NoError(t, err)

	httpAddr := fmt.Sprintf("%s:%s", connectionHost, httpPort.Port())
	httpClient := &http.Client{Timeout: 120 * time.Second}

	createV3TableQuery := `CREATE TABLE statshouse_v3_1h (
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
		percentiles AggregateFunction(quantilesTDigest(0.5), Float32),
		uniq_state AggregateFunction(uniq, Int64)
	) ENGINE = AggregatingMergeTree()
	ORDER BY (metric, time, tag0, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag8, tag9, tag10, tag11, tag12, tag13, tag14, tag15, stag47)`

	req := chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       httpAddr,
		User:       "default",
		Password:   "secret",
		Query:      createV3TableQuery,
	}
	resp, err := req.Execute(context.Background())
	require.NoError(t, err)
	resp.Close()

	testData := createTestData()
	t.Logf("Generated %d test rows", len(testData))

	body := make([]byte, 0, 1024)
	for _, row := range testData {
		body = convertRowV2ToV3(body, row)
	}

	// Insert data
	insertQuery := `INSERT INTO statshouse_v3_1h(
		metric,time,
		tag0,tag1,tag2,tag3,tag4,tag5,tag6,tag7,
		tag8,tag9,tag10,tag11,tag12,tag13,tag14,tag15,stag47,
		count,min,max,sum,sumsquare,
		min_host,max_host,percentiles,uniq_state
	)
	FORMAT RowBinary`
	insertReq := &chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       httpAddr,
		User:       "default",
		Password:   "secret",
		Query:      insertQuery,
		Body:       body,
		UrlParams:  map[string]string{"input_format_values_interpret_expressions": "0"},
	}
	resp, err = insertReq.Execute(context.Background())
	require.NoError(t, err)
	resp.Close()

	t.Logf("SUCCESS: Inserted %d converted rows into V3 table", len(testData))
}

func createTestData() []*v2Row {
	perc1 := tdigest.New()
	perc1.Add(0.5, 2.5)
	uniq := &data_model.ChUnique{}
	uniq.Insert(100)

	perc2 := tdigest.New()
	perc2.Add(0.25, 1.75)
	perc2.Add(0.75, 3.25)

	perc3 := tdigest.New()
	perc3.Add(0.1, 0.5)
	perc3.Add(0.9, 4.5)

	testData := []*v2Row{
		{
			metric:    1,
			time:      1733000400,                                                    // Fixed timestamp for testing
			keys:      [16]int32{123, 567, 8, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 0}, // Match SQL insert
			skey:      "test_skey_1",
			count:     10.0, // Step 5: Use default values
			min:       1.0,
			max:       5.0,
			sum:       30.0,
			sumsquare: 100.0,
			perc:      &data_model.ChDigest{Digest: perc1},
			uniq:      uniq,
			min_host:  data_model.ArgMinInt32Float32{ArgMinMaxInt32Float32: data_model.ArgMinMaxInt32Float32{Arg: 1234132, Val: 1.5}},
			max_host:  data_model.ArgMaxInt32Float32{ArgMinMaxInt32Float32: data_model.ArgMinMaxInt32Float32{Arg: 1065353216, Val: -2}},
		},
		{
			metric:    2,
			time:      1733000400,
			keys:      [16]int32{456, 789, 12, 0, 0, 0, 0, 0, 15, 0, 0, 0, 0, 0, 0, 0},
			skey:      "test_skey_2",
			count:     15.0,
			min:       0.5,
			max:       8.0,
			sum:       45.0,
			sumsquare: 200.0,
			perc:      &data_model.ChDigest{Digest: perc2},
			uniq:      uniq,
			min_host:  data_model.ArgMinInt32Float32{ArgMinMaxInt32Float32: data_model.ArgMinMaxInt32Float32{Arg: 2345678, Val: 0.5}},
			max_host:  data_model.ArgMaxInt32Float32{ArgMinMaxInt32Float32: data_model.ArgMinMaxInt32Float32{Arg: 3456789, Val: 8.0}},
		},
		{
			metric:    3,
			time:      1733000400,
			keys:      [16]int32{789, 123, 16, 0, 0, 0, 0, 0, 21, 0, 0, 0, 0, 0, 0, 0},
			skey:      "test_skey_3",
			count:     20.0,
			min:       0.1,
			max:       10.0,
			sum:       60.0,
			sumsquare: 300.0,
			perc:      &data_model.ChDigest{Digest: perc3},
			uniq:      uniq,
			min_host:  data_model.ArgMinInt32Float32{ArgMinMaxInt32Float32: data_model.ArgMinMaxInt32Float32{Arg: 4567890, Val: 0.1}},
			max_host:  data_model.ArgMaxInt32Float32{ArgMinMaxInt32Float32: data_model.ArgMinMaxInt32Float32{Arg: 5678901, Val: 10.0}},
		},
	}
	return testData
}

func insertTestData(httpClient *http.Client, httpAddr, user, password string, testData []*v2Row) error {
	// Insert test data using SQL with all fields including percentiles
	if len(testData) == 0 {
		return fmt.Errorf("no test data provided")
	}

	// For now, we'll use the first row's data to generate a SELECT statement
	// This is a simplified approach - in a real implementation, you might want to handle multiple rows
	for _, row := range testData {
		insertQuery := fmt.Sprintf(`
			INSERT INTO statshouse_value_1h_dist (
				metric, time, key0, key1, key2, key3, key4, key5, key6, key7, key8, key9, key10, key11, key12, key13, key14, key15, skey,
				count, min, max, sum, sumsquare, percentiles, min_host, max_host, uniq_state
			)
			SELECT 
				%d as metric,
				toDateTime(%d) as time,
				%d as key0, %d as key1, %d as key2, %d as key3, %d as key4, %d as key5, %d as key6, %d as key7,
				%d as key8, %d as key9, %d as key10, %d as key11, %d as key12, %d as key13, %d as key14, %d as key15,
				'%s' as skey,
				%.2f as count,
				%.2f as min,
				%.2f as max,
				%.2f as sum,
				%.2f as sumsquare,
				quantilesTDigestState(0.5)(toFloat32(%.2f)) as percentiles,
				argMinState(toInt32(%d), toFloat32(%.2f)) as min_host,
				argMaxState(toInt32(%d), toFloat32(%.2f)) as max_host,
				uniqState(toInt64(100)) as uniq_state
			`,
			row.metric, row.time,
			row.keys[0], row.keys[1], row.keys[2], row.keys[3], row.keys[4], row.keys[5], row.keys[6], row.keys[7],
			row.keys[8], row.keys[9], row.keys[10], row.keys[11], row.keys[12], row.keys[13], row.keys[14], row.keys[15],
			row.skey,
			row.count, row.min, row.max, row.sum, row.sumsquare,
			row.min, // Use min value for percentile test data
			row.min_host.Arg, row.min_host.Val, row.max_host.Arg, row.max_host.Val)

		req := &chutil.ClickHouseHttpRequest{
			HttpClient: httpClient,
			Addr:       httpAddr,
			User:       user,
			Password:   password,
			Query:      insertQuery,
		}
		resp, err := req.Execute(context.Background())
		if err != nil {
			return fmt.Errorf("failed to execute insert query: %w", err)
		}
		defer resp.Close()
	}

	return nil
}

// TestMigrateSingleHourIntegration tests the complete migrateSingleHour function
func TestMigrateSingleHourIntegration(t *testing.T) {
	ctx := context.Background()

	// Start ClickHouse container
	clickHouseContainer, err := clickhouse.Run(ctx,
		"clickhouse/clickhouse-server:24.3-alpine",
		clickhouse.WithDatabase("default"),
		clickhouse.WithUsername("default"),
		clickhouse.WithPassword("secret"),
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

	httpPort, err := clickHouseContainer.MappedPort(ctx, "8123/tcp")
	require.NoError(t, err)

	httpAddr := fmt.Sprintf("%s:%s", connectionHost, httpPort.Port())
	httpClient := &http.Client{Timeout: 120 * time.Second}

	// Create V2 table
	createV2TableQuery := `CREATE TABLE statshouse_value_1h_dist (
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
	ORDER BY (metric, time, key0, key1, key2, key3, key4, key5, key6, key7, key8, key9, key10, key11, key12, key13, key14, key15, skey)`

	req := &chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       httpAddr,
		User:       "default",
		Password:   "secret",
		Query:      createV2TableQuery,
	}
	resp, err := req.Execute(context.Background())
	require.NoError(t, err)
	resp.Close()

	// Create V3 table
	createV3TableQuery := `CREATE TABLE statshouse_v3_1h (
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
		percentiles AggregateFunction(quantilesTDigest(0.5), Float32),
		uniq_state AggregateFunction(uniq, Int64)
	) ENGINE = AggregatingMergeTree()
	ORDER BY (metric, time, tag0, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag8, tag9, tag10, tag11, tag12, tag13, tag14, tag15, stag47)`

	req.Query = createV3TableQuery
	resp, err = req.Execute(context.Background())
	require.NoError(t, err)
	resp.Close()

	// Create test data and insert into V2 table
	testData := createTestData()
	err = insertTestData(httpClient, httpAddr, "default", "secret", testData)
	require.NoError(t, err)
	t.Logf("Inserted %d test rows into V2 table", len(testData))

	// Test migrateSingleHour
	testHour := uint32(testData[0].time)
	shardKey := int32(1) // Test shard 1

	err = TestMigrateSingleHour(httpAddr, "default", "secret", testHour, shardKey)
	require.NoError(t, err)
	t.Logf("Migration completed successfully for hour %d, shard %d", testHour, shardKey)

	// Verify migration results
	checkQuery := fmt.Sprintf(`
		SELECT count() as cnt 
		FROM statshouse_v3_1h 
		WHERE time = toDateTime(%d)`, testHour)

	req.Query = checkQuery
	resp, err = req.Execute(context.Background())
	require.NoError(t, err)

	var migratedCount uint64
	_, err = fmt.Fscanf(resp, "%d", &migratedCount)
	require.NoError(t, err)
	resp.Close()

	// Should have migrated the rows where metric % 16 = 0 (shard 1)
	expectedCount := 0
	for _, row := range testData {
		if row.metric%16 == 0 { // shard 1 processes metrics where metric % 16 = 0
			expectedCount++
		}
	}

	require.Equal(t, uint64(expectedCount), migratedCount, "Should migrate correct number of rows for shard")
	t.Logf("SUCCESS: Migrated %d rows as expected", migratedCount)
}

// TestFullMigrationE2E tests the complete migration system end-to-end
func TestFullMigrationE2EIntegration(t *testing.T) {
	ctx := context.Background()

	// Start ClickHouse container
	clickHouseContainer, err := clickhouse.Run(ctx,
		"clickhouse/clickhouse-server:24.3-alpine",
		clickhouse.WithDatabase("default"),
		clickhouse.WithUsername("default"),
		clickhouse.WithPassword("secret"),
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

	httpPort, err := clickHouseContainer.MappedPort(ctx, "8123/tcp")
	require.NoError(t, err)

	httpAddr := fmt.Sprintf("%s:%s", connectionHost, httpPort.Port())
	httpClient := &http.Client{Timeout: 120 * time.Second}

	// Create all necessary tables (V2, V3, migration state, logs)
	t.Log("Creating database tables...")
	err = createAllTestTables(httpClient, httpAddr, "default", "secret")
	require.NoError(t, err)

	// Create comprehensive test data for multiple hours
	t.Log("Creating test data for multiple hours...")
	testHours := []time.Time{
		time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC),
		time.Date(2025, 1, 1, 11, 0, 0, 0, time.UTC),
		time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC),
	}

	totalRowsInserted := 0
	for _, hour := range testHours {
		testData := createTestDataForHour(hour)
		err = insertTestDataForHour(httpClient, httpAddr, "default", "secret", testData)
		require.NoError(t, err)
		totalRowsInserted += len(testData)
		t.Logf("Inserted %d rows for hour %s", len(testData), hour.Format("2006-01-02 15:04:05"))
	}

	// Create mock aggregator instances for testing
	aggregators := createMockAggregators(httpAddr, "default", "secret", 3) // Test with 3 shards

	// Test migration coordination
	t.Log("Starting migration simulation...")

	// Run migrations for each aggregator in separate goroutines
	var wg sync.WaitGroup
	migrationResults := make(chan error, len(aggregators))

	for i, agg := range aggregators {
		wg.Add(1)
		go func(aggIndex int, aggregator *MockAggregator) {
			defer wg.Done()

			// Simulate migration for this aggregator
			err := simulateMigrationForAggregator(aggregator, testHours)
			if err != nil {
				t.Logf("Aggregator %d migration failed: %v", aggIndex, err)
				migrationResults <- fmt.Errorf("aggregator %d failed: %w", aggIndex, err)
			} else {
				t.Logf("Aggregator %d migration completed successfully", aggIndex)
				migrationResults <- nil
			}
		}(i, agg)
	}

	// Wait for all migrations to complete
	wg.Wait()
	close(migrationResults)

	// Check results
	var migrationErrors []error
	for err := range migrationResults {
		if err != nil {
			migrationErrors = append(migrationErrors, err)
		}
	}

	require.Empty(t, migrationErrors, "All migrations should succeed")

	// Verify final state
	t.Log("Verifying migration results...")

	// Check that all data was migrated correctly
	for _, hour := range testHours {
		migratedCount, err := countMigratedRowsForHour(httpClient, httpAddr, "default", "secret", hour)
		require.NoError(t, err)

		originalCount, err := countOriginalRowsForHour(httpClient, httpAddr, "default", "secret", hour)
		require.NoError(t, err)

		t.Logf("Hour %s: Original=%d, Migrated=%d", hour.Format("2006-01-02 15:04:05"), originalCount, migratedCount)
		require.Equal(t, originalCount, migratedCount, "All rows should be migrated for hour %s", hour.Format("2006-01-02 15:04:05"))
	}

	// Check migration state table
	completedShards, err := countCompletedShards(httpClient, httpAddr, "default", "secret", testHours)
	require.NoError(t, err)
	require.Equal(t, len(aggregators)*len(testHours), completedShards, "All shard-hour combinations should be completed")

	// Check migration logs
	logCount, err := countMigrationLogs(httpClient, httpAddr, "default", "secret")
	require.NoError(t, err)
	require.Greater(t, logCount, 0, "Should have migration logs")

	t.Log("SUCCESS: Full E2E migration test completed successfully!")
}

// MockAggregator represents a mock aggregator for testing
type MockAggregator struct {
	shardKey   int32
	InsertAddr string
	user       string
	password   string
	httpClient *http.Client
}

// Helper functions for E2E testing

func createAllTestTables(httpClient *http.Client, httpAddr, user, password string) error {
	queries := []string{
		// V2 table
		`CREATE TABLE IF NOT EXISTS statshouse_value_1h_dist (
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
		ORDER BY (metric, time, key0, key1, key2, key3, key4, key5, key6, key7, key8, key9, key10, key11, key12, key13, key14, key15, skey)`,

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
			percentiles AggregateFunction(quantilesTDigest(0.5), Float32),
			uniq_state AggregateFunction(uniq, Int64)
		) ENGINE = AggregatingMergeTree()
		ORDER BY (metric, time, tag0, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag8, tag9, tag10, tag11, tag12, tag13, tag14, tag15, stag47)`,

		// Migration state table
		`CREATE TABLE IF NOT EXISTS migration_state (
			shard_key Int32,
			current_hour DateTime,
			status String,
			started_at DateTime,
			completed_at Nullable(DateTime),
			rows_migrated UInt64,
			error_message String,
			retry_count UInt32
		) ENGINE = ReplacingMergeTree(started_at)
		ORDER BY (shard_key, current_hour)`,

		// Migration logs table
		`CREATE TABLE IF NOT EXISTS migration_logs (
			timestamp DateTime,
			shard_key Int32,
			hour DateTime,
			level String,
			message String,
			details String
		) ENGINE = MergeTree()
		ORDER BY (timestamp, shard_key)`,
	}

	for _, query := range queries {
		req := &chutil.ClickHouseHttpRequest{
			HttpClient: httpClient,
			Addr:       httpAddr,
			User:       user,
			Password:   password,
			Query:      query,
		}
		resp, err := req.Execute(context.Background())
		if err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
		resp.Close()
	}

	return nil
}

func createTestDataForHour(hour time.Time) []*v2Row {
	perc1 := tdigest.New()
	perc1.Add(0.5, 2.5)
	uniq := &data_model.ChUnique{}
	uniq.Insert(100)

	perc2 := tdigest.New()
	perc2.Add(0.25, 1.75)
	perc2.Add(0.75, 3.25)

	// Create metrics that will be distributed across different shards
	testData := []*v2Row{
		{
			metric:    1, // metric % 16 = 1 (shard 2)
			time:      uint32(hour.Unix()),
			keys:      [16]int32{100, 200, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			skey:      fmt.Sprintf("test_hour_%s_metric_1", hour.Format("15")),
			count:     10.0,
			min:       1.0,
			max:       5.0,
			sum:       30.0,
			sumsquare: 100.0,
			perc:      &data_model.ChDigest{Digest: perc1},
			uniq:      uniq,
			min_host:  data_model.ArgMinInt32Float32{ArgMinMaxInt32Float32: data_model.ArgMinMaxInt32Float32{Arg: 1000, Val: 1.0}},
			max_host:  data_model.ArgMaxInt32Float32{ArgMinMaxInt32Float32: data_model.ArgMinMaxInt32Float32{Arg: 2000, Val: 5.0}},
		},
		{
			metric:    16, // metric % 16 = 0 (shard 1)
			time:      uint32(hour.Unix()),
			keys:      [16]int32{300, 400, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			skey:      fmt.Sprintf("test_hour_%s_metric_16", hour.Format("15")),
			count:     20.0,
			min:       0.5,
			max:       8.0,
			sum:       60.0,
			sumsquare: 200.0,
			perc:      &data_model.ChDigest{Digest: perc2},
			uniq:      uniq,
			min_host:  data_model.ArgMinInt32Float32{ArgMinMaxInt32Float32: data_model.ArgMinMaxInt32Float32{Arg: 3000, Val: 0.5}},
			max_host:  data_model.ArgMaxInt32Float32{ArgMinMaxInt32Float32: data_model.ArgMinMaxInt32Float32{Arg: 4000, Val: 8.0}},
		},
		{
			metric:    34, // metric % 16 = 2 (shard 3)
			time:      uint32(hour.Unix()),
			keys:      [16]int32{500, 600, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			skey:      fmt.Sprintf("test_hour_%s_metric_34", hour.Format("15")),
			count:     15.0,
			min:       2.0,
			max:       7.0,
			sum:       45.0,
			sumsquare: 150.0,
			perc:      &data_model.ChDigest{Digest: perc1},
			uniq:      uniq,
			min_host:  data_model.ArgMinInt32Float32{ArgMinMaxInt32Float32: data_model.ArgMinMaxInt32Float32{Arg: 5000, Val: 2.0}},
			max_host:  data_model.ArgMaxInt32Float32{ArgMinMaxInt32Float32: data_model.ArgMinMaxInt32Float32{Arg: 6000, Val: 7.0}},
		},
	}

	return testData
}

func insertTestDataForHour(httpClient *http.Client, httpAddr, user, password string, testData []*v2Row) error {
	return insertTestData(httpClient, httpAddr, user, password, testData)
}

func createMockAggregators(httpAddr, user, password string, numShards int) []*MockAggregator {
	var aggregators []*MockAggregator

	for i := 1; i <= numShards; i++ {
		agg := &MockAggregator{
			shardKey:   int32(i),
			InsertAddr: httpAddr,
			user:       user,
			password:   password,
			httpClient: &http.Client{Timeout: 120 * time.Second},
		}
		aggregators = append(aggregators, agg)
	}

	return aggregators
}

func simulateMigrationForAggregator(agg *MockAggregator, hours []time.Time) error {
	// Simulate migration for each hour
	for _, hour := range hours {
		// Check if this shard has data for this hour
		hasDataQuery := fmt.Sprintf(`
			SELECT count() as cnt
			FROM statshouse_value_1h_dist 
			WHERE time = toDateTime(%d) AND metric %% 16 = %d`,
			hour.Unix(), agg.shardKey-1)

		req := &chutil.ClickHouseHttpRequest{
			HttpClient: agg.httpClient,
			Addr:       agg.InsertAddr,
			User:       agg.user,
			Password:   agg.password,
			Query:      hasDataQuery,
		}
		resp, err := req.Execute(context.Background())
		if err != nil {
			return fmt.Errorf("failed to check for data: %w", err)
		}

		var count uint64
		if _, err := fmt.Fscanf(resp, "%d", &count); err == nil && count > 0 {
			resp.Close()

			// Migrate this hour
			err = migrateSingleHour(agg.httpClient, agg.InsertAddr, agg.user, agg.password, uint32(hour.Unix()), agg.shardKey)
			if err != nil {
				return fmt.Errorf("failed to migrate hour %s for shard %d: %w", hour.Format("2006-01-02 15:04:05"), agg.shardKey, err)
			}

			// Update migration state
			updateQuery := fmt.Sprintf(`
				INSERT INTO migration_state 
				(shard_key, current_hour, status, started_at, completed_at, rows_migrated, error_message, retry_count)
				VALUES (%d, toDateTime(%d), 'completed', '%s', '%s', %d, '', 0)`,
				agg.shardKey, hour.Unix(), time.Now().Format("2006-01-02 15:04:05"),
				time.Now().Format("2006-01-02 15:04:05"), count)

			req.Query = updateQuery
			resp2, err := req.Execute(context.Background())
			if err != nil {
				return fmt.Errorf("failed to update migration state: %w", err)
			}
			resp2.Close()
		} else {
			resp.Close()
		}
	}

	return nil
}

func countMigratedRowsForHour(httpClient *http.Client, httpAddr, user, password string, hour time.Time) (uint64, error) {
	countQuery := fmt.Sprintf(`
		SELECT count() as cnt 
		FROM statshouse_v3_1h 
		WHERE time = toDateTime(%d)`, hour.Unix())

	req := &chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       httpAddr,
		User:       user,
		Password:   password,
		Query:      countQuery,
	}
	resp, err := req.Execute(context.Background())
	if err != nil {
		return 0, fmt.Errorf("failed to count migrated rows: %w", err)
	}
	defer resp.Close()

	var count uint64
	if _, err := fmt.Fscanf(resp, "%d", &count); err != nil {
		return 0, fmt.Errorf("failed to parse migrated count: %w", err)
	}

	return count, nil
}

func countOriginalRowsForHour(httpClient *http.Client, httpAddr, user, password string, hour time.Time) (uint64, error) {
	countQuery := fmt.Sprintf(`
		SELECT count() as cnt 
		FROM statshouse_value_1h_dist 
		WHERE time = toDateTime(%d)`, hour.Unix())

	req := &chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       httpAddr,
		User:       user,
		Password:   password,
		Query:      countQuery,
	}
	resp, err := req.Execute(context.Background())
	if err != nil {
		return 0, fmt.Errorf("failed to count original rows: %w", err)
	}
	defer resp.Close()

	var count uint64
	if _, err := fmt.Fscanf(resp, "%d", &count); err != nil {
		return 0, fmt.Errorf("failed to parse original count: %w", err)
	}

	return count, nil
}

func countCompletedShards(httpClient *http.Client, httpAddr, user, password string, hours []time.Time) (int, error) {
	// Count completed shard-hour combinations
	var conditions []string
	for _, hour := range hours {
		conditions = append(conditions, fmt.Sprintf("current_hour = toDateTime(%d)", hour.Unix()))
	}

	countQuery := fmt.Sprintf(`
		SELECT count() as cnt 
		FROM migration_state 
		WHERE status = 'completed' AND (%s)`,
		fmt.Sprintf("(%s)", conditions[0]))

	if len(conditions) > 1 {
		for _, condition := range conditions[1:] {
			countQuery += fmt.Sprintf(" OR (%s)", condition)
		}
	}

	req := &chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       httpAddr,
		User:       user,
		Password:   password,
		Query:      countQuery,
	}
	resp, err := req.Execute(context.Background())
	if err != nil {
		return 0, fmt.Errorf("failed to count completed shards: %w", err)
	}
	defer resp.Close()

	var count int
	if _, err := fmt.Fscanf(resp, "%d", &count); err != nil {
		return 0, fmt.Errorf("failed to parse completed count: %w", err)
	}

	return count, nil
}

func countMigrationLogs(httpClient *http.Client, httpAddr, user, password string) (int, error) {
	countQuery := `SELECT count() as cnt FROM migration_logs`

	req := &chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       httpAddr,
		User:       user,
		Password:   password,
		Query:      countQuery,
	}
	resp, err := req.Execute(context.Background())
	if err != nil {
		return 0, fmt.Errorf("failed to count migration logs: %w", err)
	}
	defer resp.Close()

	var count int
	if _, err := fmt.Fscanf(resp, "%d", &count); err != nil {
		return 0, fmt.Errorf("failed to parse log count: %w", err)
	}

	return count, nil
}
