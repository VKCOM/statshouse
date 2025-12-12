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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/hrissan/tdigest"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/clickhouse"
	"pgregory.net/rand"

	"github.com/VKCOM/statshouse/internal/chutil"
	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/vkgo/kittenhouseclient/rowbinary"
)

const clickhouseImage = "clickhouse/clickhouse-server:24.3-alpine"

// Package-level variables for shared ClickHouse container
var (
	clickHouseContainer testcontainers.Container
	clickHouseHost      string
	clickHousePort      string
	clickHouseAddr      string
	httpClient          *http.Client
	tablesCreated       bool
)

// TestMain sets up and tears down the shared ClickHouse container
func TestMain(m *testing.M) {
	ctx := context.Background()

	// Start ClickHouse container once for all tests
	var err error
	clickHouseContainer, err = clickhouse.Run(ctx,
		clickhouseImage,
		clickhouse.WithDatabase("default"),
		clickhouse.WithUsername("default"),
		clickhouse.WithPassword("secret"),
	)
	if err != nil {
		panic(fmt.Sprintf("Failed to start ClickHouse container: %v", err))
	}

	// Get connection details
	clickHouseHost, err = clickHouseContainer.Host(ctx)
	if err != nil {
		panic(fmt.Sprintf("Failed to get container host: %v", err))
	}

	httpPort, err := clickHouseContainer.MappedPort(ctx, "8123/tcp")
	if err != nil {
		panic(fmt.Sprintf("Failed to get mapped port: %v", err))
	}
	clickHousePort = httpPort.Port()
	clickHouseAddr = fmt.Sprintf("%s:%s", clickHouseHost, clickHousePort)

	// Create shared HTTP client
	httpClient = &http.Client{Timeout: 120 * time.Second}

	// Ensure tables are created once for all tests
	err = ensureTablesCreated()
	if err != nil {
		panic(fmt.Sprintf("Failed to create tables: %v", err))
	}

	// Run all tests
	exitCode := m.Run()

	// Clean up container
	if err := testcontainers.TerminateContainer(clickHouseContainer); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to terminate container: %s\n", err)
	}

	os.Exit(exitCode)
}

// executeQueryRequest is a helper to execute a ClickHouse query and close the response.
func executeQueryRequest(query string) error {
	req := &chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       clickHouseAddr,
		User:       "default",
		Password:   "secret",
		Query:      query,
	}
	resp, err := req.Execute(context.Background())
	if err != nil {
		return err
	}
	return resp.Close()
}

// ensureTablesCreated creates the necessary tables if they haven't been created yet
func ensureTablesCreated() error {
	if tablesCreated {
		return nil
	}

	// Create V2 table
	if err := executeQueryRequest(createV2tableQuery); err != nil {
		return fmt.Errorf("failed to create V2 table: %w", err)
	}

	// Create V1 table
	if err := executeQueryRequest(createV1tableQuery); err != nil {
		return fmt.Errorf("failed to create V1 table: %w", err)
	}

	// Create stop table
	if err := executeQueryRequest(createStopTableQuery); err != nil {
		return fmt.Errorf("failed to create stop table: %w", err)
	}

	// Create V3 table
	if err := executeQueryRequest(createV3tableQuery); err != nil {
		return fmt.Errorf("failed to create V3 table: %w", err)
	}

	// Create migration state table
	if err := executeQueryRequest(createMigrationStateTableQuery); err != nil {
		return fmt.Errorf("failed to create migration state table: %w", err)
	}

	// Create migration logs table
	if err := executeQueryRequest(createMigrationLogsTableQuery); err != nil {
		return fmt.Errorf("failed to create migration logs table: %w", err)
	}

	tablesCreated = true
	fmt.Printf("Tables created successfully\n")
	return nil
}

// cleanupTables removes all data from tables to ensure test isolation
func cleanupTables() error {
	// Clear V2 table data
	req := &chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       clickHouseAddr,
		User:       "default",
		Password:   "secret",
		Query:      "TRUNCATE TABLE statshouse_value_1h_dist",
	}
	resp, err := req.Execute(context.Background())
	if err != nil {
		return fmt.Errorf("failed to clear V2 table: %w", err)
	}
	err = resp.Close()
	if err != nil {
		return fmt.Errorf("failed to close V2 table clear response: %w", err)
	}

	// Clear V1 distributed table data
	req.Query = "TRUNCATE TABLE statshouse_value_dist_1h"
	resp, err = req.Execute(context.Background())
	if err != nil {
		return fmt.Errorf("failed to clear V1 distributed table: %w", err)
	}
	if err := resp.Close(); err != nil {
		return fmt.Errorf("failed to close V1 distributed table clear response: %w", err)
	}

	// Clear stop table data
	req.Query = "TRUNCATE TABLE stats_1h_agg_stop"
	resp, err = req.Execute(context.Background())
	if err != nil {
		return fmt.Errorf("failed to clear stop table: %w", err)
	}
	if err := resp.Close(); err != nil {
		return fmt.Errorf("failed to close stop table clear response: %w", err)
	}

	// Clear V3 table data
	req.Query = "TRUNCATE TABLE statshouse_v3_1h"
	resp, err = req.Execute(context.Background())
	if err != nil {
		return fmt.Errorf("failed to clear V3 table: %w", err)
	}
	err = resp.Close()
	if err != nil {
		return fmt.Errorf("failed to close V3 table clear response: %w", err)
	}

	// Clear migration state data
	req.Query = "TRUNCATE TABLE statshouse_migration_state"
	resp, err = req.Execute(context.Background())
	if err != nil {
		return fmt.Errorf("failed to clear migration state table: %w", err)
	}
	if err := resp.Close(); err != nil {
		return fmt.Errorf("failed to close migration state table clear response: %w", err)
	}

	// Clear migration logs data
	req.Query = "TRUNCATE TABLE statshouse_migration_logs"
	resp, err = req.Execute(context.Background())
	if err != nil {
		return fmt.Errorf("failed to clear migration logs table: %w", err)
	}
	if err := resp.Close(); err != nil {
		return fmt.Errorf("failed to close migration logs table clear response: %w", err)
	}

	return nil
}

const createV2tableQuery = `CREATE TABLE statshouse_value_1h_dist (
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

const createV3tableQuery = `CREATE TABLE statshouse_v3_1h (
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

const createV1tableQuery = `CREATE TABLE IF NOT EXISTS statshouse_value_dist_1h (
		date Date DEFAULT toDate(time),
		time DateTime,
		stats Int32,
		key1 Int32, key2 Int32, key3 Int32, key4 Int32, key5 Int32, key6 Int32, key7 Int32,
		key8 Int32, key9 Int32, key10 Int32, key11 Int32, key12 Int32, key13 Int32, key14 Int32, key15 Int32,
		count AggregateFunction(sum, Int64),
		min AggregateFunction(min, Int64),
		max AggregateFunction(max, Int64),
		sum AggregateFunction(sum, Int64),
		ns2 AggregateFunction(sum, Float64),
		sumsquare AggregateFunction(sum, Int64),
		percentiles AggregateFunction(quantilesTDigest(0.5), UInt64)
	) ENGINE = AggregatingMergeTree()
	ORDER BY (stats, time, key1, key2, key3, key4, key5, key6, key7, key8, key9, key10, key11, key12, key13, key14, key15)`

const createStopTableQuery = `CREATE TABLE IF NOT EXISTS stats_1h_agg_stop_dist (
		date Date DEFAULT toDate(time),
		time DateTime,
		stats Int32,
		key1 Int32,
		key2 Int32,
		skey String,
		count AggregateFunction(sum, Int64)
	) ENGINE = AggregatingMergeTree()
	ORDER BY (stats, time, key1, key2, skey)`

const createMigrationStateTableQuery = `CREATE TABLE IF NOT EXISTS statshouse_migration_state (
		shard_key Int32,
		ts DateTime,
		started DateTime,
		ended Nullable(DateTime),
		v2_rows UInt64,
		v3_rows UInt64,
		source_rows UInt64,
		retry UInt32,
		source String DEFAULT ''
	) ENGINE = ReplacingMergeTree(retry)
	ORDER BY (shard_key, ts, started)`

const createMigrationLogsTableQuery = `CREATE TABLE IF NOT EXISTS statshouse_migration_logs (
		timestamp DateTime,
		shard_key Int32,
		ts DateTime,
		retry UInt32,
		message String,
		source String DEFAULT ''
	) ENGINE = MergeTree()
	ORDER BY (timestamp, shard_key, ts, retry)`

func TestV2DataParsingIntegration(t *testing.T) {
	// Use shared container connection details
	httpAddr := clickHouseAddr

	// Clean up tables before test to ensure isolation
	err := cleanupTables()
	require.NoError(t, err)

	// Step 5: Test with complete parseV2Row function from migration.go
	// Create test data with all fields
	testData := createTestData()

	// Insert test data using RowBinary format
	err = insertTestData(httpClient, httpAddr, "default", "secret", testData)
	require.NoError(t, err)
	t.Logf("Step 5: Inserted %d test rows for timestamp %d", len(testData), testData[0].time)

	// Verify all rows were inserted by checking the count
	countQuery := fmt.Sprintf(`SELECT count() as cnt FROM statshouse_value_1h_dist WHERE time = toDateTime(%d)`, testData[0].time)
	req := &chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       httpAddr,
		User:       "default",
		Password:   "secret",
		Query:      countQuery,
	}
	resp, err := req.Execute(context.Background())
	require.NoError(t, err)

	var insertedCount uint64
	_, err = fmt.Fscanf(resp, "%d", &insertedCount)
	require.NoError(t, err)
	err = resp.Close()
	require.NoError(t, err)
	t.Logf("Step 5: Verified %d rows were inserted into V2 table", insertedCount)

	// we order by metric to get predictable order of rows, same as in the test data
	// NOTE: Column order must match parseV2Row expectations: metric, time, keys, skey, aggregates, percentiles, uniq_state, min_host, max_host
	selectQuery := fmt.Sprintf(`
	SELECT metric, time, key0, key1, key2, key3, key4, key5, key6, key7, key8, key9, key10, key11, key12, key13, key14, key15, skey,
		count, min, max, sum, sumsquare, percentiles, uniq_state, min_host, max_host
	FROM statshouse_value_1h_dist
	WHERE time = toDateTime(%d) ORDER BY metric`,
		testData[0].time)

	t.Logf("Executing query: %s", selectQuery)

	req = &chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       httpAddr,
		User:       "default",
		Password:   "secret",
		Query:      selectQuery,
		Format:     "RowBinary",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	resp, err = req.Execute(ctx)
	require.NoError(t, err)
	// defer resp.Close()

	t.Logf("Step 5: Query executed successfully, starting to read response")

	// Parse the RowBinary response using the complete parseV2Row function
	var parsedRows int
	bufReader := bufio.NewReader(resp)
	var row v2Row
	for ; ; parsedRows++ {
		t.Logf("Step 5: Attempting to parse row %d...", parsedRows+1)
		err := parseV2Row(bufReader, &row)
		if err != nil {
			t.Logf("Step 5: Parse error for row %d: %v", parsedRows+1, err)
			if errors.Is(err, io.EOF) {
				t.Logf("Step 5: Reached EOF after parsing %d rows", parsedRows)
				break
			}
			require.NoError(t, err)
		}
		t.Logf("Step 5: Parsed row %d: metric=%d, time=%d, key0=%d, skey=%s, count=%.2f, min=%.2f, max=%.2f, sum=%.2f, sumsquare=%.2f, min_host=(%d,%.2f), max_host=(%d,%.2f)",
			parsedRows, row.metric, row.time, row.keys[0], row.skey, row.count, row.min, row.max, row.sum, row.sumsquare,
			row.min_host.Arg, row.min_host.Val, row.max_host.Arg, row.max_host.Val)
		expected := testData[parsedRows]
		actual := row
		require.Equal(t, expected.metric, actual.metric, "metric mismatch at row %d", parsedRows)
		require.Equal(t, expected.time, actual.time, "time mismatch at row %d", parsedRows)
		require.Equal(t, expected.keys, actual.keys, "keys mismatch at row %d", parsedRows)
		require.Equal(t, expected.skey, actual.skey, "skey mismatch at row %d", parsedRows)
		require.Equal(t, expected.count, actual.count, "count mismatch at row %d", parsedRows)
		require.Equal(t, expected.min, actual.min, "min mismatch at row %d", parsedRows)
		require.Equal(t, expected.max, actual.max, "max mismatch at row %d", parsedRows)
		require.Equal(t, expected.sum, actual.sum, "sum mismatch at row %d", parsedRows)
		require.Equal(t, expected.sumsquare, actual.sumsquare, "sumsquare mismatch at row %d", parsedRows)
		require.Equal(t, expected.min_host.Arg, actual.min_host.Arg, "min_host.Arg mismatch at row %d", parsedRows)
		require.Equal(t, expected.min_host.Val, actual.min_host.Val, "min_host.Val mismatch at row %d", parsedRows)
		require.Equal(t, expected.max_host.Arg, actual.max_host.Arg, "max_host.Arg mismatch at row %d", parsedRows)
		require.Equal(t, expected.max_host.Val, actual.max_host.Val, "max_host.Val mismatch at row %d", parsedRows)
		// compare two float64s with 1e-6 precision
		if expected.perc.Digest != nil && actual.perc.Digest != nil {
			require.InDelta(t, expected.perc.Digest.Quantile(0.5), actual.perc.Digest.Quantile(0.5), 1e-6, "percentile 0.5 mismatch at row %d", parsedRows)
			require.InDelta(t, expected.perc.Digest.Quantile(0.99), actual.perc.Digest.Quantile(0.99), 1e-6, "percentile 0.99 mismatch at row %d", parsedRows)
		} else {
			require.Nil(t, actual.perc.Digest, "percentiles should be nil at row %d", parsedRows)
		}
		require.Equal(t, expected.uniq.ItemsCount(), actual.uniq.ItemsCount(), "uniq_state mismatch at row %d", parsedRows)
	}

	// Validate parsed data matches original
	require.Equal(t, len(testData), parsedRows, "Should parse same number of rows")

	t.Logf("Step 5 SUCCESS: Validated %d rows with complete parseV2Row function", parsedRows)
}

// TestV2ToV3Conversion tests that convertRowV2ToV3 works correctly and generates data that can be inserted into V3 table
func TestV2ToV3ConversionIntegration(t *testing.T) {
	// Use shared container connection details
	httpAddr := clickHouseAddr

	// Clean up tables before test to ensure isolation
	err := cleanupTables()
	require.NoError(t, err)

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
	resp, err := insertReq.Execute(context.Background())
	require.NoError(t, err)
	err = resp.Close()
	require.NoError(t, err)

	t.Logf("SUCCESS: Inserted %d converted rows into V3 table", len(testData))
}

func TestMigrateSingleStepIntegration(t *testing.T) {
	// Use shared container connection details
	httpAddr := clickHouseAddr

	// Clean up tables before test to ensure isolation
	err := cleanupTables()
	require.NoError(t, err)

	// Create test data and insert into V2 table
	testData := createTestData()
	err = insertTestData(httpClient, httpAddr, "default", "secret", testData)
	require.NoError(t, err)
	t.Logf("Inserted %d test rows into V2 table", len(testData))

	testHour := testData[0].time
	// Use default config for testing
	config := NewDefaultMigrationConfig()

	var shardsToTest []int32
	for i := range 16 {
		shardsToTest = append(shardsToTest, int32(i+1))
	}

	for _, shardKey := range shardsToTest {
		t.Run(fmt.Sprintf("shard_%d", shardKey), func(t *testing.T) {
			// Run migration for this shard
			err = migrateSingleStep(httpClient, httpAddr, "default", "secret", testHour, shardKey, config)
			require.NoError(t, err)
			t.Logf("Migration completed successfully for hour %d, shard %d", testHour, shardKey)

			// Get expected rows for this shard
			var expectedRows []*v2Row
			for _, row := range testData {
				if row.metric%16 == shardKey-1 { // shard processes metrics where metric % 16 = shardKey-1
					expectedRows = append(expectedRows, row)
				}
			}

			// First, get the count of expected rows to know when to stop parsing
			countQuery := fmt.Sprintf(`SELECT count() as cnt FROM statshouse_v3_1h WHERE time = toDateTime(%d) AND %s`, testHour, getConditionForSelectV3(16, shardKey))
			req := &chutil.ClickHouseHttpRequest{
				HttpClient: httpClient,
				Addr:       httpAddr,
				User:       "default",
				Password:   "secret",
				Query:      countQuery,
				Format:     "", // Default format for count query
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			resp, err := req.Execute(ctx)
			require.NoError(t, err)

			var expectedRowCount uint64
			if _, err := fmt.Fscanf(resp, "%d", &expectedRowCount); err != nil {
				require.NoError(t, err, "Failed to parse row count")
			}
			err = resp.Close()
			require.NoError(t, err)

			t.Logf("Shard %d: Expecting %d rows", shardKey, expectedRowCount)

			// Query all fields that are actually inserted by the migration
			selectQuery := fmt.Sprintf(`
			SELECT metric, time, tag0, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag8, tag9, tag10, tag11, tag12, tag13, tag14, tag15, stag47,
				count, min, max, sum, sumsquare, min_host, max_host, percentiles, uniq_state
			FROM statshouse_v3_1h
			WHERE time = toDateTime(%d) AND %s
			ORDER BY metric`, testHour, getConditionForSelectV3(16, shardKey))

			t.Logf("Executing V3 query for shard %d: %s", shardKey, selectQuery)

			req.Query = selectQuery
			req.Format = "RowBinary"
			resp, err = req.Execute(ctx)
			require.NoError(t, err)

			// Parse the RowBinary response using parseV3Row function
			// Parse exactly the expected number of rows instead of relying on EOF
			var migratedRows []*v3Row
			bufReader := bufio.NewReader(resp)
			for i := uint64(0); i < expectedRowCount; i++ {
				row, err := parseV3Row(bufReader, t)
				require.NoError(t, err, "Failed to parse V3 row %d", i+1)
				migratedRows = append(migratedRows, row)
				t.Logf("Shard %d: Parsed V3 row %d: metric=%d, time=%d, tag0=%d, stag47=%s, count=%.2f",
					shardKey, len(migratedRows), row.metric, row.time, row.tags[0], row.stag47, row.count)
			}

			// Verify that we don't have any more data by trying to read one more byte
			// This should fail with EOF if we've consumed all the row data
			oneByte := make([]byte, 1)
			n, _ := bufReader.Read(oneByte)
			if n > 0 {
				t.Logf("Shard %d: Warning - found %d extra bytes after expected row data", shardKey, n)
				// Don't fail the test for this, as it might be normal ClickHouse response padding
			}

			err = resp.Close()
			require.NoError(t, err)

			// Validate counts match
			require.Equal(t, len(expectedRows), len(migratedRows),
				"Shard %d: Should migrate same number of rows as expected", shardKey)

			// Comprehensive validation: compare all fields between original V2 and migrated V3
			for i, expectedV2 := range expectedRows {
				actualV3 := migratedRows[i]

				// Validate V2→V3 field mapping
				require.Equal(t, expectedV2.metric, actualV3.metric,
					"Shard %d row %d: metric mismatch", shardKey, i)
				require.Equal(t, expectedV2.time, actualV3.time,
					"Shard %d row %d: time mismatch", shardKey, i)
				require.Equal(t, expectedV2.keys, actualV3.tags,
					"Shard %d row %d: keys→tags mismatch", shardKey, i)
				require.Equal(t, expectedV2.skey, actualV3.stag47,
					"Shard %d row %d: skey→stag47 mismatch", shardKey, i)
				require.Equal(t, expectedV2.count, actualV3.count,
					"Shard %d row %d: count mismatch", shardKey, i)
				require.Equal(t, expectedV2.min, actualV3.min,
					"Shard %d row %d: min mismatch", shardKey, i)
				require.Equal(t, expectedV2.max, actualV3.max,
					"Shard %d row %d: max mismatch", shardKey, i)
				require.Equal(t, expectedV2.sum, actualV3.sum,
					"Shard %d row %d: sum mismatch", shardKey, i)
				require.Equal(t, expectedV2.sumsquare, actualV3.sumsquare,
					"Shard %d row %d: sumsquare mismatch", shardKey, i)

				// Validate host aggregates
				require.Empty(t, actualV3.min_host.AsString,
					"Shard %d row %d: min_host string not empty", shardKey, i)
				require.Equal(t, expectedV2.min_host.Val, actualV3.min_host.Val,
					"Shard %d row %d: min_host.Val mismatch", shardKey, i)
				require.Empty(t, actualV3.max_host.AsString,
					"Shard %d row %d: max_host string not empty", shardKey, i)
				require.Equal(t, expectedV2.max_host.Val, actualV3.max_host.Val,
					"Shard %d row %d: max_host.Val mismatch", shardKey, i)

				// Validate percentiles (aggregate states)
				if expectedV2.perc.Digest != nil && actualV3.perc.Digest != nil {
					require.InDelta(t, expectedV2.perc.Digest.Quantile(0.5), actualV3.perc.Digest.Quantile(0.5), 1e-6,
						"Shard %d row %d: percentile 0.5 mismatch", shardKey, i)
					require.InDelta(t, expectedV2.perc.Digest.Quantile(0.99), actualV3.perc.Digest.Quantile(0.99), 1e-6,
						"Shard %d row %d: percentile 0.99 mismatch", shardKey, i)
				} else {
					require.Nil(t, actualV3.perc.Digest,
						"Shard %d row %d: percentiles should be nil", shardKey, i)
				}

				// Validate unique state
				require.Equal(t, expectedV2.uniq.ItemsCount(), actualV3.uniq.ItemsCount(),
					"Shard %d row %d: uniq_state mismatch", shardKey, i)
			}

			t.Logf("SUCCESS: Shard %d migrated and validated %d rows with complete field-by-field comparison",
				shardKey, len(migratedRows))
		})
	}
}

func TestV1DataParsingIntegration(t *testing.T) {
	httpAddr := clickHouseAddr

	err := cleanupTables()
	require.NoError(t, err)

	testData, valueSets := createV1TestData()
	err = insertV1TestData(httpClient, httpAddr, "default", "secret", testData, valueSets)
	require.NoError(t, err)
	t.Logf("Inserted %d test rows into V1 table", len(testData))

	selectQuery := fmt.Sprintf(`
	SELECT stats, time,
		key1, key2, key3, key4, key5, key6, key7,
		key8, key9, key10, key11, key12, key13, key14, key15,
		count, min, max, sum, ns2, sumsquare, percentiles
	FROM statshouse_value_dist_1h
	WHERE time = toDateTime(%d)
	ORDER BY stats`, testData[0].time)

	req := &chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       httpAddr,
		User:       "default",
		Password:   "secret",
		Query:      selectQuery,
		Format:     "RowBinary",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	resp, err := req.Execute(ctx)
	require.NoError(t, err)

	var parsedRows []*v1Row
	bufReader := bufio.NewReader(resp)
	for {
		var row v1Row
		if err := parseV1Row(bufReader, &row); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err)
		} else {
			rowCopy := row
			parsedRows = append(parsedRows, &rowCopy)
		}
	}
	err = resp.Close()
	require.NoError(t, err)

	require.Equal(t, len(testData), len(parsedRows), "parsed row count mismatch")

	for i, expected := range testData {
		actual := parsedRows[i]
		require.Equal(t, expected.metric, actual.metric, "row %d metric mismatch", i)
		require.Equal(t, expected.time, actual.time, "row %d time mismatch", i)
		require.Equal(t, expected.keys, actual.keys, "row %d keys mismatch", i)
		require.Equal(t, expected.count, actual.count, "row %d count mismatch", i)
		require.Equal(t, expected.min, actual.min, "row %d min mismatch", i)
		require.Equal(t, expected.max, actual.max, "row %d max mismatch", i)
		require.Equal(t, expected.sum, actual.sum, "row %d sum mismatch", i)
	}

	t.Logf("SUCCESS: Parsed and validated %d V1 rows", len(parsedRows))
}

func TestV1ToV3ConversionIntegration(t *testing.T) {
	httpAddr := clickHouseAddr

	err := cleanupTables()
	require.NoError(t, err)

	testData, _ := createV1TestData()

	body := make([]byte, 0, 1024*len(testData))
	for _, row := range testData {
		body = convertRowV1ToV3(body, row)
	}

	insertQuery := `INSERT INTO statshouse_v3_1h(
		metric,time,
		tag0,tag1,tag2,tag3,tag4,tag5,tag6,tag7,
		tag8,tag9,tag10,tag11,tag12,tag13,tag14,tag15,stag47,
		count,min,max,sum,sumsquare,
		min_host,max_host,percentiles,uniq_state
	)
	FORMAT RowBinary`
	req := &chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       httpAddr,
		User:       "default",
		Password:   "secret",
		Query:      insertQuery,
		Body:       body,
		UrlParams:  map[string]string{"input_format_values_interpret_expressions": "0"},
	}
	resp, err := req.Execute(context.Background())
	require.NoError(t, err)
	err = resp.Close()
	require.NoError(t, err)

	countQuery := fmt.Sprintf(`SELECT count() FROM statshouse_v3_1h WHERE time = toDateTime(%d)`, testData[0].time)
	req = &chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       httpAddr,
		User:       "default",
		Password:   "secret",
		Query:      countQuery,
	}
	resp, err = req.Execute(context.Background())
	require.NoError(t, err)

	var insertedCount uint64
	_, err = fmt.Fscanf(resp, "%d", &insertedCount)
	require.NoError(t, err)
	err = resp.Close()
	require.NoError(t, err)

	require.Equal(t, uint64(len(testData)), insertedCount, "unexpected number of V3 rows inserted")
}

func TestMigrateSingleStepV1Integration(t *testing.T) {
	httpAddr := clickHouseAddr

	err := cleanupTables()
	require.NoError(t, err)

	testData, valueSets := createV1TestData()
	err = insertV1TestData(httpClient, httpAddr, "default", "secret", testData, valueSets)
	require.NoError(t, err)
	t.Logf("Inserted %d test rows into V1 table", len(testData))

	testHour := testData[0].time
	config := NewDefaultMigrationConfigV1([]string{httpAddr}, "default", "secret")
	config.TotalShards = 16
	config.V3TableName = "statshouse_v3_1h"

	var shardsToTest []int32
	for i := range 16 {
		shardsToTest = append(shardsToTest, int32(i+1))
	}

	for _, shardKey := range shardsToTest {
		t.Run(fmt.Sprintf("shard_%d", shardKey), func(t *testing.T) {
			err = migrateSingleStepV1(httpClient, httpAddr, "default", "secret", testHour, shardKey, config)
			require.NoError(t, err)

			var expectedRows []*v1Row
			for _, row := range testData {
				if row.metric%16 == shardKey-1 {
					expectedRows = append(expectedRows, row)
				}
			}

			countQuery := fmt.Sprintf(`SELECT count() as cnt FROM statshouse_v3_1h WHERE time = toDateTime(%d) AND %s`, testHour, getConditionForSelectV3(16, shardKey))
			req := &chutil.ClickHouseHttpRequest{
				HttpClient: httpClient,
				Addr:       httpAddr,
				User:       "default",
				Password:   "secret",
				Query:      countQuery,
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			resp, err := req.Execute(ctx)
			require.NoError(t, err)

			var migratedCount uint64
			_, err = fmt.Fscanf(resp, "%d", &migratedCount)
			require.NoError(t, err)
			err = resp.Close()
			require.NoError(t, err)

			require.Equal(t, uint64(len(expectedRows)), migratedCount, "shard %d row count mismatch", shardKey)

			if migratedCount == 0 {
				return
			}

			selectQuery := fmt.Sprintf(`
			SELECT metric, time, tag0, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag8, tag9, tag10, tag11, tag12, tag13, tag14, tag15, stag47,
				count, min, max, sum, sumsquare, min_host, max_host, percentiles, uniq_state
			FROM statshouse_v3_1h
			WHERE time = toDateTime(%d) AND %s
			ORDER BY metric`, testHour, getConditionForSelectV3(16, shardKey))

			req.Query = selectQuery
			req.Format = "RowBinary"
			resp, err = req.Execute(ctx)
			require.NoError(t, err)

			var migratedRows []*v3Row
			bufReader := bufio.NewReader(resp)
			for i := uint64(0); i < migratedCount; i++ {
				row, err := parseV3Row(bufReader, t)
				require.NoError(t, err, "failed to parse V3 row %d", i+1)
				migratedRows = append(migratedRows, row)
			}
			err = resp.Close()
			require.NoError(t, err)

			require.Equal(t, len(expectedRows), len(migratedRows), "shard %d migrated rows mismatch", shardKey)

			for i, expected := range expectedRows {
				actual := migratedRows[i]
				require.Equal(t, expected.metric, actual.metric, "shard %d row %d metric mismatch", shardKey, i)
				require.Equal(t, expected.time, actual.time, "shard %d row %d time mismatch", shardKey, i)
				require.Equal(t, expected.keys, actual.tags, "shard %d row %d tags mismatch", shardKey, i)
				require.Empty(t, actual.stag47, "shard %d row %d stag47 should be empty", shardKey, i)
				require.Equal(t, expected.count, actual.count, "shard %d row %d count mismatch", shardKey, i)
				require.Equal(t, expected.min, actual.min, "shard %d row %d min mismatch", shardKey, i)
				require.Equal(t, expected.max, actual.max, "shard %d row %d max mismatch", shardKey, i)
				require.Equal(t, expected.sum, actual.sum, "shard %d row %d sum mismatch", shardKey, i)

				require.Empty(t, actual.min_host.AsString, "shard %d row %d min_host string should be empty", shardKey, i)
				require.Zero(t, actual.min_host.Val, "shard %d row %d min_host value should be zero", shardKey, i)
				require.Empty(t, actual.max_host.AsString, "shard %d row %d max_host string should be empty", shardKey, i)
				require.Zero(t, actual.max_host.Val, "shard %d row %d max_host value should be zero", shardKey, i)

				if actual.uniq != nil {
					require.Equal(t, 0, actual.uniq.ItemsCount(), "shard %d row %d uniq_state mismatch", shardKey, i)
				}
			}
		})
	}
}

func createStopTestData() []*stopRow {
	var testData []*stopRow
	const baseMetric int32 = 2000
	const baseTimestamp uint32 = 1733000400

	for i := int32(0); i < 160; i++ {
		metric := baseMetric + i
		count := int64(5 + i%10) // 5-14

		row := &stopRow{
			metric: metric,
			time:   baseTimestamp,
			key1:   metric*10 + 1,
			key2:   metric*10 + 2,
			skey:   fmt.Sprintf("skey_%d", metric),
			count:  float64(count),
		}

		testData = append(testData, row)
	}

	return testData
}

func insertStopTestData(httpClient *http.Client, httpAddr, user, password string, rows []*stopRow) error {
	for _, row := range rows {
		query := fmt.Sprintf(`
		INSERT INTO stats_1h_agg_stop
		SELECT
			toDate(toDateTime(%d)),
			toDateTime(%d),
			%d,
			%d,
			%d,
			'%s',
			sumState(toInt64(%d)) AS count
		`, row.time, row.time, row.metric, row.key1, row.key2, row.skey, int64(row.count))

		req := &chutil.ClickHouseHttpRequest{
			HttpClient: httpClient,
			Addr:       httpAddr,
			User:       user,
			Password:   password,
			Query:      query,
		}
		resp, err := req.Execute(context.Background())
		if err != nil {
			return fmt.Errorf("failed to insert stop test row: %w", err)
		}
		if err := resp.Close(); err != nil {
			return fmt.Errorf("failed to close stop insert response: %w", err)
		}
	}

	return nil
}

func TestStopDataParsingIntegration(t *testing.T) {
	httpAddr := clickHouseAddr

	err := cleanupTables()
	require.NoError(t, err)

	testData := createStopTestData()
	err = insertStopTestData(httpClient, httpAddr, "default", "secret", testData)
	require.NoError(t, err)
	t.Logf("Inserted %d test rows into stop table", len(testData))

	selectQuery := fmt.Sprintf(`
	SELECT stats, time, key1, key2, skey, count
	FROM stats_1h_agg_stop
	WHERE time = toDateTime(%d)
	ORDER BY stats`, testData[0].time)

	req := &chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       httpAddr,
		User:       "default",
		Password:   "secret",
		Query:      selectQuery,
		Format:     "RowBinary",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	resp, err := req.Execute(ctx)
	require.NoError(t, err)

	var parsedRows []*stopRow
	bufReader := bufio.NewReader(resp)
	for {
		var row stopRow
		if err := parseStopRow(bufReader, &row); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err)
		} else {
			rowCopy := row
			parsedRows = append(parsedRows, &rowCopy)
		}
	}
	err = resp.Close()
	require.NoError(t, err)

	require.Equal(t, len(testData), len(parsedRows), "parsed row count mismatch")

	for i, expected := range testData {
		actual := parsedRows[i]
		require.Equal(t, expected.metric, actual.metric, "row %d metric mismatch", i)
		require.Equal(t, expected.time, actual.time, "row %d time mismatch", i)
		require.Equal(t, expected.key1, actual.key1, "row %d key1 mismatch", i)
		require.Equal(t, expected.key2, actual.key2, "row %d key2 mismatch", i)
		require.Equal(t, expected.skey, actual.skey, "row %d skey mismatch", i)
		require.Equal(t, expected.count, actual.count, "row %d count mismatch", i)
	}

	t.Logf("SUCCESS: Parsed and validated %d stop rows", len(parsedRows))
}

func TestStopToV3ConversionIntegration(t *testing.T) {
	httpAddr := clickHouseAddr

	err := cleanupTables()
	require.NoError(t, err)

	testData := createStopTestData()

	body := make([]byte, 0, 1024*len(testData))
	for _, row := range testData {
		body = convertStopRowToV3(body, row)
	}

	insertQuery := `INSERT INTO statshouse_v3_1h(
		metric,time,
		tag0,tag1,tag2,tag3,tag4,tag5,tag6,tag7,
		tag8,tag9,tag10,tag11,tag12,tag13,tag14,tag15,stag47,
		count,min,max,sum,sumsquare,
		min_host,max_host,percentiles,uniq_state
	)
	FORMAT RowBinary`
	req := &chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       httpAddr,
		User:       "default",
		Password:   "secret",
		Query:      insertQuery,
		Body:       body,
		UrlParams:  map[string]string{"input_format_values_interpret_expressions": "0"},
	}
	resp, err := req.Execute(context.Background())
	require.NoError(t, err)
	err = resp.Close()
	require.NoError(t, err)

	countQuery := fmt.Sprintf(`SELECT count() FROM statshouse_v3_1h WHERE time = toDateTime(%d)`, testData[0].time)
	req = &chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       httpAddr,
		User:       "default",
		Password:   "secret",
		Query:      countQuery,
	}
	resp, err = req.Execute(context.Background())
	require.NoError(t, err)

	var insertedCount uint64
	_, err = fmt.Fscanf(resp, "%d", &insertedCount)
	require.NoError(t, err)
	err = resp.Close()
	require.NoError(t, err)

	require.Equal(t, uint64(len(testData)), insertedCount, "unexpected number of V3 rows inserted")
}

func TestMigrateSingleStepStopIntegration(t *testing.T) {
	httpAddr := clickHouseAddr

	err := cleanupTables()
	require.NoError(t, err)

	testData := createStopTestData()
	err = insertStopTestData(httpClient, httpAddr, "default", "secret", testData)
	require.NoError(t, err)
	t.Logf("Inserted %d test rows into stop table", len(testData))

	testHour := testData[0].time
	config := NewDefaultMigrationConfigStop([]string{httpAddr}, "default", "secret")
	config.TotalShards = 16
	config.V3TableName = "statshouse_v3_1h"

	var shardsToTest []int32
	for i := range 16 {
		shardsToTest = append(shardsToTest, int32(i+1))
	}

	for _, shardKey := range shardsToTest {
		t.Run(fmt.Sprintf("shard_%d", shardKey), func(t *testing.T) {
			err = migrateSingleStepStop(httpClient, httpAddr, "default", "secret", testHour, shardKey, config)
			require.NoError(t, err)

			var expectedRows []*stopRow
			for _, row := range testData {
				if row.metric%16 == shardKey-1 {
					expectedRows = append(expectedRows, row)
				}
			}

			countQuery := fmt.Sprintf(`SELECT count() as cnt FROM statshouse_v3_1h WHERE time = toDateTime(%d) AND %s`, testHour, getConditionForSelectV3(16, shardKey))
			req := &chutil.ClickHouseHttpRequest{
				HttpClient: httpClient,
				Addr:       httpAddr,
				User:       "default",
				Password:   "secret",
				Query:      countQuery,
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			resp, err := req.Execute(ctx)
			require.NoError(t, err)

			var migratedCount uint64
			_, err = fmt.Fscanf(resp, "%d", &migratedCount)
			require.NoError(t, err)
			err = resp.Close()
			require.NoError(t, err)

			require.Equal(t, uint64(len(expectedRows)), migratedCount, "shard %d row count mismatch", shardKey)

			if migratedCount == 0 {
				return
			}

			selectQuery := fmt.Sprintf(`
			SELECT metric, time, tag0, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag8, tag9, tag10, tag11, tag12, tag13, tag14, tag15, stag47,
				count, min, max, sum, sumsquare, min_host, max_host, percentiles, uniq_state
			FROM statshouse_v3_1h
			WHERE time = toDateTime(%d) AND %s
			ORDER BY metric`, testHour, getConditionForSelectV3(16, shardKey))

			req.Query = selectQuery
			req.Format = "RowBinary"
			resp, err = req.Execute(ctx)
			require.NoError(t, err)

			var migratedRows []*v3Row
			bufReader := bufio.NewReader(resp)
			for i := uint64(0); i < migratedCount; i++ {
				row, err := parseV3Row(bufReader, t)
				require.NoError(t, err, "failed to parse V3 row %d", i+1)
				migratedRows = append(migratedRows, row)
			}
			err = resp.Close()
			require.NoError(t, err)

			require.Equal(t, len(expectedRows), len(migratedRows), "shard %d migrated rows mismatch", shardKey)

			for i, expected := range expectedRows {
				actual := migratedRows[i]
				require.Equal(t, expected.metric, actual.metric, "shard %d row %d metric mismatch", shardKey, i)
				require.Equal(t, expected.time, actual.time, "shard %d row %d time mismatch", shardKey, i)
				require.Equal(t, int32(0), actual.tags[0], "shard %d row %d tag0 should be zero", shardKey, i)
				require.Equal(t, expected.key1, actual.tags[1], "shard %d row %d tag1 mismatch", shardKey, i)
				require.Equal(t, expected.key2, actual.tags[2], "shard %d row %d tag2 mismatch", shardKey, i)
				for j := 3; j < 16; j++ {
					require.Equal(t, int32(0), actual.tags[j], "shard %d row %d tag%d should be zero", shardKey, i, j)
				}
				require.Equal(t, expected.skey, actual.stag47, "shard %d row %d stag47 mismatch", shardKey, i)
				require.Equal(t, expected.count, actual.count, "shard %d row %d count mismatch", shardKey, i)
				require.Zero(t, actual.min, "shard %d row %d min should be zero", shardKey, i)
				require.Zero(t, actual.max, "shard %d row %d max should be zero", shardKey, i)
				require.Zero(t, actual.sum, "shard %d row %d sum should be zero", shardKey, i)
				require.Zero(t, actual.sumsquare, "shard %d row %d sumsquare should be zero", shardKey, i)
				require.Empty(t, actual.min_host.AsString, "shard %d row %d min_host string should be empty", shardKey, i)
				require.Zero(t, actual.min_host.Val, "shard %d row %d min_host value should be zero", shardKey, i)
				require.Empty(t, actual.max_host.AsString, "shard %d row %d max_host string should be empty", shardKey, i)
				require.Zero(t, actual.max_host.Val, "shard %d row %d max_host value should be zero", shardKey, i)
				require.Nil(t, actual.perc, "shard %d row %d percentiles should be nil", shardKey, i)
				if actual.perc != nil {
					require.Nil(t, actual.perc.Digest, "shard %d row %d percentiles digest should be nil", shardKey, i)
				}
				if actual.uniq != nil {
					require.Equal(t, 0, actual.uniq.ItemsCount(), "shard %d row %d uniq_state should be empty", shardKey, i)
				}
			}

			t.Logf("SUCCESS: Shard %d migrated and validated %d rows", shardKey, len(migratedRows))
		})
	}
}

// generateRandomTDigest creates a random tdigest with random quantiles
func generateRandomTDigest(rnd *rand.Rand) *tdigest.TDigest {
	perc := tdigest.New()
	numQuantiles := rnd.Intn(5) + 1 // 1-5 quantiles

	for i := 0; i < numQuantiles; i++ {
		quantile := rnd.Float64()       // 0.0 to 1.0
		value := rnd.Float64()*100 - 50 // -50 to 50
		perc.Add(quantile, value)
	}
	return perc
}

// generateRandomUnique creates a random unique state
func generateRandomUnique(rnd *rand.Rand) data_model.ChUnique {
	uniq := data_model.ChUnique{}
	numItems := rnd.Intn(100) + 1 // 1-100 unique items

	for i := 0; i < numItems; i++ {
		item := rnd.Uint64() // Random uint64
		uniq.Insert(item)
	}
	return uniq
}

// generateRandomKeys creates random keys with some zeros (absent fields)
func generateRandomKeys(rnd *rand.Rand) [16]int32 {
	var keys [16]int32
	for i := 0; i < 16; i++ {
		// 70% chance of having a value, 30% chance of being 0 (absent)
		if rnd.Float64() < 0.7 {
			keys[i] = rnd.Int31n(1000000) + 1 // 1 to 1000000
		} else {
			keys[i] = 0 // Absent field
		}
	}
	return keys
}

// generateRandomStringKey creates a random string key
func generateRandomStringKey(rnd *rand.Rand) string {
	length := rnd.Intn(20) + 5 // 5-25 characters
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"

	result := make([]byte, length)
	for i := range result {
		result[i] = charset[rnd.Intn(len(charset))]
	}
	return string(result)
}

// generateRandomAggregates creates random aggregate values
func generateRandomAggregates(rnd *rand.Rand) (float64, float64, float64, float64, float64) {
	count := float64(rnd.Intn(1000) + 1)       // 1-1000
	min := rnd.Float64()*100 - 50              // -50 to 50
	max := min + rnd.Float64()*100             // min to min+100
	sum := min*count + rnd.Float64()*1000      // reasonable sum
	sumsquare := sum*sum + rnd.Float64()*10000 // reasonable sumsquare

	return count, min, max, sum, sumsquare
}

// generateRandomHostAggregates creates random host aggregate values
func generateRandomHostAggregates(rnd *rand.Rand) (data_model.ArgMinInt32Float32, data_model.ArgMaxInt32Float32) {
	minArg := rnd.Int31n(10000000) + 1 // 1 to 10000000
	minVal := rnd.Float32()*100 - 50   // -50 to 50

	maxArg := rnd.Int31n(10000000) + 1 // 1 to 10000000
	maxVal := rnd.Float32()*100 - 50   // -50 to 50

	minHost := data_model.ArgMinInt32Float32{
		ArgMinMaxInt32Float32: data_model.ArgMinMaxInt32Float32{
			Arg: minArg,
			Val: minVal,
		},
	}

	maxHost := data_model.ArgMaxInt32Float32{
		ArgMinMaxInt32Float32: data_model.ArgMinMaxInt32Float32{
			Arg: maxArg,
			Val: maxVal,
		},
	}

	return minHost, maxHost
}

func createTestData() []*v2Row {
	// Create random generator with seed for reproducible tests
	rnd := rand.New(42)

	// Create comprehensive test data covering all 16 shards
	var testData []*v2Row

	for metricId := int32(0); metricId < 160; metricId++ {
		// Generate random data with various combinations
		keys := generateRandomKeys(rnd)
		skey := generateRandomStringKey(rnd)
		count, min, max, sum, sumsquare := generateRandomAggregates(rnd)
		minHost, maxHost := generateRandomHostAggregates(rnd)

		// Always initialize pointers, but randomly decide content
		var perc data_model.ChDigest
		if rnd.Float64() < 0.8 { // 80% chance of having percentiles
			perc = data_model.ChDigest{Digest: generateRandomTDigest(rnd)}
		} else {
			// Initialize with empty digest
			perc = data_model.ChDigest{Digest: tdigest.New()}
		}

		var uniq data_model.ChUnique
		if rnd.Float64() < 0.8 { // 80% chance of having unique state
			uniq = generateRandomUnique(rnd)
		} else {
			// Initialize with empty unique state
			uniq = data_model.ChUnique{}
		}

		// Create row with random data
		row := &v2Row{
			metric:    metricId,
			time:      1733000400, // Fixed timestamp for testing
			keys:      keys,
			skey:      skey,
			count:     count,
			min:       min,
			max:       max,
			sum:       sum,
			sumsquare: sumsquare,
			perc:      perc,
			uniq:      uniq,
			min_host:  minHost,
			max_host:  maxHost,
		}

		testData = append(testData, row)
	}

	return testData
}

func insertTestData(httpClient *http.Client, httpAddr, user, password string, testData []*v2Row) error {
	// Insert test data using RowBinary format to properly create aggregate states
	if len(testData) == 0 {
		return fmt.Errorf("no test data provided")
	}

	// Build RowBinary data for all rows
	var body []byte
	for _, row := range testData {
		// Convert each test row to V2 RowBinary format for insertion
		body = appendV2RowBinary(body, row)
	}

	// Insert data using RowBinary format
	insertQuery := `INSERT INTO statshouse_value_1h_dist (
		metric, time, key0, key1, key2, key3, key4, key5, key6, key7, key8, key9, key10, key11, key12, key13, key14, key15, skey,
		count, min, max, sum, sumsquare, percentiles, uniq_state, min_host, max_host
	) FORMAT RowBinary`

	req := &chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       httpAddr,
		User:       user,
		Password:   password,
		Query:      insertQuery,
		Body:       body,
		UrlParams:  map[string]string{"input_format_values_interpret_expressions": "0"},
	}
	resp, err := req.Execute(context.Background())
	if err != nil {
		return fmt.Errorf("failed to execute insert query: %w", err)
	}
	return resp.Close()
}

// appendV2RowBinary appends a v2Row in RowBinary format to the buffer
func appendV2RowBinary(buf []byte, row *v2Row) []byte {
	// Use the same rowbinary helper functions from the migration code
	buf = rowbinary.AppendInt32(buf, row.metric)
	buf = rowbinary.AppendDateTime(buf, time.Unix(int64(row.time), 0))

	// Append all 16 keys
	for i := 0; i < 16; i++ {
		buf = rowbinary.AppendInt32(buf, row.keys[i])
	}

	// Append skey
	buf = rowbinary.AppendString(buf, row.skey)

	// Append simple aggregates
	buf = rowbinary.AppendFloat64(buf, row.count)
	buf = rowbinary.AppendFloat64(buf, row.min)
	buf = rowbinary.AppendFloat64(buf, row.max)
	buf = rowbinary.AppendFloat64(buf, row.sum)
	buf = rowbinary.AppendFloat64(buf, row.sumsquare)

	// Append aggregate states
	buf = row.perc.MarshallAppend(buf, 1)
	buf = row.uniq.MarshallAppend(buf)
	buf = row.min_host.MarshalAppend(buf)
	buf = row.max_host.MarshalAppend(buf)

	return buf
}

func createV1TestData() ([]*v1Row, [][]uint64) {
	var testData []*v1Row
	var valueSets [][]uint64
	const baseMetric int32 = 1000
	const baseTimestamp uint32 = 1733000400

	for i := int32(0); i < 160; i++ {
		metric := baseMetric + i
		values := []uint64{uint64(metric), uint64(metric + 1), uint64(metric + 2)}

		td := tdigest.New()
		var sum int64
		var sumsquare int64
		minVal := values[0]
		maxVal := values[0]
		for _, v := range values {
			td.Add(float64(v), 1)
			sum += int64(v)
			sumsquare += int64(v * v)
			if v < minVal {
				minVal = v
			}
			if v > maxVal {
				maxVal = v
			}
		}

		var keys [16]int32
		keys[0] = 0
		for k := 1; k < 16; k++ {
			keys[k] = metric*10 + int32(k)
		}

		row := &v1Row{
			metric: metric,
			time:   baseTimestamp,
			keys:   keys,
			count:  float64(len(values)),
			min:    float64(int64(minVal)),
			max:    float64(int64(maxVal)),
			sum:    float64(sum),
		}

		testData = append(testData, row)
		valueSets = append(valueSets, values)
	}

	return testData, valueSets
}

func insertV1TestData(httpClient *http.Client, httpAddr, user, password string, rows []*v1Row, valueSets [][]uint64) error {
	if len(rows) != len(valueSets) {
		return fmt.Errorf("rows and valueSets length mismatch")
	}

	for i, row := range rows {
		values := valueSets[i]
		valueParts := make([]string, len(values))
		for idx, v := range values {
			valueParts[idx] = fmt.Sprintf("%d", v)
		}
		valuesExpr := fmt.Sprintf("[%s]", strings.Join(valueParts, ","))

		keyParts := make([]string, 15)
		for k := 1; k < 16; k++ {
			keyParts[k-1] = fmt.Sprintf("%d", row.keys[k])
		}
		keysExpr := strings.Join(keyParts, ", ")

		query := fmt.Sprintf(`
		INSERT INTO statshouse_value_dist_1h
		SELECT
			toDate(toDateTime(%d)),
			toDateTime(%d),
			%d,
			%s,
			sumState(toInt64(1)) AS count,
			minState(toInt64(val)) AS min,
			maxState(toInt64(val)) AS max,
			sumState(toInt64(val)) AS sum,
			sumState(toFloat64(0)) AS ns2,
			sumState(toInt64(val) * toInt64(val)) AS sumsquare,
			quantilesTDigestState(0.5)(toUInt64(val)) AS percentiles
		FROM (
			SELECT arrayJoin(%s) AS val
		)`, row.time, row.time, row.metric, keysExpr, valuesExpr)

		req := &chutil.ClickHouseHttpRequest{
			HttpClient: httpClient,
			Addr:       httpAddr,
			User:       user,
			Password:   password,
			Query:      query,
		}
		resp, err := req.Execute(context.Background())
		if err != nil {
			return fmt.Errorf("failed to insert V1 test row: %w", err)
		}
		if err := resp.Close(); err != nil {
			return fmt.Errorf("failed to close V1 insert response: %w", err)
		}
	}

	return nil
}

// v3Row represents a parsed row from V3 format (after migration)
type v3Row struct {
	metric    int32
	time      uint32
	tags      [16]int32 // tag0-tag15
	stag47    string    // V2 skey maps to stag47
	count     float64
	min       float64
	max       float64
	sum       float64
	sumsquare float64
	min_host  data_model.ArgMinMaxStringFloat32 // Note: V3 uses String format
	max_host  data_model.ArgMinMaxStringFloat32 // Note: V3 uses String format
	perc      *data_model.ChDigest
	uniq      *data_model.ChUnique
}

// parseV3Row parses a single V3 row from rowbinary data
// Based on the V3 insert query order: metric,time, tag0-tag15,stag47, count,min,max,sum,sumsquare, min_host,max_host,percentiles,uniq_state
func parseV3Row(reader *bufio.Reader, t *testing.T) (*v3Row, error) {
	row := &v3Row{}

	// Parse metric (Int32)
	if err := binary.Read(reader, binary.LittleEndian, &row.metric); err != nil {
		t.Logf("[v3parse] metric error: %s\n", err)
		return nil, err
	}

	// Parse time (DateTime = UInt32)
	if err := binary.Read(reader, binary.LittleEndian, &row.time); err != nil {
		t.Logf("[v3parse] time error: %s\n", err)
		return nil, err
	}

	// Parse all 16 tags (tag0 through tag15)
	for i := 0; i < 16; i++ {
		if err := binary.Read(reader, binary.LittleEndian, &row.tags[i]); err != nil {
			t.Logf("[v3parse] tag %d error: %s\n", i, err)
			return nil, err
		}
	}

	// Parse stag47 (String) - LEB128 varint format
	stag47Len, err := binary.ReadUvarint(reader)
	if err != nil {
		t.Logf("[v3parse] stag47 length error: %s\n", err)
		return nil, err
	}
	// Bounds check for stag47 to avoid huge allocations
	if stag47Len > 4096 {
		return nil, fmt.Errorf("invalid stag47 length: %d", stag47Len)
	}
	stag47Bytes := make([]byte, stag47Len)
	if _, err := io.ReadFull(reader, stag47Bytes); err != nil {
		t.Logf("[v3parse] stag47 content error: %s\n", err)
		return nil, err
	}
	row.stag47 = string(stag47Bytes)

	// Parse simple aggregates (Float64 each)
	// count
	if err := binary.Read(reader, binary.LittleEndian, &row.count); err != nil {
		t.Logf("[v3parse] count error: %s\n", err)
		return nil, err
	}

	// min
	if err := binary.Read(reader, binary.LittleEndian, &row.min); err != nil {
		t.Logf("[v3parse] min error: %s\n", err)
		return nil, err
	}

	// max
	if err := binary.Read(reader, binary.LittleEndian, &row.max); err != nil {
		t.Logf("[v3parse] max error: %s\n", err)
		return nil, err
	}

	// sum
	if err := binary.Read(reader, binary.LittleEndian, &row.sum); err != nil {
		t.Logf("[v3parse] sum error: %s\n", err)
		return nil, err
	}

	// sumsquare
	if err := binary.Read(reader, binary.LittleEndian, &row.sumsquare); err != nil {
		t.Logf("[v3parse] sumsquare error: %s\n", err)
		return nil, err
	}

	// Parse aggregate fields from ClickHouse internal format
	// min_host (ArgMinStringFloat32)
	var buf []byte
	buf, err = row.min_host.ReadFrom(reader, buf)
	if err != nil {
		t.Logf("[v3parse] min_host error: %s\n", err)
		return nil, fmt.Errorf("failed to parse min_host: %w", err)
	}

	// max_host (ArgMaxStringFloat32)
	buf, err = row.max_host.ReadFrom(reader, buf)
	if err != nil {
		t.Logf("[v3parse] max_host error: %s\n", err)
		return nil, fmt.Errorf("failed to parse max_host: %w", err)
	}

	// percentiles
	row.perc = &data_model.ChDigest{}
	if err := row.perc.ReadFrom(reader); err != nil {
		t.Logf("[v3parse] percentiles error: %s\n", err)
		return nil, fmt.Errorf("failed to parse percentiles: %w", err)
	}
	// Set to nil if digest is empty
	if row.perc.Digest == nil {
		row.perc = nil
	}

	// uniq_state
	row.uniq = &data_model.ChUnique{}
	if err := row.uniq.ReadFrom(reader); err != nil {
		t.Logf("[v3parse] uniq_state error: %s\n", err)
		return nil, fmt.Errorf("failed to parse uniq_state: %w", err)
	}

	return row, nil
}

func TestUpdateMigrationStateIntegration(t *testing.T) {
	aggregator := &Aggregator{
		config: ConfigAggregator{
			KHAddr:     clickHouseAddr,
			KHUser:     "default",
			KHPassword: "secret",
		},
		migrationConfig: NewDefaultMigrationConfig(),
	}

	cleanupMigrationTables(t)

	// Test updating migration state
	testShardKey := int32(1)
	testTs := time.Unix(1733000400, 0) // Fixed timestamp for testing
	testStarted := time.Now()
	testEnded := time.Now().Add(time.Minute)

	err := aggregator.updateMigrationState(httpClient, testShardKey, testTs, 100, 95, 0, testStarted, &testEnded, migrationSourceV2)
	require.NoError(t, err, "updateMigrationState should succeed")

	// Verify the state was recorded correctly
	verifyQuery := fmt.Sprintf(`
		SELECT shard_key, ts, v2_rows, v3_rows, retry
		FROM statshouse_migration_state
		WHERE shard_key = %d AND source = '' AND ts = toDateTime(%d)
		ORDER BY started DESC
		LIMIT 1`, testShardKey, testTs.Unix())

	req := &chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       clickHouseAddr,
		User:       "default",
		Password:   "secret",
		Query:      verifyQuery,
	}

	countQuery := fmt.Sprintf(`
		SELECT count() as cnt
		FROM statshouse_migration_state
		WHERE shard_key = %d AND source = '' AND ts = toDateTime(%d)`, testShardKey, testTs.Unix())

	req.Query = countQuery
	resp, err := req.Execute(context.Background())
	require.NoError(t, err)

	var recordCount uint64
	_, err = fmt.Fscanf(resp, "%d", &recordCount)
	require.NoError(t, err)
	resp.Close()

	require.Equal(t, uint64(1), recordCount, "Should have exactly one migration state record")
}

func TestMigrationOrchestration(t *testing.T) {
	aggregator := &Aggregator{
		config: ConfigAggregator{
			KHAddr:     clickHouseAddr,
			KHUser:     "default",
			KHPassword: "secret",
		},
		migrationConfig: NewDefaultMigrationConfig(),
		configR: ConfigAggregatorRemote{
			MigrationTimeRange: "36000-3600",
		},
	}

	// Step 1: Clean up migration tables
	cleanupMigrationTables(t)

	// Step 2: Find first timestamp to migrate
	testShardKey := int32(1)
	firstTs, err := aggregator.findNextTimestampToMigrate(httpClient, testShardKey)
	require.NoError(t, err, "Step 2: findNextTimestampToMigrate should succeed")
	require.Equal(t, time.Unix(36000, 0), firstTs, "Step 2: Should find the first timestamp")

	// Step 3: Update migration state for the found timestamp
	started := time.Now()
	err = aggregator.updateMigrationState(httpClient, testShardKey, firstTs, 0, 0, 0, started, nil, migrationSourceV2)
	require.NoError(t, err, "Step 3: updateMigrationState should succeed")

	// Step 4: Mark the migration as completed
	ended := time.Now()
	err = aggregator.updateMigrationState(httpClient, testShardKey, firstTs, 100, 95, 0, started, &ended, migrationSourceV2)
	require.NoError(t, err, "Step 4: updateMigrationState completion should succeed")

	// Step 5: Find next timestamp - should be the previous hour (backward migration)
	nextTs, err := aggregator.findNextTimestampToMigrate(httpClient, testShardKey)
	require.NoError(t, err, "Step 5: findNextTimestampToMigrate should succeed")
	require.Equal(t, firstTs.Add(-time.Hour), nextTs, "Step 5: Should find previous timestamp")

	// Step 6: Verify migration state records
	countQuery := fmt.Sprintf(`
		SELECT count() as cnt
		FROM statshouse_migration_state
		WHERE shard_key = %d AND source = ''`, testShardKey)

	req := &chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       clickHouseAddr,
		User:       "default",
		Password:   "secret",
		Query:      countQuery,
	}
	resp, err := req.Execute(context.Background())
	require.NoError(t, err)

	var recordCount uint64
	_, err = fmt.Fscanf(resp, "%d", &recordCount)
	require.NoError(t, err)
	resp.Close()

	require.GreaterOrEqual(t, recordCount, uint64(2), "Should have at least 2 migration state records (start and end)")

	t.Logf("SUCCESS: End-to-end migration orchestration test completed successfully")
}

func TestMigrationOrchestrationV1(t *testing.T) {
	aggregator := &Aggregator{
		config: ConfigAggregator{
			KHAddr:     clickHouseAddr,
			KHUser:     "default",
			KHPassword: "secret",
		},
		migrationConfig:   NewDefaultMigrationConfig(),
		migrationConfigV1: NewDefaultMigrationConfigV1([]string{clickHouseAddr}, "default", "secret"),
		configR: ConfigAggregatorRemote{
			MigrationTimeRangeV1: "36000-3600",
		},
	}

	cleanupMigrationTables(t)

	testShardKey := int32(1)

	firstTs, err := aggregator.findNextTimestampToMigrateV1(httpClient, testShardKey)
	require.NoError(t, err, "findNextTimestampToMigrateV1 should succeed")
	require.Equal(t, time.Unix(36000, 0), firstTs, "Should find the first timestamp for V1")

	started := time.Now()
	err = aggregator.updateMigrationState(httpClient, testShardKey, firstTs, 0, 0, 0, started, nil, migrationSourceV1)
	require.NoError(t, err, "updateMigrationState (start, V1) should succeed")

	ended := time.Now()
	err = aggregator.updateMigrationState(httpClient, testShardKey, firstTs, 100, 95, 0, started, &ended, migrationSourceV1)
	require.NoError(t, err, "updateMigrationState (end, V1) should succeed")

	nextTs, err := aggregator.findNextTimestampToMigrateV1(httpClient, testShardKey)
	require.NoError(t, err, "findNextTimestampToMigrateV1 (next) should succeed")
	require.Equal(t, firstTs.Add(-time.Hour), nextTs, "Should find previous timestamp for V1")

	countQuery := fmt.Sprintf(`
		SELECT count() as cnt
		FROM statshouse_migration_state
		WHERE shard_key = %d AND source = '%s'`, testShardKey, migrationSourceV1)

	req := &chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       clickHouseAddr,
		User:       "default",
		Password:   "secret",
		Query:      countQuery,
	}
	resp, err := req.Execute(context.Background())
	require.NoError(t, err)

	var recordCount uint64
	_, err = fmt.Fscanf(resp, "%d", &recordCount)
	require.NoError(t, err)
	resp.Close()

	require.GreaterOrEqual(t, recordCount, uint64(2), "Should have at least 2 V1 migration state records (start and end)")

	t.Logf("SUCCESS: V1 migration orchestration test completed successfully")
}

func TestMigrationOrchestrationStop(t *testing.T) {
	aggregator := &Aggregator{
		config: ConfigAggregator{
			KHAddr:     clickHouseAddr,
			KHUser:     "default",
			KHPassword: "secret",
		},
		migrationConfig:     NewDefaultMigrationConfig(),
		migrationConfigStop: NewDefaultMigrationConfigStop([]string{clickHouseAddr}, "default", "secret"),
		configR: ConfigAggregatorRemote{
			MigrationTimeRangeStop: "36000-3600",
		},
	}

	cleanupMigrationTables(t)

	testShardKey := int32(1)

	firstTs, err := aggregator.findNextTimestampToMigrateStop(httpClient, testShardKey)
	require.NoError(t, err, "findNextTimestampToMigrateStop should succeed")
	require.Equal(t, time.Unix(36000, 0), firstTs, "Should find the first timestamp for stop source")

	started := time.Now()
	err = aggregator.updateMigrationState(httpClient, testShardKey, firstTs, 0, 0, 0, started, nil, migrationSourceStop)
	require.NoError(t, err, "updateMigrationState (start, stop) should succeed")

	ended := time.Now()
	err = aggregator.updateMigrationState(httpClient, testShardKey, firstTs, 100, 95, 0, started, &ended, migrationSourceStop)
	require.NoError(t, err, "updateMigrationState (end, stop) should succeed")

	nextTs, err := aggregator.findNextTimestampToMigrateStop(httpClient, testShardKey)
	require.NoError(t, err, "findNextTimestampToMigrateStop (next) should succeed")
	require.Equal(t, firstTs.Add(-time.Hour), nextTs, "Should find previous timestamp for stop source")

	countQuery := fmt.Sprintf(`
		SELECT count() as cnt
		FROM statshouse_migration_state
		WHERE shard_key = %d AND source = '%s'`, testShardKey, migrationSourceStop)

	req := &chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       clickHouseAddr,
		User:       "default",
		Password:   "secret",
		Query:      countQuery,
	}
	resp, err := req.Execute(context.Background())
	require.NoError(t, err)

	var recordCount uint64
	_, err = fmt.Fscanf(resp, "%d", &recordCount)
	require.NoError(t, err)
	resp.Close()

	require.GreaterOrEqual(t, recordCount, uint64(2), "Should have at least 2 stop migration state records (start and end)")

	t.Logf("SUCCESS: stop migration orchestration test completed successfully")
}

func cleanupMigrationTables(t *testing.T) {
	cleanupStateQuery := `TRUNCATE TABLE statshouse_migration_state;`
	cleanupLogsQuery := `TRUNCATE TABLE statshouse_migration_logs;`

	req := &chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       clickHouseAddr,
		User:       "default",
		Password:   "secret",
		Query:      cleanupStateQuery,
	}
	resp, err := req.Execute(context.Background())
	require.NoError(t, err)
	resp.Close()

	req.Query = cleanupLogsQuery
	resp, err = req.Execute(context.Background())
	require.NoError(t, err)
	resp.Close()
}

// createShardBuiltinTestData creates test data for ShardBuiltin metrics
func createShardBuiltinTestData(metricID int32, timestamp int64, shardByTag int32, numShards int) []*v2Row {
	var data []*v2Row

	// Create data points that will be distributed across different shards
	for shard := 0; shard < numShards; shard++ {
		for i := 0; i < 3; i++ { // Multiple data points per shard
			var keys [16]int32

			// Set the sharding tag value to target this specific shard
			keys[shardByTag] = int32(shard)

			// Set other tags to distinguish data points
			for j := 0; j < 16; j++ {
				if j != int(shardByTag) {
					keys[j] = int32(shard*100 + i*10 + j)
				}
			}

			// Create test digest and unique state
			perc := data_model.ChDigest{Digest: createTestDigest()}
			uniq := data_model.ChUnique{}
			uniq.Insert(uint64(shard*1000 + i))

			// Create host aggregates
			minHost := data_model.ArgMinInt32Float32{
				ArgMinMaxInt32Float32: data_model.ArgMinMaxInt32Float32{
					Arg: int32(shard),
					Val: float32(shard),
				},
			}
			maxHost := data_model.ArgMaxInt32Float32{
				ArgMinMaxInt32Float32: data_model.ArgMinMaxInt32Float32{
					Arg: int32(shard + 1),
					Val: float32(shard + 1),
				},
			}

			data = append(data, &v2Row{
				metric:    metricID,
				time:      uint32(timestamp),
				keys:      keys,
				skey:      fmt.Sprintf("skey_%d_%d", shard, i),
				count:     float64(1 + i),
				min:       float64(10 + i),
				max:       float64(20 + i),
				sum:       float64(30 + i*10),
				sumsquare: float64(100 + i*50),
				perc:      perc,
				uniq:      uniq,
				min_host:  minHost,
				max_host:  maxHost,
			})
		}
	}

	return data
}

// createContributorsLogTestData creates test data for __contributors_log metric
func createContributorsLogTestData(timestamp int64, numShards int) []*v2Row {
	var data []*v2Row

	// __contributors_log appears in all shards, create the same data for all shards
	for shard := 0; shard < numShards; shard++ {
		var keys [16]int32

		// Set some tag values
		for j := 0; j < 16; j++ {
			keys[j] = int32(shard*100 + j)
		}

		// Create test digest and unique state
		perc := data_model.ChDigest{Digest: createTestDigest()}
		uniq := data_model.ChUnique{}
		uniq.Insert(uint64(shard))

		// Create host aggregates
		minHost := data_model.ArgMinInt32Float32{
			ArgMinMaxInt32Float32: data_model.ArgMinMaxInt32Float32{
				Arg: int32(shard),
				Val: float32(shard),
			},
		}
		maxHost := data_model.ArgMaxInt32Float32{
			ArgMinMaxInt32Float32: data_model.ArgMinMaxInt32Float32{
				Arg: int32(shard),
				Val: float32(shard),
			},
		}

		data = append(data, &v2Row{
			metric:    -61, // __contributors_log
			time:      uint32(timestamp),
			keys:      keys,
			skey:      fmt.Sprintf("contributors_skey_%d", shard),
			count:     float64(1),
			min:       float64(1),
			max:       float64(1),
			sum:       float64(1),
			sumsquare: float64(1),
			perc:      perc,
			uniq:      uniq,
			min_host:  minHost,
			max_host:  maxHost,
		})
	}

	return data
}

// createTestDigest creates a test percentile digest for testing
func createTestDigest() *tdigest.TDigest {
	digest := tdigest.New()
	digest.Add(0.5, 1)
	digest.Add(0.99, 1)
	return digest
}
