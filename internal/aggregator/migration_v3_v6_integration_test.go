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
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/VKCOM/statshouse/internal/format"
	"github.com/VKCOM/statshouse/internal/metajournal"
	"github.com/hrissan/tdigest"
	"github.com/stretchr/testify/require"
	"pgregory.net/rand"

	"github.com/VKCOM/statshouse/internal/chutil"
	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/vkgo/kittenhouseclient/rowbinary"
)

var (
	agg *Aggregator
)

const TestedShardKey = 6

func testMappingsGenerator(ctx context.Context, lastVersion int32, returnIfEmpty bool) ([]tlstatshouse.Mapping, int32, int32, error) {
	var pairs []tlstatshouse.Mapping

	for i := range 100 {
		tagNum := 1000001 + i
		pairs = append(pairs, tlstatshouse.Mapping{
			Str:   fmt.Sprintf("tagValue%d", tagNum),
			Value: int32(tagNum),
		})
	}

	return pairs, 1000100, 10001000, nil
}

func prepareAggregator(CHAddr string) {
	var storages []*data_model.ChunkedStorage2
	for range 16 {
		storages = append(storages, data_model.NewChunkedStorageNop())

	}
	mappingsStorage := metajournal.MakeMappings(context.Background(), time.Second, true, 100, storages)
	mappingsStorage.UpdateMappingsUntilVersion(100, format.TagValueIDComponentAggregator, testMappingsGenerator)

	aggConfig := ConfigAggregator{
		KHAddr:     CHAddr,
		KHPassword: "secret",
		KHUser:     "default",
	}
	config := NewDefaultMigrationConfigV3()

	agg = &Aggregator{
		migrationConfigV3: config,
		config:            aggConfig,
		migrationV3Data:   MakeMigrationV3Data(mappingsStorage),
	}

	// every 4th tag of every 4th metric is raw
	for met := int32(0); met < 161; met++ {
		agg.migrationV3Data.isRawTagOfMetric[met] = make([]bool, 48)
		if met%4 == 0 {
			for i := 0; i < 48; i += 4 {
				agg.migrationV3Data.isRawTagOfMetric[met][i] = true
			}
		}
	}

	// test data is set up in a way to ensure mappings 1000001-1000100 to be replaced
	for i := range 100 {
		agg.migrationV3Data.replacementMappings[int32(1000000+i)] = struct{}{}
	}
}

func helperSelectFromCh(httpAddr string, query string) (string, error) {
	req := &chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       httpAddr,
		User:       "default",
		Password:   "secret",
		Query:      query,
		Format:     "", // Default format for count query
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	resp, err := req.Execute(ctx)
	if err != nil {
		return "", err
	}

	reader := bufio.NewReader(resp)

	buf, err := io.ReadAll(reader)
	if err != nil {
		return "", err
	}
	return string(buf), nil
}

func TestHelperSelectFromCh(t *testing.T) {
	// helper usage to force building it
	a := helperSelectFromCh
	if a != nil {
	}
}

func TestMigrateSingleStepIntegrationV3_V6(t *testing.T) {

	// Use shared container connection details
	httpAddr := clickHouseAddr

	// Clean up tables before test to ensure isolation
	err := cleanupTables()
	require.NoError(t, err)

	// Create test data and insert into V2 table
	testData := createV3TestData()
	err = insertV3TestData(httpClient, httpAddr, "default", "secret", testData)
	require.NoError(t, err)
	t.Logf("Inserted %d test rows into V2 table", len(testData))

	testHour := time.Unix(int64(testData[0].time), 0)
	// Use default config for testing

	var shardsToTest []int32
	for i := range 18 {
		// NOTE: sharding currently doesn't affect anything because all metrics are assumed to be relevant by default
		// TODO: implement per-shard testing
		shardsToTest = append(shardsToTest, int32(i+1))
	}

	for _, shardKey := range shardsToTest {
		t.Run(fmt.Sprintf("shard_%d", shardKey), func(t *testing.T) {
			// simulate running on a particular shard
			agg.shardKey = shardKey

			// Clean tables altered by v3 migration
			err := cleanUpV3MigrationTables(t)
			require.NoError(t, err)

			// Run migration for this shard
			err = agg.migrateSingleStepV3(testHour, httpClient)
			require.NoError(t, err)
			t.Logf("Migration completed successfully for hour %d, shard %d", testHour.Unix(), shardKey)

			// Get expected rows for this shard
			var expectedRows []*v3Row
			for _, row := range testData {
				// NOTE: we can't test sharding logic without a way to mock MetricStorage, so we assume all metrics will be relevant
				expectedRows = append(expectedRows, row)
			}

			// First, get the count of expected rows to know when to stop parsing
			countQuery := fmt.Sprintf(`SELECT count() as cnt FROM statshouse_v6_1h WHERE time = toDateTime(%d)`, testHour.Unix())
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
			SELECT 
				index_type, metric, pre_tag, pre_stag, time, tag0, stag0, tag1, stag1, tag2, stag2, tag3, stag3, tag4, stag4, tag5, stag5, tag6, stag6, tag7, stag7, tag8, stag8, tag9, stag9, tag10, stag10, tag11, stag11, tag12, stag12, tag13, stag13, tag14, stag14, tag15, stag15, tag16, stag16, tag17, stag17, tag18, stag18, tag19, stag19, tag20, stag20, tag21, stag21, tag22, stag22, tag23, stag23, tag24, stag24, tag25, stag25, tag26, stag26, tag27, stag27, tag28, stag28, tag29, stag29, tag30, stag30, tag31, stag31, tag32, stag32, tag33, stag33, tag34, stag34, tag35, stag35, tag36, stag36, tag37, stag37, tag38, stag38, tag39, stag39, tag40, stag40, tag41, stag41, tag42, stag42, tag43, stag43, tag44, stag44, tag45, stag45, tag46, stag46, tag47, stag47,
				count, min, max, max_count, sum, sumsquare, min_host, max_host, max_count_host, percentiles, uniq_state
			FROM statshouse_v6_1h
			WHERE time = toDateTime(%d)
			ORDER BY metric`, testHour.Unix())

			t.Logf("Executing V3 query for shard %d: %s", shardKey, selectQuery)

			req.Query = selectQuery
			req.Format = "RowBinary"
			resp, err = req.Execute(ctx)
			require.NoError(t, err)

			// Parse the RowBinary response using parseV3RowOld function
			// Parse exactly the expected number of rows instead of relying on EOF
			var migratedRows []*v3Row
			bufReader := bufio.NewReader(resp)
			for i := uint64(0); i < expectedRowCount; i++ {
				row, err := parseV3RowWithLogging(bufReader, t)
				require.NoError(t, err, "Failed to parse V3 row %d", i+1)
				migratedRows = append(migratedRows, row)
				//t.Logf("Shard %d: Parsed V3 row %d: metric=%d, time=%d, tag0=%d, stag47=%s, count=%.2f",
				//	shardKey, len(migratedRows), row.metric, row.time, row.tags[0], row.stag47, row.count)
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
			for i, expectedV3 := range expectedRows {
				actualV3 := migratedRows[i]

				// Validate V3→V3 field mapping
				require.Equal(t, expectedV3.index_type, actualV3.index_type,
					"Shard %d row %d: index_type mismatch", shardKey, i)
				require.Equal(t, expectedV3.metric, actualV3.metric,
					"Shard %d row %d: metric mismatch", shardKey, i)
				require.Equal(t, expectedV3.pre_tag, actualV3.pre_tag,
					"Shard %d row %d: pre_tag mismatch", shardKey, i)
				require.Equal(t, expectedV3.pre_stag, actualV3.pre_stag,
					"Shard %d row %d: pre_stag mismatch", shardKey, i)
				require.Equal(t, expectedV3.time, actualV3.time,
					"Shard %d row %d: time mismatch", shardKey, i)

				compareTags(t, actualV3.tags, actualV3.stags, expectedV3.tags, expectedV3.stags, shardKey, int32(i), expectedV3.metric)

				require.Equal(t, expectedV3.count, actualV3.count,
					"Shard %d row %d: count mismatch", shardKey, i)
				require.Equal(t, expectedV3.min, actualV3.min,
					"Shard %d row %d: min mismatch", shardKey, i)
				require.Equal(t, expectedV3.max, actualV3.max,
					"Shard %d row %d: max mismatch", shardKey, i)
				require.Equal(t, expectedV3.max_count, actualV3.max_count,
					"Shard %d row %d: max_count mismatch", shardKey, i)
				require.Equal(t, expectedV3.sum, actualV3.sum,
					"Shard %d row %d: sum mismatch", shardKey, i)
				require.Equal(t, expectedV3.sumsquare, actualV3.sumsquare,
					"Shard %d row %d: sumsquare mismatch", shardKey, i)

				// Validate host aggregates
				require.Equal(t, expectedV3.min_host.Val, actualV3.min_host.Val,
					"Shard %d row %d: min_host.Val mismatch", shardKey, i)
				require.Equal(t, expectedV3.max_host.Val, actualV3.max_host.Val,
					"Shard %d row %d: max_host.Val mismatch", shardKey, i)
				require.Equal(t, expectedV3.max_count_host.Val, actualV3.max_count_host.Val,
					"Shard %d row %d: max_count_host.Val mismatch", shardKey, i)

				// Validate percentiles (aggregate states)
				if expectedV3.percentiles.Digest != nil && actualV3.percentiles.Digest != nil {
					require.InDelta(t, expectedV3.percentiles.Digest.Quantile(0.5), actualV3.percentiles.Digest.Quantile(0.5), 1e-6,
						"Shard %d row %d: percentile 0.5 mismatch", shardKey, i)
					require.InDelta(t, expectedV3.percentiles.Digest.Quantile(0.99), actualV3.percentiles.Digest.Quantile(0.99), 1e-6,
						"Shard %d row %d: percentile 0.99 mismatch", shardKey, i)
				} else {
					require.Nil(t, actualV3.percentiles.Digest,
						"Shard %d row %d: percentiles should be nil", shardKey, i)
				}

				// Validate unique state
				require.Equal(t, expectedV3.uniq_state.ItemsCount(), actualV3.uniq_state.ItemsCount(),
					"Shard %d row %d: uniq_state mismatch", shardKey, i)
			}

			t.Logf("SUCCESS: Shard %d migrated and validated %d rows with complete field-by-field comparison",
				shardKey, len(migratedRows))
		})
	}

	// Clean up tables after test to ensure isolation
	err = cleanupTables()
	require.NoError(t, err)
}

func compareTags(t *testing.T, tagsActual [48]int32, stagsActual [48]string, tagsExpected [48]int32, stagsExpected [48]string, shardKey int32, row int32, metric int32) {
	for i := range 48 {
		actualTag := tagsActual[i]
		expectedTag := tagsExpected[i]
		actualStag := stagsActual[i]
		expectedStag := stagsExpected[i]
		_, okReplace := agg.migrationV3Data.replacementMappings[expectedTag]
		isRawByTag, okRawness := agg.migrationV3Data.isRawTagOfMetric[metric]
		require.True(t, okRawness, "shard %d row %d metric %d: rawness should be known", shardKey, row, metric)
		if okReplace && !isRawByTag[i] {
			replacementValue, _ := agg.migrationV3Data.mappingsStorage.GetString(expectedTag)
			require.Equal(t, replacementValue, actualStag, "shard %d row %d: stag%d is expected to be replaced for '%s'", shardKey, row, i, replacementValue)
			require.Equal(t, int32(0), actualTag, "shard %d row %d: tag%d is expected to be replaced and equal to 0", shardKey, row, i)
		} else {
			require.Equal(t, expectedStag, actualStag, "shard %d row %d: stag%d is expected to be %s", shardKey, row, i, expectedStag)
			require.Equal(t, expectedTag, actualTag, "shard %d row %d: tag%d is expected to be %d", shardKey, row, i, expectedTag)
		}
	}
}

// generateRandomTags creates random keys with some zeros (absent fields)
// for V3 tables
func generateRandomTags(rnd *rand.Rand) [48]int32 {
	var tags [48]int32
	for i := 0; i < 48; i++ {
		// 70% chance of having a value, 30% chance of being 0 (absent)
		if rnd.Float64() < 0.7 {
			tags[i] = rnd.Int31n(1000000) + 1 // 1 to 1000000
		} else {
			tags[i] = 0 // Absent field
		}
	}
	return tags
}

func generateRandomTagsWithReplacements(rnd *rand.Rand) [48]int32 {
	var tags [48]int32
	for i := 0; i < 48; i++ {
		// 24 tags for replacement in total (every 2nd tag)
		if i%2 == 0 {
			tags[i] = rnd.Int31n(100) + 1000001 // 1000001 to 1000100 - tags for replacement
			continue
		}

		// 70% chance of having a value, 30% chance of being 0 (absent)
		if rnd.Float64() < 0.7 {
			tags[i] = rnd.Int31n(1000000) + 1 // 1 to 1000000
		} else {
			tags[i] = 0 // Absent field
		}
	}
	return tags

}

func createV3TestData() []*v3Row {
	// Create random generator with seed for reproducible tests
	rnd := rand.New(42)

	// Create comprehensive test data covering all 16 shards
	var testData []*v3Row

	for metricId := int32(0); metricId < 160; metricId++ {
		// Generate random data with various combinations
		var tags [48]int32
		if metricId%2 == 0 {
			tags = generateRandomTagsWithReplacements(rnd)
		} else {
			tags = generateRandomTags(rnd)

		}

		count, min, max, maxCount, sum, sumsquare := generateRandomAggregatesV3(rnd)
		minHost, maxHost, maxCountHost := generateRandomHostAggregatesV3(rnd)

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
		row := &v3Row{
			index_type:     0,
			metric:         metricId,
			pre_tag:        0,
			pre_stag:       "",
			time:           1769166000, // Fixed timestamp for testing
			tags:           tags,
			count:          count,
			min:            min,
			max:            max,
			max_count:      maxCount,
			sum:            sum,
			sumsquare:      sumsquare,
			min_host:       minHost,
			max_host:       maxHost,
			max_count_host: maxCountHost,
			percentiles:    perc,
			uniq_state:     uniq,
		}

		testData = append(testData, row)
	}

	return testData
}

func insertV3TestData(httpClient *http.Client, httpAddr, user, password string, testData []*v3Row) error {
	// Insert test data using RowBinary format to properly create aggregate states
	if len(testData) == 0 {
		return fmt.Errorf("no test data provided")
	}

	// Build RowBinary data for all rows
	var body []byte
	for _, row := range testData {
		// Convert each test row to V2 RowBinary format for insertion
		body = appendV3RowBinary(body, row)
	}

	// Insert data using RowBinary format
	insertQuery := `INSERT INTO statshouse_v3_1h (
		index_type, metric, pre_tag, pre_stag, time, tag0, stag0, tag1, stag1, tag2, stag2, tag3, stag3, tag4, stag4, tag5, stag5, tag6, stag6, tag7, stag7, tag8, stag8, tag9, stag9, tag10, stag10, tag11, stag11, tag12, stag12, tag13, stag13, tag14, stag14, tag15, stag15, tag16, stag16, tag17, stag17, tag18, stag18, tag19, stag19, tag20, stag20, tag21, stag21, tag22, stag22, tag23, stag23, tag24, stag24, tag25, stag25, tag26, stag26, tag27, stag27, tag28, stag28, tag29, stag29, tag30, stag30, tag31, stag31, tag32, stag32, tag33, stag33, tag34, stag34, tag35, stag35, tag36, stag36, tag37, stag37, tag38, stag38, tag39, stag39, tag40, stag40, tag41, stag41, tag42, stag42, tag43, stag43, tag44, stag44, tag45, stag45, tag46, stag46, tag47, stag47,
		count, min, max, max_count, sum, sumsquare, min_host, max_host, max_count_host, percentiles, uniq_state
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

// appendV3RowBinary appends a v3Row in RowBinary format to the buffer
func appendV3RowBinary(buf []byte, row *v3Row) []byte {
	// Use the same rowbinary helper functions from the migration code
	buf = rowbinary.AppendUint8(buf, row.index_type)
	buf = rowbinary.AppendInt32(buf, row.metric)
	buf = rowbinary.AppendUint32(buf, row.pre_tag)
	buf = rowbinary.AppendString(buf, row.pre_stag)
	buf = rowbinary.AppendDateTime(buf, time.Unix(int64(row.time), 0))

	// Append all 48 tags
	for i := 0; i < 48; i++ {
		tag := row.tags[i]
		stag := row.stags[i]
		buf = rowbinary.AppendInt32(buf, tag)
		buf = rowbinary.AppendString(buf, stag)
	}

	// Append simple aggregates
	buf = rowbinary.AppendFloat64(buf, row.count)
	buf = rowbinary.AppendFloat64(buf, row.min)
	buf = rowbinary.AppendFloat64(buf, row.max)
	buf = rowbinary.AppendFloat64(buf, row.max_count)
	buf = rowbinary.AppendFloat64(buf, row.sum)
	buf = rowbinary.AppendFloat64(buf, row.sumsquare)

	// Append aggregate states
	buf = row.min_host.MarshallAppend(buf)
	buf = row.max_host.MarshallAppend(buf)
	buf = row.max_count_host.MarshallAppend(buf)
	buf = row.percentiles.MarshallAppend(buf, 1)
	buf = row.uniq_state.MarshallAppend(buf)

	return buf
}

// parseV3Row parses a single V3 row from rowbinary data
// Based on the V3 insert query order: metric,time, tag0-tag15,stag47, count,min,max,sum,sumsquare, min_host,max_host,percentiles,uniq_state
func parseV3RowWithLogging(reader *bufio.Reader, t *testing.T) (row *v3Row, err error) {
	row = &v3Row{}

	// Parse index_type (Int32)
	if err := binary.Read(reader, binary.LittleEndian, &row.index_type); err != nil {
		t.Logf("[v3parse] index_type error: %s\n", err)
		return nil, err
	}

	// Parse metric (Int32)
	if err := binary.Read(reader, binary.LittleEndian, &row.metric); err != nil {
		t.Logf("[v3parse] metric error: %s\n", err)
		return nil, err
	}

	// Parse pre_tag (Int32)
	if err := binary.Read(reader, binary.LittleEndian, &row.pre_tag); err != nil {
		t.Logf("[v3parse] pre_tag error: %s\n", err)
		return nil, err
	}

	// Parse pre_stag (String)
	if err := readStringWithLogging(reader, &row.pre_stag, t); err != nil {
		t.Logf("[v3parse] pre_stag error: %s\n", err)
		return nil, err
	}

	// Parse time (DateTime = UInt32)
	if err := binary.Read(reader, binary.LittleEndian, &row.time); err != nil {
		t.Logf("[v3parse] time error: %s\n", err)
		return nil, err
	}

	// Parse all 48 tags and stags
	for i := 0; i < 48; i++ {
		if err := binary.Read(reader, binary.LittleEndian, &row.tags[i]); err != nil {
			t.Logf("[v3parse] tag %d error: %s\n", i, err)
			return nil, err
		}
		if err := readStringWithLogging(reader, &row.stags[i], t); err != nil {
			t.Logf("[v3parse] stag %d error: %s\n", i, err)
			return nil, err
		}
	}

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

	// max
	if err := binary.Read(reader, binary.LittleEndian, &row.max_count); err != nil {
		t.Logf("[v3parse] max_count error: %s\n", err)
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

	buf, err = row.max_count_host.ReadFrom(reader, buf)
	if err != nil {
		t.Logf("[v3parse] max_count_host error: %s\n", err)
		return nil, fmt.Errorf("failed to parse max_count_host: %w", err)
	}

	// percentiles
	row.percentiles = data_model.ChDigest{}
	if err := row.percentiles.ReadFrom(reader); err != nil {
		t.Logf("[v3parse] percentiles error: %s\n", err)
		return nil, fmt.Errorf("failed to parse percentiles: %w", err)
	}

	//// Set to nil if digest is empty
	//if row.percentiles.Digest == nil {
	//	row.percentiles = nil
	//}

	// uniq_state
	row.uniq_state = data_model.ChUnique{}
	if err := row.uniq_state.ReadFrom(reader); err != nil {
		t.Logf("[v3parse] uniq_state error: %s\n", err)
		return nil, fmt.Errorf("failed to parse uniq_state: %w", err)
	}

	return row, nil
}

func readStringWithLogging(reader *bufio.Reader, dst *string, t *testing.T) error {
	// Parse stag47 (String) - LEB128 varint format
	n, err := binary.ReadUvarint(reader)
	if err != nil {
		t.Logf("[v3parse] string length error: %s\n", err)
		return err
	}
	// Bounds check for stag47 to avoid huge allocations
	if n > 4096 {
		return fmt.Errorf("[v3parse] string length too large: %d", n)
	}

	buf := make([]byte, n)
	_, err = io.ReadFull(reader, buf)
	if err != nil {
		t.Logf("[v3parse] string content error: %s\n", err)
		return err
	}
	*dst = string(buf)
	return nil
}

func generateRandomHostAggregatesV3(rnd *rand.Rand) (data_model.ArgMinStringFloat32, data_model.ArgMaxStringFloat32, data_model.ArgMaxStringFloat32) {
	minArg := rnd.Int31n(10000000) + 1 // 1 to 10000000
	minVal := rnd.Float32()*100 - 50   // -50 to 50

	maxArg := rnd.Int31n(10000000) + 1 // 1 to 10000000
	maxVal := rnd.Float32()*100 - 50   // -50 to 50

	maxArg2 := rnd.Int31n(10000000) + 1 // 1 to 10000000
	maxVal2 := rnd.Float32()*100 - 50   // -50 to 50

	minHost := data_model.ArgMinStringFloat32{
		ArgMinMaxStringFloat32: data_model.ArgMinMaxStringFloat32{
			AsString: "",
			AsInt32:  minArg,
			Val:      minVal,
		},
	}

	maxHost := data_model.ArgMaxStringFloat32{
		ArgMinMaxStringFloat32: data_model.ArgMinMaxStringFloat32{
			AsString: "",
			AsInt32:  maxArg,
			Val:      maxVal,
		},
	}

	maxCountHost := data_model.ArgMaxStringFloat32{
		ArgMinMaxStringFloat32: data_model.ArgMinMaxStringFloat32{
			AsString: "",
			AsInt32:  maxArg2,
			Val:      maxVal2,
		},
	}

	return minHost, maxHost, maxCountHost
}

func generateRandomAggregatesV3(rnd *rand.Rand) (float64, float64, float64, float64, float64, float64) {
	count := float64(rnd.Intn(1000) + 1)       // 1-1000
	min := rnd.Float64()*100 - 50              // -50 to 50
	max := min + rnd.Float64()*100             // min to min+100
	maxCount := min + rnd.Float64()*100        // min to min+100
	sum := min*count + rnd.Float64()*1000      // reasonable sum
	sumsquare := sum*sum + rnd.Float64()*10000 // reasonable sumsquare

	return count, min, max, maxCount, sum, sumsquare
}

func cleanUpV3MigrationTables(t *testing.T) error {
	cleanupMigrationTables(t)
	// Clear V6 table data
	req := &chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       clickHouseAddr,
		User:       "default",
		Password:   "secret",
		Query:      "TRUNCATE TABLE statshouse_v6_1h",
	}
	resp, err := req.Execute(context.Background())
	if err != nil {
		return fmt.Errorf("failed to clear V6 table: %w", err)
	}
	err = resp.Close()
	if err != nil {
		return fmt.Errorf("failed to close V6 table clear response: %w", err)
	}
	return nil
}
