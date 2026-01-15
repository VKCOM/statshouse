// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package aggregator

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"strings"
	"time"

	"pgregory.net/rand"

	"github.com/VKCOM/statshouse/internal/chutil"
	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/format"
	"github.com/VKCOM/statshouse/internal/vkgo/kittenhouseclient/rowbinary"
)

// MigrationConfigV1 holds configuration for migration operations from V1 schema to V3.
type MigrationConfigV1 struct {
	V1TableName    string        // Source table name (default: "statshouse_value_dist_1h")
	V3TableName    string        // Destination table name (default: "statshouse_v3_1h")
	StateTableName string        // Migration state table name
	LogsTableName  string        // Migration logs table name
	StepDuration   time.Duration // Time step for migration (default: time.Hour)
	TotalShards    int           // Total number of shards (default: 16)
	V1Hosts        []string      // List of ClickHouse V1 hosts (required)
	V1User         string        // ClickHouse V1 user
	V1Password     string        // ClickHouse V1 password
}

// NewDefaultMigrationConfigV1 returns a MigrationConfigV1 with default values.
func NewDefaultMigrationConfigV1(v1Adrs []string, v1User string, v1Pswd string) *MigrationConfigV1 {
	return &MigrationConfigV1{
		V1TableName:    "statshouse_value_dist_1h",
		V3TableName:    "statshouse_v3_1h",
		StateTableName: "statshouse_migration_state",
		LogsTableName:  "statshouse_migration_logs",
		StepDuration:   time.Hour,
		TotalShards:    16,
		V1Hosts:        v1Adrs,
		V1User:         v1User,
		V1Password:     v1Pswd,
	}
}

func getConditionForSelectV1(totalShards int, shardKey int32) string {
	shardingMetrics := getBuiltinMetricsSorted()

	var builtinMetricIDs []string
	var builtinConditions []string

	for _, metric := range shardingMetrics {
		if metric.ShardStrategy != format.ShardBuiltinDist {
			continue
		}
		metricID := metric.MetricID
		tagID := metric.MetricTagIndex()
		builtinMetricIDs = append(builtinMetricIDs, fmt.Sprintf("%d", metricID))

		if tagID == 0 {
			continue
		}
		builtinConditions = append(builtinConditions,
			fmt.Sprintf("(stats = %d AND key%d %% %d = %d)", metricID, tagID, totalShards, shardKey-1))
	}

	regularCondition := fmt.Sprintf("(stats %% %d = %d AND stats NOT IN (%s))",
		totalShards, shardKey-1, strings.Join(builtinMetricIDs, ", "))

	conditions := append([]string{regularCondition}, builtinConditions...)
	if len(conditions) == 0 {
		return "1"
	}
	return "(" + strings.Join(conditions, " OR ") + ")"
}

// goMigrateV1 runs migration loop for V1 data source.
func (a *Aggregator) goMigrateV1(cancelCtx context.Context) {
	if a.replicaKey != 1 {
		log.Printf("[migration_v1] Skipping migration: replica key is %d, expected 1", a.replicaKey)
		return
	}
	if a.shardKey > int32(a.migrationConfigV1.TotalShards) {
		log.Printf("[migration_v1] Skipping migration: shard key is %d, expected less than %d", a.shardKey, a.migrationConfigV1.TotalShards)
		return
	}

	shardKey := a.shardKey
	httpClient := makeHTTPClient()

	log.Printf("[migration_v1] Starting migration routine for shard %d", shardKey)
	for {
		select {
		case <-cancelCtx.Done():
			log.Println("[migration_v1] Exiting migration routine (context cancelled)")
			return
		default:
		}

		a.configMu.RLock()
		delaySec := a.configR.MigrationDelaySec
		if a.configR.MigrationTimeRangeV1 == "" {
			a.configMu.RUnlock()
			log.Println("[migration_v1] Migration disabled: no time range configured")
			time.Sleep(time.Duration(delaySec) * time.Second)
			continue
		}
		a.configMu.RUnlock()

		a.migrationMu.Lock()
		insertTimeEWMA := a.insertTimeEWMA
		lastErrorTs := a.lastErrorTs
		a.migrationMu.Unlock()

		nowUnix := uint32(time.Now().Unix())
		if lastErrorTs != 0 && nowUnix >= lastErrorTs && nowUnix-lastErrorTs < noErrorsWindow {
			log.Printf("[migration_v1] Skipping: last error was %d seconds ago", nowUnix-lastErrorTs)
			time.Sleep(time.Duration(noErrorsWindow-(nowUnix-lastErrorTs)) * time.Second)
			continue
		}
		if insertTimeEWMA > maxInsertTime {
			log.Printf("[migration_v1] Skipping: EWMA insert time is too high (%.2fs)", insertTimeEWMA)
			time.Sleep(time.Duration(delaySec) * time.Second)
			continue
		}

		nextTs, err := a.findNextTimestampToMigrateV1(httpClient, shardKey)
		if err != nil {
			log.Printf("[migration_v1] Error finding next timestamp: %v", err)
			time.Sleep(time.Duration(delaySec) * time.Second)
			continue
		}

		if nextTs.IsZero() {
			log.Println("[migration_v1] No more timestamps to migrate, migration complete!")
			return
		}

		log.Printf("[migration_v1] Processing timestamp: %s", nextTs.Format("2006-01-02 15:04:05"))

		v1Rows, v3Rows, err := a.migrateTimestampWithRetryV1(httpClient, nextTs, shardKey)
		if err != nil {
			log.Printf("[migration_v1] Failed to migrate timestamp %s: %v", nextTs.Format("2006-01-02 15:04:05"), err)
			time.Sleep(time.Duration(delaySec) * time.Second)
			continue
		}

		log.Printf("[migration_v1] Successfully migrated timestamp %s (v1_rows=%d, v3_rows=%d)", nextTs.Format("2006-01-02 15:04:05"), v1Rows, v3Rows)
		time.Sleep(time.Second)
	}
}

// migrateSingleStepV1 migrates one timestamp slice from V1 schema into V3 schema.
func migrateSingleStepV1(httpClient *http.Client, khAddr, khUser, khPassword string, timestamp uint32, shardKey int32, config *MigrationConfigV1) error {
	log.Printf("[migration_v1] Starting migration for timestamp %d (time: %s), shard %d (metric %% %d = %d)",
		timestamp, time.Unix(int64(timestamp), 0).Format("2006-01-02 15:04:05"), shardKey, config.TotalShards, shardKey-1)

	shardingCondition := getConditionForSelectV1(config.TotalShards, shardKey)
	selectQuery := fmt.Sprintf(`
		SELECT stats, time,
			key1, key2, key3, key4, key5, key6, key7, key8, key9, key10, key11, key12, key13, key14, key15,
			count, min, max, sum
		FROM %s
		WHERE date = toDate(%d) AND time = toDateTime(%d)
		  AND %s`,
		config.V1TableName, timestamp, timestamp, shardingCondition,
	)

	resp, err := executeV1Query(httpClient, selectQuery, "RowBinary", config)
	if err != nil {
		return err
	}
	defer resp.Close()

	log.Printf("[migration_v1] Successfully retrieved shard data, converting and inserting...")

	return streamConvertAndInsertV1(httpClient, khAddr, khUser, khPassword, resp, config)
}

// TestMigrateSingleStepV1 is a helper for manual testing similar to v2 migrator.
func TestMigrateSingleStepV1(khAddr, khUser, khPassword string, timestamp uint32, shardKey int32, config *MigrationConfigV1) error {
	httpClient := makeHTTPClient()
	if len(config.V1Hosts) == 0 {
		config.V1Hosts = []string{khAddr}
		config.V1User = khUser
		config.V1Password = khPassword
	}
	return migrateSingleStepV1(httpClient, khAddr, khUser, khPassword, timestamp, shardKey, config)
}

// streamConvertAndInsertV1 reads V1 RowBinary data, converts to V3 RowBinary, and inserts into destination table.
func streamConvertAndInsertV1(httpClient *http.Client, khAddr, khUser, khPassword string, v1Data io.Reader, config *MigrationConfigV1) error {
	insertQuery := fmt.Sprintf(`INSERT INTO %s(
		metric,time,
		tag0,tag1,tag2,tag3,tag4,tag5,tag6,tag7,
		tag8,tag9,tag10,tag11,tag12,tag13,tag14,tag15,stag47,
		count,min,max,sum,sumsquare,
		min_host,max_host,percentiles,uniq_state
	)
	FORMAT RowBinary`, config.V3TableName)

	pipeReader, pipeWriter := io.Pipe()

	var conversionErr error
	var rowsConverted int

	go func() {
		defer pipeWriter.Close()
		rowsConverted, conversionErr = convertV1ToV3Stream(v1Data, pipeWriter)
		if conversionErr != nil {
			log.Printf("[migration_v1] Error during conversion: %v", conversionErr)
			pipeWriter.CloseWithError(conversionErr)
		}
	}()

	bodyBytes, err := io.ReadAll(pipeReader)
	if err != nil {
		return fmt.Errorf("failed to read converted body: %w", err)
	}

	log.Printf("[migration_v1] Converted %d rows, body size: %d bytes", rowsConverted, len(bodyBytes))

	req := &chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       khAddr,
		User:       khUser,
		Password:   khPassword,
		Query:      insertQuery,
		Body:       bodyBytes,
		UrlParams:  map[string]string{"input_format_values_interpret_expressions": "0"},
	}
	resp, err := req.Execute(context.Background())
	if err != nil {
		return fmt.Errorf("failed to execute insert query: %w", err)
	}
	defer resp.Close()

	log.Printf("[migration_v1] Successfully inserted converted data (%d rows)", rowsConverted)
	return nil
}

// convertV1ToV3Stream streams V1 data through conversion into V3 RowBinary format.
func convertV1ToV3Stream(input io.Reader, output io.Writer) (int, error) {
	reader := bufio.NewReaderSize(input, 8192)
	rows, err := processV1Chunk(reader, output)
	if err != nil {
		return rows, fmt.Errorf("conversion error: %w", err)
	}
	return rows, nil
}

// processV1Chunk handles a stream of V1 rows and writes converted V3 rows.
func processV1Chunk(reader *bufio.Reader, output io.Writer) (rowsProcessed int, err error) {
	rowData := make([]byte, 0, 4096)
	log.Printf("[migration_v1] Starting to process V1 rows...")

	var v1row v1Row
	for {
		parseErr := parseV1Row(reader, &v1row)
		if parseErr != nil {
			if errors.Is(parseErr, io.EOF) {
				log.Printf("[migration_v1] Reached EOF after processing %d rows", rowsProcessed)
				break
			}
			if errors.Is(parseErr, io.ErrUnexpectedEOF) {
				log.Printf("[migration_v1] Unexpected EOF after processing %d rows: %v", rowsProcessed, parseErr)
				return rowsProcessed, parseErr
			}
			log.Printf("[migration_v1] Parse error after processing %d rows: %v", rowsProcessed, parseErr)
			return rowsProcessed, fmt.Errorf("failed to parse V1 row: %w", parseErr)
		}

		rowData = rowData[:0]
		rowData = convertRowV1ToV3(rowData, &v1row)

		if _, writeErr := output.Write(rowData); writeErr != nil {
			log.Printf("[migration_v1] Write error after processing %d rows: %v", rowsProcessed, writeErr)
			return rowsProcessed, fmt.Errorf("failed to write converted row: %w", writeErr)
		}

		rowsProcessed++
	}
	return rowsProcessed, nil
}

// v1Row models the subset of columns needed from the V1 schema.
type v1Row struct {
	metric int32
	time   uint32
	keys   [16]int32
	count  float64
	min    float64
	max    float64
	sum    float64
}

// parseV1Row decodes a single V1 row from RowBinary encoding.
func parseV1Row(reader *bufio.Reader, row *v1Row) error {
	if err := binary.Read(reader, binary.LittleEndian, &row.metric); err != nil {
		return err
	}

	if err := binary.Read(reader, binary.LittleEndian, &row.time); err != nil {
		return err
	}

	row.keys[0] = 0
	for i := 1; i < 16; i++ {
		if err := binary.Read(reader, binary.LittleEndian, &row.keys[i]); err != nil {
			return err
		}
	}

	var countRaw int64
	if err := binary.Read(reader, binary.LittleEndian, &countRaw); err != nil {
		return err
	}
	row.count = float64(countRaw)

	// min/max v1 AggregateFunction flags (instead of SimpleAggregateFunction)
	if _, err := reader.ReadByte(); err != nil {
		return err
	}
	var minRaw int64
	if err := binary.Read(reader, binary.LittleEndian, &minRaw); err != nil {
		return err
	}
	row.min = float64(minRaw)

	// min/max v1 AggregateFunction flags (instead of SimpleAggregateFunction)
	if _, err := reader.ReadByte(); err != nil {
		return err
	}
	var maxRaw int64
	if err := binary.Read(reader, binary.LittleEndian, &maxRaw); err != nil {
		return err
	}
	row.max = float64(maxRaw)

	var sumRaw int64
	if err := binary.Read(reader, binary.LittleEndian, &sumRaw); err != nil {
		return err
	}
	row.sum = float64(sumRaw)

	return nil
}

// convertRowV1ToV3 re-encodes a V1 row into V3 RowBinary format.
func convertRowV1ToV3(buf []byte, row *v1Row) []byte {
	buf = rowbinary.AppendInt32(buf, row.metric)
	buf = rowbinary.AppendDateTime(buf, time.Unix(int64(row.time), 0))

	for _, tag := range row.keys {
		buf = rowbinary.AppendInt32(buf, tag)
	}

	buf = rowbinary.AppendString(buf, "")

	buf = rowbinary.AppendFloat64(buf, row.count)
	buf = rowbinary.AppendFloat64(buf, row.min)
	buf = rowbinary.AppendFloat64(buf, row.max)
	buf = rowbinary.AppendFloat64(buf, row.sum)
	buf = rowbinary.AppendFloat64(buf, 0)

	var emptyHost data_model.ArgMinMaxStringFloat32
	buf = emptyHost.MarshallAppend(buf)
	buf = emptyHost.MarshallAppend(buf)

	buf = rowbinary.AppendEmptyCentroids(buf)

	var uniq data_model.ChUnique
	buf = uniq.MarshallAppend(buf)

	return buf
}

// executeV1Query runs the provided query against configured V1 hosts with fallback.
func executeV1Query(httpClient *http.Client, query, format string, config *MigrationConfigV1) (io.ReadCloser, error) {
	if len(config.V1Hosts) == 0 {
		return nil, fmt.Errorf("no ClickHouse V1 hosts configured")
	}

	addr := config.V1Hosts[rand.Intn(len(config.V1Hosts))]
	req := &chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       addr,
		User:       config.V1User,
		Password:   config.V1Password,
		Query:      query,
		Format:     format,
	}
	resp, err := req.Execute(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to execute V1 query on host %s: %w", addr, err)
	}
	return resp, nil
}

// findNextTimestampToMigrateV1 determines next timestamp for V1 migration.
func (a *Aggregator) findNextTimestampToMigrateV1(httpClient *http.Client, shardKey int32) (time.Time, error) {
	a.configMu.RLock()
	startTs, endTs := a.configR.ParseMigrationTimeRange(a.configR.MigrationTimeRangeV1)
	a.configMu.RUnlock()

	if startTs == 0 && endTs == 0 {
		return time.Time{}, nil
	}

	log.Printf("[migration_v1] Searching for next timestamp to migrate in range: %d (start) to %d (end)", startTs, endTs)

	latestMigratedQuery := fmt.Sprintf(`
		SELECT toUnixTimestamp(ts) as ts_unix
		FROM %s
		WHERE shard_key = %d AND source = '%s' AND ended IS NOT NULL AND ts_unix <= %d AND ts_unix >= %d
		ORDER BY ts ASC
		LIMIT 1`, a.migrationConfig.StateTableName, shardKey, migrationSourceV1, startTs, endTs)

	req := &chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       a.config.KHAddr,
		User:       a.config.KHUser,
		Password:   a.config.KHPassword,
		Query:      latestMigratedQuery,
	}
	resp, err := req.Execute(context.Background())
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to find latest migrated timestamp: %w", err)
	}
	defer resp.Close()

	scanner := bufio.NewScanner(resp)
	var latestTs int64
	foundResult := false
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		if _, err := fmt.Sscanf(line, "%d", &latestTs); err != nil {
			return time.Time{}, fmt.Errorf("failed to parse latest migrated timestamp from line '%s': %w", line, err)
		}
		foundResult = true
		break
	}
	if err := scanner.Err(); err != nil {
		return time.Time{}, fmt.Errorf("error reading response: %w", err)
	}

	nextTs := time.Unix(int64(startTs), 0).Truncate(a.migrationConfigV1.StepDuration)
	if foundResult {
		latestTime := time.Unix(latestTs, 0).Truncate(a.migrationConfigV1.StepDuration)
		nextTs = latestTime.Add(-a.migrationConfigV1.StepDuration)
	}
	log.Printf("[migration_v1] Next timestamp to migrate: %s", nextTs.Format("2006-01-02 15:04:05"))

	endTime := time.Unix(int64(endTs), 0)
	if nextTs.Before(endTime) {
		log.Printf("[migration_v1] Next timestamp %s is before or at migration end time %s, migration complete",
			nextTs.Format("2006-01-02 15:04:05"), endTime.Format("2006-01-02 15:04:05"))
		return time.Time{}, nil
	}

	return nextTs, nil
}

// migrateTimestampWithRetryV1 performs V1 migration with retries.
func (a *Aggregator) migrateTimestampWithRetryV1(httpClient *http.Client, ts time.Time, shardKey int32) (v1Rows, v3Rows uint64, err error) {
	var lastErr error
	started := time.Now()
	v1Rows, countErr := a.countV1Rows(httpClient, ts, shardKey)
	if countErr != nil {
		log.Printf("[migration_v1] Warning: failed to count V1 rows: %v", countErr)
		v1Rows = 0
	}

	for attempt := 0; attempt < maxRetryAttempts; attempt++ {
		if attempt > 0 {
			log.Printf("[migration_v1] Retry attempt %d for timestamp %s", attempt+1, ts.Format("2006-01-02 15:04:05"))
			time.Sleep(retryDelay * time.Duration(math.Pow(2, float64(attempt))))
		}
		if err := a.updateMigrationState(httpClient, shardKey, ts, v1Rows, 0, uint32(attempt), started, nil, migrationSourceV1); err != nil {
			log.Printf("[migration_v1] Warning: failed to update migration state: %v", err)
		}

		err := migrateSingleStepV1(httpClient, a.config.KHAddr, a.config.KHUser, a.config.KHPassword, uint32(ts.Unix()), shardKey, a.migrationConfigV1)
		if err == nil {
			v3Rows, countErr = a.countV3Rows(httpClient, ts, shardKey)
			if countErr != nil {
				log.Printf("[migration_v1] Warning: failed to count V3 rows: %v", countErr)
				v3Rows = 0
			}
			now := time.Now()
			if err := a.updateMigrationState(httpClient, shardKey, ts, v1Rows, v3Rows, uint32(attempt), started, &now, migrationSourceV1); err != nil {
				log.Printf("[migration_v1] Warning: failed to finalize migration state: %v", err)
			}

			migrationTags := []int32{0, int32(ts.Unix()), format.TagValueIDMigrationSourceV1}
			a.sh2.AddValueCounterHost(uint32(now.Unix()), format.BuiltinMetricMetaMigrationLog, migrationTags, float64(v3Rows), 1, a.aggregatorHostTag)

			return v1Rows, v3Rows, nil
		}

		lastErr = err
		log.Printf("[migration_v1] Attempt %d failed for timestamp %s: %v", attempt+1, ts.Format("2006-01-02 15:04:05"), err)
		if a.migrationConfigV1.LogsTableName != "" {
			logQuery := fmt.Sprintf(`
		INSERT INTO %s
		(timestamp, shard_key, ts, retry, message, source)
			VALUES ('%s', %d, toDateTime(%d), %d, '%s', '%s')`, a.migrationConfig.LogsTableName, time.Now().Format("2006-01-02 15:04:05"), shardKey, ts.Unix(), attempt+1, err.Error(), migrationSourceV1)
			logReq := &chutil.ClickHouseHttpRequest{
				HttpClient: httpClient,
				Addr:       a.config.KHAddr,
				User:       a.config.KHUser,
				Password:   a.config.KHPassword,
				Query:      logQuery,
			}
			if logResp, logErr := logReq.Execute(context.Background()); logErr == nil {
				logResp.Close()
			}
		}
	}

	if err := a.updateMigrationState(httpClient, shardKey, ts, v1Rows, 0, uint32(maxRetryAttempts), started, nil, migrationSourceV1); err != nil {
		log.Printf("[migration_v1] Warning: failed to record final migration state: %v", err)
	}
	return 0, 0, fmt.Errorf("migration v1 failed after %d attempts, last error: %w", maxRetryAttempts, lastErr)
}

// countV1Rows counts rows in V1 table for specified timestamp and shard.
func (a *Aggregator) countV1Rows(httpClient *http.Client, ts time.Time, shardKey int32) (uint64, error) {
	shardingCondition := getConditionForSelectV1(a.migrationConfigV1.TotalShards, shardKey)
	countQuery := fmt.Sprintf(`
		SELECT count() as cnt
		FROM %s
		WHERE date = toDate(%d) AND time = toDateTime(%d) AND %s`,
		a.migrationConfigV1.V1TableName, ts.Unix(), ts.Unix(), shardingCondition)

	resp, err := executeV1Query(httpClient, countQuery, "", a.migrationConfigV1)
	if err != nil {
		return 0, fmt.Errorf("failed to count V1 rows: %w", err)
	}
	defer resp.Close()

	var count uint64
	if _, err := fmt.Fscanf(resp, "%d", &count); err != nil {
		return 0, fmt.Errorf("failed to parse V1 row count: %w", err)
	}
	return count, nil
}
