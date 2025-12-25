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

// MigrationConfigStop holds configuration for migration operations from stats_1h_agg_stop to V3.
type MigrationConfigStop struct {
	StopTableName  string        // Source table name (default: "stats_1h_agg_stop")
	V3TableName    string        // Destination table name (default: "statshouse_v3_1h")
	StateTableName string        // Migration state table name
	LogsTableName  string        // Migration logs table name
	StepDuration   time.Duration // Time step for migration (default: time.Hour)
	TotalShards    int           // Total number of shards (default: 16)
	StopHosts      []string      // List of ClickHouse stop table hosts (required)
	StopUser       string        // ClickHouse stop table user
	StopPassword   string        // ClickHouse stop table password
}

// NewDefaultMigrationConfigStop returns a MigrationConfigStop with default values.
func NewDefaultMigrationConfigStop(stopAddrs []string, stopUser string, stopPswd string) *MigrationConfigStop {
	return &MigrationConfigStop{
		StopTableName:  "stats_1h_agg_stop_dist",
		V3TableName:    "statshouse_v3_1h",
		StateTableName: "statshouse_migration_state",
		LogsTableName:  "statshouse_migration_logs",
		StepDuration:   time.Hour,
		TotalShards:    16,
		StopHosts:      stopAddrs,
		StopUser:       stopUser,
		StopPassword:   stopPswd,
	}
}

// getConditionForSelectStop returns the sharding condition for stats_1h_agg_stop table.
// Only uses key1 and key2 since stop table doesn't have key3-key15.
func getConditionForSelectStop(totalShards int, shardKey int32) string {
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
		// Only use key1 and key2 for stop table (tagID 1 and 2)
		if tagID != 1 && tagID != 2 {
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

// goMigrateStop runs migration loop for stats_1h_agg_stop data source.
func (a *Aggregator) goMigrateStop(cancelCtx context.Context) {
	if a.replicaKey != 1 {
		log.Printf("[migration_stop] Skipping migration: replica key is %d, expected 1", a.replicaKey)
		return
	}
	if a.shardKey > int32(a.migrationConfigStop.TotalShards) {
		log.Printf("[migration_stop] Skipping migration: shard key is %d, expected less than %d", a.shardKey, a.migrationConfigStop.TotalShards)
		return
	}

	shardKey := a.shardKey
	httpClient := makeHTTPClient()

	log.Printf("[migration_stop] Starting migration routine for shard %d", shardKey)
	for {
		select {
		case <-cancelCtx.Done():
			log.Println("[migration_stop] Exiting migration routine (context cancelled)")
			return
		default:
		}

		a.configMu.RLock()
		delaySec := a.configR.MigrationDelaySec
		if a.configR.MigrationTimeRangeStop == "" {
			a.configMu.RUnlock()
			log.Println("[migration_stop] Migration disabled: no time range configured")
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
			log.Printf("[migration_stop] Skipping: last error was %d seconds ago", nowUnix-lastErrorTs)
			time.Sleep(time.Duration(noErrorsWindow-(nowUnix-lastErrorTs)) * time.Second)
			continue
		}
		if insertTimeEWMA > maxInsertTime {
			log.Printf("[migration_stop] Skipping: EWMA insert time is too high (%.2fs)", insertTimeEWMA)
			time.Sleep(time.Duration(delaySec) * time.Second)
			continue
		}

		nextTs, err := a.findNextTimestampToMigrateStop(httpClient, shardKey)
		if err != nil {
			log.Printf("[migration_stop] Error finding next timestamp: %v", err)
			time.Sleep(time.Duration(delaySec) * time.Second)
			continue
		}

		if nextTs.IsZero() {
			log.Println("[migration_stop] No more timestamps to migrate, migration complete!")
			return
		}

		log.Printf("[migration_stop] Processing timestamp: %s", nextTs.Format("2006-01-02 15:04:05"))

		stopRows, v3Rows, err := a.migrateTimestampWithRetryStop(httpClient, nextTs, shardKey)
		if err != nil {
			log.Printf("[migration_stop] Failed to migrate timestamp %s: %v", nextTs.Format("2006-01-02 15:04:05"), err)
			time.Sleep(time.Duration(delaySec) * time.Second)
			continue
		}

		log.Printf("[migration_stop] Successfully migrated timestamp %s (stop_rows=%d, v3_rows=%d)", nextTs.Format("2006-01-02 15:04:05"), stopRows, v3Rows)
		time.Sleep(time.Second)
	}
}

// migrateSingleStepStop migrates one timestamp slice from stats_1h_agg_stop into V3 schema.
func migrateSingleStepStop(httpClient *http.Client, khAddr, khUser, khPassword string, timestamp uint32, shardKey int32, config *MigrationConfigStop) error {
	log.Printf("[migration_stop] Starting migration for timestamp %d (time: %s), shard %d (metric %% %d = %d)",
		timestamp, time.Unix(int64(timestamp), 0).Format("2006-01-02 15:04:05"), shardKey, config.TotalShards, shardKey-1)

	shardingCondition := getConditionForSelectStop(config.TotalShards, shardKey)
	selectQuery := fmt.Sprintf(`
		SELECT stats, time, key1, key2, skey, count
		FROM %s
		WHERE date = toDate(%d) AND time = toDateTime(%d)
		  AND %s`,
		config.StopTableName, timestamp, timestamp, shardingCondition,
	)

	resp, err := executeStopQuery(httpClient, selectQuery, "RowBinary", config)
	if err != nil {
		return err
	}
	defer resp.Close()

	log.Printf("[migration_stop] Successfully retrieved shard data, converting and inserting...")

	return streamConvertAndInsertStop(httpClient, khAddr, khUser, khPassword, resp, config)
}

// TestMigrateSingleStepStop is a helper for manual testing.
func TestMigrateSingleStepStop(khAddr, khUser, khPassword string, timestamp uint32, shardKey int32, config *MigrationConfigStop) error {
	httpClient := makeHTTPClient()
	if len(config.StopHosts) == 0 {
		config.StopHosts = []string{khAddr}
		config.StopUser = khUser
		config.StopPassword = khPassword
	}
	return migrateSingleStepStop(httpClient, khAddr, khUser, khPassword, timestamp, shardKey, config)
}

// streamConvertAndInsertStop reads stop table RowBinary data, converts to V3 RowBinary, and inserts.
func streamConvertAndInsertStop(httpClient *http.Client, khAddr, khUser, khPassword string, stopData io.Reader, config *MigrationConfigStop) error {
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
		rowsConverted, conversionErr = convertStopToV3Stream(stopData, pipeWriter)
		if conversionErr != nil {
			log.Printf("[migration_stop] Error during conversion: %v", conversionErr)
			pipeWriter.CloseWithError(conversionErr)
		}
	}()

	bodyBytes, err := io.ReadAll(pipeReader)
	if err != nil {
		return fmt.Errorf("failed to read converted body: %w", err)
	}

	log.Printf("[migration_stop] Converted %d rows, body size: %d bytes", rowsConverted, len(bodyBytes))

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

	log.Printf("[migration_stop] Successfully inserted converted data (%d rows)", rowsConverted)
	return nil
}

// convertStopToV3Stream streams stop table data through conversion into V3 RowBinary format.
func convertStopToV3Stream(input io.Reader, output io.Writer) (int, error) {
	reader := bufio.NewReaderSize(input, 8192)
	rows, err := processStopChunk(reader, output)
	if err != nil {
		return rows, fmt.Errorf("conversion error: %w", err)
	}
	return rows, nil
}

// processStopChunk handles a stream of stop table rows and writes converted V3 rows.
func processStopChunk(reader *bufio.Reader, output io.Writer) (rowsProcessed int, err error) {
	rowData := make([]byte, 0, 4096)
	log.Printf("[migration_stop] Starting to process stop rows...")

	var stopRow stopRow
	for {
		parseErr := parseStopRow(reader, &stopRow)
		if parseErr != nil {
			if errors.Is(parseErr, io.EOF) {
				log.Printf("[migration_stop] Reached EOF after processing %d rows", rowsProcessed)
				break
			}
			if errors.Is(parseErr, io.ErrUnexpectedEOF) {
				log.Printf("[migration_stop] Unexpected EOF after processing %d rows: %v", rowsProcessed, parseErr)
				return rowsProcessed, parseErr
			}
			log.Printf("[migration_stop] Parse error after processing %d rows: %v", rowsProcessed, parseErr)
			return rowsProcessed, fmt.Errorf("failed to parse stop row: %w", parseErr)
		}

		rowData = rowData[:0]
		rowData = convertStopRowToV3(rowData, &stopRow)

		if _, writeErr := output.Write(rowData); writeErr != nil {
			log.Printf("[migration_stop] Write error after processing %d rows: %v", rowsProcessed, writeErr)
			return rowsProcessed, fmt.Errorf("failed to write converted row: %w", writeErr)
		}

		rowsProcessed++
	}
	return rowsProcessed, nil
}

// stopRow models the columns from stats_1h_agg_stop table.
type stopRow struct {
	metric int32
	time   uint32
	key1   int32
	key2   int32
	skey   string
	count  float64
}

// parseStopRow decodes a single stop table row from RowBinary encoding.
func parseStopRow(reader *bufio.Reader, row *stopRow) error {
	// Parse stats (Int32) - this is the metric ID
	if err := binary.Read(reader, binary.LittleEndian, &row.metric); err != nil {
		return err
	}

	// Parse time (DateTime = UInt32)
	if err := binary.Read(reader, binary.LittleEndian, &row.time); err != nil {
		return err
	}

	// Parse key1 (Int32)
	if err := binary.Read(reader, binary.LittleEndian, &row.key1); err != nil {
		return err
	}

	// Parse key2 (Int32)
	if err := binary.Read(reader, binary.LittleEndian, &row.key2); err != nil {
		return err
	}

	// Parse skey (String) - LEB128 varint format
	skeyLen, err := binary.ReadUvarint(reader)
	if err != nil {
		return err
	}
	skeyBytes := make([]byte, skeyLen)
	if _, err := io.ReadFull(reader, skeyBytes); err != nil {
		return err
	}
	// Validate and sanitize string tag value to avoid validation violations
	row.skey = string(format.ForceValidStringValue(string(skeyBytes)))
	skeyBytes = nil

	// Parse count (AggregateFunction(sum, Int64)) - read as int64, convert to float64
	var countRaw int64
	if err := binary.Read(reader, binary.LittleEndian, &countRaw); err != nil {
		return err
	}
	row.count = float64(countRaw)

	return nil
}

// convertStopRowToV3 re-encodes a stop table row into V3 RowBinary format.
// Maps: key1->tag1, key2->tag2, skey->stag47, count->count
// All other tags are zeroed, other aggregates are empty/zero.
func convertStopRowToV3(buf []byte, row *stopRow) []byte {
	// V3 Schema order for our INSERT:
	// metric,time, tag0-tag15,stag47, count,min,max,sum,sumsquare, min_host,max_host,percentiles,uniq_state

	// Basic fields
	buf = rowbinary.AppendInt32(buf, row.metric)
	buf = rowbinary.AppendDateTime(buf, time.Unix(int64(row.time), 0))

	// tag0 = 0 (not present in stop table)
	buf = rowbinary.AppendInt32(buf, 0)
	// tag1 = key1
	buf = rowbinary.AppendInt32(buf, row.key1)
	// tag2 = key2
	buf = rowbinary.AppendInt32(buf, row.key2)
	// tag3-tag15 = 0 (not present in stop table)
	for i := 3; i < 16; i++ {
		buf = rowbinary.AppendInt32(buf, 0)
	}

	// stag47 = skey
	buf = rowbinary.AppendString(buf, row.skey)

	// count from source
	buf = rowbinary.AppendFloat64(buf, row.count)
	// min, max, sum, sumsquare = 0 (not present in stop table)
	buf = rowbinary.AppendFloat64(buf, 0) // min
	buf = rowbinary.AppendFloat64(buf, 0) // max
	buf = rowbinary.AppendFloat64(buf, 0) // sum
	buf = rowbinary.AppendFloat64(buf, 0) // sumsquare

	// min_host, max_host = empty
	var emptyHost data_model.ArgMinMaxStringFloat32
	buf = emptyHost.MarshallAppend(buf)
	buf = emptyHost.MarshallAppend(buf)

	// percentiles = empty
	buf = rowbinary.AppendEmptyCentroids(buf)

	// uniq_state = empty
	var uniq data_model.ChUnique
	buf = uniq.MarshallAppend(buf)

	return buf
}

// executeStopQuery runs the provided query against configured stop table hosts.
func executeStopQuery(httpClient *http.Client, query, format string, config *MigrationConfigStop) (io.ReadCloser, error) {
	if len(config.StopHosts) == 0 {
		return nil, fmt.Errorf("no ClickHouse stop table hosts configured")
	}

	addr := config.StopHosts[rand.Intn(len(config.StopHosts))]
	req := &chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       addr,
		User:       config.StopUser,
		Password:   config.StopPassword,
		Query:      query,
		Format:     format,
	}
	resp, err := req.Execute(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to execute stop query on host %s: %w", addr, err)
	}
	return resp, nil
}

// findNextTimestampToMigrateStop determines next timestamp for stop table migration.
func (a *Aggregator) findNextTimestampToMigrateStop(httpClient *http.Client, shardKey int32) (time.Time, error) {
	if a.migrationConfigStop == nil {
		return time.Time{}, fmt.Errorf("migration config stop not configured")
	}

	a.configMu.RLock()
	startTs, endTs := a.configR.ParseMigrationTimeRange(a.configR.MigrationTimeRangeStop)
	a.configMu.RUnlock()

	if startTs == 0 && endTs == 0 {
		return time.Time{}, nil
	}

	log.Printf("[migration_stop] Searching for next timestamp to migrate in range: %d (start) to %d (end)", startTs, endTs)

	latestMigratedQuery := fmt.Sprintf(`
		SELECT toUnixTimestamp(ts) as ts_unix
		FROM %s
		WHERE shard_key = %d AND source = '%s' AND ended IS NOT NULL AND ts_unix <= %d AND ts_unix >= %d
		ORDER BY ts ASC
		LIMIT 1`, a.migrationConfig.StateTableName, shardKey, migrationSourceStop, startTs, endTs)

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

	nextTs := time.Unix(int64(startTs), 0).Truncate(a.migrationConfigStop.StepDuration)
	if foundResult {
		latestTime := time.Unix(latestTs, 0).Truncate(a.migrationConfigStop.StepDuration)
		nextTs = latestTime.Add(-a.migrationConfigStop.StepDuration)
	}
	log.Printf("[migration_stop] Next timestamp to migrate: %s", nextTs.Format("2006-01-02 15:04:05"))

	endTime := time.Unix(int64(endTs), 0)
	if nextTs.Before(endTime) {
		log.Printf("[migration_stop] Next timestamp %s is before or at migration end time %s, migration complete",
			nextTs.Format("2006-01-02 15:04:05"), endTime.Format("2006-01-02 15:04:05"))
		return time.Time{}, nil
	}

	return nextTs, nil
}

// migrateTimestampWithRetryStop performs stop table migration with retries.
func (a *Aggregator) migrateTimestampWithRetryStop(httpClient *http.Client, ts time.Time, shardKey int32) (stopRows, v3Rows uint64, err error) {
	var lastErr error
	started := time.Now()
	stopRows, countErr := a.countStopRows(httpClient, ts, shardKey)
	if countErr != nil {
		log.Printf("[migration_stop] Warning: failed to count stop rows: %v", countErr)
		stopRows = 0
	}

	for attempt := 0; attempt < maxRetryAttempts; attempt++ {
		if attempt > 0 {
			log.Printf("[migration_stop] Retry attempt %d for timestamp %s", attempt+1, ts.Format("2006-01-02 15:04:05"))
			time.Sleep(retryDelay * time.Duration(math.Pow(2, float64(attempt))))
		}
		if err := a.updateMigrationState(httpClient, shardKey, ts, stopRows, 0, uint32(attempt), started, nil, migrationSourceStop); err != nil {
			log.Printf("[migration_stop] Warning: failed to update migration state: %v", err)
		}

		err := migrateSingleStepStop(httpClient, a.config.KHAddr, a.config.KHUser, a.config.KHPassword, uint32(ts.Unix()), shardKey, a.migrationConfigStop)
		if err == nil {
			v3Rows, countErr = a.countV3Rows(httpClient, ts, shardKey)
			if countErr != nil {
				log.Printf("[migration_stop] Warning: failed to count V3 rows: %v", countErr)
				v3Rows = 0
			}
			now := time.Now()
			if err := a.updateMigrationState(httpClient, shardKey, ts, stopRows, v3Rows, uint32(attempt), started, &now, migrationSourceStop); err != nil {
				log.Printf("[migration_stop] Warning: failed to finalize migration state: %v", err)
			}

			migrationTags := []int32{0, int32(ts.Unix()), format.TagValueIDMigrationSourceStop}
			a.sh2.AddValueCounterHost(uint32(now.Unix()), format.BuiltinMetricMetaMigrationLog, migrationTags, float64(v3Rows), 1, a.aggregatorHostTag)

			return stopRows, v3Rows, nil
		}

		lastErr = err
		log.Printf("[migration_stop] Attempt %d failed for timestamp %s: %v", attempt+1, ts.Format("2006-01-02 15:04:05"), err)
		if a.migrationConfigStop.LogsTableName != "" {
			logQuery := fmt.Sprintf(`
		INSERT INTO %s
		(timestamp, shard_key, ts, retry, message, source)
			VALUES ('%s', %d, toDateTime(%d), %d, '%s', '%s')`, a.migrationConfig.LogsTableName, time.Now().Format("2006-01-02 15:04:05"), shardKey, ts.Unix(), attempt+1, err.Error(), migrationSourceStop)
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

	if err := a.updateMigrationState(httpClient, shardKey, ts, stopRows, 0, uint32(maxRetryAttempts), started, nil, migrationSourceStop); err != nil {
		log.Printf("[migration_stop] Warning: failed to record final migration state: %v", err)
	}
	return 0, 0, fmt.Errorf("migration stop failed after %d attempts, last error: %w", maxRetryAttempts, lastErr)
}

// countStopRows counts rows in stop table for specified timestamp and shard.
func (a *Aggregator) countStopRows(httpClient *http.Client, ts time.Time, shardKey int32) (uint64, error) {
	shardingCondition := getConditionForSelectStop(a.migrationConfigStop.TotalShards, shardKey)
	countQuery := fmt.Sprintf(`
		SELECT count() as cnt
		FROM %s
		WHERE time = toDateTime(%d) AND %s`,
		a.migrationConfigStop.StopTableName, ts.Unix(), shardingCondition)

	resp, err := executeStopQuery(httpClient, countQuery, "", a.migrationConfigStop)
	if err != nil {
		return 0, fmt.Errorf("failed to count stop rows: %w", err)
	}
	defer resp.Close()

	var count uint64
	if _, err := fmt.Fscanf(resp, "%d", &count); err != nil {
		return 0, fmt.Errorf("failed to parse stop row count: %w", err)
	}
	return count, nil
}
