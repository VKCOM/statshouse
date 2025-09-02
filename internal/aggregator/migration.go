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
	"time"

	"github.com/VKCOM/statshouse/internal/chutil"
	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/vkgo/rowbinary"
)

const (
	noErrorsWindow = 5 * 60 // 5 minutes
	maxInsertTime  = 2.0    // 2 seconds
	sleepInterval  = 30 * time.Second

	// Migration coordination constants
	maxRetryAttempts = 10
	retryDelay       = 5 * time.Second
)

// MigrationConfig holds configuration for migration operations
type MigrationConfig struct {
	V2TableName    string        // Source table name (default: "statshouse_value_1h_dist")
	V3TableName    string        // Destination table name (default: "statshouse_v3_1h")
	StateTableName string        // Migration state table name (default: "migration_state")
	LogsTableName  string        // Migration logs table name (default: "migration_logs")
	StepDuration   time.Duration // Time step for migration (default: time.Hour)
	TotalShards    int           // Total number of shards (default: 16)
}

// NewDefaultMigrationConfig returns a MigrationConfig with default values
func NewDefaultMigrationConfig() *MigrationConfig {
	return &MigrationConfig{
		V2TableName:    "statshouse_value_1h_dist",
		V3TableName:    "statshouse_v3_1h",
		StateTableName: "migration_state",
		LogsTableName:  "migration_logs",
		StepDuration:   time.Hour,
		TotalShards:    16,
	}
}

// MigrationState represents the state of migration for a shard and timestamp
type MigrationState struct {
	ShardKey int32      `json:"shard_key"`
	Ts       time.Time  `json:"ts"`              // Migration timestamp
	Started  time.Time  `json:"started"`         // When migration started
	Ended    *time.Time `json:"ended,omitempty"` // When migration completed
	V2Rows   uint64     `json:"v2_rows"`         // Rows read from V2 table
	V3Rows   uint64     `json:"v3_rows"`         // Rows written to V3 table
	Retry    uint32     `json:"retry"`           // Number of retries attempted
}

// MigrationLog represents a log entry for migration operations
type MigrationLog struct {
	Timestamp time.Time `json:"timestamp"`
	ShardKey  int32     `json:"shard_key"`
	Ts        time.Time `json:"ts"`      // Migration timestamp
	Retry     uint32    `json:"retry"`   // Retry attempt number
	Message   string    `json:"message"` // Error message from ClickHouse (or other info)
}

// goMigrate runs the migration loop in a goroutine.
// It coordinates with other shards to migrate data from V2 to V3 format.
func (a *Aggregator) goMigrate(cancelCtx context.Context) {
	if a.replicaKey != 1 {
		log.Printf("[migration] Skipping migration: replica key is %d, expected 1", a.replicaKey)
		return // Only one replica should run migration per shard
	}

	shardKey := a.shardKey
	httpClient := makeHTTPClient()

	log.Printf("[migration] Starting migration routine for shard %d", shardKey)

	// Initialize migration tables
	if err := a.createMigrationTables(httpClient); err != nil {
		log.Printf("[migration] Failed to create migration tables: %v", err)
		return
	}

	log.Printf("[migration] Starting migration routine for shard %d", shardKey)

	for {
		// Check if we should continue migrating
		select {
		case <-cancelCtx.Done():
			log.Println("[migration] Exiting migration routine (context cancelled)")
			return
		default:
		}

		// Check if migration is enabled (time range must be configured)
		a.configMu.RLock()
		if a.configR.MigrationTimeRange == "" {
			a.configMu.RUnlock()
			log.Println("[migration] Migration disabled: no time range configured")
			time.Sleep(sleepInterval)
			continue
		}
		a.configMu.RUnlock()

		// Check system load before proceeding
		nowUnix := uint32(time.Now().Unix())
		if a.lastErrorTs != 0 && nowUnix >= a.lastErrorTs && nowUnix-a.lastErrorTs < noErrorsWindow {
			log.Printf("[migration] Skipping: last error was %d seconds ago", nowUnix-a.lastErrorTs)
			time.Sleep(time.Duration(noErrorsWindow-(nowUnix-a.lastErrorTs)) * time.Second)
			continue
		}
		if a.insertTimeEWMA > maxInsertTime {
			log.Printf("[migration] Skipping: EWMA insert time is too high (%.2fs)", a.insertTimeEWMA)
			time.Sleep(sleepInterval)
			continue
		}

		// Find next timestamp to migrate
		nextTs, err := a.findNextTimestampToMigrate(httpClient, shardKey)
		if err != nil {
			log.Printf("[migration] Error finding next timestamp: %v", err)
			time.Sleep(sleepInterval)
			continue
		}

		if nextTs.IsZero() {
			log.Println("[migration] No more timestamps to migrate, migration complete!")
			return
		}

		log.Printf("[migration] Processing timestamp: %s", nextTs.Format("2006-01-02 15:04:05"))

		// Perform migration for this timestamp
		v2Rows, v3Rows, err := a.migrateTimestampWithRetry(httpClient, nextTs, shardKey)
		if err != nil {
			log.Printf("[migration] Failed to migrate timestamp %s: %v", nextTs.Format("2006-01-02 15:04:05"), err)
			time.Sleep(sleepInterval)
			continue
		}

		log.Printf("[migration] Successfully migrated timestamp %s (v2_rows=%d, v3_rows=%d)", nextTs.Format("2006-01-02 15:04:05"), v2Rows, v3Rows)

		// Small delay before processing next timestamp
		time.Sleep(time.Second)
	}
}

// migrateSingleStep migrates data for a single time step from V2 to V3 format
// timestamp should be rounded to the appropriate time boundary based on config.StepDuration
func migrateSingleStep(httpClient *http.Client, khAddr, khUser, khPassword string, timestamp uint32, shardKey int32, config *MigrationConfig) error {
	log.Printf("[migration] Starting migration for timestamp %d (time: %s), shard %d (metric %% %d = %d)", timestamp, time.Unix(int64(timestamp), 0).Format("2006-01-02 15:04:05"), shardKey, config.TotalShards, shardKey-1)

	// Step 1: Select data from V2 table for the given timestamp and shard
	// Include all aggregate fields - our parsers handle ClickHouse internal format correctly
	selectQuery := fmt.Sprintf(`
		SELECT metric, time, key0, key1, key2, key3, key4, key5, key6, key7, key8, key9, key10, key11, key12, key13, key14, key15, skey,
			count, min, max, sum, sumsquare, percentiles, uniq_state, min_host, max_host
		FROM %s 
		WHERE time = toDateTime(%d)
		  AND metric %% %d = %d`,
		config.V2TableName, timestamp, config.TotalShards, shardKey-1,
	)

	// Step 2: Execute query and get response
	req := &chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       khAddr,
		User:       khUser,
		Password:   khPassword,
		Query:      selectQuery,
		Format:     "RowBinary",
	}
	resp, err := req.Execute(context.Background())
	if err != nil {
		return fmt.Errorf("failed to execute select query: %w", err)
	}
	defer resp.Close()

	log.Printf("[migration] Successfully retrieved shard data, converting and inserting...")

	// Step 3: Stream convert and insert data
	return streamConvertAndInsert(httpClient, khAddr, khUser, khPassword, resp, config)
}

// TestMigrateSingleStep is a standalone function for testing migration of a single time step
// This function can be called from external tools like test-migration
func TestMigrateSingleStep(khAddr, khUser, khPassword string, timestamp uint32, shardKey int32, config *MigrationConfig) error {
	httpClient := makeHTTPClient()
	return migrateSingleStep(httpClient, khAddr, khUser, khPassword, timestamp, shardKey, config)
}

// streamConvertAndInsert reads V2 rowbinary data, converts to V3 format, and inserts
func streamConvertAndInsert(httpClient *http.Client, khAddr, khUser, khPassword string, v2Data io.Reader, config *MigrationConfig) error {
	// Create insert query for V3 table - ClickHouse will use defaults for missing fields
	insertQuery := fmt.Sprintf(`INSERT INTO %s(
		metric,time,
		tag0,tag1,tag2,tag3,tag4,tag5,tag6,tag7,
		tag8,tag9,tag10,tag11,tag12,tag13,tag14,tag15,stag47,
		count,min,max,sum,sumsquare,
		min_host,max_host,percentiles,uniq_state
	)
	FORMAT RowBinary`, config.V3TableName)

	// Create pipe for streaming conversion
	pipeReader, pipeWriter := io.Pipe()

	// Track conversion errors
	var conversionErr error
	var rowsConverted int

	// Start goroutine to convert data
	go func() {
		defer pipeWriter.Close()
		rowsConverted, conversionErr = convertV2ToV3Stream(v2Data, pipeWriter)
		if conversionErr != nil {
			log.Printf("[migration] Error during conversion: %v", conversionErr)
			pipeWriter.CloseWithError(conversionErr)
		}
	}()

	// Execute insert with converted data
	bodyBytes, err := io.ReadAll(pipeReader)
	if err != nil {
		return fmt.Errorf("failed to read body: %w", err)
	}

	log.Printf("[migration] Converted %d rows, body size: %d bytes", rowsConverted, len(bodyBytes))

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

	log.Printf("[migration] Successfully inserted converted data (%d rows)", rowsConverted)
	return nil
}

// convertV2ToV3Stream reads V2 rowbinary format, writes V3 rowbinary format, and returns row count
func convertV2ToV3Stream(input io.Reader, output io.Writer) (int, error) {
	reader := bufio.NewReaderSize(input, 8192)
	rows, err := processV2Chunk(reader, output)
	if err != nil {
		return rows, fmt.Errorf("conversion error: %w", err)
	}
	return rows, nil
}

// processV2Chunk processes a chunk of V2 rowbinary data and converts complete rows to V3 format
func processV2Chunk(reader *bufio.Reader, output io.Writer) (rowsProcessed int, err error) {
	rowData := make([]byte, 0, 4096) // Buffer for single converted row
	log.Printf("[migration] Starting to process V2 rows...")

	for {
		// Try to parse one V2 row
		v2Row, parseErr := parseV2Row(reader)
		if parseErr != nil {
			if errors.Is(parseErr, io.EOF) {
				// End of input, we're done
				log.Printf("[migration] Reached EOF after processing %d rows", rowsProcessed)
				break
			}
			if errors.Is(parseErr, io.ErrUnexpectedEOF) {
				// Incomplete row, but we're using io.Reader so this shouldn't happen
				// unless the reader itself is incomplete
				log.Printf("[migration] Unexpected EOF after processing %d rows: %v", rowsProcessed, parseErr)
				return rowsProcessed, parseErr
			}
			log.Printf("[migration] Parse error after processing %d rows: %v", rowsProcessed, parseErr)
			return rowsProcessed, fmt.Errorf("failed to parse V2 row: %w", parseErr)
		}

		// Convert to V3 format
		rowData = rowData[:0] // Reset buffer
		rowData = convertRowV2ToV3(rowData, v2Row)

		// Write converted row
		if _, writeErr := output.Write(rowData); writeErr != nil {
			log.Printf("[migration] Write error after processing %d rows: %v", rowsProcessed, writeErr)
			return rowsProcessed, fmt.Errorf("failed to write converted row: %w", writeErr)
		}

		rowsProcessed++
	}
	return rowsProcessed, nil
}

// v2Row represents a parsed row from V2 format
type v2Row struct {
	metric    int32
	time      uint32
	keys      [16]int32
	skey      string
	count     float64
	min       float64
	max       float64
	sum       float64
	sumsquare float64
	perc      *data_model.ChDigest
	uniq      *data_model.ChUnique
	min_host  data_model.ArgMinInt32Float32
	max_host  data_model.ArgMaxInt32Float32
}

// parseV2Row parses a single V2 row from rowbinary data using io.ByteReader
func parseV2Row(reader *bufio.Reader) (*v2Row, error) {
	row := &v2Row{}

	// Parse metric (Int32)
	if err := binary.Read(reader, binary.LittleEndian, &row.metric); err != nil {
		return nil, err
	}

	// Parse time (DateTime = UInt32)
	if err := binary.Read(reader, binary.LittleEndian, &row.time); err != nil {
		return nil, err
	}

	// Parse all 16 keys (key0 through key15)
	for i := 0; i < 16; i++ {
		if err := binary.Read(reader, binary.LittleEndian, &row.keys[i]); err != nil {
			return nil, err
		}
	}

	// Parse skey (String) - LEB128 varint format
	skeyLen, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, err
	}
	// Bounds check for skey in order to avoid huge allocations in case of a bug
	if skeyLen > 4096 {
		return nil, fmt.Errorf("invalid skey length: %d", skeyLen)
	}
	skeyBytes := make([]byte, skeyLen)
	if _, err := io.ReadFull(reader, skeyBytes); err != nil {
		return nil, err
	}
	row.skey = string(skeyBytes)
	skeyBytes = nil

	// Parse simple aggregates (Float64 each)
	// count
	if err := binary.Read(reader, binary.LittleEndian, &row.count); err != nil {
		return nil, err
	}

	// min
	if err := binary.Read(reader, binary.LittleEndian, &row.min); err != nil {
		return nil, err
	}

	// max
	if err := binary.Read(reader, binary.LittleEndian, &row.max); err != nil {
		return nil, err
	}

	// sum
	if err := binary.Read(reader, binary.LittleEndian, &row.sum); err != nil {
		return nil, err
	}

	// sumsquare
	if err := binary.Read(reader, binary.LittleEndian, &row.sumsquare); err != nil {
		return nil, err
	}

	// Parse aggregate fields from ClickHouse internal format
	// percentiles
	row.perc = &data_model.ChDigest{}
	if err := row.perc.ReadFrom(reader); err != nil {
		return nil, fmt.Errorf("failed to parse percentiles: %w", err)
	}

	// uniq_state
	row.uniq = &data_model.ChUnique{}
	if err := row.uniq.ReadFrom(reader); err != nil {
		return nil, fmt.Errorf("failed to parse uniq_state: %w", err)
	}

	// min_host
	if err := row.min_host.ReadFrom(reader); err != nil {
		return nil, fmt.Errorf("failed to parse min_host: %w", err)
	}

	// max_host
	if err := row.max_host.ReadFrom(reader); err != nil {
		return nil, fmt.Errorf("failed to parse max_host: %w", err)
	}

	return row, nil
}

// convertRowV2ToV3 converts a single V2 row to V3 rowbinary format
func convertRowV2ToV3(buf []byte, row *v2Row) []byte {
	// V3 Schema order for our INSERT:
	// metric,time, tag0-tag15,stag47, count,min,max,sum,sumsquare, min_host,max_host,percentiles,uniq_state

	// Basic fields
	buf = rowbinary.AppendInt32(buf, row.metric)
	buf = rowbinary.AppendDateTime(buf, time.Unix(int64(row.time), 0))

	// tag0-tag15 from V2 keys
	for i := 0; i < 16; i++ {
		buf = rowbinary.AppendInt32(buf, row.keys[i])
	}

	// stag47 - V2 skey maps to this
	buf = rowbinary.AppendString(buf, row.skey)

	// Basic aggregates
	buf = rowbinary.AppendFloat64(buf, row.count)
	buf = rowbinary.AppendFloat64(buf, row.min)
	buf = rowbinary.AppendFloat64(buf, row.max)
	buf = rowbinary.AppendFloat64(buf, row.sum)
	buf = rowbinary.AppendFloat64(buf, row.sumsquare)

	// min_host - convert from ArgMinInt32Float32 to ArgMinStringFloat32
	minHost := data_model.ArgMinMaxStringFloat32{
		AsInt32: row.min_host.Arg,
		Val:     row.min_host.Val,
	}
	buf = minHost.MarshallAppend(buf)

	// max_host - convert from ArgMaxInt32Float32 to ArgMaxStringFloat32
	maxHost := data_model.ArgMinMaxStringFloat32{
		AsInt32: row.max_host.Arg,
		Val:     row.max_host.Val,
	}
	buf = maxHost.MarshallAppend(buf)

	// percentiles - serialize the parsed digest
	buf = row.perc.MarshallAppend(buf, 1)

	// uniq_state - serialize the parsed unique state
	buf = row.uniq.MarshallAppend(buf)

	return buf
}

// createMigrationTables creates the migration state and log tables if they don't exist
func (a *Aggregator) createMigrationTables(httpClient *http.Client) error {
	var config *MigrationConfig = a.migrationConfig
	stateTableQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			shard_key Int32,
			ts DateTime,
			started DateTime,
			ended Nullable(DateTime),
			v2_rows UInt64,
			v3_rows UInt64,
			retry UInt32
		) ENGINE = ReplacingMergeTree(retry)
		ORDER BY (shard_key, ts, started)`, config.StateTableName)
	req := &chutil.ClickHouseHttpRequest{HttpClient: httpClient, Addr: a.config.KHAddr, User: a.config.KHUser, Password: a.config.KHPassword, Query: stateTableQuery}
	resp, err := req.Execute(context.Background())
	if err != nil {
		return fmt.Errorf("failed to create migration_state table: %w", err)
	}
	resp.Close()
	logTableQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			timestamp DateTime,
			shard_key Int32,
			ts DateTime,
			retry UInt32,
			message String
		) ENGINE = MergeTree()
		ORDER BY (timestamp, shard_key, ts, retry)`, config.LogsTableName)
	req.Query = logTableQuery
	resp, err = req.Execute(context.Background())
	if err != nil {
		return fmt.Errorf("failed to create migration_logs table: %w", err)
	}
	resp.Close()
	return nil
}

// findNextTimestampToMigrate finds the next timestamp that needs migration for this shard
func (a *Aggregator) findNextTimestampToMigrate(httpClient *http.Client, shardKey int32) (time.Time, error) {
	a.configMu.RLock()
	startTs, endTs := a.configR.ParseMigrationTimeRange()
	a.configMu.RUnlock()

	// If no time range configured, migration is disabled
	if startTs == 0 && endTs == 0 {
		return time.Time{}, nil
	}

	log.Printf("[migration] Searching for next timestamp to migrate in range: %d (start) to %d (end)", startTs, endTs)

	latestMigratedQuery := fmt.Sprintf(`
		SELECT toUnixTimestamp(ts) as ts_unix
		FROM %s
		WHERE shard_key = %d AND ended IS NOT NULL AND ts_unix <= %d AND ts_unix >= %d
		ORDER BY ts ASC
		LIMIT 1`, a.migrationConfig.StateTableName, shardKey, startTs, endTs)

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

	// Read the entire response to check if there are any results
	scanner := bufio.NewScanner(resp)
	var latestTs int64
	foundResult := false
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue // Skip empty lines
		}
		if _, err := fmt.Sscanf(line, "%d", &latestTs); err != nil {
			return time.Time{}, fmt.Errorf("failed to parse latest migrated timestamp from line '%s': %w", line, err)
		}
		foundResult = true
		break // We only need the first (and should be only) result
	}
	if err := scanner.Err(); err != nil {
		return time.Time{}, fmt.Errorf("error reading response: %w", err)
	}

	nextTs := time.Unix(int64(startTs), 0).Truncate(a.migrationConfig.StepDuration)
	if foundResult {
		// Found latest migrated timestamp - start from the previous timestamp before it (backward migration)
		latestTime := time.Unix(latestTs, 0).Truncate(a.migrationConfig.StepDuration)
		nextTs = latestTime.Add(-a.migrationConfig.StepDuration)
	}
	log.Printf("[migration] Next timestamp to migrate: %s", nextTs.Format("2006-01-02 15:04:05"))

	// Check if we've reached the start of the migration range (migration complete)
	endTime := time.Unix(int64(endTs), 0)
	if nextTs.Before(endTime) {
		log.Printf("[migration] Next timestamp %s is before or at migration end time %s, migration complete",
			nextTs.Format("2006-01-02 15:04:05"), endTime.Format("2006-01-02 15:04:05"))
		return time.Time{}, nil
	}

	return nextTs, nil
}

// updateMigrationState updates the migration state for a shard and timestamp
func (a *Aggregator) updateMigrationState(httpClient *http.Client, shardKey int32, ts time.Time, v2Rows, v3Rows uint64, retryCount uint32, started time.Time, ended *time.Time) error {
	endedStr := "NULL"
	if ended != nil {
		endedStr = fmt.Sprintf("'%s'", ended.Format("2006-01-02 15:04:05"))
	}

	updateQuery := fmt.Sprintf(`
		INSERT INTO %s
		(shard_key, ts, started, ended, v2_rows, v3_rows, retry)
		VALUES (%d, toDateTime(%d), '%s', %s, %d, %d, %d)`,
		a.migrationConfig.StateTableName, shardKey, ts.Unix(), started.Format("2006-01-02 15:04:05"),
		endedStr, v2Rows, v3Rows, retryCount)

	req := &chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       a.config.KHAddr,
		User:       a.config.KHUser,
		Password:   a.config.KHPassword,
		Query:      updateQuery,
	}
	resp, err := req.Execute(context.Background())
	if err != nil {
		return fmt.Errorf("failed to update migration state: %w", err)
	}
	resp.Close()

	return nil
}

// migrateTimestampWithRetry performs migration for a timestamp with retry logic
func (a *Aggregator) migrateTimestampWithRetry(httpClient *http.Client, ts time.Time, shardKey int32) (v2Rows, v3Rows uint64, err error) {
	var lastErr error
	started := time.Now()
	v2Rows, v2CountErr := a.countV2RowsWithConfig(httpClient, ts, shardKey, a.migrationConfig)
	if v2CountErr != nil {
		log.Printf("[migration] Warning: failed to count V2 rows: %v", v2CountErr)
		v2Rows = 0
	}
	for attempt := 0; attempt < maxRetryAttempts; attempt++ {
		if attempt > 0 {
			log.Printf("[migration] Retry attempt %d for timestamp %s", attempt+1, ts.Format("2006-01-02 15:04:05"))
			time.Sleep(retryDelay * time.Duration(math.Pow(2, float64(attempt))))
		}
		a.updateMigrationState(httpClient, shardKey, ts, v2Rows, 0, uint32(attempt), started, nil)
		err := migrateSingleStep(httpClient, a.config.KHAddr, a.config.KHUser, a.config.KHPassword, uint32(ts.Unix()), shardKey, a.migrationConfig)
		if err == nil {
			v3Rows, countErr := a.countV3Rows(httpClient, ts, shardKey)
			if countErr != nil {
				log.Printf("[migration] Warning: failed to count V3 rows: %v", countErr)
				v3Rows = 0
			}
			now := time.Now()
			a.updateMigrationState(httpClient, shardKey, ts, v2Rows, v3Rows, uint32(attempt), started, &now)
			return v2Rows, v3Rows, nil
		}
		lastErr = err
		log.Printf("[migration] Attempt %d failed for timestamp %s: %v", attempt+1, ts.Format("2006-01-02 15:04:05"), err)
		logQuery := fmt.Sprintf(`
		INSERT INTO %s
		(timestamp, shard_key, ts, retry, message)
		VALUES ('%s', %d, toDateTime(%d), %d, '%s')`, a.migrationConfig.LogsTableName, time.Now().Format("2006-01-02 15:04:05"), shardKey, ts.Unix(), attempt+1, err.Error())
		logReq := &chutil.ClickHouseHttpRequest{HttpClient: httpClient, Addr: a.config.KHAddr, User: a.config.KHUser, Password: a.config.KHPassword, Query: logQuery}
		if logResp, logErr := logReq.Execute(context.Background()); logErr == nil {
			logResp.Close()
		}
	}
	a.updateMigrationState(httpClient, shardKey, ts, v2Rows, 0, uint32(maxRetryAttempts), started, nil)
	return 0, 0, fmt.Errorf("migration failed after %d attempts, last error: %w", maxRetryAttempts, lastErr)
}

// countV2RowsWithConfig counts how many rows exist in V2 table for a specific timestamp and shard with custom config
func (a *Aggregator) countV2RowsWithConfig(httpClient *http.Client, ts time.Time, shardKey int32, config *MigrationConfig) (uint64, error) {
	countQuery := fmt.Sprintf(`
		SELECT count() as cnt
		FROM %s
		WHERE time = toDateTime(%d) AND metric %% %d = %d`,
		config.V2TableName, ts.Unix(), config.TotalShards, shardKey-1)

	req := &chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       a.config.KHAddr,
		User:       a.config.KHUser,
		Password:   a.config.KHPassword,
		Query:      countQuery,
	}
	resp, err := req.Execute(context.Background())
	if err != nil {
		return 0, fmt.Errorf("failed to count V2 rows: %w", err)
	}
	defer resp.Close()

	var count uint64
	if _, err := fmt.Fscanf(resp, "%d", &count); err != nil {
		return 0, fmt.Errorf("failed to parse V2 row count: %w", err)
	}

	return count, nil
}

// countV3Rows counts how many rows exist in V3 table for a specific timestamp and shard
func (a *Aggregator) countV3Rows(httpClient *http.Client, ts time.Time, shardKey int32) (uint64, error) {
	countQuery := fmt.Sprintf(`
		SELECT count() as cnt
		FROM %s
		WHERE time = toDateTime(%d) AND metric %% %d = %d`,
		a.migrationConfig.V3TableName, ts.Unix(), a.migrationConfig.TotalShards, shardKey-1)

	req := &chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       a.config.KHAddr,
		User:       a.config.KHUser,
		Password:   a.config.KHPassword,
		Query:      countQuery,
	}
	resp, err := req.Execute(context.Background())
	if err != nil {
		return 0, fmt.Errorf("failed to count V3 rows: %w", err)
	}
	defer resp.Close()

	var count uint64
	if _, err := fmt.Fscanf(resp, "%d", &count); err != nil {
		return 0, fmt.Errorf("failed to parse V3 row count: %w", err)
	}

	return count, nil
}
