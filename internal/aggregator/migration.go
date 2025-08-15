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
	"net/http"
	"time"

	"github.com/VKCOM/statshouse/internal/chutil"
	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/vkgo/rowbinary"
)

const (
	noErrorsWindow   = 5 * 60 // 5 minutes
	maxInsertTime    = 2.0    // 2 seconds
	sleepInterval    = 30 * time.Second
	maxAggregateSize = 10 * 1024 * 1024 // 10MB

	// Migration coordination constants
	maxRetryAttempts    = 3
	retryDelay          = 5 * time.Second
	coordinationTimeout = 30 * time.Second
	migrationBatchHours = 1 // Process 1 hour at a time
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

// MigrationState represents the state of migration for a shard and time step
type MigrationState struct {
	ShardKey     int32      `json:"shard_key"`
	CurrentStep  time.Time  `json:"current_step"` // Renamed from CurrentHour
	Status       string     `json:"status"`       // 'in_progress', 'completed', 'failed'
	StartedAt    time.Time  `json:"started_at"`
	CompletedAt  *time.Time `json:"completed_at,omitempty"`
	RowsMigrated uint64     `json:"rows_migrated"`
	ErrorMessage string     `json:"error_message"`
	RetryCount   uint32     `json:"retry_count"`
}

// MigrationLog represents a log entry for migration operations
type MigrationLog struct {
	Timestamp time.Time `json:"timestamp"`
	ShardKey  int32     `json:"shard_key"`
	Step      time.Time `json:"step"`  // Renamed from Hour
	Level     string    `json:"level"` // 'INFO', 'WARN', 'ERROR'
	Message   string    `json:"message"`
	Details   string    `json:"details"`
}

// goMigrate runs the migration loop in a goroutine.
// It coordinates with other shards to migrate data from V2 to V3 format.
func (a *Aggregator) goMigrate(cancelCtx context.Context) {
	if a.replicaKey != 1 {
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

	a.logMigrationEvent(httpClient, time.Time{}, "INFO", "Migration routine started", fmt.Sprintf("shard=%d", shardKey))

	for {
		// Check if we should continue migrating
		select {
		case <-cancelCtx.Done():
			log.Println("[migration] Exiting migration routine (context cancelled)")
			a.logMigrationEvent(httpClient, time.Time{}, "INFO", "Migration routine stopped", "context cancelled")
			return
		default:
		}

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

		// Find next step to migrate
		nextStep, err := a.findNextStepToMigrate(httpClient, shardKey)
		if err != nil {
			log.Printf("[migration] Error finding next step: %v", err)
			a.logMigrationEvent(httpClient, time.Time{}, "ERROR", "Failed to find next step", err.Error())
			time.Sleep(sleepInterval)
			continue
		}

		if nextStep.IsZero() {
			log.Println("[migration] No more steps to migrate, migration complete!")
			a.logMigrationEvent(httpClient, time.Time{}, "INFO", "Migration completed", "no more steps to process")
			return
		}

		log.Printf("[migration] Processing step: %s", nextStep.Format("2006-01-02 15:04:05"))

		// Update state to in_progress
		if err := a.updateMigrationState(httpClient, shardKey, nextStep, "in_progress", "", 0, 0); err != nil {
			log.Printf("[migration] Failed to update state to in_progress: %v", err)
			time.Sleep(sleepInterval)
			continue
		}

		// Perform migration for this step
		rowsMigrated, err := a.migrateStepWithRetry(httpClient, nextStep, shardKey)
		if err != nil {
			log.Printf("[migration] Failed to migrate step %s: %v", nextStep.Format("2006-01-02 15:04:05"), err)
			a.logMigrationEvent(httpClient, nextStep, "ERROR", "Migration failed", err.Error())
			a.updateMigrationState(httpClient, shardKey, nextStep, "failed", err.Error(), 0, rowsMigrated)
			time.Sleep(sleepInterval)
			continue
		}

		// Update state to completed
		if err := a.updateMigrationState(httpClient, shardKey, nextStep, "completed", "", 0, rowsMigrated); err != nil {
			log.Printf("[migration] Failed to update state to completed: %v", err)
		}

		log.Printf("[migration] Successfully migrated step %s (%d rows)", nextStep.Format("2006-01-02 15:04:05"), rowsMigrated)
		a.logMigrationEvent(httpClient, nextStep, "INFO", "Step migration completed", fmt.Sprintf("rows_migrated=%d", rowsMigrated))

		// Wait for all shards to complete this step before moving to next
		if err := a.waitForAllShardsToComplete(httpClient, nextStep); err != nil {
			log.Printf("[migration] Error waiting for shards: %v", err)
			a.logMigrationEvent(httpClient, nextStep, "WARN", "Coordination warning", err.Error())
		}

		// Small delay before processing next step
		time.Sleep(5 * time.Second)
	}
}

// TestMigrateSingleStep is a standalone function for testing migration with local ClickHouse
// Example usage: TestMigrateSingleStep("localhost:8123", "", "", 1672531200, 1, config)
func TestMigrateSingleStep(khAddr, khUser, khPassword string, timestamp uint32, shardKey int32, config *MigrationConfig) error {
	if config == nil {
		config = NewDefaultMigrationConfig()
	}
	httpClient := makeHTTPClient()
	return migrateSingleStep(httpClient, khAddr, khUser, khPassword, timestamp, shardKey, config)
}

// TestMigrateSingleHour is a legacy function for backward compatibility
func TestMigrateSingleHour(khAddr, khUser, khPassword string, timestamp uint32, shardKey int32) error {
	return TestMigrateSingleStep(khAddr, khUser, khPassword, timestamp, shardKey, nil)
}

// CreateTestDataV2 creates some test data in the V2 table for testing migration
func CreateTestDataV2(khAddr, khUser, khPassword string, timestamp uint32) error {
	httpClient := makeHTTPClient()

	// Create test data with actual aggregate values
	insertQuery := fmt.Sprintf(`
		INSERT INTO statshouse_value_1h_dist (
			metric, time, key0, key1, key2, key3, key4, key5, key6, key7, key8, key9, key10, key11, key12, key13, key14, key15, skey,
			count, min, max, sum, sumsquare, percentiles, uniq_state, min_host, max_host
		)
		SELECT 
			1 as metric,
			toDateTime(%d) as time,
			123 as key0,
			0 as key1, 0 as key2, 0 as key3, 0 as key4, 0 as key5, 0 as key6, 0 as key7,
			0 as key8, 0 as key9, 0 as key10, 0 as key11, 0 as key12, 0 as key13, 0 as key14, 0 as key15,
			'test_skey' as skey,
			10.0 as count,
			1.5 as min,
			9.8 as max,
			55.5 as sum,
			123.45 as sumsquare,
			quantilesTDigestState(0.5)(toFloat32(2.5)) as percentiles,
			uniqState(toInt64(100)) as uniq_state,
			argMinState(toInt32(42), toFloat32(1.5)) as min_host,
			argMaxState(toInt32(99), toFloat32(9.8)) as max_host
		UNION ALL
		SELECT 
			17 as metric,
			toDateTime(%d) as time,
			456 as key0,
			1 as key1, 2 as key2, 0 as key3, 0 as key4, 0 as key5, 0 as key6, 0 as key7,
			0 as key8, 0 as key9, 0 as key10, 0 as key11, 0 as key12, 0 as key13, 0 as key14, 0 as key15,
			'another_test' as skey,
			5.0 as count,
			0.1 as min,
			4.9 as max,
			12.5 as sum,
			31.25 as sumsquare,
			quantilesTDigestState(0.5)(toFloat32(2.0)) as percentiles,
			uniqState(toInt64(200)) as uniq_state,
			argMinState(toInt32(10), toFloat32(0.1)) as min_host,
			argMaxState(toInt32(20), toFloat32(4.9)) as max_host`,
		timestamp, timestamp)

	req := &chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       khAddr,
		User:       khUser,
		Password:   khPassword,
		Query:      insertQuery,
	}
	resp, err := req.Execute(context.Background())
	if err != nil {
		return fmt.Errorf("failed to execute insert query: %w", err)
	}
	defer resp.Close()

	log.Printf("[migration] Created test data for timestamp %d", timestamp)
	return nil
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

	log.Printf("[migration] Executing V2 select query: %s", selectQuery)

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

// migrateSingleHour is a legacy function for backward compatibility
func migrateSingleHour(httpClient *http.Client, khAddr, khUser, khPassword string, timestamp uint32, shardKey int32) error {
	config := NewDefaultMigrationConfig()
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

	log.Printf("[migration] Executing V3 insert query: %s", insertQuery)

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

	log.Printf("[migration] Converted %d rows", rows)
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

		// Log details of first few rows for debugging
		if rowsProcessed < 3 {
			log.Printf("[migration] Processing V2 row %d: metric=%d, time=%d, skey=%s, count=%.2f",
				rowsProcessed+1, v2Row.metric, v2Row.time, v2Row.skey, v2Row.count)
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

		// Log progress every 100 rows
		if rowsProcessed%100 == 0 {
			log.Printf("[migration] Processed %d rows...", rowsProcessed)
		}
	}

	log.Printf("[migration] Finished processing, total rows: %d", rowsProcessed)
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
	config := NewDefaultMigrationConfig() // Use default for existing aggregator
	return a.createMigrationTablesWithConfig(httpClient, config)
}

// createMigrationTablesWithConfig creates the migration state and log tables if they don't exist
func (a *Aggregator) createMigrationTablesWithConfig(httpClient *http.Client, config *MigrationConfig) error {
	// Create migration_state table
	stateTableQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			shard_key Int32,
			current_step DateTime,
			status String,
			started_at DateTime,
			completed_at Nullable(DateTime),
			rows_migrated UInt64,
			error_message String,
			retry_count UInt32
		) ENGINE = ReplacingMergeTree(started_at)
		ORDER BY (shard_key, current_step)`, config.StateTableName)

	req := &chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       a.config.KHAddr,
		User:       a.config.KHUser,
		Password:   a.config.KHPassword,
		Query:      stateTableQuery,
	}
	resp, err := req.Execute(context.Background())
	if err != nil {
		return fmt.Errorf("failed to create migration_state table: %w", err)
	}
	resp.Close()

	// Create migration_logs table
	logTableQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			timestamp DateTime,
			shard_key Int32,
			step DateTime,
			level String,
			message String,
			details String
		) ENGINE = MergeTree()
		ORDER BY (timestamp, shard_key)`, config.LogsTableName)

	req.Query = logTableQuery
	resp, err = req.Execute(context.Background())
	if err != nil {
		return fmt.Errorf("failed to create migration_logs table: %w", err)
	}
	resp.Close()

	return nil
}

// findNextStepToMigrate finds the next time step that needs migration
func (a *Aggregator) findNextStepToMigrate(httpClient *http.Client, shardKey int32) (time.Time, error) {
	config := NewDefaultMigrationConfig() // Use default for existing aggregator
	return a.findNextStepToMigrateWithConfig(httpClient, shardKey, config)
}

// findNextStepToMigrateWithConfig finds the next time step that needs migration with custom config
func (a *Aggregator) findNextStepToMigrateWithConfig(httpClient *http.Client, shardKey int32, config *MigrationConfig) (time.Time, error) {
	// Strategy: Look for the earliest step with data in V2 table that hasn't been migrated yet
	// Start from current time and work backwards

	currentStep := time.Now().Truncate(config.StepDuration)

	// TODO: For optimization, we could query completed steps first to skip checking them:
	// completedStepsQuery := `SELECT DISTINCT current_step FROM migration_state WHERE status = 'completed' AND shard_key = ? ORDER BY current_step DESC`

	// For now, let's use a simple approach: check recent time steps for unmigrated data
	maxChecks := 48 // Default to checking 48 time steps
	if config.StepDuration < time.Hour {
		maxChecks = int(time.Hour/config.StepDuration) * 48 // Scale based on step duration
	}

	for i := 0; i < maxChecks; i++ {
		checkStep := currentStep.Add(-time.Duration(i) * config.StepDuration)

		// Check if this step has data in V2 table
		hasDataQuery := fmt.Sprintf(`
			SELECT count() as cnt
			FROM %s 
			WHERE time = toDateTime(%d) AND metric %% %d = %d
			LIMIT 1`, config.V2TableName, checkStep.Unix(), config.TotalShards, shardKey-1)

		req := &chutil.ClickHouseHttpRequest{
			HttpClient: httpClient,
			Addr:       a.config.KHAddr,
			User:       a.config.KHUser,
			Password:   a.config.KHPassword,
			Query:      hasDataQuery,
		}
		resp, err := req.Execute(context.Background())
		if err != nil {
			return time.Time{}, fmt.Errorf("failed to check for V2 data: %w", err)
		}

		// Read count result
		var count uint64
		if _, err := fmt.Fscanf(resp, "%d", &count); err == nil && count > 0 {
			// This step has data, check if it's already migrated
			migratedQuery := fmt.Sprintf(`
				SELECT count() as cnt
				FROM %s 
				WHERE current_step = toDateTime(%d) 
				AND shard_key = %d 
				AND status = 'completed'`, config.StateTableName, checkStep.Unix(), shardKey)

			req.Query = migratedQuery
			resp2, err := req.Execute(context.Background())
			if err != nil {
				resp.Close()
				return time.Time{}, fmt.Errorf("failed to check migration status: %w", err)
			}

			var migratedCount uint64
			if _, err := fmt.Fscanf(resp2, "%d", &migratedCount); err == nil && migratedCount == 0 {
				// Step has data but not migrated yet
				resp.Close()
				resp2.Close()
				return checkStep, nil
			}
			resp2.Close()
		}
		resp.Close()
	}

	// No unmigrated steps found
	return time.Time{}, nil
}

// updateMigrationState updates the migration state for a shard and time step
func (a *Aggregator) updateMigrationState(httpClient *http.Client, shardKey int32, step time.Time, status, errorMsg string, retryCount uint32, rowsMigrated uint64) error {
	config := NewDefaultMigrationConfig() // Use default for existing aggregator
	return a.updateMigrationStateWithConfig(httpClient, shardKey, step, status, errorMsg, retryCount, rowsMigrated, config)
}

// updateMigrationStateWithConfig updates the migration state for a shard and time step with custom config
func (a *Aggregator) updateMigrationStateWithConfig(httpClient *http.Client, shardKey int32, step time.Time, status, errorMsg string, retryCount uint32, rowsMigrated uint64, config *MigrationConfig) error {
	var completedAt string
	if status == "completed" {
		completedAt = fmt.Sprintf("'%s'", time.Now().Format("2006-01-02 15:04:05"))
	} else {
		completedAt = "NULL"
	}

	updateQuery := fmt.Sprintf(`
		INSERT INTO %s 
		(shard_key, current_step, status, started_at, completed_at, rows_migrated, error_message, retry_count)
		VALUES (%d, toDateTime(%d), '%s', '%s', %s, %d, '%s', %d)`,
		config.StateTableName, shardKey, step.Unix(), status, time.Now().Format("2006-01-02 15:04:05"),
		completedAt, rowsMigrated, errorMsg, retryCount)

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

// logMigrationEvent logs an event to the migration_logs table
func (a *Aggregator) logMigrationEvent(httpClient *http.Client, step time.Time, level, message, details string) {
	config := NewDefaultMigrationConfig() // Use default for existing aggregator
	a.logMigrationEventWithConfig(httpClient, step, level, message, details, config)
}

// logMigrationEventWithConfig logs an event to the migration_logs table with custom config
func (a *Aggregator) logMigrationEventWithConfig(httpClient *http.Client, step time.Time, level, message, details string, config *MigrationConfig) {
	stepStr := "toDateTime(0)"
	if !step.IsZero() {
		stepStr = fmt.Sprintf("toDateTime(%d)", step.Unix())
	}

	logQuery := fmt.Sprintf(`
		INSERT INTO %s 
		(timestamp, shard_key, step, level, message, details)
		VALUES ('%s', %d, %s, '%s', '%s', '%s')`,
		config.LogsTableName, time.Now().Format("2006-01-02 15:04:05"), a.shardKey, stepStr, level, message, details)

	req := &chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       a.config.KHAddr,
		User:       a.config.KHUser,
		Password:   a.config.KHPassword,
		Query:      logQuery,
	}
	resp, err := req.Execute(context.Background())
	if err != nil {
		log.Printf("[migration] Failed to log event: %v", err)
		return
	}
	resp.Close()
}

// migrateStepWithRetry performs migration for a time step with retry logic
func (a *Aggregator) migrateStepWithRetry(httpClient *http.Client, step time.Time, shardKey int32) (uint64, error) {
	config := NewDefaultMigrationConfig() // Use default for existing aggregator
	return a.migrateStepWithRetryWithConfig(httpClient, step, shardKey, config)
}

// migrateStepWithRetryWithConfig performs migration for a time step with retry logic using custom config
func (a *Aggregator) migrateStepWithRetryWithConfig(httpClient *http.Client, step time.Time, shardKey int32, config *MigrationConfig) (uint64, error) {
	var lastErr error

	for attempt := 0; attempt < maxRetryAttempts; attempt++ {
		if attempt > 0 {
			log.Printf("[migration] Retry attempt %d for step %s", attempt+1, step.Format("2006-01-02 15:04:05"))
			time.Sleep(retryDelay * time.Duration(attempt)) // Exponential backoff
		}

		err := migrateSingleStep(httpClient, a.config.KHAddr, a.config.KHUser, a.config.KHPassword, uint32(step.Unix()), shardKey, config)
		if err == nil {
			// Success! Count migrated rows
			rowCount, countErr := a.countMigratedRowsWithConfig(httpClient, step, shardKey, config)
			if countErr != nil {
				log.Printf("[migration] Warning: failed to count migrated rows: %v", countErr)
				rowCount = 0 // Use 0 as fallback
			}
			return rowCount, nil
		}

		lastErr = err
		log.Printf("[migration] Attempt %d failed for step %s: %v", attempt+1, step.Format("2006-01-02 15:04:05"), err)
		a.logMigrationEventWithConfig(httpClient, step, "WARN", "Migration attempt failed", fmt.Sprintf("attempt=%d, error=%v", attempt+1, err), config)

		// Update retry count in state
		a.updateMigrationStateWithConfig(httpClient, shardKey, step, "in_progress", err.Error(), uint32(attempt+1), 0, config)
	}

	return 0, fmt.Errorf("migration failed after %d attempts, last error: %w", maxRetryAttempts, lastErr)
}

// countMigratedRowsWithConfig counts how many rows were migrated for a specific time step and shard with custom config
func (a *Aggregator) countMigratedRowsWithConfig(httpClient *http.Client, step time.Time, shardKey int32, config *MigrationConfig) (uint64, error) {
	countQuery := fmt.Sprintf(`
		SELECT count() as cnt
		FROM %s 
		WHERE time = toDateTime(%d) AND metric %% %d = %d`,
		config.V3TableName, step.Unix(), config.TotalShards, shardKey-1)

	req := &chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       a.config.KHAddr,
		User:       a.config.KHUser,
		Password:   a.config.KHPassword,
		Query:      countQuery,
	}
	resp, err := req.Execute(context.Background())
	if err != nil {
		return 0, fmt.Errorf("failed to count migrated rows: %w", err)
	}
	defer resp.Close()

	var count uint64
	if _, err := fmt.Fscanf(resp, "%d", &count); err != nil {
		return 0, fmt.Errorf("failed to parse row count: %w", err)
	}

	return count, nil
}

// waitForAllShardsToComplete waits for all shards to complete migration of a specific time step
func (a *Aggregator) waitForAllShardsToComplete(httpClient *http.Client, step time.Time) error {
	config := NewDefaultMigrationConfig() // Use default for existing aggregator
	return a.waitForAllShardsToCompleteWithConfig(httpClient, step, config)
}

// waitForAllShardsToCompleteWithConfig waits for all shards to complete migration of a specific time step with custom config
func (a *Aggregator) waitForAllShardsToCompleteWithConfig(httpClient *http.Client, step time.Time, config *MigrationConfig) error {
	timeout := time.Now().Add(coordinationTimeout)

	for time.Now().Before(timeout) {
		// Count how many shards have completed this step
		completedQuery := fmt.Sprintf(`
			SELECT count(DISTINCT shard_key) as completed_shards
			FROM %s 
			WHERE current_step = toDateTime(%d) AND status = 'completed'`,
			config.StateTableName, step.Unix())

		req := &chutil.ClickHouseHttpRequest{
			HttpClient: httpClient,
			Addr:       a.config.KHAddr,
			User:       a.config.KHUser,
			Password:   a.config.KHPassword,
			Query:      completedQuery,
		}
		resp, err := req.Execute(context.Background())
		if err != nil {
			return fmt.Errorf("failed to check shard completion: %w", err)
		}

		var completedShards int
		if _, err := fmt.Fscanf(resp, "%d", &completedShards); err == nil {
			resp.Close()
			if completedShards >= config.TotalShards {
				log.Printf("[migration] All shards completed step %s", step.Format("2006-01-02 15:04:05"))
				return nil
			}
			log.Printf("[migration] Waiting for shards to complete step %s (%d/%d completed)",
				step.Format("2006-01-02 15:04:05"), completedShards, config.TotalShards)
		} else {
			resp.Close()
		}

		time.Sleep(5 * time.Second)
	}

	return fmt.Errorf("timeout waiting for all shards to complete step %s", step.Format("2006-01-02 15:04:05"))
}
