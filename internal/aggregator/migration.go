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

// MigrationState represents the state of migration for a shard and hour
type MigrationState struct {
	ShardKey     int32      `json:"shard_key"`
	CurrentHour  time.Time  `json:"current_hour"`
	Status       string     `json:"status"` // 'in_progress', 'completed', 'failed'
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
	Hour      time.Time `json:"hour"`
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

		// Find next hour to migrate
		nextHour, err := a.findNextHourToMigrate(httpClient, shardKey)
		if err != nil {
			log.Printf("[migration] Error finding next hour: %v", err)
			a.logMigrationEvent(httpClient, time.Time{}, "ERROR", "Failed to find next hour", err.Error())
			time.Sleep(sleepInterval)
			continue
		}

		if nextHour.IsZero() {
			log.Println("[migration] No more hours to migrate, migration complete!")
			a.logMigrationEvent(httpClient, time.Time{}, "INFO", "Migration completed", "no more hours to process")
			return
		}

		log.Printf("[migration] Processing hour: %s", nextHour.Format("2006-01-02 15:04:05"))

		// Update state to in_progress
		if err := a.updateMigrationState(httpClient, shardKey, nextHour, "in_progress", "", 0, 0); err != nil {
			log.Printf("[migration] Failed to update state to in_progress: %v", err)
			time.Sleep(sleepInterval)
			continue
		}

		// Perform migration for this hour
		rowsMigrated, err := a.migrateHourWithRetry(httpClient, nextHour, shardKey)
		if err != nil {
			log.Printf("[migration] Failed to migrate hour %s: %v", nextHour.Format("2006-01-02 15:04:05"), err)
			a.logMigrationEvent(httpClient, nextHour, "ERROR", "Migration failed", err.Error())
			a.updateMigrationState(httpClient, shardKey, nextHour, "failed", err.Error(), 0, rowsMigrated)
			time.Sleep(sleepInterval)
			continue
		}

		// Update state to completed
		if err := a.updateMigrationState(httpClient, shardKey, nextHour, "completed", "", 0, rowsMigrated); err != nil {
			log.Printf("[migration] Failed to update state to completed: %v", err)
		}

		log.Printf("[migration] Successfully migrated hour %s (%d rows)", nextHour.Format("2006-01-02 15:04:05"), rowsMigrated)
		a.logMigrationEvent(httpClient, nextHour, "INFO", "Hour migration completed", fmt.Sprintf("rows_migrated=%d", rowsMigrated))

		// Wait for all shards to complete this hour before moving to next
		if err := a.waitForAllShardsToComplete(httpClient, nextHour); err != nil {
			log.Printf("[migration] Error waiting for shards: %v", err)
			a.logMigrationEvent(httpClient, nextHour, "WARN", "Coordination warning", err.Error())
		}

		// Small delay before processing next hour
		time.Sleep(5 * time.Second)
	}
}

// TestMigrateSingleHour is a standalone function for testing migration with local ClickHouse
// Example usage: TestMigrateSingleHour("localhost:8123", "", "", 1672531200, 1)
func TestMigrateSingleHour(khAddr, khUser, khPassword string, timestamp uint32, shardKey int32) error {
	httpClient := makeHTTPClient()
	return migrateSingleHour(httpClient, khAddr, khUser, khPassword, timestamp, shardKey)
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

// migrateSingleHour migrates data for a single hour from V2 to V3 format
// timestamp should be rounded to hour boundary
func migrateSingleHour(httpClient *http.Client, khAddr, khUser, khPassword string, timestamp uint32, shardKey int32) error {
	log.Printf("[migration] Starting migration for timestamp %d (hour: %s), shard %d (metric %% 16 = %d)", timestamp, time.Unix(int64(timestamp), 0).Format("2006-01-02 15:04:05"), shardKey, shardKey-1)

	// Step 1: Select data from V2 table for the given timestamp and shard
	// Include all aggregate fields using special functions to get raw data
	selectQuery := fmt.Sprintf(`
		SELECT metric, time, key0, key1, key2, key3, key4, key5, key6, key7, key8, key9, key10, key11, key12, key13, key14, key15, skey,
			count, min, max, sum, sumsquare, percentiles, uniq_state, min_host, max_host
		FROM statshouse_value_1h_dist 
		WHERE time = toDateTime(%d)
		  AND metric %% 16 = %d`,
		timestamp, shardKey-1,
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
	return streamConvertAndInsert(httpClient, khAddr, khUser, khPassword, resp)
}

// streamConvertAndInsert reads V2 rowbinary data, converts to V3 format, and inserts
func streamConvertAndInsert(httpClient *http.Client, khAddr, khUser, khPassword string, v2Data io.Reader) error {
	// Create insert query for V3 table
	insertQuery := `INSERT INTO statshouse_v3_1h(
		metric,time,
		tag0,tag1,tag2,tag3,tag4,tag5,tag6,tag7,
		tag8,tag9,tag10,tag11,tag12,tag13,tag14,tag15,stag47,
		count,min,max,sum,sumsquare,
		min_host,max_host,percentiles,uniq_state
	)
	FORMAT RowBinary`

	// Create pipe for streaming conversion
	pipeReader, pipeWriter := io.Pipe()

	// Start goroutine to convert data
	go func() {
		defer pipeWriter.Close()
		err := convertV2ToV3Stream(v2Data, pipeWriter)
		if err != nil {
			log.Printf("[migration] Error during conversion: %v", err)
			pipeWriter.CloseWithError(err)
		}
	}()

	// Execute insert with converted data
	bodyBytes, err := io.ReadAll(pipeReader)
	if err != nil {
		return fmt.Errorf("failed to read body: %w", err)
	}
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

	log.Printf("[migration] Successfully inserted converted data")
	return nil
}

// convertV2ToV3Stream reads V2 rowbinary format and writes V3 rowbinary format
func convertV2ToV3Stream(input io.Reader, output io.Writer) error {
	reader := bufio.NewReaderSize(input, 8192)
	rows, err := processV2Chunk(reader, output)
	if err != nil {
		return fmt.Errorf("conversion error: %w", err)
	}

	log.Printf("[migration] Converted %d rows", rows)
	return nil
}

// processV2Chunk processes a chunk of V2 rowbinary data and converts complete rows to V3 format
func processV2Chunk(reader *bufio.Reader, output io.Writer) (rowsProcessed int, err error) {
	rowData := make([]byte, 0, 4096) // Buffer for single converted row

	for {
		// Try to parse one V2 row
		v2Row, parseErr := parseV2Row(reader)
		if parseErr != nil {
			if errors.Is(parseErr, io.EOF) {
				// End of input, we're done
				break
			}
			if errors.Is(parseErr, io.ErrUnexpectedEOF) {
				// Incomplete row, but we're using io.Reader so this shouldn't happen
				// unless the reader itself is incomplete
				return rowsProcessed, parseErr
			}
			return rowsProcessed, fmt.Errorf("failed to parse V2 row: %w", parseErr)
		}

		// Convert to V3 format
		rowData = rowData[:0] // Reset buffer
		rowData = convertRowV2ToV3(rowData, v2Row)

		// Write converted row
		if _, writeErr := output.Write(rowData); writeErr != nil {
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

	// min_host
	row.min_host.ReadFrom(reader)

	// max_host
	row.max_host.ReadFrom(reader)

	// percentiles
	row.perc = &data_model.ChDigest{}
	row.perc.ReadFrom(reader)

	// uniq_state
	row.uniq = &data_model.ChUnique{}
	row.uniq.ReadFrom(reader)

	return row, nil
}

// convertRowV2ToV3 converts a single V2 row to V3 rowbinary format
func convertRowV2ToV3(buf []byte, row *v2Row) []byte {
	// Basic fields
	buf = rowbinary.AppendInt32(buf, row.metric)
	buf = rowbinary.AppendDateTime(buf, time.Unix(int64(row.time), 0))

	// First 16 tags
	for i := 0; i < 16; i++ {
		buf = rowbinary.AppendInt32(buf, row.keys[i])
	}

	// stag47
	buf = rowbinary.AppendString(buf, row.skey) // stag47

	// Basic aggregates
	buf = rowbinary.AppendFloat64(buf, row.count)
	buf = rowbinary.AppendFloat64(buf, row.min)
	buf = rowbinary.AppendFloat64(buf, row.max)
	buf = rowbinary.AppendFloat64(buf, row.sum)
	buf = rowbinary.AppendFloat64(buf, row.sumsquare)

	// min_host
	minHost := data_model.ArgMinMaxStringFloat32{
		AsInt32: row.min_host.Arg,
		Val:     row.min_host.Val,
	}
	buf = minHost.MarshallAppend(buf)
	// max_host
	maxHost := data_model.ArgMinMaxStringFloat32{
		AsInt32: row.max_host.Arg,
		Val:     row.max_host.Val,
	}
	buf = maxHost.MarshallAppend(buf)

	buf = row.perc.MarshallAppend(buf, 1) // percentiles
	buf = row.uniq.MarshallAppend(buf)    // uniq_state

	return buf
}

// createMigrationTables creates the migration state and log tables if they don't exist
func (a *Aggregator) createMigrationTables(httpClient *http.Client) error {
	// Create migration_state table
	stateTableQuery := `
		CREATE TABLE IF NOT EXISTS migration_state (
			shard_key Int32,
			current_hour DateTime,
			status String,
			started_at DateTime,
			completed_at Nullable(DateTime),
			rows_migrated UInt64,
			error_message String,
			retry_count UInt32
		) ENGINE = ReplacingMergeTree(started_at)
		ORDER BY (shard_key, current_hour)`

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
	logTableQuery := `
		CREATE TABLE IF NOT EXISTS migration_logs (
			timestamp DateTime,
			shard_key Int32,
			hour DateTime,
			level String,
			message String,
			details String
		) ENGINE = MergeTree()
		ORDER BY (timestamp, shard_key)`

	req.Query = logTableQuery
	resp, err = req.Execute(context.Background())
	if err != nil {
		return fmt.Errorf("failed to create migration_logs table: %w", err)
	}
	resp.Close()

	return nil
}

// findNextHourToMigrate finds the next hour that needs migration
func (a *Aggregator) findNextHourToMigrate(httpClient *http.Client, shardKey int32) (time.Time, error) {
	// Strategy: Look for the earliest hour with data in V2 table that hasn't been migrated yet
	// Start from current hour and work backwards

	currentHour := time.Now().Truncate(time.Hour)

	// TODO: For optimization, we could query completed hours first to skip checking them:
	// completedHoursQuery := `SELECT DISTINCT current_hour FROM migration_state WHERE status = 'completed' AND shard_key = ? ORDER BY current_hour DESC`

	// For now, let's use a simple approach: check the last 48 hours for unmigrated data
	for i := 0; i < 48; i++ {
		checkHour := currentHour.Add(-time.Duration(i) * time.Hour)

		// Check if this hour has data in V2 table
		hasDataQuery := fmt.Sprintf(`
			SELECT count() as cnt
			FROM statshouse_value_1h_dist 
			WHERE time = toDateTime(%d) AND metric %% 16 = %d
			LIMIT 1`, checkHour.Unix(), shardKey-1)

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
			// This hour has data, check if it's already migrated
			migratedQuery := fmt.Sprintf(`
				SELECT count() as cnt
				FROM migration_state 
				WHERE current_hour = toDateTime(%d) 
				AND shard_key = %d 
				AND status = 'completed'`, checkHour.Unix(), shardKey)

			req.Query = migratedQuery
			resp2, err := req.Execute(context.Background())
			if err != nil {
				resp.Close()
				return time.Time{}, fmt.Errorf("failed to check migration status: %w", err)
			}

			var migratedCount uint64
			if _, err := fmt.Fscanf(resp2, "%d", &migratedCount); err == nil && migratedCount == 0 {
				// Hour has data but not migrated yet
				resp.Close()
				resp2.Close()
				return checkHour, nil
			}
			resp2.Close()
		}
		resp.Close()
	}

	// No unmigrated hours found
	return time.Time{}, nil
}

// updateMigrationState updates the migration state for a shard and hour
func (a *Aggregator) updateMigrationState(httpClient *http.Client, shardKey int32, hour time.Time, status, errorMsg string, retryCount uint32, rowsMigrated uint64) error {
	var completedAt string
	if status == "completed" {
		completedAt = fmt.Sprintf("'%s'", time.Now().Format("2006-01-02 15:04:05"))
	} else {
		completedAt = "NULL"
	}

	updateQuery := fmt.Sprintf(`
		INSERT INTO migration_state 
		(shard_key, current_hour, status, started_at, completed_at, rows_migrated, error_message, retry_count)
		VALUES (%d, toDateTime(%d), '%s', '%s', %s, %d, '%s', %d)`,
		shardKey, hour.Unix(), status, time.Now().Format("2006-01-02 15:04:05"),
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
func (a *Aggregator) logMigrationEvent(httpClient *http.Client, hour time.Time, level, message, details string) {
	hourStr := "toDateTime(0)"
	if !hour.IsZero() {
		hourStr = fmt.Sprintf("toDateTime(%d)", hour.Unix())
	}

	logQuery := fmt.Sprintf(`
		INSERT INTO migration_logs 
		(timestamp, shard_key, hour, level, message, details)
		VALUES ('%s', %d, %s, '%s', '%s', '%s')`,
		time.Now().Format("2006-01-02 15:04:05"), a.shardKey, hourStr, level, message, details)

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

// migrateHourWithRetry performs migration for an hour with retry logic
func (a *Aggregator) migrateHourWithRetry(httpClient *http.Client, hour time.Time, shardKey int32) (uint64, error) {
	var lastErr error

	for attempt := 0; attempt < maxRetryAttempts; attempt++ {
		if attempt > 0 {
			log.Printf("[migration] Retry attempt %d for hour %s", attempt+1, hour.Format("2006-01-02 15:04:05"))
			time.Sleep(retryDelay * time.Duration(attempt)) // Exponential backoff
		}

		err := migrateSingleHour(httpClient, a.config.KHAddr, a.config.KHUser, a.config.KHPassword, uint32(hour.Unix()), shardKey)
		if err == nil {
			// Success! Count migrated rows
			rowCount, countErr := a.countMigratedRows(httpClient, hour, shardKey)
			if countErr != nil {
				log.Printf("[migration] Warning: failed to count migrated rows: %v", countErr)
				rowCount = 0 // Use 0 as fallback
			}
			return rowCount, nil
		}

		lastErr = err
		log.Printf("[migration] Attempt %d failed for hour %s: %v", attempt+1, hour.Format("2006-01-02 15:04:05"), err)
		a.logMigrationEvent(httpClient, hour, "WARN", "Migration attempt failed", fmt.Sprintf("attempt=%d, error=%v", attempt+1, err))

		// Update retry count in state
		a.updateMigrationState(httpClient, shardKey, hour, "in_progress", err.Error(), uint32(attempt+1), 0)
	}

	return 0, fmt.Errorf("migration failed after %d attempts, last error: %w", maxRetryAttempts, lastErr)
}

// countMigratedRows counts how many rows were migrated for a specific hour and shard
func (a *Aggregator) countMigratedRows(httpClient *http.Client, hour time.Time, shardKey int32) (uint64, error) {
	countQuery := fmt.Sprintf(`
		SELECT count() as cnt
		FROM statshouse_v3_1h 
		WHERE time = toDateTime(%d) AND metric %% 16 = %d`,
		hour.Unix(), shardKey-1)

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

// waitForAllShardsToComplete waits for all shards to complete migration of a specific hour
func (a *Aggregator) waitForAllShardsToComplete(httpClient *http.Client, hour time.Time) error {
	timeout := time.Now().Add(coordinationTimeout)

	for time.Now().Before(timeout) {
		// Count how many shards have completed this hour
		completedQuery := fmt.Sprintf(`
			SELECT count(DISTINCT shard_key) as completed_shards
			FROM migration_state 
			WHERE current_hour = toDateTime(%d) AND status = 'completed'`,
			hour.Unix())

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
			// Assuming 16 shards total (could make this configurable)
			if completedShards >= 16 {
				log.Printf("[migration] All shards completed hour %s", hour.Format("2006-01-02 15:04:05"))
				return nil
			}
			log.Printf("[migration] Waiting for shards to complete hour %s (%d/16 completed)",
				hour.Format("2006-01-02 15:04:05"), completedShards)
		} else {
			resp.Close()
		}

		time.Sleep(5 * time.Second)
	}

	return fmt.Errorf("timeout waiting for all shards to complete hour %s", hour.Format("2006-01-02 15:04:05"))
}
