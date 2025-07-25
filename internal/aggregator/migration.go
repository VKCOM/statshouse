// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package aggregator

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"time"

	"github.com/vkcom/statshouse/internal/vkgo/rowbinary"
)

const (
	noErrorsWindow = 5 * 60 // 5 minutes
	maxInsertTime  = 2.0    // 2 seconds
	sleepInterval  = 30 * time.Second
)

// goMigrate runs the migration loop in a goroutine.
// It checks Aggregator's timeSinceLastError and insertTimeEWMA to decide whether to proceed.
func (a *Aggregator) goMigrate(cancelCtx context.Context) {
	// TODO: implement, rough plan below
	// border ts is timestamp after which all data is migrated
	// single ts can be too big, so we need to migrate in chunks in order to do it we store additional offset
	// 1. [x] check remote config flag for migration
	// 2. [x] check current load and decide if we need to migrate or just wait (look at insert timings and errors)
	// 3. [ ] look for migration state if there is no state, create it (some table in ClickHouse)
	// 4. [x] if we need to migrate more data, select data from V2 - data is capped by 2GB per shard per hour, so no need to break hour into chunks
	// 5. [x] insert into V3
	// 6. [ ] if success, save new border of migration and offset on current border in ClickHouse migration state table

	if a.replicaKey != 1 {
		return // Only one replica should run migration
	}
	log.Println("[migration] Starting migration routine")
	for {
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

		// TODO: Add actual migration logic here
		log.Println("[migration] Would run migration step here")
		time.Sleep(10 * time.Second)

		select {
		case <-cancelCtx.Done():
			log.Println("[migration] Exiting migration routine (context cancelled)")
			return
		default:
		}
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
		INSERT INTO statshouse_value_1h (
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	queryURL := fmt.Sprintf("http://%s/?query=%s", khAddr, url.QueryEscape(insertQuery))
	req, err := http.NewRequestWithContext(ctx, "POST", queryURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	if khUser != "" {
		req.Header.Add("X-ClickHouse-User", khUser)
	}
	if khPassword != "" {
		req.Header.Add("X-ClickHouse-Key", khPassword)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute insert query: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("ClickHouse insert returned status %d: %s", resp.StatusCode, string(body))
	}

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
		SELECT 
			metric, time, key0, key1, key2, key3, key4, key5, key6, key7, key8, key9, key10, key11, key12, key13, key14, key15, skey,
			sum(count) as count, 
			min(min) as min, 
			max(max) as max, 
			sum(sum) as sum, 
			sum(sumsquare) as sumsquare,
			quantilesTDigestMergeState(0.5)(percentiles) as percentiles_state,
			uniqMergeState(uniq_state) as uniq_merge_state,
			argMinMergeState(min_host) as min_host_state,
			argMaxMergeState(max_host) as max_host_state
		FROM statshouse_value_1h_dist 
		WHERE time = toDateTime(%d)
		  AND metric %% 16 = %d
		GROUP BY metric, time, key0, key1, key2, key3, key4, key5, key6, key7, key8, key9, key10, key11, key12, key13, key14, key15, skey
		FORMAT RowBinary`,
		timestamp, shardKey-1,
	)

	// Step 2: Execute query and get response
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute) // Long timeout for large data
	defer cancel()

	queryURL := fmt.Sprintf("http://%s/?query=%s", khAddr, url.QueryEscape(selectQuery))
	req, err := http.NewRequestWithContext(ctx, "POST", queryURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	if khUser != "" {
		req.Header.Add("X-ClickHouse-User", khUser)
	}
	if khPassword != "" {
		req.Header.Add("X-ClickHouse-Key", khPassword)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute select query: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("ClickHouse returned status %d: %s", resp.StatusCode, string(body))
	}

	log.Printf("[migration] Successfully retrieved shard data, converting and inserting...")

	// Step 3: Stream convert and insert data
	return streamConvertAndInsert(httpClient, khAddr, khUser, khPassword, resp.Body)
}

// streamConvertAndInsert reads V2 rowbinary data, converts to V3 format, and inserts
func streamConvertAndInsert(httpClient *http.Client, khAddr, khUser, khPassword string, v2Data io.Reader) error {
	// Create insert query for V3 incoming table (not the final table)
	insertQuery := `INSERT INTO statshouse_v3_incoming FORMAT RowBinary`

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

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
	queryURL := fmt.Sprintf("http://%s/?query=%s", khAddr, url.QueryEscape(insertQuery))
	req, err := http.NewRequestWithContext(ctx, "POST", queryURL, pipeReader)
	if err != nil {
		return fmt.Errorf("failed to create insert request: %w", err)
	}

	if khUser != "" {
		req.Header.Add("X-ClickHouse-User", khUser)
	}
	if khPassword != "" {
		req.Header.Add("X-ClickHouse-Key", khPassword)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute insert query: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("ClickHouse insert returned status %d: %s", resp.StatusCode, string(body))
	}

	log.Printf("[migration] Successfully inserted converted data")
	return nil
}

// convertV2ToV3Stream reads V2 rowbinary format and writes V3 rowbinary format
func convertV2ToV3Stream(input io.Reader, output io.Writer) error {
	buf := make([]byte, 0, 1024*1024) // 1MB buffer for building rows
	readBuf := make([]byte, 64*1024)  // 64KB read buffer

	var totalRows int

	for {
		// Read chunk of data
		n, err := input.Read(readBuf)
		if n > 0 {
			rows, newBuf, convertErr := processV2Chunk(append(buf, readBuf[:n]...), output)
			if convertErr != nil {
				return fmt.Errorf("conversion error: %w", convertErr)
			}
			totalRows += rows
			buf = newBuf
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read error: %w", err)
		}
	}

	// Process remaining data in buffer
	if len(buf) > 0 {
		rows, _, err := processV2Chunk(buf, output)
		if err != nil {
			return fmt.Errorf("final conversion error: %w", err)
		}
		totalRows += rows
	}

	log.Printf("[migration] Converted %d rows", totalRows)
	return nil
}

// processV2Chunk processes a chunk of V2 rowbinary data and converts complete rows to V3 format
func processV2Chunk(data []byte, output io.Writer) (rowsProcessed int, remainingData []byte, err error) {
	pos := 0
	rowData := make([]byte, 0, 4096) // Buffer for single converted row

	for pos < len(data) {
		// Try to parse one V2 row
		v2Row, bytesRead, parseErr := parseV2Row(data[pos:])
		if parseErr != nil {
			if parseErr == io.ErrUnexpectedEOF {
				// Incomplete row, return remaining data
				return rowsProcessed, data[pos:], nil
			}
			return rowsProcessed, nil, fmt.Errorf("failed to parse V2 row: %w", parseErr)
		}

		// Convert to V3 format
		rowData = rowData[:0] // Reset buffer
		rowData = convertRowV2ToV3(rowData, v2Row)

		// Write converted row
		if _, writeErr := output.Write(rowData); writeErr != nil {
			return rowsProcessed, nil, fmt.Errorf("failed to write converted row: %w", writeErr)
		}

		pos += bytesRead
		rowsProcessed++
	}

	return rowsProcessed, nil, nil
}

// v2Row represents a parsed row from V2 format
type v2Row struct {
	metric      int32
	time        uint32
	keys        [16]int32
	skey        string
	count       float64
	min         float64
	max         float64
	sum         float64
	sumsquare   float64
	percentiles []byte // raw aggregatefunction data
	uniq_state  []byte // raw aggregatefunction data
	min_host    []byte // raw aggregatefunction data
	max_host    []byte // raw aggregatefunction data
}

// parseV2Row parses a single V2 row from rowbinary data
func parseV2Row(data []byte) (*v2Row, int, error) {
	if len(data) < 4 {
		return nil, 0, io.ErrUnexpectedEOF
	}

	pos := 0
	row := &v2Row{}

	// Parse metric (Int32)
	if pos+4 > len(data) {
		return nil, 0, io.ErrUnexpectedEOF
	}
	row.metric = int32(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4

	// Parse time (DateTime = UInt32)
	if pos+4 > len(data) {
		return nil, 0, io.ErrUnexpectedEOF
	}
	row.time = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	// Parse all 16 keys (key0 through key15)
	for i := 0; i < 16; i++ {
		if pos+4 > len(data) {
			return nil, 0, io.ErrUnexpectedEOF
		}
		row.keys[i] = int32(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4
	}

	// Parse skey (String) - LEB128 varint format
	if pos >= len(data) {
		return nil, 0, io.ErrUnexpectedEOF
	}

	// Read LEB128 varint for string length
	var skeyLen uint64
	var shift uint
	for i := 0; i < 9; i++ { // Max 9 bytes for 64-bit varint
		if pos+i >= len(data) {
			return nil, 0, io.ErrUnexpectedEOF
		}
		b := data[pos+i]
		skeyLen |= uint64(b&0x7F) << shift
		if (b & 0x80) == 0 {
			pos += i + 1
			break
		}
		shift += 7
	}

	// Bounds check for skey
	if pos+int(skeyLen) > len(data) {
		return nil, 0, io.ErrUnexpectedEOF
	}
	if skeyLen > 1<<31-1 || pos < 0 {
		return nil, 0, fmt.Errorf("invalid skey length: %d, pos: %d, data len: %d", skeyLen, pos, len(data))
	}
	row.skey = string(data[pos : pos+int(skeyLen)])
	pos += int(skeyLen)

	// Parse simple aggregates (Float64 each)
	// count
	if pos+8 > len(data) {
		return nil, 0, io.ErrUnexpectedEOF
	}
	row.count = math.Float64frombits(binary.LittleEndian.Uint64(data[pos:]))
	pos += 8

	// min
	if pos+8 > len(data) {
		return nil, 0, io.ErrUnexpectedEOF
	}
	row.min = math.Float64frombits(binary.LittleEndian.Uint64(data[pos:]))
	pos += 8

	// max
	if pos+8 > len(data) {
		return nil, 0, io.ErrUnexpectedEOF
	}
	row.max = math.Float64frombits(binary.LittleEndian.Uint64(data[pos:]))
	pos += 8

	// sum
	if pos+8 > len(data) {
		return nil, 0, io.ErrUnexpectedEOF
	}
	row.sum = math.Float64frombits(binary.LittleEndian.Uint64(data[pos:]))
	pos += 8

	// sumsquare
	if pos+8 > len(data) {
		return nil, 0, io.ErrUnexpectedEOF
	}
	row.sumsquare = math.Float64frombits(binary.LittleEndian.Uint64(data[pos:]))
	pos += 8

	// Parse complex aggregates (raw binary data with LEB128 length prefix)
	// percentiles_state
	percentilesLen, newPos, err := parseLEB128(data, pos)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to parse percentiles length: %w", err)
	}
	pos = newPos
	if pos+int(percentilesLen) > len(data) {
		return nil, 0, io.ErrUnexpectedEOF
	}
	row.percentiles = make([]byte, percentilesLen)
	copy(row.percentiles, data[pos:pos+int(percentilesLen)])
	pos += int(percentilesLen)

	// uniq_merge_state
	uniqLen, newPos, err := parseLEB128(data, pos)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to parse uniq_state length: %w", err)
	}
	pos = newPos
	if pos+int(uniqLen) > len(data) {
		return nil, 0, io.ErrUnexpectedEOF
	}
	row.uniq_state = make([]byte, uniqLen)
	copy(row.uniq_state, data[pos:pos+int(uniqLen)])
	pos += int(uniqLen)

	// min_host_state
	minHostLen, newPos, err := parseLEB128(data, pos)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to parse min_host length: %w", err)
	}
	pos = newPos
	if pos+int(minHostLen) > len(data) {
		return nil, 0, io.ErrUnexpectedEOF
	}
	row.min_host = make([]byte, minHostLen)
	copy(row.min_host, data[pos:pos+int(minHostLen)])
	pos += int(minHostLen)

	// max_host_state
	maxHostLen, newPos, err := parseLEB128(data, pos)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to parse max_host length: %w", err)
	}
	pos = newPos
	if pos+int(maxHostLen) > len(data) {
		return nil, 0, io.ErrUnexpectedEOF
	}
	row.max_host = make([]byte, maxHostLen)
	copy(row.max_host, data[pos:pos+int(maxHostLen)])
	pos += int(maxHostLen)

	return row, pos, nil
}

// parseLEB128 parses a LEB128 varint from the data starting at pos
func parseLEB128(data []byte, pos int) (uint64, int, error) {
	var result uint64
	var shift uint
	for i := 0; i < 9; i++ { // Max 9 bytes for 64-bit varint
		if pos+i >= len(data) {
			return 0, 0, io.ErrUnexpectedEOF
		}
		b := data[pos+i]
		result |= uint64(b&0x7F) << shift
		if (b & 0x80) == 0 {
			return result, pos + i + 1, nil
		}
		shift += 7
	}
	return 0, 0, fmt.Errorf("LEB128 varint too long")
}

// convertRowV2ToV3 converts a single V2 row to V3 rowbinary format
func convertRowV2ToV3(buf []byte, v2 *v2Row) []byte {
	// V3 format according to actual table structure (DESCRIBE statshouse_v3_incoming):
	// index_type,metric,pre_tag,pre_stag,time,tag0,stag0,...,tag47,stag47,
	// count,min,max,max_count,sum,sumsquare,min_host,max_host,max_count_host,min_host_legacy,max_host_legacy,percentiles,uniq_state
	//
	// Conversion rules:
	// - index_type = 0 (for hour data)
	// - pre_tag = 0, pre_stag = ""
	// - key0-key15 → tag0-tag15, tag16-tag47 = 0
	// - skey → stag47, all other stags = ""
	// - max_count = max (V2 doesn't have separate max_count)
	// - max_count_host = max_host (V2 doesn't have separate max_count_host)

	// index_type = 0 (for hour data)
	buf = rowbinary.AppendUint8(buf, 0)

	// metric
	buf = rowbinary.AppendInt32(buf, v2.metric)

	// pre_tag = 0 (always empty for migration)
	buf = rowbinary.AppendUint32(buf, 0)

	// pre_stag = "" (always empty for migration)
	buf = rowbinary.AppendString(buf, "")

	// time - convert Unix timestamp to DateTime
	buf = rowbinary.AppendDateTime(buf, time.Unix(int64(v2.time), 0))

	// tags 0-47: first 16 from V2 keys, rest are 0
	for i := 0; i < 48; i++ {
		if i < 16 {
			buf = rowbinary.AppendInt32(buf, v2.keys[i])
		} else {
			buf = rowbinary.AppendInt32(buf, 0)
		}
		// stag47 gets skey from V2, all other stags are empty
		if i == 47 {
			buf = rowbinary.AppendString(buf, v2.skey)
		} else {
			buf = rowbinary.AppendString(buf, "")
		}
	}

	// Simple aggregates - use actual data from V2
	buf = rowbinary.AppendFloat64(buf, v2.count)
	buf = rowbinary.AppendFloat64(buf, v2.min)
	buf = rowbinary.AppendFloat64(buf, v2.max)
	buf = rowbinary.AppendFloat64(buf, v2.max) // max_count = max (V2 doesn't have separate max_count)
	buf = rowbinary.AppendFloat64(buf, v2.sum)
	buf = rowbinary.AppendFloat64(buf, v2.sumsquare)

	// String-based argMin/argMax - temporarily use empty to avoid corruption
	buf = rowbinary.AppendArgMinMaxStringEmpty(buf) // min_host (temporarily empty)
	buf = rowbinary.AppendArgMinMaxStringEmpty(buf) // max_host (temporarily empty)
	buf = rowbinary.AppendArgMinMaxStringEmpty(buf) // max_count_host (temporarily empty)

	// Legacy argMin/argMax (Int32 format) - temporarily use empty to avoid corruption
	buf = rowbinary.AppendArgMinMaxInt32Float32Empty(buf) // min_host_legacy (temporarily empty)
	buf = rowbinary.AppendArgMinMaxInt32Float32Empty(buf) // max_host_legacy (temporarily empty)

	// Complex aggregates - use raw V2 data directly (already has length prefix)
	buf = append(buf, v2.percentiles...) // percentiles (raw V2 data)
	buf = append(buf, v2.uniq_state...)  // uniq_state (raw V2 data)

	return buf
}

// convertArgMinMaxToString converts argMin/argMax from Int32 to String format
func convertArgMinMaxToString(buf []byte, argMinMaxData []byte) []byte {
	if len(argMinMaxData) == 0 {
		return rowbinary.AppendArgMinMaxStringEmpty(buf)
	}

	// V2 format (Int32): [hasArg:1byte, arg:4bytes if hasArg!=0, hasVal:1byte, val:4bytes if hasVal!=0]
	// V3 format (String): [string_len:4bytes, string_data, string_terminator:1byte, hasVal:1byte, val:8bytes if hasVal!=0]

	pos := 0

	// Parse hasArg flag
	if pos >= len(argMinMaxData) {
		return rowbinary.AppendArgMinMaxStringEmpty(buf)
	}
	hasArg := argMinMaxData[pos]
	pos++

	var argInt32 int32
	// Parse arg if present
	if hasArg != 0 {
		if pos+4 > len(argMinMaxData) {
			return rowbinary.AppendArgMinMaxStringEmpty(buf)
		}
		argInt32 = int32(binary.LittleEndian.Uint32(argMinMaxData[pos:]))
		pos += 4
	}

	// Parse hasVal flag
	if pos >= len(argMinMaxData) {
		return rowbinary.AppendArgMinMaxStringEmpty(buf)
	}
	hasVal := argMinMaxData[pos]
	pos++

	var valueFloat32 float32
	// Parse value if present
	if hasVal != 0 {
		if pos+4 > len(argMinMaxData) {
			return rowbinary.AppendArgMinMaxStringEmpty(buf)
		}
		valueFloat32 = math.Float32frombits(binary.LittleEndian.Uint32(argMinMaxData[pos:]))
		pos += 4
	}

	// If no arg or value, return empty
	if hasArg == 0 || hasVal == 0 {
		return rowbinary.AppendArgMinMaxStringEmpty(buf)
	}

	// Convert Int32 to String format: 5 bytes total (0 + 4 bytes of int32 in little endian)
	// Create the special string format that chutil recognizes as converted Int32
	argString := make([]byte, 5)
	argString[0] = 0 // First byte is zero to indicate this is a converted Int32
	binary.LittleEndian.PutUint32(argString[1:], uint32(argInt32))

	// Use AppendArgMinMaxStringFloat64 (note: Float64, not Float32)
	return rowbinary.AppendArgMinMaxStringFloat64(buf, string(argString), float64(valueFloat32))
}
