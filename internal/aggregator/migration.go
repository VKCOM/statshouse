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
