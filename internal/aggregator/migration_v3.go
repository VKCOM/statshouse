package aggregator

import (
	"bufio"
	"context"
	"encoding/binary"
	_ "encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"time"

	"github.com/VKCOM/statshouse/internal/chutil"
	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/format"
	"github.com/VKCOM/statshouse/internal/vkgo/kittenhouseclient/rowbinary"
)

const V3_PK_COLUMNS_STR = "index_type, metric, pre_tag, pre_stag, time, tag0, stag0, tag1, stag1, tag2, stag2, tag3, stag3, tag4, stag4, tag5, stag5, tag6, stag6, tag7, stag7, tag8, stag8, tag9, stag9, tag10, stag10, tag11, stag11, tag12, stag12, tag13, stag13, tag14, stag14, tag15, stag15, tag16, stag16, tag17, stag17, tag18, stag18, tag19, stag19, tag20, stag20, tag21, stag21, tag22, stag22, tag23, stag23, tag24, stag24, tag25, stag25, tag26, stag26, tag27, stag27, tag28, stag28, tag29, stag29, tag30, stag30, tag31, stag31, tag32, stag32, tag33, stag33, tag34, stag34, tag35, stag35, tag36, stag36, tag37, stag37, tag38, stag38, tag39, stag39, tag40, stag40, tag41, stag41, tag42, stag42, tag43, stag43, tag44, stag44, tag45, stag45, tag46, stag46, tag47, stag47"
const V3_VALUE_COLUMNS_STR = "count, min, max, max_count, sum, sumsquare, min_host, max_host, max_count_host, percentiles, uniq_state"

type MigrationConfigV3 struct {
	SourceTableName string        // Source table name (default: "statshouse_value_dist_1h")
	TargetTableName string        // Destination table name (default: "statshouse_v3_1h")
	StateTableName  string        // Migration state table name
	LogsTableName   string        // Migration logs table name
	StepDuration    time.Duration // Time step for migration (default: time.Hour)
	TotalShards     int           // Total number of shards (default: 16)
	Format          string
}

// v3Row models the subset of columns needed from the V1 schema.
type v3Row struct {
	index_type     uint8
	metric         int32
	pre_tag        uint32
	pre_stag       string
	time           uint32
	tags           [48]int32
	stags          [48]string
	count          float64
	min            float64
	max            float64
	max_count      float64
	sum            float64
	sumsquare      float64
	min_host       data_model.ArgMinStringFloat32
	max_host       data_model.ArgMaxStringFloat32
	max_count_host data_model.ArgMaxStringFloat32
	percentiles    data_model.ChDigest
	uniq_state     data_model.ChUnique
}

func NewDefaultMigrationConfigV3() *MigrationConfigV3 {
	return &MigrationConfigV3{
		SourceTableName: "statshouse_v3_1h",
		TargetTableName: "statshouse_v6_1h",
		StateTableName:  "statshouse_migration_state",
		LogsTableName:   "statshouse_migration_logs",
		StepDuration:    time.Hour,
		TotalShards:     18,
		Format:          "RowBinary",
	}
}

func (a *Aggregator) goMigrateV3(cancelCtx context.Context) {

	if a.replicaKey != 1 {
		log.Printf("[migration_v3] Skipping migration: replica key is %d, expected 1", a.replicaKey)
		return // Only one replica should run migration per shard
	}
	// when shards are added we don't want to automatically migrate data into them
	if a.shardKey > int32(a.migrationConfigV3.TotalShards) {
		log.Printf("[migration_v3] Skipping migration: shard key is %d, expected less than %d", a.shardKey, a.migrationConfig.TotalShards)
		return
	}

	httpClient := makeHTTPClient()

	log.Printf("[migration_v3] Starting migration routine for shard %d", a.shardKey)
	for {
		// Check if we should continue migrating
		select {
		case <-cancelCtx.Done():
			log.Println("[migration_v3] Exiting migration routine (context cancelled)")
			return
		default:
		}

		//Check if migration is enabled (time range must be configured)
		a.configMu.RLock()
		if a.configR.MigrationTimeRange == "" {
			a.configMu.RUnlock()
			log.Println("[migration_v3] Migration disabled: no time range configured")
			time.Sleep(time.Duration(a.configR.MigrationDelaySec) * time.Second)
			continue
		}
		a.configMu.RUnlock()

		a.migrationMu.Lock()
		insertTimeEWMA := a.insertTimeEWMA
		lastErrorTs := a.lastErrorTs
		a.migrationMu.Unlock()

		// Check system load before proceeding
		nowUnix := uint32(time.Now().Unix())
		if lastErrorTs != 0 && nowUnix >= lastErrorTs && nowUnix-lastErrorTs < noErrorsWindow {
			log.Printf("[migration_v3] Skipping: last error was %d seconds ago", nowUnix-lastErrorTs)
			time.Sleep(time.Duration(noErrorsWindow-(nowUnix-lastErrorTs)) * time.Second)
			continue
		}
		if insertTimeEWMA > maxInsertTime {
			log.Printf("[migration_v3] Skipping: EWMA insert time is too high (%.2fs)", insertTimeEWMA)
			a.configMu.RLock()
			delaySec := a.configR.MigrationDelaySec
			a.configMu.RUnlock()
			time.Sleep(time.Duration(delaySec) * time.Second)
			continue
		}

		// Find next timestamp to migrate
		nextTs, err := a.findNextTimestampToMigrateV3(httpClient)

		if err != nil {
			log.Printf("[migration_v3] Error finding next timestamp: %v", err)
			a.configMu.RLock()
			delaySec := a.configR.MigrationDelaySec
			a.configMu.RUnlock()
			time.Sleep(time.Duration(delaySec) * time.Second)
			continue
		}

		if nextTs.IsZero() {
			log.Println("[migration_v3] No more timestamps to migrate, migration complete!")
			return
		}

		log.Printf("[migration_v3] Processing timestamp: %s", nextTs.Format("2006-01-02 15:04:05"))

		// Perform migration for this timestamp
		sourceRowsNum, targetRowsNum, err := a._migrateTimestampWithRetry(httpClient, nextTs)
		if err != nil {
			log.Printf("[migration_v3] Failed to migrate timestamp %s: %v", nextTs.Format("2006-01-02 15:04:05"), err)
			a.configMu.RLock()
			delaySec := a.configR.MigrationDelaySec
			a.configMu.RUnlock()
			time.Sleep(time.Duration(delaySec) * time.Second)
			continue
		}

		log.Printf("[migration_v3] Successfully migrated timestamp %s (source_rows=%d, target_rows=%d)", nextTs.Format("2006-01-02 15:04:05"), sourceRowsNum, targetRowsNum)

		// Small delay before processing next timestamp
		time.Sleep(time.Second)
	}
}

func (a *Aggregator) _migrateTimestampWithRetry(httpClient *http.Client, ts time.Time) (sourceRowsNum, targetRowsNum uint64, err error) {
	var lastErr error
	started := time.Now()
	sourceRowsNum, sourceCountErr := a.countSourceRowsForTs(httpClient, ts)
	if sourceCountErr != nil {
		log.Printf("[migration_v3] Warning: failed to count source rows: %v", sourceCountErr)
		sourceRowsNum = 0
	}
	for attempt := 0; attempt < maxRetryAttempts; attempt++ {
		if attempt > 0 {
			log.Printf("[migration_v3] Retry attempt %d for timestamp %s", attempt+1, ts.Format("2006-01-02 15:04:05"))
			time.Sleep(retryDelay * time.Duration(math.Pow(2, float64(attempt))))
		}
		a.updateMigrationStateV3(httpClient, ts, sourceRowsNum, 0, uint32(attempt), started, nil, migrationSourceV3)

		err = a.migrateSingleStepV3(ts, httpClient)

		if err == nil {
			targetRowsNum, countErr := a.countTargetRows(httpClient, ts)
			if countErr != nil {
				log.Printf("[migration_v3] Warning: failed to count target rows: %v", countErr)
				targetRowsNum = 0
			}
			now := time.Now()
			a.updateMigrationStateV3(httpClient, ts, sourceRowsNum, targetRowsNum, uint32(attempt), started, &now, migrationSourceV3)

			// Write migration log metric
			migrationTags := []int32{0, int32(ts.Unix()), format.TagValueIDMigrationSourceV3}
			a.sh2.AddValueCounterHost(uint32(now.Unix()), format.BuiltinMetricMetaMigrationLog, migrationTags, float64(targetRowsNum), 1, a.aggregatorHostTag)

			return sourceRowsNum, targetRowsNum, nil
		}
		lastErr = err
		log.Printf("[migration_v3] Attempt %d failed for timestamp %s: %v", attempt+1, ts.Format("2006-01-02 15:04:05"), err)
		logQuery := fmt.Sprintf(`
		INSERT INTO %s
		(timestamp, shard_key, ts, retry, message, source)
		VALUES ('%s', %d, toDateTime(%d), %d, '%s', '%s')`, a.migrationConfigV3.LogsTableName, time.Now().Format("2006-01-02 15:04:05"), a.shardKey, ts.Unix(), attempt+1, err.Error(), migrationSourceV3)
		logReq := &chutil.ClickHouseHttpRequest{HttpClient: httpClient, Addr: a.config.KHAddr, User: a.config.KHUser, Password: a.config.KHPassword, Query: logQuery}
		if logResp, logErr := logReq.Execute(context.Background()); logErr == nil {
			logResp.Close()
		}
	}
	a.updateMigrationStateV3(httpClient, ts, sourceRowsNum, 0, uint32(maxRetryAttempts), started, nil, migrationSourceV3)
	return 0, 0, fmt.Errorf("migration failed after %d attempts, last error: %w", maxRetryAttempts, lastErr)
}

func (a *Aggregator) countTargetRows(httpClient *http.Client, ts time.Time) (uint64, error) {
	countQuery := fmt.Sprintf(`
		SELECT count() as cnt
		FROM %s
		WHERE time = toDateTime(%d)`,
		a.migrationConfigV3.TargetTableName, ts.Unix())

	req := &chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       a.config.KHAddr,
		User:       a.config.KHUser,
		Password:   a.config.KHPassword,
		Query:      countQuery,
	}
	resp, err := req.Execute(context.Background())
	if err != nil {
		return 0, fmt.Errorf("failed to count target rows: %w", err)
	}
	defer resp.Close()

	var count uint64
	if _, err := fmt.Fscanf(resp, "%d", &count); err != nil {
		return 0, fmt.Errorf("failed to parse target row count: %w", err)
	}

	return count, nil
}

func (a *Aggregator) updateMigrationStateV3(httpClient *http.Client, ts time.Time, sourceRows, targetRows uint64, retryCount uint32, started time.Time, ended *time.Time, source string) error {
	endedStr := "NULL"
	if ended != nil {
		endedStr = fmt.Sprintf("'%s'", ended.Format("2006-01-02 15:04:05"))
	}

	updateQuery := fmt.Sprintf(`
		INSERT INTO %s
		(shard_key, ts, started, ended, v3_rows, source_rows, retry, source)
		VALUES (%d, toDateTime(%d), '%s', %s, %d, %d, %d, '%s')`,
		a.migrationConfigV3.StateTableName, a.shardKey, ts.Unix(), started.Format("2006-01-02 15:04:05"),
		endedStr, targetRows, sourceRows, retryCount, source)

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

func (a *Aggregator) countSourceRowsForTs(httpClient *http.Client, ts time.Time) (uint64, error) {
	countQuery := fmt.Sprintf(`
		SELECT count() as cnt
		FROM %s
		WHERE time = toDateTime(%d)`,
		a.migrationConfigV3.SourceTableName, ts.Unix())

	req := &chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       a.config.KHAddr,
		User:       a.config.KHUser,
		Password:   a.config.KHPassword,
		Query:      countQuery,
	}
	resp, err := req.Execute(context.Background())
	if err != nil {
		return 0, fmt.Errorf("failed to count source rows: %w", err)
	}
	defer resp.Close()

	var count uint64
	if _, err := fmt.Fscanf(resp, "%d", &count); err != nil {
		return 0, fmt.Errorf("failed to parse source row count: %w", err)
	}

	return count, nil
}

//func (a *Aggregator) findNextTimestampToMigrateV3() (time.Time, error) {
//	return time.Unix(1769086800, 0), nil
//}

func (a *Aggregator) findNextTimestampToMigrateV3(httpClient *http.Client) (time.Time, error) {
	a.configMu.RLock()
	startTs, endTs := a.configR.ParseMigrationTimeRange(a.configR.MigrationTimeRange)
	a.configMu.RUnlock()

	// If no time range configured, migration is disabled
	if startTs == 0 && endTs == 0 {
		return time.Time{}, nil
	}

	log.Printf("[migration_v3] Searching for next timestamp to migrate in range: %d (start) to %d (end)", startTs, endTs)

	latestMigratedQuery := fmt.Sprintf(`
		SELECT toUnixTimestamp(ts) as ts_unix
		FROM %s
		WHERE shard_key = %d AND source = '%s' AND ended IS NOT NULL AND ts_unix <= %d AND ts_unix >= %d
		ORDER BY ts ASC
		LIMIT 1`, a.migrationConfigV3.StateTableName, a.shardKey, migrationSourceV3, startTs, endTs)

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

	nextTs := time.Unix(int64(startTs), 0).Truncate(a.migrationConfigV3.StepDuration)
	if foundResult {
		// Found latest migrated timestamp - start from the previous timestamp before it (backward migration)
		latestTime := time.Unix(latestTs, 0).Truncate(a.migrationConfigV3.StepDuration)
		nextTs = latestTime.Add(-a.migrationConfigV3.StepDuration)
	}
	log.Printf("[migration_v3] Next timestamp to migrate: %s", nextTs.Format("2006-01-02 15:04:05"))

	// Check if we've reached the start of the migration range (migration complete)
	endTime := time.Unix(int64(endTs), 0)
	if nextTs.Before(endTime) {
		log.Printf("[migration_v3] Next timestamp %s is before or at migration end time %s, migration complete",
			nextTs.Format("2006-01-02 15:04:05"), endTime.Format("2006-01-02 15:04:05"))
		return time.Time{}, nil
	}

	return nextTs, nil
}

func (a *Aggregator) migrateSingleStepV3(ts time.Time, httpClient *http.Client) error {
	selectQuery := fmt.Sprintf(`
		SELECT %s, %s
		FROM %s
		WHERE time = toDateTime(%d)`,
		V3_PK_COLUMNS_STR, V3_VALUE_COLUMNS_STR, a.migrationConfigV3.SourceTableName, uint32(ts.Unix()))

	v3DataResp, err := a.executeV3Query(selectQuery, true, nil, httpClient)

	if err != nil {
		return fmt.Errorf("failed to execute select query: %w", err)
	}
	defer v3DataResp.Close()

	log.Printf("[migration_v3] Successfully retrieved shard data, converting and inserting...")

	insertQuery := fmt.Sprintf(`INSERT INTO %s (%s, %s) FORMAT %s`,
		a.migrationConfigV3.TargetTableName,
		V3_PK_COLUMNS_STR,
		V3_VALUE_COLUMNS_STR,
		a.migrationConfigV3.Format,
	)

	pipeReader, pipeWriter := io.Pipe()
	reader := bufio.NewReaderSize(v3DataResp, 8192)

	var rowsConverted int
	var conversionErr error

	go func() {
		defer pipeWriter.Close()
		rowsConverted, conversionErr = a.convertV3Response(reader, pipeWriter)
		if conversionErr != nil {
			log.Printf("[migration_v3] Error during conversion: %v", conversionErr)
			pipeWriter.CloseWithError(conversionErr)
		}
	}()

	bodyBytes, err := io.ReadAll(pipeReader)
	if err != nil {
		return fmt.Errorf("failed to read body: %w", err)
	}

	log.Printf("[migration_v3] Converted %d rows, body size: %d bytes", rowsConverted, len(bodyBytes))

	resp, err := a.executeV3Query(insertQuery, false, bodyBytes, httpClient)

	if err != nil {
		return fmt.Errorf("failed to execute insert query: %w", err)
	}

	defer resp.Close()
	log.Printf("[migration_v3] finished done for ts %d", uint32(ts.Unix()))
	return nil

}

func parseV3Row(reader *bufio.Reader, row *v3Row) error {
	if err := binary.Read(reader, binary.LittleEndian, &row.index_type); err != nil {
		return err
	}
	if err := binary.Read(reader, binary.LittleEndian, &row.metric); err != nil {
		return err
	}
	if err := binary.Read(reader, binary.LittleEndian, &row.pre_tag); err != nil {
		return err
	}
	if err := readString(reader, &row.pre_stag); err != nil {
		return err
	}
	if err := binary.Read(reader, binary.LittleEndian, &row.time); err != nil {
		return err
	}

	for i := 0; i < 48; i++ {
		if err := binary.Read(reader, binary.LittleEndian, &row.tags[i]); err != nil {
			return err
		}
		if err := readString(reader, &row.stags[i]); err != nil {
			return err
		}
	}

	if err := binary.Read(reader, binary.LittleEndian, &row.count); err != nil {
		return err
	}
	if err := binary.Read(reader, binary.LittleEndian, &row.min); err != nil {
		return err
	}
	if err := binary.Read(reader, binary.LittleEndian, &row.max); err != nil {
		return err
	}
	if err := binary.Read(reader, binary.LittleEndian, &row.max_count); err != nil {
		return err
	}
	if err := binary.Read(reader, binary.LittleEndian, &row.sum); err != nil {
		return err
	}
	if err := binary.Read(reader, binary.LittleEndian, &row.sumsquare); err != nil {
		return err
	}

	buf := make([]byte, 6)
	if _, err := row.min_host.ReadFrom(reader, buf); err != nil {
		return err
	}
	buf = make([]byte, 6)
	if _, err := row.max_host.ReadFrom(reader, buf); err != nil {
		return err
	}
	buf = make([]byte, 6)
	if _, err := row.max_count_host.ReadFrom(reader, buf); err != nil {
		return err
	}
	if err := row.percentiles.ReadFrom(reader); err != nil {
		return err
	}
	if err := row.uniq_state.ReadFrom(reader); err != nil {
		return err
	}

	return nil
}

func readString(reader *bufio.Reader, dst *string) error {
	n, err := binary.ReadUvarint(reader)
	if err != nil {
		return err
	}
	buf := make([]byte, n)
	_, err = io.ReadFull(reader, buf)
	if err != nil {
		return err
	}
	*dst = string(buf)
	return nil
}

func (a *Aggregator) executeV3Query(query string, isSelect bool, body []byte, httpClient *http.Client) (io.ReadCloser, error) {
	req := &chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       a.config.KHAddr,
		User:       a.config.KHUser,
		Password:   a.config.KHPassword,
		Query:      query,
		Body:       body,
	}

	if isSelect {
		req.Format = a.migrationConfigV3.Format
	} else {
		req.UrlParams = map[string]string{"input_format_values_interpret_expressions": "0"}
	}
	log.Printf("[migration_v3] Executing query: %s", query)
	response, err := req.Execute(context.Background())
	if err != nil {
		log.Printf("[migration_v3] Error executing query: %v\nquery: %s", err, query)
		return nil, err
	}
	return response, nil
}

func (a *Aggregator) convertV3Response(v3Data io.Reader, output io.Writer) (rowsProcessed int, err error) {
	reader := bufio.NewReaderSize(v3Data, 8192)
	rowData := make([]byte, 0, 4096)
	var v3row v3Row

	for {
		if parseErr := parseV3Row(reader, &v3row); parseErr != nil {
			if errors.Is(parseErr, io.EOF) {
				// End of input, we're done
				log.Printf("[migration_v3] Reached EOF after processing %d rows", rowsProcessed)
				break
			}
			if errors.Is(parseErr, io.ErrUnexpectedEOF) {
				// Incomplete row, but we're using io.Reader so this shouldn't happen
				// unless the reader itself is incomplete
				log.Printf("[migration_v3] Unexpected EOF after processing %d rows: %v", rowsProcessed, parseErr)
				return rowsProcessed, parseErr
			}
			log.Printf("[migration_v3] Parse error after processing %d rows: %v", rowsProcessed, parseErr)
			return rowsProcessed, fmt.Errorf("failed to parse V2 row: %w", parseErr)
		}
		rowData = rowData[:0]
		rowData = encodeV3Row(rowData, &v3row)

		if _, writeErr := output.Write(rowData); writeErr != nil {
			log.Printf("[migration] Write error after processing %d rows: %v", rowsProcessed, writeErr)
			return rowsProcessed, fmt.Errorf("failed to write converted row: %w", writeErr)
		}

		rowsProcessed++
	}
	return rowsProcessed, nil
}

func encodeV3Row(buf []byte, row *v3Row) []byte {
	buf = rowbinary.AppendUint8(buf, row.index_type)
	buf = rowbinary.AppendInt32(buf, row.metric)
	buf = rowbinary.AppendUint32(buf, row.pre_tag)
	buf = rowbinary.AppendString(buf, row.pre_stag)
	buf = rowbinary.AppendDateTime(buf, time.Unix(int64(row.time), 0))

	for i := 0; i < 48; i++ {
		buf = rowbinary.AppendInt32(buf, row.tags[i])
		buf = rowbinary.AppendString(buf, row.stags[i])
	}

	buf = rowbinary.AppendFloat64(buf, row.count)
	buf = rowbinary.AppendFloat64(buf, row.min)
	buf = rowbinary.AppendFloat64(buf, row.max)
	buf = rowbinary.AppendFloat64(buf, row.max_count)
	buf = rowbinary.AppendFloat64(buf, row.sum)
	buf = rowbinary.AppendFloat64(buf, row.sumsquare)

	buf = row.min_host.MarshallAppend(buf)
	buf = row.max_host.MarshallAppend(buf)
	buf = row.max_count_host.MarshallAppend(buf)
	buf = row.percentiles.MarshallAppend(buf, 1)
	buf = row.uniq_state.MarshallAppend(buf)

	return buf
}
