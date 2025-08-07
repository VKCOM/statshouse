package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/VKCOM/statshouse/internal/aggregator"
	"github.com/VKCOM/statshouse/internal/chutil"
)

func main() {
	var (
		khAddr      = flag.String("kh-addr", "localhost:8123", "ClickHouse address")
		khUser      = flag.String("kh-user", "", "ClickHouse user")
		khPassword  = flag.String("kh-password", "", "ClickHouse password")
		timeFlag    = flag.String("time", "", "Time: single timestamp (1754546400) or range (1754539200-1754546400)")
		shardKey    = flag.Int("shard-key", 1, "Shard key (1-16)")
		v2Table     = flag.String("v2-table", "statshouse_value_1h_dist", "V2 source table name")
		v3Table     = flag.String("v3-table", "statshouse_v3_1h", "V3 destination table name")
		step        = flag.String("step", "1h", "Time step duration (e.g., '1s', '1m', '1h')")
		totalShards = flag.Int("total-shards", 16, "Total number of shards")
	)
	flag.Parse()

	// Parse step duration
	stepDuration, err := time.ParseDuration(*step)
	if err != nil {
		log.Fatalf("Failed to parse step '%s': %v", *step, err)
	}

	// Create migration config
	config := &aggregator.MigrationConfig{
		V2TableName:    *v2Table,
		V3TableName:    *v3Table,
		StateTableName: "migration_state",
		LogsTableName:  "migration_logs",
		StepDuration:   stepDuration,
		TotalShards:    *totalShards,
	}

	log.Printf("Migration configuration:")
	log.Printf("  ClickHouse: %s", *khAddr)
	log.Printf("  V2 Table: %s", config.V2TableName)
	log.Printf("  V3 Table: %s", config.V3TableName)
	log.Printf("  Step Duration: %s", stepDuration)
	log.Printf("  Shard Key: %d", *shardKey)
	log.Printf("  Total Shards: %d", config.TotalShards)

	// Parse time window
	timestamps, err := parseTimeWindow(*timeFlag, stepDuration)
	if err != nil {
		log.Fatalf("Failed to parse time: %v", err)
	}

	if len(timestamps) == 0 {
		log.Fatal("No timestamps specified. Use --time flag with either a single timestamp or range")
	}

	log.Printf("Migrating %d time steps", len(timestamps))

	// Migrate each timestamp
	totalRowsMigrated := uint64(0)
	for i, ts := range timestamps {
		log.Printf("[%d/%d] Migrating timestamp %d (%s)",
			i+1, len(timestamps), ts, time.Unix(int64(ts), 0).Format("2006-01-02 15:04:05"))

		// Count V2 rows before migration for verification
		v2RowCount, err := countV2Rows(*khAddr, *khUser, *khPassword, config.V2TableName, ts, int32(*shardKey), config.TotalShards)
		if err != nil {
			log.Printf("Warning: failed to count V2 rows for timestamp %d: %v", ts, err)
		} else {
			log.Printf("V2 table has %d rows for timestamp %d", v2RowCount, ts)
		}

		err = aggregator.TestMigrateSingleStep(*khAddr, *khUser, *khPassword, ts, int32(*shardKey), config)
		if err != nil {
			log.Printf("Migration failed for timestamp %d: %v", ts, err)
			continue
		}

		// Count migrated rows for verification
		v3RowCount, err := countMigratedRows(*khAddr, *khUser, *khPassword, config.V3TableName, ts, int32(*shardKey), config.TotalShards)
		if err != nil {
			log.Printf("Warning: failed to count migrated rows for timestamp %d: %v", ts, err)
		} else {
			totalRowsMigrated += v3RowCount
			if v2RowCount > 0 {
				log.Printf("Successfully migrated timestamp %d: V2=%d rows -> V3=%d rows", ts, v2RowCount, v3RowCount)
				if v2RowCount != v3RowCount {
					log.Printf("WARNING: Row count mismatch for timestamp %d! V2=%d, V3=%d", ts, v2RowCount, v3RowCount)
				}
			} else {
				log.Printf("Successfully processed timestamp %d (%d rows in V3)", ts, v3RowCount)
			}
		}
	}

	log.Printf("Migration completed! Total rows migrated: %d", totalRowsMigrated)
}

// parseTimeWindow parses time window specification
func parseTimeWindow(window string, stepDuration time.Duration) ([]uint32, error) {
	if window == "" {
		// Use current time step rounded down
		now := time.Now()
		currentStep := now.Truncate(stepDuration)
		return []uint32{uint32(currentStep.Unix())}, nil
	}

	if strings.Contains(window, "-") {
		// Range format: start-end
		parts := strings.Split(window, "-")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid time range format. Expected: start-end")
		}

		start, err := strconv.ParseUint(parts[0], 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid start timestamp: %v", err)
		}

		end, err := strconv.ParseUint(parts[1], 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid end timestamp: %v", err)
		}

		if start >= end {
			return nil, fmt.Errorf("start timestamp must be less than end timestamp")
		}

		// Generate timestamps for the range
		var timestamps []uint32
		for ts := start; ts <= end; ts += uint64(stepDuration.Seconds()) {
			timestamps = append(timestamps, uint32(ts))
		}

		return timestamps, nil
	} else {
		// Single timestamp
		ts, err := strconv.ParseUint(window, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid timestamp: %v", err)
		}
		return []uint32{uint32(ts)}, nil
	}
}

// countV2Rows counts rows in V2 table for a specific timestamp and shard
func countV2Rows(khAddr, khUser, khPassword, tableName string, timestamp uint32, shardKey int32, totalShards int) (uint64, error) {
	httpClient := &http.Client{Timeout: 30 * time.Second}

	countQuery := fmt.Sprintf(`
		SELECT count() as cnt
		FROM %s 
		WHERE time = toDateTime(%d) AND metric %% %d = %d`,
		tableName, timestamp, totalShards, shardKey-1)

	log.Printf("V2 count query: %s", countQuery)

	req := &chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       khAddr,
		User:       khUser,
		Password:   khPassword,
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

// countMigratedRows counts how many rows were migrated for verification
func countMigratedRows(khAddr, khUser, khPassword, tableName string, timestamp uint32, shardKey int32, totalShards int) (uint64, error) {
	httpClient := &http.Client{Timeout: 30 * time.Second}

	countQuery := fmt.Sprintf(`
		SELECT count() as cnt
		FROM %s 
		WHERE time = toDateTime(%d) AND metric %% %d = %d`,
		tableName, timestamp, totalShards, shardKey-1)

	log.Printf("V3 count query: %s", countQuery)

	req := &chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       khAddr,
		User:       khUser,
		Password:   khPassword,
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
