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
		source      = flag.String("source", "v2", "Migration source: v2, v1, or stop")
		timeFlag    = flag.String("time", "", "Time: single timestamp (1754546400) or range (1754539200-1754546400)")
		shardKey    = flag.Int("shard-key", 1, "Shard key (1-16)")
		sourceTable = flag.String("source-table", "", "Source table name (defaults based on source type)")
		v3Table     = flag.String("v3-table", "statshouse_v3_1h", "V3 destination table name")
		step        = flag.String("step", "1h", "Time step duration (e.g., '1s', '1m', '1h')")
		totalShards = flag.Int("total-shards", 16, "Total number of shards")
		sourceHosts = flag.String("source-hosts", "", "Comma-separated list of source hosts (defaults to kh-addr)")
	)
	flag.Parse()

	// Parse step duration
	stepDuration, err := time.ParseDuration(*step)
	if err != nil {
		log.Fatalf("Failed to parse step '%s': %v", *step, err)
	}

	// Set default source table
	if *sourceTable == "" {
		switch *source {
		case "v2":
			*sourceTable = "statshouse_value_1h_dist"
		case "v1":
			*sourceTable = "statshouse_value_dist_1h"
		case "stop":
			*sourceTable = "stats_1h_agg_stop"
		default:
			log.Fatalf("Invalid source type: %s. Must be v2, v1, or stop", *source)
		}
	}

	hosts := parseHosts(*sourceHosts, *khAddr)
	httpClient := &http.Client{Timeout: 30 * time.Second}

	log.Printf("Migration configuration:")
	log.Printf("  Source: %s", *source)
	log.Printf("  ClickHouse: %s", *khAddr)
	log.Printf("  Source Table: %s", *sourceTable)
	log.Printf("  V3 Table: %s", *v3Table)
	log.Printf("  Step Duration: %s", stepDuration)
	log.Printf("  Shard Key: %d", *shardKey)
	log.Printf("  Total Shards: %d", *totalShards)
	log.Printf("  Source Hosts: %s", strings.Join(hosts, ", "))

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

		var sourceRowCount uint64
		var err error

		// Count source rows before migration for verification
		var metricColumn string
		switch *source {
		case "v2":
			metricColumn = "metric"
		case "v1", "stop":
			metricColumn = "stats"
		}
		sourceRowCount, err = countRows(httpClient, *khAddr, *khUser, *khPassword, *sourceTable, ts, int32(*shardKey), *totalShards, metricColumn)
		if err != nil {
			log.Printf("Warning: failed to count source rows for timestamp %d: %v", ts, err)
		} else {
			log.Printf("Source table has %d rows for timestamp %d", sourceRowCount, ts)
		}

		// Perform migration
		switch *source {
		case "v2":
			config := &aggregator.MigrationConfig{
				V2TableName:    *sourceTable,
				V3TableName:    *v3Table,
				StateTableName: "statshouse_migration_state",
				LogsTableName:  "statshouse_migration_logs",
				StepDuration:   stepDuration,
				TotalShards:    *totalShards,
			}
			err = aggregator.TestMigrateSingleStep(*khAddr, *khUser, *khPassword, ts, int32(*shardKey), config)
		case "v1":
			config := aggregator.NewDefaultMigrationConfigV1(hosts, *khUser, *khPassword)
			config.V1TableName = *sourceTable
			config.V3TableName = *v3Table
			config.StateTableName = "statshouse_migration_state"
			config.LogsTableName = "statshouse_migration_logs"
			config.StepDuration = stepDuration
			config.TotalShards = *totalShards
			err = aggregator.TestMigrateSingleStepV1(*khAddr, *khUser, *khPassword, ts, int32(*shardKey), config)
		case "stop":
			config := aggregator.NewDefaultMigrationConfigStop(hosts, *khUser, *khPassword)
			config.StopTableName = *sourceTable
			config.V3TableName = *v3Table
			config.StateTableName = "statshouse_migration_state"
			config.LogsTableName = "statshouse_migration_logs"
			config.StepDuration = stepDuration
			config.TotalShards = *totalShards
			err = aggregator.TestMigrateSingleStepStop(*khAddr, *khUser, *khPassword, ts, int32(*shardKey), config)
		}

		if err != nil {
			log.Printf("Migration failed for timestamp %d: %v", ts, err)
			continue
		}

		// Count migrated rows for verification
		v3RowCount, err := countRows(httpClient, *khAddr, *khUser, *khPassword, *v3Table, ts, int32(*shardKey), *totalShards, "metric")
		if err != nil {
			log.Printf("Warning: failed to count migrated rows for timestamp %d: %v", ts, err)
		} else {
			totalRowsMigrated += v3RowCount
			if sourceRowCount > 0 {
				log.Printf("Successfully migrated timestamp %d: Source=%d rows -> V3=%d rows", ts, sourceRowCount, v3RowCount)
				if sourceRowCount != v3RowCount {
					log.Printf("WARNING: Row count mismatch for timestamp %d! Source=%d, V3=%d", ts, sourceRowCount, v3RowCount)
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

// countRows counts rows in a table for a specific timestamp and shard
func countRows(httpClient *http.Client, khAddr, khUser, khPassword, tableName string, timestamp uint32, shardKey int32, totalShards int, metricColumn string) (uint64, error) {
	countQuery := fmt.Sprintf(`
		SELECT count() as cnt
		FROM %s 
		WHERE time = toDateTime(%d) AND %s %% %d = %d`,
		tableName, timestamp, metricColumn, totalShards, shardKey-1)

	log.Printf("Count query: %s", countQuery)

	req := &chutil.ClickHouseHttpRequest{
		HttpClient: httpClient,
		Addr:       khAddr,
		User:       khUser,
		Password:   khPassword,
		Query:      countQuery,
	}

	resp, err := req.Execute(context.Background())
	if err != nil {
		return 0, fmt.Errorf("failed to count rows: %w", err)
	}
	defer resp.Close()

	var count uint64
	if _, err := fmt.Fscanf(resp, "%d", &count); err != nil {
		return 0, fmt.Errorf("failed to parse row count: %w", err)
	}

	return count, nil
}

func parseHosts(list, fallback string) []string {
	if list == "" {
		return []string{fallback}
	}
	parts := strings.Split(list, ",")
	var out []string
	for _, p := range parts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	if len(out) == 0 {
		return []string{fallback}
	}
	return out
}
