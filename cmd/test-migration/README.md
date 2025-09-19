# Migration Test Tool

This tool tests the V2 to V3 data migration functionality for StatsHouse with configurable table names and flexible time steps.

## Prerequisites

1. ClickHouse server running (tested with local instance)
2. Both V2 and V3 table schemas created (see `localdebug/clickhouse-cluster/scripts/`)

## Usage

### Basic migration test

```bash
# Build the test tool
go build -o test-migration ./cmd/test-migration/

# Migrate existing data for a specific timestamp
./test-migration -time 1672531200 -shard-key 1

# Migrate a range of timestamps with second-level precision
./test-migration -time 1672531200-1672531260 -step 1s -shard-key 1

# Clean V3 table before migration
./test-migration -time 1672531200 -clean-v3 -shard-key 1
```

### Command line options

- `-kh-addr`: ClickHouse address (default: `localhost:8123`)
- `-kh-user`: ClickHouse user (default: empty)
- `-kh-password`: ClickHouse password (default: empty)
- `-time`: Time specification:
  - Single timestamp: `1754546400`
  - Time range: `1754539200-1754546400`
  - If empty, uses current time step
- `-shard-key`: Shard key from 1-16 (default: 1)
- `-v2-table`: V2 source table name (default: `statshouse_value_1h_dist`)
- `-v3-table`: V3 destination table name (default: `statshouse_v3_1h`)
- `-step`: Time step duration, e.g., `1s`, `1m`, `1h` (default: `1h`)
- `-clean-v3`: Clean V3 table before migration (default: false)
- `-total-shards`: Total number of shards (default: 16)

### Examples

```bash
# Migrate current hour for shard 1
./test-migration -shard-key 1

# Migrate specific timestamp
./test-migration -time 1672531200 -shard-key 2

# Migrate with custom table names
./test-migration -time 1672531200 -v2-table my_v2_table -v3-table my_v3_table -shard-key 1

# Migrate with second-level precision for a 5-minute window
./test-migration -time 1672531200-1672531500 -step 1s -shard-key 1

# Use remote ClickHouse with credentials and clean V3 first
./test-migration -kh-addr remote.clickhouse.com:8123 -kh-user myuser -kh-password mypass -time 1672531200 -clean-v3 -shard-key 1

# Migrate with custom shard configuration (8 shards instead of 16)
./test-migration -time 1672531200 -total-shards 8 -shard-key 1
```

## How it works

1. **Configuration**:
   - Creates migration configuration with custom table names and step duration
   - Supports flexible time steps (seconds, minutes, hours)

2. **V3 Table Cleanup** (if `-clean-v3` is specified):
   - Truncates the V3 destination table before migration

3. **Time Window Processing**:
   - Parses time window specification (single timestamp or range)
   - Generates list of timestamps based on step duration
   - Processes each timestamp individually

4. **Migration Process** (for each timestamp):
   - Selects data from V2 table with condition `metric % 16 == (shard_key-1)`
   - Converts V2 format to V3 format in-place:
     - Maps `key0-key15` to `tag0-tag15`, sets `tag16-tag47` to 0
     - Maps `skey` to `stag47`, sets all other `stag0-stag46` to empty strings
     - Sets `pre_tag=0` and `pre_stag=""` (always empty)
     - Converts `min_host`/`max_host` from argMin/argMax Int32 to String format
     - Sets `index_type=0`, `max_count=max`, `max_count_host=empty`
   - Inserts converted data directly into V3 table

5. **Verification**:
   - Counts migrated rows for each timestamp
   - Reports total rows migrated
   - Logs success/failure for each timestamp

## API Functions

The migration functionality is also available as Go functions:

```go
import "github.com/vkcom/statshouse/internal/aggregator"

// Create migration configuration
config := &aggregator.MigrationConfig{
    V2TableName:    "custom_v2_table",
    V3TableName:    "custom_v3_table", 
    StateTableName: "migration_state",
    LogsTableName:  "migration_logs",
    StepDuration:   time.Second, // Use second-level precision
    TotalShards:    8,           // Custom shard count
}

// Migrate single time step with custom config
err := aggregator.TestMigrateSingleStep("localhost:8123", "", "", timestamp, shardKey, config)

// Legacy function for backward compatibility (uses default hour-based config)
err := aggregator.TestMigrateSingleHour("localhost:8123", "", "", timestamp, shardKey)
``` 