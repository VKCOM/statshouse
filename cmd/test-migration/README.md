# Migration Test Tool

This tool tests data migration functionality for StatsHouse, supporting migration from V2 tables to V3 with configurable table names and flexible time steps.

## Prerequisites

1. ClickHouse server running (tested with local instance)
2. Source and V3 table schemas created (see `localdebug/clickhouse-cluster/scripts/`)
3. HTTP access enabled (the tool uses the HTTP protocol just like the production migrator)

## Usage

### Basic migration test

```bash
# Build the test tool
go build -o test-migration ./cmd/test-migration/

# Migrate from V2 (default)
./test-migration -source v2 -time 1672531200 -shard-key 1

# Migrate a range of timestamps with second-level precision
./test-migration -source v2 -time 1672531200-1672531260 -step 1s -shard-key 1
```

### Command line options

- `-kh-addr`: ClickHouse address (default: `localhost:8123`)
- `-kh-user`: ClickHouse user (default: empty)
- `-kh-password`: ClickHouse password (default: empty)
- `-source`: Migration source type: `v2` (default: `v2`)
- `-source-table`: Source table name (defaults based on source type)
  - V2: `statshouse_value_1h_dist`
- `-v3-table`: V3 destination table name (default: `statshouse_v3_1h`)
- `-time`: Time specification:
  - Single timestamp: `1754546400`
  - Time range: `1754539200-1754546400`
  - If empty, uses current time step
- `-shard-key`: Shard key from 1-16 (default: 1)
- `-step`: Time step duration, e.g., `1s`, `1m`, `1h` (default: `1h`)
- `-total-shards`: Total number of shards (default: 16)

### Examples

```bash
# Migrate current hour for shard 1 (V2)
./test-migration -source v2 -shard-key 1

# Migrate with second-level precision for a 5-minute window
./test-migration -source v2 -time 1672531200-1672531500 -step 1s -shard-key 1

# Migrate with custom shard configuration (8 shards instead of 16)
./test-migration -source v2 -time 1672531200 -total-shards 8 -shard-key 1
```

## Migration Types

### V2 → V3 Migration

Maps V2 format to V3 format:
- Maps `key0-key15` to `tag0-tag15`, sets `tag16-tag47` to 0
- Maps `skey` to `stag47`, sets all other `stag0-stag46` to empty strings
- Sets `pre_tag=0` and `pre_stag=""` (always empty)
- Converts `min_host`/`max_host` from argMin/argMax Int32 to String format
- Sets `index_type=0`, `max_count=max`, `max_count_host=empty`

## How it works

1. **Configuration**:
   - Creates migration configuration based on source type
   - Supports flexible time steps (seconds, minutes, hours)

2. **Time Window Processing**:
   - Parses time window specification (single timestamp or range)
   - Generates list of timestamps based on step duration
   - Processes each timestamp individually

3. **Migration Process** (for each timestamp):
   - Counts rows in source table for verification
   - Selects data from source table with appropriate sharding condition
   - Converts source format to V3 format in-place
   - Inserts converted data directly into V3 table

4. **Verification**:
   - Counts migrated rows for each timestamp
   - Reports total rows migrated
   - Logs success/failure for each timestamp
   - Warns if row counts don't match

## State and Log Tables

All migration types use shared state and log tables:
- `statshouse_migration_state` - tracks migration progress
- `statshouse_migration_logs` - logs migration events

The state table includes a `source` column to differentiate between migration types:
- Empty string for V2 migrations

