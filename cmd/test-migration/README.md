# Migration Test Tool

This tool tests data migration functionality for StatsHouse, supporting migration from V2, V1, or stop tables to V3 with configurable table names and flexible time steps.

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

# Migrate from V1
./test-migration -source v1 -time 1672531200 -shard-key 1

# Migrate from stop table
./test-migration -source stop -time 1672531200 -shard-key 1

# Migrate a range of timestamps with second-level precision
./test-migration -source v2 -time 1672531200-1672531260 -step 1s -shard-key 1
```

### Command line options

- `-kh-addr`: ClickHouse address (default: `localhost:8123`)
- `-kh-user`: ClickHouse user (default: empty)
- `-kh-password`: ClickHouse password (default: empty)
- `-source`: Migration source type: `v2`, `v1`, or `stop` (default: `v2`)
- `-source-table`: Source table name (defaults based on source type)
  - V2: `statshouse_value_1h_dist`
  - V1: `statshouse_value_dist_1h`
  - Stop: `stats_1h_agg_stop`
- `-source-hosts`: Comma-separated list of source hosts (defaults to `-kh-addr`, used for V1 and stop)
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

# Migrate specific timestamp from V1
./test-migration -source v1 -time 1672531200 -shard-key 2

# Migrate from stop table with custom table names
./test-migration -source stop -time 1672531200 -source-table my_stop_table -v3-table my_v3_table -shard-key 1

# Migrate with second-level precision for a 5-minute window
./test-migration -source v2 -time 1672531200-1672531500 -step 1s -shard-key 1

# Use remote ClickHouse with credentials
./test-migration -source v1 -kh-addr remote.clickhouse.com:8123 -kh-user myuser -kh-password mypass -time 1672531200 -shard-key 1

# Migrate with custom shard configuration (8 shards instead of 16)
./test-migration -source v2 -time 1672531200 -total-shards 8 -shard-key 1

# Migrate from V1 with multiple source hosts
./test-migration -source v1 -source-hosts "host1:8123,host2:8123,host3:8123" -time 1672531200 -shard-key 1
```

## Migration Types

### V2 → V3 Migration

Maps V2 format to V3 format:
- Maps `key0-key15` to `tag0-tag15`, sets `tag16-tag47` to 0
- Maps `skey` to `stag47`, sets all other `stag0-stag46` to empty strings
- Sets `pre_tag=0` and `pre_stag=""` (always empty)
- Converts `min_host`/`max_host` from argMin/argMax Int32 to String format
- Sets `index_type=0`, `max_count=max`, `max_count_host=empty`

### V1 → V3 Migration

Maps V1 format to V3 format:
- Maps `stats` to `metric`
- Maps `key1-key15` to `tag1-tag15`, sets `tag0` and `tag16-tag47` to 0
- All `stag0-stag47` are set to empty strings (no string tags in V1)
- Sets `pre_tag=0` and `pre_stag=""`
- Other aggregates (`uniq_state`, `min_host`, `max_host`) are left empty/zero

### Stop → V3 Migration

Maps `stats_1h_agg_stop` format to V3 format:
- Maps `stats` to `metric`
- Maps `key1` to `tag1`, `key2` to `tag2`
- Maps `skey` to `stag47`
- Maps `count` to `count`
- All other tags (`tag0`, `tag3-tag15`, `tag16-tag46`) are set to 0
- All other string tags (`stag0-stag46`) are set to empty
- All other aggregates (`min`, `max`, `sum`, `sumsquare`, `min_host`, `max_host`, `percentiles`, `uniq_state`) are set to empty/zero

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
- `'v1'` for V1 migrations
- `'stop'` for stop table migrations

