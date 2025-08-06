# Migration Test Tool

This tool tests the V2 to V3 data migration functionality for StatsHouse.

## Prerequisites

1. ClickHouse server running (tested with local instance)
2. Both V2 and V3 table schemas created (see `localdebug/clickhouse-cluster/scripts/`)

## Usage

### Basic migration test

```bash
# Build the test tool
go build -o test-migration ./cmd/test-migration/

# Test migration with local ClickHouse
./test-migration -create-test -timestamp 1672531200 -shard-key 1
```

### Command line options

- `-kh-addr`: ClickHouse address (default: `localhost:8123`)
- `-kh-user`: ClickHouse user (default: empty)
- `-kh-password`: ClickHouse password (default: empty)
- `-timestamp`: Unix timestamp to migrate, rounded to hour boundary (default: current hour)
- `-shard-key`: Shard key from 1-16 (default: 1)
- `-create-test`: Create test data before migration (default: false)

### Examples

```bash
# Create test data and migrate current hour for shard 1
./test-migration -create-test -shard-key 1

# Migrate specific timestamp without creating test data
./test-migration -timestamp 1672531200 -shard-key 2

# Use remote ClickHouse with credentials
./test-migration -kh-addr remote.clickhouse.com:8123 -kh-user myuser -kh-password mypass -create-test
```

## How it works

1. **Create test data** (if `-create-test` is specified):
   - Inserts sample rows into `statshouse_value_1h` table
   - Creates metrics with different shard keys for testing

2. **Migration process**:
   - Selects data from `statshouse_value_1h_dist` with condition `metric % 16 == (shard_key-1)`
   - Converts V2 format to V3 format in-place:
     - Maps `key0-key15` to `tag0-tag15`, sets `tag16-tag47` to 0
     - Maps `skey` to `stag47`, sets all other `stag0-stag46` to empty strings
     - Sets `pre_tag=0` and `pre_stag=""` (always empty)
     - Converts `min_host`/`max_host` from argMin/argMax Int32 to String format
     - Sets `index_type=0`, `max_count=max`, `max_count_host=empty`
   - Inserts converted data directly into `statshouse_v3_1h`

3. **Verification**:
   - Check logs for successful conversion
   - Query both tables to verify data consistency

## API Functions

The migration functionality is also available as Go functions:

```go
import "github.com/vkcom/statshouse/internal/aggregator"

// Create test data in V2 table
err := aggregator.CreateTestDataV2("localhost:8123", "", "", timestamp)

// Migrate single hour from V2 to V3
err := aggregator.TestMigrateSingleHour("localhost:8123", "", "", timestamp, shardKey)
``` 