CREATE TABLE IF NOT EXISTS statshouse_migration_state (
    shard_key Int32,
    ts DateTime,
    started DateTime,
    ended Nullable(DateTime),
    v2_rows UInt64,
    v3_rows UInt64,
    retry UInt32
) ENGINE = ReplacingMergeTree(retry)
ORDER BY (shard_key, ts, started);


CREATE TABLE IF NOT EXISTS statshouse_migration_logs (
    timestamp DateTime,
    shard_key Int32,
    ts DateTime,
    retry UInt32,
    message String
) ENGINE = MergeTree()
ORDER BY (timestamp, shard_key, ts, retry);

ALTER TABLE statshouse_migration_state
    ADD COLUMN IF NOT EXISTS source_rows UInt64,
    ADD COLUMN IF NOT EXISTS source String DEFAULT '';

ALTER TABLE statshouse_migration_logs
    ADD COLUMN IF NOT EXISTS source String DEFAULT '';
