CREATE TABLE IF NOT EXISTS statshouse_internal_log ON CLUSTER statlogs2
(`time` DateTime,
 `host` String,
 `type` String,
 `key0` String,
 `key1` String,
 `key2` String,
 `key3` String,
 `key4` String,
 `key5` String,
 `message` String)
ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/{table}', '{replica}')
PARTITION BY toYYYYMM(time)
ORDER BY (time, host, type, key0, key1, key2, key3, key4, key5)
TTL time + toIntervalMonth(3)
SETTINGS ttl_only_drop_parts = 1, index_granularity = 8192;

CREATE TABLE IF NOT EXISTS statshouse_internal_log_buffer ON CLUSTER statlogs2
(`time` DateTime,
 `host` String,
 `type` String,
 `key0` String,
 `key1` String,
 `key2` String,
 `key3` String,
 `key4` String,
 `key5` String,
 `message` String)
ENGINE = Buffer('default', 'statshouse_internal_log', 2, 120, 120, 10000000, 10000000, 100000000, 100000000);

CREATE TABLE IF NOT EXISTS statshouse_internal_log_dist ON CLUSTER statlogs2
(`time` DateTime,
 `host` String,
 `type` String,
 `key0` String,
 `key1` String,
 `key2` String,
 `key3` String,
 `key4` String,
 `key5` String,
 `message` String)
ENGINE = Distributed('statlogs2', 'default', 'statshouse_internal_log');
