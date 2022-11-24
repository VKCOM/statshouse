CREATE TABLE IF NOT EXISTS statshouse_contributors_log ON CLUSTER statlogs2
(`insert_time` DateTime,
 `time` DateTime,
 `count` SimpleAggregateFunction(sum, Float64))
ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/{table}', '{replica}')
PARTITION BY toStartOfInterval(insert_time, toIntervalHour(6))
ORDER BY (insert_time, time)
TTL insert_time + toIntervalHour(52)
SETTINGS storage_policy = 'ssd_then_hdd', ttl_only_drop_parts = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS statshouse_contributors_log_agg TO statshouse_contributors_log_buffer ON CLUSTER statlogs2
(`insert_time` DateTime,
 `time` DateTime,
 `count` SimpleAggregateFunction(sum, Float64))
AS SELECT now() AS insert_time, time, count
FROM statshouse_value_incoming
WHERE (toDate(time) >= (today() - 3)) AND (toDate(time) <= (today() + 1)) AND (metric = -2);

CREATE MATERIALIZED VIEW IF NOT EXISTS statshouse_contributors_log_agg2 TO statshouse_contributors_log_buffer ON CLUSTER statlogs2
(`insert_time` DateTime,
 `time` DateTime,
 `count` SimpleAggregateFunction(sum, Float64))
AS SELECT now() AS insert_time, time, count
FROM statshouse_value_incoming_prekey
WHERE (toDate(time) >= (today() - 3)) AND (toDate(time) <= (today() + 1)) AND (metric = -2);

CREATE TABLE IF NOT EXISTS statshouse_contributors_log_buffer ON CLUSTER statlogs2
(`insert_time` DateTime,
 `time` DateTime,
 `count` SimpleAggregateFunction(sum, Float64))
ENGINE = Buffer('default', 'statshouse_contributors_log', 4, 2, 2, 1000000000, 1000000000, 1000000, 1000000);

CREATE TABLE IF NOT EXISTS statshouse_contributors_log_dist ON CLUSTER statlogs2
(`insert_time` DateTime,
 `time` DateTime,
 `count` SimpleAggregateFunction(sum, Float64))
ENGINE = Distributed('statlogs_video', 'default', 'statshouse_contributors_log');

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
ENGINE = Distributed('statlogs_video', 'default', 'statshouse_internal_log');

CREATE TABLE IF NOT EXISTS statshouse_metric_1d ON CLUSTER statlogs2
(`metric` Int32,
 `time` DateTime,
 `row_count` SimpleAggregateFunction(sum, Float64),
 `count` SimpleAggregateFunction(sum, Float64))
ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/{table}', '{replica}')
ORDER BY (metric, time)
SETTINGS storage_policy = 'ssd_then_hdd', index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS statshouse_metric_1d_agg TO statshouse_metric_1d_buffer ON CLUSTER statlogs2
(`metric` Int32,
 `time` DateTime,
 `row_count` UInt8,
 `count` SimpleAggregateFunction(sum, Float64))
AS SELECT metric, toStartOfInterval(time, toIntervalDay(1)) AS time, 1 AS row_count, count
FROM statshouse_value_incoming
WHERE (toDate(time) >= (today() - 3)) AND (toDate(time) <= (today() + 1));

CREATE MATERIALIZED VIEW IF NOT EXISTS statshouse_metric_1d_agg2 TO statshouse_metric_1d_buffer ON CLUSTER statlogs2
(`metric` Int32,
 `time` DateTime,
 `row_count` UInt8,
 `count` SimpleAggregateFunction(sum, Float64))
AS SELECT metric, toStartOfInterval(time, toIntervalDay(1)) AS time, 1 AS row_count, count
FROM statshouse_value_incoming_prekey
WHERE (toDate(time) >= (today() - 3)) AND (toDate(time) <= (today() + 1));

CREATE MATERIALIZED VIEW IF NOT EXISTS statshouse_metric_1d_agg2_move TO statshouse_metric_1d_buffer ON CLUSTER statlogs2
(`metric` Int32,
 `time` DateTime,
 `row_count` UInt8,
 `count` SimpleAggregateFunction(sum, Float64))
AS SELECT metric, toStartOfInterval(time, toIntervalDay(1)) AS time, 1 AS row_count, count
FROM statshouse_value_incoming_prekey_move;

CREATE TABLE IF NOT EXISTS statshouse_metric_1d_buffer ON CLUSTER statlogs2
(`metric` Int32,
 `time` DateTime,
 `row_count` SimpleAggregateFunction(sum, Float64),
 `count` SimpleAggregateFunction(sum, Float64))
ENGINE = Buffer('default', 'statshouse_metric_1d', 4, 60, 60, 1000000000, 1000000000, 100000000, 100000000);

CREATE TABLE IF NOT EXISTS statshouse_metric_1d_dist ON CLUSTER statlogs2
(`metric` Int32,
 `time` DateTime,
 `row_count` SimpleAggregateFunction(sum, Float64),
 `count` SimpleAggregateFunction(sum, Float64))
ENGINE = Distributed('statlogs_video', 'default', 'statshouse_metric_1d');

CREATE TABLE IF NOT EXISTS statshouse_value_1h ON CLUSTER statlogs2
(`metric` Int32,
 `time` DateTime,
 `key0` Int32,
 `key1` Int32,
 `key2` Int32,
 `key3` Int32,
 `key4` Int32,
 `key5` Int32,
 `key6` Int32,
 `key7` Int32,
 `key8` Int32,
 `key9` Int32,
 `key10` Int32,
 `key11` Int32,
 `key12` Int32,
 `key13` Int32,
 `key14` Int32,
 `key15` Int32,
 `skey` String,
 `count` SimpleAggregateFunction(sum, Float64),
 `min` SimpleAggregateFunction(min, Float64),
 `max` SimpleAggregateFunction(max, Float64),
 `sum` SimpleAggregateFunction(sum, Float64),
 `sumsquare` SimpleAggregateFunction(sum, Float64),
 `percentiles` AggregateFunction(quantilesTDigest(0.5), Float32),
 `uniq_state` AggregateFunction(uniq, Int64),
 `min_host` AggregateFunction(argMin, Int32, Float32),
 `max_host` AggregateFunction(argMax, Int32, Float32))
ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/{table}', '{replica}')
PARTITION BY toYYYYMM(time)
ORDER BY (metric, time, key0, key1, key2, key3, key4, key5, key6, key7, key8, key9, key10, key11, key12, key13, key14, key15, skey)
TTL time + toIntervalDay(4) TO DISK 'default'
SETTINGS storage_policy = 'ssd_then_hdd', index_granularity = 8192, ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS statshouse_value_1h_agg TO statshouse_value_1h ON CLUSTER statlogs2
(`metric` Int32,
 `time` DateTime,
 `key0` Int32,
 `key1` Int32,
 `key2` Int32,
 `key3` Int32,
 `key4` Int32,
 `key5` Int32,
 `key6` Int32,
 `key7` Int32,
 `key8` Int32,
 `key9` Int32,
 `key10` Int32,
 `key11` Int32,
 `key12` Int32,
 `key13` Int32,
 `key14` Int32,
 `key15` Int32,
 `skey` String,
 `count` SimpleAggregateFunction(sum, Float64),
 `min` SimpleAggregateFunction(min, Float64),
 `max` SimpleAggregateFunction(max, Float64),
 `sum` SimpleAggregateFunction(sum, Float64),
 `sumsquare` SimpleAggregateFunction(sum, Float64),
 `percentiles` AggregateFunction(quantilesTDigest(0.5), Float32),
 `uniq_state` AggregateFunction(uniq, Int64),
 `min_host` AggregateFunction(argMin, Int32, Float32),
 `max_host` AggregateFunction(argMax, Int32, Float32))
AS SELECT metric, toStartOfInterval(time, toIntervalHour(1)) AS time, key0, key1, key2, key3, key4, key5, key6, key7, key8, key9, key10, key11, key12, key13, key14, key15, skey, count, min, max, sum, sumsquare, percentiles, uniq_state, min_host, max_host
FROM statshouse_value_incoming
WHERE (toDate(time) >= (today() - 3)) AND (toDate(time) <= (today() + 1));

CREATE MATERIALIZED VIEW IF NOT EXISTS statshouse_value_1h_agg2 TO statshouse_value_1h ON CLUSTER statlogs2
(`metric` Int32,
 `time` DateTime,
 `key0` Int32,
 `key1` Int32,
 `key2` Int32,
 `key3` Int32,
 `key4` Int32,
 `key5` Int32,
 `key6` Int32,
 `key7` Int32,
 `key8` Int32,
 `key9` Int32,
 `key10` Int32,
 `key11` Int32,
 `key12` Int32,
 `key13` Int32,
 `key14` Int32,
 `key15` Int32,
 `skey` String,
 `count` SimpleAggregateFunction(sum, Float64),
 `min` SimpleAggregateFunction(min, Float64),
 `max` SimpleAggregateFunction(max, Float64),
 `sum` SimpleAggregateFunction(sum, Float64),
 `sumsquare` SimpleAggregateFunction(sum, Float64),
 `percentiles` AggregateFunction(quantilesTDigest(0.5), Float32),
 `uniq_state` AggregateFunction(uniq, Int64),
 `min_host` AggregateFunction(argMin, Int32, Float32),
 `max_host` AggregateFunction(argMax, Int32, Float32))
AS SELECT metric, toStartOfInterval(time, toIntervalHour(1)) AS time, key0, key1, key2, key3, key4, key5, key6, key7, key8, key9, key10, key11, key12, key13, key14, key15, skey, count, min, max, sum, sumsquare, percentiles, uniq_state, min_host, max_host
FROM statshouse_value_incoming_prekey
WHERE (toDate(time) >= (today() - 3)) AND (toDate(time) <= (today() + 1));

CREATE MATERIALIZED VIEW IF NOT EXISTS statshouse_value_1h_agg2_move TO statshouse_value_1h ON CLUSTER statlogs2
(`metric` Int32,
 `time` DateTime,
 `key0` Int32,
 `key1` Int32,
 `key2` Int32,
 `key3` Int32,
 `key4` Int32,
 `key5` Int32,
 `key6` Int32,
 `key7` Int32,
 `key8` Int32,
 `key9` Int32,
 `key10` Int32,
 `key11` Int32,
 `key12` Int32,
 `key13` Int32,
 `key14` Int32,
 `key15` Int32,
 `skey` String,
 `count` SimpleAggregateFunction(sum, Float64),
 `min` SimpleAggregateFunction(min, Float64),
 `max` SimpleAggregateFunction(max, Float64),
 `sum` SimpleAggregateFunction(sum, Float64),
 `sumsquare` SimpleAggregateFunction(sum, Float64),
 `percentiles` AggregateFunction(quantilesTDigest(0.5), Float32),
 `uniq_state` AggregateFunction(uniq, Int64),
 `min_host` AggregateFunction(argMin, Int32, Float32),
 `max_host` AggregateFunction(argMax, Int32, Float32))
AS SELECT metric, toStartOfInterval(time, toIntervalHour(1)) AS time, key0, key1, key2, key3, key4, key5, key6, key7, key8, key9, key10, key11, key12, key13, key14, key15, skey, count, min, max, sum, sumsquare, percentiles, uniq_state, min_host, max_host
FROM statshouse_value_incoming_prekey_move;

CREATE MATERIALIZED VIEW IF NOT EXISTS statshouse_value_1h_agg_prekey TO statshouse_value_1h_prekey ON CLUSTER statlogs2
(`metric` Int32,
 `prekey` Int32,
 `prekey_set` UInt8,
 `time` DateTime,
 `key0` Int32,
 `key1` Int32,
 `key2` Int32,
 `key3` Int32,
 `key4` Int32,
 `key5` Int32,
 `key6` Int32,
 `key7` Int32,
 `key8` Int32,
 `key9` Int32,
 `key10` Int32,
 `key11` Int32,
 `key12` Int32,
 `key13` Int32,
 `key14` Int32,
 `key15` Int32,
 `skey` String,
 `count` SimpleAggregateFunction(sum, Float64),
 `min` SimpleAggregateFunction(min, Float64),
 `max` SimpleAggregateFunction(max, Float64),
 `sum` SimpleAggregateFunction(sum, Float64),
 `sumsquare` SimpleAggregateFunction(sum, Float64),
 `percentiles` AggregateFunction(quantilesTDigest(0.5), Float32),
 `uniq_state` AggregateFunction(uniq, Int64),
 `min_host` AggregateFunction(argMin, Int32, Float32),
 `max_host` AggregateFunction(argMax, Int32, Float32))
AS SELECT metric, prekey, prekey_set, toStartOfInterval(time, toIntervalHour(1)) AS time, key0, key1, key2, key3, key4, key5, key6, key7, key8, key9, key10, key11, key12, key13, key14, key15, skey, count, min, max, sum, sumsquare, percentiles, uniq_state, min_host, max_host
FROM statshouse_value_incoming_prekey
WHERE (toDate(time) >= (today() - 3)) AND (toDate(time) <= (today() + 1)) AND (prekey_set = 1);

CREATE MATERIALIZED VIEW IF NOT EXISTS statshouse_value_1h_agg_prekey_move TO statshouse_value_1h_prekey ON CLUSTER statlogs2
(`metric` Int32,
 `prekey` Int32,
 `prekey_set` UInt8,
 `time` DateTime,
 `key0` Int32,
 `key1` Int32,
 `key2` Int32,
 `key3` Int32,
 `key4` Int32,
 `key5` Int32,
 `key6` Int32,
 `key7` Int32,
 `key8` Int32,
 `key9` Int32,
 `key10` Int32,
 `key11` Int32,
 `key12` Int32,
 `key13` Int32,
 `key14` Int32,
 `key15` Int32,
 `skey` String,
 `count` SimpleAggregateFunction(sum, Float64),
 `min` SimpleAggregateFunction(min, Float64),
 `max` SimpleAggregateFunction(max, Float64),
 `sum` SimpleAggregateFunction(sum, Float64),
 `sumsquare` SimpleAggregateFunction(sum, Float64),
 `percentiles` AggregateFunction(quantilesTDigest(0.5), Float32),
 `uniq_state` AggregateFunction(uniq, Int64),
 `min_host` AggregateFunction(argMin, Int32, Float32),
 `max_host` AggregateFunction(argMax, Int32, Float32))
AS SELECT metric, prekey, prekey_set, toStartOfInterval(time, toIntervalHour(1)) AS time, key0, key1, key2, key3, key4, key5, key6, key7, key8, key9, key10, key11, key12, key13, key14, key15, skey, count, min, max, sum, sumsquare, percentiles, uniq_state, min_host, max_host
FROM statshouse_value_incoming_prekey_move
WHERE prekey_set = 1;

CREATE TABLE IF NOT EXISTS statshouse_value_1h_dist ON CLUSTER statlogs2
(`metric` Int32,
 `time` DateTime,
 `key0` Int32,
 `key1` Int32,
 `key2` Int32,
 `key3` Int32,
 `key4` Int32,
 `key5` Int32,
 `key6` Int32,
 `key7` Int32,
 `key8` Int32,
 `key9` Int32,
 `key10` Int32,
 `key11` Int32,
 `key12` Int32,
 `key13` Int32,
 `key14` Int32,
 `key15` Int32,
 `skey` String,
 `count` SimpleAggregateFunction(sum, Float64),
 `min` SimpleAggregateFunction(min, Float64),
 `max` SimpleAggregateFunction(max, Float64),
 `sum` SimpleAggregateFunction(sum, Float64),
 `sumsquare` SimpleAggregateFunction(sum, Float64),
 `percentiles` AggregateFunction(quantilesTDigest(0.5), Float32),
 `uniq_state` AggregateFunction(uniq, Int64),
 `min_host` AggregateFunction(argMin, Int32, Float32),
 `max_host` AggregateFunction(argMax, Int32, Float32))
ENGINE = Distributed('statlogs_video', 'default', 'statshouse_value_1h');

CREATE TABLE IF NOT EXISTS statshouse_value_1h_prekey ON CLUSTER statlogs2
(`metric` Int32,
 `prekey` Int32,
 `time` DateTime,
 `key0` Int32,
 `key1` Int32,
 `key2` Int32,
 `key3` Int32,
 `key4` Int32,
 `key5` Int32,
 `key6` Int32,
 `key7` Int32,
 `key8` Int32,
 `key9` Int32,
 `key10` Int32,
 `key11` Int32,
 `key12` Int32,
 `key13` Int32,
 `key14` Int32,
 `key15` Int32,
 `skey` String,
 `count` SimpleAggregateFunction(sum, Float64),
 `min` SimpleAggregateFunction(min, Float64),
 `max` SimpleAggregateFunction(max, Float64),
 `sum` SimpleAggregateFunction(sum, Float64),
 `sumsquare` SimpleAggregateFunction(sum, Float64),
 `percentiles` AggregateFunction(quantilesTDigest(0.5), Float32),
 `uniq_state` AggregateFunction(uniq, Int64),
 `min_host` AggregateFunction(argMin, Int32, Float32),
 `max_host` AggregateFunction(argMax, Int32, Float32))
ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/{table}', '{replica}')
PARTITION BY toYYYYMM(time)
ORDER BY (metric, prekey, time, key0, key1, key2, key3, key4, key5, key6, key7, key8, key9, key10, key11, key12, key13, key14, key15, skey)
TTL time + toIntervalDay(4) TO DISK 'default'
SETTINGS storage_policy = 'ssd_then_hdd', ttl_only_drop_parts = 1, index_granularity = 8192;

CREATE TABLE IF NOT EXISTS statshouse_value_1h_prekey_dist ON CLUSTER statlogs2
(`metric` Int32,
 `prekey` Int32,
 `time` DateTime,
 `key0` Int32,
 `key1` Int32,
 `key2` Int32,
 `key3` Int32,
 `key4` Int32,
 `key5` Int32,
 `key6` Int32,
 `key7` Int32,
 `key8` Int32,
 `key9` Int32,
 `key10` Int32,
 `key11` Int32,
 `key12` Int32,
 `key13` Int32,
 `key14` Int32,
 `key15` Int32,
 `skey` String,
 `count` SimpleAggregateFunction(sum, Float64),
 `min` SimpleAggregateFunction(min, Float64),
 `max` SimpleAggregateFunction(max, Float64),
 `sum` SimpleAggregateFunction(sum, Float64),
 `sumsquare` SimpleAggregateFunction(sum, Float64),
 `percentiles` AggregateFunction(quantilesTDigest(0.5), Float32),
 `uniq_state` AggregateFunction(uniq, Int64),
 `min_host` AggregateFunction(argMin, Int32, Float32),
 `max_host` AggregateFunction(argMax, Int32, Float32))
ENGINE = Distributed('statlogs_video', 'default', 'statshouse_value_1h_prekey');

CREATE TABLE IF NOT EXISTS statshouse_value_1m ON CLUSTER statlogs2
(`metric` Int32,
 `time` DateTime,
 `key0` Int32,
 `key1` Int32,
 `key2` Int32,
 `key3` Int32,
 `key4` Int32,
 `key5` Int32,
 `key6` Int32,
 `key7` Int32,
 `key8` Int32,
 `key9` Int32,
 `key10` Int32,
 `key11` Int32,
 `key12` Int32,
 `key13` Int32,
 `key14` Int32,
 `key15` Int32,
 `skey` String,
 `count` SimpleAggregateFunction(sum, Float64),
 `min` SimpleAggregateFunction(min, Float64),
 `max` SimpleAggregateFunction(max, Float64),
 `sum` SimpleAggregateFunction(sum, Float64),
 `sumsquare` SimpleAggregateFunction(sum, Float64),
 `percentiles` AggregateFunction(quantilesTDigest(0.5), Float32),
 `uniq_state` AggregateFunction(uniq, Int64),
 `min_host` AggregateFunction(argMin, Int32, Float32),
 `max_host` AggregateFunction(argMax, Int32, Float32))
ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/{table}', '{replica}')
PARTITION BY toDate(time)
ORDER BY (metric, time, key0, key1, key2, key3, key4, key5, key6, key7, key8, key9, key10, key11, key12, key13, key14, key15, skey)
TTL time + toIntervalDay(4) TO DISK 'default', time + toIntervalDay(33)
SETTINGS storage_policy = 'ssd_then_hdd', ttl_only_drop_parts = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS statshouse_value_1m_agg TO statshouse_value_1m ON CLUSTER statlogs2
(`metric` Int32,
 `time` DateTime,
 `key0` Int32,
 `key1` Int32,
 `key2` Int32,
 `key3` Int32,
 `key4` Int32,
 `key5` Int32,
 `key6` Int32,
 `key7` Int32,
 `key8` Int32,
 `key9` Int32,
 `key10` Int32,
 `key11` Int32,
 `key12` Int32,
 `key13` Int32,
 `key14` Int32,
 `key15` Int32,
 `skey` String,
 `count` SimpleAggregateFunction(sum, Float64),
 `min` SimpleAggregateFunction(min, Float64),
 `max` SimpleAggregateFunction(max, Float64),
 `sum` SimpleAggregateFunction(sum, Float64),
 `sumsquare` SimpleAggregateFunction(sum, Float64),
 `percentiles` AggregateFunction(quantilesTDigest(0.5), Float32),
 `uniq_state` AggregateFunction(uniq, Int64),
 `min_host` AggregateFunction(argMin, Int32, Float32),
 `max_host` AggregateFunction(argMax, Int32, Float32))
AS SELECT metric, toStartOfInterval(time, toIntervalMinute(1)) AS time, key0, key1, key2, key3, key4, key5, key6, key7, key8, key9, key10, key11, key12, key13, key14, key15, skey, count, min, max, sum, sumsquare, percentiles, uniq_state, min_host, max_host
FROM statshouse_value_incoming
WHERE (toDate(time) >= (today() - 3)) AND (toDate(time) <= (today() + 1));

CREATE MATERIALIZED VIEW IF NOT EXISTS statshouse_value_1m_agg2 TO statshouse_value_1m ON CLUSTER statlogs2
(`metric` Int32,
 `time` DateTime,
 `key0` Int32,
 `key1` Int32,
 `key2` Int32,
 `key3` Int32,
 `key4` Int32,
 `key5` Int32,
 `key6` Int32,
 `key7` Int32,
 `key8` Int32,
 `key9` Int32,
 `key10` Int32,
 `key11` Int32,
 `key12` Int32,
 `key13` Int32,
 `key14` Int32,
 `key15` Int32,
 `skey` String,
 `count` SimpleAggregateFunction(sum, Float64),
 `min` SimpleAggregateFunction(min, Float64),
 `max` SimpleAggregateFunction(max, Float64),
 `sum` SimpleAggregateFunction(sum, Float64),
 `sumsquare` SimpleAggregateFunction(sum, Float64),
 `percentiles` AggregateFunction(quantilesTDigest(0.5), Float32),
 `uniq_state` AggregateFunction(uniq, Int64),
 `min_host` AggregateFunction(argMin, Int32, Float32),
 `max_host` AggregateFunction(argMax, Int32, Float32))
AS SELECT metric, toStartOfInterval(time, toIntervalMinute(1)) AS time, key0, key1, key2, key3, key4, key5, key6, key7, key8, key9, key10, key11, key12, key13, key14, key15, skey, count, min, max, sum, sumsquare, percentiles, uniq_state, min_host, max_host
FROM statshouse_value_incoming_prekey
WHERE (toDate(time) >= (today() - 3)) AND (toDate(time) <= (today() + 1));

CREATE MATERIALIZED VIEW IF NOT EXISTS statshouse_value_1m_agg2_move TO statshouse_value_1m ON CLUSTER statlogs2
(`metric` Int32,
 `time` DateTime,
 `key0` Int32,
 `key1` Int32,
 `key2` Int32,
 `key3` Int32,
 `key4` Int32,
 `key5` Int32,
 `key6` Int32,
 `key7` Int32,
 `key8` Int32,
 `key9` Int32,
 `key10` Int32,
 `key11` Int32,
 `key12` Int32,
 `key13` Int32,
 `key14` Int32,
 `key15` Int32,
 `skey` String,
 `count` SimpleAggregateFunction(sum, Float64),
 `min` SimpleAggregateFunction(min, Float64),
 `max` SimpleAggregateFunction(max, Float64),
 `sum` SimpleAggregateFunction(sum, Float64),
 `sumsquare` SimpleAggregateFunction(sum, Float64),
 `percentiles` AggregateFunction(quantilesTDigest(0.5), Float32),
 `uniq_state` AggregateFunction(uniq, Int64),
 `min_host` AggregateFunction(argMin, Int32, Float32),
 `max_host` AggregateFunction(argMax, Int32, Float32))
AS SELECT metric, toStartOfInterval(time, toIntervalMinute(1)) AS time, key0, key1, key2, key3, key4, key5, key6, key7, key8, key9, key10, key11, key12, key13, key14, key15, skey, count, min, max, sum, sumsquare, percentiles, uniq_state, min_host, max_host
FROM statshouse_value_incoming_prekey_move;

CREATE MATERIALIZED VIEW IF NOT EXISTS statshouse_value_1m_agg_prekey TO statshouse_value_1m_prekey ON CLUSTER statlogs2
(`metric` Int32,
 `prekey` Int32,
 `prekey_set` UInt8,
 `time` DateTime,
 `key0` Int32,
 `key1` Int32,
 `key2` Int32,
 `key3` Int32,
 `key4` Int32,
 `key5` Int32,
 `key6` Int32,
 `key7` Int32,
 `key8` Int32,
 `key9` Int32,
 `key10` Int32,
 `key11` Int32,
 `key12` Int32,
 `key13` Int32,
 `key14` Int32,
 `key15` Int32,
 `skey` String,
 `count` SimpleAggregateFunction(sum, Float64),
 `min` SimpleAggregateFunction(min, Float64),
 `max` SimpleAggregateFunction(max, Float64),
 `sum` SimpleAggregateFunction(sum, Float64),
 `sumsquare` SimpleAggregateFunction(sum, Float64),
 `percentiles` AggregateFunction(quantilesTDigest(0.5), Float32),
 `uniq_state` AggregateFunction(uniq, Int64),
 `min_host` AggregateFunction(argMin, Int32, Float32),
 `max_host` AggregateFunction(argMax, Int32, Float32))
AS SELECT metric, prekey, prekey_set, toStartOfInterval(time, toIntervalMinute(1)) AS time, key0, key1, key2, key3, key4, key5, key6, key7, key8, key9, key10, key11, key12, key13, key14, key15, skey, count, min, max, sum, sumsquare, percentiles, uniq_state, min_host, max_host
FROM statshouse_value_incoming_prekey
WHERE (toDate(time) >= (today() - 3)) AND (toDate(time) <= (today() + 1)) AND (prekey_set = 1);

CREATE MATERIALIZED VIEW IF NOT EXISTS statshouse_value_1m_agg_prekey_move TO statshouse_value_1m_prekey ON CLUSTER statlogs2
(`metric` Int32,
 `prekey` Int32,
 `prekey_set` UInt8,
 `time` DateTime,
 `key0` Int32,
 `key1` Int32,
 `key2` Int32,
 `key3` Int32,
 `key4` Int32,
 `key5` Int32,
 `key6` Int32,
 `key7` Int32,
 `key8` Int32,
 `key9` Int32,
 `key10` Int32,
 `key11` Int32,
 `key12` Int32,
 `key13` Int32,
 `key14` Int32,
 `key15` Int32,
 `skey` String,
 `count` SimpleAggregateFunction(sum, Float64),
 `min` SimpleAggregateFunction(min, Float64),
 `max` SimpleAggregateFunction(max, Float64),
 `sum` SimpleAggregateFunction(sum, Float64),
 `sumsquare` SimpleAggregateFunction(sum, Float64),
 `percentiles` AggregateFunction(quantilesTDigest(0.5), Float32),
 `uniq_state` AggregateFunction(uniq, Int64),
 `min_host` AggregateFunction(argMin, Int32, Float32),
 `max_host` AggregateFunction(argMax, Int32, Float32))
AS SELECT metric, prekey, prekey_set, toStartOfInterval(time, toIntervalMinute(1)) AS time, key0, key1, key2, key3, key4, key5, key6, key7, key8, key9, key10, key11, key12, key13, key14, key15, skey, count, min, max, sum, sumsquare, percentiles, uniq_state, min_host, max_host
FROM statshouse_value_incoming_prekey_move
WHERE prekey_set = 1;

CREATE TABLE IF NOT EXISTS statshouse_value_1m_dist ON CLUSTER statlogs2
(`metric` Int32,
 `time` DateTime,
 `key0` Int32,
 `key1` Int32,
 `key2` Int32,
 `key3` Int32,
 `key4` Int32,
 `key5` Int32,
 `key6` Int32,
 `key7` Int32,
 `key8` Int32,
 `key9` Int32,
 `key10` Int32,
 `key11` Int32,
 `key12` Int32,
 `key13` Int32,
 `key14` Int32,
 `key15` Int32,
 `skey` String,
 `count` SimpleAggregateFunction(sum, Float64),
 `min` SimpleAggregateFunction(min, Float64),
 `max` SimpleAggregateFunction(max, Float64),
 `sum` SimpleAggregateFunction(sum, Float64),
 `sumsquare` SimpleAggregateFunction(sum, Float64),
 `percentiles` AggregateFunction(quantilesTDigest(0.5), Float32),
 `uniq_state` AggregateFunction(uniq, Int64),
 `min_host` AggregateFunction(argMin, Int32, Float32),
 `max_host` AggregateFunction(argMax, Int32, Float32))
ENGINE = Distributed('statlogs_video', 'default', 'statshouse_value_1m');

CREATE TABLE IF NOT EXISTS statshouse_value_1m_prekey ON CLUSTER statlogs2
(`metric` Int32,
 `prekey` Int32,
 `time` DateTime,
 `key0` Int32,
 `key1` Int32,
 `key2` Int32,
 `key3` Int32,
 `key4` Int32,
 `key5` Int32,
 `key6` Int32,
 `key7` Int32,
 `key8` Int32,
 `key9` Int32,
 `key10` Int32,
 `key11` Int32,
 `key12` Int32,
 `key13` Int32,
 `key14` Int32,
 `key15` Int32,
 `skey` String,
 `count` SimpleAggregateFunction(sum, Float64),
 `min` SimpleAggregateFunction(min, Float64),
 `max` SimpleAggregateFunction(max, Float64),
 `sum` SimpleAggregateFunction(sum, Float64),
 `sumsquare` SimpleAggregateFunction(sum, Float64),
 `percentiles` AggregateFunction(quantilesTDigest(0.5), Float32),
 `uniq_state` AggregateFunction(uniq, Int64),
 `min_host` AggregateFunction(argMin, Int32, Float32),
 `max_host` AggregateFunction(argMax, Int32, Float32))
ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/{table}', '{replica}')
PARTITION BY toDate(time)
ORDER BY (metric, prekey, time, key0, key1, key2, key3, key4, key5, key6, key7, key8, key9, key10, key11, key12, key13, key14, key15, skey)
TTL time + toIntervalDay(4) TO DISK 'default', time + toIntervalDay(33)
SETTINGS storage_policy = 'ssd_then_hdd', ttl_only_drop_parts = 1, index_granularity = 8192;

CREATE TABLE IF NOT EXISTS statshouse_value_1m_prekey_dist ON CLUSTER statlogs2
(`metric` Int32,
 `prekey` Int32,
 `time` DateTime,
 `key0` Int32,
 `key1` Int32,
 `key2` Int32,
 `key3` Int32,
 `key4` Int32,
 `key5` Int32,
 `key6` Int32,
 `key7` Int32,
 `key8` Int32,
 `key9` Int32,
 `key10` Int32,
 `key11` Int32,
 `key12` Int32,
 `key13` Int32,
 `key14` Int32,
 `key15` Int32,
 `skey` String,
 `count` SimpleAggregateFunction(sum, Float64),
 `min` SimpleAggregateFunction(min, Float64),
 `max` SimpleAggregateFunction(max, Float64),
 `sum` SimpleAggregateFunction(sum, Float64),
 `sumsquare` SimpleAggregateFunction(sum, Float64),
 `percentiles` AggregateFunction(quantilesTDigest(0.5), Float32),
 `uniq_state` AggregateFunction(uniq, Int64),
 `min_host` AggregateFunction(argMin, Int32, Float32),
 `max_host` AggregateFunction(argMax, Int32, Float32))
ENGINE = Distributed('statlogs_video', 'default', 'statshouse_value_1m_prekey');

CREATE TABLE IF NOT EXISTS statshouse_value_1s ON CLUSTER statlogs2
(`metric` Int32,
 `time` DateTime,
 `key0` Int32,
 `key1` Int32,
 `key2` Int32,
 `key3` Int32,
 `key4` Int32,
 `key5` Int32,
 `key6` Int32,
 `key7` Int32,
 `key8` Int32,
 `key9` Int32,
 `key10` Int32,
 `key11` Int32,
 `key12` Int32,
 `key13` Int32,
 `key14` Int32,
 `key15` Int32,
 `skey` String,
 `count` SimpleAggregateFunction(sum, Float64),
 `min` SimpleAggregateFunction(min, Float64),
 `max` SimpleAggregateFunction(max, Float64),
 `sum` SimpleAggregateFunction(sum, Float64),
 `sumsquare` SimpleAggregateFunction(sum, Float64),
 `percentiles` AggregateFunction(quantilesTDigest(0.5), Float32),
 `uniq_state` AggregateFunction(uniq, Int64),
 `min_host` AggregateFunction(argMin, Int32, Float32),
 `max_host` AggregateFunction(argMax, Int32, Float32))
ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/{table}', '{replica}')
PARTITION BY toStartOfInterval(time, toIntervalHour(6))
ORDER BY (metric, time, key0, key1, key2, key3, key4, key5, key6, key7, key8, key9, key10, key11, key12, key13, key14, key15, skey)
TTL time + toIntervalHour(52)
SETTINGS storage_policy = 'ssd_then_hdd', ttl_only_drop_parts = 1, index_granularity = 8192, max_bytes_to_merge_at_max_space_in_pool = 16106127360;

CREATE MATERIALIZED VIEW IF NOT EXISTS statshouse_value_1s_agg TO statshouse_value_1s ON CLUSTER statlogs2
(`metric` Int32,
 `time` DateTime,
 `key0` Int32,
 `key1` Int32,
 `key2` Int32,
 `key3` Int32,
 `key4` Int32,
 `key5` Int32,
 `key6` Int32,
 `key7` Int32,
 `key8` Int32,
 `key9` Int32,
 `key10` Int32,
 `key11` Int32,
 `key12` Int32,
 `key13` Int32,
 `key14` Int32,
 `key15` Int32,
 `skey` String,
 `count` SimpleAggregateFunction(sum, Float64),
 `min` SimpleAggregateFunction(min, Float64),
 `max` SimpleAggregateFunction(max, Float64),
 `sum` SimpleAggregateFunction(sum, Float64),
 `sumsquare` SimpleAggregateFunction(sum, Float64),
 `percentiles` AggregateFunction(quantilesTDigest(0.5), Float32),
 `uniq_state` AggregateFunction(uniq, Int64),
 `min_host` AggregateFunction(argMin, Int32, Float32),
 `max_host` AggregateFunction(argMax, Int32, Float32))
AS SELECT metric, time, key0, key1, key2, key3, key4, key5, key6, key7, key8, key9, key10, key11, key12, key13, key14, key15, skey, count, min, max, sum, sumsquare, percentiles, uniq_state, min_host, max_host
FROM statshouse_value_incoming
WHERE (toDate(time) >= (today() - 3)) AND (toDate(time) <= (today() + 1));

CREATE MATERIALIZED VIEW IF NOT EXISTS statshouse_value_1s_agg2 TO statshouse_value_1s ON CLUSTER statlogs2
(`metric` Int32,
 `time` DateTime,
 `key0` Int32,
 `key1` Int32,
 `key2` Int32,
 `key3` Int32,
 `key4` Int32,
 `key5` Int32,
 `key6` Int32,
 `key7` Int32,
 `key8` Int32,
 `key9` Int32,
 `key10` Int32,
 `key11` Int32,
 `key12` Int32,
 `key13` Int32,
 `key14` Int32,
 `key15` Int32,
 `skey` String,
 `count` SimpleAggregateFunction(sum, Float64),
 `min` SimpleAggregateFunction(min, Float64),
 `max` SimpleAggregateFunction(max, Float64),
 `sum` SimpleAggregateFunction(sum, Float64),
 `sumsquare` SimpleAggregateFunction(sum, Float64),
 `percentiles` AggregateFunction(quantilesTDigest(0.5), Float32),
 `uniq_state` AggregateFunction(uniq, Int64),
 `min_host` AggregateFunction(argMin, Int32, Float32),
 `max_host` AggregateFunction(argMax, Int32, Float32))
AS SELECT metric, time, key0, key1, key2, key3, key4, key5, key6, key7, key8, key9, key10, key11, key12, key13, key14, key15, skey, count, min, max, sum, sumsquare, percentiles, uniq_state, min_host, max_host
FROM statshouse_value_incoming_prekey
WHERE (toDate(time) >= (today() - 3)) AND (toDate(time) <= (today() + 1));

CREATE MATERIALIZED VIEW IF NOT EXISTS statshouse_value_1s_agg_prekey TO statshouse_value_1s_prekey ON CLUSTER statlogs2
(`metric` Int32,
 `prekey` Int32,
 `prekey_set` UInt8,
 `time` DateTime,
 `key0` Int32,
 `key1` Int32,
 `key2` Int32,
 `key3` Int32,
 `key4` Int32,
 `key5` Int32,
 `key6` Int32,
 `key7` Int32,
 `key8` Int32,
 `key9` Int32,
 `key10` Int32,
 `key11` Int32,
 `key12` Int32,
 `key13` Int32,
 `key14` Int32,
 `key15` Int32,
 `skey` String,
 `count` SimpleAggregateFunction(sum, Float64),
 `min` SimpleAggregateFunction(min, Float64),
 `max` SimpleAggregateFunction(max, Float64),
 `sum` SimpleAggregateFunction(sum, Float64),
 `sumsquare` SimpleAggregateFunction(sum, Float64),
 `percentiles` AggregateFunction(quantilesTDigest(0.5), Float32),
 `uniq_state` AggregateFunction(uniq, Int64),
 `min_host` AggregateFunction(argMin, Int32, Float32),
 `max_host` AggregateFunction(argMax, Int32, Float32))
AS SELECT metric, prekey, prekey_set, time, key0, key1, key2, key3, key4, key5, key6, key7, key8, key9, key10, key11, key12, key13, key14, key15, skey, count, min, max, sum, sumsquare, percentiles, uniq_state, min_host, max_host
FROM statshouse_value_incoming_prekey
WHERE (toDate(time) >= (today() - 3)) AND (toDate(time) <= (today() + 1)) AND (prekey_set = 1);

CREATE TABLE IF NOT EXISTS statshouse_value_1s_dist ON CLUSTER statlogs2
(`metric` Int32,
 `time` DateTime,
 `key0` Int32,
 `key1` Int32,
 `key2` Int32,
 `key3` Int32,
 `key4` Int32,
 `key5` Int32,
 `key6` Int32,
 `key7` Int32,
 `key8` Int32,
 `key9` Int32,
 `key10` Int32,
 `key11` Int32,
 `key12` Int32,
 `key13` Int32,
 `key14` Int32,
 `key15` Int32,
 `skey` String,
 `count` SimpleAggregateFunction(sum, Float64),
 `min` SimpleAggregateFunction(min, Float64),
 `max` SimpleAggregateFunction(max, Float64),
 `sum` SimpleAggregateFunction(sum, Float64),
 `sumsquare` SimpleAggregateFunction(sum, Float64),
 `percentiles` AggregateFunction(quantilesTDigest(0.5), Float32),
 `uniq_state` AggregateFunction(uniq, Int64),
 `min_host` AggregateFunction(argMin, Int32, Float32),
 `max_host` AggregateFunction(argMax, Int32, Float32))
ENGINE = Distributed('statlogs_video', 'default', 'statshouse_value_1s');

CREATE TABLE IF NOT EXISTS statshouse_value_1s_prekey ON CLUSTER statlogs2
(`metric` Int32,
 `prekey` Int32,
 `time` DateTime,
 `key0` Int32,
 `key1` Int32,
 `key2` Int32,
 `key3` Int32,
 `key4` Int32,
 `key5` Int32,
 `key6` Int32,
 `key7` Int32,
 `key8` Int32,
 `key9` Int32,
 `key10` Int32,
 `key11` Int32,
 `key12` Int32,
 `key13` Int32,
 `key14` Int32,
 `key15` Int32,
 `skey` String,
 `count` SimpleAggregateFunction(sum, Float64),
 `min` SimpleAggregateFunction(min, Float64),
 `max` SimpleAggregateFunction(max, Float64),
 `sum` SimpleAggregateFunction(sum, Float64),
 `sumsquare` SimpleAggregateFunction(sum, Float64),
 `percentiles` AggregateFunction(quantilesTDigest(0.5), Float32),
 `uniq_state` AggregateFunction(uniq, Int64),
 `min_host` AggregateFunction(argMin, Int32, Float32),
 `max_host` AggregateFunction(argMax, Int32, Float32))
ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/{table}', '{replica}')
PARTITION BY toStartOfInterval(time, toIntervalHour(6))
ORDER BY (metric, prekey, time, key0, key1, key2, key3, key4, key5, key6, key7, key8, key9, key10, key11, key12, key13, key14, key15, skey)
TTL time + toIntervalHour(52)
SETTINGS storage_policy = 'ssd_then_hdd', ttl_only_drop_parts = 1, max_bytes_to_merge_at_max_space_in_pool = 16106127360, index_granularity = 8192;

CREATE TABLE IF NOT EXISTS statshouse_value_1s_prekey_dist ON CLUSTER statlogs2
(`metric` Int32,
 `prekey` Int32,
 `time` DateTime,
 `key0` Int32,
 `key1` Int32,
 `key2` Int32,
 `key3` Int32,
 `key4` Int32,
 `key5` Int32,
 `key6` Int32,
 `key7` Int32,
 `key8` Int32,
 `key9` Int32,
 `key10` Int32,
 `key11` Int32,
 `key12` Int32,
 `key13` Int32,
 `key14` Int32,
 `key15` Int32,
 `skey` String,
 `count` SimpleAggregateFunction(sum, Float64),
 `min` SimpleAggregateFunction(min, Float64),
 `max` SimpleAggregateFunction(max, Float64),
 `sum` SimpleAggregateFunction(sum, Float64),
 `sumsquare` SimpleAggregateFunction(sum, Float64),
 `percentiles` AggregateFunction(quantilesTDigest(0.5), Float32),
 `uniq_state` AggregateFunction(uniq, Int64),
 `min_host` AggregateFunction(argMin, Int32, Float32),
 `max_host` AggregateFunction(argMax, Int32, Float32))
ENGINE = Distributed('statlogs_video', 'default', 'statshouse_value_1s_prekey');

CREATE TABLE IF NOT EXISTS statshouse_value_incoming ON CLUSTER statlogs2
(`time` DateTime,
 `metric` Int32,
 `key0` Int32,
 `key1` Int32,
 `key2` Int32,
 `key3` Int32,
 `key4` Int32,
 `key5` Int32,
 `key6` Int32,
 `key7` Int32,
 `key8` Int32,
 `key9` Int32,
 `key10` Int32,
 `key11` Int32,
 `key12` Int32,
 `key13` Int32,
 `key14` Int32,
 `key15` Int32,
 `skey` String,
 `count` SimpleAggregateFunction(sum, Float64),
 `min` SimpleAggregateFunction(min, Float64),
 `max` SimpleAggregateFunction(max, Float64),
 `sum` SimpleAggregateFunction(sum, Float64),
 `sumsquare` SimpleAggregateFunction(sum, Float64),
 `min_host` AggregateFunction(argMin, Int32, Float32),
 `max_host` AggregateFunction(argMax, Int32, Float32),
 `percentiles` AggregateFunction(quantilesTDigest(0.5), Float32),
 `uniq_state` AggregateFunction(uniq, Int64))
ENGINE = Null;

CREATE TABLE IF NOT EXISTS statshouse_value_incoming_prekey ON CLUSTER statlogs2
(`metric` Int32,
 `prekey` Int32,
 `prekey_set` UInt8,
 `time` DateTime,
 `key0` Int32,
 `key1` Int32,
 `key2` Int32,
 `key3` Int32,
 `key4` Int32,
 `key5` Int32,
 `key6` Int32,
 `key7` Int32,
 `key8` Int32,
 `key9` Int32,
 `key10` Int32,
 `key11` Int32,
 `key12` Int32,
 `key13` Int32,
 `key14` Int32,
 `key15` Int32,
 `skey` String,
 `count` SimpleAggregateFunction(sum, Float64),
 `min` SimpleAggregateFunction(min, Float64),
 `max` SimpleAggregateFunction(max, Float64),
 `sum` SimpleAggregateFunction(sum, Float64),
 `sumsquare` SimpleAggregateFunction(sum, Float64),
 `min_host` AggregateFunction(argMin, Int32, Float32),
 `max_host` AggregateFunction(argMax, Int32, Float32),
 `percentiles` AggregateFunction(quantilesTDigest(0.5), Float32),
 `uniq_state` AggregateFunction(uniq, Int64))
ENGINE = Null;

CREATE TABLE IF NOT EXISTS statshouse_value_incoming_prekey_move ON CLUSTER statlogs2
(`metric` Int32,
 `prekey` Int32,
 `prekey_set` UInt8,
 `time` DateTime,
 `key0` Int32,
 `key1` Int32,
 `key2` Int32,
 `key3` Int32,
 `key4` Int32,
 `key5` Int32,
 `key6` Int32,
 `key7` Int32,
 `key8` Int32,
 `key9` Int32,
 `key10` Int32,
 `key11` Int32,
 `key12` Int32,
 `key13` Int32,
 `key14` Int32,
 `key15` Int32,
 `skey` String,
 `count` SimpleAggregateFunction(sum, Float64),
 `min` SimpleAggregateFunction(min, Float64),
 `max` SimpleAggregateFunction(max, Float64),
 `sum` SimpleAggregateFunction(sum, Float64),
 `sumsquare` SimpleAggregateFunction(sum, Float64),
 `min_host` AggregateFunction(argMin, Int32, Float32),
 `max_host` AggregateFunction(argMax, Int32, Float32),
 `percentiles` AggregateFunction(quantilesTDigest(0.5), Float32),
 `uniq_state` AggregateFunction(uniq, Int64))
ENGINE = Null;
