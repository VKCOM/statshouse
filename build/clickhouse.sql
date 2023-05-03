CREATE TABLE IF NOT EXISTS statshouse_value_incoming_prekey3
(
    `metric` Int32,
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
    `uniq_state` AggregateFunction(uniq, Int64)
)
ENGINE = Null;

-- Section per time resolution - 1s
CREATE TABLE IF NOT EXISTS statshouse_value_1s_dist
(
    `metric`         Int32,
    `prekey`         Int32,
    `time`           DateTime,
    `key0`           Int32, `key1` Int32, `key2` Int32, `key3` Int32, `key4` Int32, `key5` Int32, `key6` Int32, `key7` Int32,
    `key8`           Int32, `key9` Int32, `key10` Int32, `key11` Int32, `key12` Int32, `key13` Int32, `key14` Int32, `key15` Int32,
    `skey`           String,
    `count`          SimpleAggregateFunction(sum, Float64),
    `min`            SimpleAggregateFunction(min, Float64),
    `max`            SimpleAggregateFunction(max, Float64),
    `sum`            SimpleAggregateFunction(sum, Float64),
    `sumsquare`      SimpleAggregateFunction(sum, Float64),
    `percentiles`    AggregateFunction(quantilesTDigest(0.5), Float32),
    `uniq_state`     AggregateFunction(uniq, Int64),
    `min_host`       AggregateFunction(argMin, Int32, Float32),
    `max_host`       AggregateFunction(argMax, Int32, Float32)
    )
    ENGINE = AggregatingMergeTree()
    PARTITION BY toStartOfInterval(time, INTERVAL 6 hour)
    ORDER BY (metric, time, key0, key1, key2, key3, key4, key5, key6, key7, key8, key9, key10, key11,
              key12, key13, key14, key15, skey);

CREATE TABLE IF NOT EXISTS statshouse_value_1s_prekey_dist
(
    `metric`         Int32,
    `prekey`         Int32,
    `time`           DateTime,
    `key0`           Int32, `key1` Int32, `key2` Int32, `key3` Int32, `key4` Int32, `key5` Int32, `key6` Int32, `key7` Int32,
    `key8`           Int32, `key9` Int32, `key10` Int32, `key11` Int32, `key12` Int32, `key13` Int32, `key14` Int32, `key15` Int32,
    `skey`           String,
    `count`          SimpleAggregateFunction(sum, Float64),
    `min`            SimpleAggregateFunction(min, Float64),
    `max`            SimpleAggregateFunction(max, Float64),
    `sum`            SimpleAggregateFunction(sum, Float64),
    `sumsquare`      SimpleAggregateFunction(sum, Float64),
    `percentiles`    AggregateFunction(quantilesTDigest(0.5), Float32),
    `uniq_state`     AggregateFunction(uniq, Int64),
    `min_host`       AggregateFunction(argMin, Int32, Float32),
    `max_host`       AggregateFunction(argMax, Int32, Float32)
)
    ENGINE = AggregatingMergeTree()
    PARTITION BY toStartOfInterval(time, INTERVAL 6 hour)
        ORDER BY (metric, prekey, time, key0, key1, key2, key3, key4, key5, key6, key7, key8, key9, key10, key11,
                                    key12, key13, key14, key15, skey);

CREATE MATERIALIZED VIEW IF NOT EXISTS statshouse_value_1s_agg3 TO statshouse_value_1s_dist AS
SELECT metric,
       time,
       key0, key1, key2, key3, key4, key5, key6, key7, key8, key9, key10, key11, key12, key13, key14, key15,
       skey,
       count,
       min,
       max,
       sum,
       sumsquare,
       percentiles,
       uniq_state,
       min_host,
       max_host
FROM statshouse_value_incoming_prekey3
WHERE (toDate(time) >= today() - 3)
  AND (toDate(time) <= today() + 1) AND prekey_set <> 2 ;

CREATE MATERIALIZED VIEW IF NOT EXISTS statshouse_value_1s_agg_prekey3 TO statshouse_value_1s_prekey_dist AS
SELECT metric,
    prekey,
    prekey_set,
    time,
    key0, key1, key2, key3, key4, key5, key6, key7, key8, key9, key10, key11, key12, key13, key14, key15,
    skey,
    count,
    min,
    max,
    sum,
    sumsquare,
    percentiles,
    uniq_state,
    min_host,
    max_host
FROM statshouse_value_incoming_prekey3
WHERE (toDate(time) >= today() - 3)
  AND (toDate(time) <= today() + 1) AND prekey_set > 0;

-- Section per time resolution - 5s
CREATE TABLE IF NOT EXISTS statshouse_value_1m_dist
(
    `metric`         Int32,
    `time`           DateTime,
    `key0`           Int32, `key1` Int32, `key2` Int32, `key3` Int32, `key4` Int32, `key5` Int32, `key6` Int32, `key7` Int32,
    `key8`           Int32, `key9` Int32, `key10` Int32, `key11` Int32, `key12` Int32, `key13` Int32, `key14` Int32, `key15` Int32,
    `skey`           String,
    `count`          SimpleAggregateFunction(sum, Float64),
    `min`            SimpleAggregateFunction(min, Float64),
    `max`            SimpleAggregateFunction(max, Float64),
    `sum`            SimpleAggregateFunction(sum, Float64),
    `sumsquare`      SimpleAggregateFunction(sum, Float64),
    `percentiles`    AggregateFunction(quantilesTDigest(0.5), Float32),
    `uniq_state`     AggregateFunction(uniq, Int64),
    `min_host`       AggregateFunction(argMin, Int32, Float32),
    `max_host`       AggregateFunction(argMax, Int32, Float32)
)
    ENGINE = AggregatingMergeTree()
        PARTITION BY toDate(time) ORDER BY (metric, time, key0, key1, key2, key3, key4, key5, key6, key7, key8, key9, key10, key11,
                                    key12, key13, key14, key15, skey);

CREATE TABLE IF NOT EXISTS statshouse_value_1m_prekey_dist
(
    `metric`         Int32,
    `prekey`         Int32,
    `time`           DateTime,
    `key0`           Int32, `key1` Int32, `key2` Int32, `key3` Int32, `key4` Int32, `key5` Int32, `key6` Int32, `key7` Int32,
    `key8`           Int32, `key9` Int32, `key10` Int32, `key11` Int32, `key12` Int32, `key13` Int32, `key14` Int32, `key15` Int32,
    `skey`           String,
    `count`          SimpleAggregateFunction(sum, Float64),
    `min`            SimpleAggregateFunction(min, Float64),
    `max`            SimpleAggregateFunction(max, Float64),
    `sum`            SimpleAggregateFunction(sum, Float64),
    `sumsquare`      SimpleAggregateFunction(sum, Float64),
    `percentiles`    AggregateFunction(quantilesTDigest(0.5), Float32),
    `uniq_state`     AggregateFunction(uniq, Int64),
    `min_host`       AggregateFunction(argMin, Int32, Float32),
    `max_host`       AggregateFunction(argMax, Int32, Float32)
)
    ENGINE = AggregatingMergeTree()
        PARTITION BY toDate(time) ORDER BY (metric, prekey, time, key0, key1, key2, key3, key4, key5, key6, key7, key8, key9, key10, key11,
                                    key12, key13, key14, key15, skey);

CREATE MATERIALIZED VIEW IF NOT EXISTS statshouse_value_1m_agg3 TO statshouse_value_1m_dist AS
SELECT metric,
       toStartOfInterval(time, INTERVAL 1 minute)                             as time,
       key0, key1, key2, key3, key4, key5, key6, key7, key8, key9, key10, key11, key12, key13, key14, key15,
       skey,
       count,
       min,
       max,
       sum,
       sumsquare,
       percentiles,
       uniq_state,
       min_host,
       max_host
FROM statshouse_value_incoming_prekey3
WHERE (toDate(time) >= today() - 3)
  AND (toDate(time) <= today() + 1) AND prekey_set <> 2;

CREATE MATERIALIZED VIEW IF NOT EXISTS statshouse_value_1m_agg_prekey3 TO statshouse_value_1m_prekey_dist AS
SELECT metric,
       prekey,
       prekey_set,
       toStartOfInterval(time, INTERVAL 1 minute)                             as time,
       key0, key1, key2, key3, key4, key5, key6, key7, key8, key9, key10, key11, key12, key13, key14, key15,
       skey,
       count,
       min,
       max,
       sum,
       sumsquare,
       percentiles,
       uniq_state,
       min_host,
       max_host
FROM statshouse_value_incoming_prekey3
WHERE (toDate(time) >= today() - 3)
  AND (toDate(time) <= today() + 1) AND prekey_set > 0;

-- Section per time resolution - 1h
CREATE TABLE IF NOT EXISTS statshouse_value_1h_dist
(
    `metric`         Int32,
    `time`           DateTime,
    `key0`           Int32, `key1` Int32, `key2` Int32, `key3` Int32, `key4` Int32, `key5` Int32, `key6` Int32, `key7` Int32,
    `key8`           Int32, `key9` Int32, `key10` Int32, `key11` Int32, `key12` Int32, `key13` Int32, `key14` Int32, `key15` Int32,
    `skey`           String,
    `count`          SimpleAggregateFunction(sum, Float64),
    `min`            SimpleAggregateFunction(min, Float64),
    `max`            SimpleAggregateFunction(max, Float64),
    `sum`            SimpleAggregateFunction(sum, Float64),
    `sumsquare`      SimpleAggregateFunction(sum, Float64),
    `percentiles`    AggregateFunction(quantilesTDigest(0.5), Float32),
    `uniq_state`     AggregateFunction(uniq, Int64),
    `min_host`       AggregateFunction(argMin, Int32, Float32),
    `max_host`       AggregateFunction(argMax, Int32, Float32)
)
    ENGINE = AggregatingMergeTree()
        PARTITION BY toYYYYMM(time) ORDER BY (metric, time, key0, key1, key2, key3, key4, key5, key6, key7, key8, key9, key10, key11,
                                    key12, key13, key14, key15, skey);

CREATE TABLE IF NOT EXISTS statshouse_value_1h_prekey_dist
(
    `metric`         Int32,
    `prekey`         Int32,
    `time`           DateTime,
    `key0`           Int32, `key1` Int32, `key2` Int32, `key3` Int32, `key4` Int32, `key5` Int32, `key6` Int32, `key7` Int32,
    `key8`           Int32, `key9` Int32, `key10` Int32, `key11` Int32, `key12` Int32, `key13` Int32, `key14` Int32, `key15` Int32,
    `skey`           String,
    `count`          SimpleAggregateFunction(sum, Float64),
    `min`            SimpleAggregateFunction(min, Float64),
    `max`            SimpleAggregateFunction(max, Float64),
    `sum`            SimpleAggregateFunction(sum, Float64),
    `sumsquare`      SimpleAggregateFunction(sum, Float64),
    `percentiles`    AggregateFunction(quantilesTDigest(0.5), Float32),
    `uniq_state`     AggregateFunction(uniq, Int64),
    `min_host`       AggregateFunction(argMin, Int32, Float32),
    `max_host`       AggregateFunction(argMax, Int32, Float32)
)
    ENGINE = AggregatingMergeTree()
        PARTITION BY toYYYYMM(time) ORDER BY (metric, prekey, time, key0, key1, key2, key3, key4, key5, key6, key7, key8, key9, key10, key11,
                                    key12, key13, key14, key15, skey);

CREATE MATERIALIZED VIEW IF NOT EXISTS statshouse_value_1h_agg3 TO statshouse_value_1h_dist AS
SELECT metric,
       toStartOfInterval(time, INTERVAL 1 hour)                             as time,
       key0, key1, key2, key3, key4, key5, key6, key7, key8, key9, key10, key11, key12, key13, key14, key15,
       skey,
       count,
       min,
       max,
       sum,
       sumsquare,
       percentiles,
       uniq_state,
       min_host,
       max_host
FROM statshouse_value_incoming_prekey3
WHERE (toDate(time) >= today() - 3)
  AND (toDate(time) <= today() + 1) AND prekey_set <> 2;

CREATE MATERIALIZED VIEW IF NOT EXISTS statshouse_value_1h_agg_prekey3 TO statshouse_value_1h_prekey_dist AS
SELECT metric,
       prekey,
       prekey_set,
       toStartOfInterval(time, INTERVAL 1 hour)                             as time,
       key0, key1, key2, key3, key4, key5, key6, key7, key8, key9, key10, key11, key12, key13, key14, key15,
       skey,
       count,
       min,
       max,
       sum,
       sumsquare,
       percentiles,
       uniq_state,
       min_host,
       max_host
FROM statshouse_value_incoming_prekey3
WHERE (toDate(time) >= today() - 3)
  AND (toDate(time) <= today() + 1) AND prekey_set > 0;


-- section for O(1) invalidation of contributors cache
CREATE TABLE IF NOT EXISTS statshouse_contributors_log_dist
(
    `insert_time`    DateTime,
    `time`           DateTime,
    `count`          SimpleAggregateFunction(sum, Float64)
)
    ENGINE = AggregatingMergeTree()
    PARTITION BY toStartOfInterval(insert_time, INTERVAL 6 hour)
        ORDER BY (insert_time, time);

CREATE TABLE IF NOT EXISTS statshouse_contributors_log_buffer
(
    `insert_time`    DateTime,
    `time`           DateTime,
    `count`          SimpleAggregateFunction(sum, Float64)
)
    ENGINE = Buffer(default, statshouse_contributors_log_dist, 4, 2, 2, 1000000000, 1000000000, 1000000, 1000000);
-- 2 seconds is good enough to aggregate multiple inserts per second

CREATE MATERIALIZED VIEW IF NOT EXISTS statshouse_contributors_log_agg2 TO statshouse_contributors_log_buffer AS
SELECT now() as insert_time,
    time,
    count
FROM statshouse_value_incoming_prekey3
WHERE (toDate(time) >= today() - 3)
  AND (toDate(time) <= today() + 1)
  AND (metric = -2);


-- select insert_time,time,sum(count) as count from statshouse_contributors_log where insert_time >= now()-30 group by insert_time, time order by insert_time, time settings optimize_aggregation_in_order = 1


-- section for O(1) last used for metric
CREATE TABLE IF NOT EXISTS statshouse_metric_1d_dist
(
    `metric`         Int32,
    `time`           DateTime,
    `row_count`      SimpleAggregateFunction(sum, Float64),
    `count`          SimpleAggregateFunction(sum, Float64)
    )
    ENGINE = AggregatingMergeTree()
    ORDER BY (metric, time);

CREATE TABLE IF NOT EXISTS statshouse_metric_1d_buffer
(
    `metric`         Int32,
    `time`           DateTime,
    `row_count`      SimpleAggregateFunction(sum, Float64),
    `count`          SimpleAggregateFunction(sum, Float64)
    )
    ENGINE = Buffer(default, statshouse_metric_1d_dist, 4, 60, 60, 1000000000, 1000000000, 100000000, 100000000);

CREATE MATERIALIZED VIEW IF NOT EXISTS statshouse_metric_1d_agg2 TO statshouse_metric_1d_buffer AS
SELECT metric,
       toStartOfInterval(time, INTERVAL 1 day)                             as time,
       1 as row_count,
       count
FROM statshouse_value_incoming_prekey3
WHERE (toDate(time) >= today() - 3)
  AND (toDate(time) <= today() + 1);

CREATE TABLE IF NOT EXISTS statshouse_internal_log_dist
(
    `time`          DateTime,
    `host`          String,
    `type`          String,
    `key0`          String,
    `key1`          String,
    `key2`          String,
    `key3`          String,
    `key4`          String,
    `key5`          String,
    `message` String
    )
ENGINE = AggregatingMergeTree()
    PARTITION BY toYYYYMM(time)
    ORDER BY (time, host, type, key0, key1, key2, key3, key4, key5);


CREATE TABLE IF NOT EXISTS statshouse_internal_log_buffer
(
    `time`          DateTime,
    `host`          String,
    `type`          String,
    `key0`          String,
    `key1`          String,
    `key2`          String,
    `key3`          String,
    `key4`          String,
    `key5`          String,
    `message` String
)
ENGINE = Buffer(default, statshouse_internal_log_dist, 2, 120, 120, 10000000, 10000000, 100000000, 100000000);

