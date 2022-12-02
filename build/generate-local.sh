#!/bin/sh
set -e

cp cluster.sql clickhouse.sql
sed -i -r 's/^ENGINE = Distributed[^)]*/ENGINE = AggregatingMergeTree\(/g' clickhouse.sql
sed -i 's/^ENGINE = ReplicatedAggregatingMergeTree[^)]*/ENGINE = AggregatingMergeTree\(/g' clickhouse.sql
sed -i 's/ ON CLUSTER statlogs2//g' clickhouse.sql
patch clickhouse.sql clickhouse.sql.patch
