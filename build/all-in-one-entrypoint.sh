#!/bin/bash
set -e
/entrypoint.sh &
/bin/statshouse-metadata --db-path=/var/lib/statshouse/metadata/db --binlog-prefix=/var/lib/statshouse/metadata/binlog/bl &
until clickhouse-client --query="SELECT 1"; do sleep 0.2; done
/bin/statshouse aggregator --cluster=test_shard_localhost --log-level=trace --agg-addr=':13336' --kh=127.0.0.1:8123 \
  --auto-create --cache-dir=/var/lib/statshouse/cache/aggregator -u=root -g=root &
/bin/statshouse agent --cluster=test_shard_localhost --log-level=trace --remote-write-enabled \
  --agg-addr='127.0.0.1:13336,127.0.0.1:13336,127.0.0.1:13336' --cache-dir=/var/lib/statshouse/cache/agent \
  -u=root -g=root &
/bin/statshouse-api --verbose --local-mode --access-log --clickhouse-v1-addrs= --clickhouse-v2-addrs=127.0.0.1:9000 \
  --listen-addr=:10888 --statshouse-addr=127.0.0.1:13337 --disk-cache=/var/lib/statshouse/cache/api/mapping_cache.sqlite3 \
  --static-dir=/usr/lib/statshouse-api/statshouse-ui/ &
wait -n
exit $?
