#!/bin/sh

set -e

mkdir -p cache/api
../target/statshouse-api --local-mode --insecure-mode --access-log \
  --clickhouse-v2-addrs=localhost:9000,localhost:9000,localhost:9000 \
  --listen-rpc-addr=localhost:10889 \
  --verbose --listen-addr localhost:10888 --static-dir ../statshouse-ui/build/ \
  --metadata-addr "127.0.0.1:2442" --available-shards "1" --cache-dir=cache/api --announcement=Hello \
  "$@"

# --clickhouse-v2-addrs=localhost:9000,localhost:9000,localhost:9000,localhost:9000,localhost:9000,localhost:9000 \
# --shard-by-metric-shards=2 \
# --available-shards "1,2"
