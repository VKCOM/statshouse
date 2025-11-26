#!/bin/sh
set -e

mkdir -p cache/aggregator2s2/
../target/statshouse-agg \
  --agg-addr=127.0.0.1:13336,127.0.0.1:13336,127.0.0.1:13336,127.0.0.1:13346,127.0.0.1:13346,127.0.0.1:13346 \
  --local-replica=1 --local-shard=2 --cluster=statlogs2 --kh=127.0.0.1:8123 --deny-old-agents=false \
  --metadata-addr "127.0.0.1:2442" --cache-dir=cache/aggregator2s2 "$@"

# --auto-create --auto-create-default-namespace
