#!/bin/sh
set -e

mkdir -p cache/aggregator6/
../target/statshouse-agg --auto-create --auto-create-default-namespace --agg-addr=127.0.0.1:13396,127.0.0.1:13406,127.0.0.1:13416 --local-replica=3 --local-shard=3 \
--cluster-shards-addrs=127.0.0.1:13336,127.0.0.1:13346,127.0.0.1:13356 \
--cluster-shards-addrs=127.0.0.1:13366,127.0.0.1:13376,127.0.0.1:13386 \
--cluster-shards-addrs=127.0.0.1:13396,127.0.0.1:13406,127.0.0.1:13416 \
 --cluster=statlogs2 --kh=127.0.0.1:8123 --deny-old-agents=false --metadata-addr "127.0.0.1:2442" --cache-dir=cache/aggregator9 "$@"


