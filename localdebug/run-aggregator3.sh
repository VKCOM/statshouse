#!/bin/sh
set -e

mkdir -p cache/aggregator3/
../target/statshouse-agg --auto-create --auto-create-default-namespace --agg-addr=127.0.0.1:13336,127.0.0.1:13346,127.0.0.1:13356 --previous-shards=9 --local-replica=3 --local-shard=1 \
--local-agg-hosts=127.0.0.1:13336 \
--local-agg-hosts=127.0.0.1:13346 \
--local-agg-hosts=127.0.0.1:13356 \
--local-agg-hosts=127.0.0.1:13366 \
--local-agg-hosts=127.0.0.1:13376 \
--local-agg-hosts=127.0.0.1:13386 \
 --cluster=statlogs2 --kh=127.0.0.1:8123 --deny-old-agents=false --metadata-addr "127.0.0.1:2442" --cache-dir=cache/aggregator3 "$@"
