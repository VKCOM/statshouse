#!/bin/sh
set -e

mkdir -p cache/aggregator1/
../target/statshouse-agg --auto-create --auto-create-default-namespace --agg-addr=127.0.0.1:13336,127.0.0.1:13346,127.0.0.1:13356 --local-replica=1 --cluster=statlogs2 --kh=127.0.0.1:8123 --deny-old-agents=false --metadata-addr "127.0.0.1:2442" --cache-dir=cache/aggregator1 "$@"
