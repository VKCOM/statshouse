#!/bin/sh
set -e

mkdir -p cache/aggregator/
../target/statshouse --aggregator --auto-create --auto-create-default-namespace --agg-addr=127.0.0.1:13336  --cluster=default --kh=127.0.0.1:8123 --metadata-addr "127.0.0.1:2442" --cache-dir=cache/aggregator "$@"
