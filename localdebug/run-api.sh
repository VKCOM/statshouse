#!/bin/sh

set -e

mkdir -p cache/api
../target/statshouse-api --local-mode --insecure-mode --access-log --clickhouse-v1-addrs= --clickhouse-v2-addrs=localhost:9000 --verbose --listen-addr localhost:10888 --static-dir ../statshouse-ui/build/ --metadata-addr "127.0.0.1:2442" --disk-cache=cache/api/mapping_cache.sqlite3 "$@"

