#!/bin/sh
set -e

cd .. && make build-main-daemons

# run clickhouse server in docker
# docker run --network=host --name some-clickhouse-server --ulimit nofile=262144:262144 clickhouse/clickhouse-server
#
# run clickhouse client in docker
# docker exec -it some-clickhouse-server clickhouse-client
