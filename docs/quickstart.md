# StatsHouse Quick Start Guide

## Running from docker
The command below will start in-memory StatsHouse instance. Nothing is persisted there, all will disappear after containers shutdown.
```shell
docker compose --profile sh up
```

After containers started, navigate to http://localhost:10888/view?f=-300&t=0&s=__contributors_log_rev and toggle "play" button.

## Building
The following command will compile all three StatsHouse binaries as well as frontend SPA:
```bash
make build-sh build-sh-api build-sh-metadata build-sh-ui
```
Find them in target/ and statshouse-ui/build/ directories:
```bash
ls target/
statshouse  statshouse-api  statshouse-metadata

ls ./statshouse-ui/build/
asset-manifest.json  favicon.ico  index.html  logo192.png  logo512.png  manifest.json  openapi  robots.txt  static
```

## Before first run
### Create directories
StatsHouse does not create directories and files on its own, it expects them to exist. Need to create directories for cache and binary logs. Directory exact location is unimportant, but it should be writeable. Consider the following directory structure:
```shell
mkdir -p ~/statshouse/cache/aggregator
mkdir -p ~/statshouse/cache/agent
mkdir -p ~/statshouse/cache/api
mkdir -p ~/statshouse/metadata/binlog
```

Further here it is assumed directories above exist.

### Create metadata engine's binlog
```shell
./target/statshouse-metadata --binlog-prefix=$HOME/statshouse/metadata/binlog/bl --create-binlog "0,1"
```
### Configure ClickHouse
StatsHouse expects particular tables exist in ClickHouse. To create them use on of the two following scripts:
* `docker/clickhouse.sql` for localhost ClickHouse
* `docker/clickhouse-cluster.sql` for ClickHouse cluster

For starters, you can use the docker-compose service in this repo (it already has all necessary tables created):
``` shell
docker compose --profile kh up -d
```
Verify docker-compose instance is running with:
```shell
docker run -it --rm --network=statshouse_default --link kh:clickhouse-server yandex/clickhouse-client --host clickhouse-server
```

## Running from command line
1. Start StatsHouse metadata engine:
    ```shell
    ./target/statshouse-metadata --statshouse-addr= --db-path=$HOME/statshouse/metadata/db --binlog-prefix=$HOME/statshouse/metadata/binlog/bl
    ```

2. Start StatsHouse aggregator:
    ```shell
    ./target/statshouse aggregator --log-level=trace --agg-addr='localhost:13336' --kh=localhost:8123 --cache-dir=$HOME/statshouse/cache/aggregator --cluster=test_shard_localhost
    ```

3. Start StatsHouse agent:
    ```shell
    ./target/statshouse agent --log-level=trace --agg-addr='localhost:13336,localhost:13336,localhost:13336' --cache-dir=$HOME/statshouse/cache/agent --cluster=test_shard_localhost
    ```

4. Start StatsHouse API:
    ```shell
    ./target/statshouse-api --verbose --local-mode --access-log --clickhouse-v1-addrs= --clickhouse-v2-addrs=localhost:9000 --listen-addr=localhost:10888 --disk-cache=$HOME/statshouse/cache/api/mapping_cache.sqlite3 --static-dir=statshouse-ui/build
    ```
5. Open StatsHouse dashboard http://localhost:10888/view?f=-300&t=0&s=__contributors_log_rev and toggle "play" button.
