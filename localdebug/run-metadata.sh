#!/bin/sh
set -e

mkdir -p dbmeta
mkdir -p dbmeta/binlog
if [ -z "$(ls -A dbmeta/binlog)" ]; then
  ../target/statshouse-metadata -p 2442 --db-path=dbmeta/db --binlog-prefix=dbmeta/binlog/bl --create-binlog "0,1"
fi
../target/statshouse-metadata -p 2442 --db-path=dbmeta/db --binlog-prefix=dbmeta/binlog/bl "$@"
