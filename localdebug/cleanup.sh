#!/bin/sh

rm -rf dbmeta
rm -rf cache
exec clickhouse-cluster/cleanup.sh
