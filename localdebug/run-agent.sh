#!/bin/sh
set -e

mkdir -p cache/agent/
../target/statshouse -agent --cluster=local_test_cluster --hostname=agent1 --agg-addr=127.0.0.1:13336,127.0.0.1:13346,127.0.0.1:13356 --cache-dir=cache/agent "$@"
