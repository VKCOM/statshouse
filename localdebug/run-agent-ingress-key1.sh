#!/bin/sh
set -e

mkdir -p cache/agent-ingress-key1

../target/statshouse agent --cluster=test_shard_localhost --hostname=agent_ingress_key1 -p=13334 --agg-addr=127.0.0.1:13327,127.0.0.1:13327,127.0.0.1:13327 --log-level=trace --cache-dir=cache/agent-ingress-key1 --aes-pwd-file=ingress_keys/key1.txt "$@"
