#!/bin/sh
set -e

mkdir -p cache/agent-ingress-key2

../target/statshouse agent --cluster=local_test_cluster --hostname=agent_ingress_key2 -p=13333 --agg-addr=127.0.0.1:13327,127.0.0.1:13327,127.0.0.1:13327 --log-level=trace --cache-dir=cache/agent-ingress-key2 --aes-pwd-file=ingress_keys/key2.txt "$@"
