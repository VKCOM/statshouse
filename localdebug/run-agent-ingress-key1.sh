#!/bin/sh
set -e

../target/statshouse agent --cluster=local_test_cluster --hostname=agent_ingress_3 -p=13334 --agg-addr=127.0.0.1:13335,127.0.0.1:13335,127.0.0.1:13335 --log-level=trace --cache-dir=cache/agent-ingress-key1 --aes-pwd-file=ingress_keys/key1.txt "$@"
