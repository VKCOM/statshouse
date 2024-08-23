#!/bin/sh
set -e

mkdir -p cache/agent-ingress2-key3

../target/statshouse agent --log-level=trace --hardware-metric-scrape-disable --cluster=default --hostname=agent_ingress2_key3 -p=13332 --agg-addr=127.0.0.1:13326,127.0.0.1:13326,127.0.0.1:13326 --log-level=trace --cache-dir=cache/agent-ingress2-key3 --aes-pwd-file=ingress_keys2/key3.txt "$@"
