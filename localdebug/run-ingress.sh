#!/bin/sh
set -e

../target/statshouse ingress_proxy --cluster=test_shard_localhost --hostname=ingress --ingress-addr=127.0.0.1:13335 --agg-addr=127.0.0.1:13336,127.0.0.1:13336,127.0.0.1:13336 --ingress-external-addr=127.0.0.1:13335,127.0.0.1:13335,127.0.0.1:13335 --ingress-pwd-dir=ingress_keys "$@"
