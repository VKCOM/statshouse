#!/bin/sh
set -e

../target/statshouse-igp --cluster=statlogs2 --hostname=ingress --ingress-addr=127.0.0.1:13327 --agg-addr=127.0.0.1:13336,127.0.0.1:13346,127.0.0.1:13356 --ingress-external-addr=127.0.0.1:13327,127.0.0.1:13327,127.0.0.1:13327 --ingress-pwd-dir=ingress_keys "$@"
