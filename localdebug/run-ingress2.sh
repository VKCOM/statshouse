#!/bin/sh
set -e

../target/statshouse-igp --cluster=statlogs2 --hostname=ingress2 --ingress-addr=127.0.0.1:13326 --agg-addr=127.0.0.1:13327,127.0.0.1:13327,127.0.0.1:13327 --ingress-external-addr=127.0.0.1:13326,127.0.0.1:13326,127.0.0.1:13326 --ingress-pwd-dir=ingress_keys2 --aes-pwd-file=ingress_keys/key1.txt "$@"
