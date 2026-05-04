#!/bin/sh
set -e

../target/statshouse-balancer --hostname=balancer1 --upstream-addr=127.0.0.1:13338 --listen-unix=:13337 --listen-tcp=:13337 --listen-udp4=:13337 "$@"
