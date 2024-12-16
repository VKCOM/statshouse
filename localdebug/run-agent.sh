#!/bin/sh
set -e

mkdir -p cache/agent/
../target/statshouse -agent --hardware-metric-scrape-disable --cluster=statlogs2 --hostname=agent1 --agg-addr=127.0.0.1:13336,127.0.0.1:13346,127.0.0.1:13356 --cache-dir=cache/agent "$@"

# other useful options you sometimes want to add
# --cores-udp=2 --listen-addr-ipv6=[::1]:13337 --listen-addr-unix=@statshouse
