#!/bin/bash
set -e
if [ -z "$STATSHOUSE_DEBUG" ]; then
  /bin/statshouse agent --cluster=test_shard_aggregator --log-level=trace --remote-write-enabled \
   --agg-addr="aggregator:13336,aggregator:13336,aggregator:13336=" --cache-dir=/var/lib/statshouse/cache/agent \
   -u=root -g=root &
else
  /bin/dlv exec --headless --listen=:8000 --api-version=2 /bin/statshouse -- agent --cluster=test_shard_aggregator --log-level=trace --remote-write-enabled \
   --agg-addr="aggregator:13336,aggregator:13336,aggregator:13336=" --cache-dir=/var/lib/statshouse/cache/agent \
   -u=root -g=root &
fi
/bin/consul agent -data-dir=/consul/data -config-dir=/consul/config &
/bin/node_exporter &
wait -n
exit $?
