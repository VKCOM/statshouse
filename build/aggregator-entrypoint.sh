#!/bin/bash
/entrypoint.sh &
until clickhouse-client --query="SELECT shard_num, replica_num, is_local, host_name FROM system.clusters where cluster='statlogs2' and replica_num <= 3 order by shard_num, replica_num"; do sleep 1; done
/bin/statshouse "$@" &
wait -n
exit $?
