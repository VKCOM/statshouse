# Run StatsHouse locally for debug

## Run clickhouse locally in docker

We run cluster containing 1 clickhouse server plus 1 zookeeper instance.

Start clickhouse and create all necessary tables
```
clickhouse-cluster/start.sh
```

Stop clickhouse
```
clickhouse-cluster/stop.sh
```

Clear all tables from clickhouse mini cluster
```
clickhouse-cluster/cleanup.sh
```

## Run daemons locally without containers

Quickly rebuild you daemons if you are working on them (but not UI)
```
./build.sh
```

Next commands should be run in separate terminal tabs, one tab per daemon.

First run meta
```
./run-metadata.sh
```

Then run aggregator
```
./run-aggregator.sh
```

Then run API
```
./run-api.sh
```

Then go to http://localhost:10888 from your browser, this is minimal working system.

Then you can run agent
```
./run-agent.sh
```

Then you can run ingress proxy connected to aggregator, and second ingress connected through the first ingress.
Keys for ingress proxies are in dirs `ingress_keys` and `ingress_keys2`,
```
./run-ingress.sh
./run-ingress2.sh
```

Then you can run 2 agents connected through the first ingress using different crypto keys,
and another agent connected through second ingress.
```
./run-agent-ingress-key1.sh
./run-agent-ingress-key2.sh
./run-agent-ingress2-key3.sh
```

Scripts with the names `run-aggregator1.sh`, `run-aggregator2.sh`, `run-aggregator3.sh` are old scripts for testing
more complicated cluster with 3 aggregators, if you need to test such cluster, please first ask @hrissan to update them.

# Clean up

Daemons keep various metadata in their caches, so if you clean up meta, you must also clean caches.

You must stop all daemons before cleaning up. 
```
./cleanup.sh
```

If you only need to clean caches of daemons, but not medata use
```
rm -rf cache
```


## For testing v3 pipeline

Create remote_config metrics with correct flags
```
go run ./cmd/loadgen new-pipeline
```
Run load generator
```
go run ./cmd/loadgen client
```
