# Local clickhouse cluster
 
To check new clickhouse versions

## How to start
Start clickhouse cluster locally
```
docker compose up --force-recreate --renew-anon-volumes --detach
```

Create tables
```
clickhouse-client --queries-file ../../build/cluster.sql
```

After you finish. Stop cluster
```
docker compose down --volumes
```

