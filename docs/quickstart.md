# StatsHouse Quick Start Guide

## Using Docker
### Running StatsHouse
The command below will start a local StatsHouse instance and open the UI once ready.
```shell
./localrun.sh
```

[`statshouse-example.go`](../cmd/statshouse-example/statshouse-example.go) contains a simple instrumented web server.
Run it to start sending metrics to the StatsHouse instance you've just launched:
```shell
go run ./cmd/statshouse-example/statshouse-example.go
```

### Сlearing used disk space
Docker commands below assume StatsHouse source code is located under `statshouse` directory. Change `--filter` option value to `label=com.docker.compose.project=<name_of_directory_containing_StatsHouse_source_code>` if it's not the case.

`./localrun.sh` uses Docker Compose volumes to persist data between runs. Use the following command to delete volumes and free up disk space:
```shell
docker volume prune --force --filter label=com.docker.compose.project=statshouse
```

Run the following command to delete all Docker resources specific to StatsHouse:
```shell
docker system prune --force --volumes --filter label=com.docker.compose.project=statshouse
```

## Local debug build

See details in [localdebug](../localdebug/README.md)
