# StatsHouse Quick Start Guide

## Using tmux
### Requirements
- docker 
- docker compose v2
- golang - for backend
- nodejs + npm - for frontend
- tmux

### Running StatsHouse
The commands below will start a local StatsHouse instance and open the UI once ready.
```shell
cd localdebug
./run-in-tmux.sh
```
Also you should have loadgen dashboard avaliable with different kinds of plots.

### Example
[`statshouse-example.go`](../cmd/statshouse-example/statshouse-example.go) contains a simple instrumented web server.
Run it to start sending metrics to the StatsHouse instance you've just launched:
```shell
go run ./cmd/statshouse-example/statshouse-example.go
```

### Сlearing used disk space
```shell
cd localdebug
./cleanup.sh
```

## Local debug build

See details in [localdebug](../localdebug/README.md)
