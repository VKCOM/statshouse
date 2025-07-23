package main

import (
	"log"
	"os"
	"time"

	"github.com/VKCOM/statshouse/internal/env"
	"github.com/VKCOM/statshouse/internal/stats"
)

func main() {
	host, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	dir, err := os.UserHomeDir()
	if err != nil {
		panic(err)
	}
	envPath := dir + "/statshouse_env.yml"
	envLoader, _ := env.ListenEnvFile(envPath)
	collector, err := stats.NewCollectorManager(stats.CollectorManagerOptions{ScrapeInterval: time.Second, HostName: host}, nil, envLoader, log.New(os.Stderr, "[collector]", 0))
	if err != nil {
		log.Panic(err)
	}
	defer collector.StopCollector()
	err = collector.RunCollector()
	if err != nil {
		panic(err)
	}
}
