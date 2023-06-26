package main

import (
	"log"
	"os"
	"time"

	"github.com/vkcom/statshouse/internal/stats"
)

func main() {
	host, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	collector, err := stats.NewCollectorManager(stats.CollectorManagerOptions{ScrapeInterval: time.Second, HostName: host}, nil, log.New(os.Stderr, "[collector]", 0))
	if err != nil {
		log.Panic(err)
	}
	defer collector.StopCollector()
	err = collector.RunCollector()
	if err != nil {
		panic(err)
	}
}
