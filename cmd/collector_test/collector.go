package main

import (
	"log"
	"os"
	"time"

	"github.com/vkcom/statshouse/internal/stats"
)

func main() {
	//todo fix
	collector, err := stats.NewCollectorManager(stats.CollectorManagerOptions{ScrapeInterval: time.Second, HostName: "dev-host"}, nil, log.New(os.Stderr, "[collector]", 0))
	if err != nil {
		log.Panic(err)
	}
	defer collector.StopCollector()
	err = collector.RunCollector()
	if err != nil {
		panic(err)
	}
}
