package main

import (
	"log"
	"time"

	"github.com/vkcom/statshouse/internal/stats"
)

func main() {
	collector, err := stats.NewCollectorManager(stats.CollectorManagerOptions{ScrapeInterval: time.Second, HostName: "adm512"}, nil)
	if err != nil {
		log.Panic(err)
	}
	defer collector.StopCollector()
	err = collector.RunCollector()
	if err != nil {
		panic(err)
	}
}
