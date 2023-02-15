package main

import (
	"log"
	"time"

	"github.com/vkcom/statshouse/internal/stats"
)

func main() {
	// todo replace with good hostname detection
	collector, err := stats.NewCollectorManager(stats.CollectorManagerOptions{ScrapeInterval: time.Second})
	if err != nil {
		log.Panic(err)
	}
	defer collector.StopCollector()
	err = collector.RunCollector()
	if err != nil {
		panic(err)
	}
}
