package main

import (
	"log"
	"os"
	"time"

	"github.com/vkcom/statshouse/internal/stats"
)

func main() {
	// todo replace with good hostname detection
	h, err := os.Hostname()
	if err != nil {
		log.Panic(err)
	}
	pusher := stats.Pusher{HostName: h}
	cpuCollector, err := stats.NewCpuStats(pusher.PushCPUUsage)
	if err != nil {
		log.Panic(err)
	}
	for {
		time.Sleep(time.Second)
		err := cpuCollector.PushMetrics()
		if err != nil {
			log.Println("failed to push metrics", err)
		}

	}
}
