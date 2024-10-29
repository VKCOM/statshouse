package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"pgregory.net/rand"

	"github.com/vkcom/statshouse-go"
)

func randomString(rng *rand.Rand) string {
	length := rng.Intn(100)
	result := make([]byte, length)
	const totalLetters = 'z' - 'A'
	for i := 0; i < length; i++ {
		result[i] = 'A' + byte(rng.Intn(totalLetters))
	}
	return string(result)
}

func randomWalk(ctx context.Context, client *statshouse.Client, tags statshouse.NamedTags, metricsN int, randomTag bool) {
	ticker := time.NewTicker(time.Second)
	values := make([]float64, metricsN)
	rng := rand.New()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for m := 0; m < metricsN; m++ {
				metricTags := tags
				if randomTag {
					metricTags = append(metricTags, [2]string{"random", randomString(rng)})
				}
				client.NamedValue(fmt.Sprint(metricPrefix, m), metricTags, values[m])
				sign := float64(1)
				if rng.Int31n(2) == 1 {
					sign = float64(-1)
				}
				values[m] += rng.Float64() * sign
			}
		}
	}
}

func doesNotExist(ctx context.Context, client *statshouse.Client) {
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			client.Count("does_not_exist", statshouse.Tags{"dev"}, 100)
		}
	}
}

func main() {
	var (
		metricsN  int
		clientsN  int
		randomTag bool
		signals   = make(chan os.Signal, 1)
	)
	flag.IntVar(&metricsN, "m", 6, "number of metrics")
	flag.IntVar(&clientsN, "c", 2, "number of clients")
	flag.BoolVar(&randomTag, "r", false, "add random tag")
	flag.Parse()
	randomTagLog := ""
	if randomTag {
		randomTagLog = "with random tag"
	}
	log.Println("creating", clientsN, "clients that write", metricsN, "metrics", randomTagLog)

	ctx, cancel := context.WithCancel(context.Background())
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signals
		cancel()
	}()

	client := http.Client{}
	metricNames := make([]string, metricsN)
	for i := range metricNames {
		metricNames[i] = fmt.Sprint(metricPrefix, i)
	}
	ensureMetrics(ctx, client, metricNames)
	dashboardMetricN := metricsN
	if dashboardMetricN > 8 {
		log.Println("dashboard will contain only first 8 metircs")
		dashboardMetricN = 8
	}
	select {
	case <-ctx.Done():
		return
	default:
	}
	ensureDashboardExists(client, dashboardMetricN)

	var wg sync.WaitGroup
	for c := 0; c < clientsN; c++ {
		ci := c
		wg.Add(1)
		go func() {
			defer wg.Done()
			shClient := statshouse.NewClient(log.Printf, statshouse.DefaultNetwork, statshouse.DefaultAddr, "")
			randomWalk(ctx, shClient, statshouse.NamedTags{{"client", fmt.Sprint(ci)}}, metricsN, randomTag)
		}()
	}
	shClient := statshouse.NewClient(log.Printf, statshouse.DefaultNetwork, statshouse.DefaultAddr, "")
	wg.Add(1)
	go func() {
		defer wg.Done()
		doesNotExist(ctx, shClient)
	}()

	<-ctx.Done()
	log.Print("Stopping...")
	wg.Wait()
	log.Print("Stopped")
}
