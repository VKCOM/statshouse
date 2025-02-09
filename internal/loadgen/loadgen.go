package loadgen

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/vkcom/statshouse/internal/api"
	"github.com/vkcom/statshouse/internal/format"
	"pgregory.net/rand"

	"github.com/vkcom/statshouse-go"
)

func RunClientLoad() {
	ctx := makeInterruptibleContext()
	apiClient := api.NewClient("http://127.0.0.1:10888", "loadgen")
	sh := statshouse.NewClient(log.Printf, "tcp", statshouse.DefaultAddr, "")
	g := Generator{
		rng:     rand.New(),
		clients: []*statshouse.Client{sh},
	}

	wasLen := 0
	addMetrics(&g, 1)
	res1Slice := g.metrics[wasLen:len(g.metrics)]
	wasLen = len(g.metrics)
	addMetrics(&g, 15)
	res15Slice := g.metrics[wasLen:len(g.metrics)]
	wasLen = len(g.metrics)
	addMetrics(&g, 60)
	res60Slice := g.metrics[wasLen:len(g.metrics)]

	log.Print("Ensure metrics exist")
	for _, metric := range g.metrics {
		metric.Ensure(ctx, apiClient)
	}
	log.Print("Ensure dashboard exist")
	err := EnsureDashboardExists(ctx, apiClient)
	if err != nil {
		log.Fatalf("Failed to ensure dashboard: %v", err)
	}
	log.Print("Running load on agent via StatsHouse client")
	// 1000 writes per resolution period
	go g.goRun(ctx, 1*time.Millisecond, res1Slice)
	go g.goRun(ctx, 15*time.Millisecond, res15Slice)
	go g.goRun(ctx, 60*time.Millisecond, res60Slice)

	<-ctx.Done()
	log.Print("Stopping...")
	_ = sh.Close()
	log.Print("DONE")
}

func addMetrics(g *Generator, resolution int) {
	// metrics that do not change tag values
	g.AddConstCounter(resolution)
	g.AddConstValue(resolution)
	g.AddConstPercentile(resolution)
	g.AddConstValueHost(resolution, "host_1")
	g.AddConstValueHost(resolution, "host_2")
	// metrics with changing tag values
	g.AddChangingCounter(resolution)
	g.AddChangingValue(resolution)
	g.AddChangingPercentile(resolution)
	g.AddChangingStringTop(resolution, 10)
	g.AddChangingValueHost(resolution)
}

func makeInterruptibleContext() context.Context {
	signals := make(chan os.Signal, 1)
	ctx, cancel := context.WithCancel(context.Background())
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signals
		cancel()
	}()
	return ctx
}

func RunEnableNewPipeline() {
	ctx := makeInterruptibleContext()
	c := api.NewClient("http://127.0.0.1:10888", "loadgen")
	ensureMetricWithDescription(ctx, c, "statshouse_api_remote_config", `
--version3-start=1
--version3-prob=1
`)
	ensureMetricWithDescription(ctx, c, "statshouse_agent_remote_config", `
-new-conveyor=true
`)
	ensureMetricWithDescription(ctx, c, "statshouse_aggregator_remote_config", `
-map-string-top=true
`)
}

func ensureMetricWithDescription(ctx context.Context, client *api.Client, name, desc string) {
	m, err := client.GetMetric(ctx, name)
	if err != nil {
		log.Printf("Failed to get metric: %v", err)
		m = &api.MetricInfo{
			Metric: format.MetricMetaValue{
				Name: name,
				Kind: format.MetricKindMixed,
			},
		}
	}
	m.Metric.Description = desc
	err = client.PostMetric(ctx, m)
	if err != nil {
		log.Fatalf("Failed to post metric: %v", err)
	}
}
