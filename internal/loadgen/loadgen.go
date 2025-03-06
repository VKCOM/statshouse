package loadgen

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"pgregory.net/rand"

	"github.com/vkcom/statshouse/internal/api"
	"github.com/vkcom/statshouse/internal/format"

	"github.com/vkcom/statshouse-go"
)

func RunClientLoad() {
	baseCard := 1
	if len(os.Args) >= 3 {
		card, err := strconv.Atoi(os.Args[2])
		if err != nil {
			log.Fatalf("Invalid card number %s: %v", os.Args[2], err)
		}
		baseCard = card
	}
	ctx := makeInterruptibleContext()
	apiClient := api.NewClient("http://127.0.0.1:10888", "loadgen")
	sh := statshouse.NewClient(log.Printf, "tcp", statshouse.DefaultAddr, "")
	g := Generator{
		rng:     rand.New(),
		clients: []*statshouse.Client{sh},
	}

	res1Slices := make([][]GenericMetric, baseCard)
	res15Slices := make([][]GenericMetric, baseCard)
	res60Slices := make([][]GenericMetric, baseCard)
	wasLen := 0
	for idx := 1; idx <= baseCard; idx++ {
		addMetrics(&g, 1, idx)
		res1Slices[idx-1] = g.metrics[wasLen:len(g.metrics)]
		wasLen = len(g.metrics)
	}
	for idx := 1; idx <= baseCard; idx++ {
		addMetrics(&g, 15, idx)
		res15Slices[idx-1] = g.metrics[wasLen:len(g.metrics)]
		wasLen = len(g.metrics)
	}
	for idx := 1; idx <= baseCard; idx++ {
		addMetrics(&g, 60, idx)
		res60Slices[idx-1] = g.metrics[wasLen:len(g.metrics)]
		wasLen = len(g.metrics)
	}

	log.Print("Ensure metrics exist")
	created := map[string]bool{}
	for _, metric := range g.metrics {
		if _, ok := created[metric.Name()]; !ok {
			metric.Ensure(ctx, apiClient)
			created[metric.Name()] = true
		}
	}
	created = nil
	log.Print("Ensure dashboard exist")
	err := EnsureDashboardExists(ctx, apiClient)
	if err != nil {
		log.Fatalf("Failed to ensure dashboard: %v", err)
	}
	log.Print("Running load on agent via StatsHouse client")
	// 10 writes per resolution period
	for _, res1Slice := range res1Slices {
		go g.goRun(ctx, 100*time.Millisecond, res1Slice)
	}
	for _, res15Slice := range res15Slices {
		go g.goRun(ctx, 1500*time.Millisecond, res15Slice)
	}
	for _, res60Slice := range res60Slices {
		go g.goRun(ctx, 6000*time.Millisecond, res60Slice)
	}

	<-ctx.Done()
	log.Print("Stopping...")
	_ = sh.Close()
	log.Print("DONE")
}

func addMetrics(g *Generator, resolution int, idx int) {
	// metrics that do not change tag values
	g.AddConstCounter(resolution, idx)
	g.AddConstValue(resolution, idx)
	g.AddConstPercentile(resolution, idx)
	g.AddConstValueHost(resolution, idx, "host_1")
	g.AddConstValueHost(resolution, idx, "host_2")
	// metrics with changing tag values
	g.AddChangingCounter(resolution, idx)
	g.AddChangingValue(resolution, idx)
	g.AddChangingPercentile(resolution, idx)
	g.AddChangingStringTop(resolution, idx, 10)
	g.AddChangingValueHost(resolution, idx)
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
	ensureMetricWithDescription(ctx, c, format.StatshouseAPIRemoteConfig, `
--version3-start=1
--version3-prob=1
`)
	ensureMetricWithDescription(ctx, c, format.StatshouseAgentRemoteConfigMetric, `
-conveyor-v3-staging=production,staging1,staging2,development
`)
	ensureMetricWithDescription(ctx, c, format.StatshouseAggregatorRemoteConfigMetric, `
-map-string-top=true
`)
	ensureMetricWithDescription(ctx, c, format.StatshouseJournalDump, `
`)
}

func ensureMetricWithDescription(ctx context.Context, client *api.Client, name, desc string) {
	m, err := client.GetMetric(ctx, name)
	tags := []format.MetricMetaTag{{
		Description: "environment",
	}}
	if err != nil {
		log.Printf("Failed to get metric: %v", err)
		m = &api.MetricInfo{
			Metric: format.MetricMetaValue{
				Name: name,
				Kind: format.MetricKindMixed,
				Tags: tags,
			},
		}
	}
	m.Metric.Description = desc
	m.Metric.Tags = tags
	err = client.PostMetric(ctx, m)
	if err != nil {
		log.Fatalf("Failed to post metric: %v", err)
	}
}

func RunSetSharding() {
	if len(os.Args) < 4 {
		fmt.Println("Usage: loadgen set-sharding <metric> <strategy> [<shard>]")
		os.Exit(1)
	}
	metric := os.Args[2]
	strategy := os.Args[3]
	var shard int
	var err error
	if len(os.Args) > 4 {
		shard, err = strconv.Atoi(os.Args[4])
		if err != nil || shard < 0 {
			log.Fatal("Invalid shard value: ", os.Args[4], err)
		}
	}
	ctx := makeInterruptibleContext()
	c := api.NewClient("http://127.0.0.1:10888", "loadgen")
	m, err := c.GetMetric(ctx, metric)
	if err != nil {
		log.Fatalf("Failed to get metric: %v", err)
	}
	m.Metric.ShardStrategy = strategy
	m.Metric.ShardNum = uint32(shard)
	err = c.PostMetric(ctx, m)
	if err != nil {
		log.Fatalf("Failed to post metric: %v", err)
	}
}
