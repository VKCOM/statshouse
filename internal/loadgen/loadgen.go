package loadgen

import (
	"context"
	"flag"
	"fmt"
	"github.com/vkcom/statshouse/internal/api"
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

func RunLegacy() {
	var (
		metricsN  int
		clientsN  int
		randomTag bool
		agentAddr string
	)
	flag.IntVar(&metricsN, "m", 6, "number of metrics")
	flag.IntVar(&clientsN, "c", 2, "number of clients")
	flag.BoolVar(&randomTag, "r", false, "add random tag")
	flag.StringVar(&agentAddr, "a", statshouse.DefaultAddr, "where to send metrics")
	flag.Parse()
	randomTagLog := ""
	if randomTag {
		randomTagLog = "with random tag"
	}
	log.Println("creating", clientsN, "clients that write", metricsN, "metrics", randomTagLog)

	ctx := makeInterruptibleContext()

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
			shClient := statshouse.NewClient(log.Printf, statshouse.DefaultNetwork, agentAddr, "")
			randomWalk(ctx, shClient, statshouse.NamedTags{{"client", fmt.Sprint(ci)}}, metricsN, randomTag)
		}()
	}
	shClient := statshouse.NewClient(log.Printf, statshouse.DefaultNetwork, agentAddr, "")
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(time.Second)
		stagValues := []string{"one", "two", "three"}
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				shClient.Count("does_not_exist", statshouse.Tags{"dev"}, 100)
				for i := range stagValues {
					shClient.StringsTop("string_top", statshouse.Tags{}, stagValues[i:])
				}
			}
		}
	}()

	<-ctx.Done()
	log.Print("Stopping...")
	wg.Wait()
	log.Print("Stopped")
}

func RunAgentLoad() {
	ctx := makeInterruptibleContext()
	apiClent := api.NewClient("http://127.0.0.1:10888", "loadgen")
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
		metric.Ensure(ctx, apiClent)
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
	// metrics with changing tag values
	g.AddChangingCounter(resolution)
	g.AddChangingValue(resolution)
	g.AddChangingPercentile(resolution)
	g.AddChangingStringTop(resolution, 10)
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
