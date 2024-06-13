package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"pgregory.net/rand"

	"github.com/vkcom/statshouse-go"
)

func main() {
	var (
		numberOfMetrics int
		done            = make(chan os.Signal, 1)
	)
	flag.IntVar(&numberOfMetrics, "m", 6, "number of metrics")
	flag.Parse()
	signal.Notify(done, os.Interrupt, syscall.SIGTERM)

	client := http.Client{}
	ensureDashboardExists(client, numberOfMetrics)

	ticker := time.NewTicker(time.Second)
	values := make([]float64, numberOfMetrics)
	rng := rand.New()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			for m := 0; m < numberOfMetrics; m++ {
				statshouse.MetricNamed(fmt.Sprint(randomWalkPrefix, m), statshouse.NamedTags{}).Value(values[m])
				sign := float64(1)
				if rng.Int31n(2) == 1 {
					sign = float64(-1)
				}
				values[m] += rng.Float64() * sign
			}
		}
	}
}
