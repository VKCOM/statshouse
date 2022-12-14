package main

import (
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"runtime/metrics"
	"time"

	"github.com/vkcom/statshouse-go"
)

const html = `<!DOCTYPE html>
<html lang="en">
<head><title>StatsHouse Client Example</title></head>
<style>li {line-height: 1.5;}</style>
<body>
<h1>StatsHouse Client Example</h1>
<ul>
<li>Web server instrumented with a <a target="_blank" href="https://pkg.go.dev/github.com/vkcom/statshouse-go">StatsHouse client library for Go</a> is running</li>
<li>Backend reports the following metrics every time this page is served:
  <ul>
    <li>"example_metric_time" time spent writing response;</li>
    <li>"example_metric_bytes" response bytes written;</li>
  </ul>
</li>
<li>Metrics listed above have the following tags:
  <ul>
    <li>"path" holding "/", "/foo" or "/bar";</li>
    <li>"status" holding "OK" if succeeded and "NOK" if error occurred;</li>
  </ul>
</li>
<li>Metric named "example_metric_gc_heap_objects" is reported every second in background (number of objects, live or unswept, occupying heap memory);</li>
<li>Follow <a target="_blank" href="http://localhost:10888/view?f=-300&t=0&s=example_metric_gc_heap_objects&qw=avg">the link</a>
to view statistics on your local StatsHouse instance;</li>
<li>Run ApacheBench (or similar) testing tool on http://localhost:3333/ and watch the load.</li>
</ul>
</body>
</html>`

func handle(p string) {
	http.HandleFunc(p, func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		n, err := io.WriteString(w, html)
		elapsed := time.Since(start)
		var s string
		if err == nil {
			s = "OK"
		} else {
			s = "NOK"
		}
		statshouse.AccessMetric("example_metric_time", statshouse.Tags{{"path", p}, {"status", s}}).Value(float64(elapsed))
		statshouse.AccessMetric("example_metric_bytes", statshouse.Tags{{"path", p}, {"status", s}}).Value(float64(n))
	})
}

func main() {
	handle("/foo")
	handle("/bar")
	handle("/")
	id := statshouse.StartRegularMeasurement(func(r *statshouse.Registry) {
		s := []metrics.Sample{{Name: "/gc/heap/objects:objects"}}
		metrics.Read(s)
		r.AccessMetric("example_metric_gc_heap_objects", nil).Value(float64(s[0].Value.Uint64()))
	})
	defer statshouse.StopRegularMeasurement(id)
	// listen and serve
	l, err := net.Listen("tcp4", ":3333")
	if err != nil {
		log.Printf("error starting server: %v", err)
		return
	}
	log.Print("running on http://localhost:3333/")
	s := http.Server{}
	err = s.Serve(l)
	if errors.Is(err, http.ErrServerClosed) {
		log.Print("server closed")
	} else if err != nil {
		log.Printf("error starting server: %v", err)
	}
}
