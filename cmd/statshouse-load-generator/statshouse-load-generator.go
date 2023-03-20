package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/vkcom/statshouse-go"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

var (
	i int64
	p = message.NewPrinter(language.English)
)

func printCounter() {
	log.Printf("#%s", p.Sprintf("%d", i))
}

func main() {
	var (
		n int64
		m string
		c = make(chan os.Signal, 1)
	)
	flag.Int64Var(&n, "n", 1_000_000, "Number of AccessMetric calls.")
	flag.StringVar(&m, "m", "load_generator_value", "Metric name.")
	flag.Parse()
	log.SetPrefix(fmt.Sprintf("%d %q ", os.Getpid(), m))
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	printCounter()
	defer printCounter()
	for ; i < n; i++ {
		select {
		case <-c:
			return
		default:
			statshouse.AccessMetric(m, statshouse.Tags{}).Value(float64(i))
		}
	}
}
