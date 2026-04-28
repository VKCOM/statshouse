package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/VKCOM/statshouse/internal/balancer"
)

var argv struct {
	upstreamAddr      string
	hostName          string
	listenUDP4        string
	listenUDP6        string
	listenUnixgram    string
	listenTCP         string
	coresUDP          int
	udpBufferSize     int
	dnsRefreshSeconds int
	queueSize         int
	pprofListenAddr   string
}

func main() {
	runtime.GOMAXPROCS(1)
	parseFlags()
	if argv.pprofListenAddr != "" {
		go func() {
			if err := http.ListenAndServe(argv.pprofListenAddr, nil); err != nil {
				log.Printf("failed to listen pprof on %q: %v", argv.pprofListenAddr, err)
			}
		}()
	}

	cfg := balancer.Config{
		HostName:       argv.hostName,
		UpstreamAddr:   argv.upstreamAddr,
		ListenUDP4:     argv.listenUDP4,
		ListenUDP6:     argv.listenUDP6,
		ListenUnixgram: argv.listenUnixgram,
		ListenTCP:      argv.listenTCP,
		CoresUDP:       argv.coresUDP,
		UDPBufferSize:  argv.udpBufferSize,
		Egress: balancer.EgressConfig{
			Network:            "tcp",
			DNSRefreshInterval: time.Duration(argv.dnsRefreshSeconds) * time.Second,
		},
		Handler: balancer.HandlerConfig{},
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	svc, err := balancer.New(cfg)
	if err != nil {
		log.Printf("failed to initialize balancer: %v", err)
		os.Exit(1)
	}
	defer func() {
		_ = svc.Close()
	}()

	if err := svc.Run(ctx); err != nil {
		log.Printf("balancer failed: %v", err)
		os.Exit(1)
	}
}

func parseFlags() {
	flag.StringVar(&argv.upstreamAddr, "upstream-addr", "127.0.0.1:13338,127.0.0.1:13338", "comma-separated upstream agent tcp addresses or DNS names")
	flag.StringVar(&argv.hostName, "hostname", "", "override auto-detected host tag (_h)")
	flag.StringVar(&argv.listenUDP4, "listen-udp4", ":13337", "udp4 listen address")
	flag.StringVar(&argv.listenUDP6, "listen-udp6", "", "udp6 listen address")
	flag.StringVar(&argv.listenUnixgram, "listen-unixgram", "", "unixgram listen path")
	flag.StringVar(&argv.listenTCP, "listen-tcp", ":13337", "shared tcp listen address for raw TCP, HTTP and RPC (agent-compatible)")
	flag.IntVar(&argv.coresUDP, "cores-udp", 1, "CPU cores to use for udp receiving. 0 switches UDP off")
	flag.IntVar(&argv.udpBufferSize, "buffer-size-udp", 16*1024*1024, "udp receive buffer size")
	flag.IntVar(&argv.dnsRefreshSeconds, "dns-refresh-seconds", 60, "upstream dns refresh interval in seconds")
	flag.IntVar(&argv.queueSize, "queue-size", 1, "bounded queue size for handler")
	flag.StringVar(&argv.pprofListenAddr, "pprof-listen", "", "pprof listen address (disabled when empty)")
	flag.Parse()
	if argv.coresUDP < 0 {
		log.Fatalf("--cores-udp must be >= 0")
	}
	if argv.dnsRefreshSeconds < 0 {
		log.Fatalf("--dns-refresh-seconds must be >= 0")
	}
}
