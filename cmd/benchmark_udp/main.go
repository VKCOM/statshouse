package main

import (
	"crypto/sha1"
	"flag"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/receiver"

	"go.uber.org/atomic"
)

// mode0 - multiple goroutines reading N independent sockets created with REUSE_PORT
// mode1 - multiple goroutines reading FD dup of single socket
// mode2 - multiple goroutines reading single net.Conn

// UDP (unix dgram has similar performance)
// packet_size cores_udp mode0 mode1 mode2 (MB/sec)
// --------------------------------------
// 400         1         252   249   234
// 400         2         426   423   288
// 400         3         604   585   325
// 400         4         580   670   343
// 400         6         360   262   386
// 400         8         277   234   400
// --------------------------------------
// 4000        1         268   260   285
// 4000        2         466   495   400
// 4000        3         598   633   455
// 4000        4         640   692   468
// 4000        6         350   321   532
// 4000        8         251   258   560
// --------------------------------------
// 40000       1         300   302   302
// 40000       2         561   578   577
// 40000       3         752   763   700
// 40000       4         742   708   659
// 40000       6         348   346   363
// 40000       8         309   290   257

type packetPrinter struct {
	total  atomic.Int64
	errors atomic.Int64
	haha   [20]byte
}

func (w *packetPrinter) HandleMetrics(args data_model.HandlerArgs) (h data_model.MappedMetricHeader, done bool) {
	// some random work
	ha := sha1.Sum(args.MetricBytes.Name)
	for i, b := range ha {
		w.haha[i] ^= b
	}
	w.total.Inc()
	return h, true
}

func (w *packetPrinter) HandleParseError(pkt []byte, err error) {
	w.errors.Inc()
}

func main() {
	os.Exit(receiveBenchmark())
}

func receiveBenchmark() int {
	go func() {
		_ = http.ListenAndServe(":11123", nil)
	}()

	var coresUDP int
	var maxCores int
	var bufferSizeUDP int
	var network string
	var listenAddr string
	var coresSend int
	var packetSize int
	var mode int
	flag.StringVar(&network, "network", "udp4", "UDP listen network (udp4, udp5, unixgram)")
	flag.StringVar(&listenAddr, "p", "127.0.0.1:13337", "UDP listen address")
	flag.IntVar(&coresUDP, "cores-udp", 1, "CPU cores to use for udp receiving. 0 switches UDP off")
	flag.IntVar(&bufferSizeUDP, "buffer-size-udp", receiver.DefaultConnBufSize, "UDP receiving buffer size")
	flag.IntVar(&maxCores, "cores", -1, "CPU cores usage limit. 0 all available, <0 use (cores-udp*3/2 + 1)")
	flag.IntVar(&coresSend, "cores-send", 1, "Generate packets using this # of cores")
	flag.IntVar(&packetSize, "packet-size", 32768, "Generate ackets approximately this size")
	flag.IntVar(&mode, "mode", 0, "0 - multiple UDP sockets, 1 - multiple FD (dup), other - reading same socket from many goroutines")
	flag.Parse()

	if coresUDP < 0 {
		log.Printf("--cores-udp must be set to at least 0")
		return 1
	}
	if maxCores < 0 {
		maxCores = 1 + coresUDP*3/2
	}
	if maxCores > 0 {
		runtime.GOMAXPROCS(maxCores)
	}

	var (
		receiversUDP []*receiver.UDP
	)

	for i := 0; i < coresUDP; i++ {
		var u *receiver.UDP
		var err error
		action := "created"
		if mode == 1 && i != 0 {
			u, err = receiversUDP[0].Duplicate()
			action = "duplicated"
		} else {
			u, err = receiver.ListenUDP(network, listenAddr, 10000000, coresUDP > 1 && mode == 0, nil, nil, nil)
		}
		if err != nil {
			log.Printf("ListenUDP: %v", err)
			return 1
		}
		log.Printf("UDP Listener %d %s, buffer size %d", i, action, u.ReceiveBufferSize())
		defer func() { _ = u.Close() }()
		receiversUDP = append(receiversUDP, u)
		if mode != 0 && mode != 1 {
			break
		}
	}
	//if argv.listenAddrUnix != "" {
	//	u, err := receiver.ListenUDP("unixgram", argv.listenAddrUnix, argv.bufferSizeUDP, argv.coresUDP > 1, sh2, logPackets)
	//	if err != nil {
	//		logErr.Printf("ListenUDP Unix: %v", err)
	//		return 1
	//	}
	//	defer func() { _ = u.Close() }()
	//	receiversUDP = append(receiversUDP, u)
	//}

	w := &packetPrinter{}

	if mode != 0 && mode != 1 {
		for i := 0; i < coresUDP; i++ {
			go func() {
				err := receiversUDP[0].Serve(w)
				if err != nil {
					log.Fatalf("Serve: %v", err)
				}
			}()
		}
	} else {
		for _, u := range receiversUDP {
			go func(u *receiver.UDP) {
				err := u.Serve(w)
				if err != nil {
					log.Fatalf("Serve: %v", err)
				}
			}(u)
		}
	}

	sentBytes := atomic.Int64{}

	go func() {
		previous := w.total.Load()
		previousSentByte := sentBytes.Load()
		previousReceivedBytes := uint64(0)
		for {
			current := w.total.Load()
			currentSentByte := sentBytes.Load()
			currentReceivedBytes := uint64(0)
			for _, u := range receiversUDP {
				currentReceivedBytes += u.StatBytesTotal()
			}
			log.Printf("%8.2f MB/sec sent %8.2f MB/sec received %10d metrics/sec (%d errors)",
				float64(currentSentByte-previousSentByte)/1024/1024,
				float64(currentReceivedBytes-previousReceivedBytes)/1024/1024,
				current-previous, w.errors.Load())
			previous = current
			previousSentByte = currentSentByte
			previousReceivedBytes = currentReceivedBytes
			time.Sleep(time.Second)
		}
	}()

	metric := tlstatshouse.Metric{
		Name: "metric",
		Tags: map[string]string{"0": "zero", "1": "one", "2": "twp", "3": "three"},
	}
	metric.SetCounter(1)
	metric.SetValue([]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	metric.SetTs(1)
	batch := tlstatshouse.AddMetricsBatch{
		Metrics: []tlstatshouse.Metric{metric, metric, metric, metric, metric, metric, metric, metric, metric, metric},
	}
	metricBytes := batch.WriteBoxed(nil)
	packet := metricBytes
	for len(packet)+len(metricBytes) < packetSize {
		packet = append(packet, metricBytes...)
	}
	log.Printf("packet size requested %d actual %d", packetSize, len(packet))

	sender := func(i int) {
		var conns []net.Conn
		for i := 0; i < 32; i++ {
			conn, err := net.Dial(network, listenAddr)
			if err != nil {
				log.Fatalf("failed to dial statshouse: %v", err)
			}
			conns = append(conns, conn)
		}
		log.Printf("UDP Sender created: %d", i)
		next := 0
		for {
			_, err := conns[next].Write(packet)
			next = (next + 1) % len(conns)
			if err != nil {
				log.Printf("failed to write statshouse: %v", err)
			}
			sentBytes.Add(int64(len(packet)))
		}
	}

	for i := 0; i < coresSend; i++ {
		go func(k int) {
			sender(k)
		}(i)
	}

	chSignal := make(chan os.Signal, 1)
	signal.Notify(chSignal, syscall.SIGINT)

loop:
	for {
		sig := <-chSignal
		switch sig {
		case syscall.SIGINT:
			log.Printf("Shutting down...")
			break loop
		}
	}
	log.Printf("Bye")
	return 0
}
