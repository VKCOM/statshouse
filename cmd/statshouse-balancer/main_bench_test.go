package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"math/rand"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VKCOM/tl/pkg/rpc"

	"github.com/VKCOM/statshouse/internal/balancer"
	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/VKCOM/statshouse/internal/receiver"
)

func benchHTTPClient() *http.Client {
	return &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 64,
			IdleConnTimeout:     90 * time.Second,
			ForceAttemptHTTP2:   false,
		},
	}
}

type benchEnv struct {
	cancel context.CancelFunc
	wg     sync.WaitGroup
	svc    *balancer.Service

	upstreamLn net.Listener
	upstreamWG sync.WaitGroup

	tcpAddr string
	udpAddr string
}

/*
BenchmarkBalancerIngress/udp-14         	  365722	      3231 ns/op	       6 B/op	       0 allocs/op
BenchmarkBalancerIngress/tcp-14         	  402336	      3061 ns/op	       4 B/op	       0 allocs/op
BenchmarkBalancerIngress/http-14        	   28711	     42040 ns/op	    9762 B/op	     101 allocs/op
BenchmarkBalancerIngress/rpc-14         	   43365	     27079 ns/op	      29 B/op	       1 allocs/op
*/
func BenchmarkBalancerIngress(b *testing.B) {
	env := newBenchEnv(b)
	defer env.Close(b)

	payload := buildTLPacketPayload()
	framed := framePacket(payload)

	b.Run("udp", func(b *testing.B) {
		conn, err := net.Dial("udp", env.udpAddr)
		if err != nil {
			b.Fatalf("dial udp: %v", err)
		}
		defer conn.Close()

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := conn.Write(payload); err != nil {
				b.Fatalf("write udp: %v", err)
			}
		}
	})

	b.Run("tcp", func(b *testing.B) {
		conn, err := net.Dial("tcp", env.tcpAddr)
		if err != nil {
			b.Fatalf("dial tcp: %v", err)
		}
		defer conn.Close()
		if _, err := conn.Write([]byte(receiver.TCPPrefix)); err != nil {
			b.Fatalf("write tcp prefix: %v", err)
		}

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := conn.Write(framed); err != nil {
				b.Fatalf("write tcp frame: %v", err)
			}
		}
	})

	b.Run("http", func(b *testing.B) {
		client := benchHTTPClient()
		url := "http://" + env.tcpAddr + receiver.StatshouseHTTPV1Endpoint

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			resp, err := client.Post(url, "application/octet-stream", bytes.NewReader(payload))
			if err != nil {
				b.Fatalf("http post: %v", err)
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				b.Fatalf("unexpected status %d", resp.StatusCode)
			}
		}
	})

	b.Run("rpc", func(b *testing.B) {
		client := rpc.NewClient(rpc.ClientWithProtocolVersion(1))
		ctx := context.Background()
		tlClient := tlstatshouse.Client{
			Client:  client,
			Network: "tcp4",
			Address: env.tcpAddr,
			Timeout: 2 * time.Second,
		}
		args := tlstatshouse.AddMetricsBatchBytes{
			Metrics: []tlstatshouse.MetricBytes{
				{
					Name:       []byte("bench.metric.rpc"),
					FieldsMask: 1 << 1, // value
					Value:      []float64{1},
				},
			},
		}

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var ret tlstatshouse.AddMetricsBatch__Result
			if err := tlClient.AddMetricsBatchBytes(ctx, args, nil, &ret); err != nil {
				b.Fatalf("rpc addMetricsBatch: %v", err)
			}
		}
	})
}

/*
BenchmarkBalancerIngressParallel/udp_parallel-14         	  731035	      1641 ns/op	       0 B/op	       0 allocs/op
BenchmarkBalancerIngressParallel/tcp_parallel-14         	  668817	      4851 ns/op	     145 B/op	       2 allocs/op
BenchmarkBalancerIngressParallel/http_parallel-14        	   58188	     20181 ns/op	    8599 B/op	     102 allocs/op
BenchmarkBalancerIngressParallel/rpc_parallel-14         	   56852	     21265 ns/op	     224 B/op	       1 allocs/op
*/
func BenchmarkBalancerIngressParallel(b *testing.B) {
	env := newBenchEnv(b)
	defer env.Close(b)

	payload := buildTLPacketPayload()
	framed := framePacket(payload)

	b.Run("udp_parallel", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			conn, err := net.Dial("udp", env.udpAddr)
			if err != nil {
				b.Errorf("dial udp: %v", err)
				return
			}
			defer conn.Close()
			for pb.Next() {
				if _, err := conn.Write(payload); err != nil {
					b.Errorf("write udp: %v", err)
					return
				}
			}
		})
	})

	b.Run("tcp_parallel", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			conn, err := net.Dial("tcp", env.tcpAddr)
			if err != nil {
				b.Errorf("dial tcp: %v", err)
				return
			}
			defer conn.Close()
			if _, err := conn.Write([]byte(receiver.TCPPrefix)); err != nil {
				b.Errorf("write tcp prefix: %v", err)
				return
			}
			for pb.Next() {
				if _, err := conn.Write(framed); err != nil {
					b.Errorf("write tcp frame: %v", err)
					return
				}
			}
		})
	})

	b.Run("http_parallel", func(b *testing.B) {
		// Default RunParallelism can exhaust ephemeral ports on localhost (macOS).
		b.SetParallelism(4)
		url := "http://" + env.tcpAddr + receiver.StatshouseHTTPV1Endpoint
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			client := benchHTTPClient()
			for pb.Next() {
				resp, err := client.Post(url, "application/octet-stream", bytes.NewReader(payload))
				if err != nil {
					b.Errorf("http post: %v", err)
					return
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					b.Errorf("unexpected status %d", resp.StatusCode)
					return
				}
			}
		})
	})

	b.Run("rpc_parallel", func(b *testing.B) {
		b.SetParallelism(4)
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			client := rpc.NewClient(rpc.ClientWithProtocolVersion(1))
			ctx := context.Background()
			tlClient := tlstatshouse.Client{
				Client:  client,
				Network: "tcp4",
				Address: env.tcpAddr,
				Timeout: 2 * time.Second,
			}
			args := tlstatshouse.AddMetricsBatchBytes{
				Metrics: []tlstatshouse.MetricBytes{
					{
						Name:       []byte("bench.metric.rpc.parallel"),
						FieldsMask: 1 << 1,
						Value:      []float64{1},
					},
				},
			}
			for pb.Next() {
				var ret tlstatshouse.AddMetricsBatch__Result
				if err := tlClient.AddMetricsBatchBytes(ctx, args, nil, &ret); err != nil {
					b.Errorf("rpc addMetricsBatch: %v", err)
					return
				}
			}
		})
	})
}

/*
BenchmarkBalancerMatrix/size_matrix/udp_256-14 	  324715	      3518 ns/op	  68.23 MB/s	       6 B/op	       0 allocs/op
BenchmarkBalancerMatrix/size_matrix/udp_4096-14         	  322670	      3731 ns/op	1093.61 MB/s	       4 B/op	       0 allocs/op
BenchmarkBalancerMatrix/size_matrix/udp_8192-14         	  350787	      3563 ns/op	2290.07 MB/s	       4 B/op	       0 allocs/op
BenchmarkBalancerMatrix/size_matrix/tcp_256-14          	  375880	      3455 ns/op	  70.61 MB/s	      54 B/op	       1 allocs/op
BenchmarkBalancerMatrix/size_matrix/tcp_4096-14         	   78487	     15911 ns/op	 256.67 MB/s	      88 B/op	       1 allocs/op
BenchmarkBalancerMatrix/size_matrix/tcp_32768-14        	    9380	    124866 ns/op	 262.20 MB/s	    1387 B/op	      27 allocs/op
BenchmarkBalancerMatrix/mixed_traffic-14                	  151458	      7290 ns/op	     341 B/op	       3 allocs/op
BenchmarkBalancerMatrix/scaling_tcp_parallelism/p_1-14  	  785067	      3836 ns/op	     140 B/op	       2 allocs/op
BenchmarkBalancerMatrix/scaling_tcp_parallelism/p_4-14  	  496671	      3090 ns/op	     188 B/op	       2 allocs/op
BenchmarkBalancerMatrix/scaling_tcp_parallelism/p_16-14 	  376490	      4011 ns/op	     249 B/op	       2 allocs/op
*/
func BenchmarkBalancerMatrix(b *testing.B) {
	env := newBenchEnv(b)
	defer env.Close(b)

	b.Run("size_matrix", func(b *testing.B) {
		// UDP datagram size must stay below typical loopback/jumbo limits (macOS).
		for _, sz := range []int{256, 4096, 8192} {
			sz := sz
			b.Run("udp_"+strconv.Itoa(sz), func(b *testing.B) {
				payload := buildTLPacketPayloadSized(sz)
				conn, err := net.Dial("udp", env.udpAddr)
				if err != nil {
					b.Fatalf("dial udp: %v", err)
				}
				defer conn.Close()
				b.SetBytes(int64(len(payload)))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := conn.Write(payload); err != nil {
						b.Fatalf("write udp: %v", err)
					}
				}
			})
		}
		for _, sz := range []int{256, 4096, 32768} {
			sz := sz
			b.Run("tcp_"+strconv.Itoa(sz), func(b *testing.B) {
				payload := buildTLPacketPayloadSized(sz)
				framed := framePacket(payload)
				conn, err := net.Dial("tcp", env.tcpAddr)
				if err != nil {
					b.Fatalf("dial tcp: %v", err)
				}
				defer conn.Close()
				if _, err := conn.Write([]byte(receiver.TCPPrefix)); err != nil {
					b.Fatalf("write tcp prefix: %v", err)
				}
				b.SetBytes(int64(len(framed)))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := conn.Write(framed); err != nil {
						b.Fatalf("write tcp frame: %v", err)
					}
				}
			})
		}
	})

	b.Run("mixed_traffic", func(b *testing.B) {
		payload := buildTLPacketPayload()
		framed := framePacket(payload)
		tcpConn, err := net.Dial("tcp", env.tcpAddr)
		if err != nil {
			b.Fatalf("dial tcp: %v", err)
		}
		defer tcpConn.Close()
		if _, err := tcpConn.Write([]byte(receiver.TCPPrefix)); err != nil {
			b.Fatalf("write tcp prefix: %v", err)
		}
		udpConn, err := net.Dial("udp", env.udpAddr)
		if err != nil {
			b.Fatalf("dial udp: %v", err)
		}
		defer udpConn.Close()
		httpClient := benchHTTPClient()
		httpURL := "http://" + env.tcpAddr + receiver.StatshouseHTTPV1Endpoint

		rpcClient := rpc.NewClient(rpc.ClientWithProtocolVersion(1))
		defer rpcClient.Close()
		tlRPC := tlstatshouse.Client{
			Client:  rpcClient,
			Network: "tcp4",
			Address: env.tcpAddr,
			Timeout: 2 * time.Second,
		}
		rpcArgs := tlstatshouse.AddMetricsBatchBytes{
			Metrics: []tlstatshouse.MetricBytes{
				{Name: []byte("bench.mix.rpc"), FieldsMask: 1 << 1, Value: []float64{1}},
			},
		}

		rng := rand.New(rand.NewSource(42))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			v := rng.Intn(100)
			switch {
			case v < 70: // 70% udp
				if _, err := udpConn.Write(payload); err != nil {
					b.Fatalf("write udp: %v", err)
				}
			case v < 90: // 20% tcp
				if _, err := tcpConn.Write(framed); err != nil {
					b.Fatalf("write tcp: %v", err)
				}
			case v < 97: // 7% rpc (one shared client; avoids ephemeral port exhaustion)
				var ret tlstatshouse.AddMetricsBatch__Result
				if err := tlRPC.AddMetricsBatchBytes(context.Background(), rpcArgs, nil, &ret); err != nil {
					b.Fatalf("rpc: %v", err)
				}
			default: // 3% http
				resp, err := httpClient.Post(httpURL, "application/octet-stream", bytes.NewReader(payload))
				if err != nil {
					b.Fatalf("http post: %v", err)
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					b.Fatalf("status %d", resp.StatusCode)
				}
			}
		}
	})

	b.Run("scaling_tcp_parallelism", func(b *testing.B) {
		payload := framePacket(buildTLPacketPayload())
		for _, p := range []int{1, 4, 16} {
			p := p
			b.Run("p_"+strconv.Itoa(p), func(b *testing.B) {
				b.SetParallelism(p)
				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					conn, err := net.Dial("tcp", env.tcpAddr)
					if err != nil {
						b.Errorf("dial tcp: %v", err)
						return
					}
					defer conn.Close()
					if _, err := conn.Write([]byte(receiver.TCPPrefix)); err != nil {
						b.Errorf("write prefix: %v", err)
						return
					}
					for pb.Next() {
						if _, err := conn.Write(payload); err != nil {
							b.Errorf("write payload: %v", err)
							return
						}
					}
				})
			})
		}
	})
}

/*
go test ./cmd/statshouse-balancer -run '^$' -bench BenchmarkBalancerSubscribers -benchmem -benchtime=15s -count=2

	15        1065977056 ns/op           0.00 MB/s            14.19 batches/s    11293 B/op         62 allocs/op

2026/04/24 22:40:43 balancer stats: recv=1104 fwd=1104 drop=0 parse_err=0 reconnect_err=12 dns_err=0 write_err=0
*/
func BenchmarkBalancerSubscribers(b *testing.B) {
	env := newBenchEnv(b)
	defer env.Close(b)

	const subscribers = 15
	payload := buildTLPacketPayloadBatch(8)
	framed := framePacket(payload)

	conns := make([]net.Conn, 0, subscribers)
	for i := 0; i < subscribers; i++ {
		conn, err := net.Dial("tcp", env.tcpAddr)
		if err != nil {
			b.Fatalf("dial tcp subscriber %d: %v", i, err)
		}
		if _, err := conn.Write([]byte(receiver.TCPPrefix)); err != nil {
			_ = conn.Close()
			b.Fatalf("write tcp prefix subscriber %d: %v", i, err)
		}
		conns = append(conns, conn)
	}
	defer func() {
		for _, c := range conns {
			_ = c.Close()
		}
	}()

	var sentBatches atomic.Uint64
	errCh := make(chan error, subscribers)
	stopCh := make(chan struct{})
	var wg sync.WaitGroup
	now := time.Now()
	for i, conn := range conns {
		// Deterministic stagger over one second to emulate non-synchronized SDK send loops.
		offset := time.Duration(i) * (time.Second / subscribers)
		firstTick := now.Truncate(time.Second).Add(time.Second).Add(offset)
		initialDelay := time.Until(firstTick)
		if initialDelay < 0 {
			initialDelay = offset
		}

		wg.Add(1)
		go func(c net.Conn, startAfter time.Duration) {
			defer wg.Done()

			timer := time.NewTimer(startAfter)
			defer timer.Stop()
			select {
			case <-stopCh:
				return
			case <-timer.C:
			}
			if _, err := c.Write(framed); err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
			sentBatches.Add(1)

			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-stopCh:
					return
				case <-ticker.C:
					if _, err := c.Write(framed); err != nil {
						select {
						case errCh <- err:
						default:
						}
						return
					}
					sentBatches.Add(1)
				}
			}
		}(conn, initialDelay)
	}
	defer func() {
		close(stopCh)
		wg.Wait()
	}()

	b.SetBytes(int64(len(framed)))
	b.ReportAllocs()
	b.ResetTimer()

	start := time.Now()
	for i := 0; i < b.N; i++ {
		select {
		case err := <-errCh:
			b.Fatalf("subscriber write failed: %v", err)
		case <-time.After(time.Second):
		}
	}
	elapsed := time.Since(start).Seconds()
	if elapsed > 0 {
		b.ReportMetric(float64(sentBatches.Load())/elapsed, "batches/s")
	}
}

func newBenchEnv(tb testing.TB) *benchEnv {
	tb.Helper()
	upstreamLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("listen upstream: %v", err)
	}
	port := mustFreeTCPPort(tb)
	listen := "127.0.0.1:" + strconv.Itoa(port)

	cfg := balancer.Config{
		UpstreamAddr:   upstreamLn.Addr().String(),
		ListenUDP4:     listen,
		ListenTCP:      listen,
		UDPBufferSize:  4 * 1024 * 1024,
		CoresUDP:       1,
		ListenUDP6:     "",
		ListenUnixgram: "",
		Handler: balancer.HandlerConfig{
			QueueSize: 1,
		},
		Egress: balancer.EgressConfig{
			Network:            "tcp",
			DNSRefreshInterval: time.Hour,
		},
	}
	svc, err := balancer.New(cfg)
	if err != nil {
		tb.Fatalf("new balancer service: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	env := &benchEnv{
		cancel:     cancel,
		svc:        svc,
		upstreamLn: upstreamLn,
		tcpAddr:    listen,
		udpAddr:    listen,
	}

	env.upstreamWG.Add(1)
	go func() {
		defer env.upstreamWG.Done()
		env.serveUpstreamDiscard(upstreamLn)
	}()

	env.wg.Add(1)
	go func() {
		defer env.wg.Done()
		_ = svc.Run(ctx)
	}()
	waitForTCP(tb, listen, 2*time.Second)
	return env
}

func (e *benchEnv) Close(tb testing.TB) {
	tb.Helper()
	e.cancel()
	_ = e.svc.Close()
	_ = e.upstreamLn.Close()
	e.wg.Wait()
	e.upstreamWG.Wait()
}

func (e *benchEnv) serveUpstreamDiscard(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		func(c net.Conn) {
			defer c.Close()
			if !consumePrefix(c, receiver.TCPPrefix) {
				return
			}
			var sizeBuf [4]byte
			for {
				if _, err := io.ReadFull(c, sizeBuf[:]); err != nil {
					return
				}
				n := binary.LittleEndian.Uint32(sizeBuf[:])
				if _, err := io.CopyN(io.Discard, c, int64(n)); err != nil {
					return
				}
			}
		}(conn)
	}
}

func consumePrefix(r io.Reader, prefix string) bool {
	buf := make([]byte, len(prefix))
	if _, err := io.ReadFull(r, buf); err != nil {
		return false
	}
	return string(buf) == prefix
}

func buildTLPacketPayload() []byte {
	batch := tlstatshouse.AddMetricsBatchBytes{
		Metrics: []tlstatshouse.MetricBytes{
			{
				Name:       []byte("bench.metric"),
				FieldsMask: 1 << 1, // value
				Value:      []float64{1},
			},
		},
	}
	return batch.WriteTL1Boxed(nil)
}

func buildTLPacketPayloadBatch(metricCount int) []byte {
	if metricCount <= 1 {
		return buildTLPacketPayload()
	}
	batch := tlstatshouse.AddMetricsBatchBytes{
		Metrics: make([]tlstatshouse.MetricBytes, 0, metricCount),
	}
	for i := 0; i < metricCount; i++ {
		batch.Metrics = append(batch.Metrics, tlstatshouse.MetricBytes{
			Name:       []byte("bench.metric.batch." + strconv.Itoa(i)),
			FieldsMask: 1 << 1,
			Value:      []float64{1},
		})
	}
	return batch.WriteTL1Boxed(nil)
}

func buildTLPacketPayloadSized(target int) []byte {
	base := buildTLPacketPayload()
	if target <= len(base) {
		return base
	}
	out := make([]byte, 0, target)
	for len(out)+len(base) <= target {
		out = append(out, base...)
	}
	// Keep TL payload valid; do not append partial frame.
	if len(out) == 0 {
		out = append(out, base...)
	}
	return out
}

func framePacket(payload []byte) []byte {
	framed := make([]byte, 4+len(payload))
	binary.LittleEndian.PutUint32(framed[:4], uint32(len(payload)))
	copy(framed[4:], payload)
	return framed
}

func mustFreeTCPPort(tb testing.TB) int {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("reserve tcp port: %v", err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func waitForTCP(tb testing.TB, addr string, timeout time.Duration) {
	tb.Helper()
	deadline := time.Now().Add(timeout)
	for {
		conn, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		if time.Now().After(deadline) {
			tb.Fatalf("timeout waiting for tcp listener %s: %v", addr, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
