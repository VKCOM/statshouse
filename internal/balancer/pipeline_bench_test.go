package balancer

import (
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/VKCOM/statshouse/internal/receiver"
)

func BenchmarkEgressWritePacket(b *testing.B) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("listen upstream: %v", err)
	}
	defer ln.Close()

	done := make(chan struct{})
	go serveDiscardUpstream(ln, done)
	defer func() {
		_ = ln.Close()
		<-done
	}()

	eg := NewEgress(EgressConfig{
		Network:            "tcp",
		Address:            ln.Addr().String(),
		DNSRefreshInterval: time.Second,
		ReconnectDelay:     10 * time.Millisecond,
	})
	defer eg.Close()

	batch := buildBenchBatch(8)
	payload := batch.WriteTL1Boxed(nil)
	pkt := make([]byte, 4+len(payload))
	binary.LittleEndian.PutUint32(pkt[:4], uint32(len(payload)))
	copy(pkt[4:], payload)

	waitForEgressReady(b, eg, pkt)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		eg.WritePacket(pkt)
	}
	b.StopTimer()
}

func BenchmarkHandlerEncodeOnly(b *testing.B) {
	batch := buildBenchBatch(8)
	h := newHandler(HandlerConfig{HostTag: []byte("bench-host"), QueueSize: 1}, &Egress{})
	defer h.Close()
	var scratch []byte
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := h.encodeBatchPacket(&batch, &scratch); !ok {
			b.Fatalf("encodeBatchPacket returned oversized packet")
		}
	}
}

func buildBenchBatch(metricCount int) tlstatshouse.AddMetricsBatchBytes {
	batch := tlstatshouse.AddMetricsBatchBytes{
		Metrics: make([]tlstatshouse.MetricBytes, 0, metricCount),
	}
	for i := 0; i < metricCount; i++ {
		batch.Metrics = append(batch.Metrics, tlstatshouse.MetricBytes{
			Name:       []byte("bench.metric.stage"),
			FieldsMask: 1 << 1,
			Value:      []float64{1},
		})
	}
	return batch
}

func waitForEgressReady(tb testing.TB, eg *Egress, pkt []byte) {
	tb.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if eg.WritePacket(pkt) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	tb.Fatalf("egress not ready before benchmark")
}

func serveDiscardUpstream(ln net.Listener, done chan<- struct{}) {
	defer close(done)
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go func(c net.Conn) {
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
