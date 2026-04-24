package balancer

import (
	"bytes"
	"context"
	"encoding/binary"
	"log"
	"os"
	"sync/atomic"
	"time"

	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/data_model/gen2/tl"
	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/VKCOM/statshouse/internal/vkgo/semaphore"
)

const (
	defaultQueueSize = 1
	pktBodyMax       = 65535
	pktFrameMax      = 4 + pktBodyMax
)

var hostTagKey = []byte("_h")

type handlerStats struct {
	ReceivedMetrics uint64
	Forwarded       uint64
	Dropped         uint64
	ParseErrors     uint64
}

type HandlerConfig struct {
	HostTag   []byte
	QueueSize int
}

type handler struct {
	cfg     HandlerConfig
	egress  *Egress
	connSem *semaphore.Weighted
	pktRing [][]byte

	pktIdx          atomic.Uint64
	receivedMetrics atomic.Uint64
	forwarded       atomic.Uint64
	dropped         atomic.Uint64
	parseErrors     atomic.Uint64

	reportInterval time.Duration
	stopReport     chan struct{}
}

func newHandler(cfg HandlerConfig, e *Egress) *handler {
	cfg.fillDefaults()
	h := &handler{
		cfg:            cfg,
		egress:         e,
		connSem:        semaphore.NewWeighted(int64(cfg.QueueSize)),
		pktRing:        newPktRing(cfg.QueueSize + 4),
		reportInterval: 45 * time.Second,
		stopReport:     make(chan struct{}),
	}
	go h.reportLoop()
	return h
}

func (cfg *HandlerConfig) fillDefaults() {
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = defaultQueueSize
	}
}

func (h *handler) Close() {
	close(h.stopReport)
}

func (h *handler) HandleMetrics(args data_model.HandlerArgs) data_model.MappedMetricHeader {
	var single tlstatshouse.AddMetricsBatchBytes
	single.Metrics = append(single.Metrics, *args.MetricBytes)
	_ = h.HandleMetricsBatch(&single, args.Scratch)
	return data_model.MappedMetricHeader{}
}

func (h *handler) HandleMetricsBatch(batch *tlstatshouse.AddMetricsBatchBytes, scratch *[]byte) error {
	if batch == nil || len(batch.Metrics) == 0 {
		return nil
	}
	h.connSem.Acquire(context.Background(), 1)
	defer h.connSem.Release(1)

	n := uint64(len(batch.Metrics))
	h.receivedMetrics.Add(n)
	pkt, ok := h.encodeBatchPacket(batch, scratch)
	if !ok {
		h.dropped.Add(n)
		h.parseErrors.Add(1)
		return nil
	}

	if ok := h.egress.WritePacket(pkt); !ok {
		h.dropped.Add(n)
		return nil
	}
	h.forwarded.Add(n)
	return nil
}

func newPktRing(size int) [][]byte {
	ring := make([][]byte, size)
	for i := range ring {
		ring[i] = make([]byte, pktFrameMax)
	}
	return ring
}

func (h *handler) nextPktBuffer(n int) []byte {
	if len(h.pktRing) == 0 {
		return make([]byte, n)
	}
	idx := int((h.pktIdx.Add(1) - 1) % uint64(len(h.pktRing)))
	return h.pktRing[idx][:n]
}

func (h *handler) encodeBatchPacket(batch *tlstatshouse.AddMetricsBatchBytes, scratch *[]byte) ([]byte, bool) {
	for i := range batch.Metrics {
		ensureHostTag(&batch.Metrics[i], h.cfg.HostTag)
	}
	var scr []byte
	if scratch != nil {
		scr = *scratch
	}
	scr = batch.WriteTL1Boxed(scr[:0])
	if scratch != nil {
		*scratch = scr[:0]
	}
	if len(scr) > pktBodyMax {
		log.Printf("too big packet body size: %d", len(scr)) // must be never
		return nil, false
	}
	pkt := h.nextPktBuffer(4 + len(scr))
	binary.LittleEndian.PutUint32(pkt[:4], uint32(len(scr)))
	copy(pkt[4:], scr)
	return pkt, true
}

func (h *handler) HandleParseError(_ []byte, err error) {
	h.parseErrors.Add(1)
	log.Printf("balancer parse error: %v", err)
}

func (h *handler) Stats() handlerStats {
	return handlerStats{
		ReceivedMetrics: h.receivedMetrics.Load(),
		Forwarded:       h.forwarded.Load(),
		Dropped:         h.dropped.Load(),
		ParseErrors:     h.parseErrors.Load(),
	}
}

func ensureHostTag(m *tlstatshouse.MetricBytes, hostTag []byte) {
	for i := range m.Tags {
		if bytes.Equal(m.Tags[i].Key, hostTagKey) {
			return
		}
	}
	appendMetricTag(m, hostTagKey, hostTag)
}

func appendMetricTag(m *tlstatshouse.MetricBytes, tag []byte, value []byte) {
	m.Tags = append(m.Tags, tl.DictFieldStringStringBytes{
		Key:   tag,
		Value: value,
	})
}

func (h *handler) reportLoop() {
	t := time.NewTicker(h.reportInterval)
	defer t.Stop()
	for {
		select {
		case <-h.stopReport:
			return
		case <-t.C:
			hs := h.Stats()
			es := h.egress.Stats()
			log.Printf("balancer stats: recv=%d fwd=%d drop=%d parse_err=%d reconnect_err=%d dns_err=%d write_err=%d",
				hs.ReceivedMetrics, hs.Forwarded, hs.Dropped, hs.ParseErrors,
				es.ReconnectErrors, es.DNSRefreshErrors, es.WriteErrors)
		}
	}
}

func detectHostTag(override string) string {
	if override != "" {
		return override
	}
	host, err := os.Hostname()
	if err != nil || host == "" {
		return "unknown"
	}
	return host
}
