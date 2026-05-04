package balancer

import (
	"encoding/binary"
	"log"
	"os"
	"sync"
	"time"

	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"
)

const (
	pktHeadLen   = 4
	pktBodyMax   = 65535
	pktFrameMax  = pktHeadLen + pktBodyMax
	sendInterval = 1 * time.Second // guaranteed send for small traffic
)

type handlerStats struct {
	ParseErrors uint64
}

type HandlerConfig struct {
	HostTag []byte
}

type handler struct {
	cfg    HandlerConfig
	egress *Egress
	mu     sync.Mutex

	pkt      []byte
	lastSend time.Time

	parseErrs uint64

	sendInterval   time.Duration
	reportInterval time.Duration
	stop           chan struct{}
}

func newHandler(cfg HandlerConfig, e *Egress) *handler {
	h := &handler{
		cfg:            cfg,
		egress:         e,
		sendInterval:   sendInterval,
		reportInterval: 45 * time.Second,
		pkt:            make([]byte, pktHeadLen, pktFrameMax),
		stop:           make(chan struct{}),
	}
	go h.sendLoop()
	go h.reportLoop()
	return h
}

func (h *handler) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.egress != nil {
		h.flushLocked()
		es := h.egress.Stats()
		log.Printf("balancer stats: fwd=%d drop=%d parse_err=%d reconnect_err=%d dns_err=%d write_err=%d",
			es.ForwardedPackets, es.DroppedPackets, h.parseErrs,
			es.ReconnectErrors, es.DNSRefreshErrors, es.WriteErrors)
	}
	close(h.stop)
}

func (h *handler) HandleMetrics(args data_model.HandlerArgs) data_model.MappedMetricHeader {
	var single tlstatshouse.AddMetricsBatchBytes
	single.Metrics = append(single.Metrics, *args.MetricBytes)
	_ = h.HandleMetricsBatch(&single, 0, args.Scratch)
	return data_model.MappedMetricHeader{}
}

func (h *handler) HandleMetricsBatch(batch *tlstatshouse.AddMetricsBatchBytes, size int, _ *[]byte) error {
	if batch == nil || len(batch.Metrics) == 0 {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()

	// we use field_mask instead of _h tag because:
	// - no need for range tags + zero allocs
	// - append(tag[_h]) spoiled full batch data, reading was incorrect
	// - in case when metric=20b, host=128b => 65535b raise to 490kb (lose traffic speed)
	batch.SetHost(h.cfg.HostTag)
	estimateSize := size + len(h.cfg.HostTag) + 4 // 4 bytes for string len
	if estimateSize <= pktBodyMax {
		if len(h.pkt)+estimateSize > pktFrameMax {
			h.flushLocked()
		}
		h.encodeLocked(batch)
		return nil
	}
	h.splitWhalesLocked(batch)
	return nil
}

func (h *handler) flushLocked() {
	if len(h.pkt) <= pktHeadLen {
		return
	}
	binary.LittleEndian.PutUint32(h.pkt[:pktHeadLen], uint32(len(h.pkt)-pktHeadLen))
	h.pkt = h.egress.WritePacket(h.pkt)[:pktHeadLen] // swap packet
	h.lastSend = time.Now()
}

func (h *handler) encodeLocked(batch *tlstatshouse.AddMetricsBatchBytes) {
	was := len(h.pkt)
	h.pkt = batch.WriteTL1Boxed(h.pkt)
	if len(h.pkt) < pktFrameMax {
		return
	}
	// too big batch, rollback
	h.pkt = h.pkt[:was]
	h.splitWhalesLocked(batch) // recursion
}

func (h *handler) splitWhalesLocked(batch *tlstatshouse.AddMetricsBatchBytes) {
	if len(batch.Metrics) <= 1 {
		// we can't send 1 super whale
		h.parseErrs++
		return
	}
	// For whales metrics we split batch until fit limit [pktFrameMax] or lose single super whale
	// like O(log_2(n)) for whales
	half := len(batch.Metrics) / 2
	s1 := &tlstatshouse.AddMetricsBatchBytes{
		FieldsMask: batch.FieldsMask,
		Host:       batch.Host,
		Metrics:    batch.Metrics[:half],
	}
	s2 := &tlstatshouse.AddMetricsBatchBytes{
		FieldsMask: batch.FieldsMask,
		Host:       batch.Host,
		Metrics:    batch.Metrics[half:],
	}
	// we don't know sizes, so flush it
	h.flushLocked()
	h.encodeLocked(s1)
	h.flushLocked()
	h.encodeLocked(s2)
}

func (h *handler) HandleParseError(_ []byte, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.parseErrs++
	log.Printf("balancer parse error: %v", err)
}

func (h *handler) Stats() handlerStats {
	h.mu.Lock()
	defer h.mu.Unlock()
	return handlerStats{
		ParseErrors: h.parseErrs,
	}
}

func (h *handler) sendLoop() {
	t := time.NewTicker(h.sendInterval)
	defer t.Stop()
	for {
		select {
		case <-h.stop:
			return
		case now := <-t.C:
			func() {
				h.mu.Lock()
				defer h.mu.Unlock()
				if !h.lastSend.IsZero() && now.Sub(h.lastSend) < h.sendInterval {
					return
				}
				h.flushLocked()
			}()
		}
	}
}

func (h *handler) reportLoop() {
	t := time.NewTicker(h.reportInterval)
	defer t.Stop()
	last := time.Now()
	for {
		select {
		case <-h.stop:
			return
		case <-t.C:
			now := time.Now()
			dif := uint64(now.Sub(last).Seconds())

			hs := h.Stats()
			es := h.egress.Stats()
			log.Printf("balancer stats: fwd=%d pkt/sec drop=%d pkt/sec parse_err=%d reconnect_err=%d dns_err=%d write_err=%d",
				es.ForwardedPackets/dif, es.DroppedPackets/dif, hs.ParseErrors,
				es.ReconnectErrors, es.DNSRefreshErrors, es.WriteErrors)
			last = now
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
