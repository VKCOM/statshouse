package balancer

import (
	"encoding/binary"
	"log"
	"os"
	"sync"
	"time"

	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/format"
	"github.com/VKCOM/statshouse/internal/receiver"
)

const (
	pktHeadLen   = 4
	pktBodyMax   = receiver.MaxTCPFrameBody
	pktFrameMax  = pktHeadLen + pktBodyMax
	sendInterval = 1 * time.Second // guaranteed send for small traffic
)

type handler struct {
	egress *Egress
	mu     sync.Mutex

	pkt      []byte
	lastSend time.Time

	parseErrs uint64

	sendInterval   time.Duration
	reportInterval time.Duration
	stop           chan struct{}
}

func newHandler(e *Egress) *handler {
	h := &handler{
		egress:         e,
		sendInterval:   sendInterval,
		reportInterval: 30 * time.Second,
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
}

func (h *handler) HandleMetrics(data_model.HandlerArgs) (r data_model.MappedMetricHeader) { return }

func (h *handler) HandleMetricsBatchRaw(pkt []byte) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.pkt)+len(pkt) > pktFrameMax { // ok for tcp
		h.flushLocked()
	}
	h.pkt = append(h.pkt, pkt...)
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

func (h *handler) HandleParseError(_ []byte, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.parseErrs++
	log.Printf("balancer parse error: %v", err)
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
		case now := <-t.C:
			dif := uint64(now.Sub(last).Seconds())
			if dif == 0 {
				continue
			}
			es := h.egress.Stats()
			log.Printf("balancer stats: fwd=%d pkt/sec drop=%d pkt/sec reconnect_err=%d dns_err=%d write_err=%d",
				es.ForwardedPackets/dif, es.DroppedPackets/dif,
				es.ReconnectErrors, es.DNSRefreshErrors, es.WriteErrors)
			last = now
		}
	}
}

func detectHostTag(override []byte) []byte {
	if len(override) > 0 {
		return override
	}
	host, err := os.Hostname()
	if err != nil {
		return nil
	}
	return format.ForceValidStringValue(host) // worse alternative is do not run at all
}
