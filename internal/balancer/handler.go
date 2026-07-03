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
	pktHeadLen  = 4
	pktBodyMax  = receiver.MaxTCPFrameBody
	pktFrameMax = pktHeadLen + pktBodyMax
)

type handler struct {
	egress *Egress
	mu     sync.Mutex

	pkt []byte

	reportInterval time.Duration
	stop           chan struct{}
}

func newHandler(e *Egress) *handler {
	h := &handler{
		egress:         e,
		reportInterval: 30 * time.Second,
		pkt:            make([]byte, pktHeadLen, pktFrameMax),
		stop:           make(chan struct{}),
	}
	go h.reportLoop()
	return h
}

func (h *handler) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.egress != nil {
		es := h.egress.Stats()
		log.Printf("balancer stats: fwd=%d drop=%d reconnect_err=%d dns_err=%d write_err=%d",
			es.ForwardedPackets, es.DroppedPackets,
			es.ReconnectErrors, es.DNSRefreshErrors, es.WriteErrors)
	}
	close(h.stop)
}

func (h *handler) HandleMetrics(data_model.HandlerArgs) (r data_model.MappedMetricHeader) { return }

func (h *handler) HandleMetricsBatchRaw(pkt []byte) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(pkt) <= 0 {
		return nil
	}
	h.pkt = append(h.pkt[:pktHeadLen], pkt...)
	binary.LittleEndian.PutUint32(h.pkt[:pktHeadLen], uint32(len(pkt)))
	h.pkt = h.egress.WritePacketLocked(h.pkt)[:pktHeadLen] // swap packet
	return nil
}

func (h *handler) HandleParseError(_ []byte, _ error) {}

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

func detectHostTag(override string) string {
	if len(override) > 0 {
		return override
	}
	host, err := os.Hostname()
	if err != nil {
		return ""
	}
	return string(format.ForceValidStringValue(host)) // worse alternative is do not run at all
}
