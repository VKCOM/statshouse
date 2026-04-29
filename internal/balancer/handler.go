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
	pktRingLen   = 5 // 1 for handler, 2 for tcpSender[2], 2 for processing in tunnel
	pktHeadLen   = 4
	pktBodyMax   = 65535
	pktFrameMax  = pktHeadLen + pktBodyMax
	sendInterval = 4 * time.Second // guaranteed send for small traffic
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

	ringI    int
	ring     [pktRingLen][]byte // we don't know when prev pkt becomes free, so we can't make stack for L1 cache
	scratch  tlstatshouse.AddMetricsBatchBytes
	lastSend time.Time

	parseErrs uint64

	sendInterval time.Duration
	stop         chan struct{}
}

func newHandler(cfg HandlerConfig, e *Egress) *handler {
	h := &handler{
		cfg:          cfg,
		egress:       e,
		sendInterval: sendInterval,
		stop:         make(chan struct{}),
	}
	for i := range h.ring {
		h.ring[i] = make([]byte, pktHeadLen, pktFrameMax*2)
	}
	go h.sendLoop()
	return h
}

func (h *handler) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.egress != nil {
		h.flushLocked(h.shiftRingLocked(-1))
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
	// - no need for range tags
	// - append(tag[_h]) spoiled full batch data, reading was incorrect
	// - in case when metric=20b, host=128b => 65535b raise to 490kb (lose traffic speed)
	batch.SetHost(h.cfg.HostTag)
	if size+len(h.cfg.HostTag) <= pktBodyMax {
		if pkt, ok := h.encodeLocked(batch); ok {
			h.flushLocked(pkt)
		}
		return nil
	}
	// For whales metrics we split batch until fit limit [pktFrameMax] or lose single super whale
	// like O(log_2(n)) for whales
	half := len(h.scratch.Metrics) / 2
	s1 := &tlstatshouse.AddMetricsBatchBytes{
		FieldsMask: h.scratch.FieldsMask,
		Host:       h.scratch.Host,
		Metrics:    h.scratch.Metrics[:half],
	}
	s2 := &tlstatshouse.AddMetricsBatchBytes{
		FieldsMask: h.scratch.FieldsMask,
		Host:       h.scratch.Host,
		Metrics:    h.scratch.Metrics[half:],
	}
	if pkt, ok := h.encodeLocked(s1); ok {
		h.flushLocked(pkt)
	}
	if pkt, ok := h.encodeLocked(s2); ok {
		h.flushLocked(pkt)
	}
	return nil
}

func (h *handler) flushLocked(pkt []byte) {
	if len(pkt) == 0 {
		return
	}
	if ok := h.egress.WritePacket(pkt); !ok {
		h.lastSend = time.Now()
		return
	}
	h.lastSend = time.Now()
}

func (h *handler) encodeLocked(batch *tlstatshouse.AddMetricsBatchBytes) ([]byte, bool) {
	h.ring[h.ringI] = batch.WriteTL1Boxed(h.ring[h.ringI])
	if len(h.ring[h.ringI]) < pktFrameMax {
		return nil, false
	}
	offset, err := h.findOffsetLocked()
	if offset > pktFrameMax {
		h.ring[h.ringI] = append(h.ring[h.ringI][:pktHeadLen], h.ring[h.ringI][offset:]...) // skip big data
		if err != nil || len(h.scratch.Metrics) <= 1 {
			// single superbig metric or invalid data
			h.parseErrs++
		}
		// For whales metrics we split batch until fit limit [pktFrameMax] or lose single super whale
		// like O(log_2(n)) for whales
		half := len(h.scratch.Metrics) / 2
		s1 := &tlstatshouse.AddMetricsBatchBytes{
			FieldsMask: h.scratch.FieldsMask,
			Host:       h.scratch.Host,
			Metrics:    h.scratch.Metrics[:half],
		}
		s2 := &tlstatshouse.AddMetricsBatchBytes{
			FieldsMask: h.scratch.FieldsMask,
			Host:       h.scratch.Host,
			Metrics:    h.scratch.Metrics[half:],
		}
		h.ring[h.ringI] = s1.WriteTL1Boxed(h.ring[h.ringI])
		h.ring[h.ringI] = s2.WriteTL1Boxed(h.ring[h.ringI])
		return nil, false
	}
	return h.shiftRingLocked(offset), true
}

func (h *handler) shiftRingLocked(offset int) []byte {
	if offset == -1 {
		offset = len(h.ring[h.ringI])
	}
	pkt := h.ring[h.ringI][:offset]
	binary.LittleEndian.PutUint32(pkt[:pktHeadLen], uint32(len(pkt[pktHeadLen:])))

	nextI := (h.ringI + 1) % len(h.ring)
	h.ring[nextI] = append(h.ring[nextI][:pktHeadLen], h.ring[h.ringI][offset:]...)
	h.ring[h.ringI] = h.ring[h.ringI][:pktHeadLen]
	h.ringI = nextI
	return pkt
}

func (h *handler) findOffsetLocked() (int, error) {
	var err error
	offset := pktHeadLen
	pkt := h.ring[h.ringI][pktHeadLen:]
	was := len(pkt)
	for len(pkt) > 0 {
		pkt, err = h.scratch.ReadTL1Boxed(pkt)
		if was-len(pkt) > pktBodyMax || err != nil {
			break
		}
		offset = pktHeadLen + was - len(pkt)
	}
	if offset == pktHeadLen || err != nil {
		// too big batch
		pkt = h.ring[h.ringI][pktHeadLen:]
		pkt, err = h.scratch.ReadTL1Boxed(pkt)
		if err != nil {
			return len(h.ring[h.ringI]), err
		}
		return len(h.ring[h.ringI]) - len(pkt), err
	}
	return offset, nil
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
				h.flushLocked(h.shiftRingLocked(-1))
			}()
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
