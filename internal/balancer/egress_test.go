package balancer

import (
	"errors"
	"testing"
)

func TestAddressPoolRoundRobin(t *testing.T) {
	p := addressPool{addrs: []string{"a", "b"}}
	v1, _ := p.pick()
	v2, _ := p.pick()
	v3, _ := p.pick()

	if v1 != "a" || v2 != "b" || v3 != "a" {
		t.Fatalf("unexpected sequence: %q %q %q", v1, v2, v3)
	}
}

func TestTCPPoolWriteDropsWhenBothBlocked(t *testing.T) {
	primary := &tcpSender{q: make(chan []byte, 1), closeCh: make(chan struct{})}
	secondary := &tcpSender{q: make(chan []byte, 1), closeCh: make(chan struct{})}
	primary.q <- []byte("busy")
	secondary.q <- []byte("busy")

	pool := &tcpPool{
		primary:   primary,
		secondary: secondary,
		closed:    make(chan struct{}),
	}
	err := pool.write([]byte("pkt"))
	if !errors.Is(err, errWouldBlock) {
		t.Fatalf("expected errWouldBlock, got %v", err)
	}
}

func TestTCPPoolPromotesSecondaryOnPrimaryBackpressure(t *testing.T) {
	primary := &tcpSender{q: make(chan []byte, 1), closeCh: make(chan struct{})}
	secondary := &tcpSender{q: make(chan []byte, 1), closeCh: make(chan struct{})}
	primary.q <- []byte("busy")

	pool := &tcpPool{
		primary:   primary,
		secondary: secondary,
		closed:    make(chan struct{}),
	}
	if err := pool.write([]byte("pkt")); err != nil {
		t.Fatalf("expected successful write via secondary, got %v", err)
	}
	if pool.primary != secondary {
		t.Fatalf("expected secondary to become primary after successful fallback")
	}
}
