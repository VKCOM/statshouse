package balancer

import (
	"testing"

	"github.com/VKCOM/statshouse/internal/data_model/gen2/tl"
	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"
)

func TestEnsureHostTagAddsWhenMissing(t *testing.T) {
	m := &tlstatshouse.MetricBytes{Name: []byte("metric")}
	ensureHostTag(m, []byte("balancer-host"))
	if len(m.Tags) != 1 {
		t.Fatalf("expected 1 tag, got %d", len(m.Tags))
	}
	if got := string(m.Tags[0].Key); got != "_h" {
		t.Fatalf("expected _h tag key, got %q", got)
	}
	if got := string(m.Tags[0].Value); got != "balancer-host" {
		t.Fatalf("expected host value, got %q", got)
	}
}

func TestEnsureHostTagDoesNotOverwriteExisting(t *testing.T) {
	m := &tlstatshouse.MetricBytes{Name: []byte("metric")}
	m.Tags = append(m.Tags, tl.DictFieldStringStringBytes{
		Key:   []byte("_h"),
		Value: []byte("original"),
	})
	ensureHostTag(m, []byte("new"))

	if len(m.Tags) != 1 {
		t.Fatalf("expected existing tag to stay single, got %d tags", len(m.Tags))
	}
	if got := string(m.Tags[0].Value); got != "original" {
		t.Fatalf("expected existing host tag value preserved, got %q", got)
	}
}
