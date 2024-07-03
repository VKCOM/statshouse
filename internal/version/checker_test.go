package version

import (
	"fmt"
	"github.com/vkcom/statshouse/internal/pcache"
	"pgregory.net/rand"
	"testing"
	"time"
)

func host(number int) string {
	return fmt.Sprintf("host-%d.online", number)
}

func prepare(b *testing.B) *Cache {
	cacheFilename := b.TempDir() + "/cache"
	dc, err := pcache.OpenDiskCache(cacheFilename, pcache.DefaultTxDuration)
	if err != nil {
		panic(err)
	}
	return NewVersionCache(dc)
}

func BenchmarkAllowAgent_UpToDate(b *testing.B) {
	rng := rand.New()
	aggregatorCommitTs := uint32(time.Now().Unix())
	agentCommitTs := aggregatorCommitTs
	c := prepare(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h := host(rng.Intn(100_000))
		AllowAgent(h, aggregatorCommitTs, agentCommitTs, c)
	}
}

func BenchmarkAllowAgent_OutdatedAllow(b *testing.B) {
	rng := rand.New()
	aggregatorCommitTs := uint32(time.Now().Unix())
	agentCommitTs := aggregatorCommitTs - uint32(time.Hour/time.Second*24*45)
	c := prepare(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h := host(rng.Intn(100_000))
		AllowAgent(h, aggregatorCommitTs, agentCommitTs, c)
	}
}

func BenchmarkAllowAgent_OutdatedDeny(b *testing.B) {
	rng := rand.New()
	aggregatorCommitTs := uint32(time.Now().Unix())
	agentCommitTs := aggregatorCommitTs - uint32(time.Hour/time.Second*24*45)
	c := prepare(b)
	for i := 0; i < 100_000; i++ {
		c.Set(host(i), time.Unix(int64(agentCommitTs), 0))
	}
	time.Sleep(time.Second)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		h := host(rng.Intn(100_000))
		AllowAgent(h, aggregatorCommitTs, agentCommitTs, c)
	}
}
