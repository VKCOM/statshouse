package stats

import (
	"testing"
)

func Benchmark_B(t *testing.B) {
	body := []byte(
		`<4>[12555048.809330] audit: audit_lost=50409498 audit_rate_limit=0 audit_backlog_limit=8192
<4>[12555058.445681] Killed process 3029 (Web Content) total-vm:10206696kB, anon-rss:6584572kB, file-rss:0kB, shmem-rss:8732kB
<4>[12555059.240818] audit_log_start: 2 callbacks suppressed
`)
	for i := 0; i < t.N; i++ {
		_, _, err := klogParser(body)
		if err != nil {
			panic(err)
		}
	}
}
