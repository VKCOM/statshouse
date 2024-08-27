package stats

import (
	"fmt"
	"runtime/debug"
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

var test []byte

/*
Benchmark_GC-10    	23858916	        69.53 ns/op
*/
func Benchmark_GC(t *testing.B) {
	alloc := 1000
	stats := debug.GCStats{}
	for i := 0; i < t.N; i++ {
		if i%1000 == 0 {
			t.StopTimer()
			for j := 0; j < alloc; j++ {
				test = make([]byte, 1)
			}
			t.StartTimer()
		}
		debug.ReadGCStats(&stats)
	}
	fmt.Println(stats)
	fmt.Println(test)

}
