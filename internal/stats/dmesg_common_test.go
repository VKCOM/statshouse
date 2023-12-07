package stats

import "testing"

func BenchmarkMesgStats_handleMsgs(b *testing.B) {
	c := &DMesgStats{parser: &parser{}}
	data := []byte(
		`<4>[12555048.809330] audit: audit_lost=50409498 audit_rate_limit=0 audit_backlog_limit=8192
<4>[12555059.240818] audit_log_start: 2 callbacks suppressed
<4>[12555059.240819] audit: audit_backlog=2803588 > audit_backlog_limit=8192
<4>[12555059.254320] audit: audit_lost=50409500 audit_rate_limit=0 audit_backlog_limit=8192
<4>[12555065.440034] audit: audit_backlog=2803590 > audit_backlog_limit=8192
<4>[12555065.447275] audit: audit_lost=50409501 audit_rate_limit=0 audit_backlog_limit=8192
<4>[12555065.457472] audit: audit_backlog=2803590 > audit_backlog_limit=8192
<4>[12555065.465353] audit: audit_lost=50409502 audit_rate_limit=0 audit_backlog_limit=8192
<4>[12555065.474804] audit: audit_backlog=2803590 > audit_backlog_limit=8192
<4>[12555065.482944] audit: audit_lost=50409503 audit_rate_limit=0 audit_backlog_limit=8192
<4>[12555065.492439] audit: audit_backlog=2803590 > audit_backlog_limit=8192
<4>[12555065.500576] audit: audit_lost=50409504 audit_rate_limit=0 audit_backlog_limit=8192
<4>[12555065.510258] audit: audit_backlog=2803590 > audit_backlog_limit=8192
<4>[12555065.517927] audit: audit_lost=50409505 audit_rate_limit=0 audit_backlog_limit=8192
`)
	for i := 0; i < b.N; i++ {
		err := c.handleMsgs(0, data, false)
		if err != nil {
			panic(err)
		}

	}
}
