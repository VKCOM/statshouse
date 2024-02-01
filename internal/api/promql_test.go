package api

import (
	"testing"
	"time"

	"github.com/vkcom/statshouse/internal/promql"
)

func BenchmarkGetTimescale(b *testing.B) {
	now := time.Now()
	qry := promql.Query{
		Start: now.AddDate(0, 0, -2).Unix(),
		End:   now.Unix(),
		Options: promql.Options{
			TimeNow: now.Unix(),
		},
	}
	for i := 0; i < b.N; i++ {
		var h Handler
		h.GetTimescale(qry, nil)
	}
}
