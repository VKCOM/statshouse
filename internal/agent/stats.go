package agent

import (
	"strconv"
	"sync/atomic"
)

type shardStat struct {
	recentSendSuccess   atomic.Int64
	recentSendFailed    atomic.Int64
	historicSendSuccess atomic.Int64
	historicSendFailed  atomic.Int64
	recentSendKeep      atomic.Int64
	historicSendKeep    atomic.Int64

	shardReplicaNum string
}

func (s *shardStat) fillStats(stats map[string]string) {
	m := func(str string) string {
		return str + "_" + s.shardReplicaNum
	}
	stats[m("recent_send_success")] = strconv.FormatInt(s.recentSendSuccess.Load(), 10)
	stats[m("recent_send_failed")] = strconv.FormatInt(s.recentSendFailed.Load(), 10)
	stats[m("recent_send_keep")] = strconv.FormatInt(s.recentSendKeep.Load(), 10)
	stats[m("historic_send_success")] = strconv.FormatInt(s.historicSendSuccess.Load(), 10)
	stats[m("historic_send_failed")] = strconv.FormatInt(s.historicSendFailed.Load(), 10)
	stats[m("historic_send_keep")] = strconv.FormatInt(s.historicSendKeep.Load(), 10)
}
