package sqlitev2

import (
	"time"

	"github.com/VKCOM/statshouse-go"
)

type StatsOptions struct {
	Service string
	Cluster string
	DC      string
}

const (
	queryDurationMetric         = "sqlite_query_duration"
	waitDurationMetric          = "sqlite_wait_duration"
	txDurationMetric            = "sqlite_tx_duration"
	actionDurationMetric        = "sqlite_action_duration"
	engineBrokenMetric          = "sqlite_engine_broken_event"
	walCheckpointSizeMetric     = "sqlite_wal_size"
	walCheckpointDurationMetric = "sqlite_wal_checkpoint_duration"

	txDo   = "sqlite_tx_do"
	txView = "sqlite_tx_view"

	waitView    = "wait_lock_view"
	waitDo      = "wait_lock_do"
	closeEngine = "close_engine"
	query       = "query"
	exec        = "exec"
)

func (s *StatsOptions) checkEmpty() bool {
	return len(s.Service) == 0
}

func (s *StatsOptions) measureSqliteQueryDurationSince(typ, name string, status string, start time.Time) {
	if s.checkEmpty() {
		return
	}
	statshouse.Value(queryDurationMetric, statshouse.Tags{1: s.Service, 2: s.Cluster, 3: s.DC, 4: typ, 5: name, 6: status}, time.Since(start).Seconds())
}

func (s *StatsOptions) measureWaitDurationSince(typ string, start time.Time) {
	if s.checkEmpty() {
		return
	}
	statshouse.Value(waitDurationMetric, statshouse.Tags{1: s.Service, 2: s.Cluster, 3: s.DC, 4: typ}, time.Since(start).Seconds())
}

func (s *StatsOptions) measureActionDurationSince(typ string, start time.Time) {
	if s.checkEmpty() {
		return
	}
	statshouse.Value(actionDurationMetric, statshouse.Tags{1: s.Service, 2: s.Cluster, 3: s.DC, 4: typ}, time.Since(start).Seconds())
}

func (s *StatsOptions) measureSqliteTxDurationSince(typ, name string, start time.Time) {
	if s.checkEmpty() {
		return
	}
	statshouse.Value(txDurationMetric, statshouse.Tags{1: s.Service, 2: s.Cluster, 3: s.DC, 4: typ, 5: name}, time.Since(start).Seconds())
}

func (s *StatsOptions) engineBrokenEvent() {
	if s.checkEmpty() {
		return
	}
	statshouse.Count(engineBrokenMetric, statshouse.Tags{1: s.Service, 2: s.Cluster, 3: s.DC}, 1)
}

func (s *StatsOptions) walSwitchSize(iApp int, maxFrame uint) {
	if s.checkEmpty() {
		return
	}
	k := "wal"
	if iApp > 0 {
		k = "wal2"
	}
	statshouse.Value(walCheckpointSizeMetric, statshouse.Tags{1: s.Service, 2: s.Cluster, 3: s.DC, 4: k}, float64(maxFrame))
}

func (s *StatsOptions) walCheckpointDuration(status string, dur time.Duration) {
	if s.checkEmpty() {
		return
	}
	statshouse.Value(walCheckpointDurationMetric, statshouse.Tags{1: s.Service, 2: s.Cluster, 3: s.DC, 4: status}, dur.Seconds())
}
