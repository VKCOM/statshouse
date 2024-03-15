package sqlitev2

import (
	"time"

	"github.com/vkcom/statshouse-go"
)

type StatsOptions struct {
	Service string
	Cluster string
	DC      string
}

const (
	queryDurationMetric     = "sqlite_query_duration"
	waitDurationMetric      = "sqlite_wait_duration"
	txDurationMetric        = "sqlite_tx_duration"
	actionDurationMetric    = "sqlite_action_duration"
	engineBrokenMetric      = "sqlite_engine_broken_event"
	walCheckpointSizeMetric = "sqlite_wal_checkpoint_size"

	txDo   = "sqlite_tx_do"
	txView = "sqlite_tx_view"

	waitView    = "wait_lock_view"
	waitDo      = "wait_lock_do"
	closeEngine = "close_engine"
	query       = "query"
	exec        = "exec"
)

func (s *StatsOptions) checkEmpty() bool {
	return s.Service == ""
}

func (s *StatsOptions) measureSqliteQueryDurationSince(typ, name string, start time.Time) {
	if s.checkEmpty() {
		return
	}
	statshouse.Metric(queryDurationMetric, statshouse.Tags{1: s.Service, 2: s.Cluster, 3: s.DC, 4: typ, 5: name}).Value(time.Since(start).Seconds())
}

func (s *StatsOptions) measureWaitDurationSince(typ string, start time.Time) {
	if s.checkEmpty() {
		return
	}
	statshouse.Metric(waitDurationMetric, statshouse.Tags{1: s.Service, 2: s.Cluster, 3: s.DC, 4: typ}).Value(time.Since(start).Seconds())
}

func (s *StatsOptions) measureActionDurationSince(typ string, start time.Time) {
	if s.checkEmpty() {
		return
	}
	statshouse.Metric(actionDurationMetric, statshouse.Tags{1: s.Service, 2: s.Cluster, 3: s.DC, 4: typ}).Value(time.Since(start).Seconds())
}

func (s *StatsOptions) measureSqliteTxDurationSince(typ, name string, start time.Time) {
	if s.checkEmpty() {
		return
	}
	statshouse.Metric(txDurationMetric, statshouse.Tags{1: s.Service, 2: s.Cluster, 3: s.DC, 4: typ, 5: name}).Value(time.Since(start).Seconds())
}

func (s *StatsOptions) engineBrokenEvent() {
	if s.checkEmpty() {
		return
	}
	statshouse.Metric(engineBrokenMetric, statshouse.Tags{1: s.Service, 2: s.Cluster, 3: s.DC}).Count(1)
}

func (s *StatsOptions) walCheckpointSize(iApp int, maxFrame uint) {
	if s.checkEmpty() {
		return
	}
	k := "wal"
	if iApp > 0 {
		k = "wal2"
	}
	statshouse.Metric(walCheckpointSizeMetric, statshouse.Tags{1: s.Service, 2: s.Cluster, 3: s.DC, 4: k}).Value(float64(maxFrame))
}
