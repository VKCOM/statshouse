package sqlite

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
	queryDurationMetric  = "sqlite_query_duration"
	waitDurationMetric   = "sqlite_wait_duration"
	txDurationMetric     = "sqlite_tx_duration"
	actionDurationMetric = "sqlite_action_duration"
	applyQueueSizeMetric = "sqlite_apply_queue_size"

	txDo   = "sqlite_tx_do"
	txView = "sqlite_tx_view"

	waitView       = "wait_lock_view"
	waitDo         = "wait_lock_do"
	waitBinlogSync = "binlog_wait_db_sync"

	query = "query"
	exec  = "exec"
)

func (s *StatsOptions) measureSqliteQueryDurationSince(typ, name string, start time.Time) {
	statshouse.AccessMetricRaw(queryDurationMetric, statshouse.RawTags{Tag1: s.Service, Tag2: s.Cluster, Tag3: s.DC, Tag4: typ, Tag5: name}).Value(time.Since(start).Seconds())
}

func (s *StatsOptions) measureWaitDurationSince(typ string, start time.Time) {
	statshouse.AccessMetricRaw(waitDurationMetric, statshouse.RawTags{Tag1: s.Service, Tag2: s.Cluster, Tag3: s.DC, Tag4: typ}).Value(time.Since(start).Seconds())
}

func (s *StatsOptions) measureActionDurationSince(typ string, start time.Time) {
	statshouse.AccessMetricRaw(actionDurationMetric, statshouse.RawTags{Tag1: s.Service, Tag2: s.Cluster, Tag3: s.DC, Tag4: typ}).Value(time.Since(start).Seconds())
}

func (s *StatsOptions) measureSqliteTxDurationSince(typ, name string, start time.Time) {
	statshouse.AccessMetricRaw(txDurationMetric, statshouse.RawTags{Tag1: s.Service, Tag2: s.Cluster, Tag3: s.DC, Tag4: typ, Tag5: name}).Value(time.Since(start).Seconds())
}

func (s *StatsOptions) applyQueueSize(registry *statshouse.Registry, size int64) {
	registry.AccessMetricRaw(applyQueueSizeMetric, statshouse.RawTags{Tag1: s.Service, Tag2: s.Cluster, Tag3: s.DC}).Value(float64(size))
}
