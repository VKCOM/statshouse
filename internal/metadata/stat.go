// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metadata

import (
	"time"

	"github.com/vkcom/statshouse-go"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
)

const rpcStat = "sqlengine_rpc_time"
const backupDurationMetric = "sqlengine_backup_time"
const sqlengineLoadJournalWaitQLen = "load_journal_client_waits"

func BackupTimeStat(d time.Duration, err error) {
	statshouse.AccessMetricRaw(backupDurationMetric, statshouse.RawTags{}).Value(float64(d.Nanoseconds()))
	if err != nil {
		statshouse.AccessMetricRaw(backupDurationMetric, statshouse.RawTags{}).StringTop(err.Error())
	}
}

func rpcDurationStat(host, method string, duration time.Duration, err error, status string) {
	statshouse.AccessMetricRaw(rpcStat, statshouse.RawTags{Tag1: host, Tag2: method, Tag4: status}).Value(float64(duration.Nanoseconds()))
	if err != nil && !rpc.IsHijackedResponse(err) {
		statshouse.AccessMetricRaw(rpcStat, statshouse.RawTags{Tag1: host, Tag2: method}).StringTop(err.Error())
	}
}
