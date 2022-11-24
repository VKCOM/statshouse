// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metadata

import (
	"time"

	"github.com/vkcom/statshouse/internal/vkgo/rpc"

	"github.com/vkcom/statshouse/internal/vkgo/statlogs"
)

const rpcStat = "sqlengine_rpc_time"
const backupDurationMetric = "sqlengine_backup_time"
const sqlengineLoadJournalWaitQLen = "load_journal_client_waits"

func BackupTimeStat(d time.Duration, err error) {
	statlogs.AccessMetricRaw(backupDurationMetric, statlogs.RawTags{}).Value(float64(d.Nanoseconds()))
	if err != nil {
		statlogs.AccessMetricRaw(backupDurationMetric, statlogs.RawTags{}).StringTop(err.Error())
	}
}

func rpcDurationStat(host, method string, duration time.Duration, err error, status string) {
	statlogs.AccessMetricRaw(rpcStat, statlogs.RawTags{Tag1: host, Tag2: method, Tag4: status}).Value(float64(duration.Nanoseconds()))
	if err != nil && !rpc.IsHijackedResponse(err) {
		statlogs.AccessMetricRaw(rpcStat, statlogs.RawTags{Tag1: host, Tag2: method}).StringTop(err.Error())
	}
}
