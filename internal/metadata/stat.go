// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metadata

import (
	"time"

	"github.com/VKCOM/statshouse-go"
	"github.com/VKCOM/statshouse/internal/format"
	"github.com/VKCOM/statshouse/internal/vkgo/rpc"
)

func rpcDurationStat(host, method string, duration time.Duration, err error, queryType string) {
	status := "ok"
	if err != nil && !rpc.IsHijackedResponse(err) {
		status = "error"
	}
	statshouse.Value(format.BuiltinMetricMetaMetaServiceTime.Name, statshouse.Tags{1: host, 2: method, 3: queryType, 4: status}, duration.Seconds())
}
