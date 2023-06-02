// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metadata

import (
	"time"

	"github.com/vkcom/statshouse-go"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
)

func rpcDurationStat(host, method string, duration time.Duration, err error, queryType string) {
	status := "ok"
	if err != nil && !rpc.IsHijackedResponse(err) {
		status = "error"
	}
	statshouse.AccessMetricRaw(format.BuiltinMetricNameMetaServiceTime, statshouse.RawTags{Tag1: host, Tag2: method, Tag3: queryType, Tag4: status}).Value(duration.Seconds())

}
