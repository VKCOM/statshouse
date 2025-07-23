// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package agent

import (
	"context"
	"fmt"

	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/VKCOM/statshouse/internal/vkgo/rpc"
)

func (s *Agent) AutoCreateMetric(ctx context.Context, args tlstatshouse.AutoCreate) error {
	shard, _ := s.getRandomLiveShardReplicas()
	if shard == nil {
		return fmt.Errorf("all aggregators are dead")
	}
	shard.fillProxyHeader(&args.FieldsMask, &args.Header)
	extra := rpc.InvokeReqExtra{FailIfNoConnection: true}
	client := shard.client()
	return client.AutoCreate(ctx, args, &extra, nil)
}
