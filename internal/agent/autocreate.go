// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package agent

import (
	"context"
	"fmt"

	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
)

func (s *Agent) AutoCreateMetric(ctx context.Context, args tlstatshouse.AutoCreate) error {
	shard, _ := s.getRandomLiveShards()
	if shard == nil {
		return fmt.Errorf("all aggregators are dead")
	}
	shard.fillProxyHeader(&args.FieldsMask, &args.Header)
	extra := rpc.InvokeReqExtra{FailIfNoConnection: true}
	return shard.client.AutoCreate(ctx, args, &extra, nil)
}
