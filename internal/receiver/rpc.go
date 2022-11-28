// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package receiver

import (
	"context"
	"fmt"

	"go.uber.org/atomic"

	"github.com/vkcom/statshouse/internal/agent"
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tl"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/mapping"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
)

type RPCReceiver struct {
	Handler Handler

	ag *agent.Agent

	batchSizeRPCOK   *agent.BuiltInItemValue
	batchSizeRPCErr  *agent.BuiltInItemValue
	packetSizeRPCOK  *agent.BuiltInItemValue
	packetSizeRPCErr *agent.BuiltInItemValue

	StatCallsTotalOK  atomic.Uint64
	StatCallsTotalErr atomic.Uint64
}

func MakeRPCReceiver(ag *agent.Agent, h Handler) *RPCReceiver {
	return &RPCReceiver{
		Handler:          h,
		ag:               ag,
		batchSizeRPCOK:   createBatchSizeValue(ag, format.TagValueIDPacketFormatRPC, format.TagValueIDAgentReceiveStatusOK),
		batchSizeRPCErr:  createBatchSizeValue(ag, format.TagValueIDPacketFormatRPC, format.TagValueIDAgentReceiveStatusError),
		packetSizeRPCOK:  createPacketSizeValue(ag, format.TagValueIDPacketFormatRPC, format.TagValueIDAgentReceiveStatusOK),
		packetSizeRPCErr: createPacketSizeValue(ag, format.TagValueIDPacketFormatRPC, format.TagValueIDAgentReceiveStatusError),
	}
}

func getUserData(hctx *rpc.HandlerContext) *tlstatshouse.AddMetricsBatchBytes {
	ud, ok := hctx.UserData.(*tlstatshouse.AddMetricsBatchBytes)
	if !ok {
		ud = &tlstatshouse.AddMetricsBatchBytes{}
		hctx.UserData = ud
	}
	return ud
}

func (r *RPCReceiver) RawAddMetricsBatch(ctx context.Context, hctx *rpc.HandlerContext) error {
	args := getUserData(hctx)
	packetLen := len(hctx.Request)
	_, err := args.Read(hctx.Request)
	if err != nil {
		r.StatCallsTotalErr.Inc()
		r.Handler.HandleParseError(hctx.Request, err)
		setValueSize(r.packetSizeRPCErr, packetLen)
		return fmt.Errorf("failed to deserialize statshouse.addMetricsBatch request: %w", err)
	}
	var firstError error
	notDoneCount := 0
	// TODO - store both channel and callback in UserData to prevent 2 allocations
	ch := make(chan error, len(args.Metrics)) // buffer enough so that worker does not wait
	cb := func(m tlstatshouse.MetricBytes, h data_model.MappedMetricHeader) {
		ch <- mapping.MapErrorFromHeader(m, h)
	}
	for i := range args.Metrics {
		h, done := r.Handler.HandleMetrics(&args.Metrics[i], cb) // might move out metric, if needs to
		if done && firstError == nil && h.IngestionStatus != 0 {
			firstError = mapping.MapErrorFromHeader(args.Metrics[i], h)
		}
		if !done {
			notDoneCount++
		}
	}
	for i := 0; i < notDoneCount; i++ {
		err := <-ch
		if firstError == nil {
			firstError = err
		}
	}
	if firstError != nil {
		r.StatCallsTotalErr.Inc()
		setValueSize(r.batchSizeRPCErr, packetLen)
		setValueSize(r.packetSizeRPCErr, packetLen)
		return firstError
	}
	r.StatCallsTotalOK.Inc()
	setValueSize(r.batchSizeRPCOK, packetLen)
	setValueSize(r.packetSizeRPCOK, packetLen)
	hctx.Response, err = args.WriteResult(hctx.Response, tl.True{})
	return err
}
