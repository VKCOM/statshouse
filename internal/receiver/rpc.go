// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package receiver

import (
	"context"
	"fmt"

	"github.com/vkcom/statshouse/internal/agent"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tl"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
)

type RPCReceiver struct {
	parser

	Handler Handler
}

func MakeRPCReceiver(sh2 *agent.Agent, h Handler) *RPCReceiver {
	result := &RPCReceiver{
		parser:  parser{logPacket: nil, sh2: sh2, network: "rpc"},
		Handler: h,
	}
	result.parser.createMetrics()
	return result
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
		r.statBatchesTotalErr.Inc()
		r.Handler.HandleParseError(hctx.Request, err)
		setValueSize(r.packetSizeRPCErr, packetLen)
		return fmt.Errorf("failed to deserialize statshouse.addMetricsBatch request: %w", err)
	}
	firstError := r.handleAndWaitMetrics(r.Handler, args, &hctx.Response)
	hctx.Response = hctx.Response[:0]
	if firstError != nil {
		r.statBatchesTotalErr.Inc()
		setValueSize(r.batchSizeRPCErr, packetLen)
		setValueSize(r.packetSizeRPCErr, packetLen)
		return firstError
	}
	r.statBatchesTotalOK.Inc()
	setValueSize(r.batchSizeRPCOK, packetLen)
	setValueSize(r.packetSizeRPCOK, packetLen)
	hctx.Response, err = args.WriteResult(hctx.Response, tl.True{})
	return err
}
