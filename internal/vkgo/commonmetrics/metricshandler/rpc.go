// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metricshandler

import (
	"time"

	"github.com/vkcom/statshouse/internal/vkgo/commonmetrics"
	"github.com/vkcom/statshouse/internal/vkgo/commonmetrics/internal"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
)

type rpcState struct {
}

func (s *rpcState) Reset() {
}

func (s *rpcState) BeforeCall(hctx *rpc.HandlerContext) {
}

func (s *rpcState) AfterCall(hctx *rpc.HandlerContext, err error) {
	status := commonmetrics.StatusFromError(err)

	r := InputRequest{
		Method:     internal.ParseStringAsMethod(hctx.RequestTag(), hctx.RequestFunctionName),
		Protocol:   commonmetrics.ProtocolRPC,
		Status:     status.Description,
		StatusCode: status.Code,
	}
	ResponseTime(r, time.Since(hctx.RequestTime))
	ResponseSize(r, len(hctx.Response))
	RequestSize(r, len(hctx.Request))
}

func RpcHooks() func() rpc.ServerHookState {
	return func() rpc.ServerHookState {
		return &rpcState{}
	}
}
