// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metricshandler

import (
	"time"

	"github.com/VKCOM/statshouse-go"
	"github.com/VKCOM/statshouse/internal/vkgo/commonmetrics"
	"github.com/VKCOM/statshouse/internal/vkgo/commonmetrics/internal"
	"github.com/VKCOM/statshouse/internal/vkgo/rpc"
)

func CommonRPC(hctx *rpc.HandlerContext, err error) {
	var tags statshouse.Tags
	commonmetrics.AttachBaseS(tags[:])
	AttachRPC(tags[:], hctx, err)
	ResponseTimeRaw(tags, time.Since(hctx.RequestTime))
	ResponseSizeRaw(tags, len(hctx.Response))
	RequestSizeRaw(tags, len(hctx.Request))
}

func AttachRPC(tags []string, hctx *rpc.HandlerContext, err error) []string {
	status := commonmetrics.StatusFromError(err)
	method := internal.ParseStringAsMethod(hctx.RequestTag(), hctx.RequestFunctionName)
	tags[4] = commonmetrics.ProtocolRPC
	tags[5] = method.Group
	tags[6] = method.Name
	tags[7] = status.Description
	tags[8] = status.Code
	return tags
}
