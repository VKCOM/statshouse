// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metadata

import (
	"context"
	"time"

	"github.com/vkcom/statshouse/internal/vkgo/rpc"
)

type RpcMethod func(ctx context.Context, hctx *rpc.HandlerContext) (status string, err error)
type ProxyHandler struct {
	Host string
}

func (h *ProxyHandler) HandleProxy(name string, f RpcMethod) func(ctx context.Context, hctx *rpc.HandlerContext) error {
	return func(ctx context.Context, hctx *rpc.HandlerContext) error {
		start := time.Now()
		queryType, err := f(ctx, hctx)
		duration := time.Since(start)
		rpcDurationStat(h.Host, name, duration, err, queryType)
		return err
	}
}

func HandleProxyGen[A, B any](h *ProxyHandler, name string, f func(ctx context.Context, req A) (B, string, error)) func(ctx context.Context, req A) (B, error) {
	return func(ctx context.Context, a A) (B, error) {
		start := time.Now()
		resp, queryType, err := f(ctx, a)
		duration := time.Since(start)
		rpcDurationStat(h.Host, name, duration, err, queryType)
		return resp, err
	}
}
