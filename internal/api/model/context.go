// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"context"
	"strconv"
	"strings"

	"github.com/vkcom/statshouse/internal/util"
)

type contextKey int

const (
	DebugQueriesContextKey contextKey = iota
	accessInfoContextKey
	endpointStatContextKey
)

func DebugQueriesContext(ctx context.Context, queries *[]string) context.Context {
	return context.WithValue(ctx, DebugQueriesContextKey, queries)
}

func SaveDebugQuery(ctx context.Context, query string) {
	p, ok := ctx.Value(DebugQueriesContextKey).(*[]string)
	if ok {
		query = strings.TrimSpace(strings.ReplaceAll(query, "\n", " "))
		*p = append(*p, query)
	}
}

func WithAccessInfo(ctx context.Context, ai *AccessInfo) context.Context {
	return context.WithValue(ctx, accessInfoContextKey, ai)
}

func GetAccessInfo(ctx context.Context) *AccessInfo {
	if ai, ok := ctx.Value(accessInfoContextKey).(*AccessInfo); ok {
		return ai
	}
	return nil
}

func WithEndpointStat(ctx context.Context, es *EndpointStat) context.Context {
	if es == nil {
		return ctx
	}
	return context.WithValue(ctx, endpointStatContextKey, es)
}

func ReportQueryKind(ctx context.Context, isFast, isLight bool) {
	if s, ok := ctx.Value(endpointStatContextKey).(*EndpointStat); ok {
		s.lane = strconv.Itoa(util.QueryKind(isFast, isLight))
	}
}
