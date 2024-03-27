// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/vkcom/statshouse/internal/util"
)

type contextKey int

const (
	debugQueriesContextKey contextKey = iota
	accessInfoContextKey
	endpointStatContextKey
)

func debugQueriesContext(ctx context.Context, queries *[]string) context.Context {
	return context.WithValue(ctx, debugQueriesContextKey, queries)
}

func saveDebugQuery(ctx context.Context, query string) {
	p, ok := ctx.Value(debugQueriesContextKey).(*[]string)
	if ok {
		query = strings.TrimSpace(strings.ReplaceAll(query, "\n", " "))
		*p = append(*p, query)
	}
}

func withAccessInfo(ctx context.Context, ai *accessInfo) context.Context {
	return context.WithValue(ctx, accessInfoContextKey, ai)
}

func getAccessInfo(ctx context.Context) *accessInfo {
	if ai, ok := ctx.Value(accessInfoContextKey).(*accessInfo); ok {
		return ai
	}
	return nil
}

func withEndpointStat(ctx context.Context, es *endpointStat) context.Context {
	if es == nil {
		return ctx
	}
	return context.WithValue(ctx, endpointStatContextKey, es)
}

func reportQueryKind(ctx context.Context, isFast, isLight bool) {
	if s, ok := ctx.Value(endpointStatContextKey).(*endpointStat); ok {
		s.lane = strconv.Itoa(util.QueryKind(isFast, isLight))
	}
}

func reportTiming(ctx context.Context, name string, dur time.Duration) {
	if s, ok := ctx.Value(endpointStatContextKey).(*endpointStat); ok && s.timings != nil {
		s.timings.Report(name, dur, nil)
	}
}
