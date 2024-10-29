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
	"sync"
	"time"

	"github.com/vkcom/statshouse/internal/util"
)

type contextKey int

const (
	debugQueriesContextKey contextKey = iota
	accessInfoContextKey
	endpointStatContextKey
)

type debugQueries struct {
	queries *[]string
	mutex   sync.Mutex
}

func debugQueriesContext(ctx context.Context, queries *[]string) context.Context {
	return context.WithValue(ctx, debugQueriesContextKey, &debugQueries{queries, sync.Mutex{}})
}

func saveDebugQuery(ctx context.Context, query string) {
	p, ok := ctx.Value(debugQueriesContextKey).(*debugQueries)
	if ok {
		p.mutex.Lock()
		defer p.mutex.Unlock()
		query = strings.TrimSpace(strings.ReplaceAll(query, "\n", " "))
		*p.queries = append(*p.queries, query)
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

func reportQueryKind(ctx context.Context, isFast, isLight, isHardware bool) {
	if s, ok := ctx.Value(endpointStatContextKey).(*endpointStat); ok {
		s.laneMutex.Lock()
		defer s.laneMutex.Unlock()
		if len(s.lane) == 0 {
			s.lane = strconv.Itoa(util.QueryKind(isFast, isLight, isHardware))
		}
	}
}

func reportTiming(ctx context.Context, name string, dur time.Duration) {
	if s, ok := ctx.Value(endpointStatContextKey).(*endpointStat); ok {
		s.timings.Report(name, dur)
	}
}
