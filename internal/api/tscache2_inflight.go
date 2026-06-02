// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"context"
)

type cache2InflightCtxKey struct{}

func contextWithCache2Inflight(ctx context.Context, c *cache2) context.Context {
	if c == nil || ctx == nil {
		return ctx
	}
	if ctx.Value(cache2InflightCtxKey{}) != nil {
		return ctx
	}
	return context.WithValue(ctx, cache2InflightCtxKey{}, c)
}

func cache2FromInflightCtx(ctx context.Context) *cache2 {
	if ctx == nil {
		return nil
	}
	v, _ := ctx.Value(cache2InflightCtxKey{}).(*cache2)
	return v
}

func (c *cache2) effectiveSizeLocked() int {
	return c.info.size() + int(c.inflightBytes.Load())
}

func (c *cache2) updateInflightApprox(deltaBytes int64) {
	c.inflightBytes.Add(deltaBytes)
	c.mu.Lock()
	soft := c.limits.maxSizeSoft
	shutdown := c.shutdownF
	eff := c.effectiveSizeLocked()
	c.mu.Unlock()
	if !shutdown && soft > 0 && eff > soft {
		c.trimCond.Signal()
		c.tryNotExceedMemoryHardLimit()
	}
}

func (c *cache2) afterInflightLoadFinished() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.shutdownF {
		return
	}
	eff := c.effectiveSizeLocked()
	if c.limits.maxSize == 0 || eff <= c.limits.maxSize {
		c.allocCond.Broadcast()
	}
	if c.limits.maxSizeSoft > 0 && eff > c.limits.maxSizeSoft {
		c.trimCond.Signal()
	}
}
