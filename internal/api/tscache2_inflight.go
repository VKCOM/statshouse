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
	return c.info.size() + int(c.inflightBytes)
}

func (c *cache2) NewInflightReq(cancel context.CancelFunc) uint32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.inflightReqID++
	id := c.inflightReqID
	c.inflightReqM[id] = &cache2InflightReq{cancel: cancel}
	return id
}

func (c *cache2) updateInflightApprox(reqID uint32, deltaBytes int64) {
	c.mu.Lock()
	r, ok := c.inflightReqM[reqID]
	if !ok {
		c.mu.Unlock()
		return
	}
	c.inflightBytes += deltaBytes
	r.bytes += deltaBytes
	soft := c.limits.maxSizeSoft
	shutdown := c.shutdownF
	eff := c.effectiveSizeLocked()
	c.mu.Unlock()

	if !shutdown && soft > 0 && eff > soft {
		c.trimCond.Signal()
		if deltaBytes > 0 {
			c.tryNotExceedMemoryHardLimitInflight()
		} else {
			c.tryNotExceedMemorySoftLimitInflight()
		}
	}
}

func (c *cache2) afterInflightLoadFinished(reqID uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.removeReqLocked(reqID)
}

func (c *cache2) removeReqLocked(reqID uint32) {
	r := c.inflightReqM[reqID]
	if r == nil || c.shutdownF {
		return
	}

	c.inflightBytes -= r.bytes
	r.cancel()
	delete(c.inflightReqM, reqID)

	eff := c.effectiveSizeLocked()
	if c.limits.maxSize == 0 || eff <= c.limits.maxSize {
		c.allocCond.Broadcast()
	}
	if c.limits.maxSizeSoft > 0 && eff > c.limits.maxSizeSoft {
		c.trimCond.Signal()
	}
}
