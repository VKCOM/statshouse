// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"context"
	"time"

	"github.com/hrissan/tdigest"

	"github.com/VKCOM/statshouse/internal/chutil"
	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/format"
)

const (
	maxEvictionSampleSize = 100
	invalidateFrom        = -48 * time.Hour
	invalidateLinger      = 15 * time.Second // try to work around ClickHouse table replication race
	tsValueCount          = 7
)

type tsSelectRow struct {
	what tsWhat
	time int64
	tsTags
	tsValues
}

// all numeric tags are stored as int32 to save space
type tsTags struct {
	tag       [format.MaxTags]int64
	stag      [format.MaxTags]string
	shardNum  uint32
	stagCount int
}

type tsValues struct {
	min         float64
	max         float64
	sum         float64
	count       float64
	sumsquare   float64
	unique      data_model.ChUnique
	percentile  *tdigest.TDigest
	mergeCount  int
	cardinality float64

	minHost    chutil.ArgMinInt32Float32
	maxHost    chutil.ArgMaxInt32Float32
	minHostStr chutil.ArgMinStringFloat32
	maxHostStr chutil.ArgMaxStringFloat32
}

type tsWhat [tsValueCount]data_model.DigestSelector

func (s tsWhat) len() int {
	var n int
	for s.specifiedAt(n) {
		n++
	}
	return n
}

func (s tsWhat) specifiedAt(n int) bool {
	return n < tsValueCount && s[n].What != data_model.DigestUnspecified
}

type tsLoadFunc func(ctx context.Context, h *requestHandler, pq *queryBuilder, lod data_model.LOD, ret [][]tsSelectRow, retStartIx int) (int, error)

func (v *tsValues) merge(rhs tsValues) {
	if rhs.min < v.min {
		v.min = rhs.min
	}
	if v.max < rhs.max {
		v.max = rhs.max
	}
	v.sum += rhs.sum
	v.count += rhs.count
	v.sumsquare += rhs.sumsquare
	if v.mergeCount == 0 {
		// "unique" and "percentile" are holding references to memory
		// residing in read-only cache, therefore making a deep copy of them
		u := data_model.ChUnique{}
		u.Merge(v.unique)
		u.Merge(rhs.unique)
		v.unique = u
		if v.percentile != nil || rhs.percentile != nil {
			p := tdigest.New()
			if v.percentile != nil {
				p.Merge(v.percentile)
			}
			if rhs.percentile != nil {
				p.Merge(rhs.percentile)
			}
			v.percentile = p
		} else {
			v.percentile = nil
		}
	} else {
		// already operating on cache memory copy, it's safe to modify current object
		v.unique.Merge(rhs.unique)
		if v.percentile == nil {
			v.percentile = rhs.percentile
		} else if rhs.percentile != nil {
			v.percentile.Merge(rhs.percentile)
		}
	}
	v.mergeCount++
	v.cardinality += rhs.cardinality
	v.minHost.Merge(rhs.minHost)
	v.maxHost.Merge(rhs.maxHost)
	v.minHostStr.Merge(rhs.minHostStr)
	v.maxHostStr.Merge(rhs.maxHostStr)
}
