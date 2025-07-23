// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"strconv"
	"strings"

	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/format"
)

const (
	sortNone querySort = iota
	sortAscending
	sortDescending
)

const (
	buildSeriesQuery queryBuilderMode = iota
	buildTagValuesQuery
	buildTagValueIDsQuery
)

type queryBuilder struct {
	cacheKey         string
	version          string
	user             string
	metric           *format.MetricMetaValue
	what             tsWhat
	by               []int
	filterIn         data_model.TagFilters
	filterNotIn      data_model.TagFilters
	sort             querySort // for table view requests
	strcmpOff        bool      // version 3 experimental
	minMaxHost       [2]bool   // "min" at [0], "max" at [1]
	point            bool      // point query
	play             int
	utcOffset        int64
	newShardingStart int64

	// specific to queryKindTagValues, queryKindTagValueIDs
	tag        format.MetricMetaTag
	numResults int
}

type listItemSeparator struct {
	*strings.Builder
	value       string
	listStarted bool
}

type querySort int
type queryBuilderMode int

func (b *queryBuilder) preKeyTableName(lod *data_model.LOD) string {
	var usePreKey bool
	if lod.HasPreKey {
		preKeyTagX := b.preKeyTagX()
		usePreKey = lod.PreKeyOnly ||
			b.filterIn.Contains(preKeyTagX) ||
			b.filterNotIn.Contains(preKeyTagX)
		if !usePreKey {
			for _, v := range b.by {
				if v == preKeyTagX {
					usePreKey = true
					break
				}
			}
		}
	}
	newSharding := b.metric.NewSharding(lod.FromSec, b.newShardingStart)
	if usePreKey {
		return preKeyTableNames[lod.Table(newSharding)]
	}
	return lod.Table(newSharding)
}

func (b *queryBuilder) preKeyTagX() int {
	if v := b.singleMetric(); v != nil {
		return format.TagIndex(v.PreKeyTagID)
	}
	return -1
}

func (b *queryBuilder) singleMetric() *format.MetricMetaValue {
	if b.metric != nil {
		return b.metric
	}
	if len(b.filterIn.Metrics) == 1 {
		return b.filterIn.Metrics[0]
	}
	return nil
}

func sqlAggFn(fn string, lod *data_model.LOD) string {
	if lod.Version == Version1 {
		return fn + "Merge"
	}
	return fn
}

// as appears in SELECT clause
func (b *queryBuilder) selAlias(tagX int, lod *data_model.LOD) string {
	if lod.HasPreKey && tagX == b.preKeyTagX() {
		return "_prekey"
	}
	if b.raw64(tagX) {
		return "_tag" + strconv.Itoa(int(tagX))
	}
	return b.colInt(tagX, lod)
}

func (b *queryBuilder) colInt(tagX int, lod *data_model.LOD) string {
	if lod.Version == Version3 {
		return b.colIntV3(tagX, lod)
	}
	return b.colIntV2(tagX, lod)
}

func (b *queryBuilder) colIntV3(tagX int, lod *data_model.LOD) string {
	switch tagX {
	case format.StringTopTagIndex:
		return "tag" + format.StringTopTagIDV3
	case format.ShardTagIndex:
		return "_shard_num"
	default:
		if lod.HasPreKey && tagX == b.preKeyTagX() {
			return "pre_tag"
		}
		return "tag" + format.TagID(int(tagX))
	}
}

func (b *queryBuilder) colIntV2(tagX int, lod *data_model.LOD) string {
	var name string
	switch tagX {
	case format.ShardTagIndex:
		name = "_shard_num"
	default:
		if lod.HasPreKey && tagX == b.preKeyTagX() {
			name = "prekey"
		}
		name = "key" + format.TagID(int(tagX))
	}
	return name
}

func (b *queryBuilder) colStr(tagX int, lod *data_model.LOD) string {
	if lod.Version == Version3 {
		if tagX == format.StringTopTagIndex {
			return "stag" + format.StringTopTagIDV3
		}
		return "stag" + format.TagID(int(tagX))
	} else {
		return "skey"
	}
}

func (b *queryBuilder) raw64Expr(tagX int, lod *data_model.LOD) string {
	raw64Hi := tagX + 1
	return "bitOr(bitShiftLeft(toInt64(toUInt32(" + b.colInt(raw64Hi, lod) + ")),32),toUInt32(" + b.colInt(tagX, lod) + "))"
}

func (b *queryBuilder) newListComma() listItemSeparator {
	return listItemSeparator{value: ","}
}

func (b *listItemSeparator) maybeWrite(sb *strings.Builder) {
	if b.listStarted {
		sb.WriteString(b.value)
	} else {
		b.listStarted = true
	}
}

func (b *listItemSeparator) write(sb *strings.Builder) {
	sb.WriteString(b.value)
	b.listStarted = true
}

func (b *queryBuilder) isLight() bool {
	for i := 0; b.what.specifiedAt(i); i++ {
		switch b.what[i].What {
		case data_model.DigestUnique, data_model.DigestPercentile:
			return false
		}
	}
	return true
}

func (b *queryBuilder) isHardware() bool {
	return format.HardwareMetric(b.metricID())
}

func (b *queryBuilder) isStringTop() bool {
	v := b.singleMetric()
	return v != nil && v.StringTopDescription != ""
}

func (b *queryBuilder) metricID() int32 {
	if v := b.singleMetric(); v != nil {
		return v.MetricID
	}
	return 0
}

func (b *queryBuilder) groupedBy(tagX int) bool {
	for i := 0; i < len(b.by); i++ {
		if b.by[i] == tagX {
			return true
		}
	}
	return false
}
