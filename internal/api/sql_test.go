// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/format"
)

const utcOffset = 3600 * 3 // GMT+3
var metric = &format.MetricMetaValue{MetricID: 1000}
var location = time.FixedZone("MSK", utcOffset)

func getLod(t *testing.T, version string) data_model.LOD {
	lods, err := data_model.GetLODs(data_model.GetTimescaleArgs{
		Version:       version,
		Version3Start: 1,
		Start:         10_000,
		End:           20_000,
		ScreenWidth:   100,
		TimeNow:       20_000,
		Location:      location,
		UTCOffset:     3,
		Metric:        format.BuiltinMetricMetaIngestionStatus,
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(lods))
	return lods[0]
}

func TestTagValuesQueryV2(t *testing.T) {
	// prepare
	pq := &queryBuilder{
		metric:     metric,
		tag:        format.MetricMetaTag{Index: 2},
		numResults: 5,
		strcmpOff:  true,
	}
	pq.filterIn.AppendMapped(1, 1, 2)
	pq.filterNotIn.AppendMapped(0, 3)
	lod := getLod(t, Version2)

	// execute
	query := pq.buildTagValuesQuery(lod)

	// checks
	assert.Equal(t, "SELECT key2,toFloat64(sum(count)) AS _count FROM statshouse_value_1m_dist WHERE time>=9957 AND time<20037 AND metric=1000 AND (key1 IN (1,2)) AND (key0 NOT IN (3)) GROUP BY key2 HAVING _count>0 ORDER BY _count DESC,key2 LIMIT 6 SETTINGS optimize_aggregation_in_order=1", query.body)
}

func TestTagValuesQueryV2_stringTop(t *testing.T) {
	// prepare
	pq := &queryBuilder{
		metric:     metric,
		tag:        format.MetricMetaTag{Index: format.StringTopTagIndex},
		numResults: 5,
	}
	pq.filterIn.AppendMapped(1, 1, 2)
	pq.filterNotIn.AppendMapped(0, 3)
	lod := getLod(t, Version2)

	// execute
	query := pq.buildTagValuesQuery(lod)

	// checks
	assert.Equal(t, "SELECT skey,toFloat64(sum(count)) AS _count FROM statshouse_value_1m_dist WHERE time>=9957 AND time<20037 AND metric=1000 AND (key1 IN (1,2)) AND (key0 NOT IN (3)) GROUP BY skey HAVING _count>0 ORDER BY _count DESC,skey LIMIT 6 SETTINGS optimize_aggregation_in_order=1", query.body)
}

func TestTagValuesQueryV3(t *testing.T) {
	// prepare
	pq := &queryBuilder{
		metric:     metric,
		tag:        format.MetricMetaTag{Index: 2},
		numResults: 5,
	}
	pq.filterIn.Append(1, data_model.NewTagValue("one", 1), data_model.NewTagValue("two", 2))
	pq.filterNotIn.AppendValue(0, "staging")
	lod := getLod(t, Version3)

	// execute
	query := pq.buildTagValuesQuery(lod)

	// checks
	assert.Equal(t, "SELECT tag2,stag2,toFloat64(sum(count)) AS _count FROM statshouse_v3_1m_dist WHERE time>=9957 AND time<20037 AND metric=1000 AND (tag1 IN (1,2) OR stag1 IN ('one','two')) AND (0=0 AND stag0 NOT IN ('staging')) GROUP BY tag2,stag2 HAVING _count>0 ORDER BY _count DESC,tag2,stag2 LIMIT 6 SETTINGS optimize_aggregation_in_order=1", query.body)
}

func TestLoadPointsQueryV2(t *testing.T) {
	// prepare
	pq := queryBuilder{
		metric: metric,
		user:   "test-user",
		what: tsWhat{
			data_model.DigestSelector{What: data_model.DigestCardinality},
			data_model.DigestSelector{What: data_model.DigestMax},
		},
		utcOffset: utcOffset,
	}
	pq.filterIn.AppendMapped(1, 1, 2)
	pq.filterNotIn.AppendMapped(0, 3)
	lod := getLod(t, Version2)

	// execute
	query, err := pq.buildSeriesQuery(lod)

	// checks
	assert.NoError(t, err)
	assert.Equal(t, 2, query.what.len())
	assert.False(t, query.minMaxHost[0])
	assert.False(t, query.minMaxHost[1])
	assert.Equal(t, "2", query.version)
	assert.Empty(t, query.by)
	assert.Equal(t, "SELECT toInt64(toStartOfInterval(time+10800,INTERVAL 60 second))-10800 AS _time,toFloat64(sum(1)) AS _val0,toFloat64(max(max)) AS _val1 FROM statshouse_value_1m_dist WHERE time>=9957 AND time<20037 AND metric=1000 AND (key1 IN (1,2)) AND (key0 NOT IN (3)) GROUP BY _time LIMIT 10000000 SETTINGS optimize_aggregation_in_order=1", query.body)
}

func TestLoadPointsQueryV2_maxHost(t *testing.T) {
	// prepare
	pq := queryBuilder{
		metric: metric,
		user:   "test-user",
		what: tsWhat{
			data_model.DigestSelector{What: data_model.DigestMin},
			data_model.DigestSelector{What: data_model.DigestMax},
			data_model.DigestSelector{What: data_model.DigestAvg},
			data_model.DigestSelector{What: data_model.DigestSum},
			data_model.DigestSelector{What: data_model.DigestStdDev},
			data_model.DigestSelector{What: data_model.DigestCardinality},
		},
		minMaxHost: [2]bool{true, true},
		utcOffset:  utcOffset,
	}
	pq.filterIn.AppendMapped(1, 1, 2)
	pq.filterNotIn.AppendMapped(0, 3)
	lod := getLod(t, Version2)

	// execute
	query, err := pq.buildSeriesQuery(lod)

	// checks
	assert.NoError(t, err)
	assert.Equal(t, 6, query.what.len())
	assert.True(t, query.minMaxHost[0])
	assert.True(t, query.minMaxHost[1])
	assert.Equal(t, "2", query.version)
	assert.Empty(t, query.by)
	assert.Equal(t, "SELECT toInt64(toStartOfInterval(time+10800,INTERVAL 60 second))-10800 AS _time,toFloat64(min(min)) AS _val0,toFloat64(max(max)) AS _val1,toFloat64(sum(sum)) AS _val2,toFloat64(sum(count)) AS _val3,toFloat64(sum(sumsquare)) AS _val4,toFloat64(sum(1)) AS _val5,argMinMergeState(min_host) AS _minHost,argMaxMergeState(max_host) AS _maxHost FROM statshouse_value_1m_dist WHERE time>=9957 AND time<20037 AND metric=1000 AND (key1 IN (1,2)) AND (key0 NOT IN (3)) GROUP BY _time LIMIT 10000000 SETTINGS optimize_aggregation_in_order=1", query.body)
}

func TestLoadPointsQueryV3(t *testing.T) {
	// prepare
	pq := queryBuilder{
		metric: metric,
		user:   "test-user",
		what: tsWhat{
			data_model.DigestSelector{What: data_model.DigestCardinality},
			data_model.DigestSelector{What: data_model.DigestMax},
		},
		utcOffset: utcOffset,
	}
	pq.filterIn.Append(1, data_model.NewTagValue("one", 1), data_model.NewTagValue("two", 2))
	pq.filterNotIn.AppendValue(0, "staging")
	lod := getLod(t, Version3)

	// execute
	query, err := pq.buildSeriesQuery(lod)

	// checks
	assert.NoError(t, err)
	assert.Equal(t, 2, query.what.len())
	assert.False(t, query.minMaxHost[0])
	assert.False(t, query.minMaxHost[1])
	assert.Equal(t, "3", query.version)
	assert.Empty(t, query.by)
	assert.Equal(t, `SELECT toInt64(toStartOfInterval(time+10800,INTERVAL 60 second))-10800 AS _time,toFloat64(sum(1)) AS _val0,toFloat64(max(max)) AS _val1 FROM statshouse_v3_1m_dist WHERE time>=9957 AND time<20037 AND index_type=0 AND metric=1000 AND (tag1 IN (1,2) OR stag1 IN ('one','two')) AND (0=0 AND stag0 NOT IN ('staging')) GROUP BY _time LIMIT 10000000 SETTINGS optimize_aggregation_in_order=1`, query.body)
}

func TestLoadPointsQueryV3_maxHost(t *testing.T) {
	// prepare
	pq := queryBuilder{
		metric: metric,
		user:   "test-user",
		what: tsWhat{
			data_model.DigestSelector{What: data_model.DigestMin},
			data_model.DigestSelector{What: data_model.DigestMax},
			data_model.DigestSelector{What: data_model.DigestAvg},
			data_model.DigestSelector{What: data_model.DigestSum},
			data_model.DigestSelector{What: data_model.DigestStdDev},
			data_model.DigestSelector{What: data_model.DigestCardinality},
		},
		minMaxHost: [2]bool{true, true},
		utcOffset:  utcOffset,
		version:    Version3,
	}
	pq.filterIn.Append(1, data_model.NewTagValue("one", 1), data_model.NewTagValue("two", 2))
	pq.filterNotIn.AppendValue(0, "staging")
	lod := getLod(t, Version3)

	// execute
	query, err := pq.buildSeriesQuery(lod)

	// checks
	assert.NoError(t, err)
	assert.Equal(t, 6, query.what.len())
	assert.True(t, query.minMaxHost[0])
	assert.True(t, query.minMaxHost[1])
	assert.Equal(t, "3", query.version)
	assert.Empty(t, query.by)
	assert.Equal(t, `SELECT toInt64(toStartOfInterval(time+10800,INTERVAL 60 second))-10800 AS _time,toFloat64(min(min)) AS _val0,toFloat64(max(max)) AS _val1,toFloat64(sum(sum)) AS _val2,toFloat64(sum(count)) AS _val3,toFloat64(sum(sumsquare)) AS _val4,toFloat64(sum(1)) AS _val5,argMinMergeState(min_host) AS _minHost,argMaxMergeState(max_host) AS _maxHost FROM statshouse_v3_1m_dist WHERE time>=9957 AND time<20037 AND index_type=0 AND metric=1000 AND (tag1 IN (1,2) OR stag1 IN ('one','two')) AND (0=0 AND stag0 NOT IN ('staging')) GROUP BY _time LIMIT 10000000 SETTINGS optimize_aggregation_in_order=1`, query.body)
}
