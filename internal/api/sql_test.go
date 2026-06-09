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

	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/format"
)

const utcOffset = 3600 * 3 // GMT+3
var metric = &format.MetricMetaValue{MetricID: 1000}
var location = time.FixedZone("MSK", utcOffset)

func getLodForV6(t *testing.T, start int64, end int64, time_now int64, utc_offset int64) data_model.LOD {
	lods, err := data_model.GetLODs(data_model.GetTimescaleArgs{
		Start:       start,
		End:         end,
		ScreenWidth: 100,
		TimeNow:     time_now,
		Location:    location,
		UTCOffset:   utc_offset,
		Metric:      format.BuiltinMetricMetaIngestionStatus,
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(lods))
	return lods[0]
}

func TestLoadPointsQueryV6_1h(t *testing.T) {
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
	lod := getLodForV6(t, 100_000, 2_000_000, 2_000_000, 3)

	// execute
	query, err := pq.buildSeriesQuery(lod, " SETTINGS optimize_aggregation_in_order=1")

	// checks
	assert.NoError(t, err)
	assert.Equal(t, 2, query.what.len())
	assert.False(t, query.minMaxHost[0])
	assert.False(t, query.minMaxHost[1])

	assert.Empty(t, query.by)
	assert.Equal(t, `SELECT toInt64(toStartOfInterval(time+10800,INTERVAL 14400 second))-10800 AS _time,toFloat64(sum(1)) AS _val0,toFloat64(max(max)) AS _val1 FROM statshouse_v6_1h WHERE time>=86397 AND time<2001597 AND index_type=0 AND pre_tag=0 AND pre_stag='' AND metric=1000 AND (tag1 IN (1,2) OR stag1 IN ('one','two')) AND (0=0 AND stag0 NOT IN ('staging')) GROUP BY _time LIMIT 10000000 SETTINGS optimize_aggregation_in_order=1`, query.body)
}

func TestLoadPointsQueryV6_1m(t *testing.T) {
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
	lod := getLodForV6(t, 10_000, 20_000, 20_000, 3)

	// execute
	query, err := pq.buildSeriesQuery(lod, " SETTINGS optimize_aggregation_in_order=1")

	// checks
	assert.NoError(t, err)
	assert.Equal(t, 2, query.what.len())
	assert.False(t, query.minMaxHost[0])
	assert.False(t, query.minMaxHost[1])

	assert.Empty(t, query.by)
	assert.Equal(t, `SELECT toInt64(toStartOfInterval(time+10800,INTERVAL 60 second))-10800 AS _time,toFloat64(sum(1)) AS _val0,toFloat64(max(max)) AS _val1 FROM statshouse_v6_1m WHERE time>=9957 AND time<20037 AND index_type=0 AND pre_tag=0 AND pre_stag='' AND metric=1000 AND (tag1 IN (1,2) OR stag1 IN ('one','two')) AND (0=0 AND stag0 NOT IN ('staging')) GROUP BY _time LIMIT 10000000 SETTINGS optimize_aggregation_in_order=1`, query.body)
}

func TestLoadPointsQueryV6_1s(t *testing.T) {
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
	lod := getLodForV6(t, 10001, 10030, 10030, 0)

	// execute
	query, err := pq.buildSeriesQuery(lod, " SETTINGS optimize_aggregation_in_order=1")

	// checks
	assert.NoError(t, err)
	assert.Equal(t, 2, query.what.len())
	assert.False(t, query.minMaxHost[0])
	assert.False(t, query.minMaxHost[1])

	assert.Empty(t, query.by)
	assert.Equal(t, `SELECT toInt64(toStartOfInterval(time+10800,INTERVAL 1 second))-10800 AS _time,toFloat64(sum(1)) AS _val0,toFloat64(max(max)) AS _val1 FROM statshouse_v6_1s WHERE time>=10000 AND time<10030 AND index_type=0 AND pre_tag=0 AND pre_stag='' AND metric=1000 AND (tag1 IN (1,2) OR stag1 IN ('one','two')) AND (0=0 AND stag0 NOT IN ('staging')) GROUP BY _time LIMIT 10000000 SETTINGS optimize_aggregation_in_order=1`, query.body)
}
