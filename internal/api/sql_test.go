// Copyright 2024 V Kontakte LLC
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

const metricID = 1000
const utcOffset = 3600 * 3 // GMT+3

var location = time.FixedZone("MSK", utcOffset)

func getLod(t *testing.T, version string) data_model.LOD {
	lods, err := data_model.GetLODs(data_model.GetTimescaleArgs{
		Version:     version,
		Start:       10_000,
		End:         20_000,
		ScreenWidth: 100,
		TimeNow:     20_000,
		Location:    location,
		UTCOffset:   3,
		Metric:      format.BuiltinMetricMetaIngestionStatus,
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(lods))
	return lods[0]
}

func TestTagValuesQueryV2(t *testing.T) {
	// prepare
	pq := &preparedTagValuesQuery{
		version:     Version2,
		metricID:    metricID,
		tagID:       "2",
		numResults:  5,
		filterIn:    map[string][]any{"1": {"one", "two"}},
		filterNotIn: map[string][]any{"0": {"staging"}},
	}
	lod := getLod(t, pq.version)

	// execute
	query, meta, err := tagValuesQuery(pq, lod)

	// checks
	assert.NoError(t, err)
	assert.False(t, meta.stag)
	assert.Equal(t, `
SELECT
  key2 AS _value, toFloat64(sum(count)) AS _count
FROM
  statshouse_value_1m_dist
WHERE
  metric = 1000
  AND time >= 9957 AND time < 20037
  AND key1 IN ('one', 'two')
  AND key0 NOT IN ('staging')
GROUP BY
  key2
HAVING _count > 0
ORDER BY
  _count DESC,
  _value
LIMIT 6
SETTINGS
  optimize_aggregation_in_order = 1
`, query)
}

func TestTagValuesQueryV2_stringTop(t *testing.T) {
	// prepare
	pq := &preparedTagValuesQuery{
		version:     Version2,
		metricID:    metricID,
		tagID:       format.StringTopTagID,
		numResults:  5,
		filterIn:    map[string][]any{"1": {"one", "two"}},
		filterNotIn: map[string][]any{"0": {"staging"}},
	}
	lod := getLod(t, pq.version)

	// execute
	query, meta, err := tagValuesQuery(pq, lod)

	// checks
	assert.NoError(t, err)
	assert.True(t, meta.stag)
	assert.Equal(t, `
SELECT
  skey AS _string_value, toFloat64(sum(count)) AS _count
FROM
  statshouse_value_1m_dist
WHERE
  metric = 1000
  AND time >= 9957 AND time < 20037
  AND key1 IN ('one', 'two')
  AND key0 NOT IN ('staging')
GROUP BY
  skey
HAVING _count > 0
ORDER BY
  _count DESC,
  _string_value
LIMIT 6
SETTINGS
  optimize_aggregation_in_order = 1
`, query)
}

func TestTagValuesQueryV3(t *testing.T) {
	// prepare
	pq := &preparedTagValuesQuery{
		version:       Version3,
		metricID:      metricID,
		tagID:         "2",
		numResults:    5,
		filterIn:      map[string][]any{"1": {"one", "two"}},
		filterNotIn:   map[string][]any{"0": {"staging"}},
		filterInV3:    map[string][]maybeMappedTag{"1": {{"one", 1}, {"two", 2}}},
		filterNotInV3: map[string][]maybeMappedTag{"0": {{"staging", 0}}},
	}
	lod := getLod(t, pq.version)

	// execute
	query, meta, err := tagValuesQuery(pq, lod)

	// checks
	assert.NoError(t, err)
	assert.False(t, meta.stag)
	assert.True(t, meta.mixed)
	assert.Equal(t, `
SELECT tag2 AS _mapped, stag2 AS _unmapped, toFloat64(sum(count)) AS _count
FROM statshouse_v3_1m_dist
WHERE metric = 1000
  AND time >= 9957 AND time < 20037
  AND (tag1 IN (1, 2) OR stag1 IN ('one', 'two'))
  AND (tag0 NOT IN (0) AND stag0 NOT IN ('staging'))

GROUP BY _mapped, _unmapped
HAVING _count > 0
ORDER BY _count DESC, _mapped, _unmapped
LIMIT 6
SETTINGS optimize_aggregation_in_order = 1
`, query)
}

func TestLoadPointsQueryV2(t *testing.T) {
	// prepare
	pq := &preparedPointsQuery{
		user:        "test-user",
		version:     Version2,
		metricID:    metricID,
		isStringTop: false,
		kind:        data_model.DigestCountSec.Kind(false),
		filterIn:    map[string][]any{"1": {"one", "two"}},
		filterNotIn: map[string][]any{"0": {"staging"}},
	}
	lod := getLod(t, pq.version)

	// execute
	query, meta, err := loadPointsQuery(pq, lod, utcOffset)

	// checks
	assert.NoError(t, err)
	assert.Equal(t, 2, meta.vals)
	assert.False(t, meta.minMaxHost)
	assert.Equal(t, "2", meta.version)
	assert.Empty(t, meta.tags)
	assert.Equal(t, `
SELECT
  
toInt64(toStartOfInterval(time + 10800, INTERVAL 60 second)) - 10800 AS _time,
toInt64(60) AS _stepSec, 
  toFloat64(sum(count)) AS _count,
  toFloat64(sum(1)) AS _val0,
  toFloat64(max(max)) AS _val1
FROM
  statshouse_value_1m_dist
WHERE
  metric = 1000
  AND time >= 9957 AND time < 20037
  AND key1 IN ('one', 'two')
  AND key0 NOT IN ('staging')
GROUP BY
  _time
LIMIT 10000000
SETTINGS
  optimize_aggregation_in_order = 1
`, query)
}

func TestLoadPointsQueryV2_maxHost(t *testing.T) {
	// prepare
	pq := &preparedPointsQuery{
		user:        "test-user",
		version:     Version2,
		metricID:    metricID,
		isStringTop: false,
		kind:        data_model.DigestCountSec.Kind(true),
		filterIn:    map[string][]any{"1": {"one", "two"}},
		filterNotIn: map[string][]any{"0": {"staging"}},
	}
	lod := getLod(t, pq.version)

	// execute
	query, meta, err := loadPointsQuery(pq, lod, utcOffset)

	// checks
	assert.NoError(t, err)
	assert.Equal(t, 6, meta.vals)
	assert.True(t, meta.minMaxHost)
	assert.Equal(t, "2", meta.version)
	assert.Empty(t, meta.tags)
	assert.Equal(t, `
SELECT
  
toInt64(toStartOfInterval(time + 10800, INTERVAL 60 second)) - 10800 AS _time,
toInt64(60) AS _stepSec, 
  toFloat64(sum(count)) AS _count,
  toFloat64(min(min)) AS _val0,
  toFloat64(max(max)) AS _val1,
  toFloat64(sum(sum))/toFloat64(sum(count)) AS _val2,
  toFloat64(sum(sum)) AS _val3,
  if(sum(count) < 2, 0, sqrt(greatest(   (sum(sumsquare) - pow(sum(sum), 2) / sum(count)) / (sum(count) - 1)   , 0))) AS _val4,
  toFloat64(sum(1)) AS _val5,
  argMinMerge(min_host) as _minHost, argMaxMerge(max_host) as _maxHost
FROM
  statshouse_value_1m_dist
WHERE
  metric = 1000
  AND time >= 9957 AND time < 20037
  AND key1 IN ('one', 'two')
  AND key0 NOT IN ('staging')
GROUP BY
  _time
LIMIT 10000000
SETTINGS
  optimize_aggregation_in_order = 1
`, query)
}

func TestLoadPointsQueryV3(t *testing.T) {
	// prepare
	pq := &preparedPointsQuery{
		user:          "test-user",
		version:       Version3,
		metricID:      metricID,
		isStringTop:   false,
		kind:          data_model.DigestCountSec.Kind(false),
		filterIn:      map[string][]any{"1": {"one", "two"}},
		filterNotIn:   map[string][]any{"0": {"staging"}},
		filterInV3:    map[string][]maybeMappedTag{"1": {{"one", 1}, {"two", 2}}},
		filterNotInV3: map[string][]maybeMappedTag{"0": {{"staging", 0}}},
	}
	lod := getLod(t, pq.version)

	// execute
	query, meta, err := loadPointsQuery(pq, lod, utcOffset)

	// checks
	assert.NoError(t, err)
	assert.Equal(t, 2, meta.vals)
	assert.False(t, meta.minMaxHost)
	assert.Equal(t, "3", meta.version)
	assert.Empty(t, meta.tags)
	assert.Equal(t, `SELECT
  toInt64(toStartOfInterval(time + 10800, INTERVAL 60 second)) - 10800 AS _time,
  toInt64(60) AS _stepSec, 
  toFloat64(sum(count)) AS _count,
  toFloat64(sum(1)) AS _val0,
  toFloat64(max(max)) AS _val1
FROM statshouse_v3_1m_dist
WHERE index_type = 0 AND metric = 1000 AND time >= 9957 AND time < 20037  AND (tag1 IN (1, 2) OR stag1 IN ('one', 'two'))
  AND (tag0 NOT IN (0) AND stag0 NOT IN ('staging'))

GROUP BY _time
LIMIT 10000000
SETTINGS optimize_aggregation_in_order = 1`, query)
}

func TestLoadPointsQueryV3_maxHost(t *testing.T) {
	// prepare
	pq := &preparedPointsQuery{
		user:          "test-user",
		version:       Version3,
		metricID:      metricID,
		isStringTop:   false,
		kind:          data_model.DigestCountSec.Kind(true),
		filterIn:      map[string][]any{"1": {"one", "two"}},
		filterNotIn:   map[string][]any{"0": {"staging"}},
		filterInV3:    map[string][]maybeMappedTag{"1": {{"one", 1}, {"two", 2}}},
		filterNotInV3: map[string][]maybeMappedTag{"0": {{"staging", 0}}},
	}
	lod := getLod(t, pq.version)

	// execute
	query, meta, err := loadPointsQuery(pq, lod, utcOffset)

	// checks
	assert.NoError(t, err)
	assert.Equal(t, 6, meta.vals)
	assert.True(t, meta.minMaxHost)
	assert.Equal(t, "3", meta.version)
	assert.Empty(t, meta.tags)
	assert.Equal(t, `SELECT
  toInt64(toStartOfInterval(time + 10800, INTERVAL 60 second)) - 10800 AS _time,
  toInt64(60) AS _stepSec, 
  toFloat64(sum(count)) AS _count,
  toFloat64(min(min)) AS _val0,
  toFloat64(max(max)) AS _val1,
  toFloat64(sum(sum))/toFloat64(sum(count)) AS _val2,
  toFloat64(sum(sum)) AS _val3,
  if(sum(count) < 2, 0, sqrt(greatest(   (sum(sumsquare) - pow(sum(sum), 2) / sum(count)) / (sum(count) - 1)   , 0))) AS _val4,
  toFloat64(sum(1)) AS _val5,
  argMinMerge(min_host) as _minHost, argMaxMerge(max_host) as _maxHost
FROM statshouse_v3_1m_dist
WHERE index_type = 0 AND metric = 1000 AND time >= 9957 AND time < 20037  AND (tag1 IN (1, 2) OR stag1 IN ('one', 'two'))
  AND (tag0 NOT IN (0) AND stag0 NOT IN ('staging'))

GROUP BY _time
LIMIT 10000000
SETTINGS optimize_aggregation_in_order = 1`, query)
}
