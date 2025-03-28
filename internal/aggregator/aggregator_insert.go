// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package aggregator

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"pgregory.net/rand"

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/metajournal"
	"github.com/vkcom/statshouse/internal/vkgo/rowbinary"
	"github.com/vkcom/statshouse/internal/vkgo/srvfunc"
)

func getTableDesc(v3Format bool) string {
	if v3Format {
		keysFieldsNamesVec := make([]string, format.NewMaxTags)
		for i := 0; i < format.NewMaxTags; i++ {
			keysFieldsNamesVec[i] = fmt.Sprintf(`tag%d,stag%d`, i, i)
		}
		return `statshouse_v3_incoming(index_type,metric,time,` + strings.Join(keysFieldsNamesVec, `,`) + `,count,min,max,sum,sumsquare,percentiles,uniq_state,min_host_legacy,max_host_legacy,min_host,max_host)`
	}
	keysFieldsNamesVec := make([]string, format.MaxTags)
	for i := 0; i < format.MaxTags; i++ {
		keysFieldsNamesVec[i] = fmt.Sprintf(`key%d`, i)
	}
	return `statshouse_value_incoming_prekey3(metric,prekey,prekey_set,time,` + strings.Join(keysFieldsNamesVec, `,`) + `,count,min,max,sum,sumsquare,percentiles,uniq_state,skey,min_host,max_host)`
}

type lastMetricData struct {
	lastMetricPrekey        int
	lastMetricPrekeyOnly    bool
	lastMetricSkipMaxHost   bool
	lastMetricSkipMinHost   bool
	lastMetricSkipSumSquare bool
}

type metricIndexCache struct {
	// Motivation - we have statList sorted by metric, except ingestion statuses are interleaved,
	// because they are credited to the metric. Also, we have small # of builtin metrics inserted, but we do not care about speed for them.
	journal             *metajournal.MetricsStorage
	ingestionStatusData lastMetricData
	lastMetricID        int32
	lastMetric          lastMetricData
}

func makeMetricCache(journal *metajournal.MetricsStorage) *metricIndexCache {
	result := &metricIndexCache{
		journal:             journal,
		ingestionStatusData: lastMetricData{lastMetricPrekey: -1},
		lastMetric:          lastMetricData{lastMetricPrekey: -1}, // so if somehow 0 metricID is inserted first, will have no prekey

	}
	bm := format.BuiltinMetricMetaIngestionStatus
	result.ingestionStatusData.lastMetricPrekeyOnly = bm.PreKeyOnly
	result.ingestionStatusData.lastMetricPrekey = bm.PreKeyIndex
	result.ingestionStatusData.lastMetricSkipMinHost = bm.SkipMinHost
	result.ingestionStatusData.lastMetricSkipMaxHost = bm.SkipMaxHost
	result.ingestionStatusData.lastMetricSkipSumSquare = bm.SkipSumSquare
	return result
}

func (p *metricIndexCache) getPrekeyIndex(metricID int32) (int, bool) {
	if metricID == format.BuiltinMetricIDIngestionStatus {
		return p.ingestionStatusData.lastMetricPrekey, false
	}
	if p.metric(metricID) {
		return p.lastMetric.lastMetricPrekey, p.lastMetric.lastMetricPrekeyOnly
	}
	return -1, false
}

func (p *metricIndexCache) metric(metricID int32) bool {
	if metricID == p.lastMetricID {
		return true
	}
	p.lastMetricID = metricID
	if bm, ok := format.BuiltinMetrics[metricID]; ok {
		p.lastMetric.lastMetricPrekey = bm.PreKeyIndex
		p.lastMetric.lastMetricPrekeyOnly = bm.PreKeyOnly
		p.lastMetric.lastMetricSkipMinHost = bm.SkipMinHost
		p.lastMetric.lastMetricSkipMaxHost = bm.SkipMaxHost
		p.lastMetric.lastMetricSkipSumSquare = bm.SkipSumSquare
		return true
	}
	if metaMetric := p.journal.GetMetaMetric(metricID); metaMetric != nil {
		p.lastMetric.lastMetricPrekey = metaMetric.PreKeyIndex
		p.lastMetric.lastMetricPrekeyOnly = metaMetric.PreKeyOnly
		p.lastMetric.lastMetricSkipMinHost = metaMetric.SkipMinHost
		p.lastMetric.lastMetricSkipMaxHost = metaMetric.SkipMaxHost
		p.lastMetric.lastMetricSkipSumSquare = metaMetric.SkipSumSquare
		return true
	}
	return false
}

func (p *metricIndexCache) skips(metricID int32) (skipMaxHost bool, skipMinHost bool, skipSumSquare bool) {
	if metricID == format.BuiltinMetricIDIngestionStatus {
		return p.ingestionStatusData.lastMetricSkipMaxHost, p.ingestionStatusData.lastMetricSkipMinHost, p.ingestionStatusData.lastMetricSkipSumSquare
	}
	if p.metric(metricID) {
		return p.lastMetric.lastMetricSkipMaxHost, p.lastMetric.lastMetricSkipMinHost, p.lastMetric.lastMetricSkipSumSquare
	}
	return false, false, false
}

func appendKeys(res []byte, k *data_model.Key, metricCache *metricIndexCache, v3Format bool, top data_model.TagUnion, bufferedInsert bool) []byte {
	if v3Format {
		return appendKeysNewFormat(res, k, metricCache, top, bufferedInsert)
	}
	appendTag := func(res []byte, v uint32) []byte {
		res = binary.LittleEndian.AppendUint32(res, v)
		return res
	}
	res = binary.LittleEndian.AppendUint32(res, uint32(k.Metric))
	prekeyIndex, prekeyOnly := metricCache.getPrekeyIndex(k.Metric)
	if prekeyIndex >= 0 {
		res = binary.LittleEndian.AppendUint32(res, uint32(k.Tags[prekeyIndex]))
		if prekeyOnly {
			res = append(res, byte(2))
		} else {
			res = append(res, byte(1))
		}
	} else {
		res = binary.LittleEndian.AppendUint32(res, uint32(0))
		res = append(res, byte(0))
	}
	res = binary.LittleEndian.AppendUint32(res, k.Timestamp)
	tagsN := format.MaxTags
	for ki := 0; ki < tagsN; ki++ {
		res = appendTag(res, uint32(k.Tags[ki]))
	}
	return res
}

func appendKeysNewFormat(res []byte, k *data_model.Key, metricCache *metricIndexCache, top data_model.TagUnion, bufferedInsert bool) []byte {
	appendTag := func(res []byte, k *data_model.Key, i int) []byte {
		if i >= len(k.Tags) { // temporary while we in transition between 16 and 48 tags
			res = binary.LittleEndian.AppendUint32(res, 0)
			res = rowbinary.AppendString(res, "")
			return res
		}
		if len(k.GetSTag(i)) > 0 {
			res = binary.LittleEndian.AppendUint32(res, 0)
			res = rowbinary.AppendString(res, k.GetSTag(i))
			return res
		}
		res = binary.LittleEndian.AppendUint32(res, uint32(k.Tags[i]))
		res = rowbinary.AppendString(res, "")
		return res
	}
	var it uint8
	if bufferedInsert {
		it = 1
	}
	res = append(res, it)
	res = binary.LittleEndian.AppendUint32(res, uint32(k.Metric))
	// TODO write pretags
	_ = metricCache
	res = binary.LittleEndian.AppendUint32(res, k.Timestamp)
	for ki := 0; ki < format.NewMaxTags; ki++ {
		if ki == format.StringTopTagIndexV3 {
			continue
		}
		res = appendTag(res, k, ki)
	}
	// write string top
	if top.I > 0 || len(top.S) == 0 { // if we have both I and S use prefer I (we keep S to v2 compat)
		res = binary.LittleEndian.AppendUint32(res, uint32(top.I))
		res = rowbinary.AppendString(res, "")
	} else {
		res = binary.LittleEndian.AppendUint32(res, 0)
		res = rowbinary.AppendString(res, top.S)
	}
	return res
}

// TODO - badges are badly designed for now. Should be redesigned some day.
// We propose to move them inside metric with env=-1,-2,etc.
// So we can select badges for free by adding || (env < 0) to requests, then filtering result rows
// Also we must select both count and sum, then process them separately for each badge kind

func appendMultiBadge(rng *rand.Rand, res []byte, k *data_model.Key, v *data_model.MultiItem, metricCache *metricIndexCache, usedTimestamps map[uint32]struct{}, v3Format bool) []byte {
	if k.Metric >= 0 { // fastpath
		return res
	}
	for _, t := range v.Top {
		res = appendBadge(rng, res, k, t.Value, metricCache, usedTimestamps, v3Format)
	}
	return appendBadge(rng, res, k, v.Tail.Value, metricCache, usedTimestamps, v3Format)
}

func appendBadge(rng *rand.Rand, res []byte, k *data_model.Key, v data_model.ItemValue, metricCache *metricIndexCache, usedTimestamps map[uint32]struct{}, v3Format bool) []byte {
	if k.Metric >= 0 { // fastpath
		return res
	}
	ts := (k.Timestamp / 5) * 5
	usedTimestamps[ts] = struct{}{}
	// We used to select with single function (avg), so we approximated sum of counters so that any number of events produce avg >= 1
	// TODO - deprecated legacy badges and use new badges
	switch k.Metric {
	case format.BuiltinMetricIDIngestionStatus:
		if k.Tags[1] == 0 {
			return res
		}
		switch k.Tags[2] {
		case format.TagValueIDSrcIngestionStatusOKCached,
			format.TagValueIDSrcIngestionStatusOKUncached,
			format.TagValueIDSrcIngestionStatusOKDup:
			return res
		case format.TagValueIDSrcIngestionStatusWarnDeprecatedKeyName,
			format.TagValueIDSrcIngestionStatusWarnDeprecatedT,
			format.TagValueIDSrcIngestionStatusWarnDeprecatedStop,
			format.TagValueIDSrcIngestionStatusWarnMapTagSetTwice,
			format.TagValueIDSrcIngestionStatusWarnOldCounterSemantic,
			format.TagValueIDSrcIngestionStatusWarnTimestampClampedPast,
			format.TagValueIDSrcIngestionStatusWarnTimestampClampedFuture,
			format.TagValueIDSrcIngestionStatusWarnMapInvalidRawTagValue:
			return appendValueStat(rng, res, &data_model.Key{Timestamp: ts, Metric: format.BuiltinMetricIDBadges, Tags: [format.MaxTags]int32{0, format.TagValueIDBadgeIngestionWarnings, k.Tags[1]}}, v, metricCache, v3Format, false)
		}
		return appendValueStat(rng, res, &data_model.Key{Timestamp: ts, Metric: format.BuiltinMetricIDBadges, Tags: [format.MaxTags]int32{0, format.TagValueIDBadgeIngestionErrors, k.Tags[1]}}, v, metricCache, v3Format, false)
	case format.BuiltinMetricIDAgentSamplingFactor:
		return appendValueStat(rng, res, &data_model.Key{Timestamp: ts, Metric: format.BuiltinMetricIDBadges, Tags: [format.MaxTags]int32{0, format.TagValueIDBadgeAgentSamplingFactor, k.Tags[1]}}, v, metricCache, v3Format, false)
	case format.BuiltinMetricIDAggSamplingFactor:
		return appendValueStat(rng, res, &data_model.Key{Timestamp: ts, Metric: format.BuiltinMetricIDBadges, Tags: [format.MaxTags]int32{0, format.TagValueIDBadgeAggSamplingFactor, k.Tags[4]}}, v, metricCache, v3Format, false)
	case format.BuiltinMetricIDAggMappingCreated:
		if k.Tags[5] == format.TagValueIDAggMappingCreatedStatusOK ||
			k.Tags[5] == format.TagValueIDAggMappingCreatedStatusCreated {
			return res
		}
		return appendValueStat(rng, res, &data_model.Key{Timestamp: ts, Metric: format.BuiltinMetricIDBadges, Tags: [format.MaxTags]int32{0, format.TagValueIDBadgeAggMappingErrors, k.Tags[4]}}, v, metricCache, v3Format, false)
	case format.BuiltinMetricIDAggBucketReceiveDelaySec:
		return appendValueStat(rng, res, &data_model.Key{Timestamp: ts, Metric: format.BuiltinMetricIDBadges, Tags: [format.MaxTags]int32{0, format.TagValueIDBadgeContributors, 0}}, v, metricCache, v3Format, false)
	}
	return res
}

func appendAggregates(res []byte, c float64, mi float64, ma float64, su float64, su2 float64) []byte {
	var tmp [5 * 8]byte // Most efficient way
	binary.LittleEndian.PutUint64(tmp[0*8:], math.Float64bits(c))
	binary.LittleEndian.PutUint64(tmp[1*8:], math.Float64bits(mi))
	binary.LittleEndian.PutUint64(tmp[2*8:], math.Float64bits(ma))
	binary.LittleEndian.PutUint64(tmp[3*8:], math.Float64bits(su))
	binary.LittleEndian.PutUint64(tmp[4*8:], math.Float64bits(su2))
	return append(res, tmp[:]...)
}

func appendValueStat(rng *rand.Rand, res []byte, key *data_model.Key, v data_model.ItemValue, cache *metricIndexCache, newFormat bool, bufferedInsert bool) []byte {
	count := v.Count()
	if count <= 0 { // We have lots of built-in  counters which are normally 0
		return res
	}
	// for explanation of insert logic, see multiValueMarshal below
	res = appendKeys(res, key, cache, newFormat, data_model.TagUnion{}, bufferedInsert)
	skipMaxHost, skipMinHost, skipSumSquare := cache.skips(key.Metric)
	if v.ValueSet {
		res = appendAggregates(res, count, v.ValueMin, v.ValueMax, v.ValueSum, zeroIfTrue(v.ValueSumSquare, skipSumSquare))
	} else {
		res = appendAggregates(res, count, 0, count, 0, 0)
	}

	res = rowbinary.AppendEmptyCentroids(res)
	res = rowbinary.AppendEmptyUnique(res)
	if !newFormat {
		res = rowbinary.AppendString(res, "")
	}

	// min_host_legacy, max_host_legacy
	if v.ValueSet {
		if skipMinHost {
			res = rowbinary.AppendArgMinMaxInt32Float32Empty(res)
		} else {
			res = rowbinary.AppendArgMinMaxInt32Float32(res, v.MinHostTag.I, float32(v.ValueMin))
		}
		if skipMaxHost {
			res = rowbinary.AppendArgMinMaxInt32Float32Empty(res)
		} else {
			res = rowbinary.AppendArgMinMaxInt32Float32(res, v.MaxHostTag.I, float32(v.ValueMax))
		}
	} else {
		res = rowbinary.AppendArgMinMaxInt32Float32Empty(res)
		if skipMaxHost {
			res = rowbinary.AppendArgMinMaxInt32Float32Empty(res)
		} else {
			cc := float32(data_model.SkewMaxCounterHost(rng, count)) // explanation is in Skew function
			res = rowbinary.AppendArgMinMaxInt32Float32(res, v.MaxCounterHostTag.I, cc)
		}
	}
	// min_host, max_host
	if newFormat {
		if v.ValueSet {
			if skipMinHost {
				res = rowbinary.AppendArgMinMaxStringEmpty(res)
			} else {
				res = appendArgMinMaxTag(res, v.MinHostTag, float32(v.ValueMin))
			}
			if skipMaxHost {
				res = rowbinary.AppendArgMinMaxStringEmpty(res)
			} else {
				res = appendArgMinMaxTag(res, v.MaxHostTag, float32(v.ValueMax))
			}
		} else {
			res = rowbinary.AppendArgMinMaxStringEmpty(res)
			if skipMaxHost {
				res = rowbinary.AppendArgMinMaxStringEmpty(res)
			} else {
				cc := float32(data_model.SkewMaxCounterHost(rng, count)) // explanation is in Skew function
				res = appendArgMinMaxTag(res, v.MaxCounterHostTag, cc)
			}
		}
	}

	return res
}

func appendSimpleValueStat(rng *rand.Rand, res []byte, key *data_model.Key, v float64, count float64, hostTag int32, metricCache *metricIndexCache, newFormat bool) []byte {
	return appendValueStat(rng, res, key, data_model.SimpleItemValue(v, count, data_model.TagUnionBytes{I: hostTag}), metricCache, newFormat, false)
}

func appendBufferedValueStat(rng *rand.Rand, res []byte, key *data_model.Key, v float64, count float64, hostTag int32, metricCache *metricIndexCache, newFormat bool) []byte {
	return appendValueStat(rng, res, key, data_model.SimpleItemValue(v, count, data_model.TagUnionBytes{I: hostTag}), metricCache, newFormat, true)
}

func multiValueMarshal(rng *rand.Rand, metricID int32, cache *metricIndexCache, res []byte, value *data_model.MultiValue, top data_model.TagUnion, sf float64, v3Format bool) []byte {
	skipMaxHost, skipMinHost, skipSumSquare := cache.skips(metricID)
	counter := value.Value.Count() * sf
	if value.Value.ValueSet {
		res = appendAggregates(res, counter, value.Value.ValueMin, value.Value.ValueMax, value.Value.ValueSum*sf, zeroIfTrue(value.Value.ValueSumSquare*sf, skipSumSquare))
	} else {
		// motivation - we set MaxValue to aggregated counter, so this value will be preserved while merging into minute or hour table
		// later, when selecting, we can sum them from individual shards, showing approximately counter/sec spikes
		// https://clickhouse.com/docs/en/engines/table-engines/special/distributed#_shard_num
		res = appendAggregates(res, counter, 0, counter, 0, 0)
	}
	res = rowbinary.AppendCentroids(res, value.ValueTDigest, sf)
	res = value.HLL.MarshallAppend(res)
	if !v3Format {
		res = rowbinary.AppendString(res, top.S)
	}
	// min_host_legacy, max_host_legacy
	if value.Value.ValueSet {
		if skipMinHost {
			res = rowbinary.AppendArgMinMaxInt32Float32Empty(res)
		} else {
			res = rowbinary.AppendArgMinMaxInt32Float32(res, value.Value.MinHostTag.I, float32(value.Value.ValueMin))
		}
		if skipMaxHost {
			res = rowbinary.AppendArgMinMaxInt32Float32Empty(res)
		} else {
			res = rowbinary.AppendArgMinMaxInt32Float32(res, value.Value.MaxHostTag.I, float32(value.Value.ValueMax))
		}
	} else {
		res = rowbinary.AppendArgMinMaxInt32Float32Empty(res) // counters do not have min_host set
		if skipMaxHost {
			res = rowbinary.AppendArgMinMaxInt32Float32Empty(res)
		} else {
			cc := float32(data_model.SkewMaxCounterHost(rng, counter)) // explanation is in Skew function
			res = rowbinary.AppendArgMinMaxInt32Float32(res, value.Value.MaxCounterHostTag.I, cc)
		}
	}
	// min_host, max_host
	if v3Format {
		v := value.Value
		if v.ValueSet {
			if skipMinHost {
				res = rowbinary.AppendArgMinMaxStringEmpty(res)
			} else {
				res = appendArgMinMaxTag(res, v.MinHostTag, float32(v.ValueMin))
			}
			if skipMaxHost {
				res = rowbinary.AppendArgMinMaxStringEmpty(res)
			} else {
				res = appendArgMinMaxTag(res, v.MaxHostTag, float32(v.ValueMax))
			}
		} else {
			res = rowbinary.AppendArgMinMaxStringEmpty(res)
			if skipMaxHost {
				res = rowbinary.AppendArgMinMaxStringEmpty(res)
			} else {
				cc := float32(data_model.SkewMaxCounterHost(rng, counter)) // explanation is in Skew function
				res = appendArgMinMaxTag(res, v.MaxCounterHostTag, cc)
			}
		}
	}
	return res
}

type insertSize struct {
	counters    int
	values      int
	percentiles int
	uniques     int
	stringTops  int
	builtin     int
}

type samplingStatKey struct {
	namespeceId int32
	groupId     int32
}

type samplingStat struct {
	sampligSizeKeepBytes    data_model.ItemValue
	sampligSizeDiscardBytes data_model.ItemValue
	samplingGroupBudget     data_model.ItemValue
}

type insertStats struct {
	recentTs    uint32
	historicTag int32

	sizes        map[uint32]insertSize // key is bucketTs
	contributors int

	samplingMetricCount int
	samplingBudget      int64
	sampling            map[samplingStatKey]samplingStat

	sampleTimeAppend     float64
	sampleTimePartition  float64
	sampleTimeBudgeting  float64
	sampleTimeSampling   float64
	sampleTimeMetricMeta float64
	samplingEngineKeys   float64
}

func (a *Aggregator) RowDataMarshalAppendPositions(buckets []*aggregatorBucket, buffers data_model.SamplerBuffers, rnd *rand.Rand, res []byte,
	v3Format bool) ([]byte, data_model.SamplerBuffers, insertStats, time.Duration) {
	startTime := time.Now()
	recentTs := buckets[0].time // by convention first bucket is recent all others are historic
	historicTag := int32(format.TagValueIDConveyorRecent)
	if len(buckets) > 1 {
		historicTag = format.TagValueIDConveyorHistoric
	}
	stats := insertStats{
		recentTs:    recentTs,
		historicTag: historicTag,
		sizes:       make(map[uint32]insertSize, len(buckets)),
		sampling:    make(map[samplingStatKey]samplingStat),
	}

	var configR ConfigAggregatorRemote
	a.configMu.RLock()
	configR = a.configR
	a.configMu.RUnlock()

	addSizes := func(bucketTs uint32, is insertSize) {
		sizes := stats.sizes[bucketTs]
		sizes.counters += is.counters
		sizes.values += is.values
		sizes.percentiles += is.percentiles
		sizes.uniques += is.uniques
		sizes.stringTops += is.stringTops
		sizes.builtin += is.builtin
		stats.sizes[bucketTs] = sizes
	}

	metricCache := makeMetricCache(a.metricStorage)
	usedTimestamps := map[uint32]struct{}{}
	usedBufferTimestamps := map[uint32]struct{}{}

	insertItem := func(item *data_model.MultiItem, sf float64, bucketTs uint32) { // lambda is convenient here
		is := insertSize{}

		bufferedInsert := false
		if configR.BufferedInsertAgeSec > 0 && item.Key.Timestamp+uint32(configR.BufferedInsertAgeSec) < recentTs {
			bufferedInsert = true
		}
		if bufferedInsert {
			usedBufferTimestamps[item.Key.Timestamp] = struct{}{}
		} else {
			usedTimestamps[item.Key.Timestamp] = struct{}{}
		}

		resPos := len(res)
		if !item.Tail.Empty() { // only tail
			res = appendKeys(res, &item.Key, metricCache, v3Format, data_model.TagUnion{}, bufferedInsert)
			res = multiValueMarshal(rnd, item.Key.Metric, metricCache, res, &item.Tail, data_model.TagUnion{}, sf, v3Format)

			if item.Key.Metric < 0 {
				is.builtin += len(res) - resPos
			} else {
				switch {
				case item.Tail.ValueTDigest != nil:
					is.percentiles += len(res) - resPos
				case item.Tail.HLL.ItemsCount() != 0:
					is.uniques += len(res) - resPos
				case item.Tail.Value.ValueSet:
					is.values += len(res) - resPos
				default:
					is.counters += len(res) - resPos
				}
			}
		}
		resPos = len(res)
		for key, value := range item.Top {
			if value.Empty() { // must be never, but check is cheap
				continue
			}
			// We have no badges for string tops
			res = appendKeys(res, &item.Key, metricCache, v3Format, key, bufferedInsert)              // TODO - insert I
			res = multiValueMarshal(rnd, item.Key.Metric, metricCache, res, value, key, sf, v3Format) // TODO - insert I
		}
		if item.Key.Metric < 0 {
			is.builtin += len(res) - resPos
		} else {
			// TODO - separate into 3 keys - is_string_top/is_builtin and hll/percentile/value/counter
			is.stringTops += len(res) - resPos
		}
		addSizes(bucketTs, is)
	}
	// aggregate per metric sharding data
	// TODO: remove after we fully migrate to a new sharding
	metricStats := make(map[int32]metricStat)
	for _, b := range buckets {
		for i := range b.shards {
			for m := range b.shards[i].metricStats {
				st := metricStats[m]
				st.total += b.shards[i].metricStats[m].total
				st.multipliers += b.shards[i].metricStats[m].multipliers
				metricStats[m] = st
			}
		}
	}
	sampler := data_model.NewSampler(data_model.SamplerConfig{
		Meta:             a.metricStorage,
		SampleNamespaces: configR.SampleNamespaces,
		SampleGroups:     configR.SampleGroups,
		SampleKeys:       configR.SampleKeys,
		Rand:             rnd,
		SampleFactorF: func(metricID int32, sf float64) {
			key := a.aggKey(recentTs, format.BuiltinMetricIDAggSamplingFactor, [format.MaxTags]int32{0, 0, 0, 0, metricID, format.TagValueIDAggSamplingFactorReasonInsertSize})
			res = appendBadge(rnd, res, key, data_model.SimpleItemValue(sf, 1, a.aggregatorHostTag), metricCache, usedTimestamps, v3Format)
			res = appendSimpleValueStat(rnd, res, key, sf, 1, a.aggregatorHost, metricCache, v3Format)
		},
		KeepF:          func(item *data_model.MultiItem, bucketTs uint32) { insertItem(item, item.SF, bucketTs) },
		SamplerBuffers: buffers,
	})
	// First, sample with global sampling factors, depending on cardinality. Collect relative sizes for 2nd stage sampling below.
	// TODO - actual sampleFactors are empty due to code commented out in estimator.go
	for _, b := range buckets {
		is := insertSize{}
		for si := 0; si < len(b.shards); si++ {
			for _, item := range b.shards[si].MultiItems {
				whaleWeight := item.FinishStringTop(rnd, configR.StringTopCountInsert) // all excess items are baked into Tail

				resPos := len(res)
				res = appendMultiBadge(rnd, res, &item.Key, item, metricCache, usedTimestamps, v3Format)
				is.builtin += len(res) - resPos

				accountMetric := item.Key.Metric
				if item.Key.Metric < 0 {
					ingestionStatus := item.Key.Metric == format.BuiltinMetricIDIngestionStatus
					hardwareMetric := format.HardwareMetric(item.Key.Metric)
					if !ingestionStatus && !hardwareMetric {
						// For now sample only ingestion statuses and hardware metrics on aggregator. Might be bad idea. TODO - check.
						insertItem(item, 1, b.time)
						sampler.KeepBuiltin(data_model.SamplingMultiItemPair{
							Item:        item,
							WhaleWeight: whaleWeight,
							Size:        item.RowBinarySizeEstimate(),
							MetricID:    item.Key.Metric,
							BucketTs:    b.time,
						})
						continue
					}
					if ingestionStatus && item.Key.Tags[1] != 0 {
						// Ingestion status and other unlimited per-metric built-ins should use its metric budget
						// So metrics are better isolated
						accountMetric = item.Key.Tags[1]
					}
				}
				sz := item.RowBinarySizeEstimate()
				if ms, ok := metricStats[accountMetric]; ok && ms.total > 0 {
					item.WeightMultiplier = 1 + (a.config.ShardByMetricShards-1)*ms.multipliers/ms.total
				}
				sampler.Add(data_model.SamplingMultiItemPair{
					Item:        item,
					WhaleWeight: whaleWeight,
					Size:        sz,
					MetricID:    accountMetric,
					BucketTs:    b.time,
				})
			}
		}
		addSizes(b.time, is)
	}

	// same contributors from different buckets are intentionally counted separately
	// let's say agent was dead at moment t1 - budget was lower
	// at moment t2 it became alive and send historic bucket for t1 along with recent
	// budget at t2 is bigger because unused budget from t1 was transferred to t2
	numContributors := 0
	for _, b := range buckets {
		numContributors += int(b.contributorsCount())
	}
	insertBudget := configR.InsertBudget
	if shardInsertBuget, ok := configR.ShardInsertBudget[int(a.shardKey)]; ok {
		insertBudget = shardInsertBuget
	}
	remainingBudget := int64(data_model.InsertBudgetFixed) + int64(insertBudget*numContributors)
	// Budget is per contributor, so if they come in 1% groups, total size will approx. fit
	// Also if 2x contributors come to spare, budget is also 2x
	sampler.Run(remainingBudget)
	for _, v := range sampler.MetricGroups {
		sk := samplingStatKey{v.NamespaceID, v.GroupID}
		ss := stats.sampling[sk]
		ss.sampligSizeKeepBytes.Merge(rnd, &v.SumSizeKeep)
		ss.sampligSizeDiscardBytes.Merge(rnd, &v.SumSizeDiscard)
		ss.samplingGroupBudget.AddValue(v.Budget())
		stats.sampling[sk] = ss
	}
	// report sampling engine time
	stats.sampleTimeAppend = sampler.TimeAppend()
	stats.sampleTimePartition = sampler.TimePartition()
	stats.sampleTimeSampling = sampler.TimeSampling()
	stats.sampleTimeBudgeting = sampler.TimeBudgeting()
	stats.sampleTimeMetricMeta = sampler.TimeMetricMeta()
	stats.samplingEngineKeys = float64(sampler.ItemCount())

	// report budget used
	stats.samplingBudget += remainingBudget
	stats.samplingMetricCount = sampler.MetricCount
	stats.contributors += numContributors

	insertTimeUnix := uint32(time.Now().Unix()) // same quality as timestamp from advanceBuckets, can be larger or smaller
	for t := range usedTimestamps {
		key := data_model.Key{Timestamp: insertTimeUnix, Metric: format.BuiltinMetricIDContributorsLog, Tags: [format.MaxTags]int32{0, int32(t)}}
		res = appendSimpleValueStat(rnd, res, &key, float64(insertTimeUnix)-float64(t), 1, a.aggregatorHost, metricCache, v3Format)
		key = data_model.Key{Timestamp: t, Metric: format.BuiltinMetricIDContributorsLogRev, Tags: [format.MaxTags]int32{0, int32(insertTimeUnix)}}
		res = appendSimpleValueStat(rnd, res, &key, float64(insertTimeUnix)-float64(t), 1, a.aggregatorHost, metricCache, v3Format)
	}
	for t := range usedBufferTimestamps {
		key := data_model.Key{Timestamp: insertTimeUnix, Metric: format.BuiltinMetricIDContributorsLog, Tags: [format.MaxTags]int32{0, int32(t)}}
		res = appendBufferedValueStat(rnd, res, &key, float64(insertTimeUnix)-float64(t), 1, a.aggregatorHost, metricCache, v3Format)
		key = data_model.Key{Timestamp: t, Metric: format.BuiltinMetricIDContributorsLogRev, Tags: [format.MaxTags]int32{0, int32(insertTimeUnix)}}
		res = appendBufferedValueStat(rnd, res, &key, float64(insertTimeUnix)-float64(t), 1, a.aggregatorHost, metricCache, v3Format)
	}
	return res, sampler.SamplerBuffers, stats, time.Since(startTime)
}

func makeHTTPClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			DialContext: srvfunc.CachingDialer,
		},
	}
}

func sendToClickhouse(ctx context.Context, httpClient *http.Client, khAddr, khUser, khPassword string, table string, body []byte, settings string) (status int, exception int, elapsed time.Duration, err error) {
	queryPrefix := url.PathEscape(fmt.Sprintf("INSERT INTO %s %s FORMAT RowBinary", table, settings))
	URL := fmt.Sprintf("http://%s/?input_format_values_interpret_expressions=0&query=%s", khAddr, queryPrefix)
	req, err := http.NewRequestWithContext(ctx, "POST", URL, bytes.NewReader(body))
	if err != nil {
		return 0, 0, 0, err
	}
	if khUser != "" {
		req.Header.Set("X-ClickHouse-User", khUser)
	}
	if khPassword != "" {
		req.Header.Set("X-ClickHouse-Key", khPassword)
	}
	if khAddr == "" { // local mode without inserting anything
		return 0, 0, 1, nil
	}
	start := time.Now()
	req.Header.Set("X-Kittenhouse-Aggregation", "0") // aggregation adds delay
	resp, err := httpClient.Do(req)
	dur := time.Since(start)
	dur = dur / time.Millisecond * time.Millisecond
	if err != nil {
		return 0, 0, dur, err
	}
	if resp.StatusCode == http.StatusOK {
		_, _ = io.Copy(io.Discard, resp.Body) // keepalive
		_ = resp.Body.Close()
		return http.StatusOK, 0, dur, nil
	}
	partialBody := body
	if len(partialBody) > 128 {
		partialBody = partialBody[:128]
	}

	var partialMessage [1024]byte
	partialMessageLen, _ := io.ReadFull(resp.Body, partialMessage[:])
	_, _ = io.Copy(io.Discard, resp.Body) // keepalive
	_ = resp.Body.Close()

	clickhouseExceptionText := resp.Header.Get("X-ClickHouse-Exception-Code") // recent clickhouses always send this header in case of error
	ce, _ := strconv.Atoi(clickhouseExceptionText)
	err = fmt.Errorf("could not post to clickhouse (HTTP code %d, X-ClickHouse-Exception-Code: %s): %s, inserting %x", resp.StatusCode, clickhouseExceptionText, partialMessage[:partialMessageLen], partialBody)
	return resp.StatusCode, ce, dur, err
}

func zeroIfTrue(value float64, cond bool) float64 {
	if cond {
		return 0
	}
	return value
}

func appendArgMinMaxTag(res []byte, tag data_model.TagUnionBytes, value float32) []byte {
	if tag.Empty() {
		res = rowbinary.AppendArgMinMaxStringEmpty(res)
		return res
	}
	wasLen := len(res)
	// this is important, do not remove
	// without it AppendArgMinMaxBytesFloat32 will corrupt data because len and res are parts of the same slice
	res = append(res, 0, 0, 0, 0)
	if tag.I != 0 {
		res = append(res, 0)
		res = binary.LittleEndian.AppendUint32(res, uint32(tag.I))
		res = rowbinary.AppendArgMinMaxBytesFloat32(res[:wasLen], res[wasLen+4:], value)
		return res
	}
	res = append(res, 1)
	res = append(res, tag.S...)
	res = rowbinary.AppendArgMinMaxBytesFloat32(res[:wasLen], res[wasLen+4:], value)
	return res
}
