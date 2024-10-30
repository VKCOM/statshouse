// Copyright 2022 V Kontakte LLC
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

func getTableDesc() string {
	keysFieldsNamesVec := make([]string, format.MaxTags)
	for i := 0; i < format.MaxTags; i++ {
		keysFieldsNamesVec[i] = fmt.Sprintf(`key%d`, i)
	}
	return `statshouse_value_incoming_prekey3(metric,prekey,prekey_set,time,` + strings.Join(keysFieldsNamesVec, `,`) + `,count,min,max,sum,sumsquare,percentiles,uniq_state,skey,min_host,max_host)`
}

func getNewTableDesc() string {
	keysFieldsNamesVec := make([]string, format.NewMaxTags)
	for i := 0; i < format.NewMaxTags; i++ {
		keysFieldsNamesVec[i] = fmt.Sprintf(`tag%d,stag%d`, i, i)
	}
	return `statshouse_v3_incoming(metric,time,` + strings.Join(keysFieldsNamesVec, `,`) + `,count,min,max,sum,sumsquare,percentiles,uniq_state,min_host_legacy,max_host_legacy,min_host,max_host)`
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
	if metaMetric := p.journal.GetMetaMetricDelayed(metricID); metaMetric != nil {
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

func appendKeys(res []byte, k data_model.Key, metricCache *metricIndexCache, usedTimestamps map[uint32]struct{}, newFormat bool, stringTagProb float64, rnd *rand.Rand, stag string) []byte {
	if newFormat {
		return appendKeysNewFormat(res, k, metricCache, usedTimestamps, stringTagProb, rnd, stag)
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
	if usedTimestamps != nil { // do not update map when writing map itself
		usedTimestamps[k.Timestamp] = struct{}{} // TODO - optimize out bucket timestamp
	}
	tagsN := format.MaxTags
	for ki := 0; ki < tagsN; ki++ {
		res = appendTag(res, uint32(k.Tags[ki]))
	}
	return res
}

func appendKeysNewFormat(res []byte, k data_model.Key, metricCache *metricIndexCache, usedTimestamps map[uint32]struct{}, stringTagProb float64, rnd *rand.Rand, stag string) []byte {
	appendTag := func(res []byte, v uint32) []byte {
		if v > 0 && stringTagProb > 0 && stringTagProb > rnd.Float64() {
			res = binary.LittleEndian.AppendUint32(res, 0)
			res = rowbinary.AppendString(res, fmt.Sprintf("long_and_fake_tagvalue_%d", v))
		} else {
			res = binary.LittleEndian.AppendUint32(res, v)
			res = rowbinary.AppendString(res, "")
		}
		return res
	}
	res = binary.LittleEndian.AppendUint32(res, uint32(k.Metric))
	// TODO write pretags
	_ = metricCache
	res = binary.LittleEndian.AppendUint32(res, k.Timestamp)
	if usedTimestamps != nil { // do not update map when writing map itself
		usedTimestamps[k.Timestamp] = struct{}{} // TODO - optimize out bucket timestamp
	}
	tagsN := format.NewMaxTags
	for ki := 0; ki < tagsN; ki++ {
		// backward compatibility with an old format
		if ki == format.StringTopTagIndexV3 {
			res = binary.LittleEndian.AppendUint32(res, 0)
			res = rowbinary.AppendString(res, stag)
			continue
		}
		if ki < len(k.Tags) {
			res = appendTag(res, uint32(k.Tags[ki]))
		} else {
			res = appendTag(res, 0)
		}
	}
	return res
}

// TODO - badges are badly designed for now. Should be redesigned some day.
// We propose to move them inside metric with env=-1,-2,etc.
// So we can select badges for free by adding || (env < 0) to requests, then filtering result rows
// Also we must select both count and sum, then process them separately for each badge kind

func appendMultiBadge(rng *rand.Rand, res []byte, k data_model.Key, v *data_model.MultiItem, metricCache *metricIndexCache, usedTimestamps map[uint32]struct{}, newFormat bool) []byte {
	if k.Metric >= 0 { // fastpath
		return res
	}
	for _, t := range v.Top {
		res = appendBadge(rng, res, k, t.Value, metricCache, usedTimestamps, newFormat)
	}
	return appendBadge(rng, res, k, v.Tail.Value, metricCache, usedTimestamps, newFormat)
}

func appendBadge(rng *rand.Rand, res []byte, k data_model.Key, v data_model.ItemValue, metricCache *metricIndexCache, usedTimestamps map[uint32]struct{}, newFormat bool) []byte {
	if k.Metric >= 0 { // fastpath
		return res
	}
	ts := (k.Timestamp / 5) * 5
	// We used to select with single function (avg), so we approximated sum of counters so that any number of events produce avg >= 1
	// TODO - deprecated legacy badges and use new badges
	switch k.Metric {
	case format.BuiltinMetricIDIngestionStatus:
		if k.Tags[1] == 0 {
			return res
		}
		switch k.Tags[2] {
		case format.TagValueIDSrcIngestionStatusOKCached,
			format.TagValueIDSrcIngestionStatusOKUncached:
			return res
		case format.TagValueIDSrcIngestionStatusWarnDeprecatedKeyName,
			format.TagValueIDSrcIngestionStatusWarnDeprecatedT,
			format.TagValueIDSrcIngestionStatusWarnDeprecatedStop,
			format.TagValueIDSrcIngestionStatusWarnMapTagSetTwice,
			format.TagValueIDSrcIngestionStatusWarnOldCounterSemantic,
			format.TagValueIDSrcIngestionStatusWarnMapInvalidRawTagValue:
			return appendValueStat(rng, res, data_model.Key{Timestamp: ts, Metric: format.BuiltinMetricIDBadges, Tags: [16]int32{0, format.TagValueIDBadgeIngestionWarnings, k.Tags[1]}}, "", v, metricCache, usedTimestamps, newFormat)
		}
		return appendValueStat(rng, res, data_model.Key{Timestamp: ts, Metric: format.BuiltinMetricIDBadges, Tags: [16]int32{0, format.TagValueIDBadgeIngestionErrors, k.Tags[1]}}, "", v, metricCache, usedTimestamps, newFormat)
	case format.BuiltinMetricIDAgentSamplingFactor:
		return appendValueStat(rng, res, data_model.Key{Timestamp: ts, Metric: format.BuiltinMetricIDBadges, Tags: [16]int32{0, format.TagValueIDBadgeAgentSamplingFactor, k.Tags[1]}}, "", v, metricCache, usedTimestamps, newFormat)
	case format.BuiltinMetricIDAggSamplingFactor:
		return appendValueStat(rng, res, data_model.Key{Timestamp: ts, Metric: format.BuiltinMetricIDBadges, Tags: [16]int32{0, format.TagValueIDBadgeAggSamplingFactor, k.Tags[4]}}, "", v, metricCache, usedTimestamps, newFormat)
	case format.BuiltinMetricIDAggMappingCreated:
		if k.Tags[5] == format.TagValueIDAggMappingCreatedStatusOK ||
			k.Tags[5] == format.TagValueIDAggMappingCreatedStatusCreated {
			return res
		}
		return appendValueStat(rng, res, data_model.Key{Timestamp: ts, Metric: format.BuiltinMetricIDBadges, Tags: [16]int32{0, format.TagValueIDBadgeAggMappingErrors, k.Tags[4]}}, "", v, metricCache, usedTimestamps, newFormat)
	case format.BuiltinMetricIDAggBucketReceiveDelaySec:
		return appendValueStat(rng, res, data_model.Key{Timestamp: ts, Metric: format.BuiltinMetricIDBadges, Tags: [16]int32{0, format.TagValueIDBadgeContributors, 0}}, "", v, metricCache, usedTimestamps, newFormat)
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

func appendValueStat(rng *rand.Rand, res []byte, key data_model.Key, skey string, v data_model.ItemValue, cache *metricIndexCache, usedTimestamps map[uint32]struct{}, newFormat bool) []byte {
	count := v.Count()
	if count <= 0 { // We have lots of built-in  counters which are normally 0
		return res
	}
	// for explanation of insert logic, see multiValueMarshal below
	res = appendKeys(res, key, cache, usedTimestamps, newFormat, 0, nil /* don't replace mappings with strings from builtin metrics */, skey)
	skipMaxHost, skipMinHost, skipSumSquare := cache.skips(key.Metric)
	if v.ValueSet {
		res = appendAggregates(res, count, v.ValueMin, v.ValueMax, v.ValueSum, zeroIfTrue(v.ValueSumSquare, skipSumSquare))
	} else {
		res = appendAggregates(res, count, 0, count, 0, 0)
	}

	res = rowbinary.AppendEmptyCentroids(res)
	res = rowbinary.AppendEmptyUnique(res)
	if !newFormat {
		res = rowbinary.AppendString(res, skey)
	}

	// min_host_legacy, max_host_legacy
	if v.ValueSet {
		if skipMinHost {
			res = rowbinary.AppendArgMinMaxInt32Float32Empty(res)
		} else {
			res = rowbinary.AppendArgMinMaxInt32Float32(res, v.MinHostTagId, float32(v.ValueMin))
		}
		if skipMaxHost {
			res = rowbinary.AppendArgMinMaxInt32Float32Empty(res)
		} else {
			res = rowbinary.AppendArgMinMaxInt32Float32(res, v.MaxHostTagId, float32(v.ValueMax))
		}
	} else {
		res = rowbinary.AppendArgMinMaxInt32Float32Empty(res)
		if skipMaxHost {
			res = rowbinary.AppendArgMinMaxInt32Float32Empty(res)
		} else {
			cc := float32(data_model.SkewMaxCounterHost(rng, count)) // explanation is in Skew function
			res = rowbinary.AppendArgMinMaxInt32Float32(res, v.MaxCounterHostTagId, cc)
		}
	}
	// min_host, max_host
	if newFormat {
		var encodedTag [5]byte
		if v.ValueSet {
			if skipMinHost {
				res = rowbinary.AppendArgMinMaxStringEmpty(res)
			} else {
				binary.LittleEndian.PutUint32(encodedTag[1:], uint32(v.MinHostTagId))
				res = rowbinary.AppendArgMinMaxStringFloat32(res, string(encodedTag[:]), float32(v.ValueMin))
			}
			if skipMaxHost {
				res = rowbinary.AppendArgMinMaxStringEmpty(res)
			} else {
				binary.LittleEndian.PutUint32(encodedTag[1:], uint32(v.MaxHostTagId))
				res = rowbinary.AppendArgMinMaxStringFloat32(res, string(encodedTag[:]), float32(v.ValueMax))
			}
		} else {
			res = rowbinary.AppendArgMinMaxStringEmpty(res)
			if skipMaxHost {
				res = rowbinary.AppendArgMinMaxStringEmpty(res)
			} else {
				binary.LittleEndian.PutUint32(encodedTag[1:], uint32(v.MaxCounterHostTagId))
				cc := float32(data_model.SkewMaxCounterHost(rng, count)) // explanation is in Skew function
				res = rowbinary.AppendArgMinMaxStringFloat32(res, string(encodedTag[:]), cc)
			}
		}
	}

	return res
}

func appendSimpleValueStat(rng *rand.Rand, res []byte, key data_model.Key, v float64, count float64, hostTag int32, metricCache *metricIndexCache, usedTimestamps map[uint32]struct{}, newFormat bool) []byte {
	return appendValueStat(rng, res, key, "", data_model.SimpleItemValue(v, count, hostTag), metricCache, usedTimestamps, newFormat)
}

func multiValueMarshal(rng *rand.Rand, metricID int32, cache *metricIndexCache, res []byte, value *data_model.MultiValue, skey string, sf float64, newFormat bool) []byte {
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
	if !newFormat {
		res = rowbinary.AppendString(res, skey)
	}
	// min_host_legacy, max_host_legacy
	if value.Value.ValueSet {
		if skipMinHost {
			res = rowbinary.AppendArgMinMaxInt32Float32Empty(res)
		} else {
			res = rowbinary.AppendArgMinMaxInt32Float32(res, value.Value.MinHostTagId, float32(value.Value.ValueMin))
		}
		if skipMaxHost {
			res = rowbinary.AppendArgMinMaxInt32Float32Empty(res)
		} else {
			res = rowbinary.AppendArgMinMaxInt32Float32(res, value.Value.MaxHostTagId, float32(value.Value.ValueMax))
		}
	} else {
		res = rowbinary.AppendArgMinMaxInt32Float32Empty(res) // counters do not have min_host set
		if skipMaxHost {
			res = rowbinary.AppendArgMinMaxInt32Float32Empty(res)
		} else {
			cc := float32(data_model.SkewMaxCounterHost(rng, counter)) // explanation is in Skew function
			res = rowbinary.AppendArgMinMaxInt32Float32(res, value.Value.MaxCounterHostTagId, cc)
		}
	}
	// min_host, max_host
	if newFormat {
		var encodedTag [5]byte
		v := value.Value
		if v.ValueSet {
			if skipMinHost {
				res = rowbinary.AppendArgMinMaxStringEmpty(res)
			} else {
				binary.LittleEndian.PutUint32(encodedTag[1:], uint32(v.MinHostTagId))
				res = rowbinary.AppendArgMinMaxStringFloat32(res, string(encodedTag[:]), float32(v.ValueMin))
			}
			if skipMaxHost {
				res = rowbinary.AppendArgMinMaxStringEmpty(res)
			} else {
				binary.LittleEndian.PutUint32(encodedTag[1:], uint32(v.MaxHostTagId))
				res = rowbinary.AppendArgMinMaxStringFloat32(res, string(encodedTag[:]), float32(v.ValueMax))
			}
		} else {
			res = rowbinary.AppendArgMinMaxStringEmpty(res)
			if skipMaxHost {
				res = rowbinary.AppendArgMinMaxStringEmpty(res)
			} else {
				binary.LittleEndian.PutUint32(encodedTag[1:], uint32(v.MaxCounterHostTagId))
				cc := float32(data_model.SkewMaxCounterHost(rng, counter)) // explanation is in Skew function
				res = rowbinary.AppendArgMinMaxStringFloat32(res, string(encodedTag[:]), cc)
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

func (a *Aggregator) RowDataMarshalAppendPositions(buckets []*aggregatorBucket, rnd *rand.Rand, res []byte, newFormat bool, stringTagProb float64) []byte {
	startTime := time.Now()
	// sanity check, nothing to marshal if there is no buckets
	if len(buckets) < 1 {
		return res
	}

	var config ConfigAggregatorRemote
	a.configMu.RLock()
	config = a.configR
	a.configMu.RUnlock()

	insertSizes := make(map[uint32]insertSize, len(buckets))
	addSizes := func(bucketTs uint32, is insertSize) {
		sizes := insertSizes[bucketTs]
		sizes.counters += is.counters
		sizes.values += is.values
		sizes.percentiles += is.percentiles
		sizes.uniques += is.uniques
		sizes.stringTops += is.stringTops
		sizes.builtin += is.builtin
		insertSizes[bucketTs] = sizes
	}

	metricCache := makeMetricCache(a.metricStorage)
	usedTimestamps := map[uint32]struct{}{}

	insertItem := func(k data_model.Key, item *data_model.MultiItem, sf float64, bucketTs uint32) { // lambda is convenient here
		is := insertSize{}

		resPos := len(res)
		if !item.Tail.Empty() { // only tail
			res = appendKeys(res, k, metricCache, usedTimestamps, newFormat, stringTagProb, rnd, "")

			res = multiValueMarshal(rnd, k.Metric, metricCache, res, &item.Tail, "", sf, newFormat)

			if k.Metric < 0 {
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
		for skey, value := range item.Top {
			if value.Empty() { // must be never, but check is cheap
				continue
			}
			// We have no badges for string tops
			res = appendKeys(res, k, metricCache, usedTimestamps, newFormat, stringTagProb, rnd, skey)
			res = multiValueMarshal(rnd, k.Metric, metricCache, res, value, skey, sf, newFormat)
		}
		if k.Metric < 0 {
			is.builtin += len(res) - resPos
		} else {
			// TODO - separate into 3 keys - is_string_top/is_builtin and hll/percentile/value/counter
			is.stringTops += len(res) - resPos
		}
		addSizes(bucketTs, is)
	}
	var itemsCount int
	for _, b := range buckets {
		for si := 0; si < len(b.shards); si++ {
			itemsCount += len(b.shards[si].multiItems)
		}
	}
	recentTime := buckets[0].time // by convention first bucket is recent all other are historic
	sampler := data_model.NewSampler(itemsCount, data_model.SamplerConfig{
		Meta:             a.metricStorage,
		SampleNamespaces: config.SampleNamespaces,
		SampleGroups:     config.SampleGroups,
		SampleKeys:       config.SampleKeys,
		Rand:             rnd,
		SampleFactorF: func(metricID int32, sf float64) {
			key := a.aggKey(recentTime, format.BuiltinMetricIDAggSamplingFactor, [16]int32{0, 0, 0, 0, metricID, format.TagValueIDAggSamplingFactorReasonInsertSize})
			res = appendBadge(rnd, res, key, data_model.SimpleItemValue(sf, 1, a.aggregatorHost), metricCache, usedTimestamps, newFormat)
			res = appendSimpleValueStat(rnd, res, key, sf, 1, a.aggregatorHost, metricCache, usedTimestamps, newFormat)
		},
		KeepF: func(k data_model.Key, item *data_model.MultiItem, bt uint32) { insertItem(k, item, item.SF, bt) },
	})
	// First, sample with global sampling factors, depending on cardinality. Collect relative sizes for 2nd stage sampling below.
	// TODO - actual sampleFactors are empty due to code commented out in estimator.go
	for _, b := range buckets {
		is := insertSize{}
		for si := 0; si < len(b.shards); si++ {
			for k, item := range b.shards[si].multiItems {
				whaleWeight := item.FinishStringTop(rnd, config.StringTopCountInsert) // all excess items are baked into Tail

				resPos := len(res)
				res = appendMultiBadge(rnd, res, k, item, metricCache, usedTimestamps, newFormat)
				is.builtin += len(res) - resPos

				accountMetric := k.Metric
				if k.Metric < 0 {
					ingestionStatus := k.Metric == format.BuiltinMetricIDIngestionStatus
					hardwareMetric := format.HardwareMetric(k.Metric)
					if !ingestionStatus && !hardwareMetric {
						// For now sample only ingestion statuses and hardware metrics on aggregator. Might be bad idea. TODO - check.
						insertItem(k, item, 1, b.time)
						sampler.KeepBuiltin(data_model.SamplingMultiItemPair{
							Key:         k,
							Item:        item,
							WhaleWeight: whaleWeight,
							Size:        item.RowBinarySizeEstimate(),
							MetricID:    k.Metric,
							BucketTs:    b.time,
						})
						continue
					}
					if ingestionStatus && k.Tags[1] != 0 {
						// Ingestion status and other unlimited per-metric built-ins should use its metric budget
						// So metrics are better isolated
						accountMetric = k.Tags[1]
					}
				}
				sz := item.RowBinarySizeEstimate()
				sampler.Add(data_model.SamplingMultiItemPair{
					Key:         k,
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
	resPos := len(res)
	remainingBudget := int64(data_model.InsertBudgetFixed) + int64(config.InsertBudget*numContributors)
	// Budget is per contributor, so if they come in 1% groups, total size will approx. fit
	// Also if 2x contributors come to spare, budget is also 2x
	sampler.Run(remainingBudget)
	var historicTag int32 = format.TagValueIDConveyorRecent
	if len(buckets) > 1 {
		historicTag = format.TagValueIDConveyorHistoric
	}
	for _, v := range sampler.MetricGroups {
		// keep bytes
		key := a.aggKey(recentTime, format.BuiltinMetricIDAggSamplingSizeBytes, [16]int32{0, historicTag, format.TagValueIDSamplingDecisionKeep, v.NamespaceID, v.GroupID, v.MetricID})
		mi := data_model.MultiItem{Tail: data_model.MultiValue{Value: v.SumSizeKeep}}
		insertItem(key, &mi, 1, buckets[0].time)
		// discard bytes
		key = a.aggKey(recentTime, format.BuiltinMetricIDAggSamplingSizeBytes, [16]int32{0, historicTag, format.TagValueIDSamplingDecisionDiscard, v.NamespaceID, v.GroupID, v.MetricID})
		mi = data_model.MultiItem{Tail: data_model.MultiValue{Value: v.SumSizeDiscard}}
		insertItem(key, &mi, 1, buckets[0].time)
		// budget
		key = a.aggKey(recentTime, format.BuiltinMetricIDAggSamplingGroupBudget, [16]int32{0, historicTag, v.NamespaceID, v.GroupID})
		item := data_model.MultiItem{}
		item.Tail.Value.AddValue(v.Budget())
		insertItem(key, &item, 1, buckets[0].time)
	}
	// report sampling engine time
	res = appendSimpleValueStat(rnd, res, a.aggKey(recentTime, format.BuiltinMetricIDAggSamplingEngineTime, [16]int32{0, 1, 0, 0, historicTag}),
		float64(sampler.TimeAppend()), 1, a.aggregatorHost, metricCache, usedTimestamps, newFormat)
	res = appendSimpleValueStat(rnd, res, a.aggKey(recentTime, format.BuiltinMetricIDAggSamplingEngineTime, [16]int32{0, 2, 0, 0, historicTag}),
		float64(sampler.TimePartition()), 1, a.aggregatorHost, metricCache, usedTimestamps, newFormat)
	res = appendSimpleValueStat(rnd, res, a.aggKey(recentTime, format.BuiltinMetricIDAggSamplingEngineTime, [16]int32{0, 3, 0, 0, historicTag}),
		float64(sampler.TimeBudgeting()), 1, a.aggregatorHost, metricCache, usedTimestamps, newFormat)
	res = appendSimpleValueStat(rnd, res, a.aggKey(recentTime, format.BuiltinMetricIDAggSamplingEngineTime, [16]int32{0, 4, 0, 0, historicTag}),
		float64(sampler.TimeSampling()), 1, a.aggregatorHost, metricCache, usedTimestamps, newFormat)
	res = appendSimpleValueStat(rnd, res, a.aggKey(recentTime, format.BuiltinMetricIDAggSamplingEngineTime, [16]int32{0, 5, 0, 0, historicTag}),
		float64(sampler.TimeMetricMeta()), 1, a.aggregatorHost, metricCache, usedTimestamps, newFormat)
	res = appendValueStat(rnd, res, a.aggKey(recentTime, format.BuiltinMetricIDAggSamplingEngineKeys, [16]int32{0, 0, 0, 0, historicTag}),
		"", data_model.SimpleItemCounter(float64(sampler.ItemCount()), a.aggregatorHost), metricCache, usedTimestamps, newFormat)

	// report budget used
	budgetKey := a.aggKey(recentTime, format.BuiltinMetricIDAggSamplingBudget, [16]int32{0, historicTag})
	budgetItem := data_model.MultiItem{}
	budgetItem.Tail.Value.AddValue(float64(remainingBudget))
	insertItem(budgetKey, &budgetItem, 1, buckets[0].time)
	res = appendSimpleValueStat(rnd, res, a.aggKey(recentTime, format.BuiltinMetricIDAggSamplingMetricCount, [16]int32{0, historicTag}),
		float64(sampler.MetricCount), 1, a.aggregatorHost, metricCache, usedTimestamps, newFormat)

	appendInsertSizeStats := func(time uint32, is insertSize, historicTag int32) int {
		res = appendSimpleValueStat(rnd, res, a.aggKey(time, format.BuiltinMetricIDAggInsertSize, [16]int32{0, 0, 0, 0, historicTag, format.TagValueIDSizeCounter}),
			float64(is.counters), 1, a.aggregatorHost, metricCache, usedTimestamps, newFormat)
		res = appendSimpleValueStat(rnd, res, a.aggKey(time, format.BuiltinMetricIDAggInsertSize, [16]int32{0, 0, 0, 0, historicTag, format.TagValueIDSizeValue}),
			float64(is.values), 1, a.aggregatorHost, metricCache, usedTimestamps, newFormat)
		res = appendSimpleValueStat(rnd, res, a.aggKey(time, format.BuiltinMetricIDAggInsertSize, [16]int32{0, 0, 0, 0, historicTag, format.TagValueIDSizePercentiles}),
			float64(is.percentiles), 1, a.aggregatorHost, metricCache, usedTimestamps, newFormat)
		res = appendSimpleValueStat(rnd, res, a.aggKey(time, format.BuiltinMetricIDAggInsertSize, [16]int32{0, 0, 0, 0, historicTag, format.TagValueIDSizeUnique}),
			float64(is.uniques), 1, a.aggregatorHost, metricCache, usedTimestamps, newFormat)
		sizeBefore := len(res)
		res = appendSimpleValueStat(rnd, res, a.aggKey(time, format.BuiltinMetricIDAggInsertSize, [16]int32{0, 0, 0, 0, historicTag, format.TagValueIDSizeStringTop}),
			float64(is.stringTops), 1, a.aggregatorHost, metricCache, usedTimestamps, newFormat)
		return len(res) - sizeBefore
	}
	// we assume that builtin size metric takes as much bytes as string top size
	estimatedSize := appendInsertSizeStats(recentTime, insertSizes[buckets[0].time], format.TagValueIDConveyorRecent)

	res = appendSimpleValueStat(rnd, res, a.aggKey(recentTime, format.BuiltinMetricIDAggContributors, [16]int32{}),
		float64(numContributors), 1, a.aggregatorHost, metricCache, usedTimestamps, newFormat)

	insertTimeUnix := uint32(time.Now().Unix()) // same quality as timestamp from advanceBuckets, can be larger or smaller
	for t := range usedTimestamps {
		key := data_model.Key{Timestamp: insertTimeUnix, Metric: format.BuiltinMetricIDContributorsLog, Tags: [16]int32{0, int32(t)}}
		res = appendSimpleValueStat(rnd, res, key, float64(insertTimeUnix)-float64(t), 1, a.aggregatorHost, metricCache, nil, newFormat)
		key = data_model.Key{Timestamp: t, Metric: format.BuiltinMetricIDContributorsLogRev, Tags: [16]int32{0, int32(insertTimeUnix)}}
		res = appendSimpleValueStat(rnd, res, key, float64(insertTimeUnix)-float64(t), 1, a.aggregatorHost, metricCache, nil, newFormat)
	}
	dur := time.Since(startTime)
	res = appendSimpleValueStat(rnd, res, a.aggKey(recentTime, format.BuiltinMetricIDAggSamplingTime, [16]int32{0, 0, 0, 0, historicTag}),
		float64(dur.Seconds()), 1, a.aggregatorHost, metricCache, usedTimestamps, newFormat)

	var recentBuiltinSize int = insertSizes[buckets[0].time].builtin + len(res) - resPos + estimatedSize
	res = appendSimpleValueStat(rnd, res, a.aggKey(recentTime, format.BuiltinMetricIDAggInsertSize, [16]int32{0, 0, 0, 0, format.TagValueIDConveyorRecent, format.TagValueIDSizeBuiltIn}),
		float64(recentBuiltinSize), 1, a.aggregatorHost, metricCache, usedTimestamps, newFormat)

	for _, b := range buckets[1:] {
		resPos = len(res)
		appendInsertSizeStats(b.time, insertSizes[b.time], format.TagValueIDConveyorHistoric)
		var historicBuiltinSize int = insertSizes[b.time].builtin + len(res) - resPos + estimatedSize
		res = appendSimpleValueStat(rnd, res, a.aggKey(b.time, format.BuiltinMetricIDAggInsertSize, [16]int32{0, 0, 0, 0, format.TagValueIDConveyorHistoric, format.TagValueIDSizeBuiltIn}),
			float64(historicBuiltinSize), 1, a.aggregatorHost, metricCache, usedTimestamps, newFormat)
	}

	return res
}

func makeHTTPClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			DialContext: srvfunc.CachingDialer,
		},
	}
}

func sendToClickhouse(ctx context.Context, httpClient *http.Client, khAddr string, table string, body []byte) (status int, exception int, elapsed float64, err error) {
	queryPrefix := url.PathEscape(fmt.Sprintf("INSERT INTO %s FORMAT RowBinary", table))
	URL := fmt.Sprintf("http://%s/?input_format_values_interpret_expressions=0&query=%s", khAddr, queryPrefix)
	req, err := http.NewRequestWithContext(ctx, "POST", URL, bytes.NewReader(body))
	if err != nil {
		return 0, 0, 0, err
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
		return 0, 0, dur.Seconds(), err
	}
	if resp.StatusCode == http.StatusOK {
		_, _ = io.Copy(io.Discard, resp.Body) // keepalive
		_ = resp.Body.Close()
		return http.StatusOK, 0, dur.Seconds(), nil
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
	return resp.StatusCode, ce, dur.Seconds(), err
}

func zeroIfTrue(value float64, cond bool) float64 {
	if cond {
		return 0
	}
	return value
}
