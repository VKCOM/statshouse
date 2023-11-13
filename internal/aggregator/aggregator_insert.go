// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package aggregator

import (
	"bytes"
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
	if bm, ok := format.BuiltinMetrics[format.BuiltinMetricIDIngestionStatus]; ok {
		result.ingestionStatusData.lastMetricPrekeyOnly = bm.PreKeyOnly
		result.ingestionStatusData.lastMetricPrekey = bm.PreKeyIndex
		result.ingestionStatusData.lastMetricSkipMinHost = bm.SkipMinHost
		result.ingestionStatusData.lastMetricSkipMaxHost = bm.SkipMaxHost
		result.ingestionStatusData.lastMetricSkipSumSquare = bm.SkipSumSquare
	}
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

func appendKeys(res []byte, k data_model.Key, metricCache *metricIndexCache, usedTimestamps map[uint32]struct{}) []byte {
	var tmp [4 + 4 + 1 + 4 + format.MaxTags*4]byte // metric, prekey, prekey_set, time
	binary.LittleEndian.PutUint32(tmp[0:], uint32(k.Metric))
	prekeyIndex, prekeyOnly := metricCache.getPrekeyIndex(k.Metric)
	if prekeyIndex >= 0 {
		binary.LittleEndian.PutUint32(tmp[4:], uint32(k.Keys[prekeyIndex]))
		if prekeyOnly {
			tmp[8] = 2
		} else {
			tmp[8] = 1
		}
	}
	binary.LittleEndian.PutUint32(tmp[9:], k.Timestamp)
	if usedTimestamps != nil { // do not update map when writing map itself
		usedTimestamps[k.Timestamp] = struct{}{} // TODO - optimize out bucket timestamp
	}
	for ki, key := range k.Keys {
		binary.LittleEndian.PutUint32(tmp[13+ki*4:], uint32(key))
	}
	return append(res, tmp[:]...)
}

// TODO - badges are badly designed for now. Should be redesigned some day.
// We propose to move them inside metric with env=-1,-2,etc.
// So we can select badges for free by adding || (env < 0) to requests, then filtering result rows
// Also we must select both count and sum, then process them separately for each badge kind

func appendMultiBadge(res []byte, k data_model.Key, v *data_model.MultiItem, metricCache *metricIndexCache, usedTimestamps map[uint32]struct{}) []byte {
	if k.Metric >= 0 { // fastpath
		return res
	}
	for _, t := range v.Top {
		res = appendBadge(res, k, t.Value, metricCache, usedTimestamps)
	}
	return appendBadge(res, k, v.Tail.Value, metricCache, usedTimestamps)
}

func appendBadge(res []byte, k data_model.Key, v data_model.ItemValue, metricCache *metricIndexCache, usedTimestamps map[uint32]struct{}) []byte {
	if k.Metric >= 0 { // fastpath
		return res
	}
	ts := (k.Timestamp / 5) * 5
	// We used to select with single function (avg), so we approximated sum of counters so that any number of events produce avg >= 1
	// TODO - deprecated legacy badges and use new badges
	switch k.Metric {
	case format.BuiltinMetricIDIngestionStatus:
		if k.Keys[1] == 0 {
			return res
		}
		switch k.Keys[2] {
		case format.TagValueIDSrcIngestionStatusOKCached,
			format.TagValueIDSrcIngestionStatusOKUncached:
			return res
		case format.TagValueIDSrcIngestionStatusWarnDeprecatedKeyName,
			format.TagValueIDSrcIngestionStatusWarnDeprecatedT,
			format.TagValueIDSrcIngestionStatusWarnDeprecatedStop,
			format.TagValueIDSrcIngestionStatusWarnMapTagSetTwice,
			format.TagValueIDSrcIngestionStatusWarnOldCounterSemantic,
			format.TagValueIDSrcIngestionStatusWarnMapInvalidRawTagValue:
			return appendValueStat(res, data_model.Key{Timestamp: ts, Metric: format.BuiltinMetricIDBadges, Keys: [16]int32{0, format.TagValueIDBadgeIngestionWarnings, k.Keys[1]}}, "", v, metricCache, usedTimestamps)
		}
		return appendValueStat(res, data_model.Key{Timestamp: ts, Metric: format.BuiltinMetricIDBadges, Keys: [16]int32{0, format.TagValueIDBadgeIngestionErrors, k.Keys[1]}}, "", v, metricCache, usedTimestamps)
	case format.BuiltinMetricIDAgentSamplingFactor:
		return appendValueStat(res, data_model.Key{Timestamp: ts, Metric: format.BuiltinMetricIDBadges, Keys: [16]int32{0, format.TagValueIDBadgeAgentSamplingFactor, k.Keys[1]}}, "", v, metricCache, usedTimestamps)
	case format.BuiltinMetricIDAggSamplingFactor:
		return appendValueStat(res, data_model.Key{Timestamp: ts, Metric: format.BuiltinMetricIDBadges, Keys: [16]int32{0, format.TagValueIDBadgeAggSamplingFactor, k.Keys[4]}}, "", v, metricCache, usedTimestamps)
	case format.BuiltinMetricIDAggMappingCreated:
		if k.Keys[5] == format.TagValueIDAggMappingCreatedStatusOK ||
			k.Keys[5] == format.TagValueIDAggMappingCreatedStatusCreated {
			return res
		}
		return appendValueStat(res, data_model.Key{Timestamp: ts, Metric: format.BuiltinMetricIDBadges, Keys: [16]int32{0, format.TagValueIDBadgeAggMappingErrors, k.Keys[4]}}, "", v, metricCache, usedTimestamps)
	case format.BuiltinMetricIDAggBucketReceiveDelaySec:
		return appendValueStat(res, data_model.Key{Timestamp: ts, Metric: format.BuiltinMetricIDBadges, Keys: [16]int32{0, format.TagValueIDBadgeContributors, 0}}, "", v, metricCache, usedTimestamps)
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

func appendValueStat(res []byte, key data_model.Key, skey string, v data_model.ItemValue, cache *metricIndexCache, usedTimestamps map[uint32]struct{}) []byte {
	if v.Counter <= 0 { // We have lots of built-in  counters which are normally 0
		return res
	}
	// for explanation of insert logic, see multiValueMarshal below
	res = appendKeys(res, key, cache, usedTimestamps)
	skipMaxHost, skipMinHost, skipSumSquare := cache.skips(key.Metric)
	if v.ValueSet {
		res = appendAggregates(res, v.Counter, v.ValueMin, v.ValueMax, v.ValueSum, zeroIfTrue(v.ValueSumSquare, skipSumSquare))
	} else {
		res = appendAggregates(res, v.Counter, 0, v.Counter, 0, 0)
	}

	res = rowbinary.AppendEmptyCentroids(res)
	res = rowbinary.AppendEmptyUnique(res)
	res = rowbinary.AppendString(res, skey)

	if v.ValueSet {
		if skipMinHost {
			res = rowbinary.AppendArgMinMaxInt32Float32Empty(res)
		} else {
			res = rowbinary.AppendArgMinMaxInt32Float32(res, v.MinHostTag, float32(v.ValueMin))
		}
		if skipMaxHost {
			res = rowbinary.AppendArgMinMaxInt32Float32Empty(res)
		} else {
			res = rowbinary.AppendArgMinMaxInt32Float32(res, v.MaxHostTag, float32(v.ValueMax))
		}
	} else {
		res = rowbinary.AppendArgMinMaxInt32Float32Empty(res)
		if skipMaxHost {
			res = rowbinary.AppendArgMinMaxInt32Float32Empty(res)
		} else {
			res = rowbinary.AppendArgMinMaxInt32Float32(res, v.MaxCounterHostTag, float32(v.Counter))
		}
	}
	return res
}

func appendSimpleValueStat(res []byte, key data_model.Key, v float64, count float64, hostTag int32, metricCache *metricIndexCache, usedTimestamps map[uint32]struct{}) []byte {
	return appendValueStat(res, key, "", data_model.SimpleItemValue(v, count, hostTag), metricCache, usedTimestamps)
}

func multiValueMarshal(metricID int32, cache *metricIndexCache, res []byte, value *data_model.MultiValue, skey string, sf float64) []byte {
	skipMaxHost, skipMinHost, skipSumSquare := cache.skips(metricID)
	counter := value.Value.Counter * sf
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
	res = rowbinary.AppendString(res, skey)
	if value.Value.ValueSet {
		if skipMinHost {
			res = rowbinary.AppendArgMinMaxInt32Float32Empty(res)
		} else {
			res = rowbinary.AppendArgMinMaxInt32Float32(res, value.Value.MinHostTag, float32(value.Value.ValueMin))
		}
		if skipMaxHost {
			res = rowbinary.AppendArgMinMaxInt32Float32Empty(res)
		} else {
			res = rowbinary.AppendArgMinMaxInt32Float32(res, value.Value.MaxHostTag, float32(value.Value.ValueMax))
		}
	} else {
		res = rowbinary.AppendArgMinMaxInt32Float32Empty(res) // counters do not have min_host set
		if skipMaxHost {
			res = rowbinary.AppendArgMinMaxInt32Float32Empty(res)
		} else {
			res = rowbinary.AppendArgMinMaxInt32Float32(res, value.Value.MaxCounterHostTag, float32(counter)) // max_counter_host, not always correct, but hopefully good enough
		}
	}
	return res
}

func (a *Aggregator) RowDataMarshalAppendPositions(b *aggregatorBucket, rnd *rand.Rand, res []byte, historic bool) []byte {
	sizeCounters := 0
	sizeValues := 0
	sizePercentiles := 0
	sizeUniques := 0
	sizeStringTops := 0
	sizeBuiltin := 0
	metricCache := makeMetricCache(a.metricStorage)
	usedTimestamps := map[uint32]struct{}{}

	insertItem := func(k data_model.Key, item *data_model.MultiItem, sf float64) { // lambda is convenient here
		resPos := len(res)
		if !item.Tail.Empty() { // only tail
			res = appendKeys(res, k, metricCache, usedTimestamps)

			res = multiValueMarshal(k.Metric, metricCache, res, &item.Tail, "", sf)

			if k.Metric < 0 {
				sizeBuiltin += len(res) - resPos
			} else {
				switch {
				case item.Tail.ValueTDigest != nil:
					sizePercentiles += len(res) - resPos
				case item.Tail.HLL.ItemsCount() != 0:
					sizeUniques += len(res) - resPos
				case item.Tail.Value.ValueSet:
					sizeValues += len(res) - resPos
				default:
					sizeCounters += len(res) - resPos
				}
			}
		}
		resPos = len(res)
		for skey, value := range item.Top {
			if value.Empty() { // must be never, but check is cheap
				continue
			}
			// We have no badges for string tops
			res = appendKeys(res, k, metricCache, usedTimestamps)
			res = multiValueMarshal(k.Metric, metricCache, res, value, skey, sf)
		}
		if k.Metric < 0 {
			sizeBuiltin += len(res) - resPos
		} else {
			// TODO - separate into 3 keys - is_string_top/is_builtin and hll/percentile/value/counter
			sizeStringTops += len(res) - resPos
		}
	}
	var seriesCount int
	for si := 0; si < len(b.shards); si++ {
		seriesCount += len(b.shards[si].multiItems)
	}
	sampler := data_model.NewSampler(seriesCount, data_model.SamplerConfig{
		Meta:  a.metricStorage,
		Rand:  rnd,
		KeepF: func(k data_model.Key, item *data_model.MultiItem) { insertItem(k, item, item.SF) },
	})
	// First, sample with global sampling factors, depending on cardinality. Collect relative sizes for 2nd stage sampling below.
	// TODO - actual sampleFactors are empty due to code commented out in estimator.go
	for si := 0; si < len(b.shards); si++ {
		for k, item := range b.shards[si].multiItems {
			whaleWeight := item.FinishStringTop(a.config.StringTopCountInsert) // all excess items are baked into Tail

			resPos := len(res)
			res = appendMultiBadge(res, k, item, metricCache, usedTimestamps)
			sizeBuiltin += len(res) - resPos

			accountMetric := k.Metric
			if k.Metric < 0 {
				if k.Metric != format.BuiltinMetricIDIngestionStatus {
					// For now sample only ingestion statuses on aggregator. Might be bad idea. TODO - check.
					insertItem(k, item, 1)
					continue
				}
				if k.Keys[1] != 0 {
					// Ingestion status and other unlimited per-metric built-ins should use its metric budget
					// So metrics are better isolated
					accountMetric = k.Keys[1]
				}
			}
			sz := item.RowBinarySizeEstimate()
			sampler.Add(data_model.SamplingMultiItemPair{
				Key:         k,
				Item:        item,
				WhaleWeight: whaleWeight,
				Size:        sz,
				MetricID:    accountMetric,
			})
		}
	}

	// 50K + 2.5K * min(sqrt(x*100),100) + 0.5K * x, you can check curve here https://www.desmos.com/calculator?lang=ru
	numContributors := int(b.contributorsOriginal.Counter + b.contributorsSpare.Counter)
	// Account for the fact that when # of contributors is very small, they will have very bad aggregation
	// so sampling will produce huge amounts of noise. Simple code below seems to be working for now.
	remainingBudget := int64(data_model.InsertBudgetFixed)
	minSqrtContributors := math.Sqrt(float64(numContributors) * 100)
	if minSqrtContributors > 100 { // Short term part peaks at 100 contributors
		minSqrtContributors = 100
	}
	remainingBudget += int64(float64(a.config.InsertBudget100) * minSqrtContributors)
	remainingBudget += int64(a.config.InsertBudget * numContributors) // fixed part + longterm part
	// Budget is per contributor, so if they come in 1% groups, total size will approx. fit
	// Also if 2x contributors come to spare, budget is also 2x
	samplerStat := sampler.Run(remainingBudget, 1)
	key1 := int32(format.TagValueIDConveyorRecent)
	if historic {
		key1 = format.TagValueIDConveyorHistoric
	}
	resPos := len(res)
	for k, v := range samplerStat.Items {
		// keep bytes
		key := data_model.AggKey(b.time, format.BuiltinMetricIDAggSamplingSizeBytes, [16]int32{0, key1, format.TagValueIDSamplingDecisionKeep, k[0], k[1], k[2]}, a.aggregatorHost, a.shardKey, a.replicaKey)
		mi := data_model.MultiItem{Tail: data_model.MultiValue{Value: v.SumSizeKeep}}
		insertItem(key, &mi, 1)
		// discard bytes
		key = data_model.AggKey(b.time, format.BuiltinMetricIDAggSamplingSizeBytes, [16]int32{0, key1, format.TagValueIDSamplingDecisionDiscard, k[0], k[1], k[2]}, a.aggregatorHost, a.shardKey, a.replicaKey)
		mi = data_model.MultiItem{Tail: data_model.MultiValue{Value: v.SumSizeDiscard}}
		insertItem(key, &mi, 1)
	}
	for _, s := range samplerStat.GetSampleFactors(nil) {
		k := s.Metric
		sf := float64(s.Value)
		key := data_model.AggKey(b.time, format.BuiltinMetricIDAggSamplingFactor, [16]int32{0, 0, 0, 0, k, format.TagValueIDAggSamplingFactorReasonInsertSize}, a.aggregatorHost, a.shardKey, a.replicaKey)
		res = appendBadge(res, key, data_model.ItemValue{Counter: 1, ValueSum: sf}, metricCache, usedTimestamps)
		res = appendSimpleValueStat(res, key, sf, 1, a.aggregatorHost, metricCache, usedTimestamps)
	}
	res = appendSimpleValueStat(res, data_model.AggKey(b.time, format.BuiltinMetricIDAggSamplingMetricCount, [16]int32{0, key1},
		a.aggregatorHost, a.shardKey, a.replicaKey), float64(len(samplerStat.Metrics)), 1, a.aggregatorHost, metricCache, usedTimestamps)
	res = appendSimpleValueStat(res, data_model.AggKey(b.time, format.BuiltinMetricIDAggInsertSize, [16]int32{0, 0, 0, 0, key1, format.TagValueIDSizeCounter},
		a.aggregatorHost, a.shardKey, a.replicaKey), float64(sizeCounters), 1, a.aggregatorHost, metricCache, usedTimestamps)
	res = appendSimpleValueStat(res, data_model.AggKey(b.time, format.BuiltinMetricIDAggInsertSize, [16]int32{0, 0, 0, 0, key1, format.TagValueIDSizeValue},
		a.aggregatorHost, a.shardKey, a.replicaKey), float64(sizeValues), 1, a.aggregatorHost, metricCache, usedTimestamps)
	res = appendSimpleValueStat(res, data_model.AggKey(b.time, format.BuiltinMetricIDAggInsertSize, [16]int32{0, 0, 0, 0, key1, format.TagValueIDSizePercentiles},
		a.aggregatorHost, a.shardKey, a.replicaKey), float64(sizePercentiles), 1, a.aggregatorHost, metricCache, usedTimestamps)
	res = appendSimpleValueStat(res, data_model.AggKey(b.time, format.BuiltinMetricIDAggInsertSize, [16]int32{0, 0, 0, 0, key1, format.TagValueIDSizeUnique},
		a.aggregatorHost, a.shardKey, a.replicaKey), float64(sizeUniques), 1, a.aggregatorHost, metricCache, usedTimestamps)

	insertTimeUnix := uint32(time.Now().Unix()) // same quality as timestamp from advanceBuckets, can be larger or smaller
	for t := range usedTimestamps {
		key := data_model.Key{Timestamp: insertTimeUnix, Metric: format.BuiltinMetricIDContributorsLog, Keys: [16]int32{0, int32(t)}}
		res = appendSimpleValueStat(res, key, float64(insertTimeUnix)-float64(t), 1, a.aggregatorHost, metricCache, nil)
		key = data_model.Key{Timestamp: t, Metric: format.BuiltinMetricIDContributorsLogRev, Keys: [16]int32{0, int32(insertTimeUnix)}}
		res = appendSimpleValueStat(res, key, float64(insertTimeUnix)-float64(t), 1, a.aggregatorHost, metricCache, nil)
	}

	sizeBuiltin += len(res) - resPos
	resPos = len(res)
	res = appendSimpleValueStat(res, data_model.AggKey(b.time, format.BuiltinMetricIDAggInsertSize, [16]int32{0, 0, 0, 0, key1, format.TagValueIDSizeStringTop},
		a.aggregatorHost, a.shardKey, a.replicaKey), float64(sizeStringTops), 1, a.aggregatorHost, metricCache, usedTimestamps)
	sizeBuiltin += (len(res) - resPos) * 2 // hypothesis - next line inserts as many bytes as previous one
	res = appendSimpleValueStat(res, data_model.AggKey(b.time, format.BuiltinMetricIDAggInsertSize, [16]int32{0, 0, 0, 0, key1, format.TagValueIDSizeBuiltIn},
		a.aggregatorHost, a.shardKey, a.replicaKey), float64(sizeBuiltin), 1, a.aggregatorHost, metricCache, usedTimestamps)
	return res
}

func makeHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			DialContext: srvfunc.CachingDialer,
		},
		Timeout: timeout,
	}
}

func sendToClickhouse(httpClient *http.Client, khAddr string, table string, body []byte) (status int, exception int, elapsed float64, err error) {
	queryPrefix := url.PathEscape(fmt.Sprintf("INSERT INTO %s FORMAT RowBinary", table))
	URL := fmt.Sprintf("http://%s/?input_format_values_interpret_expressions=0&query=%s", khAddr, queryPrefix)
	req, err := http.NewRequest("POST", URL, bytes.NewReader(body))
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
	// TODO - use ParseExceptionFromResponseBody
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

	clickhouseExceptionText := resp.Header.Get("X-ClickHouse-Exception-Code")
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
