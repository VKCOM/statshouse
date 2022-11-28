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
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/metajournal"
	"github.com/vkcom/statshouse/internal/vkgo/rowbinary"
	"github.com/vkcom/statshouse/internal/vkgo/srvfunc"

	"pgregory.net/rand"
)

func getTableDesc() string {
	keysFieldsNamesVec := make([]string, format.MaxTags)
	for i := 0; i < format.MaxTags; i++ {
		keysFieldsNamesVec[i] = fmt.Sprintf(`key%d`, i)
	}
	return `statshouse_value_incoming_prekey(metric,prekey,prekey_set,time,` + strings.Join(keysFieldsNamesVec, `,`) + `,count,min,max,sum,sumsquare,percentiles,uniq_state,skey,max_host)`
}

type prekeyIndexCache struct {
	// Motivation - we have statList sorted by metric, except ingestion statuses are interleaved,
	// because they are credited to the metric. Also, we have small # of builtin metrics inserted, but we do not care about speed for them.
	journal               *metajournal.MetricsStorage
	ingestionStatusPrekey int
	lastMetricID          int32
	lastMetricPrekey      int
}

func makePrekeyCache(journal *metajournal.MetricsStorage) *prekeyIndexCache {
	result := &prekeyIndexCache{
		journal:               journal,
		ingestionStatusPrekey: -1,
		lastMetricPrekey:      -1, // so if somehow 0 metricID is inserted first, will have no prekey
	}
	if bm, ok := format.BuiltinMetrics[format.BuiltinMetricIDIngestionStatus]; ok {
		result.ingestionStatusPrekey = bm.PreKeyIndex
	}
	return result
}

func (p *prekeyIndexCache) getPrekeyIndex(metricID int32) int {
	if metricID == format.BuiltinMetricIDIngestionStatus {
		return p.ingestionStatusPrekey
	}
	if metricID == p.lastMetricID {
		return p.lastMetricPrekey
	}
	p.lastMetricID = metricID
	if bm, ok := format.BuiltinMetrics[metricID]; ok {
		p.lastMetricPrekey = bm.PreKeyIndex
		return bm.PreKeyIndex
	}
	if metaMetric := p.journal.GetMetaMetric(metricID); metaMetric != nil {
		p.lastMetricPrekey = metaMetric.PreKeyIndex
		return metaMetric.PreKeyIndex
	}
	return -1
}

func appendKeys(res []byte, k data_model.Key, prekeyIndexes *prekeyIndexCache, usedTimestamps map[uint32]struct{}) []byte {
	var tmp [4 + 4 + 1 + 4 + format.MaxTags*4]byte // metric, prekey, prekey_set, time
	binary.LittleEndian.PutUint32(tmp[0:], uint32(k.Metric))
	prekeyIndex := prekeyIndexes.getPrekeyIndex(k.Metric)
	if prekeyIndex >= 0 {
		binary.LittleEndian.PutUint32(tmp[4:], uint32(k.Keys[prekeyIndex]))
		tmp[8] = 1
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
func appendBadge(res []byte, k data_model.Key, v data_model.ItemValue, prekeyIndexes *prekeyIndexCache, usedTimestamps map[uint32]struct{}) []byte {
	if k.Metric >= 0 { // fastpath
		return res
	}
	ts := (k.Timestamp / 5) * 5
	// We used to select with single function (avg), so we approximated sum of counters so that any number of events produce avg >= 1
	// TODO - deprecated legacy badges and use new badges
	switch k.Metric {
	case format.BuiltinMetricIDIngestionStatus:
		if k.Keys[1] == 0 || k.Keys[2] == format.TagValueIDSrcIngestionStatusOKCached || k.Keys[2] == format.TagValueIDSrcIngestionStatusOKUncached ||
			k.Keys[2] == format.TagValueIDSrcIngestionStatusWarnDeprecatedKeyName || k.Keys[2] == format.TagValueIDSrcIngestionStatusWarnDeprecatedT ||
			k.Keys[2] == format.TagValueIDSrcIngestionStatusWarnDeprecatedStop || k.Keys[2] == format.TagValueIDSrcIngestionStatusWarnOldCounterSemantic {
			return res
		}
		res = appendValueStat(res, data_model.Key{Timestamp: ts, Metric: format.BuiltinMetricIDBadges, Keys: [16]int32{0, format.TagValueIDBadgeIngestionErrorsOld, k.Keys[1]}}, "", data_model.ItemValue{Counter: 1, ValueSum: v.Counter, MaxHostTag: v.MaxHostTag}, prekeyIndexes, usedTimestamps)
		return appendValueStat(res, data_model.Key{Timestamp: ts, Metric: format.BuiltinMetricIDBadges, Keys: [16]int32{0, format.TagValueIDBadgeIngestionErrors, k.Keys[1]}}, "", data_model.ItemValue{Counter: v.Counter, MaxHostTag: v.MaxHostTag}, prekeyIndexes, usedTimestamps)
	case format.BuiltinMetricIDAgentSamplingFactor:
		return appendValueStat(res, data_model.Key{Timestamp: ts, Metric: format.BuiltinMetricIDBadges, Keys: [16]int32{0, format.TagValueIDBadgeAgentSamplingFactor, k.Keys[1]}}, "", v, prekeyIndexes, usedTimestamps)
	case format.BuiltinMetricIDAggSamplingFactor:
		return appendValueStat(res, data_model.Key{Timestamp: ts, Metric: format.BuiltinMetricIDBadges, Keys: [16]int32{0, format.TagValueIDBadgeAggSamplingFactor, k.Keys[4]}}, "", v, prekeyIndexes, usedTimestamps)
	case format.BuiltinMetricIDAggMappingCreated:
		if k.Keys[5] == format.TagValueIDAggMappingCreatedStatusOK ||
			k.Keys[5] == format.TagValueIDAggMappingCreatedStatusCreated {
			return res
		}
		res = appendValueStat(res, data_model.Key{Timestamp: ts, Metric: format.BuiltinMetricIDBadges, Keys: [16]int32{0, format.TagValueIDBadgeAggMappingErrorsOld, k.Keys[4]}}, "", data_model.ItemValue{Counter: 1, ValueSum: v.Counter, MaxHostTag: v.MaxHostTag}, prekeyIndexes, usedTimestamps)
		return appendValueStat(res, data_model.Key{Timestamp: ts, Metric: format.BuiltinMetricIDBadges, Keys: [16]int32{0, format.TagValueIDBadgeAggMappingErrors, k.Keys[4]}}, "", data_model.ItemValue{Counter: v.Counter, MaxHostTag: v.MaxHostTag}, prekeyIndexes, usedTimestamps)
	case format.BuiltinMetricIDAggBucketReceiveDelaySec:
		return appendValueStat(res, data_model.Key{Timestamp: ts, Metric: format.BuiltinMetricIDBadges, Keys: [16]int32{0, format.TagValueIDBadgeContributors, 0}}, "", data_model.ItemValue{Counter: v.Counter, MaxHostTag: v.MaxHostTag}, prekeyIndexes, usedTimestamps)
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

func appendValueStat(res []byte, key data_model.Key, skey string, v data_model.ItemValue, prekeyIndexes *prekeyIndexCache, usedTimestamps map[uint32]struct{}) []byte {
	if v.Counter <= 0 { // We have lots of built-in  counters which are normally 0
		return res
	}
	res = appendKeys(res, key, prekeyIndexes, usedTimestamps)

	res = appendAggregates(res, v.Counter, v.ValueMin, v.ValueMax, v.ValueSum, v.ValueSumSquare)

	res = rowbinary.AppendEmptyCentroids(res)
	res = rowbinary.AppendEmptyUnique(res)
	res = rowbinary.AppendString(res, skey)

	// many built-in metrics are aggregator-specific and have no hosts set, but we write hosts always, because optimization would be tiny
	// res = rowbinary.AppendArgMinMaxInt32Float32(res, v.MinHostTag, float32(v.ValueMin))
	res = rowbinary.AppendArgMinMaxInt32Float32(res, v.MaxHostTag, float32(v.ValueMax))
	return res
}

func appendSimpleValueStat(res []byte, key data_model.Key, v float64, count float64, hostTag int32, prekeyIndexes *prekeyIndexCache, usedTimestamps map[uint32]struct{}) []byte {
	return appendValueStat(res, key, "", data_model.SimpleItemValue(v, count, hostTag), prekeyIndexes, usedTimestamps)
}

func multiValueMarshal(res []byte, value *data_model.MultiValue, skey string, sf float64) []byte {
	res = appendAggregates(res, value.Value.Counter*sf, value.Value.ValueMin, value.Value.ValueMax, value.Value.ValueSum*sf, value.Value.ValueSumSquare*sf)
	res = rowbinary.AppendCentroids(res, value.ValueTDigest, sf)
	res = value.HLL.MarshallAppend(res)
	res = rowbinary.AppendString(res, skey)
	res = rowbinary.AppendArgMinMaxInt32Float32(res, value.Value.MaxHostTag, float32(value.Value.Counter*sf)) // max_host, not always correct, but hopefully good enough
	return res
}

func (a *Aggregator) RowDataMarshalAppendPositions(b *aggregatorBucket, rnd *rand.Rand, res []byte, historic bool) []byte {
	sizeCounters := 0
	sizeValues := 0
	sizePercentiles := 0
	sizeUniques := 0
	sizeStringTops := 0
	sizeBuiltin := 0
	prekeyIndexes := makePrekeyCache(a.metricStorage)
	usedTimestamps := map[uint32]struct{}{}

	insertItem := func(k data_model.Key, item *data_model.MultiItem, sf float64) { // lambda is convenient here
		resPos := len(res)
		if !item.Tail.Empty() { // only tail
			res = appendKeys(res, k, prekeyIndexes, usedTimestamps)

			res = multiValueMarshal(res, &item.Tail, "", sf)

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
			// We have no badges for string tops
			res = appendKeys(res, k, prekeyIndexes, usedTimestamps)
			res = multiValueMarshal(res, value, skey, sf)
		}
		if k.Metric < 0 {
			sizeBuiltin += len(res) - resPos
		} else {
			// TODO - separate into 3 keys - is_string_top/is_builtin and hll/percentile/value/counter
			sizeStringTops += len(res) - resPos
		}
	}

	metricsMap := map[int32]*data_model.SamplingMetric{}
	var metricsList []*data_model.SamplingMetric
	totalItemsSize := 0
	var remainingWeight int64

	// First, sample with global sampling factors, depending on cardinality. Collect relative sizes for 2nd stage sampling below.
	// TODO - actual sampleFactors are empty due to code commented out in estimator.go
	for si := 0; si < len(b.shards); si++ {
		for k, item := range b.shards[si].multiItems {
			resPos := len(res)
			res = appendBadge(res, k, item.Tail.Value, prekeyIndexes, usedTimestamps)
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
			whaleWeight := item.FinishStringTop(a.config.StringTopCountInsert) // all excess items are baked into Tail
			sz := item.RowBinarySizeEstimate()
			samplingMetric, ok := metricsMap[accountMetric]
			if !ok {
				metricInfo := a.metricStorage.GetMetaMetric(accountMetric)
				samplingMetric = &data_model.SamplingMetric{
					MetricID:     accountMetric,
					MetricWeight: format.EffectiveWeightOne,
					RoundFactors: false, // default is no rounding
				}
				if metricInfo != nil {
					samplingMetric.MetricWeight = metricInfo.EffectiveWeight
					samplingMetric.RoundFactors = metricInfo.RoundSampleFactors
				}
				metricsMap[accountMetric] = samplingMetric
				metricsList = append(metricsList, samplingMetric)
				remainingWeight += samplingMetric.MetricWeight
			}
			samplingMetric.SumSize += int64(sz)
			samplingMetric.Items = append(samplingMetric.Items, data_model.SamplingMultiItemPair{Key: k, Item: item, WhaleWeight: whaleWeight})
			totalItemsSize += sz
		}
	}

	sort.Slice(metricsList, func(i, j int) bool {
		// comparing rational numbers
		return metricsList[i].SumSize*metricsList[j].MetricWeight < metricsList[j].SumSize*metricsList[i].MetricWeight
	})
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
	pos := 0
	for ; pos < len(metricsList) && metricsList[pos].SumSize*remainingWeight <= remainingBudget*metricsList[pos].MetricWeight; pos++ { // statIdCount <= totalBudget/remainedStats
		samplingMetric := metricsList[pos]
		// No sampling for this stat - do not add to samplingThresholds
		remainingBudget -= samplingMetric.SumSize
		remainingWeight -= samplingMetric.MetricWeight
		for _, v := range samplingMetric.Items {
			insertItem(v.Key, v.Item, 1)
		}
	}
	sampleFactors := map[int32]float64{}
	for i := pos; i < len(metricsList); i++ {
		samplingMetric := metricsList[i]
		metric := samplingMetric.MetricID
		sf := float64(samplingMetric.SumSize*remainingWeight) / float64(samplingMetric.MetricWeight*remainingBudget)
		if samplingMetric.RoundFactors {
			sf = data_model.RoundSampleFactor(rnd, sf)
			if sf <= 1 { // Many sample factors are between 1 and 2, so this is worthy optimization
				for _, v := range samplingMetric.Items {
					insertItem(v.Key, v.Item, sf)
				}
				continue
			}
		}
		sampleFactors[metric] = sf
		whalesAllowed := int64(0)
		if samplingMetric.SumSize*remainingWeight > 0 { // should be never but check is cheap
			whalesAllowed = int64(len(samplingMetric.Items)) * (samplingMetric.MetricWeight * remainingBudget) / (samplingMetric.SumSize * remainingWeight) / 2 // len(items) / sf / 2
		}
		// Motivation - often we have a few rows with dominating counts (whales). If we randomly discard those rows, we get wild fluctuation
		// of sums. On the other hand if we systematically discard rows with small counts, rare events, like errors cannot get through.
		// So we allow half of sampling budget for whales, and the other half is spread fairly between other events.
		// TODO - model this approach. Adjust algorithm parameters.
		if whalesAllowed > 0 {
			if whalesAllowed > int64(len(samplingMetric.Items)) { // should be never but check is cheap
				whalesAllowed = int64(len(samplingMetric.Items))
			}
			sort.Slice(samplingMetric.Items, func(i, j int) bool {
				return samplingMetric.Items[i].WhaleWeight > samplingMetric.Items[j].WhaleWeight
			})
			for _, v := range samplingMetric.Items[:whalesAllowed] {
				insertItem(v.Key, v.Item, 1)
			}
			samplingMetric.Items = samplingMetric.Items[whalesAllowed:]
		}
		sf *= 2 // half of space is occupied by whales now. TODO - we can be more exact here, and save lots of rnd calls
		for _, v := range samplingMetric.Items {
			if rnd.Float64()*sf < 1 {
				insertItem(v.Key, v.Item, sf)
			}
		}
	}
	key1 := int32(format.TagValueIDConveyorRecent)
	if historic {
		key1 = format.TagValueIDConveyorHistoric
	}
	resPos := len(res)
	for k, sf := range sampleFactors {
		key := data_model.AggKey(b.time, format.BuiltinMetricIDAggSamplingFactor, [16]int32{0, 0, 0, 0, k, format.TagValueIDAggSamplingFactorReasonInsertSize}, a.aggregatorHost, a.shardKey, a.replicaKey)
		res = appendBadge(res, key, data_model.ItemValue{Counter: 1, ValueSum: sf}, prekeyIndexes, usedTimestamps)
		res = appendSimpleValueStat(res, key, sf, 1, a.aggregatorHost, prekeyIndexes, usedTimestamps)
	}
	res = appendSimpleValueStat(res, data_model.AggKey(b.time, format.BuiltinMetricIDAggInsertSize, [16]int32{0, 0, 0, 0, key1, format.TagValueIDSizeCounter},
		a.aggregatorHost, a.shardKey, a.replicaKey), float64(sizeCounters), 1, a.aggregatorHost, prekeyIndexes, usedTimestamps)
	res = appendSimpleValueStat(res, data_model.AggKey(b.time, format.BuiltinMetricIDAggInsertSize, [16]int32{0, 0, 0, 0, key1, format.TagValueIDSizeValue},
		a.aggregatorHost, a.shardKey, a.replicaKey), float64(sizeValues), 1, a.aggregatorHost, prekeyIndexes, usedTimestamps)
	res = appendSimpleValueStat(res, data_model.AggKey(b.time, format.BuiltinMetricIDAggInsertSize, [16]int32{0, 0, 0, 0, key1, format.TagValueIDSizePercentiles},
		a.aggregatorHost, a.shardKey, a.replicaKey), float64(sizePercentiles), 1, a.aggregatorHost, prekeyIndexes, usedTimestamps)
	res = appendSimpleValueStat(res, data_model.AggKey(b.time, format.BuiltinMetricIDAggInsertSize, [16]int32{0, 0, 0, 0, key1, format.TagValueIDSizeUnique},
		a.aggregatorHost, a.shardKey, a.replicaKey), float64(sizeUniques), 1, a.aggregatorHost, prekeyIndexes, usedTimestamps)

	insertTimeUnix := uint32(time.Now().Unix()) // same quality as timestamp from advanceBuckets, can be larger or smaller
	for t := range usedTimestamps {
		key := data_model.Key{Timestamp: insertTimeUnix, Metric: format.BuiltinMetricIDContributorsLog, Keys: [16]int32{0, int32(t)}}
		res = appendSimpleValueStat(res, key, float64(insertTimeUnix)-float64(t), 1, a.aggregatorHost, prekeyIndexes, nil)
		key = data_model.Key{Timestamp: t, Metric: format.BuiltinMetricIDContributorsLogRev, Keys: [16]int32{0, int32(insertTimeUnix)}}
		res = appendSimpleValueStat(res, key, float64(insertTimeUnix)-float64(t), 1, a.aggregatorHost, prekeyIndexes, nil)
	}

	sizeBuiltin += len(res) - resPos
	resPos = len(res)
	res = appendSimpleValueStat(res, data_model.AggKey(b.time, format.BuiltinMetricIDAggInsertSize, [16]int32{0, 0, 0, 0, key1, format.TagValueIDSizeStringTop},
		a.aggregatorHost, a.shardKey, a.replicaKey), float64(sizeStringTops), 1, a.aggregatorHost, prekeyIndexes, usedTimestamps)
	sizeBuiltin += (len(res) - resPos) * 2 // hypothesis - next line inserts as many bytes as previous one
	res = appendSimpleValueStat(res, data_model.AggKey(b.time, format.BuiltinMetricIDAggInsertSize, [16]int32{0, 0, 0, 0, key1, format.TagValueIDSizeBuiltIn},
		a.aggregatorHost, a.shardKey, a.replicaKey), float64(sizeBuiltin), 1, a.aggregatorHost, prekeyIndexes, usedTimestamps)
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
	if err != nil {
		return 0, 0, dur.Seconds(), err
	}
	if resp.StatusCode == http.StatusOK {
		_, _ = io.Copy(io.Discard, resp.Body) // keepalive
		_ = resp.Body.Close()
		return 0, 0, dur.Seconds(), nil
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
