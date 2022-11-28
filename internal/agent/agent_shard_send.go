// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package agent

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/pierrec/lz4"
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/vkgo/build"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
	"pgregory.net/rand"
)

// If clients wish periodic measurements, they are advised to send them around the middle of calendar second
func (s *Shard) flushBuckets(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	nowUnix := uint32(now.Unix())

	if s.CurrentTime+s.MissedSeconds >= nowUnix { // fastpath
		return
	}
	s.addBuiltInsLocked(nowUnix)
	// We send missed seconds if timestamp "jumps", which are converted  by aggregator into contributors for previous seconds
	// otherwise #contributors will fluctuate. For this reason we also send empty buckets
	if s.PreprocessingBuckets != nil {
		// s.client.Client.Logf("Zatup 1 shard %d  bucket.time %d nowUnix %d", s.ShardReplicaNum, s.CurrentBucket.time, nowUnix)
		s.MissedSeconds = nowUnix - s.CurrentTime
		// We continue aggregating if processing conveyor is stalled now
		return
	}
	for r, bs := range s.CurrentBuckets {
		if r != format.AllowedResolution(r) {
			continue
		}
		nextT := (nowUnix / uint32(r)) * uint32(r)
		for sh, b := range bs {
			if nextT == b.Time {
				continue
			}
			if b.Empty() { // optimize by only moving time forward
				s.CurrentBuckets[r][sh].Time = nextT
				continue
			}
			// future queue pos is assigned without seams if missed seconds is 0
			futureQueuePos := (nextT + uint32(sh)) % 60
			s.FutureQueue[futureQueuePos] = append(s.FutureQueue[futureQueuePos], b)
			s.CurrentBuckets[r][sh] = &data_model.MetricsBucket{Time: nextT}
		}
	}
	s.PreprocessingBuckets = s.FutureQueue[nowUnix%60]
	s.FutureQueue[nowUnix%60] = nil

	// Due to b.Empty() optimization above, if no data is collected, nothing is in FutureQueue
	// As we use PreprocessingBuckets as flag, it must be not nil so that processing and sending is performed
	// for each contributor every second
	if s.PreprocessingBuckets == nil {
		s.PreprocessingBuckets = []*data_model.MetricsBucket{}
	}

	s.PreprocessingMissedSeconds = s.MissedSeconds
	s.MissedSeconds = 0

	s.PreprocessingBucketTime = s.CurrentTime
	s.CurrentTime = nowUnix

	s.condPreprocess.Signal()
}

func addSizeByTypeMetric(sb *tlstatshouse.SourceBucket2, partKey int32, size int) {
	// This metric is added by source, because aggregator has no spare time for that
	k := data_model.Key{Metric: format.BuiltinMetricIDTLByteSizePerInflightType, Keys: [16]int32{0, partKey}}

	item := k.TLMultiItemFromKey(0)
	item.Tail.SetCounterEq1(true, &item.FieldsMask)
	item.Tail.SetValueSet(true, &item.FieldsMask)
	item.Tail.SetValueMin(float64(size), &item.FieldsMask)
	sb.Metrics = append(sb.Metrics, item)
}

func lessByShard(a *tlstatshouse.MultiItem, b *tlstatshouse.MultiItem, perm []int) bool {
	aID := a.FieldsMask >> 24
	bID := b.FieldsMask >> 24
	if aID != bID {
		return perm[aID] < perm[bID]
	}
	return a.Metric < b.Metric
}

func sourceBucketToTL(bucket *data_model.MetricsBucket, perm []int, sampleFactors map[int32]float64) tlstatshouse.SourceBucket2 {
	var marshalBuf []byte
	var sizeBuf []byte
	sb := tlstatshouse.SourceBucket2{}

	sizeUnique := 0
	sizePercentiles := 0
	sizeValue := 0
	sizeSingleValue := 0
	sizeCounter := 0
	sizeStringTop := 0 // Of all types

	for k, v := range bucket.MultiItems {
		if k.Metric == format.BuiltinMetricIDIngestionStatus && k.Keys[2] == format.TagValueIDSrcIngestionStatusOKCached {
			// transfer optimization.
			sb.IngestionStatusOk2 = append(sb.IngestionStatusOk2, tlstatshouse.IngestionStatus2{Env: k.Keys[0], Metric: k.Keys[1], Value: float32(v.Tail.Value.Counter)})
			continue
		}
		item := k.TLMultiItemFromKey(bucket.Time)
		v.Tail.MultiValueToTL(&item.Tail, v.SF, &item.FieldsMask, &marshalBuf)
		sizeBuf, _ = item.Write(sizeBuf[:0])
		switch { // This is only an approximation
		case item.Tail.IsSetUniques(item.FieldsMask):
			sizeUnique += len(sizeBuf)
		case item.Tail.IsSetCentroids(item.FieldsMask):
			sizePercentiles += len(sizeBuf)
		case item.Tail.IsSetValueMax(item.FieldsMask):
			sizeValue += len(sizeBuf)
		case item.Tail.IsSetValueMin(item.FieldsMask):
			sizeSingleValue += len(sizeBuf)
		default:
			sizeCounter += len(sizeBuf)
		}
		var top []tlstatshouse.TopElement
		for skey, value := range v.Top {
			el := tlstatshouse.TopElement{Key: skey}
			value.MultiValueToTL(&el.Value, v.SF, &el.FieldsMask, &marshalBuf)
			top = append(top, el)
			sizeBuf, _ = el.Write(sizeBuf[:0])
			sizeStringTop += len(sizeBuf)
		}
		if len(top) != 0 {
			item.SetTop(top)
		}
		sb.Metrics = append(sb.Metrics, item)
	}
	addSizeByTypeMetric(&sb, format.TagValueIDSizeUnique, sizeUnique)
	addSizeByTypeMetric(&sb, format.TagValueIDSizePercentiles, sizePercentiles)
	addSizeByTypeMetric(&sb, format.TagValueIDSizeValue, sizeValue)
	addSizeByTypeMetric(&sb, format.TagValueIDSizeSingleValue, sizeSingleValue)
	addSizeByTypeMetric(&sb, format.TagValueIDSizeCounter, sizeCounter)
	addSizeByTypeMetric(&sb, format.TagValueIDSizeStringTop, sizeStringTop)

	for k, v := range sampleFactors {
		sb.SampleFactors = append(sb.SampleFactors, tlstatshouse.SampleFactor{
			Metric: k,
			Value:  float32(v),
		})
	}

	sbSizeCalc := tlstatshouse.SourceBucket2{SampleFactors: sb.SampleFactors}
	sizeBuf, _ = sbSizeCalc.Write(sizeBuf[:0])
	addSizeByTypeMetric(&sb, format.TagValueIDSizeSampleFactors, len(sizeBuf))

	sbSizeCalc = tlstatshouse.SourceBucket2{IngestionStatusOk: sb.IngestionStatusOk, IngestionStatusOk2: sb.IngestionStatusOk2}
	sizeBuf, _ = sbSizeCalc.Write(sizeBuf[:0])
	addSizeByTypeMetric(&sb, format.TagValueIDSizeIngestionStatusOK, len(sizeBuf))

	sort.Slice(sb.Metrics, func(i, j int) bool {
		return lessByShard(&sb.Metrics[i], &sb.Metrics[j], perm)
	})
	return sb
}

func (s *Shard) goPreProcess() {
	rnd := rand.New() // We use distinct rand so that we can use it without locking

	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		if s.PreprocessingBuckets == nil {
			s.condPreprocess.Wait()
			continue
		}
		buckets := s.PreprocessingBuckets
		s.PreprocessingBuckets = nil

		missedSeconds := s.PreprocessingMissedSeconds
		s.PreprocessingMissedSeconds = 0

		bucket := &data_model.MetricsBucket{Time: s.PreprocessingBucketTime}
		s.PreprocessingBucketTime = 0
		s.mu.Unlock()

		s.mergeBuckets(bucket, buckets)
		sampleFactors := s.sampleBucket(bucket, rnd)
		s.sendToSenders(bucket, missedSeconds, sampleFactors)

		s.mu.Lock()
	}
}

func (s *Shard) mergeBuckets(bucket *data_model.MetricsBucket, buckets []*data_model.MetricsBucket) {
	for _, b := range buckets { // optimization to merge into the largest map
		if len(b.MultiItems) > len(bucket.MultiItems) {
			b.MultiItems, bucket.MultiItems = bucket.MultiItems, b.MultiItems
		}
	}
	for _, b := range buckets {
		for k, v := range b.MultiItems {
			mi := data_model.MapKeyItemMultiItem(&bucket.MultiItems, k, s.config.StringTopCapacity, nil)
			mi.Merge(v)
		}
	}
}

// this function will be replaced by sampleBucketWithGroups, do not improve
func (s *Shard) sampleBucket(bucket *data_model.MetricsBucket, rnd *rand.Rand) map[int32]float64 { // returns sample factors
	if s.config.SampleGroups {
		return s.sampleBucketWithGroups(bucket, rnd)
	}

	// Same algorithm as in aggregator, but instead of inserting selected, we remove items which were not selected by sampling algorithm
	metricsMap := map[int32]*data_model.SamplingMetric{}
	var metricsList []*data_model.SamplingMetric
	totalItemsSize := 0
	var remainingWeight int64

	for k, item := range bucket.MultiItems {
		whaleWeight := item.FinishStringTop(s.config.StringTopCountSend) // all excess items are baked into Tail
		accountMetric := k.Metric
		sz := k.TLSizeEstimate(bucket.Time) + item.TLSizeEstimate()
		if k.Metric == format.BuiltinMetricIDIngestionStatus {
			if k.Keys[1] != 0 {
				// Ingestion status and other unlimited per-metric built-ins should use its metric budget
				// So metrics are better isolated
				accountMetric = k.Keys[1]
			}
			if k.Keys[2] == format.TagValueIDSrcIngestionStatusOKCached {
				// These are so common, we have transfer optimization for them
				sz = 3 * 4 // see statshouse.ingestion_status2
			}
		}

		samplingMetric, ok := metricsMap[accountMetric]
		if !ok {
			var metricInfo *format.MetricMetaValue
			if s.statshouse.metricStorage != nil {
				metricInfo = s.statshouse.metricStorage.GetMetaMetric(accountMetric)
			}
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

	sort.Slice(metricsList, func(i, j int) bool {
		// comparing rational numbers
		return metricsList[i].SumSize*metricsList[j].MetricWeight < metricsList[j].SumSize*metricsList[i].MetricWeight
	})
	numShards := s.statshouse.NumShardReplicas()
	remainingBudget := int64((s.config.SampleBudget + numShards - 1) / numShards)
	if remainingBudget <= 0 { // if happens, will lead to divide by zero below, so we add cheap protection
		remainingBudget = 1
	}
	if remainingBudget > data_model.MaxUncompressedBucketSize/2 { // Algorithm is not exact
		remainingBudget = data_model.MaxUncompressedBucketSize / 2
	}
	sampleFactors := map[int32]float64{}
	pos := 0
	for ; pos < len(metricsList) && metricsList[pos].SumSize*remainingWeight <= remainingBudget*metricsList[pos].MetricWeight; pos++ { // statIdCount <= totalBudget/remainedStats
		samplingMetric := metricsList[pos]
		// No sampling for this stat - do not add to samplingThresholds
		remainingBudget -= samplingMetric.SumSize
		remainingWeight -= samplingMetric.MetricWeight
		// Keep all elements in bucket
	}
	for i := pos; i < len(metricsList); i++ {
		samplingMetric := metricsList[i]
		metric := samplingMetric.MetricID
		if metric < 0 {
			// optimization first. below are list of safe metrics which have very few rows so sampling can be avoided
			if metric == format.BuiltinMetricIDAgentMapping || metric == format.BuiltinMetricIDJournalVersions ||
				metric == format.BuiltinMetricIDAgentReceivedPacketSize || metric == format.BuiltinMetricIDAgentReceivedBatchSize ||
				metric == format.BuiltinMetricIDHeartbeatVersion ||
				metric == format.BuiltinMetricIDHeartbeatArgs || metric == format.BuiltinMetricIDHeartbeatArgs2 ||
				metric == format.BuiltinMetricIDHeartbeatArgs3 || metric == format.BuiltinMetricIDHeartbeatArgs4 ||
				metric == format.BuiltinMetricIDAgentDiskCacheErrors || metric == format.BuiltinMetricIDTimingErrors ||
				metric == format.BuiltinMetricIDUsageMemory || metric == format.BuiltinMetricIDUsageCPU {
				continue
			}
		}

		sf := float64(samplingMetric.SumSize*remainingWeight) / float64(samplingMetric.MetricWeight*remainingBudget)
		if samplingMetric.RoundFactors {
			sf = data_model.RoundSampleFactor(rnd, sf)
			if sf <= 1 { // Many sample factors are between 1 and 2, so this is worthy optimization
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
			// Keep all whale elements in bucket
			samplingMetric.Items = samplingMetric.Items[whalesAllowed:]
		}
		sf *= 2 // half of space is occupied by whales now. TODO - we can be more exact here, make permutations and take as many elements as we need, saving lots of rnd calls
		for _, v := range samplingMetric.Items {
			if rnd.Float64()*sf < 1 {
				v.Item.SF = sf // communicate selected factor to next step of processing
				continue
			}
			delete(bucket.MultiItems, v.Key)
		}
	}
	if pos < len(metricsList) {
		value := float64(remainingBudget) / float64(remainingWeight)
		key := data_model.Key{Metric: format.BuiltinMetricIDAgentPerMetricSampleBudget, Keys: [16]int32{0, format.TagValueIDAgentFirstSampledMetricBudgetPerMetric}}
		mi := data_model.MapKeyItemMultiItem(&bucket.MultiItems, key, s.config.StringTopCapacity, nil)
		mi.Tail.Value.AddValueCounterHost(value, 1, 0)
	} else {
		key := data_model.Key{Metric: format.BuiltinMetricIDAgentPerMetricSampleBudget, Keys: [16]int32{0, format.TagValueIDAgentFirstSampledMetricBudgetUnused}}
		mi := data_model.MapKeyItemMultiItem(&bucket.MultiItems, key, s.config.StringTopCapacity, nil)
		mi.Tail.Value.AddValueCounterHost(float64(remainingBudget), 1, 0)
	}
	return sampleFactors
}

// contents of statItem.Items will be destructively modified after this function
func sampleStatItem(bucket *data_model.MetricsBucket, statItem *data_model.SamplingMetric, sf float64, rnd *rand.Rand) bool { // returns if we decided to sample
	metric := statItem.MetricID
	if metric < 0 {
		// optimization first. below are list of safe metrics which have very few rows so sampling can be avoided
		if metric == format.BuiltinMetricIDAgentMapping || metric == format.BuiltinMetricIDJournalVersions ||
			metric == format.BuiltinMetricIDAgentReceivedPacketSize || metric == format.BuiltinMetricIDAgentReceivedBatchSize ||
			metric == format.BuiltinMetricIDHeartbeatVersion ||
			metric == format.BuiltinMetricIDHeartbeatArgs || metric == format.BuiltinMetricIDHeartbeatArgs2 ||
			metric == format.BuiltinMetricIDHeartbeatArgs3 || metric == format.BuiltinMetricIDHeartbeatArgs4 ||
			metric == format.BuiltinMetricIDAgentDiskCacheErrors || metric == format.BuiltinMetricIDTimingErrors ||
			metric == format.BuiltinMetricIDUsageMemory || metric == format.BuiltinMetricIDUsageCPU {
			return false
		}
	}

	if statItem.RoundFactors {
		sf = data_model.RoundSampleFactor(rnd, sf)
	}
	if sf <= 1 { // Many sample factors will be between 1 RoundSampleFactor, plus this protects code below from overflow
		return false
	}
	// Motivation - often we have a few rows with dominating counts (whales). If we randomly discard those rows, we get wild fluctuation
	// of sums. On the other hand if we systematically discard rows with small counts, rare events, like errors cannot get through.
	// So we allow half of sampling budget for whales, and the other half is spread fairly between other events.
	// TODO - model this approach. Adjust algorithm parameters. Seems working great in the wild, though.
	items := statItem.Items
	itemsAllowedF := float64(len(items)) / sf
	// for very tight squeezes when itemsAllowedF < 1, probabilistic rounding will allow items to pass through with right chance
	itemsAllowed := int(data_model.RoundSampleFactor(rnd, itemsAllowedF))
	if itemsAllowed < 0 { // should be never but check is cheap
		itemsAllowed = 0
	}
	if itemsAllowed > len(items) { // should be never but check is cheap
		itemsAllowed = len(items)
	}
	whalesAllowed := itemsAllowed / 2 // if single slot, do not want to keep single whale, so not (itemsAllowed + 1) / 2
	if whalesAllowed > 0 {
		sort.Slice(items, func(i, j int) bool {
			return items[i].WhaleWeight > items[j].WhaleWeight
		})
		// Keep all whale elements in bucket
		items = items[whalesAllowed:]
		itemsAllowed -= whalesAllowed
	}
	// Old algorithm
	// sf *= 2 // half of space is occupied by whales now. TODO - we can be more exact here, make permutations and take as many elements as we need, saving lots of rnd calls
	// for _, v := range items {
	//	if rnd.Float64()*sf < 1 {
	//		v.Item.SF = sf // communicate selected factor to next step of processing
	//		continue
	//	}
	//	delete(bucket.MultiItems, v.Key)
	// }
	// remove from items everything we want to keep
	for ; itemsAllowed > 0; itemsAllowed-- {
		l := len(items)
		index := rnd.Intn(l)
		items[index] = items[l-1]
		items = items[:l-1]
	}
	// so this steps keeps in bucket.MultiItems items we removed above
	for _, v := range items {
		delete(bucket.MultiItems, v.Key)
	}
	return true
}

// relatively new, can contain disastrous bugs, turned off for now
func (s *Shard) sampleBucketWithGroups(bucket *data_model.MetricsBucket, rnd *rand.Rand) map[int32]float64 { // returns sample factors
	// Same algorithm as in aggregator, but instead of inserting selected, we remove items which were not selected by sampling algorithm
	metricsMap := map[int32]*data_model.SamplingMetric{}
	groupsMap := map[int32]*data_model.SamplingGroup{}
	var groupsList []*data_model.SamplingGroup
	totalItemsSize := 0
	var remainingGroupWeight int64

	for k, item := range bucket.MultiItems {
		whaleWeight := item.FinishStringTop(s.config.StringTopCountSend) // all excess items are baked into Tail
		accountMetric := k.Metric
		sz := k.TLSizeEstimate(bucket.Time) + item.TLSizeEstimate()
		if k.Metric == format.BuiltinMetricIDIngestionStatus {
			if k.Keys[1] != 0 {
				// Ingestion status and other unlimited per-metric built-ins should use its metric budget
				// So metrics are better isolated
				accountMetric = k.Keys[1]
			}
			if k.Keys[2] == format.TagValueIDSrcIngestionStatusOKCached {
				// These are so common, we have transfer optimization for them
				sz = 3 * 4 // see statshouse.ingestion_status2
			}
		}

		samplingMetric, ok := metricsMap[accountMetric]
		if !ok {
			var metricInfo *format.MetricMetaValue
			if s.statshouse.metricStorage != nil {
				metricInfo = s.statshouse.metricStorage.GetMetaMetric(accountMetric)
				if metricInfo == nil {
					metricInfo = format.BuiltinMetrics[accountMetric]
				}
			}
			samplingMetric = &data_model.SamplingMetric{
				MetricID:     accountMetric,
				MetricWeight: format.EffectiveWeightOne,
				RoundFactors: false, // default is no rounding
			}
			groupID := int32(format.BuiltinGroupIDDefault)
			if metricInfo != nil {
				samplingMetric.MetricWeight = metricInfo.EffectiveWeight
				samplingMetric.RoundFactors = metricInfo.RoundSampleFactors
				groupID = metricInfo.GroupID
			}
			samplingGroup, ok := groupsMap[groupID]
			if !ok {
				groupInfo := s.statshouse.metricStorage.GetGroup(groupID)
				samplingGroup = &data_model.SamplingGroup{
					GroupID:     groupID,
					GroupWeight: format.EffectiveWeightOne,
				}
				if groupInfo != nil {
					samplingGroup.GroupWeight = groupInfo.EffectiveWeight
				}
				groupsMap[groupID] = samplingGroup
				groupsList = append(groupsList, samplingGroup)
				remainingGroupWeight += samplingGroup.GroupWeight
			}
			metricsMap[accountMetric] = samplingMetric
			samplingGroup.MetricList = append(samplingGroup.MetricList, samplingMetric)
			samplingGroup.SumMetricWeight += samplingMetric.MetricWeight
		}
		samplingMetric.SumSize += int64(sz)
		samplingMetric.Group.SumSize += int64(sz)
		samplingMetric.Items = append(samplingMetric.Items, data_model.SamplingMultiItemPair{Key: k, Item: item, WhaleWeight: whaleWeight})
		totalItemsSize += sz
	}

	numShards := s.statshouse.NumShardReplicas()
	remainingGroupBudget := int64((s.config.SampleBudget + numShards - 1) / numShards)
	if remainingGroupBudget <= 0 { // if happens, will lead to divide by zero below, so we add cheap protection
		remainingGroupBudget = 1
	}
	if remainingGroupBudget > data_model.MaxUncompressedBucketSize/2 { // Algorithm is not exact
		remainingGroupBudget = data_model.MaxUncompressedBucketSize / 2
	}

	sort.Slice(groupsList, func(i, j int) bool {
		// comparing rational numbers
		return groupsList[i].SumSize*groupsList[j].GroupWeight < groupsList[j].SumSize*groupsList[i].GroupWeight
	})

	// We presume we have only a few groups, so reporting sizes are over budget and itself not sampled
	reportGroupSampling := func(sizeBefore int64, sizeAfter int64, groupID int32, fitTag int32) {
		key := data_model.Key{Metric: format.BuiltinMetricIDGroupSizeBeforeSampling, Keys: [16]int32{0, s.statshouse.componentTag, groupID, fitTag}}
		mi := data_model.MapKeyItemMultiItem(&bucket.MultiItems, key, s.config.StringTopCapacity, nil)
		mi.Tail.Value.AddValueCounterHost(float64(sizeBefore), 1, 0)
		key = data_model.Key{Metric: format.BuiltinMetricIDGroupSizeAfterSampling, Keys: [16]int32{0, s.statshouse.componentTag, groupID, fitTag}}
		mi = data_model.MapKeyItemMultiItem(&bucket.MultiItems, key, s.config.StringTopCapacity, nil)
		mi.Tail.Value.AddValueCounterHost(float64(sizeAfter), 1, 0)
	}
	gpos := 0
	for ; gpos < len(groupsList) && groupsList[gpos].SumSize*remainingGroupWeight <= remainingGroupBudget*groupsList[gpos].GroupWeight; gpos++ { // statIdCount <= totalBudget/remainedStats
		// No sampling for this group
		samplingGroup := groupsList[gpos]

		remainingGroupBudget -= samplingGroup.SumSize
		remainingGroupWeight -= samplingGroup.GroupWeight

		reportGroupSampling(samplingGroup.SumSize, samplingGroup.SumSize, samplingGroup.GroupID, format.TagValueIDGroupSizeSamplingFit)
	}
	sampleFactors := map[int32]float64{}
	for ; gpos < len(groupsList); gpos++ {
		samplingGroup := groupsList[gpos]

		remainingWeight := samplingGroup.SumMetricWeight
		remainingBudget := remainingGroupBudget * samplingGroup.GroupWeight / remainingGroupWeight // lose up to byte of budget per iteration, this is ok for simplicity
		metricsList := samplingGroup.MetricList

		reportGroupSampling(samplingGroup.SumSize, remainingBudget, samplingGroup.GroupID, format.TagValueIDGroupSizeSamplingSampled)

		sort.Slice(metricsList, func(i, j int) bool {
			// comparing rational numbers
			return metricsList[i].SumSize*metricsList[j].MetricWeight < metricsList[j].SumSize*metricsList[i].MetricWeight
		})

		pos := 0
		for ; pos < len(metricsList) && metricsList[pos].SumSize*remainingWeight <= remainingBudget*metricsList[pos].MetricWeight; pos++ { // statIdCount <= totalBudget/remainedStats
			samplingMetric := metricsList[pos]
			// No sampling for this stat - do not add to samplingThresholds
			remainingBudget -= samplingMetric.SumSize
			remainingWeight -= samplingMetric.MetricWeight
			// Keep all elements in bucket
		}
		for ; pos < len(metricsList); pos++ {
			samplingMetric := metricsList[pos]
			sf := float64(samplingMetric.SumSize*remainingWeight) / float64(samplingMetric.MetricWeight*remainingBudget)
			if sampleStatItem(bucket, samplingMetric, sf, rnd) {
				sampleFactors[samplingMetric.MetricID] = sf
			}
		}
	}
	// TODO - collect more information about sampling of individual metrics
	return sampleFactors
}

func (s *Shard) sendToSenders(bucket *data_model.MetricsBucket, missedSeconds uint32, sampleFactors map[int32]float64) {
	cbd, err := s.compressBucket(bucket, missedSeconds, sampleFactors)

	if err != nil {
		s.statshouse.statErrorsDiskCompressFailed.AddValueCounter(0, 1)
		s.client.Client.Logf("Internal Error: Failed to compress bucket %v for shard %d bucket %d", err, s.ShardReplicaNum, bucket.Time)
		return
	}
	if s.config.SaveSecondsImmediately {
		s.diskCachePutWithLog(cbd) // Continue sending anyway on error
	}
	select {
	case s.BucketsToSend <- cbd:
	default:
		// s.client.Client.Logf("Slowdown: Buckets Channel full for shard %d. Moving bucket %d to Historic Conveyor", s.ShardReplicaNum, cbd.time)
		if !s.config.SaveSecondsImmediately {
			s.diskCachePutWithLog(cbd)
		}
		s.appendHistoricBucketsToSend(cbd)
	}
}

func (s *Shard) compressBucket(bucket *data_model.MetricsBucket, missedSeconds uint32, sampleFactors map[int32]float64) (compressedBucketData, error) {
	cb := compressedBucketData{time: bucket.Time}

	sb := sourceBucketToTL(bucket, s.perm, sampleFactors)
	sb.MissedSeconds = missedSeconds

	w, err := sb.WriteBoxed(nil)
	if err != nil {
		return cb, fmt.Errorf("sb.WriteBoxed failed: %w", err)
	}
	compressed := make([]byte, 4+lz4.CompressBlockBound(len(w))) // Framing - first 4 bytes is original size
	cs, err := lz4.CompressBlockHC(w, compressed[4:], 0)
	if err != nil {
		return cb, fmt.Errorf("CompressBlockHC failed: %w", err)
	}
	binary.LittleEndian.PutUint32(compressed, uint32(len(w)))
	if cs >= len(w) { // does not compress (rare for large buckets, so copy is not a problem)
		compressed = append(compressed[:4], w...)
		cb.data = compressed
	} else {
		cb.data = compressed[:4+cs]
	}
	return cb, nil
}

func (s *Shard) sendSourceBucketCompressed(ctx context.Context, cbd compressedBucketData, historic bool, spare bool, ret *[]byte) error {
	extra := rpc.InvokeReqExtra{FailIfNoConnection: true}
	args := tlstatshouse.SendSourceBucket2Bytes{
		Time:            cbd.time,
		BuildCommit:     []byte(build.Commit()),
		BuildCommitDate: s.statshouse.commitDateTag,
		BuildCommitTs:   s.statshouse.commitTimestamp,
		QueueSizeDisk:   math.MaxInt32,
		QueueSizeMemory: math.MaxInt32,
		OriginalSize:    int32(binary.LittleEndian.Uint32(cbd.data)),
		CompressedData:  cbd.data[4:],
	}
	s.fillProxyHeaderBytes(&args.FieldsMask, &args.Header)
	args.SetHistoric(historic)
	args.SetSpare(spare)

	sizeMem := s.HistoricBucketsDataSizeMemory()
	if sizeMem < math.MaxInt32 {
		args.QueueSizeMemory = int32(sizeMem)
	}
	sizeDisk := s.HistoricBucketsDataSizeDisk()
	if sizeDisk < math.MaxInt32 {
		args.QueueSizeDisk = int32(sizeDisk)
	}
	if s.client.Address != "" { // Skip sending to "empty" shards. Provides fast way to answer "what if there were more shards" question
		if err := s.client.SendSourceBucket2Bytes(ctx, args, &extra, ret); err != nil {
			return err
		}
	}
	return nil
}

func (s *Shard) sendRecent(cbd compressedBucketData) bool {
	now := time.Now()
	nowUnix := uint32(now.Unix())
	if cbd.time+data_model.MaxShortWindow+data_model.FutureWindow < nowUnix { // Not bother sending, will receive error anyway
		return false
	}
	// bucket.time 12 is finished when now() is 13, if spread delay is 0.2 sec, we will not send it earlier than 13.2
	spreadDelay := now.Add(s.timeSpreadDelta).Sub(time.Unix(int64(cbd.time+1), 0))
	if spreadDelay > 0 {
		time.Sleep(spreadDelay)
	}
	var resp []byte
	// Motivation - can save sending request, as rpc.Client checks for timeout before sending
	ctx, cancel := context.WithDeadline(context.Background(), now.Add(time.Second*(data_model.MaxConveyorDelay+data_model.AgentAggregatorDelay)))
	defer cancel()
	var err error
	if s.alive.Load() {
		err = s.sendSourceBucketCompressed(ctx, cbd, false, false, &resp)
		s.recordSendResult(err == nil)
	} else {
		spare := SpareShardReplica(s.ShardReplicaNum, cbd.time)
		if !s.statshouse.Shards[spare].alive.Load() {
			return false
		}
		err = s.statshouse.Shards[spare].sendSourceBucketCompressed(ctx, cbd, false, true, &resp)
	}
	if err != nil {
		if !data_model.SilentRPCError(err) {
			s.client.Client.Logf("Send Error: s.client.Do returned error %v, moving bucket %d to historic conveyor for shard %d", err, cbd.time, s.ShardReplicaNum)
		}
		return false
	}
	return true
}

func (s *Shard) goSendRecent() {
	for cbd := range s.BucketsToSend {
		if s.sendRecent(cbd) {
			s.diskCacheEraseWithLog(cbd.time, "after sending")
		} else {
			if !s.config.SaveSecondsImmediately {
				s.diskCachePutWithLog(cbd)
			}
			s.appendHistoricBucketsToSend(cbd)
		}
	}
}

func (s *Shard) sendHistoric(cbd compressedBucketData, scratchPad *[]byte) {
	var err error

	for {
		nowUnix := uint32(time.Now().Unix())
		if s.checkOutOfWindow(nowUnix, cbd.time) { // should check in for because time passes with attempts
			return
		}
		if len(cbd.data) == 0 { // Read once, if needed, but only after checking timestamp
			if s.statshouse.diskCache == nil {
				s.statshouse.statErrorsDiskReadNotConfigured.AddValueCounter(0, 1)
				return // No data and no disk storage configured, alas
			}
			if cbd.data, err = s.statshouse.diskCache.GetBucket(s.ShardReplicaNum, cbd.time, scratchPad); err != nil {
				s.client.Client.Logf("Disk Error: diskCache.GetBucket returned error %v for shard %d bucket %d", err, s.ShardReplicaNum, cbd.time)
				s.statshouse.statErrorsDiskRead.AddValueCounter(0, 1)
				return
			}
			if len(cbd.data) < 4 {
				s.client.Client.Logf("Disk Error: diskCache.GetBucket returned compressed bucket data with size %d for shard %d bucket %d", len(cbd.data), s.ShardReplicaNum, cbd.time)
				s.statshouse.statErrorsDiskRead.AddValueCounter(0, 1)
				s.diskCacheEraseWithLog(cbd.time, "after reading tiny")
				return
			}
		}
		var resp []byte

		// We use infinite timeout, because otherwise, if aggregator is busy, source will send the same bucket again and again, inflating amount of data
		// But we set FailIfNoConnection to switch to fallback immediately
		if s.alive.Load() {
			err = s.sendSourceBucketCompressed(context.Background(), cbd, true, false, &resp)
		} else {
			spare := SpareShardReplica(s.ShardReplicaNum, cbd.time)
			if !s.statshouse.Shards[spare].alive.Load() {
				time.Sleep(10 * time.Second) // TODO - better idea?
				continue
			}
			err = s.statshouse.Shards[spare].sendSourceBucketCompressed(context.Background(), cbd, true, true, &resp)
		}
		if err != nil {
			if !data_model.SilentRPCError(err) {
				s.client.Client.Logf("Send Error: s.client.Do returned error %v for historic bucket %d for shard %d, trying again", err, cbd.time, s.ShardReplicaNum)
			}
			time.Sleep(time.Second) // TODO - better idea?
			continue
		}
		s.diskCacheEraseWithLog(cbd.time, "after sending historic")
		break
	}
}

func (s *Shard) diskCachePutWithLog(cbd compressedBucketData) {
	if s.statshouse.diskCache == nil {
		return
	}
	if err := s.statshouse.diskCache.PutBucket(s.ShardReplicaNum, cbd.time, cbd.data); err != nil {
		s.client.Client.Logf("Disk Error: diskCache.PutBucket returned error %v for shard %d bucket %d", err, s.ShardReplicaNum, cbd.time)
		s.statshouse.statErrorsDiskWrite.AddValueCounter(0, 1)
	}
}

func (s *Shard) diskCacheEraseWithLog(time uint32, place string) {
	if s.statshouse.diskCache == nil {
		return
	}
	if err := s.statshouse.diskCache.EraseBucket(s.ShardReplicaNum, time); err != nil {
		s.client.Client.Logf("Disk Error: diskCache.EraseBucket returned error %v for shard %d %s bucket %d", err, s.ShardReplicaNum, place, time)
		s.statshouse.statErrorsDiskErase.AddValueCounter(0, 1)
	}
}

func (s *Shard) appendHistoricBucketsToSend(cbd compressedBucketData) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.HistoricBucketsDataSize+len(cbd.data) > data_model.MaxHistoricBucketsMemorySize/s.statshouse.NumShardReplicas() {
		cbd.data = nil
	} else {
		s.HistoricBucketsDataSize += len(cbd.data)
	}
	s.HistoricBucketsToSend = append(s.HistoricBucketsToSend, cbd)
	s.cond.Signal()
}

func (s *Shard) readHistoricSecondLocked() {
	if s.statshouse.diskCache == nil {
		return
	}
	sec, ok := s.statshouse.diskCache.ReadNextTailSecond(s.ShardReplicaNum)
	if !ok {
		return
	}
	s.HistoricBucketsToSend = append(s.HistoricBucketsToSend, compressedBucketData{time: sec}) // HistoricBucketsDataSize does not change
}

func (s *Shard) popOldestHistoricSecondLocked() compressedBucketData {
	// Sending the oldest known historic buckets is very important for "herding" strategy
	// Even tiny imperfectness in sorting explodes number of inserts aggregator makes
	pos := 0
	for i, e := range s.HistoricBucketsToSend {
		if e.time < s.HistoricBucketsToSend[pos].time {
			pos = i
		}
	}
	cbd := s.HistoricBucketsToSend[pos]
	s.HistoricBucketsToSend[pos] = s.HistoricBucketsToSend[len(s.HistoricBucketsToSend)-1]
	s.HistoricBucketsToSend = s.HistoricBucketsToSend[:len(s.HistoricBucketsToSend)-1]

	s.HistoricBucketsDataSize -= len(cbd.data)
	if s.HistoricBucketsDataSize < 0 {
		panic("HistoricBucketsDataSize < 0")
	}
	return cbd
}

func (s *Shard) checkOutOfWindow(nowUnix uint32, timestamp uint32) bool {
	if nowUnix >= data_model.MaxHistoricWindow && timestamp < nowUnix-data_model.MaxHistoricWindow { // Not bother sending, will receive error anyway
		s.client.Client.Logf("Send Disaster: Bucket %d for shard %d does not fit into full admission window (now is %d), throwing out", timestamp, s.ShardReplicaNum, nowUnix)
		s.statshouse.statLongWindowOverflow.AddValueCounter(float64(nowUnix)-float64(timestamp), 1)

		s.diskCacheEraseWithLog(timestamp, "after throwing out historic")
		return true
	}
	return false
}

func (s *Shard) goSendHistoric() {
	var scratchPad []byte
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		s.readHistoricSecondLocked() // if there were unread seconds from disk cache tail, reads one last historic second
		for len(s.HistoricBucketsToSend) == 0 {
			s.cond.Wait()
		}
		cbd := s.popOldestHistoricSecondLocked()
		s.mu.Unlock()
		s.sendHistoric(cbd, &scratchPad)
		s.mu.Lock()
	}
}

func (s *Shard) goEraseHistoric() {
	// When all senders are in infinite wait, buckets must still be erased
	// Also, we delete buckets when disk limit is reached. We cannot promise strict limit, because disk cache design is very loose.
	s.mu.Lock()
	defer s.mu.Unlock()
	diskLimit := s.config.MaxHistoricDiskSize / int64(s.statshouse.NumShardReplicas())
	for {
		s.readHistoricSecondLocked() // if there were unread seconds from disk cache tail, reads one last historic second
		for len(s.HistoricBucketsToSend) == 0 {
			s.cond.Wait()
		}
		cbd := s.popOldestHistoricSecondLocked()
		s.mu.Unlock()

		nowUnix := uint32(time.Now().Unix())

		diskUsed := s.HistoricBucketsDataSizeDisk()
		// diskUsed is sum of file size, and will not shrink until all seconds in a file are deleted
		// seconds held by historic seconds will not be deleted, because they are not poped by popOldestHistoricSecondLocked aboeve,
		// if all possible seconds are deleted, but we are still over limit, this goroutine will block on s.cond.Wait() above
		if diskUsed > diskLimit {
			s.client.Client.Logf("Send Disaster: Bucket %d for shard %d (now is %d) violates disk size limit %d (%d used), throwing out", cbd.time, s.ShardReplicaNum, nowUnix, diskLimit, diskUsed)
			s.statshouse.statLongWindowOverflow.AddValueCounter(float64(nowUnix)-float64(cbd.time), 1)

			s.diskCacheEraseWithLog(cbd.time, "after throwing out historic, due to disk limit")
			time.Sleep(200 * time.Millisecond) // Deleting 5 seconds per second is good for us, and does not spin CPU too much
			s.mu.Lock()
			continue
		}

		if s.checkOutOfWindow(nowUnix, cbd.time) { // should check, because time passes with attempts
			s.mu.Lock()
			continue
		}

		s.appendHistoricBucketsToSend(cbd) // As we consumed cond state, signal inside that func is required

		time.Sleep(60 * time.Second) // rare, because only  fail-safe against all goSendHistoric blocking in infinite sends
		s.mu.Lock()
	}
}
