// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package agent

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/pierrec/lz4"

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"

	"pgregory.net/rand"
)

// If clients want less jitter (they want), they should send data quickly after end pf calendar second.
// Agent has small window (for example, half a second) when it accepts data for previous second with zero sampling penalty.
func (s *Shard) flushBuckets(now time.Time) {
	// called several times/sec only, but must still be fast, so we do not lock shard for too long
	s.mu.Lock()
	defer s.mu.Unlock()
	if nowUnix := uint32(now.Unix()); nowUnix > s.addBuiltInsTime {
		s.addBuiltInsTime = nowUnix
		s.addBuiltInsLocked(nowUnix) // account to the current second. This is debatable.
	}
	// We want PreprocessingBucketTime to strictly increase, so that historic conveyor is strictly ordered

	currentTime := uint32(now.Add(-data_model.AgentWindow).Unix())
	wasCurrentBucketTime := s.CurrentBuckets[1].Time
	if s.PreprocessingBuckets == nil && currentTime > wasCurrentBucketTime {
		// if we cannot flush 1-second resolution, we cannot flush any higher resolution
		// 1. flush current buckets into future queue (at least 1sec resolution, sometimes more, depends on rounded second)
		// 2. move next buckets into current buckets
		// 3. add new next buckets. There could appear time gap between current and next buckets if time jumped.
		for r, b := range s.CurrentBuckets {
			if r != format.AllowedResolution(r) {
				continue
			}
			currentTimeRounded := (currentTime / uint32(r)) * uint32(r)
			// process current bucket once in a resolution
			// for 60 sec resolution this will be false once every 60 sec
			if currentTimeRounded <= b.Time {
				continue
			}
			if !b.Empty() {
				futureQueuePos := (b.Time + uint32(r)) % 60
				s.FutureQueue[futureQueuePos] = append(s.FutureQueue[futureQueuePos], b)
			}
			s.CurrentBuckets[r] = s.NextBuckets[r]
			s.NextBuckets[r] = &data_model.MetricsBucket{Time: currentTimeRounded + uint32(r), Resolution: r}
		}
		// we put second into next second future queue position,
		// same for higher resolutions, so first minute shard is sent togehter with 59-th normal second.
		s.PreprocessingBuckets = s.FutureQueue[(wasCurrentBucketTime+1)%60]
		s.FutureQueue[(wasCurrentBucketTime+1)%60] = nil

		// Due to !b.Empty() optimization above, if no data is collected, nothing is in FutureQueue
		// As we use PreprocessingBuckets as flag, it must be not nil so that processing and sending is performed
		// for each contributor every second
		if s.PreprocessingBuckets == nil {
			s.PreprocessingBuckets = []*data_model.MetricsBucket{}
		}

		s.PreprocessingBucketTime = wasCurrentBucketTime

		s.condPreprocess.Signal()
	}
	// now after CurrentBuckets flushed, we want to move NextBuckets timestamp
	// if conveyor is stuck, so that data from previous seconds is still kept there,
	// but we considere this bucket to correspond to the unixNow point of time.
	// this is important because we clamp future timestamps by content of NextBuckets timestamps
	for r, b := range s.NextBuckets {
		if r != format.AllowedResolution(r) {
			continue
		}
		currentTimeRounded := (currentTime / uint32(r)) * uint32(r)
		if currentTimeRounded+uint32(r) <= b.Time {
			continue
		}
		if r == 1 { // Add metric for missed second here only once
			key := data_model.Key{
				Timestamp: b.Time,
				Metric:    format.BuiltinMetricIDTimingErrors,
				Keys:      [16]int32{0, format.TagValueIDTimingMissedSecondsAgent},
			}
			mi := data_model.MapKeyItemMultiItem(&b.MultiItems, key, s.config.StringTopCapacity, nil, nil)
			mi.Tail.AddValueCounterHost(float64(currentTimeRounded+uint32(r)-b.Time), 1, 0) // values record jumps f more than 1 second
		}
		b.Time = currentTimeRounded + uint32(r)
	}
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

func sourceBucketToTL(bucket *data_model.MetricsBucket, perm []int, sampleFactors []tlstatshouse.SampleFactor) tlstatshouse.SourceBucket2 {
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
			sb.IngestionStatusOk2 = append(sb.IngestionStatusOk2, tlstatshouse.IngestionStatus2{Env: k.Keys[0], Metric: k.Keys[1], Value: float32(v.Tail.Value.Counter * v.SF)})
			continue
		}
		item := k.TLMultiItemFromKey(bucket.Time)
		v.Tail.MultiValueToTL(&item.Tail, v.SF, &item.FieldsMask, &marshalBuf)
		sizeBuf = item.Write(sizeBuf[:0])
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
			sizeBuf = el.Write(sizeBuf[:0])
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

	sb.SampleFactors = append(sb.SampleFactors, sampleFactors...)

	sbSizeCalc := tlstatshouse.SourceBucket2{SampleFactors: sb.SampleFactors}
	sizeBuf = sbSizeCalc.Write(sizeBuf[:0])
	addSizeByTypeMetric(&sb, format.TagValueIDSizeSampleFactors, len(sizeBuf))

	sbSizeCalc = tlstatshouse.SourceBucket2{IngestionStatusOk: sb.IngestionStatusOk, IngestionStatusOk2: sb.IngestionStatusOk2}
	sizeBuf = sbSizeCalc.Write(sizeBuf[:0])
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

		bucket := &data_model.MetricsBucket{Time: s.PreprocessingBucketTime}
		s.PreprocessingBucketTime = 0
		s.mu.Unlock()

		// We uniformly split buckets with resolution > 1 into second buckets to process them fairly
		// this is done outside of lock
		buckets = s.partitionBucketsByResolution(buckets, bucket.Time)

		s.mergeBuckets(bucket, buckets) // TODO - why we merge instead of passing array to sampleBucket
		sampleFactors := s.sampleBucket(bucket, rnd)
		s.sendToSenders(bucket, sampleFactors)

		s.mu.Lock()
		for rs, bs := range s.carryBuckets {
			if len(bs) == 0 {
				continue
			}
			s.FutureQueue[rs] = append(s.FutureQueue[rs], bs...)
			s.carryBuckets[rs] = bs[:0]
		}
	}
}

func (s *Shard) mergeBuckets(bucket *data_model.MetricsBucket, buckets []*data_model.MetricsBucket) {
	s.mu.Lock()
	stringTopCapacity := s.config.StringTopCapacity
	s.mu.Unlock()
	for _, b := range buckets { // optimization to merge into the largest map
		if len(b.MultiItems) > len(bucket.MultiItems) {
			b.MultiItems, bucket.MultiItems = bucket.MultiItems, b.MultiItems
		}
	}
	for _, b := range buckets {
		for k, v := range b.MultiItems {
			mi := data_model.MapKeyItemMultiItem(&bucket.MultiItems, k, stringTopCapacity, nil, nil)
			mi.Merge(v)
		}
	}
}

func (s *Shard) sampleBucket(bucket *data_model.MetricsBucket, rnd *rand.Rand) []tlstatshouse.SampleFactor {
	s.mu.Lock()
	config := s.config
	s.mu.Unlock()

	sampler := data_model.NewSampler(len(bucket.MultiItems), data_model.SamplerConfig{
		ModeAgent:        !config.DisableNoSampleAgent,
		SampleNamespaces: config.SampleNamespaces,
		SampleGroups:     config.SampleGroups,
		SampleKeys:       config.SampleKeys,
		Meta:             s.agent.metricStorage,
		Rand:             rnd,
		DiscardF:         func(key data_model.Key, _ *data_model.MultiItem, _ uint32) { delete(bucket.MultiItems, key) }, // remove from map
	})
	for k, item := range bucket.MultiItems {
		whaleWeight := item.FinishStringTop(config.StringTopCountSend) // all excess items are baked into Tail
		accountMetric := k.Metric
		sz := k.TLSizeEstimate(bucket.Time) + item.TLSizeEstimate()
		if k.Metric == format.BuiltinMetricIDIngestionStatus {
			if k.Keys[1] != 0 {
				// Ingestion status and other unlimited per-metric built-ins should use its metric budget
				// So metrics are better isolated
				accountMetric = k.Keys[1]
				whaleWeight = 0 // ingestion statuses do not compete for whale status
			}
			if k.Keys[2] == format.TagValueIDSrcIngestionStatusOKCached {
				// These are so common, we have transfer optimization for them
				sz = 3 * 4 // see statshouse.ingestion_status2
			}
		}
		sampler.Add(data_model.SamplingMultiItemPair{
			Key:         k,
			Item:        item,
			WhaleWeight: whaleWeight,
			Size:        sz,
			MetricID:    accountMetric,
		})
	}
	numShards := s.agent.NumShards()
	remainingBudget := int64((config.SampleBudget + numShards - 1) / numShards)
	if remainingBudget > data_model.MaxUncompressedBucketSize/2 { // Algorithm is not exact
		remainingBudget = data_model.MaxUncompressedBucketSize / 2
	}
	var samplerStat data_model.SamplerStatistics
	sampler.Run(remainingBudget, &samplerStat)
	sampleFactors := make([]tlstatshouse.SampleFactor, 0, samplerStat.Count)
	for _, s := range samplerStat.Steps {
		sampleFactors = s.GetSampleFactors(sampleFactors)
	}
	for k, v := range samplerStat.Items {
		// keep bytes
		key := data_model.Key{Metric: format.BuiltinMetricIDSrcSamplingSizeBytes, Keys: [16]int32{0, s.agent.componentTag, format.TagValueIDSamplingDecisionKeep, k[0], k[1], k[2]}}
		mi := data_model.MapKeyItemMultiItem(&bucket.MultiItems, key, config.StringTopCapacity, nil, nil)
		mi.Tail.Value.Merge(&v.SumSizeKeep)
		// discard bytes
		key = data_model.Key{Metric: format.BuiltinMetricIDSrcSamplingSizeBytes, Keys: [16]int32{0, s.agent.componentTag, format.TagValueIDSamplingDecisionDiscard, k[0], k[1], k[2]}}
		mi = data_model.MapKeyItemMultiItem(&bucket.MultiItems, key, config.StringTopCapacity, nil, nil)
		mi.Tail.Value.Merge(&v.SumSizeDiscard)
	}
	// report budget used
	budgetKey := data_model.Key{Metric: format.BuiltinMetricIDSrcSamplingBudget, Keys: [16]int32{0, s.agent.componentTag}}
	budgetItem := data_model.MapKeyItemMultiItem(&bucket.MultiItems, budgetKey, config.StringTopCapacity, nil, nil)
	budgetItem.Tail.Value.AddValue(float64(remainingBudget))
	for k, v := range samplerStat.Budget {
		key := data_model.Key{Metric: format.BuiltinMetricIDSrcSamplingGroupBudget, Keys: [16]int32{0, s.agent.componentTag, k[0], k[1]}}
		item := data_model.MapKeyItemMultiItem(&bucket.MultiItems, key, config.StringTopCapacity, nil, nil)
		item.Tail.Value.AddValue(v)
	}
	// metric count
	key := data_model.Key{Metric: format.BuiltinMetricIDSrcSamplingMetricCount, Keys: [16]int32{0, s.agent.componentTag}}
	mi := data_model.MapKeyItemMultiItem(&bucket.MultiItems, key, config.StringTopCapacity, nil, nil)
	mi.Tail.Value.AddValueCounterHost(float64(len(samplerStat.Metrics)), 1, 0)
	return sampleFactors
}

func (s *Shard) sendToSenders(bucket *data_model.MetricsBucket, sampleFactors []tlstatshouse.SampleFactor) {
	cbd, err := s.compressBucket(bucket, sampleFactors)

	if err != nil {
		s.agent.statErrorsDiskCompressFailed.AddValueCounter(0, 1)
		s.agent.logF("Internal Error: Failed to compress bucket %v for shard %d bucket %d",
			err, s.ShardKey, bucket.Time)
		return
	}
	s.mu.Lock()
	saveSecondsImmediately := s.config.SaveSecondsImmediately
	s.mu.Unlock()
	if saveSecondsImmediately {
		s.diskCachePutWithLog(cbd) // Continue sending anyway on error
	}
	select {
	case s.BucketsToSend <- compressedBucketDataOnDisk{compressedBucketData: cbd, onDisk: saveSecondsImmediately}:
	default:
		// s.client.Client.Logf("Slowdown: Buckets Channel full for shard %d replica %d (shard-replica %d). Moving bucket %d to Historic Conveyor",
		// 	s.ShardKey, s.ReplicaKey, s.ShardReplicaNum, cbd.time)
		if !saveSecondsImmediately {
			s.diskCachePutWithLog(cbd)
		}
		s.appendHistoricBucketsToSend(cbd)
	}
}

func (s *Shard) compressBucket(bucket *data_model.MetricsBucket, sampleFactors []tlstatshouse.SampleFactor) (compressedBucketData, error) {
	cb := compressedBucketData{time: bucket.Time}

	sb := sourceBucketToTL(bucket, s.perm, sampleFactors)

	w := sb.WriteBoxed(nil)
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

	shardReplica, spare := s.agent.getShardReplicaForSeccnd(s.ShardNum, cbd.time)
	if shardReplica == nil {
		return false
	}
	err = shardReplica.sendSourceBucketCompressed(ctx, cbd, false, spare, &resp, s)
	if !spare {
		shardReplica.recordSendResult(!isShardDeadError(err))
	}
	if err != nil {
		if !data_model.SilentRPCError(err) {
			shardReplica.stats.recentSendFailed.Add(1)
			s.agent.logF("Send Error: s.client.Do returned error %v, moving bucket %d to historic conveyor for shard %d",
				err, cbd.time, s.ShardKey)
		} else {
			shardReplica.stats.recentSendSkip.Add(1)
		}
		return false
	}
	if resp != nil {
		respS := string(resp)
		if respS != "Dummy historic result" {
			s.agent.logF("Send bucket returned: \"%s\"", respS)
		}
	}
	shardReplica.stats.recentSendSuccess.Add(1)
	return true
}

func (s *Shard) goSendRecent() {
	for cbd := range s.BucketsToSend {
		if s.sendRecent(cbd.compressedBucketData) {
			s.diskCacheEraseWithLog(cbd.time, "after sending")
		} else {
			if !cbd.onDisk {
				s.diskCachePutWithLog(cbd.compressedBucketData)
			}
			s.appendHistoricBucketsToSend(cbd.compressedBucketData)
		}
	}
}

func (s *Shard) sendHistoric(cbd compressedBucketData, scratchPad *[]byte) {
	var err error

	for {
		nowUnix := uint32(time.Now().Unix())
		if s.checkOutOfWindow(nowUnix, cbd.time) { // should check in for because time passes with attempts
			s.HistoricOutOfWindowDropped.Add(1)
			return
		}
		if len(cbd.data) == 0 { // Read once, if needed, but only after checking timestamp
			if s.agent.diskBucketCache == nil {
				s.agent.statErrorsDiskReadNotConfigured.AddValueCounter(0, 1)
				return // No data and no disk storage configured, alas
			}
			if cbd.data, err = s.agent.diskBucketCache.GetBucket(s.ShardNum, cbd.time, scratchPad); err != nil {
				s.agent.logF("Disk Error: diskCache.GetBucket returned error %v for shard %d bucket %d",
					err, s.ShardKey, cbd.time)
				s.agent.statErrorsDiskRead.AddValueCounter(0, 1)
				return
			}
			if len(cbd.data) < 4 {
				s.agent.logF("Disk Error: diskCache.GetBucket returned compressed bucket data with size %d for shard %d bucket %d",
					len(cbd.data), s.ShardKey, cbd.time)
				s.agent.statErrorsDiskRead.AddValueCounter(0, 1)
				s.diskCacheEraseWithLog(cbd.time, "after reading tiny")
				return
			}
		}
		var resp []byte

		shardReplica, spare := s.agent.getShardReplicaForSeccnd(s.ShardNum, cbd.time)
		if shardReplica == nil {
			time.Sleep(10 * time.Second) // TODO - better idea?
			s.agent.logF("both historic shards are dead, shard %d, time %d, %v", s.ShardKey, cbd.time, err)
			continue
		}
		// We use infinite timeout, because otherwise, if aggregator is busy, source will send the same bucket again and again, inflating amount of data
		// But we set FailIfNoConnection to switch to fallback immediately
		err = shardReplica.sendSourceBucketCompressed(context.Background(), cbd, true, spare, &resp, s)
		if err != nil {
			if !data_model.SilentRPCError(err) {
				shardReplica.stats.historicSendFailed.Add(1)
			} else {
				shardReplica.stats.historicSendSkip.Add(1)
			}
			time.Sleep(time.Second) // TODO - better idea?
			continue
		}
		shardReplica.stats.historicSendSuccess.Add(1)
		s.diskCacheEraseWithLog(cbd.time, "after sending historic")
		break
	}
}

func (s *Shard) diskCachePutWithLog(cbd compressedBucketData) {
	if s.agent.diskBucketCache == nil {
		return
	}
	// Motivation - we want to set limit dynamically.
	// Also, if limit is set to 0, we want to gradually erase all seconds.
	// That's why we create diskCache always
	s.mu.Lock()
	maxHistoricDiskSize := s.config.MaxHistoricDiskSize
	s.mu.Unlock()
	if maxHistoricDiskSize <= 0 {
		return
	}
	if err := s.agent.diskBucketCache.PutBucket(s.ShardNum, cbd.time, cbd.data); err != nil {
		s.agent.logF("Disk Error: diskCache.PutBucket returned error %v for shard %d bucket %d",
			err, s.ShardKey, cbd.time)
		s.agent.statErrorsDiskWrite.AddValueCounter(0, 1)
	}
}

func (s *Shard) diskCacheEraseWithLog(time uint32, place string) {
	if s.agent.diskBucketCache == nil {
		return
	}
	if err := s.agent.diskBucketCache.EraseBucket(s.ShardNum, time); err != nil {
		s.agent.logF("Disk Error: diskCache.EraseBucket returned error %v for shard %d %s bucket %d",
			err, s.ShardKey, place, time)
		s.agent.statErrorsDiskErase.AddValueCounter(0, 1)
	}
}

func (s *Shard) appendHistoricBucketsToSend(cbd compressedBucketData) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.HistoricBucketsDataSize+len(cbd.data) > data_model.MaxHistoricBucketsMemorySize/s.agent.NumShards() {
		cbd.data = nil
	} else {
		s.HistoricBucketsDataSize += len(cbd.data)
		s.agent.historicBucketsDataSize.Add(int64(len(cbd.data)))
	}
	s.HistoricBucketsToSend = append(s.HistoricBucketsToSend, cbd)
	s.cond.Signal()
}

func (s *Shard) readHistoricSecondLocked() {
	if s.agent.diskBucketCache == nil {
		return
	}
	sec, ok := s.agent.diskBucketCache.ReadNextTailSecond(s.ShardNum)
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
	s.agent.historicBucketsDataSize.Sub(int64(len(cbd.data)))
	if s.HistoricBucketsDataSize < 0 {
		panic("HistoricBucketsDataSize < 0")
	}
	return cbd
}

func (s *Shard) checkOutOfWindow(nowUnix uint32, timestamp uint32) bool {
	if nowUnix >= data_model.MaxHistoricWindow && timestamp < nowUnix-data_model.MaxHistoricWindow { // Not bother sending, will receive error anyway
		s.agent.logF("Send Disaster: Bucket %d for shard %d does not fit into full admission window (now is %d), throwing out",
			timestamp, s.ShardKey, nowUnix)
		s.agent.statLongWindowOverflow.AddValueCounter(float64(nowUnix)-float64(timestamp), 1)

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
	diskLimit := s.config.MaxHistoricDiskSize / int64(s.agent.NumShards())
	for {
		s.readHistoricSecondLocked() // if there were unread seconds from disk cache tail, reads one last historic second
		for len(s.HistoricBucketsToSend) == 0 {
			s.cond.Wait()
		}
		cbd := s.popOldestHistoricSecondLocked()
		s.mu.Unlock()

		nowUnix := uint32(time.Now().Unix())

		diskUsed, _ := s.HistoricBucketsDataSizeDisk()
		// diskUsed is sum of file size, and will not shrink until all seconds in a file are deleted
		// seconds held by historic seconds will not be deleted, because they are not popped by popOldestHistoricSecondLocked above,
		// if all possible seconds are deleted, but we are still over limit, this goroutine will block on s.cond.Wait() above
		if diskUsed > diskLimit {
			s.agent.logF("Send Disaster: Bucket %d for shard %d (now is %d) violates disk size limit %d (%d used), throwing out",
				cbd.time, s.ShardKey, nowUnix, diskLimit, diskUsed)
			s.agent.statDiskOverflow.AddValueCounter(float64(nowUnix)-float64(cbd.time), 1)

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

func (s *Shard) partitionBucketsByResolution(buckets []*data_model.MetricsBucket, originalTime uint32) []*data_model.MetricsBucket {
	for i := range buckets {
		if buckets[i].Resolution <= 1 {
			continue
		}
		allMultiItems := buckets[i].MultiItems
		multiItemsParts := partitionMultiItems(allMultiItems, buckets[i].Resolution)
		for p := range multiItemsParts {
			if p == 0 {
				buckets[i].Resolution = 1
				buckets[i].MultiItems = multiItemsParts[p]
				continue
			}

			futureQueuePos := (originalTime + uint32(buckets[i].Resolution) + uint32(p)) % 60
			s.carryBuckets[futureQueuePos] = append(s.carryBuckets[futureQueuePos], &data_model.MetricsBucket{
				Time:       buckets[i].Time,
				Resolution: 1,
				MultiItems: multiItemsParts[p],
			})
		}
	}
	return buckets
}

func partitionMultiItems(multiItems map[data_model.Key]*data_model.MultiItem, parts int) []map[data_model.Key]*data_model.MultiItem {
	result := make([]map[data_model.Key]*data_model.MultiItem, 0, parts)
	if parts == 1 {
		result = append(result, multiItems)
		return result
	}
	p := 0
	for k, v := range multiItems {
		if len(result) <= p {
			result = append(result, make(map[data_model.Key]*data_model.MultiItem))
		}
		result[p][k] = v
		p = (p + 1) % parts
	}
	return result
}

func isShardDeadError(err error) bool {
	if err == nil {
		return false
	}
	var rpcError rpc.Error
	if !errors.As(err, &rpcError) {
		return true
	}
	return rpcError.Code != data_model.RPCErrorMissedRecentConveyor
}
