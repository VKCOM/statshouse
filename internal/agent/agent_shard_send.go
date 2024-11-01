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
	"log"
	"sort"
	"sync"
	"time"

	"github.com/pierrec/lz4"

	"github.com/vkcom/statshouse/internal/vkgo/semaphore"

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
	wasCurrentBucketTime := s.CurrentBuckets[1][0].Time
	if len(s.BucketsToPreprocess) == 0 && currentTime > wasCurrentBucketTime {
		// if we cannot flush 1-second resolution, we cannot flush any higher resolution
		_ = s.flushSingleStep(wasCurrentBucketTime, currentTime, true)
	}
	// now after CurrentBuckets flushed, we want to move NextBuckets timestamp
	// if conveyor is stuck, so that data from previous seconds is still kept there,
	// but we consider this bucket to correspond to the unixNow point of time.
	// this is important because we clamp future timestamps by content of NextBuckets timestamps
	for r, bs := range s.NextBuckets {
		if r != format.AllowedResolution(r) {
			continue
		}
		currentTimeRounded := (currentTime / uint32(r)) * uint32(r)
		for i, b := range bs {
			if currentTimeRounded+uint32(r) <= b.Time {
				continue
			}
			if r == 1 && i == 0 { // Add metric for missed second here only once
				key := data_model.Key{
					Timestamp: b.Time,
					Metric:    format.BuiltinMetricIDTimingErrors,
					Tags:      [format.MaxTags]int32{0, format.TagValueIDTimingMissedSecondsAgent},
				}
				mi := data_model.MapKeyItemMultiItem(&b.MultiItems, key, s.config.StringTopCapacity, nil, nil)
				mi.Tail.AddValueCounterHost(s.rng, float64(currentTimeRounded+uint32(r)-b.Time), 1, 0) // values record jumps f more than 1 second
			}
			b.Time = currentTimeRounded + uint32(r)
		}
	}
}

func (s *Shard) flushSingleStep(wasCurrentBucketTime uint32, currentTime uint32, sendEmpty bool) int {
	// 1. flush current buckets into future queue (at least 1sec resolution, sometimes more, depends on rounded second)
	// 2. move next buckets into current buckets
	// 3. add new next buckets. There could appear time gap between current and next buckets if time jumped.
	for r, bs := range s.CurrentBuckets {
		if r != format.AllowedResolution(r) {
			continue
		}
		currentTimeRounded := (currentTime / uint32(r)) * uint32(r)
		for sh, b := range bs {
			if currentTimeRounded <= b.Time {
				continue
			}
			if !b.Empty() {
				// future queue pos is assigned without seams if missed seconds is 0
				futureQueuePos := (b.Time + uint32(r) + uint32(sh)) % 60
				s.FutureQueue[futureQueuePos] = append(s.FutureQueue[futureQueuePos], b)
			}
			s.CurrentBuckets[r][sh] = s.NextBuckets[r][sh]
			s.NextBuckets[r][sh] = &data_model.MetricsBucket{Time: currentTimeRounded + uint32(r), Resolution: r}
		}
	}
	// we put second into next second future queue position,
	// same for higher resolutions, so first minute shard is sent together with 59-th normal second.
	fq := s.FutureQueue[(wasCurrentBucketTime+1)%60]
	s.FutureQueue[(wasCurrentBucketTime+1)%60] = nil
	if !sendEmpty && len(fq) == 0 {
		return 0 // empty
	}
	// we wait here only when shutting dowm. During normal work we check len(chan) before calling this func
	s.BucketsToPreprocess <- preprocessorBucketData{time: wasCurrentBucketTime, buckets: fq}
	return 1 // non-empty
}

func (s *Shard) FlushAllDataSingleStep() int {
	wasCurrentBucketTime := s.CurrentBuckets[1][0].Time
	currentTime := wasCurrentBucketTime + 1
	return s.flushSingleStep(wasCurrentBucketTime, currentTime, false)
}

func (s *Shard) StopReceivingIncomingData() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopReceivingIncomingData = true
}

func (s *Shard) StopPreprocessor() {
	close(s.BucketsToPreprocess)
}

func addSizeByTypeMetric(sb *tlstatshouse.SourceBucket2, partKey int32, size int) {
	// This metric is added by source, because aggregator has no spare time for that
	k := data_model.Key{Metric: format.BuiltinMetricIDTLByteSizePerInflightType, Tags: [format.MaxTags]int32{0, partKey}}

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
		if k.Metric == format.BuiltinMetricIDIngestionStatus && k.Tags[2] == format.TagValueIDSrcIngestionStatusOKCached {
			// transfer optimization.
			sb.IngestionStatusOk2 = append(sb.IngestionStatusOk2, tlstatshouse.IngestionStatus2{Env: k.Tags[0], Metric: k.Tags[1], Value: float32(v.Tail.Value.Count() * v.SF)})
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

func (s *Shard) goPreProcess(wg *sync.WaitGroup) {
	defer wg.Done()
	rng := rand.New() // We use distinct rand so that we can use it without locking

	for pbd := range s.BucketsToPreprocess {
		bucket := &data_model.MetricsBucket{Time: pbd.time}
		// Due to !b.Empty() optimization during flushing into future queue, if no data is collected,
		// nothing is in FutureQueue. If pbd.buckets is empty, we must still do processing and sending
		// for each contributor every second.

		s.mergeBuckets(rng, bucket, pbd.buckets) // TODO - why we merge instead of passing array to sampleBucket
		sampleFactors := s.sampleBucket(bucket, rng)
		s.sendToSenders(bucket, sampleFactors)
	}
	log.Printf("Preprocessor quit")
}

func (s *Shard) mergeBuckets(rng *rand.Rand, bucket *data_model.MetricsBucket, buckets []*data_model.MetricsBucket) {
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
			mi.Merge(rng, v)
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
		whaleWeight := item.FinishStringTop(rnd, config.StringTopCountSend) // all excess items are baked into Tail
		accountMetric := k.Metric
		sz := k.TLSizeEstimate(bucket.Time) + item.TLSizeEstimate()
		if k.Metric == format.BuiltinMetricIDIngestionStatus {
			if k.Tags[1] != 0 {
				// Ingestion status and other unlimited per-metric built-ins should use its metric budget
				// So metrics are better isolated
				accountMetric = k.Tags[1]
				whaleWeight = 0 // ingestion statuses do not compete for whale status
			}
			if k.Tags[2] == format.TagValueIDSrcIngestionStatusOKCached {
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
	sampler.Run(remainingBudget)
	for _, v := range sampler.MetricGroups {
		// keep bytes
		key := data_model.Key{Metric: format.BuiltinMetricIDSrcSamplingSizeBytes, Tags: [format.MaxTags]int32{0, s.agent.componentTag, format.TagValueIDSamplingDecisionKeep, v.NamespaceID, v.GroupID, v.MetricID}}
		mi := data_model.MapKeyItemMultiItem(&bucket.MultiItems, key, config.StringTopCapacity, nil, nil)
		mi.Tail.Value.Merge(rnd, &v.SumSizeKeep)
		// discard bytes
		key = data_model.Key{Metric: format.BuiltinMetricIDSrcSamplingSizeBytes, Tags: [format.MaxTags]int32{0, s.agent.componentTag, format.TagValueIDSamplingDecisionDiscard, v.NamespaceID, v.GroupID, v.MetricID}}
		mi = data_model.MapKeyItemMultiItem(&bucket.MultiItems, key, config.StringTopCapacity, nil, nil)
		mi.Tail.Value.Merge(rnd, &v.SumSizeDiscard)
		// budget
		key = data_model.Key{Metric: format.BuiltinMetricIDSrcSamplingGroupBudget, Tags: [format.MaxTags]int32{0, s.agent.componentTag, v.NamespaceID, v.GroupID}}
		item := data_model.MapKeyItemMultiItem(&bucket.MultiItems, key, config.StringTopCapacity, nil, nil)
		item.Tail.Value.AddValue(v.Budget())
	}
	// report budget used
	budgetKey := data_model.Key{Metric: format.BuiltinMetricIDSrcSamplingBudget, Tags: [format.MaxTags]int32{0, s.agent.componentTag}}
	budgetItem := data_model.MapKeyItemMultiItem(&bucket.MultiItems, budgetKey, config.StringTopCapacity, nil, nil)
	budgetItem.Tail.Value.AddValue(float64(remainingBudget))
	// metric count
	key := data_model.Key{Metric: format.BuiltinMetricIDSrcSamplingMetricCount, Tags: [format.MaxTags]int32{0, s.agent.componentTag}}
	mi := data_model.MapKeyItemMultiItem(&bucket.MultiItems, key, config.StringTopCapacity, nil, nil)
	mi.Tail.Value.AddValueCounterHost(rnd, float64(sampler.MetricCount), 1, 0)
	return sampler.SampleFactors
}

func (s *Shard) sendToSenders(bucket *data_model.MetricsBucket, sampleFactors []tlstatshouse.SampleFactor) {
	data, err := s.compressBucket(bucket, sampleFactors)
	cbd := compressedBucketData{time: bucket.Time, data: data} // No id as not saved to disk yet
	if err != nil {
		s.agent.statErrorsDiskCompressFailed.AddValueCounter(0, 1)
		s.agent.logF("Internal Error: Failed to compress bucket %v for shard %d bucket %d",
			err, s.ShardKey, bucket.Time)
		return
	}
	s.mu.Lock() // must send under lock, otherwise s.BucketsToSend might be closed
	if s.BucketsToSend != nil {
		select {
		case s.BucketsToSend <- cbd:
			s.mu.Unlock() // now goSendRecent is responsible for saving/sending/etc.
			return
		default:
			break
		}
	}
	s.mu.Unlock()
	// s.client.Client.Logf("Slowdown: Buckets Channel full for shard %d replica %d (shard-replica %d). Moving bucket %d to Historic Conveyor",
	// 	s.ShardKey, s.ReplicaKey, s.ShardReplicaNum, cbd.time)
	cbd = s.diskCachePutWithLog(cbd) // assigns id
	s.appendHistoricBucketsToSend(cbd)
}

func (s *Shard) compressBucket(bucket *data_model.MetricsBucket, sampleFactors []tlstatshouse.SampleFactor) ([]byte, error) {
	sb := sourceBucketToTL(bucket, s.perm, sampleFactors)

	w := sb.WriteBoxed(nil)
	compressed := make([]byte, 4+lz4.CompressBlockBound(len(w))) // Framing - first 4 bytes is original size
	cs, err := lz4.CompressBlockHC(w, compressed[4:], 0)
	if err != nil {
		return nil, fmt.Errorf("CompressBlockHC failed: %w", err)
	}
	binary.LittleEndian.PutUint32(compressed, uint32(len(w)))
	if cs >= len(w) { // does not compress (rare for large buckets, so copy is not a problem)
		compressed = append(compressed[:4], w...)
		return compressed, nil
	}
	return compressed[:4+cs], nil
}

func (s *Shard) sendRecent(cancelCtx context.Context, cbd compressedBucketData) bool {
	now := time.Now()
	nowUnix := uint32(now.Unix())
	if cbd.time+data_model.MaxShortWindow+data_model.FutureWindow < nowUnix { // Not bother sending, will receive error anyway
		return false
	}
	// We have large delay between stopping new recent sends
	// and waiting all recent senders to finish.
	// timeSpreadDelta is way smaller than typical wait time (aggregator recent window).
	// so will not affect how long shutdown takes.
	// We keep this simple Sleep, without timer and context
	time.Sleep(s.timeSpreadDelta)
	var resp []byte
	// Motivation - can save sending request, as rpc.Client checks for timeout before sending
	ctx, cancel := context.WithDeadline(cancelCtx, now.Add(time.Second*data_model.MaxConveyorDelay))
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

func (s *Shard) goSendRecent(num int, wg *sync.WaitGroup, recentSendersSema *semaphore.Weighted, cancelCtx context.Context, bucketsToSend chan compressedBucketData) {
	defer wg.Done()
	defer recentSendersSema.Release(1)
	for cbd := range bucketsToSend {
		s.mu.Lock()
		saveSecondsImmediately := s.config.SaveSecondsImmediately
		s.mu.Unlock()
		if saveSecondsImmediately {
			cbd = s.diskCachePutWithLog(cbd) // save before sending. assigns id
		}
		// log.Printf("goSendRecent.sendRecent %d start", num)
		if s.sendRecent(cancelCtx, cbd) {
			s.diskCacheEraseWithLog(cbd.id, "after sending")
		} else {
			cbd = s.diskCachePutWithLog(cbd) // NOP if saved above
			s.appendHistoricBucketsToSend(cbd)
		}
		// log.Printf("goSendRecent.sendRecent %d finish", num)
	}
	log.Printf("goSendRecent.sendRecent %d quit", num)
}

func (s *Shard) sendHistoric(cancelCtx context.Context, cbd compressedBucketData, scratchPad *[]byte) {
	var err error

	for {
		nowUnix := uint32(time.Now().Unix())
		if s.checkOutOfWindow(nowUnix, cbd) { // should check in for because time passes with attempts
			time.Sleep(200 * time.Millisecond)
			// Deleting 5 seconds per second is good for us, and does not spin CPU too much
			// This sleep will not affect shutdown time, so we keep it simple without timer+context.
			return
		}
		if len(cbd.data) == 0 { // Read once, if needed, but only after checking timestamp
			if s.agent.diskBucketCache == nil {
				s.agent.statErrorsDiskReadNotConfigured.AddValueCounter(0, 1)
				return // No data and no disk storage configured, alas
			}
			if cbd.data, err = s.agent.diskBucketCache.GetBucket(s.ShardNum, cbd.id, cbd.time, scratchPad); err != nil {
				s.agent.logF("Disk Error: diskCache.GetBucket returned error %v for shard %d bucket %d",
					err, s.ShardKey, cbd.time)
				s.agent.statErrorsDiskRead.AddValueCounter(0, 1)
				return
			}
			if len(cbd.data) < 4 { // we store original size of compressed data in first 4 bytes
				s.agent.logF("Disk Error: diskCache.GetBucket returned compressed bucket data with size %d for shard %d bucket %d",
					len(cbd.data), s.ShardKey, cbd.time)
				s.agent.statErrorsDiskRead.AddValueCounter(0, 1)
				s.diskCacheEraseWithLog(cbd.id, "after reading tiny")
				return
			}
		}
		var resp []byte

		shardReplica, spare := s.agent.getShardReplicaForSeccnd(s.ShardNum, cbd.time)
		if shardReplica == nil {
			select {
			case <-cancelCtx.Done():
				return
			case <-time.After(10 * time.Second):
				break
			}
			s.agent.logF("both historic shards are dead, shard %d, time %d, %v", s.ShardKey, cbd.time, err)
			continue
		}
		// We use infinite timeout, because otherwise, if aggregator is busy, source will send the same bucket again and again, inflating amount of data
		// But we set FailIfNoConnection to switch to fallback immediately
		err = shardReplica.sendSourceBucketCompressed(cancelCtx, cbd, true, spare, &resp, s)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			if !data_model.SilentRPCError(err) {
				shardReplica.stats.historicSendFailed.Add(1)
			} else {
				shardReplica.stats.historicSendSkip.Add(1)
			}
			select {
			case <-cancelCtx.Done():
				return
			case <-time.After(time.Second):
				break
			}
			continue
		}
		shardReplica.stats.historicSendSuccess.Add(1)
		s.diskCacheEraseWithLog(cbd.id, "after sending historic")
		break
	}
}

func (s *Shard) diskCachePutWithLog(cbd compressedBucketData) compressedBucketData {
	if s.agent.diskBucketCache == nil {
		return cbd
	}
	if cbd.id != 0 {
		return cbd // already saved
	}
	// Motivation - we want to set limit dynamically.
	// Also, if limit is set to 0, we want to gradually erase all seconds.
	// That's why we create diskCache always
	s.mu.Lock()
	maxHistoricDiskSize := s.config.MaxHistoricDiskSize
	s.mu.Unlock()
	if maxHistoricDiskSize <= 0 { // if we want to disable disk cache in runtime, we cannot set s.agent.diskBucketCache == nil
		return cbd
	}
	id, err := s.agent.diskBucketCache.PutBucket(s.ShardNum, cbd.time, cbd.data)
	if err != nil {
		s.agent.logF("Disk Error: diskCache.PutBucket returned error %v for shard %d bucket %d",
			err, s.ShardKey, cbd.time)
		s.agent.statErrorsDiskWrite.AddValueCounter(0, 1)
		return cbd
	}
	cbd.id = id
	return cbd
}

func (s *Shard) diskCacheEraseWithLog(id int64, place string) {
	if s.agent.diskBucketCache == nil {
		return
	}
	if err := s.agent.diskBucketCache.EraseBucket(s.ShardNum, id); err != nil {
		s.agent.logF("Disk Error: diskCache.EraseBucket returned error %v for shard %d %s id %d",
			err, s.ShardKey, place, id)
		s.agent.statErrorsDiskErase.AddValueCounter(0, 1)
	}
}

func (s *Shard) appendHistoricBucketsToSend(cbd compressedBucketData) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.HistoricBucketsDataSize+len(cbd.data) > data_model.MaxHistoricBucketsMemorySize/s.agent.NumShards() {
		cbd.data = nil
		if cbd.id == 0 {
			// bucket is lost due to inability to save to disk and memory limit
			s.agent.logF("appendHistoricBucketsToSend: data bucket %d which is not saved to disk is thrown out due to memory limit in shard %d",
				cbd.time, s.ShardKey)
			s.agent.statMemoryOverflow.AddValueCounter(float64(time.Now().Unix())-float64(cbd.time), 1)
			return
		}
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
	sec, id := s.agent.diskBucketCache.ReadNextTailBucket(s.ShardNum)
	if id == 0 { // disk queue finished, all second are in s.HistoricBucketsToSend
		return
	}
	s.HistoricBucketsToSend = append(s.HistoricBucketsToSend, compressedBucketData{id: id, time: sec}) // HistoricBucketsDataSize does not change
}

func (s *Shard) popOldestHistoricSecondLocked(nowUnix uint32) (_ compressedBucketData, ok bool) {
	if len(s.HistoricBucketsToSend) == 0 {
		return compressedBucketData{}, false
	}
	// Sending the oldest known historic buckets is very important for "herding" strategy
	// Even tiny imperfectness in sorting explodes number of inserts aggregator makes
	oldestPos := 0
	for i, e := range s.HistoricBucketsToSend {
		if e.time < s.HistoricBucketsToSend[oldestPos].time {
			oldestPos = i
		}
	}
	cbd := s.HistoricBucketsToSend[oldestPos]
	if cbd.time >= nowUnix && cbd.time <= nowUnix+data_model.MaxFutureSecondsOnDisk {
		// When agent shuts down, it will save future queue with up to 2 minutes in the future.
		// Later when agent starts again, those will go to historic conveyor and historic senders will wait
		// until they can send to aggregator without it discarding due to data in the future.
		//
		// second condition is for the following case:
		// as historic senders now wait while oldest second is in the future,
		// we must protect against seconds in the far future, otherwise when agent clock
		// jumps, our conveyor might be stuck forever
		return compressedBucketData{}, false
	}

	s.HistoricBucketsToSend[oldestPos] = s.HistoricBucketsToSend[len(s.HistoricBucketsToSend)-1]
	s.HistoricBucketsToSend = s.HistoricBucketsToSend[:len(s.HistoricBucketsToSend)-1]

	s.HistoricBucketsDataSize -= len(cbd.data)
	s.agent.historicBucketsDataSize.Sub(int64(len(cbd.data)))
	if s.HistoricBucketsDataSize < 0 {
		panic("HistoricBucketsDataSize < 0")
	}
	s.readHistoricSecondLocked() // we've just removed item, add item if exist on disk
	return cbd, true
}

func (s *Shard) checkOutOfWindow(nowUnix uint32, cbd compressedBucketData) bool {
	if nowUnix < data_model.MaxHistoricWindow || cbd.time >= nowUnix-data_model.MaxHistoricWindow { // Not bother sending, will receive error anyway
		return false
	}
	s.agent.logF("Send Disaster: Bucket %d for shard %d does not fit into full admission window (now is %d), throwing out",
		cbd.time, s.ShardKey, nowUnix)
	s.agent.statLongWindowOverflow.AddValueCounter(float64(nowUnix)-float64(cbd.time), 1)
	s.HistoricOutOfWindowDropped.Add(1)

	s.diskCacheEraseWithLog(cbd.id, "after throwing out historic")
	return true
}

func (s *Shard) goSendHistoric(wg *sync.WaitGroup, cancelSendsCtx context.Context) {
	defer wg.Done()
	var scratchPad []byte
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		nowUnix := uint32(time.Now().Unix())
		cbd, ok := s.popOldestHistoricSecondLocked(nowUnix)
		if !ok {
			s.cond.Wait()
			continue
		}
		s.mu.Unlock()
		s.sendHistoric(cancelSendsCtx, cbd, &scratchPad)
		s.mu.Lock()
	}
}

func (s *Shard) goEraseHistoric(wg *sync.WaitGroup, cancelCtx context.Context) {
	wg.Done()
	// When all senders are in infinite wait, buckets must still be erased
	// Also, we delete buckets when disk limit is reached. We cannot promise strict limit, because disk cache design is very loose.
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		nowUnix := uint32(time.Now().Unix())
		cbd, ok := s.popOldestHistoricSecondLocked(nowUnix)
		if !ok {
			s.cond.Wait()
			continue
		}
		diskLimit := s.config.MaxHistoricDiskSize / int64(s.agent.NumShards())
		s.mu.Unlock()

		if s.checkOutOfWindow(nowUnix, cbd) { // should check, because time passes with attempts
			time.Sleep(200 * time.Millisecond)
			// Deleting 5 seconds per second is good for us, and does not spin CPU too much
			// This sleep will not affect shutdown time, so we keep it simple without timer+context.
			s.mu.Lock()
			continue
		}

		diskUsed, _ := s.HistoricBucketsDataSizeDisk()
		// diskUsed is sum of file size, and will not shrink until all seconds in a file are deleted
		// seconds held by historic seconds will not be deleted, because they are not popped by popOldestHistoricSecondLocked above,
		// if all possible seconds are deleted, but we are still over limit, this goroutine will block on s.cond.Wait() above
		if diskUsed > diskLimit {
			s.agent.logF("Send Disaster: Bucket %d for shard %d (now is %d) violates disk size limit %d (%d used), throwing out",
				cbd.time, s.ShardKey, nowUnix, diskLimit, diskUsed)
			s.agent.statDiskOverflow.AddValueCounter(float64(nowUnix)-float64(cbd.time), 1)

			s.diskCacheEraseWithLog(cbd.id, "after throwing out historic, due to disk limit")
			time.Sleep(200 * time.Millisecond)
			// Deleting 5 seconds per second is good for us, and does not spin CPU too much
			// This sleep will not affect shutdown time, so we keep it simple without timer+context.
			s.mu.Lock()
			continue
		}

		s.appendHistoricBucketsToSend(cbd) // As we consumed cond state, signal inside that func is required

		select {
		case <-cancelCtx.Done():
			return
		case <-time.After(60 * time.Second): // rare, because only  fail-safe against all goSendHistoric blocking in infinite sends
		}
		s.mu.Lock()
	}
}

func (s *Shard) DisableNewSends() {
	s.mu.Lock()
	close(s.BucketsToSend)
	s.BucketsToSend = nil
	s.mu.Unlock()
	s.cond.Broadcast()
}

func isShardDeadError(err error) bool {
	if err == nil {
		return false
	}
	var rpcError *rpc.Error
	if !errors.As(err, &rpcError) {
		return true
	}
	return rpcError.Code != data_model.RPCErrorMissedRecentConveyor
}
