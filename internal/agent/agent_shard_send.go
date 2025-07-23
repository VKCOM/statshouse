// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package agent

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log"
	"slices"
	"sync"
	"time"

	"github.com/VKCOM/statshouse/internal/compress"
	"github.com/VKCOM/statshouse/internal/vkgo/basictl"

	"github.com/VKCOM/statshouse/internal/vkgo/semaphore"

	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/VKCOM/statshouse/internal/format"
	"github.com/VKCOM/statshouse/internal/vkgo/rpc"

	"pgregory.net/rand"
)

// If clients want less jitter (most want), they should send data quickly after end of calendar second.
// Agent has small window (for example, half a second) when it accepts data for previous second with zero sampling penalty.
func (s *Shard) flushBuckets(now time.Time) (gap int64, sendTime uint32) {
	// called several times/sec only, but must still be fast, so we do not lock shard for too long
	s.mu.Lock()
	defer s.mu.Unlock()
	nowUnix := uint32(now.Unix())
	if nowUnix > s.CurrentTime {
		s.CurrentTime = nowUnix
		if gap = s.gapInReceivingQueueLocked(); gap > 0 {
			sendTime = s.SendTime
		}

		// popOldestHistoricSecondLocked condition now depends on CurrentTime
		// we wake up one consumer to see if condition change and there is
		// former future bucket not in the future anymore
		s.cond.Signal()
	}
	// We want PreprocessingBucketTime to strictly increase, so that historic conveyor is strictly ordered

	if s.SendTime < s.CurrentTime-superQueueLen { // efficient jump ahead after long machine sleep, etc.
		s.SendTime += ((s.CurrentTime - superQueueLen - s.SendTime + superQueueLen - 1) / superQueueLen) * superQueueLen // not simplified for clear correctness
	}

	sendUpToTime := uint32(now.Add(-data_model.AgentWindow).Unix())
	for {
		if sendUpToTime <= s.SendTime || len(s.BucketsToPreprocess) != 0 {
			return
		}
		if s.SendTime >= s.CurrentTime { // must be never if AgentWindow is set correctly, but better safe than sorry
			return
		}
		// During normal conveyor operation, we send every second so number of contributing
		// agents stay the same. If agent sleeps/pauses, there will be some gaps anyway,
		// so we simplify logic here by not sending empty buckets after such pauses
		_ = s.FlushAllDataSingleStep(s.gapInReceivingQueueLocked() <= 0)
	}
}

func (s *Shard) FlushAllDataSingleStep(sendEmpty bool) int {
	sendTime := s.SendTime
	s.SendTime++
	b := s.SuperQueue[sendTime%superQueueLen]
	if b.Empty() && !sendEmpty {
		return 0
	}
	s.SuperQueue[sendTime%superQueueLen] = &data_model.MetricsBucket{}
	b.Time = sendTime
	// we wait here only when shutting down. During normal work we check len(chan) before calling this func
	s.BucketsToPreprocess <- b
	return 1
}

func (s *Shard) StopReceivingIncomingData() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopReceivingIncomingData = true
}

func (s *Shard) StopPreprocessor() {
	close(s.BucketsToPreprocess)
}

func (s *Shard) addSizeByTypeMetric(t uint32, partKey int32, size int) {
	// This metric is added by source, because aggregator has no spare time for that
	s.agent.AddValueCounter(t, format.BuiltinMetricMetaTLByteSizePerInflightType,
		[]int32{0, partKey, s.agent.componentTag, format.AggShardTag: s.ShardKey},
		float64(size), 1)
}

func (s *Shard) bucketToSourceBucket2TL(bucket *data_model.MetricsBucket, sampleFactors []tlstatshouse.SampleFactor, scratch []byte) (sb tlstatshouse.SourceBucket2, _ []byte) {
	sizeUnique, sizePercentiles, sizeValue, sizeSingleValue, sizeCounter, sizeStringTop := 0, 0, 0, 0, 0, 0

	for _, v := range bucket.MultiItems {
		if v.Key.Metric == format.BuiltinMetricIDIngestionStatus && v.Key.Tags[2] == format.TagValueIDSrcIngestionStatusOKCached {
			// transfer optimization
			status := tlstatshouse.IngestionStatus2{
				Env:    v.Key.Tags[0],
				Metric: v.Key.Tags[1],
				Value:  float32(v.Tail.Value.Count() * v.SF),
			}
			sb.IngestionStatusOk2 = append(sb.IngestionStatusOk2, status)
			continue
		}

		item := v.Key.TLMultiItemFromKey(bucket.Time)
		item.SetWeightMultiplier(v.WeightMultiplier > 1) // we do not need actual values, it is either 1 or numshards
		scratch = v.Tail.MultiValueToTL(v.MetricMeta, &item.Tail, v.SF, &item.FieldsMask, scratch)
		scratch = item.Write(scratch[:0])
		switch { // This is only an approximation
		case item.Tail.IsSetUniques(item.FieldsMask):
			sizeUnique += len(scratch)
		case item.Tail.IsSetCentroids(item.FieldsMask):
			sizePercentiles += len(scratch)
		case item.Tail.IsSetValueMax(item.FieldsMask):
			sizeValue += len(scratch)
		case item.Tail.IsSetValueMin(item.FieldsMask):
			sizeSingleValue += len(scratch)
		default:
			sizeCounter += len(scratch)
		}

		var top []tlstatshouse.TopElement
		for key, value := range v.Top {
			el := tlstatshouse.TopElement{Stag: key.S} // TODO - send I
			scratch = value.MultiValueToTL(v.MetricMeta, &el.Value, v.SF, &el.FieldsMask, scratch)
			top = append(top, el)
			scratch = el.Write(scratch[:0])
			sizeStringTop += len(scratch)
		}
		if len(top) != 0 {
			item.SetTop(top)
		}

		sb.Metrics = append(sb.Metrics, item)
	}

	// Add size metrics
	s.addSizeByTypeMetric(bucket.Time, format.TagValueIDSizeUnique, sizeUnique)
	s.addSizeByTypeMetric(bucket.Time, format.TagValueIDSizePercentiles, sizePercentiles)
	s.addSizeByTypeMetric(bucket.Time, format.TagValueIDSizeValue, sizeValue)
	s.addSizeByTypeMetric(bucket.Time, format.TagValueIDSizeSingleValue, sizeSingleValue)
	s.addSizeByTypeMetric(bucket.Time, format.TagValueIDSizeCounter, sizeCounter)
	s.addSizeByTypeMetric(bucket.Time, format.TagValueIDSizeStringTop, sizeStringTop)

	sb.SampleFactors = append(sb.SampleFactors, sampleFactors...)

	// Calculate size metrics for sample factors and ingestion status - bucket 2
	sbSizeCalc := tlstatshouse.SourceBucket2{SampleFactors: sb.SampleFactors}
	scratch = sbSizeCalc.Write(scratch[:0])
	s.addSizeByTypeMetric(bucket.Time, format.TagValueIDSizeSampleFactors, len(scratch))

	sbSizeCalc = tlstatshouse.SourceBucket2{
		IngestionStatusOk:  sb.IngestionStatusOk,
		IngestionStatusOk2: sb.IngestionStatusOk2,
	}
	scratch = sbSizeCalc.Write(scratch[:0])
	s.addSizeByTypeMetric(bucket.Time, format.TagValueIDSizeIngestionStatusOK, len(scratch))

	slices.SortFunc(sb.Metrics, func(a, b tlstatshouse.MultiItem) int {
		return cmp.Compare(a.Metric, b.Metric)
	})

	return sb, scratch
}

func (s *Shard) bucketToSourceBucket3TL(bucket *data_model.MetricsBucket, sampleFactors []tlstatshouse.SampleFactor, scratch []byte) (sb tlstatshouse.SourceBucket3, _ []byte) {
	sizeUnique, sizePercentiles, sizeValue, sizeSingleValue, sizeCounter, sizeStringTop := 0, 0, 0, 0, 0, 0

	for _, v := range bucket.MultiItems {
		if v.Key.Metric == format.BuiltinMetricIDIngestionStatus && v.Key.Tags[2] == format.TagValueIDSrcIngestionStatusOKCached {
			// transfer optimization
			status := tlstatshouse.IngestionStatus2{
				Env:    v.Key.Tags[0],
				Metric: v.Key.Tags[1],
				Value:  float32(v.Tail.Value.Count() * v.SF),
			}
			sb.IngestionStatusOk2 = append(sb.IngestionStatusOk2, status)
			continue
		}

		item := v.Key.TLMultiItemFromKey(bucket.Time)
		item.SetWeightMultiplier(v.WeightMultiplier > 1) // we do not need actual values, it is either 1 or numshards
		scratch = v.Tail.MultiValueToTL(v.MetricMeta, &item.Tail, v.SF, &item.FieldsMask, scratch)
		scratch = item.Write(scratch[:0])

		switch { // This is only an approximation
		case item.Tail.IsSetUniques(item.FieldsMask):
			sizeUnique += len(scratch)
		case item.Tail.IsSetCentroids(item.FieldsMask):
			sizePercentiles += len(scratch)
		case item.Tail.IsSetValueMax(item.FieldsMask):
			sizeValue += len(scratch)
		case item.Tail.IsSetValueMin(item.FieldsMask):
			sizeSingleValue += len(scratch)
		default:
			sizeCounter += len(scratch)
		}

		var top []tlstatshouse.TopElement
		for key, value := range v.Top {
			el := tlstatshouse.TopElement{Stag: key.S}
			if key.I != 0 {
				el.SetTag(key.I)
			}
			scratch = value.MultiValueToTL(v.MetricMeta, &el.Value, v.SF, &el.FieldsMask, scratch)
			top = append(top, el)
			scratch = el.Write(scratch[:0])
			sizeStringTop += len(scratch)
		}
		if len(top) != 0 {
			item.SetTop(top)
		}

		sb.Metrics = append(sb.Metrics, item)
	}
	// Add size metrics
	s.addSizeByTypeMetric(bucket.Time, format.TagValueIDSizeUnique, sizeUnique)
	s.addSizeByTypeMetric(bucket.Time, format.TagValueIDSizePercentiles, sizePercentiles)
	s.addSizeByTypeMetric(bucket.Time, format.TagValueIDSizeValue, sizeValue)
	s.addSizeByTypeMetric(bucket.Time, format.TagValueIDSizeSingleValue, sizeSingleValue)
	s.addSizeByTypeMetric(bucket.Time, format.TagValueIDSizeCounter, sizeCounter)
	s.addSizeByTypeMetric(bucket.Time, format.TagValueIDSizeStringTop, sizeStringTop)

	sb.SampleFactors = append(sb.SampleFactors, sampleFactors...)

	// Calculate size metrics for sample factors and ingestion status
	sbSizeCalc := tlstatshouse.SourceBucket3{SampleFactors: sb.SampleFactors}
	scratch = sbSizeCalc.Write(scratch[:0])
	s.addSizeByTypeMetric(bucket.Time, format.TagValueIDSizeSampleFactors, len(scratch))

	sbSizeCalc = tlstatshouse.SourceBucket3{IngestionStatusOk2: sb.IngestionStatusOk2}
	scratch = sbSizeCalc.Write(scratch[:0])
	s.addSizeByTypeMetric(bucket.Time, format.TagValueIDSizeIngestionStatusOK, len(scratch))

	slices.SortFunc(sb.Metrics, func(a, b tlstatshouse.MultiItem) int {
		return cmp.Compare(a.Metric, b.Metric)
	})

	return sb, scratch
}

func (s *Shard) goPreProcess(wg *sync.WaitGroup) {
	defer wg.Done()
	rng := rand.New() // We use distinct rand so that we can use it without locking

	var scratch []byte
	for bucket := range s.BucketsToPreprocess {
		var buffers data_model.SamplerBuffers
		start := time.Now()
		// If bucket is empty, we must still do processing and sending
		// for each contributor every second.
		buffers = s.sampleBucket(bucket, buffers, rng)
		scratch = s.sendToSenders(bucket, buffers.SampleFactors, scratch)
		s.agent.TimingsPreprocess.AddValueCounter(time.Since(start).Seconds(), 1)
	}
	log.Printf("Preprocessor quit")
}

func (s *Shard) sampleBucket(bucket *data_model.MetricsBucket, buffers data_model.SamplerBuffers, rnd *rand.Rand) data_model.SamplerBuffers {
	s.mu.Lock()
	config := s.config
	s.mu.Unlock()

	sampler := data_model.NewSampler(data_model.SamplerConfig{
		ModeAgent:            s.agent.componentTag == format.TagValueIDComponentAgent,
		SampleKeepSingle:     config.SampleKeepSingle,
		DisableNoSampleAgent: config.DisableNoSampleAgent,
		SampleNamespaces:     config.SampleNamespaces,
		SampleGroups:         config.SampleGroups,
		SampleKeys:           config.SampleKeys,
		Meta:                 s.agent.metricStorage,
		Rand:                 rnd,
		DiscardF: func(item *data_model.MultiItem, _ uint32) {
			bucket.DeleteMultiItem(&item.Key)
		}, // remove from map
		SamplerBuffers: buffers,
	})
	for _, item := range bucket.MultiItems {
		whaleWeight := item.FinishStringTop(rnd, config.StringTopCountSend) // all excess items are baked into Tail
		accountMetric := item.Key.Metric
		sz := item.Key.TLSizeEstimate(bucket.Time) + item.TLSizeEstimate()
		if item.Key.Metric == format.BuiltinMetricIDIngestionStatus {
			if item.Key.Tags[1] != 0 {
				// Ingestion status and other unlimited per-metric built-ins should use its metric budget
				// So metrics are better isolated
				accountMetric = item.Key.Tags[1]
				whaleWeight = 0 // ingestion statuses do not compete for whale status
			}
			if item.Key.Tags[2] == format.TagValueIDSrcIngestionStatusOKCached {
				// These are so common, we have transfer optimization for them
				sz = 3 * 4 // see statshouse.ingestion_status2
			}
		}
		sampler.Add(data_model.SamplingMultiItemPair{
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
		s.agent.MergeItemValue(bucket.Time, format.BuiltinMetricMetaSrcSamplingSizeBytes,
			[]int32{0, s.agent.componentTag, format.TagValueIDSamplingDecisionKeep, v.NamespaceID, v.GroupID, v.MetricID},
			&v.SumSizeKeep)
		// discard bytes
		s.agent.MergeItemValue(bucket.Time, format.BuiltinMetricMetaSrcSamplingSizeBytes,
			[]int32{0, s.agent.componentTag, format.TagValueIDSamplingDecisionDiscard, v.NamespaceID, v.GroupID, v.MetricID},
			&v.SumSizeDiscard)
		// budget
		s.agent.AddValueCounter(bucket.Time, format.BuiltinMetricMetaSrcSamplingGroupBudget,
			[]int32{0, s.agent.componentTag, v.NamespaceID, v.GroupID},
			v.Budget(), 1)
	}
	// report budget used
	s.agent.AddValueCounter(bucket.Time, format.BuiltinMetricMetaSrcSamplingBudget,
		[]int32{0, s.agent.componentTag},
		float64(remainingBudget), 1)
	// metric count
	s.agent.AddValueCounter(bucket.Time, format.BuiltinMetricMetaSrcSamplingMetricCount,
		[]int32{0, s.agent.componentTag},
		float64(sampler.MetricCount), 1)
	return sampler.SamplerBuffers
}

func (s *Shard) sendToSenders(bucket *data_model.MetricsBucket, sampleFactors []tlstatshouse.SampleFactor, scratch []byte) []byte {
	version := uint8(3)
	if s.sendSourceBucket2 {
		version = 2
	}
	data, scratch, err := s.compressBucket(bucket, sampleFactors, version, scratch)
	cbd := compressedBucketData{time: bucket.Time, data: data, version: version} // No id as not saved to disk yet
	if err != nil {
		s.agent.statErrorsDiskCompressFailed.AddValueCounter(0, 1)
		s.agent.logF("Internal Error: Failed to compress bucket %v for shard %d bucket %d",
			err, s.ShardKey, bucket.Time)
		return scratch
	}
	s.mu.Lock() // must send under lock, otherwise s.BucketsToSend might be closed
	if s.BucketsToSend != nil {
		select {
		case s.BucketsToSend <- cbd:
			s.mu.Unlock() // now goSendRecent is responsible for saving/sending/etc.
			return scratch
		default:
			break
		}
	}
	s.mu.Unlock()
	// s.client.Client.Logf("Slowdown: Buckets Channel full for shard %d replica %d (shard-replica %d). Moving bucket %d to Historic Conveyor",
	// 	s.ShardKey, s.ReplicaKey, s.ShardReplicaNum, cbd.time)
	cbd = s.diskCachePutWithLog(cbd) // assigns id
	s.appendHistoricBucketsToSend(cbd)
	return scratch
}

func (s *Shard) compressBucket(bucket *data_model.MetricsBucket, sampleFactors []tlstatshouse.SampleFactor, version uint8, scratch []byte) (data []byte, _ []byte, err error) {
	if version == 3 {
		var sb tlstatshouse.SourceBucket3
		sb, scratch = s.bucketToSourceBucket3TL(bucket, sampleFactors, scratch)
		scratch = sb.WriteBoxed(scratch[:0])
	} else {
		var sb tlstatshouse.SourceBucket2
		sb, scratch = s.bucketToSourceBucket2TL(bucket, sampleFactors, scratch)
		scratch = sb.WriteBoxed(scratch[:0])
	}
	compressed, err := compress.CompressAndFrame(scratch)
	return compressed, scratch, err
}

func (s *Shard) sendRecent(cancelCtx context.Context, cbd compressedBucketData, sendMoreBytes int) bool {
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
	// Motivation - can save sending request, as rpc.Client checks for timeout before sending
	ctx, cancel := context.WithDeadline(cancelCtx, now.Add(time.Second*data_model.MaxConveyorDelay))
	defer cancel()

	shardReplica, spare := s.agent.getShardReplicaForSecond(s.ShardNum, cbd.time)
	if shardReplica == nil {
		return false
	}

	if cbd.version == 3 {
		var respV3 tlstatshouse.SendSourceBucket3Response
		err := shardReplica.sendSourceBucket3Compressed(ctx, cbd, sendMoreBytes, false, spare, &respV3)
		if !spare {
			shardReplica.recordSendResult(!isShardDeadError(err))
		}
		if err != nil {
			shardReplica.stats.recentSendFailed.Add(1)
			s.agent.logF("Send Error: %v, moving bucket %d to historic conveyor for shard %d",
				err, cbd.time, s.ShardKey)
			return false
		}
		s.agent.mappingsCache.AddValues(cbd.time, respV3.Mappings)
		if len(respV3.Warning) != 0 {
			s.agent.logF("Send Warning: %s, moving bucket %d to historic conveyor for shard %d",
				respV3.Warning, cbd.time, s.ShardKey)
		}
		if !respV3.IsSetDiscard() {
			shardReplica.stats.recentSendKeep.Add(1)
			return false
		}
		shardReplica.stats.recentSendSuccess.Add(1)
		return true
	}
	var respV2 string
	err := shardReplica.sendSourceBucket2Compressed(ctx, cbd, sendMoreBytes, false, spare, &respV2)
	if !spare {
		shardReplica.recordSendResult(!isShardDeadError(err))
	}
	if err != nil {
		if !data_model.SilentRPCError(err) {
			shardReplica.stats.recentSendFailed.Add(1)
			s.agent.logF("Send Error: s.client.Do returned error %v, moving bucket %d to historic conveyor for shard %d",
				err, cbd.time, s.ShardKey)
		} else {
			shardReplica.stats.recentSendKeep.Add(1)
		}
		return false
	}
	if respV2 != "Dummy historic result" {
		s.agent.logF("Send bucket returned: \"%s\"", respV2)
	}
	shardReplica.stats.recentSendSuccess.Add(1)
	return true
}

func (s *Shard) goSendRecent(num int, wg *sync.WaitGroup, recentSendersSema *semaphore.Weighted, cancelCtx context.Context, bucketsToSend chan compressedBucketData) {
	defer wg.Done()
	defer recentSendersSema.Release(1)
	for cbd := range bucketsToSend {
		start := time.Now()
		s.mu.Lock()
		saveSecondsImmediately := s.config.SaveSecondsImmediately
		sendMoreBytes := s.config.SendMoreBytes
		s.mu.Unlock()
		if saveSecondsImmediately {
			cbd = s.diskCachePutWithLog(cbd) // save before sending. assigns id
		}
		// log.Printf("goSendRecent.sendRecent %d start", num)
		if s.sendRecent(cancelCtx, cbd, sendMoreBytes) {
			s.diskCacheEraseWithLog(cbd.id, "after sending")
		} else {
			cbd = s.diskCachePutWithLog(cbd) // NOP if saved above
			s.appendHistoricBucketsToSend(cbd)
		}
		s.agent.TimingsSendRecent.AddValueCounter(time.Since(start).Seconds(), 1)
		// log.Printf("goSendRecent.sendRecent %d finish", num)
	}
	log.Printf("goSendRecent.sendRecent %d quit", num)
}

func (s *Shard) sendHistoric(cancelCtx context.Context, cbd compressedBucketData, scratchPad *[]byte) {
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
			var err error
			if cbd.data, err = s.agent.diskBucketCache.GetBucket(s.ShardNum, cbd.id, cbd.time, scratchPad); err != nil {
				s.agent.logF("Disk Error: diskCache.GetBucket returned error %v for shard %d bucket %d",
					err, s.ShardKey, cbd.time)
				s.agent.statErrorsDiskRead.AddValueCounter(0, 1)
				return
			}
			originalSize, compressedData, err := compress.DeFrame(cbd.data)
			// TODO - remove most of this code after we remove version field
			if err == nil && cbd.version == 0 { // from disk
				var originalData []byte
				originalData, err = compress.Decompress(originalSize, compressedData)
				if err == nil {
					tag, _ := basictl.NatPeekTag(originalData)
					switch tag {
					case (tlstatshouse.SourceBucket3{}).TLTag():
						cbd.version = 3
					case (tlstatshouse.SourceBucket2{}).TLTag():
						cbd.version = 2
					default:
						err = fmt.Errorf("unknown source bucket tag 0x%08x", tag)
					}
				}
			}
			if err != nil {
				s.agent.logF("Disk Error: diskCache.GetBucket returned compressed bucket data with too small size %d for shard %d bucket %d",
					len(cbd.data), s.ShardKey, cbd.time)
				s.agent.statErrorsDiskRead.AddValueCounter(0, 1)
				s.diskCacheEraseWithLog(cbd.id, "after reading tiny")
				return
			}
		}

		shardReplica, spare := s.agent.getShardReplicaForSecond(s.ShardNum, cbd.time)
		if shardReplica == nil {
			select {
			case <-cancelCtx.Done():
				return
			case <-time.After(10 * time.Second):
				continue
			}
		}
		// We use infinite timeout, because otherwise, if aggregator is busy, source will send the same bucket again and again, inflating amount of data
		// But we set FailIfNoConnection to switch to fallback immediately
		if cbd.version == 3 {
			var respV3 tlstatshouse.SendSourceBucket3Response
			err := shardReplica.sendSourceBucket3Compressed(cancelCtx, cbd, 0, true, spare, &respV3)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				shardReplica.stats.historicSendFailed.Add(1)
				select {
				case <-cancelCtx.Done():
					return
				case <-time.After(time.Second):
					continue
				}
			}
			if len(respV3.Warning) != 0 {
				s.agent.logF("Send historic bucket returned: %s", respV3.Warning)
			}
			// we choose to use bucket time here, because we do not map new strings for historic buckets
			// at the moment of sending, if they were not mapped at the moment of saving
			s.agent.mappingsCache.AddValues(cbd.time, respV3.Mappings)
			if !respV3.IsSetDiscard() {
				shardReplica.stats.historicSendKeep.Add(1)
				select {
				case <-cancelCtx.Done():
					return
				case <-time.After(time.Second):
					continue
				}
			}
			shardReplica.stats.historicSendSuccess.Add(1)
			s.diskCacheEraseWithLog(cbd.id, "after sending historic")
			break
		}
		var resp string
		err := shardReplica.sendSourceBucket2Compressed(cancelCtx, cbd, 0, true, spare, &resp)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			if !data_model.SilentRPCError(err) {
				shardReplica.stats.historicSendFailed.Add(1)
			} else {
				shardReplica.stats.historicSendKeep.Add(1)
			}
			select {
			case <-cancelCtx.Done():
				return
			case <-time.After(time.Second):
				continue
			}
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
	if s.historicBucketsDataSize+len(cbd.data) > data_model.MaxHistoricBucketsMemorySize/s.agent.NumShards() {
		cbd.data = nil
		if cbd.id == 0 {
			// bucket is lost due to inability to save to disk and memory limit
			s.agent.logF("appendHistoricBucketsToSend: data bucket %d which is not saved to disk is thrown out due to memory limit in shard %d",
				cbd.time, s.ShardKey)
			s.agent.statMemoryOverflow.AddValueCounter(float64(time.Now().Unix())-float64(cbd.time), 1)
			return
		}
	} else {
		s.historicBucketsDataSize += len(cbd.data)
		s.agent.historicBucketsDataSize.Add(int64(len(cbd.data)))
	}
	s.historicBucketsToSend = append(s.historicBucketsToSend, cbd)
	s.cond.Signal()
}

func (s *Shard) readHistoricSecondLocked() {
	if s.agent.diskBucketCache == nil {
		return
	}
	sec, id := s.agent.diskBucketCache.ReadNextTailBucket(s.ShardNum)
	if id == 0 { // disk queue finished, all second are in s.historicBucketsToSend
		return
	}
	s.historicBucketsToSend = append(s.historicBucketsToSend, compressedBucketData{id: id, time: sec}) // historicBucketsDataSize does not change
}

func (s *Shard) popOldestHistoricSecondLocked(nowUnix uint32) (_ compressedBucketData, ok bool) {
	if len(s.historicBucketsToSend) == 0 {
		return compressedBucketData{}, false
	}
	// Sending the oldest known historic buckets is very important for "herding" strategy
	// Even tiny imperfectness in sorting explodes number of inserts aggregator makes
	oldestPos := 0
	for i, e := range s.historicBucketsToSend {
		if e.time < s.historicBucketsToSend[oldestPos].time {
			oldestPos = i
		}
	}
	cbd := s.historicBucketsToSend[oldestPos]
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

	lastPos := len(s.historicBucketsToSend) - 1
	s.historicBucketsToSend[oldestPos] = s.historicBucketsToSend[lastPos]
	s.historicBucketsToSend[lastPos] = compressedBucketData{} // free memory here
	s.historicBucketsToSend = s.historicBucketsToSend[:lastPos]

	s.historicBucketsDataSize -= len(cbd.data)
	s.agent.historicBucketsDataSize.Sub(int64(len(cbd.data)))
	if s.historicBucketsDataSize < 0 {
		panic("historicBucketsDataSize < 0")
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
		start := time.Now()
		nowUnix := uint32(start.Unix())
		cbd, ok := s.popOldestHistoricSecondLocked(nowUnix)
		if !ok {
			s.cond.Wait()
			continue
		}
		s.mu.Unlock()
		s.sendHistoric(cancelSendsCtx, cbd, &scratchPad)
		s.mu.Lock()
		s.agent.TimingsSendHistoric.AddValueCounter(time.Since(start).Seconds(), 1)
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
		case <-time.After(60 * time.Second): // rare, because only fail-safe against all goSendHistoric blocking in infinite sends
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
