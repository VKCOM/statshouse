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
func (s *ShardReplica) flushBuckets(now time.Time) {
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
		// s.client.Client.Logf("Zatup 1 %d replica %d (shard-replica %d) bucket.time %d nowUnix %d",
		//	s.ShardKey, s.ReplicaKey, s.ShardReplicaNum, s.CurrentBucket.time, nowUnix)
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

	sb.SampleFactors = append(sb.SampleFactors, sampleFactors...)

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

func (s *ShardReplica) goPreProcess() {
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

		s.mergeBuckets(bucket, buckets) // TODO - why we merge instead of passing array to sampleBucket
		sampleFactors := s.sampleBucket(bucket, rnd)
		s.sendToSenders(bucket, missedSeconds, sampleFactors)

		s.mu.Lock()
	}
}

func (s *ShardReplica) mergeBuckets(bucket *data_model.MetricsBucket, buckets []*data_model.MetricsBucket) {
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

func (s *ShardReplica) sampleBucket(bucket *data_model.MetricsBucket, rnd *rand.Rand) []tlstatshouse.SampleFactor {
	s.mu.Lock()
	config := s.config
	s.mu.Unlock()

	sampler := data_model.NewSampler(len(bucket.MultiItems), data_model.SamplerConfig{
		ModeAgent: true,
		Meta:      s.agent.metricStorage,
		Rand:      rnd,
		DiscardF:  func(key data_model.Key, _ *data_model.MultiItem) { delete(bucket.MultiItems, key) }, // remove from map
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
	numShards := s.agent.NumShardReplicas()
	remainingBudget := int64((config.SampleBudget + numShards - 1) / numShards)
	if remainingBudget > data_model.MaxUncompressedBucketSize/2 { // Algorithm is not exact
		remainingBudget = data_model.MaxUncompressedBucketSize / 2
	}
	samplerStat := sampler.Run(remainingBudget, 1)
	sampleFactors := make([]tlstatshouse.SampleFactor, 0, samplerStat.Count)
	for _, s := range samplerStat.Steps {
		if s.StartPos < len(s.Groups) {
			value := float64(s.BudgetNum) / float64(s.BudgetDenom) / float64(s.SumWeight)
			key := data_model.Key{Metric: format.BuiltinMetricIDAgentPerMetricSampleBudget, Keys: [16]int32{0, format.TagValueIDAgentFirstSampledMetricBudgetPerMetric}}
			mi := data_model.MapKeyItemMultiItem(&bucket.MultiItems, key, config.StringTopCapacity, nil, nil)
			mi.Tail.Value.AddValueCounterHost(value, 1, 0)
		} else {
			key := data_model.Key{Metric: format.BuiltinMetricIDAgentPerMetricSampleBudget, Keys: [16]int32{0, format.TagValueIDAgentFirstSampledMetricBudgetUnused}}
			mi := data_model.MapKeyItemMultiItem(&bucket.MultiItems, key, config.StringTopCapacity, nil, nil)
			mi.Tail.Value.AddValueCounterHost(float64(s.BudgetNum)/float64(s.BudgetDenom), 1, 0)
		}
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
	// metric count
	key := data_model.Key{Metric: format.BuiltinMetricIDSrcSamplingMetricCount, Keys: [16]int32{0, s.agent.componentTag}}
	mi := data_model.MapKeyItemMultiItem(&bucket.MultiItems, key, config.StringTopCapacity, nil, nil)
	mi.Tail.Value.AddValueCounterHost(float64(len(samplerStat.Metrics)), 1, 0)
	return sampleFactors
}

func (s *ShardReplica) sendToSenders(bucket *data_model.MetricsBucket, missedSeconds uint32, sampleFactors []tlstatshouse.SampleFactor) {
	cbd, err := s.compressBucket(bucket, missedSeconds, sampleFactors)

	if err != nil {
		s.agent.statErrorsDiskCompressFailed.AddValueCounter(0, 1)
		s.client.Client.Logf("Internal Error: Failed to compress bucket %v for shard %d replica %d (shard-replica %d) bucket %d",
			err, s.ShardKey, s.ReplicaKey, s.ShardReplicaNum, bucket.Time)
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

func (s *ShardReplica) compressBucket(bucket *data_model.MetricsBucket, missedSeconds uint32, sampleFactors []tlstatshouse.SampleFactor) (compressedBucketData, error) {
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

func (s *ShardReplica) sendSourceBucketCompressed(ctx context.Context, cbd compressedBucketData, historic bool, spare bool, ret *[]byte) error {
	extra := rpc.InvokeReqExtra{FailIfNoConnection: true}
	args := tlstatshouse.SendSourceBucket2Bytes{
		Time:            cbd.time,
		BuildCommit:     []byte(build.Commit()),
		BuildCommitDate: s.agent.commitDateTag,
		BuildCommitTs:   s.agent.commitTimestamp,
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
	sizeDiskTotal, sizeDiskUnsent := s.HistoricBucketsDataSizeDisk()
	if sizeDiskTotal < math.MaxInt32 {
		args.QueueSizeDisk = int32(sizeDiskTotal)
	}
	if sizeDiskUnsent < math.MaxInt32 {
		args.SetQueueSizeDiskUnsent(int32(sizeDiskUnsent))
	} else {
		args.SetQueueSizeDiskUnsent(math.MaxInt32)
	}
	sizeDiskSumTotal, sizeDiskSumUnsent := s.agent.HistoricBucketsDataSizeDiskSum()
	if sizeDiskSumTotal < math.MaxInt32 {
		args.SetQueueSizeDiskSum(int32(sizeDiskSumTotal))
	} else {
		args.SetQueueSizeDiskSum(math.MaxInt32)
	}
	if sizeDiskSumUnsent < math.MaxInt32 {
		args.SetQueueSizeDiskSumUnsent(int32(sizeDiskSumUnsent))
	} else {
		args.SetQueueSizeDiskSumUnsent(math.MaxInt32)
	}
	sizeMemSum := s.agent.HistoricBucketsDataSizeMemorySum()
	if sizeMemSum < math.MaxInt32 {
		args.SetQueueSizeMemorySum(int32(sizeMemSum))
	} else {
		args.SetQueueSizeMemorySum(math.MaxInt32)
	}
	if s.client.Address != "" { // Skip sending to "empty" shards. Provides fast way to answer "what if there were more shards" question
		if err := s.client.SendSourceBucket2Bytes(ctx, args, &extra, ret); err != nil {
			return err
		}
	}
	return nil
}

func (s *ShardReplica) sendRecent(cbd compressedBucketData) bool {
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
		s.recordSendResult(!isShardDeadError(err))
	} else {
		spare := SpareShardReplica(s.ShardReplicaNum, cbd.time)
		if !s.agent.ShardReplicas[spare].alive.Load() {
			return false
		}
		err = s.agent.ShardReplicas[spare].sendSourceBucketCompressed(ctx, cbd, false, true, &resp)
	}
	if err != nil {
		if !data_model.SilentRPCError(err) {
			s.stats.recentSendFailed.Add(1)
			s.client.Client.Logf("Send Error: s.client.Do returned error %v, moving bucket %d to historic conveyor for shard %d replica %d (shard-replica %d)",
				err, cbd.time, s.ShardKey, s.ReplicaKey, s.ShardReplicaNum)
		} else {
			s.stats.recentSendSkip.Add(1)
		}
		return false
	}
	return true
}

func (s *ShardReplica) goSendRecent() {
	for cbd := range s.BucketsToSend {
		if s.sendRecent(cbd.compressedBucketData) {
			s.stats.recentSendSuccess.Add(1)
			s.diskCacheEraseWithLog(cbd.time, "after sending")
		} else {
			if !cbd.onDisk {
				s.diskCachePutWithLog(cbd.compressedBucketData)
			}
			s.appendHistoricBucketsToSend(cbd.compressedBucketData)
		}
	}
}

func (s *ShardReplica) sendHistoric(cbd compressedBucketData, scratchPad *[]byte) {
	var err error

	for {
		nowUnix := uint32(time.Now().Unix())
		if s.checkOutOfWindow(nowUnix, cbd.time) { // should check in for because time passes with attempts
			s.stats.historicOutOfWindowDropped.Add(1)
			return
		}
		if len(cbd.data) == 0 { // Read once, if needed, but only after checking timestamp
			if s.agent.diskCache == nil {
				s.agent.statErrorsDiskReadNotConfigured.AddValueCounter(0, 1)
				return // No data and no disk storage configured, alas
			}
			if cbd.data, err = s.agent.diskCache.GetBucket(s.ShardReplicaNum, cbd.time, scratchPad); err != nil {
				s.client.Client.Logf("Disk Error: diskCache.GetBucket returned error %v for shard %d replica %d (shard-replica %d) bucket %d",
					err, s.ShardKey, s.ReplicaKey, s.ShardReplicaNum, cbd.time)
				s.agent.statErrorsDiskRead.AddValueCounter(0, 1)
				return
			}
			if len(cbd.data) < 4 {
				s.client.Client.Logf("Disk Error: diskCache.GetBucket returned compressed bucket data with size %d for shard %d replica %d (shard-replica %d) bucket %d",
					len(cbd.data), s.ShardKey, s.ReplicaKey, s.ShardReplicaNum, cbd.time)
				s.agent.statErrorsDiskRead.AddValueCounter(0, 1)
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
			if !s.agent.ShardReplicas[spare].alive.Load() {
				time.Sleep(10 * time.Second) // TODO - better idea?
				s.client.Client.Logf("both historic shards are dead, shard %d replica %d (shard-replica %d), time %d, %v",
					s.ShardKey, s.ReplicaKey, s.ShardReplicaNum, cbd.time, err)
				continue
			}
			err = s.agent.ShardReplicas[spare].sendSourceBucketCompressed(context.Background(), cbd, true, true, &resp)
		}

		if err != nil {
			if !data_model.SilentRPCError(err) {
				s.stats.historicSendFailed.Add(1)
			} else {
				s.stats.historicSendSkip.Add(1)
			}
			time.Sleep(time.Second) // TODO - better idea?
			continue
		}
		s.stats.historicSendSuccess.Add(1)
		s.diskCacheEraseWithLog(cbd.time, "after sending historic")
		break
	}
}

func (s *ShardReplica) diskCachePutWithLog(cbd compressedBucketData) {
	if s.agent.diskCache == nil {
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
	if err := s.agent.diskCache.PutBucket(s.ShardReplicaNum, cbd.time, cbd.data); err != nil {
		s.client.Client.Logf("Disk Error: diskCache.PutBucket returned error %v for shard %d replica %d (shard-replica %d) bucket %d",
			err, s.ShardKey, s.ReplicaKey, s.ShardReplicaNum, cbd.time)
		s.agent.statErrorsDiskWrite.AddValueCounter(0, 1)
	}
}

func (s *ShardReplica) diskCacheEraseWithLog(time uint32, place string) {
	if s.agent.diskCache == nil {
		return
	}
	if err := s.agent.diskCache.EraseBucket(s.ShardReplicaNum, time); err != nil {
		s.client.Client.Logf("Disk Error: diskCache.EraseBucket returned error %v for shard %d replica %d (shard-replica %d) %s bucket %d",
			err, s.ShardKey, s.ReplicaKey, s.ShardReplicaNum, place, time)
		s.agent.statErrorsDiskErase.AddValueCounter(0, 1)
	}
}

func (s *ShardReplica) appendHistoricBucketsToSend(cbd compressedBucketData) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.HistoricBucketsDataSize+len(cbd.data) > data_model.MaxHistoricBucketsMemorySize/s.agent.NumShardReplicas() {
		cbd.data = nil
	} else {
		s.HistoricBucketsDataSize += len(cbd.data)
		s.agent.historicBucketsDataSize.Add(int64(len(cbd.data)))
	}
	s.HistoricBucketsToSend = append(s.HistoricBucketsToSend, cbd)
	s.cond.Signal()
}

func (s *ShardReplica) readHistoricSecondLocked() {
	if s.agent.diskCache == nil {
		return
	}
	sec, ok := s.agent.diskCache.ReadNextTailSecond(s.ShardReplicaNum)
	if !ok {
		return
	}
	s.HistoricBucketsToSend = append(s.HistoricBucketsToSend, compressedBucketData{time: sec}) // HistoricBucketsDataSize does not change
}

func (s *ShardReplica) popOldestHistoricSecondLocked() compressedBucketData {
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

func (s *ShardReplica) checkOutOfWindow(nowUnix uint32, timestamp uint32) bool {
	if nowUnix >= data_model.MaxHistoricWindow && timestamp < nowUnix-data_model.MaxHistoricWindow { // Not bother sending, will receive error anyway
		s.client.Client.Logf("Send Disaster: Bucket %d for shard %d replica %d (shard-replica %d) does not fit into full admission window (now is %d), throwing out",
			timestamp, s.ShardKey, s.ReplicaKey, s.ShardReplicaNum, nowUnix)
		s.agent.statLongWindowOverflow.AddValueCounter(float64(nowUnix)-float64(timestamp), 1)

		s.diskCacheEraseWithLog(timestamp, "after throwing out historic")
		return true
	}
	return false
}

func (s *ShardReplica) goSendHistoric() {
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

func (s *ShardReplica) goEraseHistoric() {
	// When all senders are in infinite wait, buckets must still be erased
	// Also, we delete buckets when disk limit is reached. We cannot promise strict limit, because disk cache design is very loose.
	s.mu.Lock()
	defer s.mu.Unlock()
	diskLimit := s.config.MaxHistoricDiskSize / int64(s.agent.NumShardReplicas())
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
			s.client.Client.Logf("Send Disaster: Bucket %d for shard %d replica %d (shard-replica %d) (now is %d) violates disk size limit %d (%d used), throwing out",
				cbd.time, s.ShardKey, s.ReplicaKey, s.ShardReplicaNum, nowUnix, diskLimit, diskUsed)
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
