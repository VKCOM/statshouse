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
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/vkgo/build"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
)

type ShardReplica struct {
	// Never change, so do not require protection
	agent           *Agent
	ShardReplicaNum int
	ShardKey        int32
	ReplicaKey      int32

	mu     sync.Mutex
	config Config // can change if remotely updated

	alive atomic.Bool

	timeSpreadDelta time.Duration // randomly spread bucket sending through second between sources/machines

	client tlstatshouse.Client

	// aggregator is considered live at start.
	// then, if K of L last recent conveyor sends fail, it is considered dead and keepalive process started
	// if L of L keep alive probes succeed, aggregator is considered live again

	// if original aggregator is live, data is sent to it
	// if original aggregator is dead, spare is selected by time % num_spares
	//      if spare is live, data is sent to it
	//      if spare is also dead, data is sent to original
	lastSendSuccessful []bool

	successTestConnectionDurationBucket      *BuiltInItemValue
	aggTimeDiffBucket                        *BuiltInItemValue
	noConnectionTestConnectionDurationBucket *BuiltInItemValue
	failedTestConnectionDurationBucket       *BuiltInItemValue
	rpcErrorTestConnectionDurationBucket     *BuiltInItemValue
	timeoutTestConnectionDurationBucket      *BuiltInItemValue

	stats *shardStat
}

func (s *ShardReplica) FillStats(stats map[string]string) {
	s.stats.fillStats(stats)
}

func (s *ShardReplica) sendSourceBucketCompressed(ctx context.Context, cbd compressedBucketData, historic bool, spare bool, ret *[]byte, shard *Shard) error {
	extra := rpc.InvokeReqExtra{FailIfNoConnection: true}
	args := tlstatshouse.SendSourceBucket2Bytes{
		Time:            cbd.time,
		BuildCommit:     []byte(build.Commit()),
		BuildCommitDate: s.agent.commitDateTag,
		BuildCommitTs:   s.agent.commitTimestamp,
		QueueSizeDisk:   math.MaxInt32,
		QueueSizeMemory: math.MaxInt32,
		OriginalSize:    binary.LittleEndian.Uint32(cbd.data),
		CompressedData:  cbd.data[4:],
	}
	s.fillProxyHeaderBytes(&args.FieldsMask, &args.Header)
	args.SetHistoric(historic)
	args.SetSpare(spare)

	sizeMem := shard.HistoricBucketsDataSizeMemory()
	if sizeMem < math.MaxInt32 {
		args.QueueSizeMemory = int32(sizeMem)
	}
	sizeDiskTotal, sizeDiskUnsent := shard.HistoricBucketsDataSizeDisk()
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
	if s.agent.envLoader != nil {
		env := s.agent.envLoader.Load()
		args.SetOwner([]byte(env.Owner))
	}
	if s.client.Address != "" { // Skip sending to "empty" shards. Provides fast way to answer "what if there were more shards" question
		if err := s.client.SendSourceBucket2Bytes(ctx, args, &extra, ret); err != nil {
			return err
		}
	}
	return nil
}

func (s *ShardReplica) doTestConnection(ctx context.Context) (aggTimeDiff time.Duration, duration time.Duration, err error) {
	extra := rpc.InvokeReqExtra{FailIfNoConnection: true}
	args := tlstatshouse.TestConnection2Bytes{}
	s.fillProxyHeaderBytes(&args.FieldsMask, &args.Header)

	var ret []byte

	start := time.Now()
	err = s.client.TestConnection2Bytes(ctx, args, &extra, &ret)
	finish := time.Now()
	duration = finish.Sub(start)
	if err == nil && len(ret) >= 8 {
		unixNano := int64(binary.LittleEndian.Uint64(ret))
		aggTime := time.Unix(0, unixNano)
		if aggTime.Before(start) {
			aggTimeDiff = aggTime.Sub(start) // negative
		} else if aggTime.After(finish) {
			aggTimeDiff = aggTime.Sub(finish)
		}
	}
	return aggTimeDiff, duration, err
}

func (s *ShardReplica) fillProxyHeaderBytes(fieldsMask *uint32, header *tlstatshouse.CommonProxyHeaderBytes) {
	*header = tlstatshouse.CommonProxyHeaderBytes{
		ShardReplica:      int32(s.ShardReplicaNum),
		ShardReplicaTotal: int32(s.agent.NumShardReplicas()),
		HostName:          s.agent.hostName,
		ComponentTag:      s.agent.componentTag,
		BuildArch:         s.agent.buildArchTag,
	}
	data_model.SetProxyHeaderBytesStagingLevel(header, fieldsMask, s.agent.stagingLevel)
}

func (s *ShardReplica) fillProxyHeader(fieldsMask *uint32, header *tlstatshouse.CommonProxyHeader) {
	*header = tlstatshouse.CommonProxyHeader{
		ShardReplica:      int32(s.ShardReplicaNum),
		ShardReplicaTotal: int32(s.agent.NumShardReplicas()),
		HostName:          string(s.agent.hostName),
		ComponentTag:      s.agent.componentTag,
		BuildArch:         s.agent.buildArchTag,
	}
	data_model.SetProxyHeaderStagingLevel(header, fieldsMask, s.agent.stagingLevel)
}

// We see no reason to carefully stop/wait this loop at shutdown
func (s *ShardReplica) goTestConnectionLoop() {
	calcHalfOfMinute := func() time.Duration {
		n := time.Now()
		return n.Truncate(time.Minute).Add(time.Minute + s.timeSpreadDelta*60).Sub(n)
	}
	for {
		time.Sleep(calcHalfOfMinute()) // todo graceful
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		aggTimeDiff, duration, err := s.doTestConnection(ctx)
		cancel()
		seconds := duration.Seconds()
		if err == nil {
			s.successTestConnectionDurationBucket.AddValueCounter(seconds, 1)
			if aggTimeDiff != 0 {
				s.aggTimeDiffBucket.AddValueCounter(aggTimeDiff.Seconds(), 1)
			}
		} else {
			var rpcError rpc.Error
			if errors.Is(err, rpc.ErrClientConnClosedNoSideEffect) || errors.Is(err, rpc.ErrClientConnClosedSideEffect) || errors.Is(err, rpc.ErrClientClosed) {
				s.noConnectionTestConnectionDurationBucket.AddValueCounter(seconds, 1)
			} else if errors.Is(err, &rpcError) {
				s.rpcErrorTestConnectionDurationBucket.AddValueCounter(seconds, 1)
			} else if errors.Is(err, context.DeadlineExceeded) {
				s.timeoutTestConnectionDurationBucket.AddValueCounter(seconds, 1)
			} else {
				s.failedTestConnectionDurationBucket.AddValueCounter(seconds, 1)
			}
		}
	}
}

func (s *ShardReplica) InitBuiltInMetric() {
	// Unfortunately we do not know aggregator host tag.
	s.successTestConnectionDurationBucket = s.agent.CreateBuiltInItemValue(data_model.AggKey(0,
		format.BuiltinMetricIDSrcTestConnection,
		[format.MaxTags]int32{0, s.agent.componentTag, format.TagOKConnection}, 0, s.ShardKey, s.ReplicaKey), format.BuiltinMetricMetaSrcTestConnection)
	s.noConnectionTestConnectionDurationBucket = s.agent.CreateBuiltInItemValue(data_model.AggKey(0,
		format.BuiltinMetricIDSrcTestConnection,
		[format.MaxTags]int32{0, s.agent.componentTag, format.TagNoConnection}, 0, s.ShardKey, s.ReplicaKey), format.BuiltinMetricMetaSrcTestConnection)
	s.failedTestConnectionDurationBucket = s.agent.CreateBuiltInItemValue(data_model.AggKey(0,
		format.BuiltinMetricIDSrcTestConnection,
		[format.MaxTags]int32{0, s.agent.componentTag, format.TagOtherError}, 0, s.ShardKey, s.ReplicaKey), format.BuiltinMetricMetaSrcTestConnection)
	s.rpcErrorTestConnectionDurationBucket = s.agent.CreateBuiltInItemValue(data_model.AggKey(0,
		format.BuiltinMetricIDSrcTestConnection,
		[format.MaxTags]int32{0, s.agent.componentTag, format.TagRPCError}, 0, s.ShardKey, s.ReplicaKey), format.BuiltinMetricMetaSrcTestConnection)
	s.timeoutTestConnectionDurationBucket = s.agent.CreateBuiltInItemValue(data_model.AggKey(0,
		format.BuiltinMetricIDSrcTestConnection,
		[format.MaxTags]int32{0, s.agent.componentTag, format.TagTimeoutError}, 0, s.ShardKey, s.ReplicaKey), format.BuiltinMetricMetaSrcTestConnection)

	s.aggTimeDiffBucket = s.agent.CreateBuiltInItemValue(data_model.AggKey(0,
		format.BuiltinMetricIDAgentAggregatorTimeDiff,
		[format.MaxTags]int32{0, s.agent.componentTag}, 0, s.ShardKey, s.ReplicaKey), format.BuiltinMetricMetaAgentAggregatorTimeDiff)
}
