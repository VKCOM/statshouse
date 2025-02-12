// Copyright 2025 V Kontakte LLC
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
	"sync"
	"sync/atomic"
	"time"

	"github.com/vkcom/statshouse/internal/compress"
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

	clientField tlstatshouse.Client

	// aggregator is considered live at start.
	// then, if K of L last recent conveyor sends fail, it is considered dead and keepalive process started
	// if L of L keep alive probes succeed, aggregator is considered live again

	// if original aggregator is live, data is sent to it
	// if original aggregator is dead, spare is selected by time % num_spares
	//      if spare is live, data is sent to it
	//      if spare is also dead, data is sent to original
	lastSendSuccessful []bool

	stats *shardStat
}

func (s *ShardReplica) client() tlstatshouse.Client {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.clientField
}

func (s *ShardReplica) FillStats(stats map[string]string) {
	s.stats.fillStats(stats)
}

func (s *ShardReplica) sendSourceBucket2Compressed(ctx context.Context, cbd compressedBucketData, sendMoreBytes int, historic bool, spare bool, ret *string) error {
	extra := rpc.InvokeReqExtra{FailIfNoConnection: true}
	originalSize, compressedData, _ := compress.DeFrame(cbd.data)
	args := tlstatshouse.SendSourceBucket2{
		Time:           cbd.time,
		BuildCommit:    build.Commit(),
		BuildCommitTs:  build.CommitTimestamp(),
		OriginalSize:   originalSize,
		CompressedData: string(compressedData), // unsafe.String(unsafe.SliceData(compressedData), len(compressedData)), // we either convert to string here, or convert mappings in response to string there, this is less dangerous because 100% local
	}
	s.fillProxyHeader(&args.FieldsMask, &args.Header)
	args.SetHistoric(historic)
	args.SetSpare(spare)

	c := s.client()
	if c.Address != "" { // Skip sending to "empty" shards. Provides fast way to answer "what if there were more shards" question
		//if err := s.client.SendSourceBucket2(ctx, args, &extra, ret); err != nil {
		//	return err
		//}
		var err error
		// copy SendSourceBucket2 method to add more bytes
		req := c.Client.GetRequest()
		req.ActorID = c.ActorID
		req.FunctionName = "statshouse.sendSourceBucket2"
		req.Extra = extra.RequestExtra
		req.FailIfNoConnection = extra.FailIfNoConnection
		rpc.UpdateExtraTimeout(&req.Extra, c.Timeout)
		req.Body, err = args.WriteBoxedGeneral(req.Body)
		if err != nil {
			return fmt.Errorf("failed to serialize statshouse.sendSourceBucket2 request: %w", err)
		}
		if sendMoreBytes > 0 {
			if sendMoreBytes > data_model.MaxSendMoreData {
				sendMoreBytes = data_model.MaxSendMoreData
			}
			req.Body = append(req.Body, make([]byte, sendMoreBytes)...)
		}
		resp, err := c.Client.Do(ctx, c.Network, c.Address, req)
		if resp != nil {
			extra.ResponseExtra = resp.Extra
		}
		defer c.Client.PutResponse(resp)
		if err != nil {
			return fmt.Errorf("statshouse.sendSourceBucket request to %s://%d@%s failed: %w", c.Network, c.ActorID, c.Address, err)
		}
		if ret != nil {
			if _, err = args.ReadResult(resp.Body, ret); err != nil {
				return fmt.Errorf("failed to deserialize statshouse.sendSourceBucket2 response from %s://%d@%s: %w", c.Network, c.ActorID, c.Address, err)
			}
		}
		return nil
	}
	return nil
}

func (s *ShardReplica) sendSourceBucket3Compressed(ctx context.Context, cbd compressedBucketData, sendMoreBytes int, historic bool, spare bool, response *tlstatshouse.SendSourceBucket3Response) error {
	extra := rpc.InvokeReqExtra{FailIfNoConnection: true}
	originalSize, compressedData, _ := compress.DeFrame(cbd.data)
	args := tlstatshouse.SendSourceBucket3{
		Time:           cbd.time,
		BuildCommit:    build.Commit(),
		BuildCommitTs:  build.CommitTimestamp(),
		OriginalSize:   originalSize,
		CompressedData: string(compressedData), // unsafe.String(unsafe.SliceData(compressedData), len(compressedData)), // we either convert to string here, or convert mappings in response to string there, this is less dangerous because 100% local
	}
	if sendMoreBytes > 0 {
		if sendMoreBytes > data_model.MaxSendMoreData {
			sendMoreBytes = data_model.MaxSendMoreData
		}
		args.SendMoreBytes = string(make([]byte, sendMoreBytes))
	}
	s.fillProxyHeader(&args.FieldsMask, &args.Header)
	args.SetHistoric(historic)
	args.SetSpare(spare)

	client := s.client()
	if client.Address != "" { // Skip sending to "empty" shards. Provides fast way to answer "what if there were more shards" question
		if err := client.SendSourceBucket3(ctx, args, &extra, response); err != nil {
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
	client := s.client()
	err = client.TestConnection2Bytes(ctx, args, &extra, &ret)
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
	if s.agent.envLoader != nil {
		e := s.agent.envLoader.Load()
		if len(e.Owner) != 0 {
			header.SetOwner([]byte(e.Owner), fieldsMask)
		}
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
	if s.agent.envLoader != nil {
		e := s.agent.envLoader.Load()
		if len(e.Owner) != 0 {
			header.SetOwner(e.Owner, fieldsMask)
		}
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
		statusTag := int32(format.TagOKConnection)
		rpcCodeTag := int32(0)
		if err != nil {
			var rpcError *rpc.Error
			if errors.Is(err, rpc.ErrClientConnClosedNoSideEffect) || errors.Is(err, rpc.ErrClientConnClosedSideEffect) || errors.Is(err, rpc.ErrClientClosed) {
				statusTag = format.TagNoConnection
			} else if errors.As(err, &rpcError) {
				statusTag = format.TagRPCError
				rpcCodeTag = rpcError.Code
			} else if errors.Is(err, context.DeadlineExceeded) {
				statusTag = format.TagTimeoutError
			} else {
				statusTag = format.TagOtherError
			}
		}
		s.agent.AddValueCounter(0, format.BuiltinMetricMetaSrcTestConnection,
			[]int32{0, s.agent.componentTag, statusTag, rpcCodeTag, format.AggShardTag: s.ShardKey, format.AggReplicaTag: s.ReplicaKey},
			seconds, 1)
		if aggTimeDiff != 0 { // never in case of err != nil
			s.agent.AddValueCounter(0, format.BuiltinMetricMetaAgentAggregatorTimeDiff,
				[]int32{0, s.agent.componentTag, format.AggShardTag: s.ShardKey, format.AggReplicaTag: s.ReplicaKey},
				aggTimeDiff.Seconds(), 1)
			if aggTimeDiff.Abs() > 2*time.Second {
				s.agent.logF("WARNING: time difference with aggregator is %v, more then 2 seconds, agent will be working poorly", aggTimeDiff)
			}
		}
	}
}
