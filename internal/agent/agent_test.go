// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package agent

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"pgregory.net/rand"

	"github.com/VKCOM/statshouse/internal/pcache"

	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/VKCOM/statshouse/internal/format"
)

var sideEffect uint64

// cpu: 13th Gen Intel(R) Core(TM) i7-1360P
// Benchmark_Original_Hash-16    	10591867	       109.1 ns/op	       0 B/op	       0 allocs/op
func Benchmark_Original_Hash(b *testing.B) {
	var m data_model.MappedMetricHeader
	m.OriginalTagValues[0] = []byte("production")
	m.OriginalTagValues[1] = []byte(os.Args[0])
	m.OriginalTagValues[2] = []byte("short")
	m.OriginalTagValues[3] = []byte("tags")
	m.OriginalTagValues[14] = []byte("AAAA")
	var scratch []byte
	metricInfo := &format.MetricMetaValue{MetricID: 1}
	m.MetricMeta = metricInfo
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		binary.LittleEndian.PutUint32(m.OriginalTagValues[14], uint32(i))
		var sum uint64
		scratch, sum = m.OriginalHash(scratch)
		sideEffect += sum
	}
}

// cpu: 13th Gen Intel(R) Core(TM) i7-1360P
// Benchmark_Original_Marshal-16    	12579855	        88.82 ns/op	       0 B/op	       0 allocs/op
func Benchmark_Original_Marshal(b *testing.B) {
	var m data_model.MappedMetricHeader
	m.OriginalTagValues[0] = []byte("production")
	m.OriginalTagValues[1] = []byte(os.Args[0])
	m.OriginalTagValues[2] = []byte("short")
	m.OriginalTagValues[3] = []byte("tags")
	m.OriginalTagValues[14] = []byte("AAAA")
	var scratch []byte
	metricInfo := &format.MetricMetaValue{MetricID: 1}
	m.MetricMeta = metricInfo
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		binary.LittleEndian.PutUint32(m.OriginalTagValues[14], uint32(i))
		scratch = m.OriginalMarshalAppend(scratch[:0])
		sideEffect += uint64(len(scratch))
	}
}

// cpu: 13th Gen Intel(R) Core(TM) i7-1360P
// Benchmark_XXHash-16    	10919467	       102.8 ns/op	       0 B/op	       0 allocs/op
func Benchmark_XXHash(b *testing.B) {
	var k data_model.Key
	var hash, result uint64
	var buf []byte
	k.STags[5] = "really"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		k.Tags[14]++
		k.Tags[0] = int32(i)
		buf, hash = k.XXHash(buf)
		result += hash
	}
}

// cpu: 13th Gen Intel(R) Core(TM) i7-1360P
// Benchmark_Marshal-16    	12839154	        82.68 ns/op	       0 B/op	       0 allocs/op
func Benchmark_Marshal(b *testing.B) {
	var k data_model.Key
	var result int
	var buf []byte
	k.STags[5] = "really"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		k.Tags[14]++
		k.Tags[0] = int32(i)
		var kb []byte
		buf, kb = k.MarshalAppend(buf[:0])
		result += len(kb)
	}
}

func Test_BelieveTimestampWindow(t *testing.T) {
	// we shift rounded time by this amount in func (s *Shard) resolutionShardFromHashLocked,
	// so it must be multiple of 60.
	if (data_model.BelieveTimestampWindow/60)*60 != data_model.BelieveTimestampWindow {
		t.Fail()
	}
}

func Test_AgentWindow(t *testing.T) {
	// We have primitive queue with 2 slots only. So window must be 1 + (0..1) seconds
	if data_model.AgentWindow <= time.Second || data_model.AgentWindow >= 2*time.Second {
		t.Fail()
	}
}

func Test_SuperQueueLength(t *testing.T) {
	if superQueueLen < 128 {
		t.Fail() // must fit at least 2 minutes plus several seconds queue
	}
	for i := 0; i < 32; i++ {
		if superQueueLen == (1 << i) {
			return
		}
	}
	t.Fail() // keep power of two for efficient % operation
}

func Test_AgentQueue(t *testing.T) {
	config := Config{}
	agent := &Agent{
		config:                            config,
		logF:                              func(f string, a ...any) { fmt.Printf(f, a...) },
		mappingsCache:                     pcache.NewMappingsCache(1024*1024, 86400),
		shardByMetricCount:                1,
		builtinMetricMetaUsageCPU:         *format.BuiltinMetricMetaUsageCPU,
		builtinMetricMetaUsageMemory:      *format.BuiltinMetricMetaUsageMemory,
		builtinMetricMetaHeartbeatVersion: *format.BuiltinMetricMetaHeartbeatVersion,
		builtinMetricMetaHeartbeatArgs:    *format.BuiltinMetricMetaHeartbeatArgs,
	}
	startTime := time.Unix(1000*24*3600, 0) // arbitrary deterministic test time
	nowUnix := uint32(startTime.Unix())

	shard := &Shard{
		config:      config,
		agent:       agent,
		CurrentTime: nowUnix,
		SendTime:    nowUnix - 2, // accept previous seconds at the start of the agent
	}
	for j := 0; j < superQueueLen; j++ {
		shard.SuperQueue[j] = &data_model.MetricsBucket{} // timestamp will be assigned at queue flush
	}
	shard.cond = sync.NewCond(&shard.mu)
	shard.BucketsToPreprocess = make(chan *data_model.MetricsBucket, 1)
	agent.Shards = append(agent.Shards, shard)
	agent.initBuiltInMetrics()

	metric1sec := &format.MetricMetaValue{MetricID: 1, Name: "m1", EffectiveResolution: 1}
	metric5sec := &format.MetricMetaValue{MetricID: 5, Name: "m5", EffectiveResolution: 5}
	// TODO - here we metrics at the perfect moments, odd metrics at wrong moments
	agent.goFlushIteration(startTime)
	testEnsureNoFlush(t, shard)
	agent.goFlushIteration(startTime.Add(time.Second))
	testEnsureFlush(t, shard, nowUnix-2)
	agent.AddCounter(nowUnix, metric1sec, []int32{}, 1)
	agent.AddCounter(nowUnix, metric5sec, []int32{}, 1)
	agent.AddCounter(nowUnix+1, metric1sec, []int32{}, 1)
	agent.AddCounter(nowUnix+1, metric5sec, []int32{}, 1)
	agent.goFlushIteration(startTime.Add(data_model.AgentWindow))
	testEnsureFlush(t, shard, nowUnix-1)
	agent.AddCounter(nowUnix+1, metric1sec, []int32{}, 1)
	agent.AddCounter(nowUnix+1, metric5sec, []int32{}, 1)
	agent.AddCounter(nowUnix+2, metric1sec, []int32{}, 1)
	agent.AddCounter(nowUnix+2, metric5sec, []int32{}, 1)
	agent.goFlushIteration(startTime.Add(2 * time.Second))
	testEnsureNoFlush(t, shard)
	for i := 1; i < 12; i++ { // wait until 5-seconds metrics flushed
		agent.goFlushIteration(startTime.Add(time.Duration(i)*time.Second + data_model.AgentWindow))
		testEnsureFlush(t, shard, nowUnix+uint32(i-1))
	}
}

func testEnsureNoFlush(t *testing.T, shard *Shard) {
	select {
	case <-shard.BucketsToPreprocess:
		t.Fatalf("testEnsureNoFlush")
	default:
	}
}

func testEnsureFlush(t *testing.T, shard *Shard, time uint32) {
	var b *data_model.MetricsBucket
	select {
	case b = <-shard.BucketsToPreprocess:
	default:
		t.Fatalf("testEnsureFlush no flush")
	}
	if b.Time != time {
		t.Fatalf("wrong PreprocessingBucketTime")
	}
	for _, item := range b.MultiItems {
		mustBeTime5 := ((time - 5) / 5) * 5

		if item.Key.Metric == 1 && item.Key.Timestamp != time {
			t.Fatalf("wrong metric 1sec time")
		}
		if item.Key.Metric == 5 && item.Key.Timestamp != mustBeTime5 {
			t.Fatalf("wrong metric 5sec time")
		}
		if item.Key.Timestamp > b.Time { // metric from the future
			t.Fatalf("metric from the future")
		}
	}
}

func Benchmark_SampleFactor(b *testing.B) {
	sampleFactors := map[int32]float64{}
	for i := 0; i < 1000; i++ {
		sampleFactors[int32(i)] = 0.1
	}

	rnd := rand.New()

	var k data_model.Key
	var result uint64
	for i := 0; i < b.N; i++ {
		k.Metric = int32(i & 2047)
		k.Tags[14]++
		k.Tags[0] = int32(i)
		_, ok := data_model.SampleFactor(rnd, sampleFactors, k.Metric)
		if ok {
			result++
		}
	}
}

func Benchmark_AgentApplyMetric(b *testing.B) {
	startTime := time.Unix(1000*24*3600, 0) // arbitrary deterministic test time
	nowUnix := uint32(startTime.Unix())
	config := Config{}
	agent := makeAgent(config, nowUnix)
	m := tlstatshouse.MetricBytes{
		Counter: 1,
		Value:   make([]float64, 1),
	}
	h := data_model.MappedMetricHeader{
		MetricMeta: &format.MetricMetaValue{
			EffectiveResolution: 1,
			PipelineVersion:     3,
		},
	}

	rng := rand.New()
	b.ResetTimer()
	b.ReportAllocs()
	var scratch []byte
	for i := 0; i < b.N; i++ {
		h.Key = randKey(rng, nowUnix, 1)
		agent.ApplyMetric(m, h, format.TagValueIDAggMappingStatusOKCached, &scratch)
	}
}

func randKey(rng *rand.Rand, ts uint32, metricOffset int32) data_model.Key {
	key := data_model.Key{
		Timestamp: ts,
		Metric:    metricOffset + rng.Int31n(100_000),
	}
	tagsN := rng.Int31n(16)
	for t := 0; t < int(tagsN); t++ {
		key.Tags[t] = rng.Int31n(100_000)
	}
	return key
}

func makeAgent(config Config, nowUnix uint32) *Agent {
	agent := &Agent{
		config:        config,
		logF:          func(f string, a ...any) { fmt.Printf(f, a...) },
		mappingsCache: pcache.NewMappingsCache(1024*1024, 86400),
	}
	agent.Shards = make([]*Shard, 5)
	for i := range agent.Shards {
		shard := &Shard{
			ShardNum:    i,
			config:      config,
			agent:       agent,
			CurrentTime: nowUnix,
			SendTime:    nowUnix - 2, // accept previous seconds at the start of the agent
		}
		for j := 0; j < superQueueLen; j++ {
			shard.SuperQueue[j] = &data_model.MetricsBucket{} // timestamp will be assigned at queue flush
		}
		shard.cond = sync.NewCond(&shard.mu)
		agent.Shards[i] = shard
	}
	agent.initBuiltInMetrics()
	return agent
}
