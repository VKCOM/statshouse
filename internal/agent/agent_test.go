// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package agent

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"pgregory.net/rand"

	"github.com/mailru/easyjson/opt"
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/sharding"
)

func Benchmark_Hash(b *testing.B) {
	var k data_model.Key
	var result uint64
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		k.Keys[14]++
		k.Keys[0] = int32(i)
		result += k.Hash()
	}
}

func Benchmark_HashSafe(b *testing.B) {
	var k data_model.Key
	var result uint64
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		k.Keys[14]++
		k.Keys[0] = int32(i)
		result += k.HashSafe()
	}
}

func Test_HashSafeUnsafe(t *testing.T) {
	var k data_model.Key
	for i := 0; i < 1000; i++ {
		k.Keys[14]++
		k.Keys[0] = int32(i)
		if k.Hash() != k.HashSafe() {
			t.Fail()
		}
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
	if data_model.AgentWindow <= 0 || data_model.AgentWindow >= time.Second {
		t.Fail()
	}
}

func Test_AgentQueue(t *testing.T) {
	config := Config{}
	agent := &Agent{
		config: config,
		logF:   func(f string, a ...any) { fmt.Printf(f, a...) },
	}
	startTime := time.Unix(1000*24*3600, 0) // arbitrary deterministic test time
	nowUnix := uint32(startTime.Unix())

	shard := &Shard{
		config:          config,
		agent:           agent,
		addBuiltInsTime: nowUnix,
	}
	for r := range shard.CurrentBuckets {
		if r != format.AllowedResolution(r) {
			continue
		}
		ur := uint32(r)
		bucketTime := (nowUnix / ur) * ur
		for sh := 0; sh < r; sh++ {
			shard.CurrentBuckets[r] = append(shard.CurrentBuckets[r], &data_model.MetricsBucket{Time: bucketTime, Resolution: r})
			shard.NextBuckets[r] = append(shard.NextBuckets[r], &data_model.MetricsBucket{Time: bucketTime + ur, Resolution: r})
		}
	}
	shard.cond = sync.NewCond(&shard.mu)
	shard.BucketsToPreprocess = make(chan preprocessorBucketData, 1)
	agent.Shards = append(agent.Shards, shard)

	metric1sec := &format.MetricMetaValue{MetricID: 1, EffectiveResolution: 1}
	metric5sec := &format.MetricMetaValue{MetricID: 5, EffectiveResolution: 5}
	// TODO - here we metrics at the perfect moments, odd metrics at wrong moments
	agent.goFlushIteration(startTime)
	testEnsureNoFlush(t, shard)
	agent.goFlushIteration(startTime.Add(time.Second))
	testEnsureNoFlush(t, shard)
	agent.AddCounterHost(data_model.Key{Timestamp: nowUnix, Metric: 1}, 1, 0, metric1sec)
	agent.AddCounterHost(data_model.Key{Timestamp: nowUnix, Metric: 5}, 1, 0, metric5sec)
	agent.AddCounterHost(data_model.Key{Timestamp: nowUnix + 1, Metric: 1}, 1, 0, metric1sec)
	agent.AddCounterHost(data_model.Key{Timestamp: nowUnix + 1, Metric: 5}, 1, 0, metric5sec)
	agent.goFlushIteration(startTime.Add(time.Second + data_model.AgentWindow))
	testEnsureFlush(t, shard, nowUnix)
	agent.AddCounterHost(data_model.Key{Timestamp: nowUnix + 1, Metric: 1}, 1, 0, metric1sec)
	agent.AddCounterHost(data_model.Key{Timestamp: nowUnix + 1, Metric: 5}, 1, 0, metric5sec)
	agent.AddCounterHost(data_model.Key{Timestamp: nowUnix + 2, Metric: 1}, 1, 0, metric1sec)
	agent.AddCounterHost(data_model.Key{Timestamp: nowUnix + 2, Metric: 5}, 1, 0, metric5sec)
	agent.goFlushIteration(startTime.Add(2 * time.Second))
	testEnsureNoFlush(t, shard)
	for i := 1; i < 12; i++ { // wait until 5-seconds metrics flushed
		agent.goFlushIteration(startTime.Add(time.Duration(i+1)*time.Second + data_model.AgentWindow))
		testEnsureFlush(t, shard, nowUnix+uint32(i))
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
	var cbd preprocessorBucketData
	select {
	case cbd = <-shard.BucketsToPreprocess:
	default:
		t.Fatalf("testEnsureFlush no flush")
	}
	if cbd.time != time {
		t.Fatalf("wrong PreprocessingBucketTime")
	}
	for _, b := range cbd.buckets {
		mustBeTime := (time + 1 - uint32(b.Resolution)) / uint32(b.Resolution) * uint32(b.Resolution)
		if b.Time != mustBeTime {
			t.Fatalf("wrong bucket time")
		}
		for key := range b.MultiItems {
			if key.Metric == 1 && key.Timestamp != mustBeTime {
				t.Fatalf("wrong metric 1sec time")
			}
			if key.Metric == 5 && key.Timestamp != mustBeTime {
				t.Fatalf("wrong metric 5sec time")
			}
			if key.Timestamp < mustBeTime { // metric from the future
				t.Fatalf("metric from the future")
			}
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
		k.Keys[14]++
		k.Keys[0] = int32(i)
		_, ok := data_model.SampleFactor(rnd, sampleFactors, k.Metric)
		if ok {
			result++
		}
	}
}

func Benchmark_sampleFactorDeterministic(b *testing.B) {
	sampleFactors := map[int32]float64{}
	for i := 0; i < 1000; i++ {
		sampleFactors[int32(i)] = 0.1
	}

	var k data_model.Key
	var result uint64
	for i := 0; i < b.N; i++ {
		k.Metric = int32(i & 2047)
		k.Keys[14]++
		k.Keys[0] = int32(i)
		_, ok := data_model.SampleFactorDeterministic(sampleFactors, k, uint32(i))
		if ok {
			result++
		}
	}
}

func Test_AgentSharding(t *testing.T) {
	config := Config{
		ShardByMetric: true,
	}
	agent := &Agent{
		config: config,
		logF:   func(f string, a ...any) { fmt.Printf(f, a...) },
	}
	agent.shardByMetric.Store(config.ShardByMetric)
	startTime := time.Unix(1000*24*3600, 0) // arbitrary deterministic test time
	nowUnix := uint32(startTime.Unix())

	agent.Shards = make([]*Shard, 5)
	for i := range agent.Shards {
		shard := &Shard{
			ShardNum:        i,
			config:          config,
			agent:           agent,
			addBuiltInsTime: nowUnix,
		}
		for r := range shard.CurrentBuckets {
			if r != format.AllowedResolution(r) {
				continue
			}
			ur := uint32(r)
			bucketTime := (nowUnix / ur) * ur
			for sh := 0; sh < r; sh++ {
				shard.CurrentBuckets[r] = append(shard.CurrentBuckets[r], &data_model.MetricsBucket{Time: bucketTime, Resolution: r})
				shard.NextBuckets[r] = append(shard.NextBuckets[r], &data_model.MetricsBucket{Time: bucketTime + ur, Resolution: r})
			}
		}
		shard.cond = sync.NewCond(&shard.mu)
		shard.condPreprocess = sync.NewCond(&shard.mu)
		agent.Shards[i] = shard
	}

	rng := rand.New()
	for i := 0; i < 1000; i++ {
		applyRandCountMetric(agent, rng, nowUnix)
		applyRandValueMetric(agent, rng, nowUnix)
		// we have special logic for unique values for now
		// applyRandUniqueMetric(agent, rng, nowUnix)
	}

	totalCount := 0
	for si, shard := range agent.Shards {
		shardCount := 0
		for _, b := range shard.CurrentBuckets {
			if b == nil {
				continue
			}
			for _, sh := range b {
				if sh == nil {
					continue
				}
				for key := range sh.MultiItems {
					shardCount++
					expectedShardNum := uint32(0)
					if key.Metric > 0 {
						expectedShardNum, _ = sharding.Shard(key, format.MetricSharding{Strategy: format.ShardByMappedTags}, agent.NumShards())
					} else if key.Metric == format.BuiltinMetricIDIngestionStatus {
						expectedShardNum, _ = sharding.Shard(key, format.MetricSharding{Strategy: format.ShardByTag, TagId: opt.OUint32(1)}, agent.NumShards())
					}
					if int(expectedShardNum) != si {
						t.Fatalf("failed for metric %v expected shard %d but got %d", key, expectedShardNum, si)
					}
				}
			}
		}
		t.Log("shard", si, "count", shardCount)
		totalCount += shardCount
	}
	// totalCount is twice as much because each metric is duplicated by __src_ingestion_status
	if totalCount != 4000 {
		t.Fatalf("expected to have 4000 metrics added to shards but got only %d", totalCount)
	}
}

func randKey(rng *rand.Rand, ts uint32, metricOffset int32) data_model.Key {
	key := data_model.Key{
		Timestamp: ts,
		Metric:    metricOffset + rng.Int31n(100_000),
		Keys:      [16]int32{},
	}
	tagsN := rng.Int31n(16)
	for t := 0; t < int(tagsN); t++ {
		key.Keys[t] = rng.Int31n(100_000)
	}
	return key
}

func randResolution(rng *rand.Rand) int {
	resolutions := []int{1, 5, 6, 10, 15, 20, 30, 60}
	return resolutions[rng.Int31n(int32(len(resolutions)))]
}

func applyRandCountMetric(a *Agent, rng *rand.Rand, ts uint32) {
	m := tlstatshouse.MetricBytes{
		Counter: float64(1 + rng.Int31n(100)),
	}
	h := data_model.MappedMetricHeader{
		Key: randKey(rng, ts, 1),
		MetricInfo: &format.MetricMetaValue{
			EffectiveResolution: randResolution(rng),
			Sharding:            format.MetricSharding{Strategy: format.ShardByMappedTags},
		},
	}
	a.ApplyMetric(m, h, format.TagValueIDAggMappingStatusOKCached)
}

func applyRandValueMetric(a *Agent, rng *rand.Rand, ts uint32) {
	count := 1 + rng.Int31n(100)
	m := tlstatshouse.MetricBytes{
		Counter: float64(count),
		Value:   make([]float64, count),
	}
	for i := range m.Value {
		m.Value[i] = rng.Float64()
	}
	h := data_model.MappedMetricHeader{
		Key: randKey(rng, ts, 100_001),
		MetricInfo: &format.MetricMetaValue{
			EffectiveResolution: randResolution(rng),
			Sharding:            format.MetricSharding{Strategy: format.ShardByMappedTags},
		},
	}
	a.ApplyMetric(m, h, format.TagValueIDAggMappingStatusOKCached)
}

// func applyRandUniqueMetric(a *Agent, rng *rand.Rand, ts uint32) {
// 	count := 1 + rng.Int31n(10)
// 	m := tlstatshouse.MetricBytes{
// 		Counter: float64(count),
// 		Unique:  make([]int64, count),
// 	}
// 	for i := range m.Unique {
// 		m.Unique[i] = rng.Int63n(1000)
// 	}
// 	h := data_model.MappedMetricHeader{
// 		Key: randKey(rng, ts, 200_001),
// 		MetricInfo: &format.MetricMetaValue{
// 			EffectiveResolution: randResolution(rng),
// 			ShardUniqueValues:   true,
// 			Sharding:            format.MetricSharding{Strategy: format.ShardByMappedTags},
// 		},
// 	}
// 	a.ApplyMetric(m, h, format.TagValueIDAggMappingStatusOKCached)
// }
