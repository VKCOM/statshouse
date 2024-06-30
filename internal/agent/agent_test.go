// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package agent

import (
	"testing"
	"time"

	"github.com/vkcom/statshouse/internal/data_model"

	"pgregory.net/rand"
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
