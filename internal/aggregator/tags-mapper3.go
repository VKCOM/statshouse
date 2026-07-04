// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package aggregator

import (
	"context"
	"sync"
	"time"

	"github.com/VKCOM/statshouse/internal/agent"
	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/format"
	"github.com/VKCOM/statshouse/internal/metajournal"
	"pgregory.net/rand"
)

type unknownTag struct { // fits well into cache line
	time  uint32
	hits  uint32
	total int64
}

type createMappingExtra struct {
	MetricID  int32
	TagIDKey  int32
	ClientEnv int32
	Aera      data_model.AgentEnvRouteArch
	HostTag   data_model.TagUnion
	total     int64 // statistics
}

type configTagsMapper3 struct {
	MaxCreateTagsPerIteration int // limit to protect against all kind of errors
	TagHitsToCreate           int // if used in N different seconds, then create
	TagTotalToCreate          int // if inserted in database K times, then create
	MaxUnknownTagsToKeep      int
	KeepTime                  int // if not used for some time, string is removed from unknownTags
	MaxSendTagsToAgent        int
}

type tagsMapper3 struct {
	agg           *Aggregator
	sh2           *agent.Agent
	metricStorage *metajournal.MetricsStorage
	loader        metajournal.MetadataLoader

	mu               sync.Mutex
	sampleFactorLog2 int
	unknownTags      map[string]unknownTag // collect statistics here
	sumHits          int64
	sumTotal         int64
	sumTime          int64
	sfReduceTime     uint32

	createTags map[string]createMappingExtra

	config configTagsMapper3
}

func NewTagsMapper3(agg *Aggregator, sh2 *agent.Agent, metricStorage *metajournal.MetricsStorage, loader metajournal.MetadataLoader) *tagsMapper3 {
	ms := &tagsMapper3{
		agg:           agg,
		sh2:           sh2,
		metricStorage: metricStorage,
		loader:        loader,
		unknownTags:   map[string]unknownTag{},
		createTags:    map[string]createMappingExtra{},
		config:        agg.configR.configTagsMapper3,
	}
	return ms
}

func (ms *tagsMapper3) SetConfig(c configTagsMapper3) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.config = c
}

func (ms *tagsMapper3) UnknownTagsStats(now uint32) (totalLen int, sampleFactor int, avgHits float64, avgTotal float64, avgTime float64) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if len(ms.unknownTags) == 0 {
		return len(ms.unknownTags), ms.sampleFactorLog2, 0, 0, 0
	}
	le := float64(len(ms.unknownTags))
	return len(ms.unknownTags), ms.sampleFactorLog2, float64(ms.sumHits) / le, float64(ms.sumTotal) / le, float64(now) - float64(ms.sumTime)/le
}

func (ms *tagsMapper3) addUnknownTagLocked(time uint32, rng *rand.Rand, k string, v createMappingExtra) bool {
	c, ok := ms.unknownTags[k]
	if ok {
		ms.removeTotal(c)
		c.total += v.total
		if time > c.time {
			c.time = time
			c.hits++
		}
		if int(c.hits) > ms.config.TagHitsToCreate && c.total > int64(ms.config.TagTotalToCreate) {
			delete(ms.unknownTags, k)
			return true
		}
	} else {
		sf := int64(1) << ms.sampleFactorLog2
		if v.total < sf && v.total <= rng.Int63n(sf) { // first condition is an optimization
			return false
		}
		// this can stop for relatively long time, contributing to insert time, but very rarely.
		// first condition prevents infinite loop
		for len(ms.unknownTags) > 1 && len(ms.unknownTags) >= ms.config.MaxUnknownTagsToKeep {
			ms.resample(time, rng)
		}
		c = unknownTag{
			time:  time,
			hits:  1,
			total: v.total,
		}
	}
	ms.addTotal(c)
	ms.unknownTags[k] = c
	return false
}

func (ms *tagsMapper3) resample(time uint32, rng *rand.Rand) {
	ms.sampleFactorLog2++
	sf := int64(1) << ms.sampleFactorLog2
	for k, v := range ms.unknownTags {
		if int64(time) >= int64(v.time)+int64(ms.config.KeepTime) {
			ms.removeTotal(v)
			delete(ms.unknownTags, k)
			continue
		}
		if v.total >= sf || v.total > rng.Int63n(sf) { // first condition is an optimization
			continue
		}
		ms.removeTotal(v)
		delete(ms.unknownTags, k)
	}
}

func (ms *tagsMapper3) addTotal(v unknownTag) {
	ms.sumHits += int64(v.hits)
	ms.sumTotal += v.total
	ms.sumTime += int64(v.time)
}

func (ms *tagsMapper3) removeTotal(v unknownTag) {
	ms.sumHits -= int64(v.hits)
	ms.sumTotal -= v.total
	ms.sumTime -= int64(v.time)
}

func (ms *tagsMapper3) AddUnknownTags(unknownTags map[string]createMappingExtra, time uint32, rng *rand.Rand) (createMapAdd int) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	numIter := 0
	// work proportional to len(unknownTags).
	for k, v := range ms.unknownTags {
		if int64(time) >= int64(v.time)+int64(ms.config.KeepTime) {
			ms.removeTotal(v)
			delete(ms.unknownTags, k)
		}
		numIter++
		if numIter > 128+4*len(unknownTags) { // tiny constant + 4x multiplier so if 25% of items is stale, enough space is freed to fit all new tags
			break
		}
	}
	if len(ms.unknownTags) >= ms.config.MaxUnknownTagsToKeep/2 {
		ms.sfReduceTime = time
	}
	if ms.sampleFactorLog2 > 0 && len(ms.unknownTags) < ms.config.MaxUnknownTagsToKeep/2 &&
		int64(time) >= int64(ms.sfReduceTime)+int64(ms.config.KeepTime) {
		// reduce sample factor slowly after large load of random strings stops
		ms.sfReduceTime = time
		ms.sampleFactorLog2--
	}
	for k, v := range unknownTags {
		if ms.addUnknownTagLocked(time, rng, k, v) && len(ms.createTags) < ms.config.MaxCreateTagsPerIteration {
			createMapAdd++
			ms.createTags[k] = v
		}
	}
	return
}

func (ms *tagsMapper3) goRun() {
	for { // no reason for graceful shutdown
		time.Sleep(500 * time.Millisecond) // arbitrary delay to reduce meta DDOS
		createTags := ms.getTagsToCreate() // limited by MaxCreateTagsPerIteration
		for str, extra := range createTags {
			ms.createTag(str, extra)
		}
	}
}

func (ms *tagsMapper3) createTag(str string, extra createMappingExtra) {
	var metricID int32
	metricName := ""
	var unknownMetricID int32
	if bm := format.BuiltinMetrics[extra.MetricID]; bm != nil {
		metricID = extra.MetricID
		metricName = bm.Name
	} else if mm := ms.metricStorage.GetMetaMetric(extra.MetricID); mm != nil {
		metricID = extra.MetricID
		metricName = mm.Name
	} else {
		metricID = format.BuiltinMetricIDBudgetUnknownMetric
		metricName = format.BuiltinMetricMetaBudgetUnknownMetric.Name
		unknownMetricID = extra.MetricID
		// Unknown metrics (also loads from caches after initial error, because cache does not store extra). They all share common limit.
		// Journal can be stale, while mapping works.
		// Explicit metric for this situation allows resetting limit from UI, like any other metric
	}
	keyValue, c, _ := ms.loader.GetTagMapping(context.Background(), str, metricName, true)
	ms.sh2.AddValueCounterHostAERA(0, format.BuiltinMetricMetaAggMappingCreated,
		[]int32{extra.ClientEnv, 0, 0, 0, metricID, c, extra.TagIDKey, format.TagValueIDAggMappingCreatedConveyorNew, unknownMetricID, keyValue},
		float64(keyValue), 1, extra.HostTag, extra.Aera)
}

func (ms *tagsMapper3) getTagsToCreate() map[string]createMappingExtra {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	createTags := ms.createTags
	ms.createTags = map[string]createMappingExtra{}
	return createTags
}
