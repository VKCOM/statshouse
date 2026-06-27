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
	KeepTime                  int // if not used for the long time, it is removed from unknownTags
	MaxSendTagsToAgent        int
}

type tagsMapper3 struct {
	agg           *Aggregator
	sh2           *agent.Agent
	metricStorage *metajournal.MetricsStorage
	loader        metajournal.MetadataLoader

	mu          sync.Mutex
	unknownTags map[string]unknownTag // collect statistics here
	createTags  map[string]createMappingExtra

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

func (ms *tagsMapper3) UnknownTagsLen() int {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return len(ms.unknownTags)
}

func (ms *tagsMapper3) AddUnknownTags(unknownTags map[string]createMappingExtra, time uint32) (
	unknownMapRemove int, unknownMapAdd int, createMapAdd int, avgRemovedHits float64, avgRemovedTotal float64) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	var sumHits int64
	var sumTotal int64
	for k, v := range ms.unknownTags { // can delete a bit more items than strictly necessary
		if len(ms.unknownTags)+len(unknownTags) <= ms.config.MaxUnknownTagsToKeep &&
			int64(time) < int64(v.time)+int64(ms.config.KeepTime) {
			// second condition deletes without limit, because it reduces ms.unknownTags so the next call is faster.
			break
		}
		unknownMapRemove++
		sumHits += int64(v.hits)
		sumTotal += v.total
		delete(ms.unknownTags, k)
	}
	if unknownMapRemove != 0 {
		avgRemovedHits = float64(sumHits) / float64(unknownMapRemove)
		avgRemovedTotal = float64(sumTotal) / float64(unknownMapRemove)
	}
	for k, v := range unknownTags {
		u := ms.unknownTags[k]
		u.total += v.total
		if time > u.time {
			u.time = time
			u.hits++
			if int(u.hits) > ms.config.TagHitsToCreate && u.total > int64(ms.config.TagTotalToCreate) &&
				len(ms.createTags) < ms.config.MaxCreateTagsPerIteration {
				createMapAdd++
				ms.createTags[k] = v
				u.hits = 0
				u.total = 0
				// we do not delete from ms.unknownTags, because it will be most likely added back immediately,
				// but we clear counter and total, so we will not add to ms.createTags every iteration.
			}
			unknownMapAdd++
		}
		ms.unknownTags[k] = u
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
