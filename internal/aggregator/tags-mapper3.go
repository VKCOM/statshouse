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
	time uint32
	hits uint32
}

type configTagsMapper3 struct {
	MaxUnknownTagsInBucket    int // keep for low at first, then increase gradually
	MaxCreateTagsPerIteration int // keep for low at first, then increase gradually
	MaxLoadTagsPerIteration   int // deprecated: keep for low at first, then increase gradually
	TagHitsToCreate           int // if used in 10 different seconds, then create
	MaxUnknownTagsToKeep      int
	MaxSendTagsToAgent        int
}

type tagsMapper3 struct {
	agg           *Aggregator
	sh2           *agent.Agent
	metricStorage *metajournal.MetricsStorage
	loader        *metajournal.MetricMetaLoader

	mu          sync.Mutex
	unknownTags map[string]unknownTag // collect statistics here
	createTags  map[string]data_model.CreateMappingExtra

	config configTagsMapper3
}

func NewTagsMapper3(agg *Aggregator, sh2 *agent.Agent, metricStorage *metajournal.MetricsStorage, loader *metajournal.MetricMetaLoader) *tagsMapper3 {
	ms := &tagsMapper3{
		agg:           agg,
		sh2:           sh2,
		metricStorage: metricStorage,
		loader:        loader,
		unknownTags:   map[string]unknownTag{},
		createTags:    map[string]data_model.CreateMappingExtra{},
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

func (ms *tagsMapper3) AddUnknownTags(unknownTags map[string]data_model.CreateMappingExtra, time uint32) (
	unknownMapRemove int, unknownMapAdd int, createMapAdd int, avgRemovedHits float64) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	var sumHits int64
	for k, v := range ms.unknownTags { // can delete a bit more items than strictly necessary
		if len(ms.unknownTags)+len(unknownTags) <= ms.config.MaxUnknownTagsToKeep {
			break
		}
		unknownMapRemove++
		sumHits += int64(v.hits)
		delete(ms.unknownTags, k) // but stays in list, so list and map do not correspond 1-1 to each other
	}
	if unknownMapRemove != 0 {
		avgRemovedHits = float64(sumHits) / float64(unknownMapRemove)
	}
	for k, v := range unknownTags {
		u := ms.unknownTags[k]
		if time > u.time {
			u.time = time
			u.hits++
			if int(u.hits) > ms.config.TagHitsToCreate {
				createMapAdd++
				ms.createTags[k] = v
				u.hits = 0
				// we do not delete from ms.unknownTags, because it will be most likely added back immediately,
				// but we clear counter, so we will not add to ms.createTags every iteration
			}
			ms.unknownTags[k] = u
			unknownMapAdd++
		}
	}
	return
}

func (ms *tagsMapper3) goRun() {
	for { // no reason for graceful shutdown
		time.Sleep(500 * time.Millisecond) // arbitrary delay to reduce meta DDOS
		createTags, maxCreateTagsPerIteration := ms.getTagsToCreate()
		counter := 0
		for str, extra := range createTags {
			counter++
			if counter > maxCreateTagsPerIteration {
				break // simply forget the rest, will load/create more on the next iteration
			}
			ms.createTag(str, extra)
		}
	}
}

func (ms *tagsMapper3) createTag(str string, extra data_model.CreateMappingExtra) {
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
	keyValue, c, _, _ := ms.loader.GetTagMapping(context.Background(), str, metricName, extra.Create)
	ms.sh2.AddValueCounterHostAERA(0, format.BuiltinMetricMetaAggMappingCreated,
		[]int32{extra.ClientEnv, 0, 0, 0, metricID, c, extra.TagIDKey, format.TagValueIDAggMappingCreatedConveyorNew, unknownMetricID, keyValue},
		float64(keyValue), 1, extra.HostTag, extra.Aera)
}

func (ms *tagsMapper3) getTagsToCreate() (map[string]data_model.CreateMappingExtra, int) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	createTags := ms.createTags
	ms.createTags = map[string]data_model.CreateMappingExtra{}
	return createTags, ms.config.MaxCreateTagsPerIteration
}
