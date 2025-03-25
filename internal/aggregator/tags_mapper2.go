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

	"github.com/vkcom/statshouse/internal/agent"
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/metajournal"
	"github.com/vkcom/statshouse/internal/pcache"
)

type unknownTag struct { // fits well into cache line
	time uint32
	hits uint32
}

type configTagsMapper2 struct {
	MaxUnknownTagsInBucket    int // keep for low at first, then increase gradually
	MaxCreateTagsPerIteration int // keep for low at first, then increase gradually
	MaxLoadTagsPerIteration   int // keep for low at first, then increase gradually
	TagHitsToCreate           int // if used in 10 different seconds, then create
	MaxUnknownTagsToKeep      int
	MaxSendTagsToAgent        int
}

type tagsMapper2 struct {
	agg           *Aggregator
	sh2           *agent.Agent
	metricStorage *metajournal.MetricsStorage
	loader        *metajournal.MetricMetaLoader

	mu              sync.Mutex
	unknownTags     map[string]unknownTag // collect statistics here
	unknownTagsList []string              // ordered list of keys. IF unknownTags is large, but does not change, we will try to load each key once and stop until new keys are added.
	createTags      map[string]format.CreateMappingExtra

	config configTagsMapper2
}

// TODO make unknownTagsList algo.CircularSlice[string]

func NewTagsMapper2(agg *Aggregator, sh2 *agent.Agent, metricStorage *metajournal.MetricsStorage, loader *metajournal.MetricMetaLoader) *tagsMapper2 {
	ms := &tagsMapper2{
		agg:           agg,
		sh2:           sh2,
		metricStorage: metricStorage,
		loader:        loader,
		unknownTags:   map[string]unknownTag{},
		createTags:    map[string]format.CreateMappingExtra{},
		config:        agg.configR.configTagsMapper2,
	}
	return ms
}

func (ms *tagsMapper2) SetConfig(c configTagsMapper2) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.config = c
}

func (ms *tagsMapper2) UnknownTagsLen() int {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return len(ms.unknownTags)
}

func (ms *tagsMapper2) AddUnknownTags(unknownTags map[string]format.CreateMappingExtra, time uint32) (
	unknownMapRemove int, unknownMapAdd int, unknownListAdd int, createMapAdd int, avgRemovedHits float64) {
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
		u, ok := ms.unknownTags[k]
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
			if !ok && len(ms.unknownTagsList) < ms.config.MaxUnknownTagsToKeep {
				unknownListAdd++
				ms.unknownTagsList = append(ms.unknownTagsList, k)
			}
		}
	}
	return
}

func (ms *tagsMapper2) goRun() {
	var pairs []pcache.MappingPair
	for { // no reason for graceful shutdown
		time.Sleep(500 * time.Millisecond) // arbitrary delay to reduce meta DDOS
		createTags, loadTags, maxCreateTagsPerIteration := ms.getTagsToCreateOrLoad()
		pairs = pairs[:0]
		counter := 0
		for str, extra := range createTags {
			counter++
			if counter > maxCreateTagsPerIteration {
				break // simply forget the rest, will load/create more on the next iteration
			}
			tagValue := ms.createTag(str, extra) // tagValue might be 0 or -1, they are ignored by AddValues
			pairs = append(pairs, pcache.MappingPair{Str: str, Value: tagValue})
		}
		for _, str := range loadTags { // also limited to maxCreateOrLoadTagsPerIteration
			extra := format.CreateMappingExtra{
				Create: false, // for documenting intent
				Host:   ms.agg.aggregatorHost,
			}
			tagValue := ms.createTag(str, extra) // tagValue might be 0 or -1, they are ignored by AddValues
			pairs = append(pairs, pcache.MappingPair{Str: str, Value: tagValue})
		}
		nowUnix := uint32(time.Now().Unix())
		ms.agg.mappingsCache.AddValues(nowUnix, pairs)
	}
}

func (ms *tagsMapper2) createTag(str string, extra format.CreateMappingExtra) int32 {
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
		float64(keyValue), 1, data_model.TagUnionBytes{I: extra.Host}, extra.Aera)
	return keyValue
}

func (ms *tagsMapper2) getTagsToCreateOrLoad() (map[string]format.CreateMappingExtra, []string, int) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	createTags := ms.createTags
	ms.createTags = map[string]format.CreateMappingExtra{}
	loadTags := ms.unknownTagsList
	if len(loadTags) > ms.config.MaxLoadTagsPerIteration {
		loadTags = loadTags[:ms.config.MaxLoadTagsPerIteration]
	}
	ms.unknownTagsList = ms.unknownTagsList[len(loadTags):]
	return createTags, loadTags, ms.config.MaxCreateTagsPerIteration
}
