// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package prometheus

import (
	"sync"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/textparse"
)

type scrapeCache struct {
	metadata metricCache

	dropped      map[string]struct{}
	droppedMtx   *sync.RWMutex
	droppedBytes uint64
}

func newScrapeCache() *scrapeCache {
	return &scrapeCache{
		metadata: metricCache{},

		droppedMtx: &sync.RWMutex{},
		dropped:    map[string]struct{}{},
	}
}

func (cache *scrapeCache) CleanCache() {
	cache.metadata = metricCache{}
}

func (cache *scrapeCache) AddDropped(s string) {
	cache.droppedMtx.Lock()
	defer cache.droppedMtx.Unlock()
	cache.dropped[s] = struct{}{}
	cache.droppedBytes += uint64(len(s))
}

func (cache *scrapeCache) IsDropped(series []byte) bool {
	cache.droppedMtx.RLock()
	defer cache.droppedMtx.RUnlock()
	_, ok := cache.dropped[string(series)]
	return ok
}

func (cache *scrapeCache) setType(metricName []byte, typ textparse.MetricType) {
	cache.metadata.processTextParseType(metricName, typ)
}

func (cache *scrapeCache) getMeta(ls []labels.Label) (cacheMetric, bool) {
	return cache.metadata.getMetric(ls)
}
