// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package mapping

import (
	"time"

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/pcache"
)

type Mapper struct {
	pipeline *mapPipeline
}

func NewTagsCache(loader pcache.LoaderFunc, suffix string, dc *pcache.DiskCache) *pcache.Cache {
	result := &pcache.Cache{
		Loader:                  loader,
		DiskCache:               dc,
		DiskCacheNamespace:      data_model.TagValueDiskNamespace + suffix,
		MaxMemCacheSize:         data_model.MappingMaxMemCacheSize,
		MaxDiskCacheSize:        data_model.MappingMaxDiskCacheSize,
		SpreadCacheTTL:          true,
		DefaultCacheTTL:         data_model.MappingCacheTTLMinimum,
		DefaultNegativeCacheTTL: data_model.MappingNegativeCacheTTL,
		LoadMinInterval:         data_model.MappingMinInterval,
		Empty: func() pcache.Value {
			var empty pcache.Int32Value
			return &empty
		},
	}
	return result
}

func NewMapper(suffix string, pmcLoader pcache.LoaderFunc, dc *pcache.DiskCache, ac *AutoCreate, metricMapQueueSize int, mapCallback data_model.MapCallbackFunc) *Mapper {
	tagValue := NewTagsCache(pmcLoader, suffix, dc)

	return &Mapper{
		pipeline: newMapPipeline(mapCallback, tagValue, ac, data_model.MappingMaxMetricsInQueue, metricMapQueueSize),
	}
}

func (m *Mapper) TagValueDiskCacheEmpty() bool {
	return m.pipeline.tagValue.DiskCacheEmpty()
}

func (m *Mapper) SetBootstrapValue(now time.Time, key string, v pcache.Value, ttl time.Duration) error {
	return m.pipeline.tagValue.SetBootstrapValue(now, key, v, ttl)
}

func (m *Mapper) Stop() {
	m.pipeline.stop()
}

// cb.MetricInfo must be set from journal. If nil, will lookup allowed built-in metric, otherwise set ingestion status not found
func (m *Mapper) Map(args data_model.HandlerArgs, metricInfo *format.MetricMetaValue, h *data_model.MappedMetricHeader) (done bool) {
	return m.pipeline.Map(args, metricInfo, h)
}

// We wish to know which environment generates 'metric not found' events and other errors
// so we call it even if we had an error
func (m *Mapper) MapEnvironment(metric *tlstatshouse.MetricBytes, h *data_model.MappedMetricHeader) {
	m.pipeline.MapEnvironment(metric, h)
}
