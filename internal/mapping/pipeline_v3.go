// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package mapping

import (
	"time"

	"go4.org/mem"

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tl"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/pcache"
)

type mapPipelineV3 struct {
	mappingCache *pcache.Cache
	autoCreate   *data_model.AutoCreate
}

func newMapPipelineV3(mappingCache *pcache.Cache, ac *data_model.AutoCreate) *mapPipelineV3 {
	return &mapPipelineV3{
		mappingCache: mappingCache,
		autoCreate:   ac,
	}
}

func (mp *mapPipelineV3) Map(args data_model.HandlerArgs, metricInfo *format.MetricMetaValue, h *data_model.MappedMetricHeader) {
	mp.mapAllTags(h, args.MetricBytes)
	if h.IngestionStatus != 0 {
		return
	}
	// validate values only if metirc is valid
	h.IngestionStatus = data_model.ValidateMetricData(args.MetricBytes)
	h.ValuesChecked = true // not used in v3, just to avoid confusion
}

// mapAllTags processes all tags in a single pass, including environment tag
// unlike v2, it doesn't stop on the first invalid tag
func (mp *mapPipelineV3) mapAllTags(h *data_model.MappedMetricHeader, metric *tlstatshouse.MetricBytes) {
	for i := 0; i < len(metric.Tags); i++ {
		v := &metric.Tags[i]
		tagMeta, tagIDKey, valid := data_model.ValidateTag(v, metric, h, mp.autoCreate)
		if !valid {
			continue
		}
		if tagIDKey == 0 { // that tag is not in metric meta
			continue
		}
		switch {
		case tagMeta.SkipMapping:
			h.SetSTag(tagMeta.Index, string(v.Value), tagIDKey)
		case tagMeta.Index == format.StringTopTagIndex:
			h.SValue = v.Value
			if h.IsSKeySet {
				h.TagSetTwiceKey = tagIDKey
			}
			h.IsSKeySet = true
		case len(v.Value) == 0: // this case is also valid for raw values
			h.SetTag(tagMeta.Index, 0, tagIDKey) // we interpret "1" => "vasya", "1" => "petya" as second one overriding the first, but generating a warning
		case tagMeta.Raw:
			id, ok := format.ContainsRawTagValue(mem.B(v.Value))
			if !ok {
				h.InvalidRawValue = v.Value
				h.InvalidRawTagKey = tagIDKey
				continue
			}
			h.SetTag(tagMeta.Index, id, tagIDKey)
		default:
			id, err, found := mp.getTagValueIDCached(h.ReceiveTime, v.Value)
			if err != nil {
				h.SetInvalidString(format.TagValueIDSrcIngestionStatusErrMapTagValueCached, tagIDKey, v.Value)
				continue
			}
			if found {
				h.SetTag(tagMeta.Index, id, tagIDKey)
			} else {
				h.SetSTag(tagMeta.Index, string(v.Value), tagIDKey)
			}
		}
	}
}

func (mp *mapPipelineV3) handleEnvironmentTag(h *data_model.MappedMetricHeader, v *tl.DictionaryFieldStringBytes) {
	var err error
	v.Value, err = format.AppendValidStringValue(v.Value[:0], v.Value)
	if err != nil || len(v.Value) == 0 {
		return
	}

	id, err, found := mp.getTagValueIDCached(h.ReceiveTime, v.Value)
	if err != nil {
		return
	}
	if found {
		h.Key.Tags[0] = id
	}
}

func (mp *mapPipelineV3) getTagValueIDCached(now time.Time, tagValue []byte) (int32, error, bool) {
	r := mp.mappingCache.GetCached(now, tagValue)
	return pcache.ValueToInt32(r.Value), r.Err, r.Found()
}

func (mp *mapPipelineV3) MapEnvironment(metric *tlstatshouse.MetricBytes, h *data_model.MappedMetricHeader) {
	for _, v := range metric.Tags {
		if string(v.Key) != format.EnvTagID {
			continue
		}
		mp.handleEnvironmentTag(h, &v)
		return
	}
}
