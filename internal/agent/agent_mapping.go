// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package agent

import (
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tl"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
	"go4.org/mem"
)

func (s *Agent) Map(args data_model.HandlerArgs, h *data_model.MappedMetricHeader, autoCreate *data_model.AutoCreate) {
	s.mapAllTags(h, args.MetricBytes, autoCreate)
	if h.IngestionStatus != 0 {
		return
	}
	// validate values only if metric is valid
	h.IngestionStatus = data_model.ValidateMetricData(args.MetricBytes)
	h.ValuesChecked = true // not used in v3, just to avoid confusion
}

// mapAllTags processes all tags in a single pass, including environment tag
// unlike v2, it doesn't stop on the first invalid tag
func (s *Agent) mapAllTags(h *data_model.MappedMetricHeader, metric *tlstatshouse.MetricBytes, autoCreate *data_model.AutoCreate) {
	for i := 0; i < len(metric.Tags); i++ {
		v := &metric.Tags[i]
		tagMeta, tagIDKey, valid := data_model.ValidateTag(v, metric, h, autoCreate)
		if !valid {
			continue
		}
		if tagIDKey == 0 { // that tag is not in metric meta
			continue
		}
		var tagValue data_model.TagUnionBytes
		switch {
		case len(v.Value) == 0: // this case is also valid for raw values
		case tagMeta.Raw:
			id, ok := format.ContainsRawTagValue(mem.B(v.Value))
			if !ok {
				h.InvalidRawValue = v.Value
				h.InvalidRawTagKey = tagIDKey
				continue
			}
			tagValue.I = id
		default:
			id, found := s.mappingsCache.GetValueBytes(uint32(h.ReceiveTime.Unix()), v.Value)
			if found {
				tagValue.I = id
			} else {
				tagValue.S = v.Value
			}
		}
		if tagMeta.Index == format.StringTopTagIndex || tagMeta.Index == format.StringTopTagIndexV3 {
			// "_s" is alternative/legacy name for "47". We always have "top" function set for this tag.
			// This tag is not part of resolution hash, so not placed into OriginalTagValues
			// TODO - after old conveyor removed, we can simplify this code by setting tagMeta.Index to 47 for "_s"
			// also we will remove IsSKeySet and use IsTagSet[47] automatically instead
			h.TopValue = tagValue
			if h.IsSKeySet {
				h.TagSetTwiceKey = tagIDKey
			}
			h.IsSKeySet = true
			continue
		}
		if tagValue.I != 0 {
			h.SetTag(tagMeta.Index, tagValue.I, tagIDKey)
		} else {
			h.SetSTag(tagMeta.Index, tagValue.S, tagIDKey) // TODO - remove allocation here
		}
		if tagMeta.Index != format.HostTagIndex { // This tag is not part of resolution hash, so not placed into OriginalTagValues
			h.OriginalTagValues[tagMeta.Index] = v.Value
		}
	}
}

func (s *Agent) mapEnvironmentTag(h *data_model.MappedMetricHeader, v *tl.DictionaryFieldStringBytes) {
	var err error
	v.Value, err = format.AppendValidStringValue(v.Value[:0], v.Value)
	if err != nil || len(v.Value) == 0 {
		return
	}
	id, found := s.mappingsCache.GetValueBytes(uint32(h.ReceiveTime.Unix()), v.Value)
	if found {
		h.Key.Tags[0] = id
	} else {
		h.Key.STags[0] = string(v.Value) // TODO - remove allocation here
	}
}

// Subset of Map which only maps environment and produces no errors. Used to report environment of not found metrics.
func (s *Agent) MapEnvironment(metric *tlstatshouse.MetricBytes, h *data_model.MappedMetricHeader) {
	for _, v := range metric.Tags {
		if string(v.Key) != format.EnvTagID {
			continue
		}
		s.mapEnvironmentTag(h, &v)
		return
	}
}
