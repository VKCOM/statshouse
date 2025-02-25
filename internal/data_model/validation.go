// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package data_model

import (
	"github.com/vkcom/statshouse/internal/data_model/gen2/tl"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
	"go4.org/mem"
)

func ValidateMetricData(metricBytes *tlstatshouse.MetricBytes) (ingestionStatus int32) {
	if len(metricBytes.Value)+len(metricBytes.Histogram) != 0 && len(metricBytes.Unique) != 0 {
		ingestionStatus = format.TagValueIDSrcIngestionStatusErrValueUniqueBothSet
		return
	}
	if metricBytes.Counter, ingestionStatus = format.ClampCounter(metricBytes.Counter); ingestionStatus != 0 {
		return
	}
	for i, v := range metricBytes.Value {
		if metricBytes.Value[i], ingestionStatus = format.ClampValue(v); ingestionStatus != 0 {
			return
		}
	}
	for i, v := range metricBytes.Histogram {
		if metricBytes.Histogram[i][0], ingestionStatus = format.ClampValue(v[0]); ingestionStatus != 0 {
			return
		}
		if metricBytes.Histogram[i][1], ingestionStatus = format.ClampCounter(v[1]); ingestionStatus != 0 {
			return
		}
	}
	return
}

// TODO - remove isDup after metric duplicates removed
func ValidateTag(v *tl.DictionaryFieldStringBytes, metricBytes *tlstatshouse.MetricBytes, h *MappedMetricHeader, autoCreate *AutoCreate, isDup bool) (tagMeta *format.MetricMetaTag, tagIDKey int32, validEvent bool) {
	tagMeta, legacyName := h.MetricMeta.APICompatGetTagFromBytes(v.Key)
	validEvent = true
	if tagMeta == nil || tagMeta.Index >= format.MaxTags {
		tagMeta = nil // pretend we did not find that tag
		validKey, err := format.AppendValidStringValue(v.Key[:0], v.Key)
		if err != nil { // important case with garbage in tag name
			validEvent = false
			if isDup { // do not destroy Key, it will be needed to map original metric
				h.SetInvalidString(format.TagValueIDSrcIngestionStatusErrMapTagNameEncoding, 0, nil)
			} else {
				v.Key = format.AppendHexStringValue(v.Key[:0], v.Key)
				h.SetInvalidString(format.TagValueIDSrcIngestionStatusErrMapTagNameEncoding, 0, v.Key)
			}
			return
		}
		v.Key = validKey
		if _, ok := h.MetricMeta.GetTagDraft(v.Key); ok {
			h.FoundDraftTagName = v.Key
		} else {
			h.NotFoundTagName = v.Key
		}
		if autoCreate != nil && format.ValidTagName(mem.B(v.Key)) {
			_ = autoCreate.AutoCreateTag(metricBytes, v.Key, h.ReceiveTime)
		}
		// metric without meta gives validEvent=true, but tagMeta will be empty
		return
	}
	tagIDKey = tagMeta.Index + format.TagIDShift
	if legacyName {
		h.LegacyCanonicalTagKey = tagIDKey
	}

	validValue, err := format.AppendValidStringValue(v.Value[:0], v.Value)
	if err != nil {
		validEvent = false
		if isDup { // do not destroy Value, it will be needed to map original metric
			h.SetInvalidString(format.TagValueIDSrcIngestionStatusErrMapTagValueEncoding, tagIDKey, nil)
		} else {
			v.Value = format.AppendHexStringValue(v.Value[:0], v.Value)
			h.SetInvalidString(format.TagValueIDSrcIngestionStatusErrMapTagValueEncoding, tagIDKey, v.Value)
		}
		return
	}
	v.Value = validValue
	return
}
