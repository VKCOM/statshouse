// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package mapping

import (
	"fmt"
	"time"

	"go4.org/mem"

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/pcache"
)

type mapPipeline struct {
	mapCallback   data_model.MapCallbackFunc
	tagValueQueue *metricQueue
	tagValue      *pcache.Cache
	autoCreate    *AutoCreate
}

func newMapPipeline(mapCallback data_model.MapCallbackFunc, tagValue *pcache.Cache, ac *AutoCreate, maxMetrics int, maxMetricRequests int) *mapPipeline {
	mp := &mapPipeline{
		mapCallback: mapCallback,
		tagValue:    tagValue,
		autoCreate:  ac,
	}
	mp.tagValueQueue = newMetricQueue("tag_value", mp.tagValueProgress, mp.tagValueIsDone, mp.tagValueFinish, maxMetrics, maxMetricRequests)
	return mp
}

func (mp *mapPipeline) stop() {
	mp.tagValueQueue.stop()
}

func (mp *mapPipeline) Map(args data_model.HandlerArgs, metricInfo *format.MetricMetaValue) (h data_model.MappedMetricHeader, done bool) {
	h.ReceiveTime = time.Now() // mapping time is set once for all functions
	h.MetricInfo = metricInfo
	if done = mp.fillMetricInfo(&h, args); done {
		return h, done
	}
	h.RawKey.Metric = h.MetricInfo.MetricID
	h.Key.Metric = h.MetricInfo.MetricID
	if !h.MetricInfo.Visible {
		h.IngestionStatus = format.TagValueIDSrcIngestionStatusErrMetricInvisible
		return h, true
	}
	if done = mp.fillRawKeys(&h, args.MetricBytes); done {
		return h, done
	}
	if done = mp.fillCachedTags(&h, args.MetricBytes); done {
		if h.IngestionStatus == 0 {
			mp.validateValues(&h, args.MetricBytes)
		}
		return h, done
	}
	if done = mp.validateValues(&h, args.MetricBytes); done {
		return h, done
	}
	if done = mp.tagValueQueue.enqueue(args.MetricBytes, &h, args.MapCallback); done {
		return h, done
	}
	return h, done
}

func (mp *mapPipeline) fillMetricInfo(h *data_model.MappedMetricHeader, args data_model.HandlerArgs) bool {
	metric := args.MetricBytes
	if h.MetricInfo != nil {
		return false
	}
	h.MetricInfo = format.BuiltinMetricAllowedToReceive[string(metric.Name)]
	if h.MetricInfo != nil {
		return false
	}
	if mp.autoCreate != nil && format.ValidMetricName(mem.B(metric.Name)) {
		// before normalizing metric.Name so we do not fill auto create data structures with invalid metric names
		_ = mp.autoCreate.autoCreateMetric(metric, args.Description, args.ScrapeInterval, h.ReceiveTime)
	}
	validName, err := format.AppendValidStringValue(metric.Name[:0], metric.Name)
	if err != nil {
		metric.Name = format.AppendHexStringValue(metric.Name[:0], metric.Name)
		h.InvalidString = metric.Name
		h.IngestionStatus = format.TagValueIDSrcIngestionStatusErrMetricNameEncoding
		return true
	}
	metric.Name = validName
	h.InvalidString = metric.Name
	h.IngestionStatus = format.TagValueIDSrcIngestionStatusErrMetricNotFound
	return true
}

func (mp *mapPipeline) fillRawKeys(h *data_model.MappedMetricHeader, metric *tlstatshouse.MetricBytes) bool {
	// We do not validate metric name or tag keys, because they will be searched in finite maps
	for i := 0; i < len(metric.Tags); i++ {
		entry := &metric.Tags[i]
		tagInfo, ok, legacyName := h.MetricInfo.APICompatGetTagFromBytes(entry.Key)
		if !ok {
			validKey, err := format.AppendValidStringValue(entry.Key[:0], entry.Key)
			if err != nil {
				entry.Key = format.AppendHexStringValue(entry.Key[:0], entry.Key)
				h.SetInvalidString(format.TagValueIDSrcIngestionStatusErrMapTagNameEncoding, 0, entry.Key)
				return true
			}
			entry.Key = validKey
			if _, ok := h.MetricInfo.GetTagDraft(entry.Key); ok {
				h.FoundDraftTagName = entry.Key
			} else {
				h.NotFoundTagName = entry.Key
			}
			if mp.autoCreate != nil && format.ValidMetricName(mem.B(entry.Key)) {
				// before normalizing v.Key, so we do not fill auto create data structures with invalid key names
				_ = mp.autoCreate.autoCreateTag(metric, entry.Key, h.ReceiveTime)
			}
			continue
		}
		tagIDKey := int32(tagInfo.Index + format.TagIDShift)
		if legacyName {
			h.LegacyCanonicalTagKey = tagIDKey
		}
		validValue, err := format.AppendValidStringValue(entry.Value[:0], entry.Value)
		if err != nil {
			entry.Value = format.AppendHexStringValue(entry.Value[:0], entry.Value)
			h.SetInvalidString(format.TagValueIDSrcIngestionStatusErrMapTagValueEncoding, tagIDKey, entry.Value)
			return true
		}
		entry.Value = validValue

		switch {
		case tagInfo.Index == format.StringTopTagIndex:
			h.SValue = entry.Value
			if h.IsSKeySet {
				h.TagSetTwiceKey = tagIDKey
			}
			h.IsSKeySet = true
		case tagInfo.Raw:
			_, ok := format.ContainsRawTagValue(mem.B(entry.Value)) // TODO - remove allocation in case of error
			if !ok {
				h.InvalidRawValue = entry.Value
				h.InvalidRawTagKey = tagIDKey
				// We could arguably call h.SetKey, but there is very little difference in semantic to care
				continue
			}
			h.RawKey.Keys[tagInfo.Index] = entry.Value
		default:
			h.RawKey.Keys[tagInfo.Index] = entry.Value
		}
	}
	return false
}

func (mp *mapPipeline) fillCachedTags(h *data_model.MappedMetricHeader, metric *tlstatshouse.MetricBytes) bool {
	allMapped := true
	for i, key := range h.RawKey.Keys {
		if len(key) == 0 {
			continue
		}
		mappedId, err, found := mp.getTagValueIDCached(h.ReceiveTime, key) // returns err from cache, so no allocations
		if err != nil {
			h.SetInvalidString(format.TagValueIDSrcIngestionStatusErrMapTagValueCached, int32(i), key)
			h.CheckedTagIndex++
			return true
		}
		if !found {
			allMapped = false
			continue
		}
		h.Key.Keys[i] = mappedId
	}
	return allMapped
}

// transforms not yet mapped tags from metric into header
func (mp *mapPipeline) mapTags(h *data_model.MappedMetricHeader, metric *tlstatshouse.MetricBytes, cached bool) (done bool) {
	if h.IngestionStatus != 0 {
		return true // finished with error
	}

	// We do not validate metric name or tag keys, because they will be searched in finite maps
	for ; h.CheckedTagIndex < len(metric.Tags); h.CheckedTagIndex++ {
		v := &metric.Tags[h.CheckedTagIndex]
		tagInfo, ok, legacyName := h.MetricInfo.APICompatGetTagFromBytes(v.Key)
		if !ok {
			validKey, err := format.AppendValidStringValue(v.Key[:0], v.Key)
			if err != nil {
				v.Key = format.AppendHexStringValue(v.Key[:0], v.Key)
				h.SetInvalidString(format.TagValueIDSrcIngestionStatusErrMapTagNameEncoding, 0, v.Key)
				h.CheckedTagIndex++
				return true
			}
			v.Key = validKey
			if _, ok := h.MetricInfo.GetTagDraft(v.Key); ok {
				h.FoundDraftTagName = v.Key
			} else {
				h.NotFoundTagName = v.Key
			}
			if mp.autoCreate != nil && format.ValidMetricName(mem.B(v.Key)) {
				// before normalizing v.Key, so we do not fill auto create data structures with invalid key names
				_ = mp.autoCreate.autoCreateTag(metric, v.Key, h.ReceiveTime)
			}
			continue
		}
		tagIDKey := int32(tagInfo.Index + format.TagIDShift)
		if legacyName {
			h.LegacyCanonicalTagKey = tagIDKey
		}
		validValue, err := format.AppendValidStringValue(v.Value[:0], v.Value)
		if err != nil {
			v.Value = format.AppendHexStringValue(v.Value[:0], v.Value)
			h.SetInvalidString(format.TagValueIDSrcIngestionStatusErrMapTagValueEncoding, tagIDKey, v.Value)
			h.CheckedTagIndex++
			return true
		}
		v.Value = validValue
		switch {
		case tagInfo.Index == format.StringTopTagIndex:
			h.SValue = v.Value
			if h.IsSKeySet {
				h.TagSetTwiceKey = tagIDKey
			}
			h.IsSKeySet = true
		case tagInfo.Raw:
			id, ok := format.ContainsRawTagValue(mem.B(v.Value)) // TODO - remove allocation in case of error
			if !ok {
				h.InvalidRawValue = v.Value
				h.InvalidRawTagKey = tagIDKey
				// We could arguably call h.SetKey, but there is very little difference in semantic to care
				continue
			}
			h.SetKey(tagInfo.Index, id, tagIDKey)
		case len(v.Value) == 0: // TODO - move knowledge about "" <-> 0 mapping to more general place
			h.SetKey(tagInfo.Index, 0, tagIDKey) // we interpret "0" => "vasya", "0" => "" as second one overriding the first, generating a warning
		default:
			if !cached { // We need to map single tag and exit. Slow path.
				extra := format.CreateMappingExtra{ // Host and AgentEnv are added by source when sending
					Metric:    string(metric.Name),
					TagIDKey:  tagIDKey,
					ClientEnv: h.Key.Keys[0], // mapEnvironment sets this, but only if already in cache, which is normally almost always.
				}
				id, err := mp.getTagValueID(h.ReceiveTime, v.Value, extra)
				if err != nil {
					h.SetInvalidString(format.TagValueIDSrcIngestionStatusErrMapTagValue, tagIDKey, v.Value)
					h.CheckedTagIndex++
					return true
				}
				h.SetKey(tagInfo.Index, id, tagIDKey)
				h.CheckedTagIndex++          // CheckedTagIndex is advanced each time we return, so early or later mapStatusDone is returned
				h.IngestionTagKey = tagIDKey // so we know which tag causes "uncached" status
				return false
			}
			id, err, found := mp.getTagValueIDCached(h.ReceiveTime, v.Value) // returns err from cache, so no allocations
			if err != nil {
				h.SetInvalidString(format.TagValueIDSrcIngestionStatusErrMapTagValueCached, tagIDKey, v.Value)
				h.CheckedTagIndex++
				return true
			}
			if !found {
				return false
			}
			h.SetKey(tagInfo.Index, id, tagIDKey)
		}
	}
	return true
}

func (mp *mapPipeline) validateValues(h *data_model.MappedMetricHeader, metric *tlstatshouse.MetricBytes) bool {
	// We validate values here, because we want errors to contain metric ID
	if h.ValuesChecked {
		return true
	}
	h.ValuesChecked = true
	if len(metric.Value) != 0 && len(metric.Unique) != 0 {
		h.IngestionStatus = format.TagValueIDSrcIngestionStatusErrValueUniqueBothSet
		return true
	}
	if metric.Counter < 0 {
		h.IngestionStatus = format.TagValueIDSrcIngestionStatusErrNegativeCounter
		return true
	}
	if !format.ValidFloatValue(metric.Counter) {
		h.IngestionStatus = format.TagValueIDSrcIngestionStatusErrNanInfCounter
		return true
	}
	metric.Counter = format.ClampFloatValue(metric.Counter)
	for i, v := range metric.Value {
		if !format.ValidFloatValue(v) {
			h.IngestionStatus = format.TagValueIDSrcIngestionStatusErrNanInfValue
			return true
		}
		metric.Value[i] = format.ClampFloatValue(v)
	}
	return false
}

func (mp *mapPipeline) doCallback(req *mapRequest) {
	if req.cb != nil {
		req.cb(req.metric, req.result)
	}
	mp.mapCallback(req.metric, req.result)
}

func (mp *mapPipeline) tagValueProgress(req *mapRequest) {
	_ = mp.mapTags(&req.result, &req.metric, false)
}

func (mp *mapPipeline) tagValueIsDone(req *mapRequest) bool {
	return mp.mapTags(&req.result, &req.metric, true)
}

func (mp *mapPipeline) tagValueFinish(req *mapRequest) {
	mp.doCallback(req)
}

func (mp *mapPipeline) getTagValueIDCached(now time.Time, tagValue []byte) (int32, error, bool) {
	r := mp.tagValue.GetCached(now, tagValue)
	return pcache.ValueToInt32(r.Value), r.Err, r.Found()
}

func (mp *mapPipeline) getTagValueID(now time.Time, tagValue []byte, extra format.CreateMappingExtra) (int32, error) {
	r := mp.tagValue.GetOrLoad(now, string(tagValue), extra)
	return pcache.ValueToInt32(r.Value), r.Err
}

func MapErrorFromHeader(m tlstatshouse.MetricBytes, h data_model.MappedMetricHeader) error {
	if h.IngestionStatus == 0 { // no errors
		return nil
	}
	ingestionTagName := format.TagIDTagToTagID(h.IngestionTagKey)
	envTag := h.Key.Keys[0] // TODO - we do not want to remember original string value somewhere yet (to print better errors here)
	switch h.IngestionStatus {
	case format.TagValueIDSrcIngestionStatusErrMetricNotFound:
		return fmt.Errorf("metric %q not found (envTag %d)", m.Name, envTag)
	case format.TagValueIDSrcIngestionStatusErrNanInfValue:
		return fmt.Errorf("NaN/Inf value for metric %q (envTag %d)", m.Name, envTag)
	case format.TagValueIDSrcIngestionStatusErrNanInfCounter:
		return fmt.Errorf("NaN/Inf counter for metric %q (envTag %d)", m.Name, envTag)
	case format.TagValueIDSrcIngestionStatusErrNegativeCounter:
		return fmt.Errorf("negative counter for metric %q (envTag %d)", m.Name, envTag)
	case format.TagValueIDSrcIngestionStatusErrMapOther:
		return nil // not written
	case format.TagValueIDSrcIngestionStatusErrMapInvalidRawTagValue:
		return fmt.Errorf("invalid raw tag value %q for key %q of metric %q (envTag %d)", h.InvalidString, ingestionTagName, m.Name, envTag)
	case format.TagValueIDSrcIngestionStatusErrMapTagValueCached:
		return fmt.Errorf("error mapping tag value (cached) %q for key %q of metric %q (envTag %d)", h.InvalidString, ingestionTagName, m.Name, envTag)
	case format.TagValueIDSrcIngestionStatusErrMapTagValue:
		return fmt.Errorf("failed to map tag value %q for key %q of metric %q (envTag %d)", h.InvalidString, ingestionTagName, m.Name, envTag)
	case format.TagValueIDSrcIngestionStatusErrMapGlobalQueueOverload:
		return fmt.Errorf("failed to map metric %q: too many metrics in queue (envTag %d)", m.Name, envTag)
	case format.TagValueIDSrcIngestionStatusErrMapPerMetricQueueOverload:
		return fmt.Errorf("failed to map metric %q: per-metric mapping queue overloaded (envTag %d)", m.Name, envTag)
	case format.TagValueIDSrcIngestionStatusErrMapTagValueEncoding:
		return fmt.Errorf("not utf-8 value %q (hex) for for key %q of metric %q (envTag %d)", h.InvalidString, ingestionTagName, m.Name, envTag)
	case format.TagValueIDSrcIngestionStatusOKLegacy:
		return nil // not written
	case format.TagValueIDSrcIngestionStatusErrMetricNonCanonical:
		return nil // not written
	case format.TagValueIDSrcIngestionStatusErrMetricInvisible:
		return fmt.Errorf("metric %q is disabled (envTag %d)", m.Name, envTag)
	case format.TagValueIDSrcIngestionStatusErrLegacyProtocol:
		return nil // not written
	case format.TagValueIDSrcIngestionStatusErrMetricNameEncoding:
		return fmt.Errorf("not utf-8 metric name %q (hex) (envTag %d)", h.InvalidString, envTag)
	case format.TagValueIDSrcIngestionStatusErrMapTagNameEncoding:
		return fmt.Errorf("not utf-8 name %q (hex) for key of metric %q (envTag %d)", h.InvalidString, m.Name, envTag)
	default:
		return fmt.Errorf("unexpected error status %d with invalid string value %q for key %q of metric %q (envTag %d)", h.IngestionStatus, h.InvalidString, ingestionTagName, m.Name, envTag)
	}
}
