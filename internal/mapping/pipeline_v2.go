// Copyright 2025 V Kontakte LLC
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

type mapPipelineV2 struct {
	mapCallback   data_model.MapCallbackFunc
	tagValueQueue *metricQueue
	tagValue      *pcache.Cache
	autoCreate    *data_model.AutoCreate
}

func newMapPipelineV2(mapCallback data_model.MapCallbackFunc, tagValue *pcache.Cache, ac *data_model.AutoCreate, maxMetrics int, maxMetricRequests int) *mapPipelineV2 {
	mp := &mapPipelineV2{
		mapCallback: mapCallback,
		tagValue:    tagValue,
		autoCreate:  ac,
	}
	mp.tagValueQueue = newMetricQueue("tag_value", mp.tagValueProgress, mp.tagValueIsDone, mp.tagValueFinish, maxMetrics, maxMetricRequests)
	return mp
}

func (mp *mapPipelineV2) stop() {
	mp.tagValueQueue.stop()
}

func (mp *mapPipelineV2) Map(args data_model.HandlerArgs, metricInfo *format.MetricMetaValue, h *data_model.MappedMetricHeader) (done bool) {
	done = mp.doMap(args, h)
	// We call MapEnvironment in all 3 cases
	// done and no errors - very fast NOP
	// done and errors - try to find environment in tags after error tag
	// not done - when making requests to map, we want to send our environment to server, so it can record it in builtin metric
	mp.MapEnvironment(args.MetricBytes, h)
	return done
}

func (mp *mapPipelineV2) doMap(args data_model.HandlerArgs, h *data_model.MappedMetricHeader) (done bool) {
	metric := args.MetricBytes
	if done = mp.mapTags(h, metric, true); done {
		return done
	}
	if done = mp.tagValueQueue.enqueue(metric, h, args.MapCallback); done {
		return done
	}
	return done
}

// transforms not yet mapped tags from metric into header
func (mp *mapPipelineV2) mapTags(h *data_model.MappedMetricHeader, metric *tlstatshouse.MetricBytes, cached bool) (done bool) {
	if h.IngestionStatus != 0 {
		return true // finished with error
	}

	// We do not validate metric name or tag keys, because they will be searched in finite maps
	for ; h.CheckedTagIndex < len(metric.Tags); h.CheckedTagIndex++ {
		v := &metric.Tags[h.CheckedTagIndex]
		tagMeta, tagIDKey, validEvent := data_model.ValidateTag(v, metric, h, mp.autoCreate, false)
		if !validEvent { // invalid tag key encoding, drop the whole event
			return true
		}
		if tagMeta == nil { // that tag is not in metric meta
			continue
		}
		switch {
		case tagMeta.Index == format.StringTopTagIndex:
			h.TopValue.S = v.Value
			if h.IsSKeySet {
				h.TagSetTwiceKey = tagIDKey
			}
			h.IsSKeySet = true
		case len(v.Value) == 0: // this case is also valid for raw values
			h.SetTag(tagMeta.Index, 0, tagIDKey) // we interpret "1" => "vasya", "1" => "petya" as second one overriding the first, but generating a warning
		case tagMeta.Raw64():
			lo, hi, ok := format.ContainsRawTagValue64(mem.B(v.Value)) // TODO - remove allocation in case of error
			if !ok {
				h.InvalidRawValue = v.Value
				h.InvalidRawTagKey = tagIDKey
				// We could arguably call h.SetKey, but there is very little difference in semantic to care
				continue
			}
			if tagMeta.Index+1 < format.MaxTags { // TODO - remove after NewMaxTags
				h.SetTag(tagMeta.Index+1, hi, tagIDKey+1) // last tag is never Raw64, checked by RestoreCachedInfo
			}
			h.SetTag(tagMeta.Index, lo, tagIDKey)
		case tagMeta.Raw:
			id, ok := format.ContainsRawTagValue(mem.B(v.Value)) // TODO - remove allocation in case of error
			if !ok {
				h.InvalidRawValue = v.Value
				h.InvalidRawTagKey = tagIDKey
				// We could arguably call h.SetKey, but there is very little difference in semantic to care
				continue
			}
			h.SetTag(tagMeta.Index, id, tagIDKey)
		default:
			if !cached { // We need to map single tag and exit. Slow path.
				extra := format.CreateMappingExtra{ // Host and AgentEnv are added by source when sending
					Metric:    string(metric.Name),
					TagIDKey:  tagIDKey,
					ClientEnv: h.Key.Tags[0], // mapEnvironment sets this, but only if already in cache, which is normally almost always.
				}
				id, err := mp.getTagValueID(h.ReceiveTime, v.Value, extra)
				if err != nil {
					h.SetInvalidString(format.TagValueIDSrcIngestionStatusErrMapTagValue, tagIDKey, v.Value)
					h.CheckedTagIndex++
					return true
				}
				h.SetTag(tagMeta.Index, id, tagIDKey)
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
			h.SetTag(tagMeta.Index, id, tagIDKey)
		}
	}
	// We validate values here, because we want errors to contain metric ID
	if h.ValuesChecked {
		return true
	}
	h.IngestionStatus = data_model.ValidateMetricData(metric)
	h.ValuesChecked = true
	return true
}

// If environment is not in cache, we will not detect it, but this should be relatively rare
// We might wish to load in background, but this must be fair with normal mapping queues, and
// we do not know metric here. So we decided to only load environments from cache.
// If called after mapTags consumed env, h.Tags[0] is already set by mapTags, otherwise will set h.Tags[0] here
func (mp *mapPipelineV2) MapEnvironment(metric *tlstatshouse.MetricBytes, h *data_model.MappedMetricHeader) {
	// fast NOP when all tags already mapped
	// must not change h.CheckedTagIndex or h.IsKeySet because mapTags will be called after this func by mapping queue in slow path
	for i := h.CheckedTagIndex; i < len(metric.Tags); i++ {
		v := &metric.Tags[i]
		if string(v.Key) != format.EnvTagID {
			continue
		}
		var err error
		v.Value, err = format.AppendValidStringValue(v.Value[:0], v.Value)
		if err != nil {
			return // do not bother if the first one set is crappy
		}
		if len(v.Value) == 0 {
			return // we are ok with the first one
		}
		id, err, found := mp.getTagValueIDCached(h.ReceiveTime, v.Value)
		if err != nil || !found {
			return // do not bother if the first one set is crappy
		}
		h.Key.Tags[0] = id
		return // we are ok with the first one
	}
}

func (mp *mapPipelineV2) doCallback(req *mapRequest) {
	if req.cb != nil {
		req.cb(req.metric, req.result)
	}
	mp.mapCallback(req.metric, req.result)
}

func (mp *mapPipelineV2) tagValueProgress(req *mapRequest) {
	_ = mp.mapTags(&req.result, &req.metric, false)
}

func (mp *mapPipelineV2) tagValueIsDone(req *mapRequest) bool {
	return mp.mapTags(&req.result, &req.metric, true)
}

func (mp *mapPipelineV2) tagValueFinish(req *mapRequest) {
	mp.doCallback(req)
}

func (mp *mapPipelineV2) getTagValueIDCached(now time.Time, tagValue []byte) (int32, error, bool) {
	r := mp.tagValue.GetCached(now, tagValue)
	return pcache.ValueToInt32(r.Value), r.Err, r.Found()
}

func (mp *mapPipelineV2) getTagValueID(now time.Time, tagValue []byte, extra format.CreateMappingExtra) (int32, error) {
	r := mp.tagValue.GetOrLoad(now, string(tagValue), extra)
	return pcache.ValueToInt32(r.Value), r.Err
}

func MapErrorFromHeader(m tlstatshouse.MetricBytes, h data_model.MappedMetricHeader) error {
	if h.IngestionStatus == 0 { // no errors
		return nil
	}
	ingestionTagName := format.TagIDTagToTagID(h.IngestionTagKey)
	envTag := h.Key.Tags[0] // TODO - we do not want to remember original string value somewhere yet (to print better errors here)
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
	case format.TagValueIDSrcIngestionStatusErrMetricDisabled:
		return fmt.Errorf("metric %q is disabled (envTag %d)", m.Name, envTag)
	case format.TagValueIDSrcIngestionStatusErrLegacyProtocol:
		return nil // not written
	case format.TagValueIDSrcIngestionStatusErrMetricNameEncoding:
		return fmt.Errorf("not utf-8 metric name %q (hex) (envTag %d)", h.InvalidString, envTag)
	case format.TagValueIDSrcIngestionStatusErrMapTagNameEncoding:
		return fmt.Errorf("not utf-8 name %q (hex) for key of metric %q (envTag %d)", h.InvalidString, m.Name, envTag)
	case format.TagValueIDSrcIngestionStatusErrValueUniqueBothSet:
		return fmt.Errorf("both value and unique fields set in metric event %q (envTag %d)", m.Name, envTag)
	case format.TagValueIDSrcIngestionStatusErrShardingFailed:
		return fmt.Errorf("metric %q shard is beyond configured shards (envTag %d)", m.Name, envTag)
	case format.TagValueIDSrcIngestionStatusErrMetricBuiltin:
		return fmt.Errorf("metric %q is builtin (envTag %d)", m.Name, envTag)
	default:
		return fmt.Errorf("unexpected error status %d with invalid string value %q for key %q of metric %q (envTag %d)", h.IngestionStatus, h.InvalidString, ingestionTagName, m.Name, envTag)
	}
}
