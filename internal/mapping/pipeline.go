// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package mapping

import (
	"fmt"
	"sync"
	"time"

	"go4.org/mem"

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/pcache"
)

type mapRequest struct {
	metric tlstatshouse.MetricBytes      // in
	result data_model.MappedMetricHeader // out, contains probable error in IngestionStatus
	cb     MapCallbackFunc               // will be called only when processing required enqueue
}

type metricQueue struct {
	name     string
	progress func(*mapRequest)      // called with mutex unlocked
	isDone   func(*mapRequest) bool // called with mutex locked, should be wait-free
	finish   func(*mapRequest)      // called with mutex unlocked

	// separate progress and done are because progress in one mapping request can make others done

	cond              sync.Cond
	mu                sync.Mutex
	queue             map[int32][]*mapRequest // map iteration is the source of quasi-fairness. length of slices is never 0 here
	size              int
	maxMetrics        int
	maxMetricRequests int
	shouldStop        bool
	stopped           chan struct{}

	pool []tlstatshouse.MetricBytes // for reuse
}

func newMetricQueue(
	name string,
	progress func(*mapRequest),
	isDone func(*mapRequest) bool,
	finish func(*mapRequest),
	maxMetrics int,
	maxMetricRequests int,
) *metricQueue {
	q := &metricQueue{
		name:              name,
		progress:          progress,
		isDone:            isDone,
		finish:            finish,
		queue:             map[int32][]*mapRequest{},
		maxMetrics:        maxMetrics,
		maxMetricRequests: maxMetricRequests,
		stopped:           make(chan struct{}),
	}
	q.cond.L = &q.mu
	go q.run()
	return q
}

func (mq *metricQueue) stop() {
	mq.mu.Lock()
	mq.shouldStop = true
	mq.mu.Unlock()
	mq.cond.Signal()
	<-mq.stopped
}

// to avoid allocations in fastpath, returns ingestion statuses only. They are converted to errors before printing, if needed
// we do not want allocation if queues overloaded
func (mq *metricQueue) enqueue(metric *tlstatshouse.MetricBytes, result *data_model.MappedMetricHeader, cb MapCallbackFunc) (done bool) {
	mq.mu.Lock()
	defer mq.mu.Unlock()

	if mq.shouldStop {
		return false
	}

	q, ok := mq.queue[result.Key.Metric]
	if !ok {
		if len(mq.queue) >= mq.maxMetrics {
			result.IngestionStatus = format.TagValueIDSrcIngestionStatusErrMapGlobalQueueOverload
			return true
		}
	}
	if len(q) >= mq.maxMetricRequests {
		result.IngestionStatus = format.TagValueIDSrcIngestionStatusErrMapPerMetricQueueOverload
		return true
	}

	mq.queue[result.Key.Metric] = append(q, &mapRequest{metric: *metric, result: *result, cb: cb})
	// We now own all slices inside metric. We replace them from our pool, or set to nil
	if len(mq.pool) != 0 {
		*metric = mq.pool[len(mq.pool)-1]
		mq.pool = mq.pool[:len(mq.pool)-1]
	} else {
		*metric = tlstatshouse.MetricBytes{}
	}
	mq.size++
	mq.cond.Signal()

	return false
}

func (mq *metricQueue) run() {
	var localPool []tlstatshouse.MetricBytes
	var newQ []*mapRequest // for reuse
	var done []*mapRequest // for reuse
	for {
		mq.mu.Lock()
		for !(mq.shouldStop || mq.size > 0) {
			mq.cond.Wait()
		}
		if mq.shouldStop {
			mq.mu.Unlock()
			close(mq.stopped)
			return
		}
		mq.pool = append(mq.pool, localPool...) // move from local pool under lock
		localPool = localPool[:0]
		var (
			chosenM int32
			chosenR *mapRequest
		)
		for m, q := range mq.queue {
			chosenM = m
			chosenR = q[0]
			break
		}
		mq.mu.Unlock()

		mq.progress(chosenR)

		mq.mu.Lock()
		curQ := mq.queue[chosenM]
		// mapping of single key can release many items from the queue
		for _, r := range curQ {
			if mq.isDone(r) {
				done = append(done, r)
			} else {
				newQ = append(newQ, r)
			}
		}
		if len(newQ) > 0 {
			mq.queue[chosenM] = newQ
			newQ = curQ[:0]
		} else {
			delete(mq.queue, chosenM)
		}
		mq.size -= len(done)
		mq.mu.Unlock()

		for _, r := range done {
			mq.finish(r)
			r.metric.Reset()                        // strictly not required
			localPool = append(localPool, r.metric) // We pool fields, not metric itself
		}
		done = done[:0]
	}
}

type mapPipeline struct {
	mapCallback   MapCallbackFunc
	tagValueQueue *metricQueue
	tagValue      *pcache.Cache
	autoCreate    *AutoCreate
}

func newMapPipeline(mapCallback MapCallbackFunc, tagValue *pcache.Cache, ac *AutoCreate, maxMetrics int, maxMetricRequests int) *mapPipeline {
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

func (mp *mapPipeline) Map(metric *tlstatshouse.MetricBytes, metricInfo *format.MetricMetaValue, cb MapCallbackFunc) (h data_model.MappedMetricHeader, done bool) {
	h.ReceiveTime = time.Now() // mapping time is set once for all functions
	h.MetricInfo = metricInfo
	done = mp.doMap(metric, &h, cb)
	// We map environment in all 3 cases
	// done and no errors - very fast NOP
	// done and errors - try to find environment in tags after error tag
	// not done - when making requests to map, we want to send our environment to server, so it can record it in builtin metric
	mp.mapEnvironment(&h, metric)
	if done {
		return h, done
	}
	return h, done
}

func (mp *mapPipeline) doMap(metric *tlstatshouse.MetricBytes, h *data_model.MappedMetricHeader, cb MapCallbackFunc) (done bool) {
	if h.MetricInfo == nil {
		h.MetricInfo = format.BuiltinMetricAllowedToReceive[string(metric.Name)]
		if h.MetricInfo == nil {
			if mp.autoCreate != nil && format.ValidMetricName(mem.B(metric.Name)) {
				// before normalizing metric.Name so we do not fill auto create data structures with invalid metric names
				_ = mp.autoCreate.autoCreateMetric(metric, h.ReceiveTime)
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
	}
	h.Key.Metric = h.MetricInfo.MetricID
	if !h.MetricInfo.Visible {
		h.IngestionStatus = format.TagValueIDSrcIngestionStatusErrMetricInvisible
		return true
	}
	if done = mp.mapTags(h, metric, true); done {
		return done
	}
	if done = mp.tagValueQueue.enqueue(metric, h, cb); done {
		return done
	}
	return done
}

// transforms not yet mapped tags from metric into header
func (mp *mapPipeline) mapTags(h *data_model.MappedMetricHeader, metric *tlstatshouse.MetricBytes, cached bool) (done bool) {
	if h.IngestionStatus != 0 {
		return true // finished with error
	}

	// We do not validate metric name or tag keys, because they will be searched in finite maps
	for ; h.CheckedTagIndex < len(metric.Tags); h.CheckedTagIndex++ {
		v := &metric.Tags[h.CheckedTagIndex]
		tagInfo, ok := h.MetricInfo.Name2Tag[string(v.Key)]
		if !ok {
			if mp.autoCreate != nil && format.ValidMetricName(mem.B(v.Key)) {
				// before normalizing v.Key, so we do not fill auto create data structures with invalid key names
				_ = mp.autoCreate.autoCreateTag(metric, v.Key, h.ReceiveTime)
			}
			validKey, err := format.AppendValidStringValue(v.Key[:0], v.Key)
			if err != nil {
				v.Key = format.AppendHexStringValue(v.Key[:0], v.Key)
				h.SetInvalidString(format.TagValueIDSrcIngestionStatusErrMapTagNameEncoding, 0, v.Key)
				h.CheckedTagIndex++
				return true
			}
			v.Key = validKey
			h.NotFoundTagName = v.Key
			continue
		}
		tagIDKey := int32(tagInfo.Index + format.TagIDShift)
		if tagInfo.LegacyName {
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
				h.SetInvalidString(format.TagValueIDSrcIngestionStatusErrMapInvalidRawTagValue, tagIDKey, v.Value)
				h.CheckedTagIndex++
				return true
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
	for _, v := range metric.Value {
		if !format.ValidFloatValue(v) {
			h.IngestionStatus = format.TagValueIDSrcIngestionStatusErrNanInfValue
			return true
		}
	}
	return true
}

// We wish to know which environment generates 'metric not found' events and other errors
// Also we need to know which environment generates mapping create events. So we do it before adding to mapping queue
// If environment is not in cache, we will not detect it, but this should be relatively rare
// We might wish to load in background, but this must be fair with normal mapping queues, and
// we do not know metric here. So we decided to only load environments from cache.
// If called after mapTags consumed env, h.Tags[0] is already set by mapTags, otherwise will set h.Tags[0] here
func (mp *mapPipeline) mapEnvironment(h *data_model.MappedMetricHeader, metric *tlstatshouse.MetricBytes) {
	// fast NOP when all tags already mapped
	// must not change h.CheckedTagIndex or h.IsKeySet because mapTags will be called after this func by mapping queue in slow path
	for i := h.CheckedTagIndex; i < len(metric.Tags); i++ {
		v := &metric.Tags[i]
		if string(v.Key) != format.EnvTagID && string(v.Key) != format.EnvTagName && string(v.Key) != "0" {
			// TODO - remove extra checks after all libraries use new canonical name
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
		h.Key.Keys[0] = id
		return // we are ok with the first one
	}
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
