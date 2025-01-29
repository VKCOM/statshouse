// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package aggregator

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/vkcom/statshouse/internal/data_model/gen2/tl"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/metajournal"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
)

type autoCreate struct {
	agg        *Aggregator
	client     *tlmetadata.Client
	storage    *metajournal.MetricsStorage
	mu         sync.Mutex
	co         *sync.Cond
	queue      []*rpc.HandlerContext // protected by "mu"
	args       map[*rpc.HandlerContext]tlstatshouse.AutoCreateBytes
	ctx        context.Context
	shutdownFn func()

	defaultNamespaceAllowed bool      // never changes
	knownTags               KnownTags // protected by "configMu"
	scrapeNamespaces        []int32   // protected by "configMu"
	configMu                sync.RWMutex

	running bool // guard against double "run"
}

type KnownTags map[int32][]SelectorTags // by namespace ID

type SelectorTags struct {
	Selector string     `json:"selector,omitempty"`
	Tags     []KnownTag `json:"tags,omitempty"`
}

type KnownTag struct {
	Name        string   `json:"name"`
	ID          string   `json:"id,omitempty"`
	Description string   `json:"description,omitempty"`
	RawKind     string   `json:"raw_kind,omitempty"`
	Whitelist   []string `json:"whitelist,omitempty"`
}

func newAutoCreate(a *Aggregator, client *tlmetadata.Client, defaultNamespaceAllowed bool) *autoCreate {
	ac := &autoCreate{
		agg:                     a,
		client:                  client,
		args:                    make(map[*rpc.HandlerContext]tlstatshouse.AutoCreateBytes),
		defaultNamespaceAllowed: defaultNamespaceAllowed,
	}
	ac.co = sync.NewCond(&ac.mu)
	ac.ctx, ac.shutdownFn = context.WithCancel(context.Background())
	return ac
}

func (ac *autoCreate) run(storage *metajournal.MetricsStorage) {
	if ac.running {
		return
	}
	ac.storage = storage
	go ac.goWork()
	ac.running = true
}

func (ac *autoCreate) applyConfig(configID int32, configS string) {
	switch configID {
	case format.KnownTagsConfigID:
		if v, err := ParseKnownTags([]byte(configS), ac.storage); err == nil {
			ac.configMu.Lock()
			ac.knownTags = v
			ac.configMu.Unlock()
		}
	case format.PrometheusConfigID:
		if s, err := DeserializeScrapeConfig([]byte(configS), ac.storage); err == nil {
			var scrapeNamespaces []int32
			if len(s) != 0 {
				scrapeNamespaces = make([]int32, 0, len(s))
				for i := range s {
					namespaceID := s[i].Options.NamespaceID
					switch namespaceID {
					case 0, format.BuiltinNamespaceIDDefault:
						// autocreate disabled for metrics without namespace
					default:
						scrapeNamespaces = append(scrapeNamespaces, namespaceID)
					}
				}
			}
			ac.configMu.Lock()
			ac.scrapeNamespaces = scrapeNamespaces
			ac.configMu.Unlock()
		}
	}
}

func (ac *autoCreate) Shutdown() {
	ac.shutdownFn()
	// broadcast under lock to ensure main loop either waiting on condVar
	// or haven't taken mutex yet and will check if "done" before waiting
	ac.mu.Lock()
	defer ac.mu.Unlock()
	ac.co.Broadcast()
}

func (ac *autoCreate) CancelHijack(hctx *rpc.HandlerContext) {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	delete(ac.args, hctx)
	// TODO - must remove from queue here, otherwise the same hctx will be reused and added to map, and we'll break rpc.Server internal invariants
}

func (ac *autoCreate) handleAutoCreate(_ context.Context, hctx *rpc.HandlerContext) error {
	var args tlstatshouse.AutoCreateBytes
	_, err := args.Read(hctx.Request)
	if err != nil {
		return fmt.Errorf("failed to deserialize statshouse.autoCreate request: %w", err)
	}
	ac.mu.Lock()
	defer ac.mu.Unlock()
	ac.queue = append(ac.queue, hctx)
	ac.args[hctx] = args
	ac.co.Signal()
	return hctx.HijackResponse(ac)
}

func (ac *autoCreate) goWork() {
	for {
		var ok bool
		hctx, args, ok := ac.getWork()
		if !ok { // done
			return
		}
		err := ac.createMetric(args)
		if ac.done() {
			return
		}
		ac.mu.Lock()
		if _, ok := ac.args[hctx]; ok {
			delete(ac.args, hctx)
			hctx.Response, _ = args.WriteResult(hctx.Response, tl.True{})
			hctx.SendHijackedResponse(err)
		}
		ac.mu.Unlock()
		if err != nil {
			// backoff for a second
			time.Sleep(1 * time.Second)
		}
	}
}

func (ac *autoCreate) getWork() (*rpc.HandlerContext, tlstatshouse.AutoCreateBytes, bool) {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	for {
		for len(ac.queue) == 0 {
			if ac.done() {
				return nil, tlstatshouse.AutoCreateBytes{}, false
			}
			ac.co.Wait()
		}
		hctx := ac.queue[0]
		ac.queue = ac.queue[1:] // TODO - reuse buffer
		if args, ok := ac.args[hctx]; ok {
			return hctx, args, true
		}
	}
}

func (ac *autoCreate) createMetric(args tlstatshouse.AutoCreateBytes) error {
	// get or build metric
	value := format.MetricMetaValue{}
	metricExists := false
	if v := ac.storage.GetMetaMetricByNameBytes(args.Metric); v != nil {
		// deep copy
		s, err := v.MarshalBinary()
		if err != nil {
			return fmt.Errorf("MarshalBinary failed: %w", err)
		}
		err = value.UnmarshalBinary(s)
		if err != nil {
			return fmt.Errorf("UnmarshalBinary failed: %w", err)
		}
		value.NamespaceID = v.NamespaceID
		value.GroupID = v.GroupID
		metricExists = true
	} else {
		validName, err := format.AppendValidStringValue(args.Metric[:0], args.Metric)
		if err != nil {
			return err // metric name is not valid
		}
		value = format.MetricMetaValue{
			Name:        string(validName),
			Description: string(args.Description),
			Tags:        make([]format.MetricMetaTag, format.MaxTags),
			Visible:     true,
			Kind:        string(args.Kind),
		}
		if i := strings.Index(value.Name, ":"); i != -1 {
			if namespace := ac.storage.GetNamespaceByName(value.Name[:i]); namespace != nil {
				value.NamespaceID = namespace.ID
			}
		}
		if 0 < args.Resolution && args.Resolution <= 60 {
			value.Resolution = int(args.Resolution)
		} else {
			value.Resolution = 1
		}
		err = value.RestoreCachedInfo()
		if err != nil {
			return fmt.Errorf("RestoreCachedInfo failed: %w", err)
		}
	}
	if !ac.namespaceAllowed(value.NamespaceID) {
		return nil // autocreate disabled for this namespace
	}
	// map tags
	var newTagDraftCount int
	for _, tagName := range args.Tags {
		if len(value.TagsDraft) >= format.MaxDraftTags {
			break
		}
		if tag := value.Name2TagBytes(tagName); tag != nil {
			continue // already mapped
		}
		if _, ok := value.GetTagDraft(tagName); ok {
			continue // already mapped
		}
		validName, err := format.AppendValidStringValue(tagName[:0], tagName)
		if err != nil {
			continue // tag name is not valid
		}
		t := format.MetricMetaTag{Name: string(validName)}
		if value.TagsDraft == nil {
			value.TagsDraft = map[string]format.MetricMetaTag{t.Name: t}
		} else {
			value.TagsDraft[t.Name] = t
		}
		newTagDraftCount++
	}
	var newTagCount int
	if len(value.TagsDraft) != 0 {
		newTagCount = ac.publishDraftTags(&value)
	}
	if metricExists && newTagDraftCount == 0 && newTagCount == 0 {
		return nil // nothing to do
	}
	// build edit request
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to serialize metric: %w", err)
	}
	edit := tlmetadata.EditEntitynew{
		Event: tlmetadata.Event{
			Id:        int64(value.MetricID),
			Name:      value.Name,
			EventType: format.MetricEvent,
			Version:   value.Version,
			Data:      string(data),
		},
	}
	var tags [16]int32
	if metricExists {
		tags[1] = 2 // edit
	} else {
		tags[1] = 1 // create
		edit.SetCreate(true)
	}
	// issue RPC call
	var ret tlmetadata.Event
	ctx, cancel := context.WithTimeout(ac.ctx, time.Minute)
	defer cancel()
	err = ac.client.EditEntitynew(ctx, edit, nil, &ret)
	if err != nil {
		tags[2] = 2 // failure
		ac.agg.sh2.AddCounter(ac.agg.aggKey(uint32(time.Now().Unix()), format.BuiltinMetricIDAutoCreateMetric, tags), 1, format.BuiltinMetricMetaAutoCreateMetric)
		return fmt.Errorf("failed to create or update metric: %w", err)
	}
	// succeeded, wait a bit until changes applied locally
	tags[2] = 1 // success
	ac.agg.sh2.AddCounter(ac.agg.aggKey(uint32(time.Now().Unix()), format.BuiltinMetricIDAutoCreateMetric, tags), 1, format.BuiltinMetricMetaAutoCreateMetric)
	ctx, cancel = context.WithTimeout(ac.ctx, 5*time.Second)
	defer cancel()
	_ = ac.storage.WaitVersion(ctx, ret.Version)
	return nil
}

func (ac *autoCreate) publishDraftTags(meta *format.MetricMetaValue) (n int) {
	ac.configMu.RLock()
	defer ac.configMu.RUnlock()
	if ac.knownTags != nil {
		n = ac.knownTags.PublishDraftTags(meta)
	}
	return n
}

func (ac *autoCreate) namespaceAllowed(namespaceID int32) (ok bool) {
	defaultNamespace := namespaceID == 0 || namespaceID == format.BuiltinNamespaceIDDefault
	if defaultNamespace {
		return ac.defaultNamespaceAllowed
	}
	ac.configMu.RLock()
	defer ac.configMu.RUnlock()
	if ac.knownTags != nil {
		_, ok = ac.knownTags[namespaceID]
	}
	return ok
}

func (ac *autoCreate) done() bool {
	select {
	case <-ac.ctx.Done():
		return true
	default:
		return false
	}
}

func (m KnownTags) PublishDraftTags(meta *format.MetricMetaValue) int {
	var n int
	if v, ok := m[meta.NamespaceID]; ok {
		for i := range v {
			if strings.HasPrefix(meta.Name, v[i].Selector) {
				n += publishDraftTags(meta, v[i].Tags)
			}
		}
	}
	return n
}

func publishDraftTags(meta *format.MetricMetaValue, knownTags []KnownTag) int {
	var n int
	for _, knownTag := range knownTags {
		if knownTag.Name == "" {
			continue
		}
		if draftTag, ok := meta.TagsDraft[knownTag.Name]; ok {
			if knownTag.ID == format.StringTopTagID {
				if meta.StringTopName == "" {
					meta.StringTopName = knownTag.Name
					if knownTag.Description != "" {
						meta.StringTopDescription = knownTag.Description
					}
					log.Printf("autocreate tag %s[_s] %s\n", meta.Name, knownTag.Name)
					delete(meta.TagsDraft, knownTag.Name)
					n++
				}
			} else {
				var x int
				if knownTag.ID != "" {
					x = format.TagIndex(knownTag.ID)
				} else {
					// search for an unnamed tag
					for x = 1; x < len(meta.Tags) && meta.Tags[x].Name != ""; x++ {
						// pass
					}
				}
				if x < 1 || format.NewMaxTags <= x || (x < len(meta.Tags) && meta.Tags[x].Name != "") {
					continue
				}
				draftTag.Name = knownTag.Name
				if knownTag.Description != "" {
					draftTag.Description = knownTag.Description
				}
				if knownTag.RawKind != "" {
					rawKind := knownTag.RawKind
					if rawKind == "int" {
						// The raw attribute is stored separately from the type string in metric meta,
						// empty type implies "int" which is not allowed
						rawKind = ""
					}
					if format.ValidRawKind(rawKind) {
						draftTag.Raw = true
						draftTag.RawKind = rawKind
					}
				}
				if len(meta.Tags) <= x {
					meta.Tags = append(make([]format.MetricMetaTag, 0, x+1), meta.Tags...)
					meta.Tags = meta.Tags[:x+1]
				}
				meta.Tags[x] = draftTag
				log.Printf("autocreate tag %s[%d] %s\n", meta.Name, x, draftTag.Name)
				delete(meta.TagsDraft, knownTag.Name)
				n++
			}
		}
	}
	return n
}

func ParseKnownTags(configS []byte, meta format.MetaStorageInterface) (KnownTags, error) {
	var s []SelectorTags
	if err := json.Unmarshal(configS, &s); err != nil {
		return nil, err
	}
	res := make(KnownTags)
	for i := 0; i < len(s); i++ {
		sel := s[i].Selector
		if n := strings.Index(sel, format.NamespaceSeparator); n != -1 {
			if v := meta.GetNamespaceByName(sel[:n]); v != nil {
				res[v.ID] = append(res[v.ID], s[i])
			}
		}
	}
	return res, nil
}
