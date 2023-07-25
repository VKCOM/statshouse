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
	client     *tlmetadata.Client
	storage    *metajournal.MetricsStorage
	mu         sync.Mutex
	co         *sync.Cond
	queue      []*rpc.HandlerContext // protected by "mu"
	args       map[*rpc.HandlerContext]tlstatshouse.AutoCreate
	ctx        context.Context
	shutdownFn func()
}

func newAutoCreate(client *tlmetadata.Client, storage *metajournal.MetricsStorage) *autoCreate {
	ac := autoCreate{
		client:  client,
		storage: storage,
		args:    map[*rpc.HandlerContext]tlstatshouse.AutoCreate{},
	}
	ac.co = sync.NewCond(&ac.mu)
	ac.ctx, ac.shutdownFn = context.WithCancel(context.Background())
	go ac.goWork()
	return &ac
}

func (ac *autoCreate) shutdown() {
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

func (ac *autoCreate) handleAutoCreate(_ context.Context, hctx *rpc.HandlerContext, args tlstatshouse.AutoCreate) error {
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

func (ac *autoCreate) getWork() (*rpc.HandlerContext, tlstatshouse.AutoCreate, bool) {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	for {
		for len(ac.queue) == 0 {
			if ac.done() {
				return nil, tlstatshouse.AutoCreate{}, false
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

func (ac *autoCreate) createMetric(args tlstatshouse.AutoCreate) error {
	// get or build metric
	value := format.MetricMetaValue{}
	metricExists := false
	if v := ac.storage.GetMetaMetricByName(args.Metric); v != nil {
		// deep copy
		s, err := v.MarshalBinary()
		if err != nil {
			return fmt.Errorf("MarshalBinary failed: %w", err)
		}
		err = value.UnmarshalBinary(s)
		if err != nil {
			return fmt.Errorf("UnmarshalBinary failed: %w", err)
		}
		metricExists = true
	} else {
		value = format.MetricMetaValue{
			Name:       args.Metric,
			Tags:       []format.MetricMetaTag{{}},
			Visible:    true,
			Kind:       args.Kind,
			Resolution: 1,
		}
		err := value.RestoreCachedInfo()
		if err != nil {
			return fmt.Errorf("RestoreCachedInfo failed: %w", err)
		}
	}
	// map tags
	newTagCount := 0
tagMappingLoop:
	for _, tagName := range args.Tags {
		if _, ok := value.Name2Tag[tagName]; ok {
			continue // already mapped
		}
		if tagName == format.LETagName {
			i := format.LETagIndex
			if i >= len(value.Tags) || len(value.Tags[i].Name) == 0 {
				for j := len(value.Tags); j < i; j++ {
					value.Tags = append(value.Tags, format.MetricMetaTag{Description: "-"})
				}
				meta := format.MetricMetaTag{
					Name:        format.LETagName,
					Description: "histogram bucket label",
					Index:       i,
					Raw:         true,
				}
				if i < len(value.Tags) {
					value.Tags[i] = meta
				} else {
					value.Tags = append(value.Tags, meta)
				}
				newTagCount++
			}
		} else {
			i := 1 // skip "env" tag
			for ; i < len(value.Tags); i++ {
				if len(value.Tags[i].Name) == 0 {
					break
				}
			}
			switch {
			case i < len(value.Tags):
				value.Tags[i].Name = tagName
				value.Tags[i].Description = ""
			case i >= format.MaxTags:
				break tagMappingLoop // all tags mapped
			default:
				value.Tags = append(value.Tags, format.MetricMetaTag{
					Name:  tagName,
					Index: len(value.Tags),
				})
			}
			newTagCount++
		}
	}
	if metricExists && newTagCount == 0 {
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
	edit.SetCreate(!metricExists)
	// issue RPC call
	var ret tlmetadata.Event
	ctx, cancel := context.WithTimeout(ac.ctx, time.Minute)
	defer cancel()
	err = ac.client.EditEntitynew(ctx, edit, nil, &ret)
	if err != nil {
		return fmt.Errorf("failed to create or update metric: %w", err)
	}
	// succeeded, wait a bit until changes applied locally
	ctx, cancel = context.WithTimeout(ac.ctx, 5*time.Second)
	defer cancel()
	_ = ac.storage.Journal().WaitVersion(ctx, ret.Version)
	return nil
}

func (ac *autoCreate) done() bool {
	select {
	case <-ac.ctx.Done():
		return true
	default:
		return false
	}
}
