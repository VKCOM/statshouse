// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metadata

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/VKCOM/statshouse-go"

	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/VKCOM/statshouse/internal/format"
	"github.com/VKCOM/statshouse/internal/vkgo/rpc"
)

const MaxBoostrapResponseSize = 1024 * 1024 // TODO move somewhere
const longPollTimeout = time.Hour
const mappingCacheSize = 500

type Handler struct {
	db *DBV2

	getJournalMx      sync.Mutex
	getJournalClients map[*rpc.HandlerContext]tlmetadata.GetJournalnew // by getJournalMx

	getMappingMx      sync.Mutex
	getMappingClients map[*rpc.HandlerContext]tlmetadata.GetNewMappings // by getMappingMx

	mappingCacheMx sync.RWMutex
	mappingCache   [mappingCacheSize]tlstatshouse.Mapping // by mappingCacheMx, ASC
	mappingHead    int                                    // by mappingCacheMx
	mappingTail    int                                    // by mappingCacheMx

	host string
	log  func(s string, args ...interface{})
}

func NewHandler(db *DBV2, host string, log func(s string, args ...interface{})) *Handler {
	h := &Handler{
		db:                db,
		getJournalClients: map[*rpc.HandlerContext]tlmetadata.GetJournalnew{},
		getMappingClients: map[*rpc.HandlerContext]tlmetadata.GetNewMappings{},
		mappingCache:      [mappingCacheSize]tlstatshouse.Mapping{},
		log:               log,
		host:              host,
	}
	go func() {
		t := time.NewTimer(longPollTimeout)
		for {
			<-t.C
			h.broadcastCancel()
			t = time.NewTimer(longPollTimeout)
		}
	}()
	h.mappingCacheMx.Lock()
	defer h.mappingCacheMx.Unlock()
	h.bootStrapMappingCacheUnlocked(context.Background())

	statshouse.StartRegularMeasurement(func(client *statshouse.Client) {
		h.getJournalMx.Lock()
		qLength := len(h.getJournalClients)
		h.getJournalMx.Unlock()
		client.Value(format.BuiltinMetricMetaMetaClientWaits.Name, statshouse.Tags{1: h.host}, float64(qLength))

		h.getMappingMx.Lock()
		mLength := len(h.getMappingClients)
		h.getMappingMx.Unlock()
		client.Value(format.BuiltinMetricMetaMappingClientWaits.Name, statshouse.Tags{1: h.host}, float64(mLength))

		h.mappingCacheMx.RLock()
		lastV := h.mappingCache[h.mappingTail].Value
		h.mappingCacheMx.RUnlock()
		client.Value(format.BuiltinMetricMetaMappingCacheVersion.Name, statshouse.Tags{1: h.host}, float64(lastV))
	})
	return h
}

func (h *Handler) CancelHijack(hctx *rpc.HandlerContext) {
	statshouse.Count("meta_cancel_hijack", statshouse.Tags{1: h.host}, 1)
	h.getJournalMx.Lock()
	delete(h.getJournalClients, hctx)
	h.getJournalMx.Unlock()

	h.getMappingMx.Lock()
	delete(h.getMappingClients, hctx)
	h.getMappingMx.Unlock()
}

func (h *Handler) broadcastCancel() {
	func() {
		h.getJournalMx.Lock()
		defer h.getJournalMx.Unlock()
		if len(h.getJournalClients) == 0 {
			return
		}
		for hctx, args := range h.getJournalClients {
			resp := tlmetadata.GetJournalResponsenew{CurrentVersion: args.From}
			var err error
			hctx.Response, err = args.WriteResult(hctx.Response, resp)
			hctx.SendHijackedResponse(err)
		}
		h.log("[info] broadcast empty journal response to %d long poll clients", len(h.getJournalClients))
		clear(h.getJournalClients)
	}()
	func() {
		h.getMappingMx.Lock()
		defer h.getMappingMx.Unlock()
		if len(h.getMappingClients) == 0 {
			return
		}
		h.mappingCacheMx.RLock()
		lastVersion := h.mappingCache[h.mappingTail].Value
		h.mappingCacheMx.RUnlock()

		for hctx, args := range h.getMappingClients {
			resp := tlmetadata.GetNewMappingsResponse{
				CurrentVersion: args.From,
				LastVersion:    lastVersion,
			}
			var err error
			hctx.Response, err = args.WriteResult(hctx.Response, resp)
			hctx.SendHijackedResponse(err)
		}
		h.log("[info] broadcast empty mapping response to %d long poll clients", len(h.getMappingClients))
		clear(h.getMappingClients)
	}()
}

func (h *Handler) broadcastJournal() {
	// for correctness, we read and update journal and waiting clients under a single lock
	h.getJournalMx.Lock()
	defer h.getJournalMx.Unlock()
	if len(h.getJournalClients) == 0 {
		return
	}
	minVersion := int64(math.MaxInt64)
	for _, args := range h.getJournalClients {
		minVersion = min(minVersion, args.From)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	journalNew, err := h.db.JournalEvents(ctx, minVersion, 100)
	if err != nil {
		return
	}
	if len(journalNew) == 0 {
		return
	}
	clientGotResponseCount := 0
	for hctx, args := range h.getJournalClients {
		resp := tlmetadata.GetJournalResponsenew{
			CurrentVersion: journalNew[len(journalNew)-1].Version,
			Events:         journalNew,
		}
		for len(resp.Events) != 0 && resp.Events[0].Version <= args.From { // minVersion is almost the same, so this is not slow
			resp.Events = resp.Events[1:]
		}
		if len(resp.Events) == 0 {
			continue
		}
		delete(h.getJournalClients, hctx)
		hctx.Response, err = args.WriteResult(hctx.Response, resp)
		hctx.SendHijackedResponse(err)
		clientGotResponseCount++
	}
	if clientGotResponseCount > 0 {
		h.log("[info] client got journal response count: %d", clientGotResponseCount)
	}
}

func (h *Handler) broadcastMapping() {
	// for correctness, we read and update mapping and waiting clients under a single lock
	h.getMappingMx.Lock()
	defer h.getMappingMx.Unlock()
	if len(h.getMappingClients) == 0 {
		return
	}
	minVersion := int32(math.MaxInt32)
	for _, args := range h.getMappingClients {
		minVersion = min(minVersion, args.From)
	}
	h.mappingCacheMx.RLock()
	m := append(append([]tlstatshouse.Mapping{}, h.mappingCache[h.mappingHead:]...), h.mappingCache[:h.mappingHead]...)
	lastVersion := h.mappingCache[h.mappingTail].Value
	minCacheVersion := h.mappingCache[h.mappingHead].Value
	cacheEnabled := h.mappingHead != h.mappingTail
	h.mappingCacheMx.RUnlock()

	var err error
	if !cacheEnabled || minCacheVersion > minVersion+1 {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		m, lastVersion, err = h.db.GetNewMappings(ctx, minVersion, 100)
		if err != nil {
			return
		}
	}
	if len(m) == 0 {
		return
	}
	clientGotResponseCount := 0
	for hctx, args := range h.getMappingClients {
		resp := tlmetadata.GetNewMappingsResponse{
			CurrentVersion: m[len(m)-1].Value,
			LastVersion:    lastVersion,
			Pairs:          m,
		}
		for len(resp.Pairs) != 0 && resp.Pairs[0].Value <= args.From { // minVersion is almost the same, so this is not slow
			resp.Pairs = resp.Pairs[1:]
		}
		if len(resp.Pairs) == 0 {
			continue
		}
		delete(h.getMappingClients, hctx)
		hctx.Response, err = args.WriteResult(hctx.Response, resp)
		hctx.SendHijackedResponse(err)
		clientGotResponseCount++
	}
	if clientGotResponseCount > 0 {
		h.log("[info] client got mapping response count: %d", clientGotResponseCount)
	}
}

func (h *Handler) RawGetJournal(ctx context.Context, hctx *rpc.HandlerContext) (string, error) {
	var args tlmetadata.GetJournalnew
	_, err := args.Read(hctx.Request)
	if err != nil {
		return "", fmt.Errorf("failed to deserialize metadata.getJournal request: %w", err)
	}
	// we need both speed and correctness here, so we first get events without lock,
	m, err := h.db.JournalEvents(ctx, args.From, args.Limit)
	if err != nil {
		return "", fmt.Errorf("failed to get metrics update: %w", err)
	}
	if len(m) > 0 {
		resp := tlmetadata.GetJournalResponsenew{
			CurrentVersion: m[len(m)-1].Version,
			Events:         m,
		}
		hctx.Response, err = args.WriteResult(hctx.Response, resp)
		return "", err
	}
	if args.IsSetReturnIfEmpty() {
		hctx.Response, err = args.WriteResult(hctx.Response, tlmetadata.GetJournalResponsenew{CurrentVersion: args.From})
		return "", err
	}
	// but if we get 0, we must take lock and repeat getting events, to see if any appeared
	// while we were not holding lock
	h.getJournalMx.Lock()
	defer h.getJournalMx.Unlock()
	m, err = h.db.JournalEvents(ctx, args.From, args.Limit)
	if err != nil {
		return "", fmt.Errorf("failed to get metrics update: %w", err)
	}
	if len(m) > 0 {
		resp := tlmetadata.GetJournalResponsenew{
			CurrentVersion: m[len(m)-1].Version,
			Events:         m,
		}
		hctx.Response, err = args.WriteResult(hctx.Response, resp)
		return "", err
	}
	h.getJournalClients[hctx] = args
	return "", hctx.HijackResponse(h)
}

func (h *Handler) RawGetNewMappings(ctx context.Context, hctx *rpc.HandlerContext) (string, error) {
	var args tlmetadata.GetNewMappings
	_, err := args.Read(hctx.Request)
	if err != nil {
		return "", fmt.Errorf("failed to deserialize metadata.GetNewMappings request: %w", err)
	}
	var lastV int32
	var m []tlstatshouse.Mapping
	getNew := func() error {
		h.mappingCacheMx.RLock()
		m = append(append(m[:0], h.mappingCache[h.mappingHead:]...), h.mappingCache[:h.mappingHead]...)
		lastV = h.mappingCache[h.mappingTail].Value
		minVersion := h.mappingCache[h.mappingHead].Value
		cacheEnabled := h.mappingHead != h.mappingTail
		h.mappingCacheMx.RUnlock()

		if cacheEnabled && minVersion <= args.From+1 {
			for len(m) != 0 && m[0].Value <= args.From {
				m = m[1:]
			}
			return nil
		}
		m, lastV, err = h.db.GetNewMappings(ctx, args.From, args.Limit)
		return err
	}
	if err = getNew(); err != nil {
		return "", fmt.Errorf("failed to get metrics update: %w", err)
	}
	if len(m) > 0 {
		resp := tlmetadata.GetNewMappingsResponse{
			CurrentVersion: m[len(m)-1].Value,
			LastVersion:    lastV,
			Pairs:          m,
		}
		hctx.Response, err = args.WriteResult(hctx.Response, resp)
		return "", err
	}
	if args.IsSetReturnIfEmpty() {
		hctx.Response, err = args.WriteResult(hctx.Response, tlmetadata.GetNewMappingsResponse{CurrentVersion: args.From, LastVersion: lastV})
		return "", err
	}
	// but if we get 0, we must take lock and repeat getting events, to see if any appeared
	// while we were not holding lock
	h.getMappingMx.Lock()
	defer h.getMappingMx.Unlock() // HijackResponse must be under our lock
	if err = getNew(); err != nil {
		return "", fmt.Errorf("failed to get metrics update: %w", err)
	}
	if len(m) > 0 {
		resp := tlmetadata.GetNewMappingsResponse{
			CurrentVersion: m[len(m)-1].Value,
			LastVersion:    lastV,
			Pairs:          m,
		}
		hctx.Response, err = args.WriteResult(hctx.Response, resp)
		return "", err
	}

	h.getMappingClients[hctx] = args
	return "", hctx.HijackResponse(h)
}

func (h *Handler) RawGetHistory(ctx context.Context, hctx *rpc.HandlerContext) (string, error) {
	var args tlmetadata.GetHistoryShortInfo
	_, err := args.Read(hctx.Request)
	if err != nil {
		return "", fmt.Errorf("failed to deserialize metadata.GetHistoryShortInfo request: %w", err)
	}
	resp, err := h.db.GetHistoryShort(ctx, args.Id)
	if err != nil {
		return "", err
	}
	hctx.Response, err = args.WriteResult(hctx.Response, resp)
	return "", err
}

func (h *Handler) RawGetEntity(ctx context.Context, hctx *rpc.HandlerContext) (string, error) {
	var args tlmetadata.GetEntity
	_, err := args.Read(hctx.Request)
	if err != nil {
		return "", fmt.Errorf("failed to deserialize metadata.GetEntity request: %w", err)
	}
	e, err := h.db.GetEntityVersioned(ctx, args.Id, args.Version)
	if err != nil {
		return "", err
	}
	hctx.Response, err = args.WriteResult(hctx.Response, e)
	return "ok", err

}

func (h *Handler) RawEditEntity(ctx context.Context, hctx *rpc.HandlerContext) (string, error) {
	var args tlmetadata.EditEntitynew
	if err := checkLimit(hctx.Request); err != nil {
		return "request_limit", err
	}
	_, err := args.Read(hctx.Request)
	if err != nil {
		return "", fmt.Errorf("failed to deserialize metadata.editMetricEvent request: %w", err)
	}
	event, err := h.db.SaveEntity(ctx, args.Event.Name, args.Event.Id, args.Event.Version, args.Event.Data, args.IsSetCreate(), args.IsSetDelete(), args.Event.EventType, args.Event.Metadata)
	if errors.Is(err, errInvalidMetricVersion) {
		return "", data_model.ErrEntityInvalidVersion
	}
	if errors.Is(err, errMetricIsExist) {
		return "", data_model.ErrEntityExists
	}
	if err != nil {
		return "", fmt.Errorf("failed to create event: %w", err)
	}
	hctx.Response, err = args.WriteResult(hctx.Response, event)
	h.broadcastJournal()
	return "", err
}

func (h *Handler) RawGetMappingByValue(ctx context.Context, hctx *rpc.HandlerContext) (string, error) {
	var args tlmetadata.GetMapping
	_, err := args.Read(hctx.Request)
	if err != nil {
		return "", fmt.Errorf("failed to deserialize metadata.getMapping request: %w", err)
	}

	var mapping tlmetadata.GetMappingResponse
	var notExists bool
	if args.IsSetCreateIfAbsent() {
		mapping, err = h.db.GetOrCreateMapping(ctx, args.Metric, args.Key)
	} else {
		var id int32
		id, notExists, err = h.db.GetMappingByValue(ctx, args.Key)
		mapping = tlmetadata.GetMappingResponse0{Id: id}.AsUnion()
	}
	if err != nil {
		return "", err
	}
	if notExists {
		mapping = tlmetadata.GetMappingResponseKeyNotExists{}.AsUnion()
	}
	status := "load_mapping"
	if m, ok := mapping.AsCreated(); ok {
		status = "create_mapping"
		h.updateMappingCache(ctx, tlstatshouse.Mapping{Str: args.Key, Value: m.Id})
		h.broadcastMapping()
	}
	hctx.Response, err = args.WriteResult(hctx.Response, mapping)
	return status, err
}

// RawPutMapping Agent cache, Aggregator cache and Aggregator mapping replica DB will not know about changes!
func (h *Handler) RawPutMapping(ctx context.Context, hctx *rpc.HandlerContext) (string, error) {
	var args tlmetadata.PutMapping
	_, err := args.Read(hctx.Request)
	if err != nil {
		return "", fmt.Errorf("failed to deserialize metadata.putMappingEvent request: %w", err)
	}
	err = h.db.PutMapping(ctx, args.Keys, args.Value)
	if err != nil {
		return "", err
	}

	// force bootstrap
	h.updateMappingCache(ctx, tlstatshouse.Mapping{Value: math.MaxInt32})
	h.broadcastMapping()
	hctx.Response, err = args.WriteResult(hctx.Response, tlmetadata.PutMappingResponse{})
	return "", err
}

func (h *Handler) updateMappingCache(ctx context.Context, pair tlstatshouse.Mapping) {
	h.mappingCacheMx.Lock()
	defer h.mappingCacheMx.Unlock()

	if h.mappingHead == h.mappingTail {
		h.bootStrapMappingCacheUnlocked(ctx)
		return
	}
	if h.mappingCache[h.mappingTail].Value >= pair.Value {
		return
	}
	if h.mappingCache[h.mappingTail].Value == pair.Value-1 {
		h.mappingCache[h.mappingHead] = pair
		h.mappingTail = h.mappingHead
		h.mappingHead = normalizeCircleIndex(h.mappingHead+1, mappingCacheSize)
		return
	}
	h.bootStrapMappingCacheUnlocked(ctx)
}

func (h *Handler) bootStrapMappingCacheUnlocked(ctx context.Context) {
	mappings, err := h.db.GetLastNMappings(ctx, mappingCacheSize)
	if err != nil || len(mappings) != mappingCacheSize {
		h.mappingHead, h.mappingTail = 0, 0
		h.log("[err] failed to bootstrap mapping cache: %v", err)
		return
	}
	copy(h.mappingCache[:], mappings)
	h.mappingHead = 0
	h.mappingTail = mappingCacheSize - 1
}

func (h *Handler) RawGetMappingByID(ctx context.Context, hctx *rpc.HandlerContext) (string, error) {
	var args tlmetadata.GetInvertMapping
	_, err := args.Read(hctx.Request)
	if err != nil {
		return "", fmt.Errorf("failed to deserialize metadata.getInvertMapping request: %w", err)
	}
	k, isExists, err := h.db.GetMappingByID(ctx, args.Id)
	if err != nil {
		return "", err
	}

	var resp tlmetadata.GetInvertMappingResponse
	var status string
	if !isExists {
		resp = tlmetadata.GetInvertMappingResponseKeyNotExists{}.AsUnion()
		status = "key_not_exists"
	} else {
		resp = tlmetadata.GetInvertMappingResponse0{Key: k}.AsUnion()
		status = "get"
	}
	hctx.Response, err = args.WriteResult(hctx.Response, resp)
	return status, err
}

// resetFlood
func (h *Handler) RawResetFlood(ctx context.Context, hctx *rpc.HandlerContext) (string, error) {
	var args tlmetadata.ResetFlood
	_, err := args.Read(hctx.Request)
	if err != nil {
		return "", fmt.Errorf("failed to deserialize metadata.resetFlood request: %w", err)
	}

	_, _, err = h.db.ResetFlood(ctx, args.Metric, 0)

	if err != nil {
		return "", err
	}
	resp := tlmetadata.ResetFloodResponse{}
	hctx.Response, err = args.WriteResult(hctx.Response, resp)
	return "", err
}

func (h *Handler) ResetFlood2(ctx context.Context, args tlmetadata.ResetFlood2) (tlmetadata.ResetFloodResponse2, string, error) {
	before, after, err := h.db.ResetFlood(ctx, args.Metric, int64(args.Value))
	return tlmetadata.ResetFloodResponse2{BudgetBefore: int32(before), BudgetAfter: int32(after)}, args.Metric, err
}

func (h *Handler) GetTagMappingBootstrap(ctx context.Context, args tlmetadata.GetTagMappingBootstrap) (tlstatshouse.GetTagMappingBootstrapResult, string, error) {
	var ret tlstatshouse.GetTagMappingBootstrapResult

	totalSizeEstimate := 0
	boostrapDifferences := 0
	response, err := h.db.GetBootstrap(ctx)
	if err != nil {
		return tlstatshouse.GetTagMappingBootstrapResult{}, "", nil
	}
	for _, ma := range response.Mappings {
		k, isExists, err := h.db.GetMappingByID(ctx, ma.Value)
		if err != nil {
			return ret, "", err
		}
		if !isExists { // skip, no problem
			continue
		}
		if k != ma.Str { // skip, log some examples
			boostrapDifferences++
			if boostrapDifferences < 10 {
				h.log("[info] tag value %q from bootstrap is different from value in DB %q for ID %d", ma.Value, k, ma.Value)
			}
			continue
		}
		totalSizeEstimate += len(k) + 3 + 4 // 3 is pessimistic padding
		ret.Mappings = append(ret.Mappings, tlstatshouse.Mapping{
			Str:   k,
			Value: ma.Value,
		})
		if totalSizeEstimate > MaxBoostrapResponseSize {
			break
		}
	}
	h.log("[info] returning boostrap of %d mappings of ~size %d, %d differences found", len(ret.Mappings), totalSizeEstimate, boostrapDifferences)
	return ret, "get_bootstrap", nil
}

func (h *Handler) PutTagMappingBootstrap(ctx context.Context, args tlmetadata.PutTagMappingBootstrap) (tlstatshouse.PutTagMappingBootstrapResult, string, error) {
	count, err := h.db.PutBootstrap(ctx, args.Mappings)
	return tlstatshouse.PutTagMappingBootstrapResult{CountInserted: count}, "put_bootstrap", err
}

func normalizeCircleIndex(ind, len int) int {
	ind %= len
	if ind >= 0 {
		return ind
	}
	return len + ind
}
