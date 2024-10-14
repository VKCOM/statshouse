// Copyright 2022 V Kontakte LLC
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

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/format"

	"github.com/vkcom/statshouse-go"

	"github.com/vkcom/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
)

const MaxBoostrapResponseSize = 1024 * 1024 // TODO move somewhere
const longPollTimeout = time.Hour

type Handler struct {
	db *DBV2

	getJournalMx      sync.Mutex
	getJournalClients map[*rpc.HandlerContext]tlmetadata.GetJournalnew // by getJournalMx
	minVersion        int64                                            // by getJournalMx

	host string
	log  func(s string, args ...interface{})
}

func NewHandler(db *DBV2, host string, log func(s string, args ...interface{})) *Handler {
	h := &Handler{
		db:                db,
		getJournalMx:      sync.Mutex{},
		getJournalClients: map[*rpc.HandlerContext]tlmetadata.GetJournalnew{},
		minVersion:        math.MaxInt64,
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
	h.initStats()
	return h
}

func (h *Handler) CancelHijack(hctx *rpc.HandlerContext) {
	statshouse.Count("meta_cancel_hijack", statshouse.Tags{1: h.host}, 1)
	h.getJournalMx.Lock()
	defer h.getJournalMx.Unlock()
	delete(h.getJournalClients, hctx)
}

func (h *Handler) broadcastCancel() {
	h.getJournalMx.Lock()
	defer h.getJournalMx.Unlock()
	clientGotResponseCount := len(h.getJournalClients)
	for hctx, args := range h.getJournalClients {
		delete(h.getJournalClients, hctx)
		resp := filterResponse(nil, nil, func(m tlmetadata.Event) bool {
			return true
		})
		resp.CurrentVersion = args.From
		var err error
		hctx.Response, err = args.WriteResult(hctx.Response, resp)
		hctx.SendHijackedResponse(err)
	}
	if clientGotResponseCount > 0 {
		h.log("[info] client got cancel error count: %d", clientGotResponseCount)
	}
}

func (h *Handler) broadcastJournal() {
	h.getJournalMx.Lock()
	qLength := len(h.getJournalClients)
	h.getJournalMx.Unlock()
	if qLength == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	journalNew, err := h.db.JournalEvents(ctx, h.minVersion, 100)
	cancel()
	if err != nil {
		return
	}
	if len(journalNew) == 0 {
		return
	}
	h.getJournalMx.Lock()
	defer h.getJournalMx.Unlock()
	eventsToClient := make([]tlmetadata.Event, 0)
	clientGotResponseCount := 0
	var newMinimum int64 = math.MaxInt64
	for hctx, args := range h.getJournalClients {
		resp := filterResponse(journalNew, eventsToClient, func(m tlmetadata.Event) bool {
			return m.Version > args.From
		})
		if len(resp.Events) == 0 {
			if args.From < newMinimum {
				newMinimum = args.From
			}
			continue
		}
		delete(h.getJournalClients, hctx)
		hctx.Response, err = args.WriteResult(hctx.Response, resp)
		hctx.SendHijackedResponse(err)
		eventsToClient = eventsToClient[:0]
		clientGotResponseCount++
	}
	if clientGotResponseCount > 0 {
		h.log("[info] client got response count: %d", clientGotResponseCount)
	}
	h.minVersion = newMinimum
}

func (h *Handler) initStats() {
	statshouse.StartRegularMeasurement(func(client *statshouse.Client) {
		h.getJournalMx.Lock()
		qLength := len(h.getJournalClients)
		h.getJournalMx.Unlock()
		client.Value(format.BuiltinMetricNameMetaClientWaits, statshouse.Tags{1: h.host}, float64(qLength))
	})
}

func (h *Handler) RawGetJournal(ctx context.Context, hctx *rpc.HandlerContext) (string, error) {
	var args tlmetadata.GetJournalnew
	_, err := args.Read(hctx.Request)
	if err != nil {
		return "", fmt.Errorf("failed to deserialize metadata.getJournal request: %w", err)
	}
	version := args.From
	m, err := h.db.JournalEvents(ctx, version, args.Limit)
	if err != nil {
		return "", fmt.Errorf("failed to get metrics update: %w", err)
	}
	if len(m) > 0 {
		resp := filterResponse(m, nil, func(m tlmetadata.Event) bool { return true })
		hctx.Response, err = args.WriteResult(hctx.Response, resp)
		return "", err
	}
	h.getJournalMx.Lock()
	defer h.getJournalMx.Unlock() // HijackResponse must be under our lock
	h.getJournalClients[hctx] = args
	if h.minVersion > args.From {
		h.minVersion = args.From
	}
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
	if mapping.IsCreated() {
		status = "create_mapping"
	}
	hctx.Response, err = args.WriteResult(hctx.Response, mapping)
	return status, err
}

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
	hctx.Response, err = args.WriteResult(hctx.Response, tlmetadata.PutMappingResponse{})
	return "", err
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

func filterResponse(ms []tlmetadata.Event, buffer []tlmetadata.Event, filter func(m tlmetadata.Event) bool) tlmetadata.GetJournalResponsenew {
	var currentVersion int64 = math.MinInt64
	result := tlmetadata.GetJournalResponsenew{}
	for _, m := range ms {
		if !filter(m) {
			continue
		}
		buffer = append(buffer, m)
		if currentVersion < m.Version {
			currentVersion = m.Version
		}
	}
	result.CurrentVersion = currentVersion
	result.Events = buffer
	return result
}
