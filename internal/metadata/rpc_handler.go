// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metadata

import (
	"fmt"
	"math"
	"sync"
	"time"

	"context"

	"go.uber.org/atomic"

	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse_metadata"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
	"github.com/vkcom/statshouse/internal/vkgo/statlogs"
)

const MaxBoostrapResponseSize = 1024 * 1024 // TODO move somewhere
const longPollTimeout = time.Hour

type Handler struct {
	db *DBV2

	getJournalMx      sync.Mutex
	getJournalClients []getJournalClient // by getJournalMx
	getJournalLength  *atomic.Int64      // to avoid lock when send stats
	minVersion        int64              // by getJournalMx
	metricChange      chan struct{}

	host string
	log  func(s string, args ...interface{})
}

type getJournalClient struct {
	args tlstatshouse_metadata.GetJournalnew
	hctx *rpc.HandlerContext
}

func NewHandler(db *DBV2, host string, log func(s string, args ...interface{})) *Handler {
	h := &Handler{
		db:                db,
		getJournalMx:      sync.Mutex{},
		getJournalLength:  atomic.NewInt64(0),
		getJournalClients: nil,
		minVersion:        math.MaxInt64,
		log:               log,
		host:              host,
		metricChange:      make(chan struct{}, 1),
	}

	go func() {
		t := time.NewTimer(longPollTimeout)
		for {
			select {
			case <-time.After(time.Second):
			case <-h.metricChange:
			case <-t.C:
				h.broadcastJournal(true)
				t = time.NewTimer(longPollTimeout)
				continue
			}
			h.broadcastJournal(false)
		}
	}()
	h.initStats()
	return h
}

func (h *Handler) notifyMetricChange() {
	select {
	case h.metricChange <- struct{}{}:
	default:
	}
}

func (h *Handler) broadcastJournal(sentToAll bool) {
	h.getJournalMx.Lock()
	qLength := len(h.getJournalClients)
	h.getJournalLength.Store(int64(qLength))
	h.getJournalMx.Unlock()
	if qLength == 0 {
		return
	}
	journalNew, err := h.db.JournalEvents(h.minVersion, 100)
	if err != nil {
		return
	}
	if len(journalNew) == 0 && !sentToAll {
		return
	}
	h.getJournalMx.Lock()
	defer h.getJournalMx.Unlock()
	pos := 0
	eventsToClient := make([]tlstatshouse_metadata.Event, 0)
	clientGotResponseCount := 0
	var newMinimum int64 = math.MaxInt64
	for _, client := range h.getJournalClients {
		resp := filterResponse(journalNew, eventsToClient, func(m tlstatshouse_metadata.Event) bool {
			return m.Version > client.args.From
		})
		if len(resp.Events) == 0 {
			if sentToAll {
				resp.CurrentVersion = client.args.From
			} else {
				h.getJournalClients[pos] = client
				pos++
				if client.args.From < newMinimum {
					newMinimum = client.args.From
				}
				continue
			}
		}
		client.hctx.Response, err = client.args.WriteResult(client.hctx.Response, resp)
		client.hctx.SendHijackedResponse(err)
		eventsToClient = eventsToClient[:0]
		clientGotResponseCount++
	}
	if clientGotResponseCount > 0 {
		h.log("[info] client got response count: %d", clientGotResponseCount)
	}
	h.minVersion = newMinimum
	h.getJournalClients = h.getJournalClients[:pos]
	h.getJournalLength.Store(int64(len(h.getJournalClients)))
}

func (h *Handler) initStats() {
	statlogs.StartRegularMeasurement(func(registry *statlogs.Registry) {
		registry.AccessMetricRaw(sqlengineLoadJournalWaitQLen, statlogs.RawTags{Tag1: h.host}).Value(float64(h.getJournalLength.Load()))
	})
}

func (h *Handler) RawGetJournal(ctx context.Context, hctx *rpc.HandlerContext) (string, error) {
	var args tlstatshouse_metadata.GetJournalnew
	_, err := args.Read(hctx.Request)
	if err != nil {
		return "", fmt.Errorf("failed to deserialize metadata.getJournal request: %w", err)
	}
	version := args.From
	m, err := h.db.JournalEvents(version, args.Limit)
	if err != nil {
		return "", fmt.Errorf("failed to get metrics update: %w", err)
	}
	if len(m) > 0 {
		resp := filterResponse(m, nil, func(m tlstatshouse_metadata.Event) bool { return true })
		hctx.Response, err = args.WriteResult(hctx.Response, resp)
		return "", err
	}
	h.getJournalMx.Lock()
	h.getJournalClients = append(h.getJournalClients, getJournalClient{
		args: args,
		hctx: hctx,
	})
	if h.minVersion > args.From {
		h.minVersion = args.From
	}
	h.getJournalMx.Unlock()
	return "", hctx.HijackResponse()
}

func (h *Handler) RawEditEntity(ctx context.Context, hctx *rpc.HandlerContext) (string, error) {
	var args tlstatshouse_metadata.EditEntitynew
	_, err := args.Read(hctx.Request)
	if err != nil {
		return "", fmt.Errorf("failed to deserialize metadata.editMetricEvent request: %w", err)
	}
	event, err := h.db.SaveEntity(args.Event.Name, args.Event.Id, args.Event.Version, args.Event.Data, args.IsSetCreate(), args.IsSetDelete(), args.Event.EventType)
	if err == errInvalidMetricVersion {
		return "", fmt.Errorf("invalid version. Reload this page and try again")
	}
	if err != nil {
		return "", fmt.Errorf("failed to create event: %w", err)
	}
	hctx.Response, err = args.WriteResult(hctx.Response, event)
	h.notifyMetricChange()
	return "", err
}

func (h *Handler) RawGetMappingByValue(ctx context.Context, hctx *rpc.HandlerContext) (string, error) {
	var args tlstatshouse_metadata.GetMapping
	_, err := args.Read(hctx.Request)
	if err != nil {
		return "", fmt.Errorf("failed to deserialize metadata.getMapping request: %w", err)
	}

	var mapping tlstatshouse_metadata.GetMappingResponseUnion
	var notExists bool
	if args.IsSetCreateIfAbsent() {
		mapping, err = h.db.GetOrCreateMapping(args.Metric, args.Key)
	} else {
		var id int32
		id, notExists, err = h.db.GetMappingByValue(args.Key)
		mapping = tlstatshouse_metadata.GetMappingResponse{Id: id}.AsUnion()
	}
	if err != nil {
		return "", err
	}
	if notExists {
		mapping = tlstatshouse_metadata.GetMappingResponseKeyNotExists{}.AsUnion()
	}
	status := "load_mapping"
	if mapping.IsCreated() {
		status = "create_mapping"
	}
	hctx.Response, err = args.WriteResult(hctx.Response, mapping)
	return status, err
}

func (h *Handler) RawPutMapping(ctx context.Context, hctx *rpc.HandlerContext) (string, error) {
	var args tlstatshouse_metadata.PutMapping
	_, err := args.Read(hctx.Request)
	if err != nil {
		return "", fmt.Errorf("failed to deserialize metadata.putMappingEvent request: %w", err)
	}
	err = h.db.PutMapping(args.Keys, args.Value)
	if err != nil {
		return "", err
	}
	hctx.Response, err = args.WriteResult(hctx.Response, tlstatshouse_metadata.PutMappingResponse{})
	return "", err
}

func (h *Handler) RawGetMappingByID(ctx context.Context, hctx *rpc.HandlerContext) (string, error) {
	var args tlstatshouse_metadata.GetInvertMapping
	_, err := args.Read(hctx.Request)
	if err != nil {
		return "", fmt.Errorf("failed to deserialize metadata.getInvertMapping request: %w", err)
	}
	k, isExists, err := h.db.GetMappingByID(args.Id)
	if err != nil {
		return "", err
	}

	var resp tlstatshouse_metadata.GetInvertMappingResponseUnion
	var status string
	if !isExists {
		resp = tlstatshouse_metadata.GetInvertMappingResponseKeyNotExists{}.AsUnion()
		status = "key_not_exists"
	} else {
		resp = tlstatshouse_metadata.GetInvertMappingResponse{Key: k}.AsUnion()
		status = "get"
	}
	hctx.Response, err = args.WriteResult(hctx.Response, resp)
	return status, err
}

// resetFlood
func (h *Handler) RawResetFlood(ctx context.Context, hctx *rpc.HandlerContext) (string, error) {
	var args tlstatshouse_metadata.ResetFlood
	_, err := args.Read(hctx.Request)
	if err != nil {
		return "", fmt.Errorf("failed to deserialize metadata.resetFlood request: %w", err)
	}

	err = h.db.ResetFlood(args.Metric)

	if err != nil {
		return "", err
	}
	resp := tlstatshouse_metadata.ResetFloodResponse{}
	hctx.Response, err = args.WriteResult(hctx.Response, resp)
	return "", err
}

func (h *Handler) ResetFlood2(ctx context.Context, args tlstatshouse_metadata.ResetFlood2) (tlstatshouse_metadata.ResetFloodResponse2, error) {
	return tlstatshouse_metadata.ResetFloodResponse2{}, h.db.ResetFlood(args.Metric) // TODO - return budgets before and after reset
}

func (h *Handler) GetTagMappingBootstrap(ctx context.Context, args tlstatshouse_metadata.GetTagMappingBootstrap) (tlstatshouse.GetTagMappingBootstrapResult, error) {
	var ret tlstatshouse.GetTagMappingBootstrapResult

	totalSizeEstimate := 0
	boostrapDifferences := 0
	response, err := h.db.GetBootstrap()
	if err != nil {
		return tlstatshouse.GetTagMappingBootstrapResult{}, nil
	}
	for _, ma := range response.Mappings {
		k, isExists, err := h.db.GetMappingByID(ma.Value)
		if err != nil {
			return ret, err
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
	return ret, nil
}

func (h *Handler) PutTagMappingBootstrap(ctx context.Context, args tlstatshouse_metadata.PutTagMappingBootstrap) (tlstatshouse.PutTagMappingBootstrapResult, error) {
	count, err := h.db.PutBootstrap(args.Mappings)
	return tlstatshouse.PutTagMappingBootstrapResult{CountInserted: count}, err
}

func filterResponse(ms []tlstatshouse_metadata.Event, buffer []tlstatshouse_metadata.Event, filter func(m tlstatshouse_metadata.Event) bool) tlstatshouse_metadata.GetJournalResponsenew {
	var currentVersion int64 = math.MinInt64
	result := tlstatshouse_metadata.GetJournalResponsenew{}
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
