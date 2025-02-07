// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metajournal

import (
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
)

// returns only for old tests
func (ms *JournalFast) getJournalDiffLocked3(verNumb int64) tlmetadata.GetJournalResponsenew {
	ms.getJournalResponse.CurrentVersion = ms.currentVersion
	ms.getJournalResponse.Events = ms.getJournalResponse.Events[:0]
	if verNumb >= ms.currentVersion { // wait until version changes
		return ms.getJournalResponse
	}
	bytesSize := 0
	ms.order.AscendGreaterOrEqual(journalOrder{version: verNumb + 1}, func(item journalOrder) bool {
		event, ok := ms.journal[item.key]
		if !ok {
			panic("missing journal entry during iteration")
		}
		event.FieldMask = 0
		event.SetNamespaceId(event.NamespaceId) // TODO - why?
		ms.getJournalResponse.Events = append(ms.getJournalResponse.Events, event.Event)
		bytesSize += len(event.Name)
		bytesSize += len(event.Data)
		bytesSize += 60
		if len(ms.getJournalResponse.Events) >= data_model.MaxJournalItemsSent {
			return false
		}
		if bytesSize >= data_model.MaxJournalBytesSent { // overshoot by 1 item max
			return false
		}
		return true
	})
	return ms.getJournalResponse
}

func (ms *JournalFast) broadcastJournal() {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	ms.clientsMu.Lock()
	defer ms.clientsMu.Unlock()
	// TODO - most clients wait with the same version, remember response bytes in local map
	for hctx, args := range ms.metricsVersionClients3 {
		if ms.metricsDead {
			delete(ms.metricsVersionClients3, hctx)
			hctx.SendHijackedResponse(errDeadMetrics)
			continue
		}
		ms.getJournalDiffLocked3(args.From)
		if len(ms.getJournalResponse.Events) == 0 {
			continue
		}
		delete(ms.metricsVersionClients3, hctx)
		var err error
		hctx.Response, err = args.WriteResult(hctx.Response, ms.getJournalResponse)
		if err != nil {
			ms.builtinAddValue(&ms.BuiltinLongPollDelayedError, 0)
		} else {
			ms.builtinAddValue(&ms.BuiltinLongPollDelayedOK, float64(len(ms.getJournalResponse.Events)))
		}
		hctx.SendHijackedResponse(err)
	}
}

func (ms *JournalFast) CancelHijack(hctx *rpc.HandlerContext) {
	ms.clientsMu.Lock()
	defer ms.clientsMu.Unlock()
	delete(ms.metricsVersionClients3, hctx)
}

func (ms *JournalFast) HandleGetMetrics3(args tlstatshouse.GetMetrics3, hctx *rpc.HandlerContext) error {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if ms.metricsDead {
		ms.builtinAddValue(&ms.BuiltinLongPollImmediateError, 0)
		return errDeadMetrics
	}
	ms.getJournalDiffLocked3(args.From)
	if len(ms.getJournalResponse.Events) != 0 {
		var err error
		hctx.Response, err = args.WriteResult(hctx.Response, ms.getJournalResponse)
		if err != nil {
			ms.builtinAddValue(&ms.BuiltinLongPollImmediateError, 0)
		} else {
			ms.builtinAddValue(&ms.BuiltinLongPollImmediateOK, float64(len(ms.getJournalResponse.Events)))
		}
		return err
	}
	ms.clientsMu.Lock()
	defer ms.clientsMu.Unlock()
	ms.metricsVersionClients3[hctx] = args
	ms.builtinAddValue(&ms.BuiltinLongPollEnqueue, 1)
	return hctx.HijackResponse(ms)
}
