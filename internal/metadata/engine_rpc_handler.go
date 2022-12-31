// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metadata

import (
	"os"
	"sync"
	"syscall"
	"time"

	"context"

	"github.com/vkcom/statshouse/internal/data_model/gen2/tl"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlengine"
)

type EngineRpcHandler struct {
	SignalCh chan os.Signal
	db       *DBV2

	backupMx      sync.Mutex
	reindexMx     sync.Mutex
	reindexStatus tlengine.ReindexStatusUnion
	binlogPrefix  string
}

func NewEngineRpcHandler(binlogPrefix string, db *DBV2) *EngineRpcHandler {
	return &EngineRpcHandler{
		binlogPrefix:  binlogPrefix,
		SignalCh:      make(chan os.Signal, 3),
		db:            db,
		backupMx:      sync.Mutex{},
		reindexMx:     sync.Mutex{},
		reindexStatus: tlengine.ReindexStatusNever{}.AsUnion(),
	}
}

func (h *EngineRpcHandler) Backup(ctx context.Context, prefix string) error {
	h.backupMx.Lock()
	defer h.backupMx.Unlock()
	h.reindexMx.Lock()
	h.reindexStatus = tlengine.ReindexStatusRunning{StartTime: int32(time.Now().Unix())}.AsUnion()
	h.reindexMx.Unlock()
	err := h.db.backup(ctx, prefix)
	h.reindexMx.Lock()
	defer h.reindexMx.Unlock()
	if err != nil {
		h.reindexStatus = tlengine.ReindexStatusFailed{FinishTime: int32(time.Now().Unix())}.AsUnion()
		return err
	}
	h.reindexStatus = tlengine.ReindexStatusDone{FinishTime: int32(time.Now().Unix())}.AsUnion()
	return nil
}

func (h *EngineRpcHandler) SendSignal(ctx context.Context, args tlengine.SendSignal) (tl.True, error) {
	sig := args.Signal
	h.SignalCh <- syscall.Signal(sig)
	return tl.True{}, nil
}

func (h *EngineRpcHandler) GetReindexStatus(ctx context.Context, args tlengine.GetReindexStatus) (tlengine.ReindexStatusUnion, error) {
	h.reindexMx.Lock()
	defer h.reindexMx.Unlock()
	return h.reindexStatus, nil
}

func (h *EngineRpcHandler) GetBinlogPrefixes(ctx context.Context, args tlengine.GetBinlogPrefixes) ([]tlengine.BinlogPrefix, error) {
	return []tlengine.BinlogPrefix{{
		BinlogPrefix:   h.binlogPrefix,
		SnapshotPrefix: h.binlogPrefix,
	}}, nil
}
