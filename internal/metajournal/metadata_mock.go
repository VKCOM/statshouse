// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metajournal

import (
	"context"
	"sync"

	"github.com/vkcom/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
)

type MetadataMock struct {
	handler tlmetadata.Handler
	dataMu  sync.RWMutex
	Data    map[string]int32
	i       int32
}

func NewMetadataMock() *MetadataMock {
	m := &MetadataMock{
		Data: map[string]int32{},
	}
	m.handler = tlmetadata.Handler{
		GetMapping: m.handleGetMapping,
	}
	return m
}

func (m *MetadataMock) Handle(ctx context.Context, hctx *rpc.HandlerContext) error {
	return m.handler.Handle(ctx, hctx)
}

func (m *MetadataMock) handleGetMapping(ctx context.Context, args tlmetadata.GetMapping) (tlmetadata.GetMappingResponseUnion, error) {
	m.dataMu.Lock()
	defer m.dataMu.Unlock()
	k, ok := m.Data[args.Key]
	if ok {
		return tlmetadata.GetMappingResponse{Id: k}.AsUnion(), nil
	}
	m.i++
	m.Data[args.Key] = m.i
	return tlmetadata.GetMappingResponseCreated{Id: m.i}.AsUnion(), nil
}
