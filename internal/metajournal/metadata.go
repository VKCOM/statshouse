// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metajournal

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"
	"go4.org/mem"

	"github.com/VKCOM/statshouse/internal/data_model"

	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/VKCOM/statshouse/internal/format"
	"github.com/VKCOM/tl/pkg/rpc"
)

const (
	DefaultMetaTimeout = 2 * time.Second
)

var errorInvalidUserRequest = errors.New("")
var errInvalidKeyValue = errors.New("key value is invalid")
var errEmptyStringMapping = errors.New("empty string mapping is special should never be created")

type MetricMetaLoader struct {
	loadTimeout time.Duration
	client      *tlmetadata.Client
}

func IsUserRequestError(err error) bool {
	return errors.Is(err, errorInvalidUserRequest)
}

func wrapSaveEntityError(err error) error {
	var rpcErr *rpc.Error
	if errors.As(err, &rpcErr) {
		switch rpcErr.Code {
		case data_model.ErrEntityInvalidVersion.Code:
			return errors.Join(errorInvalidUserRequest, fmt.Errorf("%s", data_model.ErrEntityInvalidVersion.Description))
		case data_model.ErrEntityExists.Code:
			return errors.Join(errorInvalidUserRequest, fmt.Errorf("%s", data_model.ErrEntityExists.Description))
		case data_model.ErrEntityNotExists.Code:
			return errors.Join(errorInvalidUserRequest, fmt.Errorf("%s", data_model.ErrEntityNotExists.Description))
		}
	}
	return err
}

func NewMetricMetaLoader(client *tlmetadata.Client, loadTimeout time.Duration) *MetricMetaLoader {
	return &MetricMetaLoader{
		client:      client,
		loadTimeout: loadTimeout,
	}
}

func (l *MetricMetaLoader) SaveEntity(ctx context.Context, event tlmetadata.Event, create bool, del bool) (tlmetadata.Event, error) {
	editMetricReq := tlmetadata.EditEntitynew{Event: event}
	editMetricReq.SetCreate(create)
	editMetricReq.SetDelete(del)
	ctx, cancelFunc := context.WithTimeout(ctx, l.loadTimeout)
	defer cancelFunc()
	event = tlmetadata.Event{}
	err := l.client.EditEntitynew(ctx, editMetricReq, nil, &event)
	return event, wrapSaveEntityError(err)
}

func (l *MetricMetaLoader) GetMetric(ctx context.Context, id int64, version int64) (ret format.MetricMetaValue, err error) {
	entity, err := l.getEntity(ctx, id, version)
	if err != nil {
		return ret, err
	}
	m, err := MetricMetaFromEvent(entity)
	if err != nil {
		return ret, err
	}
	return *m, nil
}

func (l *MetricMetaLoader) GetDashboard(ctx context.Context, id int64, version int64) (ret format.DashboardMeta, err error) {
	entity, err := l.getEntity(ctx, id, version)
	if err != nil {
		return ret, err
	}
	d, err := DashboardMetaFromEvent(entity)
	if err != nil {
		return ret, err
	}
	return *d, nil
}

func (l *MetricMetaLoader) getEntity(ctx context.Context, id int64, version int64) (ret tlmetadata.Event, err error) {
	err = l.client.GetEntity(ctx, tlmetadata.GetEntity{
		Id:      id,
		Version: version,
	}, nil, &ret)
	return ret, err
}

func (l *MetricMetaLoader) GetShortHistory(ctx context.Context, id int64) (ret tlmetadata.HistoryShortResponse, err error) {
	err = l.client.GetHistoryShortInfo(ctx, tlmetadata.GetHistoryShortInfo{
		Id: id,
	}, nil, &ret)
	return ret, err
}

func (l *MetricMetaLoader) LoadJournal(ctx context.Context, lastVersion int64, returnIfEmpty bool) ([]tlmetadata.Event, int64, error) {
	resp := tlmetadata.GetJournalResponsenew{}
	req := tlmetadata.GetJournalnew{
		From:  lastVersion,
		Limit: 1000,
	}
	req.SetReturnIfEmpty(returnIfEmpty)
	extra := rpc.InvokeReqExtra{FailIfNoConnection: true}
	err := l.client.GetJournalnew(ctx, req, &extra, &resp)
	if err != nil {
		log.Println("err: ", err.Error())
		return nil, 0, fmt.Errorf("failed to load journal: %w", err)
	}
	return resp.Events, resp.CurrentVersion, nil
}

func (l *MetricMetaLoader) GetNewMappings(ctx context.Context, lastVersion int32, returnIfEmpty bool) (m []tlstatshouse.Mapping, curV, lastV int32, err error) {
	resp := tlmetadata.GetNewMappingsResponse{}
	req := tlmetadata.GetNewMappings{
		From:  lastVersion,
		Limit: 50000,
	}
	req.SetReturnIfEmpty(returnIfEmpty)
	extra := rpc.InvokeReqExtra{FailIfNoConnection: true}
	err = l.client.GetNewMappings(ctx, req, &extra, &resp)
	if err != nil {
		log.Println("err: ", err.Error())
		return nil, 0, 0, fmt.Errorf("failed to load mapping: %w", err)
	}
	return resp.Pairs, resp.CurrentVersion, resp.LastVersion, nil
}

// TODO - remove from codebase after full switch to rqlite
func (l *MetricMetaLoader) PutTagMapping(ctx context.Context, tag string, id int32) error {
	ctx, cancelFunc := context.WithTimeout(ctx, l.loadTimeout)
	defer cancelFunc()

	err := l.client.PutMapping(ctx, tlmetadata.PutMapping{
		Keys:  []string{tag},
		Value: []int32{id},
	}, nil, &tlmetadata.PutMappingResponse{})
	if err != nil {
		return fmt.Errorf("failed to put mapping: %w", err)
	}
	return nil
}

func (l *MetricMetaLoader) GetTagMapping(ctx context.Context, tag string, metricName string, create bool) (int32, int32, error) {
	if tag == "" {
		return 0, format.TagValueIDAggMappingCreatedStatusErrorInvariant, errEmptyStringMapping
	}
	if !format.ValidStringValue(mem.S(tag)) {
		return 0, format.TagValueIDAggMappingCreatedStatusErrorInvalidString, errInvalidKeyValue
	}

	ctx, cancelFunc := context.WithTimeout(ctx, l.loadTimeout)
	defer cancelFunc()

	req := tlmetadata.GetMapping{
		Metric: metricName,
		Key:    tag,
	}
	req.SetCreateIfAbsent(create)
	resp := tlmetadata.GetMappingResponse{}
	err := l.client.GetMapping(ctx, req, nil, &resp)
	if err != nil {
		return 0, format.TagValueIDAggMappingCreatedStatusErrorRPCFailed, err
	}
	if resp.IsKeyNotExists() {
		return 0, format.TagValueIDAggMappingCreatedStatusErrorNotAskedToCreate, fmt.Errorf("not asked to create mapping for non-existent key %q", tag)
	}
	if resp.IsFloodLimitError() {
		return format.TagValueIDMappingFlood, format.TagValueIDAggMappingCreatedStatusFlood, nil
	}
	if r, ok := resp.AsGetMappingResponse(); ok {
		if r.Id == 0 {
			return 0, format.TagValueIDAggMappingCreatedStatusErrorInvariant, fmt.Errorf("metadata returned %q -> 0 mapping, which is invalid", tag)
		}
		return r.Id, format.TagValueIDAggMappingCreatedStatusOK, nil
	}
	if r, ok := resp.AsCreated(); ok {
		if r.Id == 0 {
			return 0, format.TagValueIDAggMappingCreatedStatusErrorInvariant, fmt.Errorf("metadata created %q -> 0 mapping, which is invalid", tag)
		}
		return r.Id, format.TagValueIDAggMappingCreatedStatusCreated, nil
	}
	// should be never here
	return 0, format.TagValueIDAggMappingCreatedStatusErrorRPCFailed, err
}

func (l *MetricMetaLoader) ResetFlood(ctx context.Context, metricName string, value int32) (before int32, after int32, _ error) {
	ctx, cancel := context.WithTimeout(ctx, l.loadTimeout)
	defer cancel()
	req := tlmetadata.ResetFlood2{
		Metric: metricName,
	}
	if value > 0 {
		req.SetValue(value)
	}
	resp := tlmetadata.ResetFloodResponse2{}
	err := l.client.ResetFlood2(ctx, req, nil, &resp)
	if err != nil {
		return resp.BudgetBefore, resp.BudgetAfter, err
	}
	return resp.BudgetBefore, resp.BudgetAfter, err
}
