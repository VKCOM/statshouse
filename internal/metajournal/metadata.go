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
	"github.com/VKCOM/statshouse/internal/vkgo/rpc"
)

const (
	pmcBigNegativeCacheTTL = 1 * time.Hour
	DefaultMetaTimeout     = 2 * time.Second
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
	var rpcErr = &rpc.Error{}
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

func (l *MetricMetaLoader) SaveDashboard(ctx context.Context, value format.DashboardMeta, create, remove bool, metadata string) (format.DashboardMeta, error) {
	if !format.ValidDashboardName(value.Name) {
		return format.DashboardMeta{}, fmt.Errorf("invalid dashboard name %w: %q", errorInvalidUserRequest, value.Name)
	}
	event, err := EventFromDashboardMeta(value, metadata)
	if err != nil {
		return format.DashboardMeta{}, err
	}
	editMetricReq := tlmetadata.EditEntitynew{Event: event}
	editMetricReq.SetCreate(create)
	editMetricReq.SetDelete(remove)
	ctx, cancelFunc := context.WithTimeout(ctx, l.loadTimeout)
	defer cancelFunc()
	event = tlmetadata.Event{}
	err = l.client.EditEntitynew(ctx, editMetricReq, nil, &event)
	if err != nil {
		return format.DashboardMeta{}, wrapSaveEntityError(err)
	}
	d, err := DashboardMetaFromEvent(event)
	if err != nil {
		return format.DashboardMeta{}, err
	}
	return *d, nil
}

func (l *MetricMetaLoader) SaveMetricsGroup(ctx context.Context, value format.MetricsGroup, create bool, metadata string) (format.MetricsGroup, error) {
	builtin := value.ID < 0
	if err := value.RestoreCachedInfo(builtin); err != nil {
		return format.MetricsGroup{}, err
	}
	if builtin {
		builtinGroup, ok := format.BuiltInGroupDefault[value.ID]
		if !ok {
			return format.MetricsGroup{}, fmt.Errorf("invalid buildin group id: %d", value.ID)
		}
		value.Name = builtinGroup.Name // disallow changing name, but if we change built-in name, set it in DB
	} else {
		if !format.ValidGroupName(value.Name) {
			return format.MetricsGroup{}, fmt.Errorf("invalid group name %w: %q", errorInvalidUserRequest, value.Name)
		}
	}
	event, err := EventFromGroupMeta(value, metadata)
	if err != nil {
		return format.MetricsGroup{}, err
	}
	editMetricReq := tlmetadata.EditEntitynew{Event: event}
	editMetricReq.SetCreate(create)
	ctx, cancelFunc := context.WithTimeout(ctx, l.loadTimeout)
	defer cancelFunc()
	event = tlmetadata.Event{}
	err = l.client.EditEntitynew(ctx, editMetricReq, nil, &event)
	if err != nil {
		return format.MetricsGroup{}, wrapSaveEntityError(err)
	}
	g, err := GroupMetaFromEvent(event)
	if err != nil {
		return format.MetricsGroup{}, err
	}
	return *g, nil
}

func (l *MetricMetaLoader) SaveNamespace(ctx context.Context, value format.NamespaceMeta, create bool, metadata string) (format.NamespaceMeta, error) {
	builtin := value.ID < 0
	if err := value.RestoreCachedInfo(builtin); err != nil {
		return format.NamespaceMeta{}, err
	}
	if builtin {
		builtinNamespace, ok := format.BuiltInNamespaceDefault[value.ID]
		if !ok {
			return format.NamespaceMeta{}, fmt.Errorf("invalid builtin namespace id: %d", value.ID)
		}
		value.Name = builtinNamespace.Name // disallow changing name, but if we change built-in name, set it in DB
		create = value.Version == 0        // redundant, but meta engine logic requires it
	} else {
		if !format.ValidMetricName(mem.S(value.Name)) {
			return format.NamespaceMeta{}, fmt.Errorf("invalid namespace name %w: %q", errorInvalidUserRequest, value.Name)
		}
	}
	event, err := EventFromNamespaceMeta(value, metadata)
	if err != nil {
		return format.NamespaceMeta{}, err
	}
	editMetricReq := tlmetadata.EditEntitynew{Event: event}
	// todo add namespace after meta release
	editMetricReq.SetCreate(create)
	ctx, cancelFunc := context.WithTimeout(ctx, l.loadTimeout)
	defer cancelFunc()
	event = tlmetadata.Event{}
	err = l.client.EditEntitynew(ctx, editMetricReq, nil, &event)
	if err != nil {
		return format.NamespaceMeta{}, wrapSaveEntityError(err)
	}
	n, err := NamespaceMetaFromEvent(event)
	if err != nil {
		return format.NamespaceMeta{}, err
	}
	return *n, nil
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

func (l *MetricMetaLoader) SaveMetric(ctx context.Context, value format.MetricMetaValue, metadata string) (format.MetricMetaValue, error) {
	create := value.MetricID == 0

	event, err := EventFromMetricMeta(value, metadata)
	if err != nil {
		return format.MetricMetaValue{}, err
	}
	editMetricReq := tlmetadata.EditEntitynew{Event: event}
	editMetricReq.SetCreate(create)
	ctx, cancelFunc := context.WithTimeout(ctx, l.loadTimeout)
	defer cancelFunc()
	event = tlmetadata.Event{}
	err = l.client.EditEntitynew(ctx, editMetricReq, nil, &event)
	if err != nil {
		return format.MetricMetaValue{}, wrapSaveEntityError(err)
	}
	mm, err := MetricMetaFromEvent(event)
	if err != nil {
		return format.MetricMetaValue{}, fmt.Errorf("failed to deserialize json metric: %w", err)
	}
	return *mm, nil
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

func (l *MetricMetaLoader) GetTagMapping(ctx context.Context, tag string, metricName string, create bool) (int32, int32, time.Duration, error) {
	if tag == "" {
		return 0, format.TagValueIDAggMappingCreatedStatusErrorInvariant, pmcBigNegativeCacheTTL, errEmptyStringMapping
	}
	if !format.ValidStringValue(mem.S(tag)) {
		return 0, format.TagValueIDAggMappingCreatedStatusErrorInvalidValue, pmcBigNegativeCacheTTL, errInvalidKeyValue
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
		return 0, format.TagValueIDAggMappingCreatedStatusErrorPMC, 0, err
	}
	if resp.IsKeyNotExists() {
		return 0, format.TagValueIDAggMappingCreatedStatusErrorNotAskedToCreate, 0, fmt.Errorf("not asked to create mapping for non-existent key %q", tag)
	}
	if resp.IsFloodLimitError() {
		return format.TagValueIDMappingFlood, format.TagValueIDAggMappingCreatedStatusFlood, -1, nil // use TTL of -1 to avoid caching the "mapping"
	}
	if r, ok := resp.AsGetMappingResponse(); ok {
		if r.Id == 0 {
			return 0, format.TagValueIDAggMappingCreatedStatusErrorInvariant, pmcBigNegativeCacheTTL, fmt.Errorf("metdata returned %q -> 0 mapping, which is invalid", tag)
		}
		return r.Id, format.TagValueIDAggMappingCreatedStatusOK, 0, nil
	}
	if r, ok := resp.AsCreated(); ok {
		if r.Id == 0 {
			return 0, format.TagValueIDAggMappingCreatedStatusErrorInvariant, pmcBigNegativeCacheTTL, fmt.Errorf("metdata created %q -> 0 mapping, which is invalid", tag)
		}
		return r.Id, format.TagValueIDAggMappingCreatedStatusCreated, 0, nil
	}
	return 0, format.TagValueIDAggMappingCreatedStatusErrorPMC, 0, err
}

func (l *MetricMetaLoader) SaveScrapeConfig(ctx context.Context, version int64, config string, metadata string) (tlmetadata.Event, error) {
	editMetricReq := tlmetadata.EditEntitynew{
		Event: tlmetadata.Event{
			Id:        format.PrometheusConfigID,
			Name:      "prom-config",
			EventType: format.PromConfigEvent,
			Version:   version,
			Data:      config,
		},
	}
	editMetricReq.Event.SetMetadata(metadata)
	ctx, cancelFunc := context.WithTimeout(ctx, l.loadTimeout)
	defer cancelFunc()
	var event tlmetadata.Event
	err := l.client.EditEntitynew(ctx, editMetricReq, nil, &event)
	if err != nil {
		return event, fmt.Errorf("failed to change prom config: %w", err)
	}
	return event, nil
}

func (l *MetricMetaLoader) SaveScrapeStaticConfig(ctx context.Context, version int64, config string) (tlmetadata.Event, error) {
	editMetricReq := tlmetadata.EditEntitynew{
		Event: tlmetadata.Event{
			Id:        format.PrometheusGeneratedConfigID,
			Name:      "prom-static-config",
			EventType: format.PromConfigEvent,
			Version:   version,
			Data:      config,
		},
	}
	ctx, cancelFunc := context.WithTimeout(ctx, l.loadTimeout)
	defer cancelFunc()
	var event tlmetadata.Event
	err := l.client.EditEntitynew(ctx, editMetricReq, nil, &event)
	if err != nil {
		return event, fmt.Errorf("failed to change prom static config: %w", err)
	}
	return event, nil
}

func (l *MetricMetaLoader) SaveKnownTagsConfig(ctx context.Context, version int64, config string) (tlmetadata.Event, error) {
	editMetricReq := tlmetadata.EditEntitynew{
		Event: tlmetadata.Event{
			Id:        format.KnownTagsConfigID,
			Name:      "prom-known-tags",
			EventType: format.PromConfigEvent,
			Version:   version,
			Data:      config,
		},
	}
	ctx, cancelFunc := context.WithTimeout(ctx, l.loadTimeout)
	defer cancelFunc()
	var event tlmetadata.Event
	err := l.client.EditEntitynew(ctx, editMetricReq, nil, &event)
	if err != nil {
		return event, fmt.Errorf("failed to change prom known tags config: %w", err)
	}
	return event, nil
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
	// TODO - return budget before and after in a message to UI
	if err != nil {
		return resp.BudgetBefore, resp.BudgetAfter, err
	}
	return resp.BudgetBefore, resp.BudgetAfter, err
}
