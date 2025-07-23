// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metajournal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"time"

	"github.com/mailru/easyjson"
	"go4.org/mem"

	"github.com/VKCOM/statshouse/internal/data_model"

	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/VKCOM/statshouse/internal/format"
	"github.com/VKCOM/statshouse/internal/pcache"
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
			return errors.Join(errorInvalidUserRequest, fmt.Errorf(data_model.ErrEntityInvalidVersion.Description))
		case data_model.ErrEntityExists.Code:
			return errors.Join(errorInvalidUserRequest, fmt.Errorf(data_model.ErrEntityExists.Description))
		case data_model.ErrEntityNotExists.Code:
			return errors.Join(errorInvalidUserRequest, fmt.Errorf(data_model.ErrEntityNotExists.Description))
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
	metricBytes, err := json.Marshal(value.JSONData)
	if err != nil {
		return format.DashboardMeta{}, fmt.Errorf("faield to serialize dashboard: %w", err)
	}
	editMetricReq := tlmetadata.EditEntitynew{
		Event: tlmetadata.Event{
			Id:        int64(value.DashboardID),
			Name:      value.Name,
			EventType: format.DashboardEvent,
			Version:   value.Version,
			Data:      string(metricBytes),
		},
	}
	editMetricReq.SetCreate(create)
	editMetricReq.SetDelete(remove)
	editMetricReq.Event.SetMetadata(metadata)
	ctx, cancelFunc := context.WithTimeout(ctx, l.loadTimeout)
	defer cancelFunc()
	event := tlmetadata.Event{}
	err = l.client.EditEntitynew(ctx, editMetricReq, nil, &event)
	if err != nil {
		return format.DashboardMeta{}, wrapSaveEntityError(err)
	}
	if event.Id < math.MinInt32 || event.Id > math.MaxInt32 {
		return format.DashboardMeta{}, fmt.Errorf("dashboard ID %d assigned by metaengine does not fit into int32 for dashboard %q", event.Id, event.Name)
	}
	m := map[string]interface{}{}
	err = json.Unmarshal([]byte(event.Data), &m)
	if err != nil {
		return format.DashboardMeta{}, fmt.Errorf("failed to deserialize json metric: %w", err)
	}
	return format.DashboardMeta{
		DashboardID: int32(event.Id),
		Name:        event.Name,
		Version:     event.Version,
		UpdateTime:  event.UpdateTime,
		DeleteTime:  event.Unused,
		JSONData:    m,
	}, nil
}

func (l *MetricMetaLoader) SaveMetricsGroup(ctx context.Context, value format.MetricsGroup, create bool, metadata string) (g format.MetricsGroup, _ error) {
	if err := value.RestoreCachedInfo(false); err != nil {
		return g, err
	}
	var err error
	if !format.ValidGroupName(value.Name) {
		return g, fmt.Errorf("invalid group name %w: %q", errorInvalidUserRequest, value.Name)
	}

	groupBytes, err := easyjson.Marshal(value)
	if err != nil {
		return format.MetricsGroup{}, fmt.Errorf("faield to serialize group: %w", err)
	}
	editMetricReq := tlmetadata.EditEntitynew{
		Event: tlmetadata.Event{
			Id:        int64(value.ID),
			Name:      value.Name,
			EventType: format.MetricsGroupEvent,
			Version:   value.Version,
			Data:      string(groupBytes),
		},
	}
	// todo add namespace after meta release
	editMetricReq.SetCreate(create)
	editMetricReq.Event.SetMetadata(metadata)
	ctx, cancelFunc := context.WithTimeout(ctx, l.loadTimeout)
	defer cancelFunc()
	event := tlmetadata.Event{}
	err = l.client.EditEntitynew(ctx, editMetricReq, nil, &event)
	if err != nil {
		return format.MetricsGroup{}, wrapSaveEntityError(err)
	}
	if event.Id < math.MinInt32 || event.Id > math.MaxInt32 {
		return g, fmt.Errorf("group ID %d assigned by metaengine does not fit into int32 for group %q", event.Id, event.Name)
	}
	err = easyjson.Unmarshal([]byte(event.Data), &g)
	if err != nil {
		return format.MetricsGroup{}, fmt.Errorf("failed to deserialize json group: %w", err)
	}
	g.Version = event.Version
	g.Name = event.Name
	g.UpdateTime = event.UpdateTime
	g.ID = int32(event.Id)
	return g, nil
}

func (l *MetricMetaLoader) SaveNamespace(ctx context.Context, value format.NamespaceMeta, create bool, metadata string) (g format.NamespaceMeta, _ error) {
	if err := value.RestoreCachedInfo(false); err != nil {
		return g, err
	}
	var err error
	if !format.ValidMetricName(mem.S(value.Name)) {
		return g, fmt.Errorf("invalid namespace name %w: %q", errorInvalidUserRequest, value.Name)
	}

	namespaceBytes, err := easyjson.Marshal(value)
	if err != nil {
		return format.NamespaceMeta{}, fmt.Errorf("faield to serialize namespace: %w", err)
	}
	editMetricReq := tlmetadata.EditEntitynew{
		Event: tlmetadata.Event{
			Id:        int64(value.ID),
			Name:      value.Name,
			EventType: format.NamespaceEvent,
			Version:   value.Version,
			Data:      string(namespaceBytes),
		},
	}
	// todo add namespace after meta release
	editMetricReq.SetCreate(create)
	editMetricReq.Event.SetMetadata(metadata)
	ctx, cancelFunc := context.WithTimeout(ctx, l.loadTimeout)
	defer cancelFunc()
	event := tlmetadata.Event{}
	err = l.client.EditEntitynew(ctx, editMetricReq, nil, &event)
	if err != nil {
		return format.NamespaceMeta{}, wrapSaveEntityError(err)
	}
	if event.Id < math.MinInt32 || event.Id > math.MaxInt32 {
		return g, fmt.Errorf("namespace ID %d assigned by metaengine does not fit into int32 for group %q", event.Id, event.Name)
	}
	err = easyjson.Unmarshal([]byte(event.Data), &g)
	if err != nil {
		return format.NamespaceMeta{}, fmt.Errorf("failed to deserialize json namespace: %w", err)
	}
	g.Version = event.Version
	g.Name = event.Name
	g.UpdateTime = event.UpdateTime
	g.ID = int32(event.Id)
	return g, nil
}

func (l *MetricMetaLoader) GetMetric(ctx context.Context, id int64, version int64) (ret format.MetricMetaValue, err error) {
	entity, err := l.GetEntity(ctx, id, version)
	if err != nil {
		return ret, err
	}
	m := format.MetricMetaValue{}
	err = easyjson.Unmarshal([]byte(entity.Data), &m)
	if err != nil {
		return ret, err
	}
	m.NamespaceID = int32(entity.NamespaceId)
	m.MetricID = int32(entity.Id)
	m.Name = entity.Name
	m.Version = entity.Version
	m.UpdateTime = entity.UpdateTime
	_ = m.RestoreCachedInfo()
	return m, nil
}

func (l *MetricMetaLoader) GetDashboard(ctx context.Context, id int64, version int64) (ret format.DashboardMeta, err error) {
	entity, err := l.GetEntity(ctx, id, version)
	if err != nil {
		return ret, err
	}
	d := format.DashboardMeta{}
	m := map[string]interface{}{}
	err = json.Unmarshal([]byte(entity.Data), &m)
	if err != nil {
		return ret, err
	}
	d.DashboardID = int32(entity.Id)
	d.Name = entity.Name
	d.Version = entity.Version
	d.UpdateTime = entity.UpdateTime
	d.JSONData = m
	return d, nil
}

func (l *MetricMetaLoader) GetEntity(ctx context.Context, id int64, version int64) (ret tlmetadata.Event, err error) {
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

func (l *MetricMetaLoader) SaveMetric(ctx context.Context, value format.MetricMetaValue, metadata string) (m format.MetricMetaValue, _ error) {
	create := value.MetricID == 0

	metricBytes, err := easyjson.Marshal(value)
	if err != nil {
		return m, fmt.Errorf("failed to serialize metric: %w", err)
	}
	editMetricReq := tlmetadata.EditEntitynew{
		Event: tlmetadata.Event{
			Id:        int64(value.MetricID),
			Name:      value.Name,
			EventType: format.MetricEvent,
			Version:   value.Version,
			Data:      string(metricBytes),
		},
	}
	// todo add namespace after meta release
	editMetricReq.SetCreate(create)
	editMetricReq.Event.SetMetadata(metadata)
	ctx, cancelFunc := context.WithTimeout(ctx, l.loadTimeout)
	defer cancelFunc()
	event := tlmetadata.Event{}
	err = l.client.EditEntitynew(ctx, editMetricReq, nil, &event)
	if err != nil {
		return m, wrapSaveEntityError(err)
	}
	mm, err := MetricMetaFromEvent(event)
	if err != nil {
		return m, fmt.Errorf("failed to deserialize json metric: %w", err)
	}
	m = *mm
	return m, nil
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

// adapter for disk cache
func (l *MetricMetaLoader) LoadOrCreateMapping(ctxParent context.Context, key string, metricName interface{}) (pcache.Value, time.Duration, error) {
	v, _, d, e := l.GetTagMapping(ctxParent, key, metricName.(string), true)
	return pcache.Int32ToValue(v), d, e
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

func (l *MetricMetaLoader) SaveBuiltInGroup(ctx context.Context, value format.MetricsGroup) (g format.MetricsGroup, _ error) {
	if err := value.RestoreCachedInfo(true); err != nil {
		return g, err
	}
	groupBytes, err := easyjson.Marshal(value)
	if err != nil {
		return g, fmt.Errorf("faield to serialize group: %w", err)
	}
	builtinGroup, ok := format.BuiltInGroupDefault[value.ID]
	if !ok {
		return g, fmt.Errorf("invalid buildin group id: %d", value.ID)
	}
	editMetricReq := tlmetadata.EditEntitynew{
		Event: tlmetadata.Event{
			Id:        int64(value.ID),
			Name:      builtinGroup.Name,
			EventType: format.MetricsGroupEvent,
			Version:   value.Version,
			Data:      string(groupBytes),
		},
	}
	ctx, cancelFunc := context.WithTimeout(ctx, l.loadTimeout)
	defer cancelFunc()
	event := tlmetadata.Event{}
	err = l.client.EditEntitynew(ctx, editMetricReq, nil, &event)
	if err != nil {
		return g, fmt.Errorf("failed to edit group: %w", err)
	}
	err = easyjson.Unmarshal([]byte(event.Data), &g)
	if err != nil {
		return g, fmt.Errorf("failed to deserialize json group: %w", err)
	}
	g.Version = event.Version
	g.Name = event.Name
	g.UpdateTime = event.UpdateTime
	g.ID = int32(event.Id)
	return g, nil
}

func (l *MetricMetaLoader) SaveBuiltinNamespace(ctx context.Context, value format.NamespaceMeta, create bool) (g format.NamespaceMeta, _ error) {
	if err := value.RestoreCachedInfo(true); err != nil {
		return g, err
	}

	builtinNamespace, ok := format.BuiltInNamespaceDefault[value.ID]
	if !ok {
		return g, fmt.Errorf("invalid buildin namespace id: %d", value.ID)
	}
	namespaceBytes, err := easyjson.Marshal(value)
	if err != nil {
		return format.NamespaceMeta{}, fmt.Errorf("faield to serialize namespace: %w", err)
	}
	editMetricReq := tlmetadata.EditEntitynew{
		Event: tlmetadata.Event{
			Id:        int64(value.ID),
			Name:      builtinNamespace.Name,
			EventType: format.NamespaceEvent,
			Version:   value.Version,
			Data:      string(namespaceBytes),
		},
	}
	editMetricReq.SetCreate(create)
	ctx, cancelFunc := context.WithTimeout(ctx, l.loadTimeout)
	defer cancelFunc()
	event := tlmetadata.Event{}
	err = l.client.EditEntitynew(ctx, editMetricReq, nil, &event)
	if err != nil {
		return format.NamespaceMeta{}, fmt.Errorf("failed to edit namespace: %w", err)
	}
	if event.Id < math.MinInt32 || event.Id > math.MaxInt32 {
		return g, fmt.Errorf("namespace ID %d assigned by metaengine does not fit into int32 for group %q", event.Id, event.Name)
	}
	err = easyjson.Unmarshal([]byte(event.Data), &g)
	if err != nil {
		return format.NamespaceMeta{}, fmt.Errorf("failed to deserialize json namespace: %w", err)
	}
	g.Version = event.Version
	g.Name = event.Name
	g.UpdateTime = event.UpdateTime
	g.ID = int32(event.Id)
	return g, nil
}

func (l *MetricMetaLoader) ResetFlood(ctx context.Context, metricName string, value int32) (_ bool, before int32, after int32, _ error) {
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
		return false, resp.BudgetBefore, resp.BudgetAfter, err
	}
	return true, resp.BudgetBefore, resp.BudgetAfter, err
}
