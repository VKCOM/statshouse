// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metajournal

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"time"

	promlog "github.com/go-kit/log"
	prom_config "github.com/prometheus/prometheus/config"
	"go4.org/mem"

	"github.com/vkcom/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/pcache"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
)

const (
	pmcBigNegativeCacheTTL = 1 * time.Hour
	DefaultMetaTimeout     = 2 * time.Second
	prometheusConfigID     = -1 // TODO move to file with predefined entities
)

type MetricMetaLoader struct {
	loadTimeout time.Duration
	client      *tlmetadata.Client
}

func NewMetricMetaLoader(client *tlmetadata.Client, loadTimeout time.Duration) *MetricMetaLoader {
	return &MetricMetaLoader{
		client:      client,
		loadTimeout: loadTimeout,
	}
}

func (l *MetricMetaLoader) SaveDashboard(ctx context.Context, value format.DashboardMeta, create, remove bool) (format.DashboardMeta, error) {
	if !format.ValidDashboardName(value.Name) {
		return format.DashboardMeta{}, fmt.Errorf("invalid dashboard name: %q", value.Name)
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
	ctx, cancelFunc := context.WithTimeout(ctx, l.loadTimeout)
	defer cancelFunc()
	event := tlmetadata.Event{}
	err = l.client.EditEntitynew(ctx, editMetricReq, nil, &event)
	if err != nil {
		return format.DashboardMeta{}, fmt.Errorf("failed to edit metric: %w", err)
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

func (l *MetricMetaLoader) SaveMetricsGroup(ctx context.Context, value format.MetricsGroup, create, delete bool) (g format.MetricsGroup, _ error) {
	if err := value.RestoreCachedInfo(); err != nil {
		return g, err
	}
	groupBytes, err := json.Marshal(value)
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
	editMetricReq.SetCreate(create)
	editMetricReq.SetDelete(delete)
	ctx, cancelFunc := context.WithTimeout(ctx, l.loadTimeout)
	defer cancelFunc()
	event := tlmetadata.Event{}
	err = l.client.EditEntitynew(ctx, editMetricReq, nil, &event)
	if err != nil {
		return format.MetricsGroup{}, fmt.Errorf("failed to edit group: %w", err)
	}
	if event.Id < math.MinInt32 || event.Id > math.MaxInt32 {
		return g, fmt.Errorf("group ID %d assigned by metaengine does not fit into int32 for group %q", event.Id, event.Name)
	}
	err = json.Unmarshal([]byte(event.Data), &g)
	if err != nil {
		return format.MetricsGroup{}, fmt.Errorf("failed to deserialize json group: %w", err)
	}
	g.Version = event.Version
	g.Name = event.Name
	g.UpdateTime = event.UpdateTime
	g.ID = int32(event.Id)
	g.DeleteTime = event.Unused
	return g, nil
}

func (l *MetricMetaLoader) SaveMetric(ctx context.Context, value format.MetricMetaValue) (m format.MetricMetaValue, _ error) {
	create := value.MetricID == 0

	if err := value.RestoreCachedInfo(); err != nil {
		return m, err
	}
	metricBytes, err := json.Marshal(value)
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
	editMetricReq.SetCreate(create)
	ctx, cancelFunc := context.WithTimeout(ctx, l.loadTimeout)
	defer cancelFunc()
	event := tlmetadata.Event{}
	err = l.client.EditEntitynew(ctx, editMetricReq, nil, &event)
	if err != nil {
		return m, fmt.Errorf("failed to edit metric: %w", err)
	}
	if event.Id < math.MinInt32 || event.Id > math.MaxInt32 {
		return m, fmt.Errorf("metric ID %d assigned by metaengine does not fit into int32 for metric %q", event.Id, event.Name)
	}
	err = json.Unmarshal([]byte(event.Data), &m)
	if err != nil {
		return m, fmt.Errorf("failed to deserialize json metric: %w", err)
	}
	m.Version = event.Version
	m.Name = event.Name
	m.UpdateTime = event.UpdateTime
	m.MetricID = int32(event.Id)
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
		return 0, format.TagValueIDAggMappingCreatedStatusErrorInvariant, pmcBigNegativeCacheTTL, fmt.Errorf("empty string mapping is special should never be created")
	}
	if !format.ValidStringValue(mem.S(tag)) {
		return 0, format.TagValueIDAggMappingCreatedStatusErrorInvalidValue, pmcBigNegativeCacheTTL, fmt.Errorf("key value is invalid")
	}

	ctx, cancelFunc := context.WithTimeout(ctx, l.loadTimeout)
	defer cancelFunc()

	req := tlmetadata.GetMapping{
		Metric: metricName,
		Key:    tag,
	}
	req.SetCreateIfAbsent(create)
	resp := tlmetadata.GetMappingResponseUnion{}
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

func checkPromConfig(cfg *prom_config.Config) error {
	if len(cfg.RemoteWriteConfigs) > 0 {
		return fmt.Errorf("statshouse doesn't support v remote write")
	}

	if len(cfg.AlertingConfig.AlertmanagerConfigs) > 0 || len(cfg.AlertingConfig.AlertRelabelConfigs) > 0 {
		return fmt.Errorf("statshouse doesn't support prometheus prometheus alerting")
	}

	if len(cfg.RuleFiles) > 0 {
		return fmt.Errorf("statshouse doesn't support prometheus rule_files field")
	}

	if len(cfg.RemoteReadConfigs) > 0 {
		return fmt.Errorf("statshouse doesn't support prometheus remote read")
	}

	if cfg.StorageConfig != prom_config.DefaultStorageConfig {
		return fmt.Errorf("statshouse doesn't support storage section")
	}

	return nil
}

func (l *MetricMetaLoader) SavePromConfig(ctx context.Context, version int64, config string) (tlmetadata.Event, error) {
	cfg, err := prom_config.Load(config, false, promlog.NewLogfmtLogger(os.Stdout))
	event := tlmetadata.Event{}
	if err != nil {
		return event, fmt.Errorf("invalid prometheus config syntax: %w", err)
	}
	if err := checkPromConfig(cfg); err != nil {
		return event, err
	}
	editMetricReq := tlmetadata.EditEntitynew{
		Event: tlmetadata.Event{
			Id:        prometheusConfigID,
			Name:      "prom-config",
			EventType: format.PromConfigEvent,
			Version:   version,
			Data:      config,
		},
	}
	ctx, cancelFunc := context.WithTimeout(ctx, l.loadTimeout)
	defer cancelFunc()
	err = l.client.EditEntitynew(ctx, editMetricReq, nil, &event)
	if err != nil {
		return event, fmt.Errorf("failed to change prometheus config: %w", err)
	}
	return event, nil
}

func (l *MetricMetaLoader) ResetFlood(ctx context.Context, metricName string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, l.loadTimeout)
	defer cancel()
	req := tlmetadata.ResetFlood2{
		Metric: metricName,
	}
	resp := tlmetadata.ResetFloodResponse2{}
	err := l.client.ResetFlood2(ctx, req, nil, &resp)
	// TODO - return budget before and after in a message to UI
	if err != nil {
		return false, err
	}
	return true, err
}
