// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metajournal

import (
	"encoding/json"
	"log"
	"sync"

	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse_metadata"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/pcache"
)

type GroupWithMetricsList struct {
	Group   *format.MetricsGroup
	Metrics []string
}

type ApplyPromConfig func(configString string, version int64)

type MetricsStorage struct {
	mu sync.RWMutex

	metricsByID   map[int32]*format.MetricMetaValue
	metricsByName map[string]*format.MetricMetaValue // only because IDs are not unique and can lead to infinite update cycle

	dashboardByID map[int32]*format.DashboardMeta

	groupsByID     map[int32]*format.MetricsGroup
	metricsByGroup map[int32]map[int32]*format.MetricMetaValue

	promConfig tlstatshouse_metadata.Event

	applyPromConfig ApplyPromConfig

	journal *Journal // can be easily moved out, if desired
}

func MakeMetricsStorage(namespaceSuffix string, dc *pcache.DiskCache, applyPromConfig ApplyPromConfig) *MetricsStorage {
	result := &MetricsStorage{
		metricsByID:     map[int32]*format.MetricMetaValue{},
		metricsByName:   map[string]*format.MetricMetaValue{},
		dashboardByID:   map[int32]*format.DashboardMeta{},
		groupsByID:      map[int32]*format.MetricsGroup{},
		metricsByGroup:  map[int32]map[int32]*format.MetricMetaValue{},
		applyPromConfig: applyPromConfig,
	}
	result.journal = MakeJournal(namespaceSuffix, dc, result.ApplyEvent)
	return result
}

// satisfy format.MetaStorageInterface
func (ms *MetricsStorage) Version() int64    { return ms.journal.Version() }
func (ms *MetricsStorage) StateHash() string { return ms.journal.StateHash() }

func (ms *MetricsStorage) Journal() *Journal { return ms.journal }

func (ms *MetricsStorage) PromConfig() tlstatshouse_metadata.Event {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.promConfig
}

func (ms *MetricsStorage) GetMetaMetric(metricID int32) *format.MetricMetaValue {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.metricsByID[metricID]
}

func (ms *MetricsStorage) GetMetaMetricByName(metricName string) *format.MetricMetaValue {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.metricsByName[metricName]
}

func (ms *MetricsStorage) GetMetaMetricByNameBytes(metric []byte) *format.MetricMetaValue {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.metricsByName[string(metric)]
}

func (ms *MetricsStorage) GetMetaMetricList(includeInvisible bool) []*format.MetricMetaValue {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	li := make([]*format.MetricMetaValue, 0, len(ms.metricsByName))
	for _, v := range ms.metricsByName {
		if !includeInvisible && !v.Visible {
			continue
		}
		li = append(li, v)
	}
	return li
}

func (ms *MetricsStorage) GetDashboardMeta(dashboardID int32) *format.DashboardMeta {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.dashboardByID[dashboardID]
}

func (ms *MetricsStorage) GetDashboardList() []*format.DashboardMeta {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	li := make([]*format.DashboardMeta, 0, len(ms.dashboardByID))
	for _, v := range ms.dashboardByID {
		if v.DeleteTime > 0 {
			continue
		}
		li = append(li, v)
	}
	return li
}

func (ms *MetricsStorage) GetGroup(id int32) *format.MetricsGroup {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.groupsByID[id]
}

func (ms *MetricsStorage) GetGroupWithMetricsList(id int32) (GroupWithMetricsList, bool) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	var metricNames []string
	if group, ok := ms.groupsByID[id]; ok {
		if metrics, ok := ms.metricsByGroup[group.ID]; ok {
			for _, metric := range metrics {
				metricNames = append(metricNames, metric.Name)
			}
		}
		return GroupWithMetricsList{Group: group, Metrics: metricNames}, true
	}
	return GroupWithMetricsList{}, false
}

func (ms *MetricsStorage) GetGroupsList() []*format.MetricsGroup {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	var groups []*format.MetricsGroup
	for _, v := range ms.groupsByID {
		if v.DeleteTime > 0 {
			continue
		}
		groups = append(groups, v)
	}
	return groups
}

func (ms *MetricsStorage) ApplyEvent(newEntries []tlstatshouse_metadata.Event) {
	promConfigSet := false
	promConfigData := ""
	promConfigVersion := int64(0)
	ms.mu.Lock()
	for _, e := range newEntries {
		switch e.EventType {
		case format.MetricEvent:
			value := &format.MetricMetaValue{}
			err := json.Unmarshal([]byte(e.Data), value)
			if err != nil {
				log.Printf("Cannot marshal metric %s: %v", value.Name, err)
				continue
			}
			value.Version = e.Version
			value.Name = e.Name
			value.MetricID = int32(e.Id) // TODO - beware!
			value.UpdateTime = e.UpdateTime
			_ = value.RestoreCachedInfo()
			valueOld, ok := ms.metricsByID[value.MetricID]
			if ok && valueOld.Name != value.Name {
				delete(ms.metricsByName, valueOld.Name)
			}
			if ok && valueOld.GroupID != value.GroupID && valueOld.GroupID > 0 {
				ms.removeMetricFromGroup(valueOld.GroupID, value.MetricID)
			}
			if value.GroupID > 0 {
				ms.addMetricToGroup(value.GroupID, value)
			}
			ms.metricsByName[value.Name] = value
			ms.metricsByID[value.MetricID] = value

		case format.DashboardEvent:
			m := map[string]interface{}{}
			err := json.Unmarshal([]byte(e.Data), &m)
			if err != nil {
				log.Printf("Cannot marshal metric %s: %v", e.Name, err)
			}
			dash := &format.DashboardMeta{
				DashboardID: int32(e.Id), // TODO - beware!
				Name:        e.Name,
				Version:     e.Version,
				UpdateTime:  e.UpdateTime,
				JSONData:    m,
				DeleteTime:  e.Unused,
			}
			ms.dashboardByID[dash.DashboardID] = dash
		case format.MetricsGroupEvent:
			value := &format.MetricsGroup{}
			err := json.Unmarshal([]byte(e.Data), value)
			if err != nil {
				log.Printf("Cannot marshal metric %s: %v", value.Name, err)
				continue
			}
			value.Version = e.Version
			value.Name = e.Name
			value.ID = int32(e.Id) // TODO - beware!
			value.UpdateTime = e.UpdateTime
			value.DeleteTime = e.Unused
			_ = value.RestoreCachedInfo()
			ms.groupsByID[value.ID] = value
		case format.PromConfigEvent:
			ms.promConfig = e
			promConfigSet = true
			promConfigData = e.Data
			promConfigVersion = e.Version
		}
	}
	ms.mu.Unlock()
	if promConfigSet && ms.applyPromConfig != nil { // outside of lock, once
		ms.applyPromConfig(promConfigData, promConfigVersion)
	}
}

func (ms *MetricsStorage) removeMetricFromGroup(groupID int32, metricID int32) {
	if metrics, ok := ms.metricsByGroup[groupID]; ok {
		delete(metrics, metricID)
		if len(metrics) == 0 {
			delete(ms.metricsByGroup, groupID)
		}
	}
}

func (ms *MetricsStorage) addMetricToGroup(groupID int32, metric *format.MetricMetaValue) {
	metrics, ok := ms.metricsByGroup[groupID]
	if !ok {
		metrics = map[int32]*format.MetricMetaValue{}
		ms.metricsByGroup[groupID] = metrics
	}
	metrics[metric.MetricID] = metric
}
