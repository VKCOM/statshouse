// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metajournal

import (
	"encoding/json"
	"log"
	"strings"
	"sync"

	"github.com/vkcom/statshouse/internal/data_model/gen2/tlmetadata"
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

	promConfig tlmetadata.Event

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

func (ms *MetricsStorage) PromConfig() tlmetadata.Event {
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

func (ms *MetricsStorage) GetGroupByMetricName(name string) *format.MetricsGroup {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	for _, group := range ms.groupsByID {
		if group.MetricIn(name) {
			return group
		}
	}
	return nil
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
		if !v.Visible {
			continue
		}
		groups = append(groups, v)
	}
	return groups
}

func (ms *MetricsStorage) ApplyEvent(newEntries []tlmetadata.Event) {
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
			ms.metricsByName[value.Name] = value
			ms.metricsByID[value.MetricID] = value
			ms.calcGroupForMetricLocked(value)
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
			_ = value.RestoreCachedInfo()
			old := ms.groupsByID[value.ID]
			ms.groupsByID[value.ID] = value
			ms.calcGroupForMetricsLocked(old, value)
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

// call when group is added or changed O(numb of metrics)
func (ms *MetricsStorage) calcGroupForMetricsLocked(old, new *format.MetricsGroup) {
	if old != nil && old.Name != new.Name {
		metrics := ms.metricsByGroup[old.ID]
		for _, m := range metrics {
			mCopy := *m
			m.GroupID = 0
			m.Group = nil
			ms.metricsByID[m.MetricID] = &mCopy
		}
		delete(ms.metricsByGroup, old.ID)
	}

	for _, m := range ms.metricsByID {
		if new.MetricIn(m.Name) {
			mCopy := *m
			mCopy.GroupID = new.ID
			ms.addMetricToGroupLocked(new.ID, &mCopy)
		}
	}
}

// call when metric is added or changed O(number of groups)
func (ms *MetricsStorage) calcGroupForMetricLocked(new *format.MetricMetaValue) {
	ms.removeMetricFromGroupLocked(new.GroupID, new.MetricID)
	new.GroupID = 0
	new.Group = nil
	for _, g := range ms.groupsByID {
		if g.MetricIn(new.Name) {
			ms.removeMetricFromGroupLocked(new.GroupID, new.MetricID)
			new.GroupID = g.ID
			new.Group = g
			ms.addMetricToGroupLocked(g.ID, new)
			return
		}
	}
}

func (ms *MetricsStorage) removeMetricFromGroupLocked(groupID int32, metricID int32) {
	if metrics, ok := ms.metricsByGroup[groupID]; ok {
		delete(metrics, metricID)
		if len(metrics) == 0 {
			delete(ms.metricsByGroup, groupID)
		}
	}
}

func (ms *MetricsStorage) addMetricToGroupLocked(groupID int32, metric *format.MetricMetaValue) {
	metrics, ok := ms.metricsByGroup[groupID]
	if !ok {
		metrics = map[int32]*format.MetricMetaValue{}
		ms.metricsByGroup[groupID] = metrics
	}
	metrics[metric.MetricID] = metric
	ms.metricsByID[metric.MetricID] = metric
}

func (ms *MetricsStorage) CanAddOrChangeGroup(name string, id int32) bool {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return ms.canAddOrChangeGroupLocked(name, id)
}

func (ms *MetricsStorage) canAddOrChangeGroupLocked(name string, id int32) bool {
	for _, m := range ms.groupsByID {
		if id != 0 && id == m.ID {
			continue
		}
		if strings.HasPrefix(name, m.Name) || strings.HasPrefix(m.Name, name) {
			return false
		}
	}
	return true
}
