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
	metricsByName map[string]*format.MetricMetaValue

	dashboardByID map[int32]*format.DashboardMeta

	groupsByID map[int32]*format.MetricsGroup
	//metricsNamesByGroup map[int32]map[int32]string

	namespaceByID   map[int32]*format.NamespaceMeta
	namespaceByName map[string]*format.NamespaceMeta

	promConfig tlmetadata.Event

	applyPromConfig ApplyPromConfig

	journal *Journal // can be easily moved out, if desired
}

func MakeMetricsStorage(namespaceSuffix string, dc *pcache.DiskCache, applyPromConfig ApplyPromConfig) *MetricsStorage {
	result := &MetricsStorage{
		metricsByID:   map[int32]*format.MetricMetaValue{},
		metricsByName: map[string]*format.MetricMetaValue{},
		dashboardByID: map[int32]*format.DashboardMeta{},
		groupsByID:    map[int32]*format.MetricsGroup{},
		//metricsNamesByGroup: map[int32]map[int32]string{},
		namespaceByID:   map[int32]*format.NamespaceMeta{},
		namespaceByName: map[string]*format.NamespaceMeta{},
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
	return ms.getMetaMetricByNameLocked(metricName)

}
func (ms *MetricsStorage) getMetaMetricByNameLocked(metricName string) *format.MetricMetaValue {
	return ms.metricsByName[metricName]
}

// TODO some fixes to avoid allocation
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

func (ms *MetricsStorage) GetGroupByMetric(m *format.MetricMetaValue) *format.MetricsGroup {
	if m.MetricID >= 0 {
		return m.Group
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return m.Group
}

func (ms *MetricsStorage) GetNamespaceByMetric(m *format.MetricMetaValue) *format.NamespaceMeta {
	if m.MetricID >= 0 {
		return m.Namespace
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return m.Namespace
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
	g, ok := ms.groupsByID[id]
	if ok {
		return g
	}
	return format.BuiltInGroup[id]
}

func (ms *MetricsStorage) GetGroupWithMetricsList(id int32) (GroupWithMetricsList, bool) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	var group *format.MetricsGroup
	var metricNames []string
	for _, metric := range ms.metricsByID {
		if metric.GroupID == id {
			metricNames = append(metricNames, metric.Name)
		}
	}
	for _, m := range format.BuiltinMetrics {
		if m.GroupID == id {
			metricNames = append(metricNames, m.Name)
		}
	}
	var ok bool
	if id >= 0 {
		if group, ok = ms.groupsByID[id]; !ok {
			return GroupWithMetricsList{}, false
		}
	} else {
		if group, ok = format.BuiltInGroup[id]; !ok {
			return GroupWithMetricsList{}, false
		}

	}
	return GroupWithMetricsList{Group: group, Metrics: metricNames}, true

}

func (ms *MetricsStorage) GetGroupsList(showInvisible bool) []*format.MetricsGroup {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	var groups []*format.MetricsGroup
	for _, v := range ms.groupsByID {
		if v.Disable && !showInvisible {
			continue
		}
		groups = append(groups, v)
	}
	for _, v := range format.BuiltInGroup {
		if v.Disable && !showInvisible {
			continue
		}
		groups = append(groups, v)
	}
	return groups
}

func (ms *MetricsStorage) GetNamespace(id int32) *format.NamespaceMeta {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.namespaceByID[id]
}

func (ms *MetricsStorage) GetNamespaceList() []*format.NamespaceMeta {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	var namespaces []*format.NamespaceMeta
	for _, namespace := range ms.namespaceByID {
		namespaces = append(namespaces, namespace)
	}
	for _, namespace := range format.BuiltInNamespace {
		namespaces = append(namespaces, namespace)
	}
	return namespaces
}

func (ms *MetricsStorage) ApplyEvent(newEntries []tlmetadata.Event) {
	// This code operates on immutable structs, it should not change any stored object, except of map
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
			value.NamespaceID = int32(e.NamespaceId)
			value.Version = e.Version
			value.Name = e.Name
			value.MetricID = int32(e.Id) // TODO - beware!
			value.UpdateTime = e.UpdateTime
			_ = value.RestoreCachedInfo()
			valueOld, ok := ms.metricsByID[value.MetricID]
			if ok && valueOld.Name != value.Name {
				delete(ms.metricsByName, valueOld.Name)
			}
			ms.updateMetric(value)
			ms.calcGroupForMetricLocked(value)
			ms.calcNamespaceForMetricLocked(value)
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
				log.Printf("Cannot marshal metric group %s: %v", value.Name, err)
				continue
			}
			value.Version = e.Version
			value.Name = e.Name
			value.ID = int32(e.Id)
			value.NamespaceID = int32(e.NamespaceId)
			value.UpdateTime = e.UpdateTime
			_ = value.RestoreCachedInfo(false)
			var old *format.MetricsGroup
			if value.ID >= 0 {
				old = ms.groupsByID[value.ID]
				ms.groupsByID[value.ID] = value
				ms.calcNamespaceForGroupLocked(value)
			} else {
				old = format.BuiltInGroup[value.ID]
				format.BuiltInGroup[value.ID] = value
			}
			ms.calcGroupForMetricsLocked(old, value)
		case format.PromConfigEvent:
			ms.promConfig = e
			promConfigSet = true
			promConfigData = e.Data
			promConfigVersion = e.Version
		case format.NamespaceEvent:
			value := &format.NamespaceMeta{}
			err := json.Unmarshal([]byte(e.Data), value)
			if err != nil {
				log.Printf("Cannot marshal metric group %s: %v", value.Name, err)
				continue
			}
			value.ID = int32(e.Id)
			value.Name = e.Name
			value.Version = e.Version
			value.UpdateTime = e.UpdateTime
			if value.ID >= 0 {
				if oldNamespace, ok := ms.namespaceByID[value.ID]; ok && oldNamespace.Name != value.Name {
					delete(ms.namespaceByName, oldNamespace.Name)
				}

				ms.namespaceByID[value.ID] = value
				ms.namespaceByName[value.Name] = value
			} else {
				format.BuiltInNamespace[value.ID] = value
			}
			ms.calcNamespaceForMetricsAndGroupsLocked(value)

		}
	}
	ms.mu.Unlock()
	if promConfigSet && ms.applyPromConfig != nil { // outside of lock, once
		ms.applyPromConfig(promConfigData, promConfigVersion)
	}
}

// call when namespace is added or changed O(number of metrics + numb of groups)
func (ms *MetricsStorage) calcNamespaceForMetricsAndGroupsLocked(new *format.NamespaceMeta) {
	for _, m := range ms.metricsByID {
		if m.NamespaceID == new.ID {
			mCopy := *m
			mCopy.Namespace = new
			ms.metricsByID[m.MetricID] = &mCopy
			ms.metricsByName[m.Name] = &mCopy
		}
	}

	for _, group := range ms.groupsByID {
		if group.NamespaceID == new.ID {
			groupCopy := *group
			groupCopy.Namespace = new
			ms.groupsByID[groupCopy.ID] = &groupCopy
		}
	}
	for _, m := range format.BuiltinMetrics {
		if m.NamespaceID == new.ID {
			m.Namespace = new
		}
	}

	for _, group := range format.BuiltInGroup {
		if group.NamespaceID == new.ID {
			group.Namespace = new
		}
	}
}

// call when metric is added or changed O(1)
func (ms *MetricsStorage) calcNamespaceForMetricLocked(new *format.MetricMetaValue) {
	if new.NamespaceID > 0 {
		if n, ok := ms.namespaceByID[new.NamespaceID]; ok {
			new.Namespace = n
		}
	} else {
		if new.NamespaceID == 0 || new.NamespaceID == format.BuiltinNamespaceIDDefault {
			new.Namespace = format.BuiltInNamespace[format.BuiltinNamespaceIDDefault]
		}
	}
}

// call when metric is added or changed O(1)
func (ms *MetricsStorage) calcNamespaceForGroupLocked(new *format.MetricsGroup) {
	if new.NamespaceID != 0 {
		if n, ok := ms.namespaceByID[new.NamespaceID]; ok {
			new.Namespace = n
		}
	} else {
		if new.NamespaceID == 0 || new.NamespaceID == format.BuiltinNamespaceIDDefault {
			new.Namespace = format.BuiltInNamespace[format.BuiltinNamespaceIDDefault]
		}
	}
}

// call when group is added or changed O(number of metrics)
func (ms *MetricsStorage) calcGroupForMetricsLocked(old, new *format.MetricsGroup) {
	if new.ID >= 0 {
		if (old != nil && old.Name != new.Name) || new.Disable {
			for _, m := range ms.metricsByID {
				if m.GroupID == new.ID {
					mCopy := *m
					mCopy.GroupID = format.BuiltinGroupIDDefault
					mCopy.Group = format.BuiltInGroup[format.BuiltinGroupIDDefault]
					ms.updateMetric(&mCopy)
				}
			}
			//metrics := ms.metricsNamesByGroup[old.ID]
			//for metricID := range metrics {
			//	if m, ok := ms.metricsByID[metricID]; ok {
			//		mCopy := *m
			//		mCopy.GroupID = 0
			//		mCopy.Group = nil
			//		ms.metricsByID[m.MetricID] = &mCopy
			//		ms.metricsByName[m.Name] = &mCopy
			//	}
			//}
			//delete(ms.metricsNamesByGroup, old.ID)
		}

		for _, m := range ms.metricsByID {
			if new.MetricIn(m) {
				mCopy := *m
				mCopy.GroupID = new.ID
				mCopy.Group = new
				ms.updateMetric(&mCopy)
			}
		}
	} else {
		for _, m := range ms.metricsByID {
			if new.ID == format.BuiltinGroupIDDefault && (m.GroupID == 0 || m.GroupID == format.BuiltinGroupIDDefault) {
				mCopy := *m
				mCopy.GroupID = new.ID
				mCopy.Group = new
				ms.updateMetric(&mCopy)
			}
		}
	}

	for _, m := range format.BuiltinMetrics {
		if m.GroupID == new.ID {
			m.Group = new
		}
	}
}

// call when metric is added or changed O(number of groups)
func (ms *MetricsStorage) calcGroupForMetricLocked(new *format.MetricMetaValue) {
	new.GroupID = format.BuiltinGroupIDDefault
	new.Group = format.BuiltInGroup[format.BuiltinGroupIDDefault]
	for _, g := range ms.groupsByID {
		if g.MetricIn(new) {
			new.GroupID = g.ID
			new.Group = g
			return
		}
	}
}

func (ms *MetricsStorage) updateMetric(metric *format.MetricMetaValue) {
	ms.metricsByID[metric.MetricID] = metric
	ms.metricsByName[metric.Name] = metric
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
