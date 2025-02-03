// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metajournal

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"sync"

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/vkcom/statshouse/internal/format"
)

type GroupWithMetricsList struct {
	Group   *format.MetricsGroup
	Metrics []string
}

type ApplyPromConfig func(configID int32, configString string)

type SnapshotMeta struct {
	MetricsByIDSnapshot   map[int32]*format.MetricMetaValue
	MetricsByNameSnapshot map[string]*format.MetricMetaValue
}

type MetricsStorage struct {
	mu            sync.RWMutex
	metricsByID   map[int32]*format.MetricMetaValue
	metricsByName map[string]*format.MetricMetaValue

	dashboardByID map[int32]*format.DashboardMeta

	builtInGroup map[int32]*format.MetricsGroup
	groupsByID   map[int32]*format.MetricsGroup
	groupsByName map[string]*format.MetricsGroup

	builtInNamespace map[int32]*format.NamespaceMeta
	namespaceByID    map[int32]*format.NamespaceMeta
	namespaceByName  map[string]*format.NamespaceMeta

	promConfig          tlmetadata.Event
	promConfigGenerated tlmetadata.Event
	knownTags           tlmetadata.Event

	applyPromConfig ApplyPromConfig

	versionClientMu sync.Mutex
	lastVersion     int64
	versionClients  []versionClient
}

func MakeMetricsStorage(applyPromConfig ApplyPromConfig) *MetricsStorage {
	result := &MetricsStorage{
		metricsByID:      map[int32]*format.MetricMetaValue{},
		metricsByName:    map[string]*format.MetricMetaValue{},
		dashboardByID:    map[int32]*format.DashboardMeta{},
		builtInGroup:     map[int32]*format.MetricsGroup{},
		groupsByID:       map[int32]*format.MetricsGroup{},
		groupsByName:     map[string]*format.MetricsGroup{},
		builtInNamespace: map[int32]*format.NamespaceMeta{},
		namespaceByID:    map[int32]*format.NamespaceMeta{},
		namespaceByName:  map[string]*format.NamespaceMeta{},
		applyPromConfig:  applyPromConfig,
	}
	for id, g := range format.BuiltInGroupDefault {
		result.builtInGroup[id] = g
	}
	for id, g := range format.BuiltInNamespaceDefault {
		result.builtInNamespace[id] = g
	}
	return result
}

func (snapshot SnapshotMeta) GetMetaMetric(metricID int32) *format.MetricMetaValue {
	return snapshot.MetricsByIDSnapshot[metricID]
}

func (snapshot SnapshotMeta) GetMetaMetricByName(metricName string) *format.MetricMetaValue {
	return snapshot.MetricsByNameSnapshot[metricName]
}

func (snapshot SnapshotMeta) GetMetaMetricByNameBytes(metric []byte) *format.MetricMetaValue {
	return snapshot.MetricsByNameSnapshot[string(metric)]
}

func (ms *MetricsStorage) PromConfig() tlmetadata.Event {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.promConfig
}

func (ms *MetricsStorage) PromConfigGenerated() tlmetadata.Event {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.promConfigGenerated
}

func (ms *MetricsStorage) KnownTags() tlmetadata.Event {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.knownTags
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

func (ms *MetricsStorage) GetNamespaceByName(name string) *format.NamespaceMeta {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	ns, ok := ms.namespaceByName[name]
	if ok {
		return ns
	}
	for _, ns := range ms.builtInNamespace {
		if ns.Name == name {
			return ns
		}
	}
	return nil
}

func (ms *MetricsStorage) GetGroupByName(name string) *format.MetricsGroup {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	g, ok := ms.groupsByName[name]
	if ok {
		return g
	}
	for _, g := range ms.builtInGroup {
		if g.Name == name {
			return g
		}
	}
	return nil
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

func (ms *MetricsStorage) MatchMetrics(f *data_model.QueryFilter) {
	if f.Namespace == "" || f.Namespace == "__default" {
		f.MatchMetrics(format.BuiltinMetricByName)
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	f.MatchMetrics(ms.metricsByName)
}

func (ms *MetricsStorage) GetDashboardMeta(dashboardID int32) *format.DashboardMeta {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.dashboardByID[dashboardID]
}

func (ms *MetricsStorage) GetDashboardList(showInvisible bool) []*format.DashboardMeta {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	li := make([]*format.DashboardMeta, 0, len(ms.dashboardByID))
	for _, v := range ms.dashboardByID {
		if v.DeleteTime > 0 && !showInvisible {
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
	return ms.builtInGroup[id]
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
		if group, ok = ms.builtInGroup[id]; !ok {
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
	for _, v := range ms.builtInGroup {
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
	m, ok := ms.namespaceByID[id]
	if ok {
		return m
	}
	return ms.builtInNamespace[id]
}

func (ms *MetricsStorage) GetNamespaceList() []*format.NamespaceMeta {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	var namespaces []*format.NamespaceMeta
	for _, namespace := range ms.namespaceByID {
		namespaces = append(namespaces, namespace)
	}
	for _, namespace := range ms.builtInNamespace {
		namespaces = append(namespaces, namespace)
	}
	return namespaces
}

func (ms *MetricsStorage) ApplyEvent(newEntries []tlmetadata.Event) {
	// This code operates on immutable structs, it should not change any stored object, except of map
	promConfigSet := false
	promConfigData := ""
	promConfigGeneratedSet := false
	promConfigGeneratedData := ""
	knownTagsSet := false
	knownTagsData := ""

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
			ms.mu.Lock()
			valueOld, ok := ms.metricsByID[value.MetricID]
			if ok && valueOld.Name != value.Name {
				delete(ms.metricsByName, valueOld.Name)
			}
			ms.updateMetric(value)
			ms.calcGroupForMetricLocked(value)
			ms.mu.Unlock()
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

			ms.mu.Lock()
			ms.dashboardByID[dash.DashboardID] = dash
			ms.mu.Unlock()
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
			_ = value.RestoreCachedInfo(value.ID < 0)
			ms.mu.Lock()
			var old *format.MetricsGroup
			if value.ID >= 0 {
				old = ms.groupsByID[value.ID]
				ms.groupsByID[value.ID] = value
			} else {
				old = ms.builtInGroup[value.ID]
				ms.builtInGroup[value.ID] = value

			}
			ms.calcGroupForMetricsLocked(old, value)
			ms.calcGroupNamesMapLocked()
			ms.mu.Unlock()
		case format.PromConfigEvent:
			ms.mu.Lock()
			switch e.Id {
			case format.PrometheusConfigID:
				ms.promConfig = e
				promConfigSet = true
				promConfigData = e.Data
			case format.PrometheusGeneratedConfigID:
				ms.promConfigGenerated = e
				promConfigGeneratedSet = true
				promConfigGeneratedData = e.Data
			case format.KnownTagsConfigID:
				ms.knownTags = e
				knownTagsSet = true
				knownTagsData = e.Data
			}
			ms.mu.Unlock()
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
			_ = value.RestoreCachedInfo(value.ID < 0)

			ms.mu.Lock()
			if value.ID >= 0 {
				if oldNamespace, ok := ms.namespaceByID[value.ID]; ok && oldNamespace.Name != value.Name {
					delete(ms.namespaceByName, oldNamespace.Name)
				}

				ms.namespaceByID[value.ID] = value
				ms.namespaceByName[value.Name] = value
			} else {
				ms.builtInNamespace[value.ID] = value
			}
			ms.calcGroupNamesMapLocked()
			ms.mu.Unlock()
		}
	}
	if ms.applyPromConfig != nil {
		// outside of lock, once
		if promConfigSet {
			ms.applyPromConfig(format.PrometheusConfigID, promConfigData)
		}
		if promConfigGeneratedSet {
			ms.applyPromConfig(format.PrometheusGeneratedConfigID, promConfigGeneratedData)
		}
		if knownTagsSet {
			ms.applyPromConfig(format.KnownTagsConfigID, knownTagsData)
		}
	}
	if ll := len(newEntries); ll != 0 {
		ms.broadcastJournalVersionClient(newEntries[ll-1].Version)
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
					ms.updateMetric(&mCopy)
				}
			}
		}

		for _, m := range ms.metricsByID {
			if new.MetricIn(m) {
				mCopy := *m
				mCopy.GroupID = new.ID
				ms.updateMetric(&mCopy)
			}
		}
	} else {
		for _, m := range ms.metricsByID {
			if new.ID == format.BuiltinGroupIDDefault && (m.GroupID == 0 || m.GroupID == format.BuiltinGroupIDDefault) {
				mCopy := *m
				mCopy.GroupID = new.ID
				ms.updateMetric(&mCopy)
			}
		}
	}
}

// call when metric is added or changed O(number of groups)
func (ms *MetricsStorage) calcGroupForMetricLocked(new *format.MetricMetaValue) {
	new.GroupID = format.BuiltinGroupIDDefault
	for _, g := range ms.groupsByID {
		if g.MetricIn(new) {
			new.GroupID = g.ID
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

func (ms *MetricsStorage) calcGroupNamesMapLocked() {
	newM := make(map[string]*format.MetricsGroup, len(ms.groupsByID))
	for _, g := range ms.groupsByID {
		newM[g.Name] = g
	}
	ms.groupsByName = newM
}

func (ms *MetricsStorage) WaitVersion(ctx context.Context, version int64) error {
	select {
	case <-ms.waitVersion(version):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (ms *MetricsStorage) waitVersion(version int64) chan struct{} {
	ms.versionClientMu.Lock()
	defer ms.versionClientMu.Unlock()
	ch := make(chan struct{})
	if ms.lastVersion >= version {
		close(ch)
		return ch
	}

	ms.versionClients = append(ms.versionClients, versionClient{
		expectedVersion: version,
		ch:              ch,
	})
	return ch
}

func (ms *MetricsStorage) broadcastJournalVersionClient(currentVersion int64) {
	ms.versionClientMu.Lock()
	defer ms.versionClientMu.Unlock()
	ms.lastVersion = currentVersion
	keepPos := 0
	for _, client := range ms.versionClients {
		if client.expectedVersion <= currentVersion {
			close(client.ch)
		} else {
			ms.versionClients[keepPos] = client
			keepPos++
		}
	}
	ms.versionClients = ms.versionClients[:keepPos]
}
