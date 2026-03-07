// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metajournal

import (
	"cmp"
	"context"
	"log"
	"slices"
	"strings"
	"sync"

	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/VKCOM/statshouse/internal/format"
)

type ApplyPromConfig func(configID int32, configString string)

type MetricsStorage struct {
	mu            sync.RWMutex
	metricUpdMu   sync.Mutex // allows consistency through metric update functions without taking mu.Lock
	metricsByID   map[int32]*format.MetricMetaValue
	metricsByName map[string]*format.MetricMetaValue

	dashboardByID map[int32]*format.DashboardMeta

	groupsByID    map[int32]*format.MetricsGroup
	groupsByName  map[string]*format.MetricsGroup
	groupsOrdered []*format.MetricsGroup // not disabled, reversed so longer suffix first

	namespaceByID   map[int32]*format.NamespaceMeta
	namespaceByName map[string]*format.NamespaceMeta

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
		metricsByID:     map[int32]*format.MetricMetaValue{},
		metricsByName:   map[string]*format.MetricMetaValue{},
		dashboardByID:   map[int32]*format.DashboardMeta{},
		groupsByID:      map[int32]*format.MetricsGroup{},
		groupsByName:    map[string]*format.MetricsGroup{},
		namespaceByID:   map[int32]*format.NamespaceMeta{},
		namespaceByName: map[string]*format.NamespaceMeta{},
		applyPromConfig: applyPromConfig,
	}
	for id, g := range format.BuiltInGroupDefault {
		result.groupsByID[id] = g
		result.groupsByName[g.Name] = g
	}
	for id, g := range format.BuiltInNamespaceDefault {
		result.namespaceByID[id] = g
		result.namespaceByName[g.Name] = g
	}
	return result
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
	return ms.namespaceByName[name]
}

func (ms *MetricsStorage) GetGroupByName(name string) *format.MetricsGroup {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.groupsByName[name]
}

func (ms *MetricsStorage) GetGroupByMetricName(metricName string) *format.MetricsGroup {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	for _, g := range ms.groupsByID {
		if g.Disable || !strings.HasPrefix(metricName, g.Name) {
			continue
		}
		return g
	}
	return nil
}

// TODO some fixes to avoid allocation
func (ms *MetricsStorage) GetMetaMetricByNameBytes(metric []byte) *format.MetricMetaValue {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.metricsByName[string(metric)]
}

func (ms *MetricsStorage) GetMetaMetricList(includeDisabled bool) []*format.MetricMetaValue {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	li := make([]*format.MetricMetaValue, 0, len(ms.metricsByName))
	for _, v := range ms.metricsByName {
		if !includeDisabled && v.Disable {
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
	return ms.groupsByID[id]
}

func (ms *MetricsStorage) GetGroupMetricsNames(id int32) []string {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
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
	return metricNames
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
	return namespaces
}

func (ms *MetricsStorage) ApplyEvent(newEntries []tlmetadata.Event) {
	// This code operates on immutable structs, it should not change any stored object, except of map
	ms.metricUpdMu.Lock()
	// do not return before unlocking below
	promConfigSet := false
	promConfigData := ""
	promConfigGeneratedSet := false
	promConfigGeneratedData := ""
	knownTagsSet := false
	knownTagsData := ""
	changedGroups := false

	for _, e := range newEntries {
		switch e.EventType {
		case format.MetricEvent:
			value, err := MetricMetaFromEvent(e)
			if err != nil {
				log.Printf("Cannot apply metric event: %v", err)
				continue
			}
			ms.mu.Lock()
			valueOld, idExists := ms.metricsByID[value.MetricID]
			if idExists && valueOld.Name != value.Name {
				delete(ms.metricsByName, valueOld.Name)
			}
			if idExists && valueOld.Name == value.Name {
				value.GroupID = valueOld.GroupID
			} else {
				value.GroupID = ms.calcGroupForMetricLocked(ms.groupsOrdered, value)
			}
			ms.metricsByID[value.MetricID] = value
			ms.metricsByName[value.Name] = value
			ms.mu.Unlock()
		case format.DashboardEvent:
			dash, err := DashboardMetaFromEvent(e)
			if err != nil {
				log.Printf("Cannot apply dashboard event: %v", err)
				continue
			}
			ms.mu.Lock()
			ms.dashboardByID[dash.DashboardID] = dash
			ms.mu.Unlock()
		case format.MetricsGroupEvent:
			value, err := GroupMetaFromEvent(e)
			if err != nil {
				log.Printf("Cannot apply metric group event: %v", err)
				continue
			}
			ms.mu.Lock()
			valueOld, idExists := ms.groupsByID[value.ID]
			ms.groupsByID[value.ID] = value
			if idExists && valueOld.Name != value.Name {
				delete(ms.groupsByName, valueOld.Name)
			}
			ms.groupsByName[value.Name] = value
			// cases when change of group might change group for metrics
			// 1. added new
			// 2. name changed
			// 3. enabled/disabled
			if !idExists || valueOld.Name != value.Name || valueOld.Disable != value.Disable {
				changedGroups = true
			}
			// We have not atomic updating, so before code below runs, for some time metric groups will be incorrectly assigned.
			// How we should solve this problem:
			// 1. create MetricGroupsAtomic class with metrics and groups, put it as a variable in MetricStorage
			// 2. when we are applying events, while there is no actual group changes, we apply directly to member
			// 3. as soon as we encounter actual group change event, we copy maps from member to new variable MetricGroupsAtomic
			//    and call ApplyEvents on it. After it finishes, we atomically swap variable into our storage
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
			value, err := NamespaceMetaFromEvent(e)
			if err != nil {
				log.Printf("Cannot apply namespace event: %v", err)
				continue
			}
			ms.mu.Lock()
			if oldNamespace, idExists := ms.namespaceByID[value.ID]; idExists && oldNamespace.Name != value.Name {
				delete(ms.namespaceByName, oldNamespace.Name)
			}
			ms.namespaceByID[value.ID] = value
			ms.namespaceByName[value.Name] = value
			ms.mu.Unlock()
		}
	}
	if changedGroups {
		ms.mu.RLock()
		metricsByID := make(map[int32]*format.MetricMetaValue, len(ms.metricsByID))
		metricsByName := make(map[string]*format.MetricMetaValue, len(ms.metricsByName))

		var groupsOrdered []*format.MetricsGroup
		for _, g := range ms.groupsByID {
			if !g.Disable {
				groupsOrdered = append(groupsOrdered, g)
			}
		}
		slices.SortFunc(groupsOrdered, func(a, b *format.MetricsGroup) int {
			return -cmp.Compare(a.Name, b.Name) // reversed
		})
		// O(metrics * groups)
		// doesn't slow down read requests
		for id, m := range ms.metricsByID {
			newGroup := ms.calcGroupForMetricLocked(groupsOrdered, m)
			if m.GroupID != newGroup { // optimize copy
				mCopy := *m
				mCopy.GroupID = newGroup
				m = &mCopy
			}
			metricsByID[id] = m
			metricsByName[m.Name] = m
		}
		ms.mu.RUnlock()

		ms.mu.Lock()
		ms.metricsByID = metricsByID
		ms.metricsByName = metricsByName
		ms.groupsOrdered = groupsOrdered
		ms.mu.Unlock()
	}
	// all callbacks below must operate not under lock
	ms.metricUpdMu.Unlock()
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

func (ms *MetricsStorage) calcGroupForMetricLocked(groupsOrdered []*format.MetricsGroup, m *format.MetricMetaValue) int32 {
	newGroup := int32(format.BuiltinGroupIDDefault)
	for _, g := range groupsOrdered {
		if strings.HasPrefix(m.Name, g.Name) {
			newGroup = g.ID
			break
		}
	}
	return newGroup
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
