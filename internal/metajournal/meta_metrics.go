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

	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/VKCOM/statshouse/internal/format"
	"github.com/mailru/easyjson"
)

type GroupWithMetricsList struct {
	Group   *format.MetricsGroup
	Metrics []string
}

type ApplyPromConfig func(configID int32, configString string)

type MetricsStorage struct {
	mu            sync.RWMutex
	metricUpdMu   sync.Mutex // allows consistency through metric update functions without taking mu.Lock
	metricsByID   map[int32]*format.MetricMetaValue
	metricsByName map[string]*format.MetricMetaValue

	dashboardByID map[int32]*format.DashboardMeta

	groupsByID   map[int32]*format.MetricsGroup
	groupsByName map[string]*format.MetricsGroup

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
	if group, ok = ms.groupsByID[id]; !ok {
		return GroupWithMetricsList{}, false
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
	promConfigSet := false
	promConfigData := ""
	promConfigGeneratedSet := false
	promConfigGeneratedData := ""
	knownTagsSet := false
	knownTagsData := ""
	type groupWithMeta struct {
		old *format.MetricsGroup
		new *format.MetricsGroup
	}
	var changedGroups []groupWithMeta

	for _, e := range newEntries {
		switch e.EventType {
		case format.MetricEvent:
			value, err := MetricMetaFromEvent(e)
			if err != nil {
				log.Printf("Cannot marshal metric: %v", err)
				continue
			}
			ms.metricUpdMu.Lock()
			ms.mu.Lock()
			valueOld, idExists := ms.metricsByID[value.MetricID]
			if idExists && valueOld.Name != value.Name {
				delete(ms.metricsByName, valueOld.Name)
			}
			if idExists && valueOld.Name == value.Name {
				value.GroupID = valueOld.GroupID
			} else {
				ms.calcGroupForMetricLocked(value)
			}
			ms.updateMetric(value)
			ms.mu.Unlock()
			ms.metricUpdMu.Unlock()
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
			err := easyjson.Unmarshal([]byte(e.Data), value)
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
			valueOld, idExists := ms.groupsByID[value.ID]
			ms.groupsByID[value.ID] = value
			if idExists && valueOld.Name != value.Name {
				delete(ms.groupsByName, valueOld.Name)
			}
			ms.groupsByName[value.Name] = value
			// cases when group might change for metrics:
			// 1. added new
			// 2. name changed
			// 3. enabled/disabled
			if !idExists || valueOld.Name != value.Name || valueOld.Disable != value.Disable {
				changedGroups = append(changedGroups, groupWithMeta{
					old: valueOld,
					new: value,
				})
			}
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
			err := easyjson.Unmarshal([]byte(e.Data), value)
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
			if oldNamespace, idExists := ms.namespaceByID[value.ID]; idExists && oldNamespace.Name != value.Name {
				delete(ms.namespaceByName, oldNamespace.Name)
			}

			ms.namespaceByID[value.ID] = value
			ms.namespaceByName[value.Name] = value
			ms.mu.Unlock()
		}
	}
	if len(changedGroups) > 0 {
		// we need separate lock in order to upgrade mu from read to write
		ms.metricUpdMu.Lock()
		ms.mu.RLock()
		metricsByID := make(map[int32]*format.MetricMetaValue, len(ms.metricsByID))
		metricsByName := make(map[string]*format.MetricMetaValue, len(ms.metricsByName))
		// O(metrics * changed_groups)
		// doesn't slow down read requests
		// can be optimized by storing metricsByName in trie or by adding metricsByGroupID
		metricsChanged := false
		for id, m := range ms.metricsByID {
			um := m // updated metric
			for _, g := range changedGroups {
				if g.old != nil && g.old.MetricIn(um) && g.old.Name != g.new.Name {
					um = um.WithGroupID(format.BuiltinGroupIDDefault)
				}
				if g.new.MetricIn(um) {
					if g.new.Disable {
						um = um.WithGroupID(format.BuiltinGroupIDDefault)
					} else {
						um = um.WithGroupID(g.new.ID)
					}
				}
			}
			if um != m {
				metricsChanged = true
			}
			metricsByID[id] = um
			metricsByName[um.Name] = um
		}
		ms.mu.RUnlock()

		if metricsChanged {
			ms.mu.Lock()
			ms.metricsByID = metricsByID
			ms.metricsByName = metricsByName
			ms.mu.Unlock()
		}
		ms.metricUpdMu.Unlock()
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
