// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metajournal

import (
	"context"
	"encoding/json"
	"math"
	"strings"
	"testing"

	"github.com/mailru/easyjson"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/VKCOM/statshouse/internal/data_model"

	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/VKCOM/statshouse/internal/format"
)

func newMetricStorage(loader MetricsStorageLoader) (*MetricsStorage, *JournalFast) {
	m := MakeMetricsStorage(nil)
	//j := MakeJournal("", data_model.JournalDDOSProtectionTimeout, nil,
	//	[]ApplyEvent{m.ApplyEvent})
	j := MakeJournalFast(data_model.JournalDDOSProtectionTimeout, false,
		[]ApplyEvent{m.ApplyEvent})
	j.metaLoader = loader
	j.parseDiscCache()
	return m, j
}

const testMetricID = 123456
const testNamespace1ID = 332
const testGroup1ID = 233
const testID6 = 62488
const testID7 = 62418

var group1 = format.MetricsGroup{
	ID:     testGroup1ID,
	Name:   "group1",
	Weight: 1,
}

var group2Metric6 = format.MetricMetaValue{MetricID: testID6, Name: group1.Name + "_metric6"}
var testMetric7 = format.MetricMetaValue{MetricID: testID7, Name: "test_metric7"}

type testCase struct {
	name string
	f    func(t *testing.T)
}

func TestMetricStorage1(t *testing.T) {
	events := []tlmetadata.Event{}
	var m *MetricsStorage
	var journal *JournalFast
	createEntity := func(id, namespaceID int64, name string, typ int32, version int64, data any) tlmetadata.Event {
		b, err := json.Marshal(data)
		if err != nil {
			panic(err)
		}
		return tlmetadata.Event{
			Id:          id,
			Name:        name,
			NamespaceId: namespaceID,
			EventType:   typ,
			Version:     version,
			Data:        string(b),
		}
	}
	testCases := []testCase{}

	ns := format.NamespaceMeta{Name: "namespace", Weight: 1}
	require.NoError(t, ns.RestoreCachedInfo(false))
	testCases = append(testCases, testCase{"create metric after namespace", func(t *testing.T) {
		namespace := createEntity(1, 0, "namespace", format.NamespaceEvent, 1, ns)
		ns.ID = 1
		ns.Version = 1
		metric := createEntity(2, 1, "namespace@metric", format.MetricEvent, 2, format.MetricMetaValue{})
		events = []tlmetadata.Event{namespace, metric}
		err := journal.updateJournal(nil)
		require.NoError(t, err)
		require.Contains(t, m.metricsByID, int32(metric.Id))
		require.Contains(t, m.metricsByName, metric.Name)
		require.Contains(t, m.namespaceByID, ns.ID)
		require.Contains(t, m.namespaceByName, ns.Name)
		require.Equal(t, ns, *m.namespaceByID[ns.ID], ns)
		require.Equal(t, ns, *m.namespaceByName[ns.Name], ns)
		actualMetric := m.metricsByID[int32(metric.Id)]
		require.Equal(t, int32(namespace.Id), actualMetric.NamespaceID)
	}})

	testCases = append(testCases, testCase{"create metric before namespace", func(t *testing.T) {
		metric := createEntity(2, 1, "namespace@metric", format.MetricEvent, 1, format.MetricMetaValue{})
		namespace := createEntity(1, 0, "namespace", format.NamespaceEvent, 2, format.NamespaceMeta{})
		events = []tlmetadata.Event{metric, namespace}
		err := journal.updateJournal(nil)
		require.NoError(t, err)
		require.Contains(t, m.metricsByID, int32(metric.Id))
		require.Contains(t, m.metricsByName, metric.Name)
		actualMetric := m.metricsByID[int32(metric.Id)]
		require.Equal(t, int32(namespace.Id), actualMetric.NamespaceID)
	}})

	testCases = append(testCases, testCase{"put metric in namespace", func(t *testing.T) {
		metric := createEntity(2, 0, "metric", format.MetricEvent, 1, format.MetricMetaValue{})
		namespace := createEntity(1, 0, "namespace", format.NamespaceEvent, 2, format.NamespaceMeta{})
		events = []tlmetadata.Event{metric, namespace}
		err := journal.updateJournal(nil)
		require.NoError(t, err)
		require.Contains(t, m.metricsByID, int32(metric.Id))
		require.Contains(t, m.metricsByName, metric.Name)
		actualMetric := m.metricsByID[int32(metric.Id)]
		require.Equal(t, int32(format.BuiltinNamespaceIDDefault), actualMetric.NamespaceID)
		events = append(events, createEntity(2, 1, "namespace@metric", format.MetricEvent, 3, format.MetricMetaValue{}))
		err = journal.updateJournal(nil)
		require.NoError(t, err)
		actualMetric = m.metricsByID[int32(metric.Id)]
		require.Equal(t, int32(namespace.Id), actualMetric.NamespaceID)
	}})

	testCases = append(testCases, testCase{"remove metric from namespace", func(t *testing.T) {
		metric := createEntity(2, 1, "namespace@metric", format.MetricEvent, 1, format.MetricMetaValue{})
		namespace := createEntity(1, 0, "namespace", format.NamespaceEvent, 2, format.NamespaceMeta{})
		events = []tlmetadata.Event{metric, namespace}
		err := journal.updateJournal(nil)
		require.NoError(t, err)
		require.Contains(t, m.metricsByID, int32(metric.Id))
		require.Contains(t, m.metricsByName, metric.Name)
		actualMetric := m.metricsByID[int32(metric.Id)]
		require.Equal(t, int32(namespace.Id), actualMetric.NamespaceID)

		events = append(events, createEntity(2, 0, "metric", format.MetricEvent, 3, format.MetricMetaValue{}))
		err = journal.updateJournal(nil)
		require.NoError(t, err)
		actualMetric = m.metricsByID[int32(metric.Id)]
		require.Equal(t, int32(format.BuiltinNamespaceIDDefault), actualMetric.NamespaceID)
	}})

	testCases = append(testCases, testCase{"create group after namespace", func(t *testing.T) {
		namespace := createEntity(1, 0, "namespace", format.NamespaceEvent, 1, format.NamespaceMeta{})
		group := createEntity(2, 1, "namespace@group", format.MetricsGroupEvent, 2, format.MetricsGroup{})
		events = []tlmetadata.Event{namespace, group}
		err := journal.updateJournal(nil)
		require.NoError(t, err)
		require.Contains(t, m.groupsByID, int32(group.Id))
		require.Contains(t, m.groupsByName, group.Name)
		actualGroup := m.groupsByID[int32(group.Id)]
		require.Equal(t, int32(namespace.Id), actualGroup.NamespaceID)
	}})

	testCases = append(testCases, testCase{"create group before namespace", func(t *testing.T) {
		group := createEntity(2, 1, "namespace@group", format.MetricsGroupEvent, 1, format.MetricsGroup{})
		namespace := createEntity(1, 0, "namespace", format.NamespaceEvent, 2, format.NamespaceMeta{})
		events = []tlmetadata.Event{group, namespace}
		err := journal.updateJournal(nil)
		require.NoError(t, err)
		require.Contains(t, m.groupsByID, int32(group.Id))
		require.Contains(t, m.groupsByName, group.Name)
		actualGroup := m.groupsByID[int32(group.Id)]
		require.Equal(t, int32(namespace.Id), actualGroup.NamespaceID)
	}})

	testCases = append(testCases, testCase{"put group in namespace", func(t *testing.T) {
		group := createEntity(2, 0, "group", format.MetricsGroupEvent, 1, format.MetricsGroup{})
		namespace := createEntity(1, 0, "namespace", format.NamespaceEvent, 2, format.NamespaceMeta{})
		events = []tlmetadata.Event{group, namespace}
		err := journal.updateJournal(nil)
		require.NoError(t, err)
		require.Contains(t, m.groupsByID, int32(group.Id))
		require.Contains(t, m.groupsByName, group.Name)
		actualGroup := m.groupsByID[int32(group.Id)]
		require.Equal(t, int32(format.BuiltinNamespaceIDDefault), actualGroup.NamespaceID)
		events = append(events, createEntity(2, 1, "namespace@group", format.MetricsGroupEvent, 3, format.MetricsGroup{}))
		err = journal.updateJournal(nil)
		require.NoError(t, err)
		actualGroup = m.groupsByID[int32(group.Id)]
		require.Equal(t, int32(namespace.Id), actualGroup.NamespaceID)
	}})

	testCases = append(testCases, testCase{"remove group from namespace", func(t *testing.T) {
		group := createEntity(2, 1, "namespace@group", format.MetricsGroupEvent, 1, format.MetricsGroup{})
		namespace := createEntity(1, 0, "namespace", format.NamespaceEvent, 2, format.NamespaceMeta{})
		events = []tlmetadata.Event{group, namespace}
		err := journal.updateJournal(nil)
		require.NoError(t, err)
		require.Contains(t, m.groupsByID, int32(group.Id))
		require.Contains(t, m.groupsByName, group.Name)
		actualGroup := m.groupsByID[int32(group.Id)]
		require.Equal(t, int32(namespace.Id), actualGroup.NamespaceID)

		events = append(events, createEntity(2, 0, "group", format.MetricsGroupEvent, 3, format.MetricsGroup{}))
		err = journal.updateJournal(nil)
		require.NoError(t, err)
		actualGroup = m.groupsByID[int32(group.Id)]
		require.Equal(t, int32(format.BuiltinNamespaceIDDefault), actualGroup.NamespaceID)
	}})

	testCases = append(testCases, testCase{"create group after metric", func(t *testing.T) {
		metric := createEntity(1, 0, "group_metric", format.MetricEvent, 1, format.MetricMetaValue{})
		group := createEntity(2, 0, "group_", format.MetricsGroupEvent, 2, format.MetricsGroup{})
		events = []tlmetadata.Event{metric, group}
		err := journal.updateJournal(nil)
		require.NoError(t, err)
		require.Contains(t, m.metricsByID, int32(metric.Id))
		require.Contains(t, m.groupsByID, int32(group.Id))
		require.Contains(t, m.groupsByName, group.Name)
		actualMetric := m.metricsByID[int32(metric.Id)]
		require.Equal(t, int32(group.Id), actualMetric.GroupID)
	}})

	testCases = append(testCases, testCase{"create group before metric", func(t *testing.T) {
		group := createEntity(2, 0, "group_", format.MetricsGroupEvent, 1, format.MetricsGroup{})
		metric := createEntity(1, 0, "group_metric", format.MetricEvent, 2, format.MetricMetaValue{})
		events = []tlmetadata.Event{group, metric}
		err := journal.updateJournal(nil)
		require.NoError(t, err)
		require.Contains(t, m.metricsByID, int32(metric.Id))
		require.Contains(t, m.groupsByID, int32(group.Id))
		require.Contains(t, m.groupsByName, group.Name)
		actualMetric := m.metricsByID[int32(metric.Id)]
		require.Equal(t, int32(group.Id), actualMetric.GroupID)
	}})

	testCases = append(testCases, testCase{"rename group and check metric", func(t *testing.T) {
		group := createEntity(2, 0, "group_", format.MetricsGroupEvent, 1, format.MetricsGroup{})
		metric := createEntity(1, 0, "group_metric", format.MetricEvent, 2, format.MetricMetaValue{})
		events = []tlmetadata.Event{group, metric}
		err := journal.updateJournal(nil)
		require.NoError(t, err)
		require.Contains(t, m.metricsByID, int32(metric.Id))
		require.Contains(t, m.groupsByID, int32(group.Id))
		require.Contains(t, m.groupsByName, group.Name)
		actualMetric := m.metricsByID[int32(metric.Id)]
		require.Equal(t, int32(group.Id), actualMetric.GroupID)
		group = createEntity(2, 0, "group1_", format.MetricsGroupEvent, 3, format.MetricsGroup{})
		events = append(events, group)
		err = journal.updateJournal(nil)
		require.NoError(t, err)
		require.Contains(t, m.metricsByID, int32(metric.Id))
		require.Contains(t, m.groupsByID, int32(group.Id))
		require.Contains(t, m.groupsByName, group.Name)
		actualMetric = m.metricsByID[int32(metric.Id)]
		require.Equal(t, int32(format.BuiltinGroupIDDefault), actualMetric.GroupID)
	}})

	testCases = append(testCases, testCase{"rename metric and check metric", func(t *testing.T) {
		group := createEntity(2, 0, "group_", format.MetricsGroupEvent, 1, format.MetricsGroup{})
		metric := createEntity(1, 0, "group_metric", format.MetricEvent, 2, format.MetricMetaValue{})
		events = []tlmetadata.Event{group, metric}
		err := journal.updateJournal(nil)
		require.NoError(t, err)
		require.Contains(t, m.metricsByID, int32(metric.Id))
		require.Contains(t, m.groupsByID, int32(group.Id))
		require.Contains(t, m.groupsByName, group.Name)
		actualMetric := m.metricsByID[int32(metric.Id)]
		require.Equal(t, int32(group.Id), actualMetric.GroupID)
		metric = createEntity(1, 0, "group1_metric", format.MetricEvent, 3, format.MetricMetaValue{})
		events = append(events, metric)
		err = journal.updateJournal(nil)
		require.NoError(t, err)
		require.Contains(t, m.metricsByID, int32(metric.Id))
		require.Contains(t, m.groupsByID, int32(group.Id))
		require.Contains(t, m.groupsByName, group.Name)
		actualMetric = m.metricsByID[int32(metric.Id)]
		require.Equal(t, int32(format.BuiltinGroupIDDefault), actualMetric.GroupID)
	}})

	testCases = append(testCases, testCase{"move group to another namespace", func(t *testing.T) {
		group := createEntity(2, 0, "group_", format.MetricsGroupEvent, 1, format.MetricsGroup{})
		metric := createEntity(1, 0, "group_metric", format.MetricEvent, 2, format.MetricMetaValue{})
		namespace := createEntity(3, 0, "namespace", format.NamespaceEvent, 3, format.NamespaceMeta{})
		events = []tlmetadata.Event{group, metric, namespace}
		err := journal.updateJournal(nil)
		require.NoError(t, err)
		require.Contains(t, m.metricsByID, int32(metric.Id))
		require.Contains(t, m.groupsByID, int32(group.Id))
		require.Contains(t, m.groupsByName, group.Name)
		actualMetric := m.metricsByID[int32(metric.Id)]
		require.Equal(t, int32(group.Id), actualMetric.GroupID)
		group = createEntity(2, 3, "namespace@group_", format.MetricsGroupEvent, 4, format.MetricsGroup{})
		events = append(events, group)
		err = journal.updateJournal(nil)
		require.NoError(t, err)
		require.Contains(t, m.metricsByID, int32(metric.Id))
		require.Contains(t, m.groupsByID, int32(group.Id))
		require.Contains(t, m.groupsByName, group.Name)
		actualMetric = m.metricsByID[int32(metric.Id)]
		require.Equal(t, int32(format.BuiltinGroupIDDefault), actualMetric.GroupID)
	}})

	testCases = append(testCases, testCase{"move metric to another namespace", func(t *testing.T) {
		group := createEntity(2, 0, "group_", format.MetricsGroupEvent, 1, format.MetricsGroup{})
		metric := createEntity(1, 0, "group_metric", format.MetricEvent, 2, format.MetricMetaValue{})
		namespace := createEntity(3, 0, "namespace", format.NamespaceEvent, 3, format.NamespaceMeta{})
		events = []tlmetadata.Event{group, metric, namespace}
		err := journal.updateJournal(nil)
		require.NoError(t, err)
		require.Contains(t, m.metricsByID, int32(metric.Id))
		require.Contains(t, m.groupsByID, int32(group.Id))
		require.Contains(t, m.groupsByName, group.Name)
		actualMetric := m.metricsByID[int32(metric.Id)]
		require.Equal(t, int32(group.Id), actualMetric.GroupID)
		metric = createEntity(1, 3, "namespace@group_metric", format.MetricEvent, 4, format.MetricMetaValue{})
		events = append(events, metric)
		err = journal.updateJournal(nil)
		require.NoError(t, err)
		require.Contains(t, m.metricsByID, int32(metric.Id))
		require.Contains(t, m.groupsByID, int32(group.Id))
		require.Contains(t, m.groupsByName, group.Name)
		actualMetric = m.metricsByID[int32(metric.Id)]
		require.Equal(t, int32(format.BuiltinGroupIDDefault), actualMetric.GroupID)
	}})

	for _, tc := range testCases {
		events = nil
		m, journal = newMetricStorage(func(ctx context.Context, lastVersion int64, returnIfEmpty bool) ([]tlmetadata.Event, int64, error) {
			var result []tlmetadata.Event
			for _, e := range events {
				if e.Version > lastVersion {
					result = append(result, e)
				}
			}
			var v int64
			if len(events) > 0 {
				v = events[len(events)-1].Version
			}
			return result, v, nil
		})
		t.Run(tc.name, tc.f)
	}

}

func TestMetricsStorage(t *testing.T) {
	events := []tlmetadata.Event{}
	var version int64 = 0
	incVersion := func() int64 {
		version++
		return version
	}
	loader := func(ctx context.Context, lastVersion int64, returnIfEmpty bool) ([]tlmetadata.Event, int64, error) {
		var result []tlmetadata.Event
		for _, e := range events {
			if e.Version > lastVersion {
				result = append(result, e)
			}
		}
		var v int64
		if len(events) > 0 {
			v = events[len(events)-1].Version
		}
		return result, v, nil
	}
	m, journal := newMetricStorage(loader)
	// actually 1 test, but grouped by small test case (need to run together)
	t.Run("updateJournal test(each other depends on previous)", func(t *testing.T) {
		descrField := "__description"
		metric := *format.BuiltinMetricMetaAPIBRS
		metric.MetricID = testMetricID
		metric.Name = "test_metric"
		metric.GroupID = 0
		metric.BuiltinAllowedToReceive = false // this field is not restored by RestoreCachedInfo
		_ = metric.RestoreCachedInfo()
		metricBytes, err := metric.MarshalBinary()
		require.NoError(t, err)
		dashboard := format.DashboardMeta{
			DashboardID: 94,
			Name:        "abc",
			JSONData: map[string]interface{}{
				descrField: "abc",
			},
		}
		dashboardBytes, err := json.Marshal(dashboard.JSONData)
		require.NoError(t, err)

		namespace := format.NamespaceMeta{
			ID:     332,
			Name:   "namespace",
			Weight: 1,
		}

		t.Run("create metric", func(t *testing.T) {
			metric.Version = incVersion()
			events = []tlmetadata.Event{
				{
					Id:        int64(metric.MetricID),
					Name:      metric.Name,
					EventType: format.MetricEvent,
					Version:   metric.Version,
					Data:      string(metricBytes),
				},
			}
			err = journal.updateJournal(nil)
			require.NoError(t, err)

			require.Len(t, journal.journal, 1)

			require.Equal(t, metric.Version, journal.versionLocked())

			require.Len(t, m.metricsByID, 1)
			require.Len(t, m.metricsByName, 1)
			require.Equal(t, metric, *m.metricsByID[metric.MetricID])
			require.Equal(t, metric, *m.metricsByName[metric.Name])
		})

		t.Run("edit metric", func(t *testing.T) {
			descr := "new descr"
			metric.Version = incVersion()
			metric.Description = descr
			bytes, err := metric.MarshalBinary()
			require.NoError(t, err)
			events = []tlmetadata.Event{
				{
					Id:        int64(metric.MetricID),
					Name:      metric.Name,
					EventType: format.MetricEvent,
					Version:   metric.Version,
					Data:      string(bytes),
				},
			}
			err = journal.updateJournal(nil)
			require.NoError(t, err)

			require.Len(t, journal.journal, 1)

			require.Equal(t, metric.Version, journal.versionLocked())

			require.Len(t, m.metricsByID, 1)
			require.Len(t, m.metricsByName, 1)
			require.Equal(t, metric, *m.metricsByID[metric.MetricID])
			require.Equal(t, metric, *m.metricsByName[metric.Name])
		})

		t.Run("rename metric", func(t *testing.T) {
			descr := "new descr"
			metric.Name = "test_abc"
			metric.Version = incVersion()
			metric.Description = descr
			bytes, err := metric.MarshalBinary()
			require.NoError(t, err)
			events = []tlmetadata.Event{
				{
					Id:        int64(metric.MetricID),
					Name:      metric.Name,
					EventType: format.MetricEvent,
					Version:   metric.Version,
					Data:      string(bytes),
				},
			}
			err = journal.updateJournal(nil)
			require.NoError(t, err)

			require.Len(t, journal.journal, 1)

			require.Equal(t, metric.Version, journal.versionLocked())

			require.Len(t, m.metricsByID, 1)
			require.Len(t, m.metricsByName, 1)
			require.Equal(t, metric, *m.metricsByID[metric.MetricID])
			require.Equal(t, metric, *m.metricsByName[metric.Name])
		})

		t.Run("create dashboard", func(t *testing.T) {
			dashboard.Version = incVersion()
			events = []tlmetadata.Event{
				{
					Id:        int64(dashboard.DashboardID),
					Name:      dashboard.Name,
					EventType: format.DashboardEvent,
					Version:   dashboard.Version,
					Data:      string(dashboardBytes),
				},
			}
			err = journal.updateJournal(nil)
			require.NoError(t, err)

			require.Len(t, journal.journal, 2)

			require.Equal(t, dashboard.Version, journal.versionLocked())

			require.Len(t, m.metricsByID, 1)
			require.Len(t, m.metricsByName, 1)
			require.Equal(t, dashboard, *m.dashboardByID[dashboard.DashboardID])
		})

		t.Run("edit dashboard", func(t *testing.T) {
			descr := "new descr"
			dashboard.Version = incVersion()
			dashboard.JSONData = map[string]interface{}{
				descrField: descr,
			}
			bytes, err := json.Marshal(dashboard.JSONData)
			require.NoError(t, err)
			events = []tlmetadata.Event{
				{
					Id:        int64(dashboard.DashboardID),
					Name:      dashboard.Name,
					EventType: format.DashboardEvent,
					Version:   dashboard.Version,
					Data:      string(bytes),
				},
			}
			err = journal.updateJournal(nil)
			require.NoError(t, err)

			require.Len(t, journal.journal, 2)

			require.Equal(t, dashboard.Version, journal.versionLocked())

			require.Len(t, m.metricsByID, 1)
			require.Len(t, m.metricsByName, 1)
			require.Equal(t, dashboard, *m.dashboardByID[dashboard.DashboardID])
		})
		t.Run("broadcast version clients", func(t *testing.T) {
			metric.Version = incVersion()
			metric.MetricID = 1235
			metric.Name = "test1_metric3"
			events = []tlmetadata.Event{
				{
					Id:        int64(metric.MetricID),
					Name:      metric.Name,
					EventType: format.MetricEvent,
					Version:   metric.Version,
					Data:      string(metricBytes),
				},
			}
			waitCh := m.waitVersion(metric.Version)
			err = journal.updateJournal(nil)
			require.NoError(t, err)
			t.Run("got version update by broadcast", func(t *testing.T) {
				select {
				case <-waitCh:
				default:
					t.Fatal("Expected to get update of journal")
				}
			})
			t.Run("got version update after journal update", func(t *testing.T) {
				select {
				case <-m.waitVersion(metric.Version):
				default:
					t.Fatal("Expected to get update of journal")
				}
			})
		})
		t.Run("same version and different types", func(t *testing.T) {
			dashboard.Version = incVersion()
			dashboard.DashboardID = metric.MetricID
			dashboard.JSONData = map[string]interface{}{}
			bytes, err := json.Marshal(dashboard.JSONData)
			require.NoError(t, err)
			events = []tlmetadata.Event{
				{
					Id:        int64(dashboard.DashboardID),
					Name:      dashboard.Name,
					EventType: format.DashboardEvent,
					Version:   dashboard.Version,
					Data:      string(bytes),
				},
			}
			err = journal.updateJournal(nil)
			require.NoError(t, err)

			require.Len(t, journal.journal, 4)

			require.Equal(t, dashboard.Version, journal.versionLocked())

			require.Len(t, m.metricsByID, 2)
			require.Len(t, m.metricsByName, 2)
			require.Len(t, m.dashboardByID, 2)
			require.Equal(t, dashboard, *m.dashboardByID[dashboard.DashboardID])
		})

		t.Run("empty journal", func(t *testing.T) {
			events = []tlmetadata.Event{}
			err = journal.updateJournal(nil)
			require.NoError(t, err)

			require.Len(t, journal.journal, 4)

			require.Equal(t, dashboard.Version, journal.versionLocked())

			require.Len(t, m.metricsByID, 2)
			require.Len(t, m.metricsByName, 2)
			require.Len(t, m.dashboardByID, 2)
			require.Equal(t, dashboard, *m.dashboardByID[dashboard.DashboardID])
		})

		t.Run("group created (check metric added to group)", func(t *testing.T) {
			group1.Version = incVersion()
			bytesGroup2, err := easyjson.Marshal(group1)
			require.NoError(t, err)
			events = []tlmetadata.Event{
				{
					Id:        int64(group1.ID),
					Name:      group1.Name,
					EventType: format.MetricsGroupEvent,
					Version:   group1.Version,
					Data:      string(bytesGroup2),
				},
			}
			err = journal.updateJournal(nil)
			require.NoError(t, err)

			require.Len(t, journal.journal, 5)

			require.Equal(t, group1.Version, journal.versionLocked())

			require.Len(t, m.metricsByID, 2)
			require.Len(t, m.metricsByName, 2)
			require.Len(t, m.dashboardByID, 2)
			require.Len(t, m.groupsByID, 1+len(format.BuiltInGroupDefault))
		})

		t.Run("metric renamed (check old metric removed from group and new metric added)", func(t *testing.T) {
			metric.Version = incVersion()
			metric.Name = group1.Name + "_metric"
			bytes, err := easyjson.Marshal(metric)
			require.NoError(t, err)
			events = []tlmetadata.Event{
				{
					Id:        int64(metric.MetricID),
					Name:      metric.Name,
					EventType: format.MetricEvent,
					Version:   metric.Version,
					Data:      string(bytes),
				},
			}
			err = journal.updateJournal(nil)
			require.NoError(t, err)

			require.Len(t, journal.journal, 5)

			require.Equal(t, metric.Version, journal.versionLocked())

			require.Len(t, m.metricsByID, 2)
			require.Len(t, m.metricsByName, 2)
			require.Len(t, m.dashboardByID, 2)
			require.Len(t, m.groupsByID, 1+len(format.BuiltInGroupDefault))
		})
		t.Run("metric created (check new metric in group)", func(t *testing.T) {
			var id int32 = 65463
			metricCopy := metric
			metricCopy.Version = incVersion()
			metricCopy.MetricID = id
			metricCopy.Name = group1.Name + "_metric5"
			bytes, err := easyjson.Marshal(metricCopy)
			require.NoError(t, err)
			events = []tlmetadata.Event{
				{
					Id:        int64(metricCopy.MetricID),
					Name:      metricCopy.Name,
					EventType: format.MetricEvent,
					Version:   metricCopy.Version,
					Data:      string(bytes),
				},
			}
			err = journal.updateJournal(nil)
			require.NoError(t, err)

			require.Len(t, journal.journal, 6)

			require.Equal(t, metricCopy.Version, journal.versionLocked())

			require.Len(t, m.metricsByID, 3)
			require.Len(t, m.metricsByName, 3)
			require.Len(t, m.dashboardByID, 2)
			require.Len(t, m.groupsByID, 1+len(format.BuiltInGroupDefault))
		})
		t.Run("group renamed (check old metric removed from group and metric added)", func(t *testing.T) {
			group1.Version = incVersion()
			group1.Name = "group3"
			bytes, err := easyjson.Marshal(group1)
			require.NoError(t, err)
			events = []tlmetadata.Event{
				{
					Id:        int64(group1.ID),
					Name:      group1.Name,
					EventType: format.MetricsGroupEvent,
					Version:   group1.Version,
					Data:      string(bytes),
				},
			}
			err = journal.updateJournal(nil)
			require.NoError(t, err)

			require.Len(t, journal.journal, 6)

			require.Equal(t, group1.Version, journal.versionLocked())

			require.Len(t, m.metricsByID, 3)
			require.Len(t, m.metricsByName, 3)
			require.Len(t, m.dashboardByID, 2)
			require.Len(t, m.groupsByID, 1+len(format.BuiltInGroupDefault))
		})
		t.Run("broadcast prom clients", func(t *testing.T) {
			v1 := incVersion()
			v2 := incVersion()
			events = []tlmetadata.Event{
				{
					Id:        format.PrometheusConfigID,
					Name:      "-",
					EventType: format.PromConfigEvent,
					Version:   v1,
					Data:      "abc",
				},
				{
					Id:        format.PrometheusConfigID,
					Name:      "-",
					EventType: format.PromConfigEvent,
					Version:   v2,
					Data:      "def",
				},
			}
			var promConfgString string
			m.applyPromConfig = func(_ int32, configString string) {
				promConfgString = configString
			}
			err = journal.updateJournal(nil)
			require.NoError(t, err)
			require.Equal(t, "def", promConfgString)
		})

		t.Run("namespace created", func(t *testing.T) {
			namespace.Version = incVersion()
			events = []tlmetadata.Event{
				{
					Id:        int64(namespace.ID),
					Name:      namespace.Name,
					EventType: format.NamespaceEvent,
					Version:   namespace.Version,
					Data:      `{"weight": 1}`,
				},
			}
			err = journal.updateJournal(nil)
			require.NoError(t, err)

			require.Len(t, journal.journal, 8)

			require.Equal(t, namespace.Version, journal.versionLocked())

			require.Len(t, m.metricsByID, 3)
			require.Len(t, m.metricsByName, 3)
			require.Len(t, m.dashboardByID, 2)
			require.Len(t, m.groupsByID, 1+len(format.BuiltInGroupDefault))
			require.Len(t, m.namespaceByID, 1+len(format.BuiltInNamespaceDefault))
		})

		t.Run("metric added to namespace", func(t *testing.T) {
			metric.Version = incVersion()
			metric.NamespaceID = namespace.ID
			_ = namespace.RestoreCachedInfo(false)
			metricBytes, err := metric.MarshalBinary()
			require.NoError(t, err)
			events = []tlmetadata.Event{
				{
					NamespaceId: int64(namespace.ID),
					Id:          int64(metric.MetricID),
					Name:        namespace.Name + format.NamespaceSeparator + metric.Name,
					EventType:   format.MetricEvent,
					Version:     metric.Version,
					Data:        string(metricBytes),
				},
			}
			err = journal.updateJournal(nil)
			require.NoError(t, err)

			require.Len(t, journal.journal, 8)

			require.Equal(t, metric.Version, journal.versionLocked())

			require.Len(t, m.metricsByID, 3)
			require.Len(t, m.metricsByName, 3)
			require.Len(t, m.dashboardByID, 2)
			require.Len(t, m.groupsByID, 1+len(format.BuiltInGroupDefault))
			require.Len(t, m.namespaceByID, 1+len(format.BuiltInNamespaceDefault))

			require.Contains(t, m.metricsByID, metric.MetricID)
		})

		t.Run("group added to namespace", func(t *testing.T) {
			_ = namespace.RestoreCachedInfo(false)
			group1.Version = incVersion()
			group1.NamespaceID = namespace.ID
			groupBytes, err := metric.MarshalBinary()
			require.NoError(t, err)
			events = []tlmetadata.Event{
				{
					NamespaceId: int64(namespace.ID),
					Id:          int64(group1.ID),
					Name:        namespace.Name + format.NamespaceSeparator + group1.Name,
					EventType:   format.MetricsGroupEvent,
					Version:     group1.Version,
					Data:        string(groupBytes),
				},
			}
			err = journal.updateJournal(nil)
			require.NoError(t, err)

			require.Len(t, journal.journal, 8)

			require.Equal(t, group1.Version, journal.versionLocked())

			require.Len(t, m.metricsByID, 3)
			require.Len(t, m.metricsByName, 3)
			require.Len(t, m.dashboardByID, 2)
			require.Len(t, m.groupsByID, 1+len(format.BuiltInGroupDefault))
			require.Len(t, m.namespaceByID, 1+len(format.BuiltInNamespaceDefault))

			require.Contains(t, m.groupsByID, group1.ID)
		})
		t.Run("metric created (check new metric not in group)", func(t *testing.T) {
			group2Metric6.Version = incVersion()
			group2Metric6.NamespaceID = 0
			metric6Bytes, err := easyjson.Marshal(group2Metric6)
			require.NoError(t, err)
			testMetric7.Version = incVersion()
			testMetric7.NamespaceID = testNamespace1ID
			metric7Bytes, err := easyjson.Marshal(testMetric7)
			require.NoError(t, err)
			events = []tlmetadata.Event{
				{
					Id:        int64(group2Metric6.MetricID),
					Name:      group2Metric6.Name,
					EventType: format.MetricEvent,
					Version:   group2Metric6.Version,
					Data:      string(metric6Bytes),
				},
				{
					Id:        int64(testMetric7.MetricID),
					Name:      testMetric7.Name,
					EventType: format.MetricEvent,
					Version:   testMetric7.Version,
					Data:      string(metric7Bytes),
				},
			}
			err = journal.updateJournal(nil)
			require.NoError(t, err)

			require.Len(t, journal.journal, 10)

			require.Equal(t, testMetric7.Version, journal.versionLocked())

			require.Len(t, m.metricsByID, 5)
			require.Len(t, m.metricsByName, 5)
			require.Len(t, m.dashboardByID, 2)
			require.Len(t, m.groupsByID, 1+len(format.BuiltInGroupDefault))
		})
		t.Run("metric change namespace (check metric in group)", func(t *testing.T) {
			group2Metric6.Version = incVersion()
			group2Metric6.NamespaceID = namespace.ID
			bytes, err := easyjson.Marshal(group2Metric6)
			require.NoError(t, err)
			events = []tlmetadata.Event{
				{
					NamespaceId: int64(namespace.ID),
					Id:          int64(group2Metric6.MetricID),
					Name:        namespace.Name + format.NamespaceSeparator + group2Metric6.Name,
					EventType:   format.MetricEvent,
					Version:     group2Metric6.Version,
					Data:        string(bytes),
				},
			}
			err = journal.updateJournal(nil)
			require.NoError(t, err)

			require.Len(t, journal.journal, 10)

			require.Equal(t, group2Metric6.Version, journal.versionLocked())

			require.Len(t, m.metricsByID, 5)
			require.Len(t, m.metricsByName, 5)
			require.Len(t, m.dashboardByID, 2)
			require.Len(t, m.groupsByID, 1+len(format.BuiltInGroupDefault))
		})
		t.Run("get metric 6", func(t *testing.T) {
			metric := m.GetMetaMetricByName(namespace.Name + format.NamespaceSeparator + group2Metric6.Name)
			require.Equal(t, group2Metric6.MetricID, metric.MetricID)
		})
	})
	journal = MakeJournalFast(data_model.JournalDDOSProtectionTimeout, false,
		nil)
	journal.metaLoader = loader

	t.Run("test getJournalDiffLocked3", func(t *testing.T) {
		test := func(clientVersion int64, expected []tlmetadata.Event) func(t *testing.T) {
			return func(t *testing.T) {
				var res tlmetadata.GetJournalResponsenew
				journal.getJournalDiffLocked3(clientVersion, &res)
				if len(expected) == 0 {
					require.Len(t, res.Events, 0)
				} else {
					require.Equal(t, expected, res.Events)
				}
				require.Equal(t, journal.versionLocked(), res.CurrentVersion)
			}
		}
		t.Run("empty journal", test(0, nil))
		events = []tlmetadata.Event{
			{FieldMask: 1, Id: 1, Version: 1},
			{FieldMask: 1, Id: 2, Version: 2},
			{FieldMask: 1, Id: 3, Version: 3},
		}
		err := journal.updateJournal(nil)
		require.NoError(t, err)
		t.Run("full journal", test(0, events))
		t.Run("part of journal1", test(1, events[1:]))
		t.Run("part of journal2", test(2, events[2:]))
		t.Run("part of journal3", test(3, nil))
		t.Run("part of journal4", test(999, nil))
	})
}

func TestMetricsStorageProperties(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		ms, _ := newMetricStorage(func(ctx context.Context, lastVersion int64, returnIfEmpty bool) ([]tlmetadata.Event, int64, error) {
			var result []tlmetadata.Event
			var v int64
			return result, v, nil
		})

		events1 := rapid.SliceOf(genEvent()).Draw(t, "events")
		ms.ApplyEvent(events1)
		checkInvariants(t, ms)

		// we want to check changes to already initialized struct
		events2 := rapid.SliceOf(genEvent()).Draw(t, "events")
		ms.ApplyEvent(events2)
		checkInvariants(t, ms)
	})
}

func checkInvariants(t *rapid.T, ms *MetricsStorage) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	checkPointerEquality(t, ms)
	checkKeyConsistency(t, ms)
	checkGroupPrefixes(t, ms)
	checkDisabledGroups(t, ms)
}

func checkPointerEquality(t *rapid.T, ms *MetricsStorage) {
	for _, byName := range ms.metricsByName {
		byId := ms.metricsByID[byName.MetricID]
		require.Same(t, byName, byId, "metric pointers mismatch")
	}

	for _, byName := range ms.groupsByName {
		byId := ms.groupsByID[byName.ID]
		require.Same(t, byName, byId, "group pointers mismatch")
	}

	for _, byName := range ms.namespaceByName {
		byId := ms.namespaceByID[byName.ID]
		require.Same(t, byId, byName, "namespace pointers mismatch")
	}
}

func checkKeyConsistency(t *rapid.T, ms *MetricsStorage) {
	for id, metric := range ms.metricsByID {
		require.Equal(t, id, metric.MetricID, "metric ID key mismatch")
	}
	for name, metric := range ms.metricsByName {
		require.Equal(t, name, metric.Name, "metric name key mismatch")
	}

	for id, group := range ms.groupsByID {
		require.Equal(t, id, group.ID, "group ID key mismatch")
	}
	for name, group := range ms.groupsByName {
		require.Equal(t, name, group.Name, "group name key mismatch")
	}

	for id, ns := range ms.namespaceByID {
		require.Equal(t, id, ns.ID, "namespace ID key mismatch")
	}
	for name, ns := range ms.namespaceByName {
		require.Equal(t, name, ns.Name, "namespace name key mismatch")
	}
}

func checkGroupPrefixes(t *rapid.T, ms *MetricsStorage) {
	for _, metric := range ms.metricsByID {
		if metric.GroupID < 0 {
			// builtin groups are exception
			continue
		}
		group := ms.groupsByID[metric.GroupID]
		if group != nil {
			require.True(t, strings.HasPrefix(metric.Name, group.Name),
				"metric %q name doesn't start with group %q", metric.Name, group.Name)
		}
	}
}

func checkDisabledGroups(t *rapid.T, ms *MetricsStorage) {
	for _, group := range ms.groupsByID {
		// format.BuiltinGroupIDDefault is special we set it when group is being disabled
		if group.ID != format.BuiltinGroupIDDefault && group.Disable {
			for _, metric := range ms.metricsByID {
				require.NotEqual(t, group.ID, metric.GroupID,
					"metric %q points to disabled group %q", metric.Name, group.Name)
			}
		}
	}
}

func genEvent() *rapid.Generator[tlmetadata.Event] {
	return rapid.Custom(func(t *rapid.T) tlmetadata.Event {
		eventType := rapid.OneOf(
			rapid.Just(format.MetricEvent),
			rapid.Just(format.DashboardEvent),
			rapid.Just(format.MetricsGroupEvent),
			rapid.Just(format.PromConfigEvent),
			rapid.Just(format.NamespaceEvent),
		).Draw(t, "eventType")

		id := int64(rapid.Int32().Draw(t, "id"))
		name := rapid.StringN(1, 120, 120).Draw(t, "name")
		version := rapid.Int64().Draw(t, "version")
		namespaceID := int64(rapid.Int32().Draw(t, "namespaceID"))
		updateTime := rapid.Uint32().Draw(t, "updateTime")

		var data string
		switch eventType {
		case format.MetricEvent:
			metric := genMetricMetaValue().Draw(t, "metric")
			_ = metric.RestoreCachedInfo()
			jsonData, err := easyjson.Marshal(metric)
			require.Nil(t, err, "metric marshal failed")
			data = string(jsonData)

		case format.MetricsGroupEvent:
			group := genMetricsGroup().Draw(t, "group")
			jsonData, err := easyjson.Marshal(group)
			require.Nil(t, err, "group marshal failed")
			data = string(jsonData)

		case format.NamespaceEvent:
			ns := genNamespaceMeta(id).Draw(t, "namespace")
			jsonData, err := easyjson.Marshal(ns)
			require.Nil(t, err, "namespace marshal failed")
			data = string(jsonData)

		case format.DashboardEvent:
			dashboard := rapid.MapOf(
				rapid.String(),
				rapid.String(),
			).Draw(t, "dashboard")
			jsonData, err := json.Marshal(dashboard)
			require.Nil(t, err, "dashboard marshal failed")
			data = string(jsonData)

		case format.PromConfigEvent:
			data = rapid.String().Draw(t, "promConfig")
		}

		return tlmetadata.Event{
			Id:          id,
			Name:        name,
			EventType:   eventType,
			Version:     version,
			Data:        data,
			NamespaceId: namespaceID,
			UpdateTime:  updateTime,
		}
	})
}

func genMetricMetaValue() *rapid.Generator[format.MetricMetaValue] {
	return rapid.Custom(func(t *rapid.T) format.MetricMetaValue {
		return format.MetricMetaValue{
			Description:          rapid.String().Draw(t, "description"),
			Tags:                 rapid.SliceOfN(genMetricMetaTag(), 0, 48).Draw(t, "tags"),
			TagsDraft:            rapid.MapOf(rapid.String(), genMetricMetaTag()).Draw(t, "tagsDraft"),
			Disable:              rapid.Bool().Draw(t, "disable"),
			Kind:                 rapid.StringMatching("^counter$|^value$|^value_p$|^unique$|^mixed$|^mixed_p$").Draw(t, "kind"),
			Weight:               rapid.Float64().Draw(t, "weight"),
			Resolution:           rapid.IntRange(0, math.MaxInt32).Draw(t, "resolution"),
			StringTopName:        rapid.String().Draw(t, "stringTopName"),
			StringTopDescription: rapid.String().Draw(t, "stringTopDescription"),
			PreKeyTagID:          rapid.String().Draw(t, "preKeyTagID"),
			PreKeyFrom:           rapid.Uint32().Draw(t, "preKeyFrom"),
			SkipMaxHost:          rapid.Bool().Draw(t, "skipMaxHost"),
			SkipMinHost:          rapid.Bool().Draw(t, "skipMinHost"),
			SkipSumSquare:        rapid.Bool().Draw(t, "skipSumSquare"),
			PreKeyOnly:           rapid.Bool().Draw(t, "preKeyOnly"),
			MetricType:           rapid.String().Draw(t, "metricType"),
			FairKeyTagIDs:        rapid.SliceOf(rapid.String()).Draw(t, "fairKeyTagIDs"),
			ShardStrategy:        rapid.StringMatching("^tags_hash$|^fixed_shard$|^metric_id$|^$").Draw(t, "shardStrategy"),
			ShardNum:             rapid.Uint32Max(16).Draw(t, "shardNum"),
			PipelineVersion:      rapid.Uint8Range(0, 3).Draw(t, "pipelineVersion"),
		}
	})
}

func genMetricMetaTag() *rapid.Generator[format.MetricMetaTag] {
	return rapid.Custom(func(t *rapid.T) format.MetricMetaTag {
		return format.MetricMetaTag{
			Name:        rapid.String().Draw(t, "name"),
			Description: rapid.String().Draw(t, "description"),
		}
	})
}

func genMetricsGroup() *rapid.Generator[format.MetricsGroup] {
	return rapid.Custom(func(t *rapid.T) format.MetricsGroup {
		return format.MetricsGroup{
			Weight:  rapid.Float64().Draw(t, "weight"),
			Disable: rapid.Bool().Draw(t, "disable"),
		}
	})
}

func genNamespaceMeta(id int64) *rapid.Generator[format.NamespaceMeta] {
	return rapid.Custom(func(t *rapid.T) format.NamespaceMeta {
		return format.NamespaceMeta{
			ID:      int32(id),
			Weight:  rapid.Float64().Draw(t, "weight"),
			Disable: rapid.Bool().Draw(t, "disable"),
		}
	})
}
