// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metajournal

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/vkcom/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/vkcom/statshouse/internal/format"
)

func newMetricStorage(loader MetricsStorageLoader) *MetricsStorage {
	result := MakeMetricsStorage("", nil, nil)
	result.journal.metaLoader = loader
	return result
}

func TestMetricsStorage(t *testing.T) {
	events := []tlmetadata.Event{}
	m := newMetricStorage(func(ctx context.Context, lastVersion int64, returnIfEmpty bool) ([]tlmetadata.Event, int64, error) {
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
	t.Run("updateJournal test(each other depends on previous)", func(t *testing.T) {
		descrField := "__description"
		value := format.BuiltinMetrics[format.BuiltinMetricIDAPIBRS]
		_ = value.RestoreCachedInfo()
		metricBytes, err := value.MarshalBinary()
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

		group := format.MetricsGroup{
			ID:     331,
			Name:   "abc",
			Weight: 1,
		}

		t.Run("create metric", func(t *testing.T) {
			value.Version = 1
			events = []tlmetadata.Event{
				{
					Id:        int64(value.MetricID),
					Name:      value.Name,
					EventType: format.MetricEvent,
					Version:   1,
					Data:      string(metricBytes),
				},
			}
			err = m.journal.updateJournal(nil)
			require.NoError(t, err)

			require.Len(t, m.journal.journal, 1)
			require.Len(t, m.journal.journalOld, 1)

			require.Equal(t, int64(1), m.journal.versionLocked())

			require.Len(t, m.metricsByID, 1)
			require.Len(t, m.metricsByName, 1)
			require.Equal(t, *value, *m.metricsByID[value.MetricID])
			require.Equal(t, *value, *m.metricsByName[value.Name])
		})

		t.Run("edit metric", func(t *testing.T) {
			descr := "new descr"
			value.Version = 2
			value.Description = descr
			bytes, err := value.MarshalBinary()
			require.NoError(t, err)
			events = []tlmetadata.Event{
				{
					Id:        int64(value.MetricID),
					Name:      value.Name,
					EventType: format.MetricEvent,
					Version:   2,
					Data:      string(bytes),
				},
			}
			err = m.journal.updateJournal(nil)
			require.NoError(t, err)

			require.Len(t, m.journal.journal, 1)
			require.Len(t, m.journal.journalOld, 1)

			require.Equal(t, int64(2), m.journal.versionLocked())

			require.Len(t, m.metricsByID, 1)
			require.Len(t, m.metricsByName, 1)
			require.Equal(t, *value, *m.metricsByID[value.MetricID])
			require.Equal(t, *value, *m.metricsByName[value.Name])
		})

		t.Run("rename metric", func(t *testing.T) {
			descr := "new descr"
			value.Name = "abc"
			value.Version = 3
			value.Description = descr
			bytes, err := value.MarshalBinary()
			require.NoError(t, err)
			events = []tlmetadata.Event{
				{
					Id:        int64(value.MetricID),
					Name:      value.Name,
					EventType: format.MetricEvent,
					Version:   3,
					Data:      string(bytes),
				},
			}
			err = m.journal.updateJournal(nil)
			require.NoError(t, err)

			require.Len(t, m.journal.journal, 1)
			require.Len(t, m.journal.journalOld, 1)

			require.Equal(t, int64(3), m.journal.versionLocked())

			require.Len(t, m.metricsByID, 1)
			require.Len(t, m.metricsByName, 1)
			require.Equal(t, *value, *m.metricsByID[value.MetricID])
			require.Equal(t, *value, *m.metricsByName[value.Name])
		})

		t.Run("create dashboard", func(t *testing.T) {
			dashboard.Version = 4
			events = []tlmetadata.Event{
				{
					Id:        int64(dashboard.DashboardID),
					Name:      dashboard.Name,
					EventType: format.DashboardEvent,
					Version:   4,
					Data:      string(dashboardBytes),
				},
			}
			err = m.journal.updateJournal(nil)
			require.NoError(t, err)

			require.Len(t, m.journal.journal, 2)
			require.Len(t, m.journal.journalOld, 1)

			require.Equal(t, int64(4), m.journal.versionLocked())

			require.Len(t, m.metricsByID, 1)
			require.Len(t, m.metricsByName, 1)
			require.Equal(t, dashboard, *m.dashboardByID[dashboard.DashboardID])
		})

		t.Run("edit dashboard", func(t *testing.T) {
			descr := "new descr"
			dashboard.Version = 5
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
					Version:   5,
					Data:      string(bytes),
				},
			}
			err = m.journal.updateJournal(nil)
			require.NoError(t, err)

			require.Len(t, m.journal.journal, 2)
			require.Len(t, m.journal.journalOld, 1)

			require.Equal(t, int64(5), m.journal.versionLocked())

			require.Len(t, m.metricsByID, 1)
			require.Len(t, m.metricsByName, 1)
			require.Equal(t, dashboard, *m.dashboardByID[dashboard.DashboardID])
		})
		t.Run("broadcast version clients", func(t *testing.T) {
			value.Version = 6
			value.MetricID = 1235
			value.Name = "testmetric3"
			events = []tlmetadata.Event{
				{
					Id:        int64(value.MetricID),
					Name:      value.Name,
					EventType: format.MetricEvent,
					Version:   value.Version,
					Data:      string(metricBytes),
				},
			}
			waitCh := m.journal.waitVersion(value.Version)
			err = m.journal.updateJournal(nil)
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
				case <-m.journal.waitVersion(value.Version):
				default:
					t.Fatal("Expected to get update of journal")
				}
			})
		})
		t.Run("same version and different types", func(t *testing.T) {
			dashboard.Version = 7
			dashboard.DashboardID = value.MetricID
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
			err = m.journal.updateJournal(nil)
			require.NoError(t, err)

			require.Len(t, m.journal.journal, 4)
			require.Len(t, m.journal.journalOld, 2)

			require.Equal(t, int64(7), m.journal.versionLocked())

			require.Len(t, m.metricsByID, 2)
			require.Len(t, m.metricsByName, 2)
			require.Len(t, m.dashboardByID, 2)
			require.Equal(t, dashboard, *m.dashboardByID[dashboard.DashboardID])
		})

		t.Run("empty journal", func(t *testing.T) {
			events = []tlmetadata.Event{}
			err = m.journal.updateJournal(nil)
			require.NoError(t, err)

			require.Len(t, m.journal.journal, 4)
			require.Len(t, m.journal.journalOld, 2)

			require.Equal(t, int64(7), m.journal.versionLocked())

			require.Len(t, m.metricsByID, 2)
			require.Len(t, m.metricsByName, 2)
			require.Len(t, m.dashboardByID, 2)
			require.Equal(t, dashboard, *m.dashboardByID[dashboard.DashboardID])
		})

		t.Run("create group", func(t *testing.T) {
			group.Version = 8
			bytes, err := json.Marshal(group)
			require.NoError(t, err)
			events = []tlmetadata.Event{
				{
					Id:        int64(group.ID),
					Name:      group.Name,
					EventType: format.MetricsGroupEvent,
					Version:   group.Version,
					Data:      string(bytes),
				},
				{
					Id:        int64(group.ID + 1),
					Name:      group.Name + "",
					EventType: format.MetricsGroupEvent,
					Version:   group.Version + 1,
					Data:      string(bytes),
				},
			}
			err = m.journal.updateJournal(nil)
			require.NoError(t, err)

			require.Len(t, m.journal.journal, 6)
			require.Len(t, m.journal.journalOld, 2)

			require.Equal(t, int64(9), m.journal.versionLocked())

			require.Len(t, m.metricsByID, 2)
			require.Len(t, m.metricsByName, 2)
			require.Len(t, m.dashboardByID, 2)
			require.Len(t, m.groupsByID, 2)
			require.Len(t, m.metricsByGroup, 0)
		})

		t.Run("move metric to group", func(t *testing.T) {
			value.Version = 10
			value.GroupID = group.ID
			bytes, err := json.Marshal(value)
			require.NoError(t, err)
			events = []tlmetadata.Event{
				{
					Id:        int64(value.MetricID),
					Name:      value.Name,
					EventType: format.MetricEvent,
					Version:   value.Version,
					Data:      string(bytes),
				},
			}
			err = m.journal.updateJournal(nil)
			require.NoError(t, err)

			require.Len(t, m.journal.journal, 6)
			require.Len(t, m.journal.journalOld, 2)

			require.Equal(t, int64(10), m.journal.versionLocked())

			require.Len(t, m.metricsByID, 2)
			require.Len(t, m.metricsByName, 2)
			require.Len(t, m.dashboardByID, 2)
			require.Len(t, m.groupsByID, 2)
			require.Len(t, m.metricsByGroup, 1)
			require.Contains(t, m.metricsByGroup, group.ID)
			require.Len(t, m.metricsByGroup[group.ID], 1)
			require.Contains(t, m.metricsByGroup[group.ID], value.MetricID)
		})
		t.Run("change metric group", func(t *testing.T) {
			value.Version = 11
			value.GroupID = group.ID + 1
			bytes, err := json.Marshal(value)
			require.NoError(t, err)
			events = []tlmetadata.Event{
				{
					Id:        int64(value.MetricID),
					Name:      value.Name,
					EventType: format.MetricEvent,
					Version:   value.Version,
					Data:      string(bytes),
				},
			}
			err = m.journal.updateJournal(nil)
			require.NoError(t, err)

			require.Len(t, m.journal.journal, 6)
			require.Len(t, m.journal.journalOld, 2)

			require.Equal(t, int64(11), m.journal.versionLocked())

			require.Len(t, m.metricsByID, 2)
			require.Len(t, m.metricsByName, 2)
			require.Len(t, m.dashboardByID, 2)
			require.Len(t, m.groupsByID, 2)
			require.NotContains(t, m.metricsByGroup, group.ID)
			require.Contains(t, m.metricsByGroup, group.ID+1)
			require.Len(t, m.metricsByGroup[group.ID], 0)
			require.Len(t, m.metricsByGroup[group.ID+1], 1)
			require.Contains(t, m.metricsByGroup[group.ID+1], value.MetricID)
		})
		t.Run("broadcast prom clients", func(t *testing.T) {
			events = []tlmetadata.Event{
				{
					Id:        351525,
					Name:      "-",
					EventType: format.PromConfigEvent,
					Version:   12,
					Data:      "abc",
				},
				{
					Id:        351525,
					Name:      "-",
					EventType: format.PromConfigEvent,
					Version:   13,
					Data:      "def",
				},
			}
			var promConfgString string
			var promConfgVersion int64
			m.applyPromConfig = func(configString string, version int64) {
				promConfgString = configString
				promConfgVersion = version
			}
			err = m.journal.updateJournal(nil)
			require.NoError(t, err)
			require.Equal(t, "def", promConfgString)
			require.Equal(t, int64(13), promConfgVersion)
		})
	})
	t.Run("test getJournalDiffLocked3", func(t *testing.T) {
		a := tlmetadata.Event{
			Id:      1,
			Version: 1,
		}
		b := tlmetadata.Event{
			Id:      2,
			Version: 2,
		}
		c := tlmetadata.Event{
			Id:      3,
			Version: 3,
		}
		test := func(clientVersion int64, journal, expected []tlmetadata.Event) func(t *testing.T) {
			return func(t *testing.T) {
				m.journal.journal = journal
				res := m.journal.getJournalDiffLocked3(clientVersion)
				if len(expected) == 0 {
					require.Len(t, res.Events, 0)
				} else {
					require.Equal(t, expected, res.Events)
				}
				require.Equal(t, m.journal.versionLocked(), res.CurrentVersion)
			}
		}
		t.Run("empty journal", test(0, nil, nil))
		t.Run("full journal", test(0, []tlmetadata.Event{a, b, c}, []tlmetadata.Event{a, b, c}))
		t.Run("part of journal1", test(1, []tlmetadata.Event{a, b, c}, []tlmetadata.Event{b, c}))
		t.Run("part of journal2", test(2, []tlmetadata.Event{a, b, c}, []tlmetadata.Event{c}))
		t.Run("part of journal3", test(3, []tlmetadata.Event{a, b, c}, nil))
		t.Run("part of journal4", test(999, []tlmetadata.Event{a, b, c}, nil))
	})
}
