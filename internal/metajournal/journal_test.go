// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metajournal

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/format"
)

func Benchmark_LoadAndJSON(b *testing.B) {
	_, events, err := getJournalFileLoader("journal.json")
	require.NoError(b, err)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for _, e := range events {
			switch e.EventType {
			case format.MetricEvent:
				value := &format.MetricMetaValue{}
				err := json.Unmarshal([]byte(e.Data), value)
				require.NoError(b, err)
				value.NamespaceID = int32(e.NamespaceId)
				value.Version = e.Version
				value.Name = e.Name
				value.MetricID = int32(e.Id) // TODO - beware!
				value.UpdateTime = e.UpdateTime
				_ = value.RestoreCachedInfo()
				//if err != nil {
				//	fmt.Printf("%v %v\n", err, time.Unix(int64(e.UpdateTime), 0))
				//}
				// require.NoError(b, err)
			}
		}
	}
}

func Benchmark_Load(b *testing.B) {
	loader, _, err := getJournalFileLoader("journal.json")
	require.NoError(b, err)
	_, journal := newMetricStorage(loader)
	b.ResetTimer()
	b.ReportAllocs()
	for {
		fin, err := journal.updateJournalIsFinished(nil)
		require.NoError(b, err)
		if fin {
			break
		}
	}
}

func Benchmark_LoadFast(b *testing.B) {
	const testFile = "load_fast.tmp"
	loader, _, err := getJournalFileLoader("journal.json")
	require.NoError(b, err)
	m := MakeMetricsStorage(nil)
	_ = os.Remove(testFile)
	fp, err := os.OpenFile(testFile, os.O_CREATE|os.O_RDWR, 0666)
	require.NoError(b, err)

	j, err := LoadJournalFastFile(fp, data_model.JournalDDOSProtectionTimeout,
		[]ApplyEvent{m.ApplyEvent})
	require.NoError(b, err)
	j.metaLoader = loader

	for {
		fin, err := j.updateJournalIsFinished(nil)
		require.NoError(b, err)
		if fin {
			break
		}
	}
	require.NoError(b, j.Save())
	_ = fp.Close()
	fp, err = os.OpenFile(testFile, os.O_CREATE|os.O_RDWR, 0666)
	require.NoError(b, err)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m2 := MakeMetricsStorage(nil)
		_, err = LoadJournalFastFile(fp, data_model.JournalDDOSProtectionTimeout,
			[]ApplyEvent{m2.ApplyEvent})
		require.NoError(b, err)
		require.Equal(b, len(m.metricsByID), len(m2.metricsByID))
		for k, v := range m.metricsByID {
			v2, ok := m2.metricsByID[k]
			require.True(b, ok)
			require.Equal(b, v.Name, v2.Name)
		}
	}
	_ = os.Remove(testFile)
}
