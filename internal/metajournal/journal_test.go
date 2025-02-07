// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metajournal

import (
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
				_, err := MetricMetaFromEvent(e)
				require.NoError(b, err)
				//if err != nil {
				//	fmt.Printf("%v %v\n", err, time.Unix(int64(e.UpdateTime), 0))
				//}
				// require.NoError(b, err)
			}
		}
	}
}

// without btree
// Benchmark_LoadJournal-16    	      45	  27680241 ns/op	33464683 B/op	    6497 allocs/op
// with btree order 2
// Benchmark_LoadJournal-16    	      25	  41577676 ns/op	41777153 B/op	  168693 allocs/op
// with btree order 4
// Benchmark_LoadJournal-16    	      31	  37482240 ns/op	39947350 B/op	   55153 allocs/op
// with btree order 8
// Benchmark_LoadJournal-16    	      32	  33325177 ns/op	39587494 B/op	   26210 allocs/op
// with btree order 16
// Benchmark_LoadJournal-16    	      36	  32985866 ns/op	36136371 B/op	   13265 allocs/op
// with btree order 32
// Benchmark_LoadJournal-16    	      37	  30259911 ns/op	35965603 B/op	    9719 allocs/op
// with btree order 64
// Benchmark_LoadJournal-16    	      36	  32230783 ns/op	35879218 B/op	    8076 allocs/op
// with btree order 128
// Benchmark_LoadJournal-16    	      36	  30982848 ns/op	35845136 B/op	    7293 allocs/op
func Benchmark_LoadJournal(b *testing.B) {
	loader, _, err := getJournalFileLoader("journal.json")
	require.NoError(b, err)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		journal := MakeJournalFast(data_model.JournalDDOSProtectionTimeout, false, nil)
		journal.metaLoader = loader
		for {
			fin, err := journal.updateJournalIsFinished(nil)
			require.NoError(b, err)
			if fin {
				break
			}
		}
	}
}

// without btree
// Benchmark_SaveJournal-16    	      96	  12223486 ns/op	 4177986 B/op	       2 allocs/op
// with btree order 2
// Benchmark_SaveJournal-16    	     169	   7121254 ns/op	 1056787 B/op	       1 allocs/op
// with btree order 8
// Benchmark_SaveJournal-16    	     198	   6135439 ns/op	 1056768 B/op	       1 allocs/op
// with btree order 16
// Benchmark_SaveJournal-16    	     188	   5696968 ns/op	 1056769 B/op	       1 allocs/op
// with btree order 32
// Benchmark_SaveJournal-16    	     210	   5952254 ns/op	 1056768 B/op	       1 allocs/op
// with btree order 128
// Benchmark_SaveJournal-16    	     212	   5958362 ns/op	 1056768 B/op	       1 allocs/op

func Benchmark_SaveJournal(b *testing.B) {
	loader, _, err := getJournalFileLoader("journal.json")
	require.NoError(b, err)
	journal := MakeJournalFast(data_model.JournalDDOSProtectionTimeout, false, nil)
	journal.metaLoader = loader
	for {
		fin, err := journal.updateJournalIsFinished(nil)
		require.NoError(b, err)
		if fin {
			break
		}
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		err := journal.Save()
		require.NoError(b, err)
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

	j, err := LoadJournalFastFile(fp, data_model.JournalDDOSProtectionTimeout, false,
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
		_, err = LoadJournalFastFile(fp, data_model.JournalDDOSProtectionTimeout, false,
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
