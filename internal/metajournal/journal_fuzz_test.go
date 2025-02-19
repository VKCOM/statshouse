// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metajournal

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/vkcom/statshouse/internal/format"
)

// to copy fuzz files into local unit test corpus, use
// cp ~/.cache/go-build/fuzz/github.com/vkcom/statshouse/internal/metajournal/FuzzCompactJournal/* testdata/fuzz/FuzzCompactJournal/

func FuzzCompactJournal(f *testing.F) {
	//testcases := []string{"Hello, world", " ", "!12345"}
	//for _, tc := range testcases {
	//	f.Add(tc) // Use f.Add to provide a seed corpus
	//}
	f.Fuzz(func(t *testing.T, fuzzData []byte) {
		var originalVersion int64
		var metrics []*format.MetricMetaValue
		var fjOriginal []byte
		journalOriginal, _ := LoadJournalFastSlice(&fjOriginal, data_model.JournalDDOSProtectionTimeout, false, nil)
		metricStorageCompact := MakeMetricsStorage(nil)
		var fjCompact []byte
		journalCompact, _ := LoadJournalFastSlice(&fjCompact, data_model.JournalDDOSProtectionTimeout, true,
			[]ApplyEvent{metricStorageCompact.ApplyEvent})
		metricStorageAgent := MakeMetricsStorage(nil)
		var fjAgent []byte
		journalAgent, _ := LoadJournalFastSlice(&fjAgent, data_model.JournalDDOSProtectionTimeout, false,
			[]ApplyEvent{metricStorageAgent.ApplyEvent})

		saveReload := func(journal *JournalFast, fj *[]byte, arg byte) (*MetricsStorage, *JournalFast) {
			if err := journal.Save(); err != nil {
				t.Error(err)
			}
			switch arg {
			case 0:
				*fj = (*fj)[:0]
			case 1:
				*fj = (*fj)[:len(*fj)/2]
			}
			metricStorage := MakeMetricsStorage(nil)
			journal, _ = LoadJournalFastSlice(fj, data_model.JournalDDOSProtectionTimeout, journal.compact,
				[]ApplyEvent{metricStorage.ApplyEvent})
			return metricStorage, journal
		}
		deliverEvents := func(to *JournalFast, from *JournalFast, arg int) (finished bool) {
			resp := from.getJournalDiffLocked3(to.loaderVersion)
			finished = len(resp.Events) == 0
			if arg < len(resp.Events) {
				resp.Events = resp.Events[:arg]
			}
			to.applyUpdate(resp.Events, resp.CurrentVersion, nil)
			return
		}

		for i := 0; i+1 < len(fuzzData); i += 2 {
			cmd := fuzzData[i]
			arg := fuzzData[i+1]
			switch cmd {
			case 0: // add original metric
				originalVersion++
				metricID := len(metrics) + 1
				metric := &format.MetricMetaValue{
					MetricID: int32(metricID),
					Name:     fmt.Sprintf("m%d", metricID),
					Tags:     []format.MetricMetaTag{{}},
				}
				if err := metric.RestoreCachedInfo(); err != nil {
					t.Error(err)
				}
				data, err := metric.MarshalBinary()
				if err != nil {
					t.Error(err)
				}
				metrics = append(metrics, metric)
				entry := tlmetadata.Event{
					Id:         int64(metric.MetricID),
					Name:       metric.Name,
					EventType:  format.MetricEvent,
					Version:    originalVersion,
					UpdateTime: uint32(time.Now().Unix()),
					Data:       string(data),
				}
				journalOriginal.addEventLocked(nil, entry)
				journalOriginal.finishUpdateLocked()
			case 1, 10: // edit original metric
				if int(arg) >= len(metrics) {
					continue
				}
				originalVersion++
				metric := metrics[arg]
				if cmd == 1 {
					if len(metric.Tags) < format.MaxTags {
						metric.Tags = append(metric.Tags, format.MetricMetaTag{
							Name: fmt.Sprintf("t%d", len(metric.Tags)+1),
						})
					}
				} else {
					metric.Description += "a"
				}
				if err := metric.RestoreCachedInfo(); err != nil {
					t.Error(err)
				}
				data, err := metric.MarshalBinary()
				if err != nil {
					t.Error(err)
				}
				entry := tlmetadata.Event{
					Id:         int64(metric.MetricID),
					Name:       metric.Name,
					EventType:  format.MetricEvent,
					Version:    originalVersion,
					UpdateTime: uint32(time.Now().Unix()),
					Data:       string(data),
				}
				journalOriginal.addEventLocked(nil, entry)
				journalOriginal.finishUpdateLocked()
			case 2: // save+reload compact (with or without corruption)
				metricStorageCompact, journalCompact = saveReload(journalCompact, &fjCompact, arg)
			case 3: // save+reload agent (with or without corruption)
				metricStorageAgent, journalAgent = saveReload(journalAgent, &fjAgent, arg)
			case 4: // deliver events to compact
				_ = deliverEvents(journalCompact, journalOriginal, int(arg))
			case 5: // deliver events to agent
				_ = deliverEvents(journalAgent, journalCompact, int(arg))
			}
		}
		for !deliverEvents(journalCompact, journalOriginal, math.MaxInt) {
		}
		for !deliverEvents(journalAgent, journalCompact, math.MaxInt) {
		}
		if len(metricStorageCompact.metricsByID) != len(metrics) {
			t.Errorf("list length mismatch")
		}
		if len(metricStorageAgent.metricsByID) != len(metrics) {
			t.Errorf("list length mismatch")
		}
		for _, m1 := range metrics {
			m2 := metricStorageCompact.GetMetaMetric(m1.MetricID)
			m3 := metricStorageAgent.GetMetaMetric(m1.MetricID)
			if !format.SameCompactMetric(m1, m2) { // we could compare full size
				t.Errorf("metric %v does not match m2 %v", m1, m2)
			}
			if !format.SameCompactMetric(m1, m3) {
				t.Errorf("metric %v does not match m2 %v", m1, m2)
			}
		}
	})
}
