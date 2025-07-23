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

	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/VKCOM/statshouse/internal/format"
)

// to copy fuzz files into local unit test corpus, use
// cp ~/.cache/go-build/fuzz/github.com/VKCOM/statshouse/internal/metajournal/FuzzCompactJournal/* testdata/fuzz/FuzzCompactJournal/

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
			if len(journal.journal) > 3 {
				fmt.Printf("aja")
			}
			saver := data_model.ChunkedStorageSaver{
				WriteAt:  journal.writeAt,
				Truncate: journal.truncate,
			}
			if err := journal.save(&saver, 1); err != nil { // maxChunkSize 1 so each event is in its own chunk
				t.Error(err)
			}
			truncateLen := int(arg) * 40 // approximate event size
			if len(*fj) > truncateLen {
				*fj = (*fj)[:truncateLen]
			}
			metricStorage := MakeMetricsStorage(nil)
			journal, _ = LoadJournalFastSlice(fj, data_model.JournalDDOSProtectionTimeout, journal.compact,
				[]ApplyEvent{metricStorage.ApplyEvent})
			return metricStorage, journal
		}
		deliverEvents := func(to *JournalFast, from *JournalFast, arg int) (finished bool) {
			var resp tlmetadata.GetJournalResponsenew
			from.getJournalDiffLocked3(to.loaderVersion, &resp)
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
		// if aggregator restarts and forgets journal, we can end up with different journal order
		//var journalCompactAll tlmetadata.GetJournalResponsenew
		//journalCompact.getJournalDiffLocked3Limits(0, &journalCompactAll, math.MaxInt, math.MaxInt)
		//var journalAgentAll tlmetadata.GetJournalResponsenew
		//journalAgent.getJournalDiffLocked3Limits(0, &journalAgentAll, math.MaxInt, math.MaxInt)
		//if len(journalCompactAll.Events) != len(journalAgentAll.Events) {
		//	t.Errorf("journal length mismatch")
		//}
		//for i, e1 := range journalCompactAll.Events {
		//	e2 := journalAgentAll.Events[i]
		//	if e1 != e2 {
		//		t.Errorf("journal entry mismatch")
		//	}
		//}
		//if journalCompactAll.CurrentVersion != journalAgentAll.CurrentVersion {
		//	t.Errorf("journal versions mismatch")
		//}
		if len(journalCompact.journal) != len(journalAgent.journal) {
			t.Errorf("journal length mismatch")
		}
		for id, e1 := range journalCompact.journal {
			e2 := journalAgent.journal[id]
			if !equalWithoutVersionJournalEvent(e1.Event, e2.Event) {
				t.Errorf("journal entry mismatch: %v %v", e1, e2)
			}
		}
		if journalAgent.stateHashStr != journalCompact.stateHashStr {
			t.Errorf("journal hash mismatch")
		}
	})
}
