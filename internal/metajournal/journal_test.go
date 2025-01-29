// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metajournal

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
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
