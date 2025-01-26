// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metajournal

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Benchmark_Load(b *testing.B) {
	loader, err := getJournalFileLoader("journal.json")
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
