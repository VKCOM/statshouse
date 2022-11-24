// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package statlogs_test

import (
	"sync"
	"testing"

	"github.com/vkcom/statshouse/internal/vkgo/statlogs"
)

func TestCountRace(t *testing.T) {
	r := statlogs.NewRegistry(t.Logf, "" /* avoid sending anything */, "")

	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				r.AccessMetricRaw("test_stat", statlogs.RawTags{Tag1: "hello", Tag2: "world"}).Count(float64(j))
			}
		}()
	}
	wg.Wait()
}

func BenchmarkValue2(b *testing.B) {
	r := statlogs.NewRegistry(b.Logf, "" /* avoid sending anything */, "")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		r.AccessMetricRaw("test_stat", statlogs.RawTags{Tag1: "hello", Tag2: "world"}).Value(float64(i))
	}
}

func BenchmarkRawValue(b *testing.B) {
	r := statlogs.NewRegistry(b.Logf, "" /* avoid sending anything */, "")
	s := r.AccessMetricRaw("test_stat", statlogs.RawTags{Tag1: "hello", Tag2: "world"})
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		s.Value(float64(i))
	}
}

func BenchmarkCount4(b *testing.B) {
	r := statlogs.NewRegistry(b.Logf, "" /* avoid sending anything */, "")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		r.AccessMetricRaw("test_stat", statlogs.RawTags{Tag1: "hello", Tag2: "brave", Tag3: "new", Tag4: "world"}).Count(float64(i))
	}
}

func BenchmarkRawCount(b *testing.B) {
	r := statlogs.NewRegistry(b.Logf, "" /* avoid sending anything */, "")
	s := r.AccessMetricRaw("test_stat", statlogs.RawTags{Tag1: "hello", Tag2: "brave", Tag3: "new", Tag4: "world"})
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		s.Count(float64(i))
	}
}

func BenchmarkLabeledValue2(b *testing.B) {
	r := statlogs.NewRegistry(b.Logf, "" /* avoid sending anything */, "")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		r.AccessMetric("test_stat", statlogs.Tags{{"hello", "world"}, {"world", "hello"}}).Value(float64(i))
	}
}

func BenchmarkRawLabeledValue(b *testing.B) {
	r := statlogs.NewRegistry(b.Logf, "" /* avoid sending anything */, "")
	s := r.AccessMetric("test_stat", statlogs.Tags{{"hello", "world"}, {"world", "hello"}})
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		s.Value(float64(i))
	}
}

func BenchmarkLabeledCount4(b *testing.B) {
	r := statlogs.NewRegistry(b.Logf, "" /* avoid sending anything */, "")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		r.AccessMetric("test_stat", statlogs.Tags{{"hello", "world"}, {"world", "hello"}, {"hello1", "world"}, {"world1", "hello"}}).Count(float64(i))
	}
}

func BenchmarkRawLabeledCount(b *testing.B) {
	r := statlogs.NewRegistry(b.Logf, "" /* avoid sending anything */, "")
	s := r.AccessMetric("test_stat", statlogs.Tags{{"hello", "world"}, {"world", "hello"}, {"hello1", "world"}, {"world1", "hello"}})
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		s.Count(float64(i))
	}
}
