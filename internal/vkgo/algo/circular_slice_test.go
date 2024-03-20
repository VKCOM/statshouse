// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package algo

import (
	"testing"

	"pgregory.net/rapid"

	"golang.org/x/exp/slices"
)

func BenchmarkCircularBufferEnqueueDequeue(b *testing.B) {
	q := CircularSlice[int]{}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.PushBack(i)
		if q.Len() > 32 {
			_ = q.PopFront()
		}
	}
}

func BenchmarkCircularBufferEnqueue(b *testing.B) {
	q := CircularSlice[int]{}

	q.Reserve(b.N)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.PushBack(i)
	}
}

func BenchmarkCircularBufferDequeue(b *testing.B) {
	q := CircularSlice[int]{}

	for i := 0; i < b.N; i++ {
		q.PushBack(i)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.PopFront()
	}
}

func TestCircularBuffer(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {

		var circular CircularSlice[int]
		var normal []int

		checkEqual := func() {
			s1, s2 := circular.Slices()
			ss := append([]int(nil), s1...)
			ss = append(ss, s2...)
			if !slices.Equal(ss, normal) {
				t.Fatalf("different slices")
			}
			for i, v := range normal {
				if circular.Index(i) != v {
					t.Fatalf("index access broken")
				}
			}
		}

		iter := rapid.IntRange(0, 1000).Draw(t, "iter")
		for i := 0; i < iter; i++ {
			action := rapid.IntRange(0, 2).Draw(t, "action")
			switch action {
			case 0: // push
				value := rapid.Int().Draw(t, "value")
				circular.PushBack(value)
				normal = append(normal, value)
			case 1: // pop
				if circular.Len() != 0 || len(normal) != 0 { // if empty status is different, panic
					v1 := circular.PopFront()
					v2 := normal[0]
					normal = normal[1:]
					if v1 != v2 {
						t.Fatalf("popped different values: %d %d", v1, v2)
					}
				}
			case 2: // reserve
				value := rapid.IntRange(0, iter).Draw(t, "capacity")
				circular.Reserve(value)
			}
			checkEqual()
		}
		checkEqual()
	})
}
