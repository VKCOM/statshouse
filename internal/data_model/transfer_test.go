// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
package data_model

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/vkcom/statshouse/internal/format"
)

const (
	maxSTagLength = 128 // Maximum length for string tags
)

func getActualSize(t require.TestingT, key Key, defaultTimestamp uint32) int {
	item := key.TLMultiItemFromKey(defaultTimestamp)
	buf := make([]byte, 0, 1024) // Initial buffer
	buf, err := item.WriteGeneral(buf)
	require.NoError(t, err)
	return len(buf)
}

// Custom generators for Key components
func genKey() *rapid.Generator[Key] {
	return rapid.Custom(func(t *rapid.T) Key {
		key := Key{
			Timestamp: uint32(rapid.Int64Range(0, int64(time.Now().Unix())).Draw(t, "timestamp")),
			Metric:    int32(rapid.Int64Range(0, 1000).Draw(t, "metric")),
		}

		// Generate random tags
		for i := range key.Tags {
			key.Tags[i] = int32(rapid.Int64Range(0, 1000).Draw(t, "tag"))
		}

		// Generate STags with different patterns
		pattern := rapid.IntRange(0, 3).Draw(t, "stags_pattern")
		switch pattern {
		case 0: // All empty
			// STags already initialized as empty strings
		case 1: // One non-empty
			idx := rapid.IntRange(0, format.MaxTags-1).Draw(t, "stag_index")
			key.STags[idx] = rapid.StringN(1, 20, maxSTagLength).Draw(t, "stag")
		case 2: // Half filled
			for i := 0; i < format.MaxTags/2; i++ {
				key.STags[i] = rapid.StringN(1, 20, maxSTagLength).Draw(t, "stag")
			}
		case 3: // All filled
			for i := range key.STags {
				key.STags[i] = rapid.StringN(1, 20, maxSTagLength).Draw(t, "stag")
			}
		}

		return key
	})
}

func TestKeySizeEstimationProperty(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		key := genKey().Draw(t, "key")
		defaultTimestamp := uint32(time.Now().Unix())

		estimated := key.TLSizeEstimate(defaultTimestamp)
		actual := getActualSize(t, key, defaultTimestamp)

		// The estimated size should never be less than actual
		if estimated < actual {
			t.Fatalf("Estimated size %d is less than actual size %d for key: %+v",
				estimated, actual, key)
		}

		// The estimation shouldn't be too far off (within 20%)
		if float64(estimated) > float64(actual)*1.2 {
			t.Fatalf("Estimated size %d is too large compared to actual size %d for key: %+v",
				estimated, actual, key)
		}
	})
}

func TestKeySizeEstimationEdgeCases(t *testing.T) {
	testCases := []struct {
		name string
		key  Key
	}{
		{
			name: "Empty key",
			key:  Key{},
		},
		{
			name: "Only timestamp",
			key: Key{
				Timestamp: 12345,
			},
		},
		{
			name: "Only metric",
			key: Key{
				Metric: 67890,
			},
		},
		{
			name: "Single tag",
			key: Key{
				Tags: [format.MaxTags]int32{42},
			},
		},
		{
			name: "Single STag",
			key: Key{
				STags: [format.MaxTags]string{"test"},
			},
		},
		{
			name: "Single STag max length",
			key: Key{
				STags: [format.MaxTags]string{strings.Repeat("a", maxSTagLength)},
			},
		},
		{
			name: "All fields filled with max length STags",
			key: Key{
				Timestamp: 12345,
				Metric:    67890,
				Tags:      [format.MaxTags]int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
				STags: func() [format.MaxTags]string {
					var stags [format.MaxTags]string
					for i := range stags {
						stags[i] = strings.Repeat("a", maxSTagLength)
					}
					return stags
				}(),
			},
		},
		{
			name: "Timestamp equals defaultTimestamp",
			key: Key{
				Timestamp: 12345, // Will be passed as defaultTimestamp in test
				Metric:    67890,
			},
		},
		{
			name: "Mixed length STags",
			key: Key{
				STags: func() [format.MaxTags]string {
					var stags [format.MaxTags]string
					for i := range stags {
						stags[i] = strings.Repeat("a", i) // Different lengths: 0, 8, 16, ...
					}
					return stags
				}(),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			defaultTimestamp := tc.key.Timestamp
			estimated := tc.key.TLSizeEstimate(defaultTimestamp)
			actual := getActualSize(t, tc.key, defaultTimestamp)

			require.Equal(t, estimated, actual,
				"Estimated size should be equal to actual size\nKey: %+v\nEstimated: %d\nActual: %d",
				tc.key, estimated, actual)
		})
	}
}
