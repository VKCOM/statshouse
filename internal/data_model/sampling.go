// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package data_model

import (
	"math"

	"pgregory.net/rand"
)

type (
	SamplingMultiItemPair struct {
		Key         Key
		Item        *MultiItem
		WhaleWeight float64 // whale selection criteria, for now sum Counters
	}

	SamplingMetric struct {
		MetricID      int32
		MetricWeight  int64 // actually, effective weight
		RoundFactors  bool
		NoSampleAgent bool
		Group         *SamplingGroup // never nil when running group sampling algorithm, nil for sampling without groups

		SumSize int64
		Items   []SamplingMultiItemPair
	}

	// we have additional sampling stage for groups of metrics
	SamplingGroup struct {
		GroupID     int32
		GroupWeight int64 // actually, effective weight

		MetricList      []*SamplingMetric
		SumMetricWeight int64
		SumSize         int64
	}
)

// This function will be used in second sampling pass to fit all saved data in predefined budget
func SampleFactor(rnd *rand.Rand, sampleFactors map[int32]float64, metric int32) (float64, bool) {
	sf, ok := sampleFactors[metric]
	if !ok {
		return 1, true
	}
	if rnd.Float64()*sf < 1 {
		return sf, true
	}
	return 0, false
}

func RoundSampleFactor(rnd *rand.Rand, sf float64) float64 {
	floor := math.Floor(sf)
	delta := sf - floor
	if rnd.Float64() < delta {
		return floor + 1
	}
	return floor
}

// This function assumes structure of hour table with time = toStartOfHour(time)
// This turned out bad idea, so we do not use it anywhere now
func SampleFactorDeterministic(sampleFactors map[int32]float64, key Key, time uint32) (float64, bool) {
	sf, ok := sampleFactors[key.Metric]
	if !ok {
		return 1, true
	}
	// Deterministic sampling - we select random set of allowed keys per hour
	key.Metric = int32(time/3600) * 3600 // a bit of hack, we do not need metric here, so we replace it with toStartOfHour(time)
	ha := key.Hash()
	if float64(ha>>32)*sf < (1 << 32) { // 0.XXXX * sf < 1 condition shifted left by 32
		return sf, true
	}
	return 0, false
}
