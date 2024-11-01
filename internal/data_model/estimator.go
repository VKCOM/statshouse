// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package data_model

import (
	"sync"

	"pgregory.net/rand"

	"github.com/vkcom/statshouse/internal/format"
)

// Algorithm idea
// time-> ..    .   .   ...
// Cur  [ ..    .   .   ... ]
// Next           [ .   ... ]
// time-> ..    .   .   ...   *
// Cur            [ .   ...   *         ]
// Next                     [ *         ]

type Estimator struct {
	mu sync.Mutex

	hour     map[uint32]map[int32]*ChUnique // estimator per hour
	halfHour map[uint32]map[int32]*ChUnique // estimator per hour, but shifted by 30 minutes

	window         uint32
	maxCardinality float64
}

func updateEstimate(e map[int32]*ChUnique, metric int32, hash uint64) {
	u, ok := e[metric]
	if !ok {
		u = &ChUnique{}
		e[metric] = u
	}
	u.Insert(hash)
}

// Will cause divide by 0 if forgotten
func (e *Estimator) Init(window int, maxCardinality int) {
	e.window = uint32(window)
	e.maxCardinality = float64(maxCardinality)
	e.hour = map[uint32]map[int32]*ChUnique{}
	e.halfHour = map[uint32]map[int32]*ChUnique{}
}

func (e *Estimator) UpdateWithKeys(time uint32, keys []Key) {
	e.mu.Lock()
	defer e.mu.Unlock()
	ah, bh := e.createEstimatorsLocked(time)
	for _, key := range keys {
		hash := key.Hash()
		updateEstimate(ah, key.Metric, hash)
		updateEstimate(bh, key.Metric, hash)
	}
}

func (e *Estimator) createEstimatorsLocked(time uint32) (map[int32]*ChUnique, map[int32]*ChUnique) {
	tp := time / e.window
	ah, ok := e.hour[tp]
	if !ok {
		ah = map[int32]*ChUnique{}
		e.hour[tp] = ah
	}
	stp := (time + e.window/2) / e.window
	bh, ok := e.halfHour[stp]
	if !ok {
		bh = map[int32]*ChUnique{}
		e.halfHour[stp] = bh
	}
	return ah, bh
}

func (e *Estimator) ReportHourCardinality(rng *rand.Rand, time uint32, usedMetrics map[int32]struct{}, builtInStat *map[Key]*MultiItem, aggregatorHost int32, shardKey int32, replicaKey int32, numShards int) {
	e.mu.Lock()
	defer e.mu.Unlock()

	ah, bh := e.createEstimatorsLocked(time)
	// Estimators overlap, we take linear combination to smooth value
	aWeight := 2 * float64(time%e.window) / float64(e.window)
	if aWeight > 1 {
		aWeight = 2 - aWeight
	}
	for k := range usedMetrics {
		// We take estimator size as is, which increases in steps. Those steps can be visible on graphs.
		// with asis:false, it will jitter with each value added, which is much worse
		ac := float64(0)
		if au, ok := ah[k]; ok {
			ac = float64(au.Size(true))
		}
		bc := float64(0)
		if bu, ok := bh[k]; ok {
			bc = float64(bu.Size(true))
		}
		cardinality := ac*aWeight + bc*(1-aWeight)
		// We tried to sample metrics with high cardinality by not writing different deterministic subspace
		// of key space each hour, but this was too gross for users. We believe we need different idea.
		// Code for reference. sampleFactor would be passed to SampleFactorDeterministic during inserting
		// if cardinality > e.maxCardinality {
		//	sf := cardinality / e.maxCardinality
		//	sampleFactors[k] = sf
		// }
		cardinality *= float64(numShards)
		// show full cardinality estimates in metrics. We need sum of average(all inserted per aggregator) over all aggregators
		// we cannot implement this, so we multiply by # of shards, expecting uniform load (which is wrong if skip shards option is given to agents)
		// so avg() of this metric shows full estimate
		key := AggKey((time/60)*60, format.BuiltinMetricIDAggHourCardinality, [format.MaxTags]int32{0, 0, 0, 0, k}, aggregatorHost, shardKey, replicaKey)
		MapKeyItemMultiItem(builtInStat, key, AggregatorStringTopCapacity, nil, nil).Tail.AddValueCounterHost(rng, cardinality, 1, aggregatorHost)
	}
}

func (e *Estimator) GarbageCollect(oldestTime uint32) {
	// We repeat algorithm in createEstimatorsLocked for last timestamp we accept
	e.mu.Lock()
	defer e.mu.Unlock()

	tp := oldestTime / e.window
	stp := (oldestTime + e.window/2) / e.window
	// Collections are small - ~50 for 1h to 2d ratio
	for k := range e.hour {
		if k < tp {
			delete(e.hour, k)
		}
	}
	for k := range e.halfHour {
		if k < stp {
			delete(e.halfHour, k)
		}
	}
}
