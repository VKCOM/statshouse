// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package data_model

import (
	"sync"

	"github.com/VKCOM/statshouse/internal/format"
	"pgregory.net/rand"
)

// Algorithm idea
// time-> ..    .   .   ...
// Cur  [ ..    .   .   ... ]
// Next           [ .   ... ]
// time-> ..    .   .   ...   *
// Cur            [ .   ...   *         ]
// Next                     [ *         ]

type EstimatorMetricHash struct {
	Metric int32
	Hash   uint64
}

const estimatorWindow = 3600 // actually, we never want to change it

type Estimator struct {
	mu sync.Mutex

	hour     map[uint32]map[int32]*ChUnique // estimator per hour
	halfHour map[uint32]map[int32]*ChUnique // estimator per hour, but shifted by 30 minutes
}

func updateEstimate(e map[int32]*ChUnique, mh EstimatorMetricHash) {
	u, ok := e[mh.Metric]
	if !ok {
		u = &ChUnique{}
		e[mh.Metric] = u
	}
	u.Insert(mh.Hash)
}

// Will cause divide by 0 if forgotten
func (e *Estimator) Init() {
	e.hour = map[uint32]map[int32]*ChUnique{}
	e.halfHour = map[uint32]map[int32]*ChUnique{}
}

func (e *Estimator) UpdateWithKeys(time uint32, mhs []EstimatorMetricHash) {
	e.mu.Lock()
	defer e.mu.Unlock()
	ah, bh := e.createEstimatorsLocked(time)
	for _, mh := range mhs {
		updateEstimate(ah, mh)
		updateEstimate(bh, mh)
	}
}

func (e *Estimator) createEstimatorsLocked(time uint32) (map[int32]*ChUnique, map[int32]*ChUnique) {
	tp := time / estimatorWindow
	ah, ok := e.hour[tp]
	if !ok {
		ah = map[int32]*ChUnique{}
		e.hour[tp] = ah
	}
	stp := (time + estimatorWindow/2) / estimatorWindow
	bh, ok := e.halfHour[stp]
	if !ok {
		bh = map[int32]*ChUnique{}
		e.halfHour[stp] = bh
	}
	return ah, bh
}

func (e *Estimator) ReportHourCardinality(rng *rand.Rand, time uint32, miMap *MultiItemMap, usedMetrics map[int32]struct{}, aggregatorHostTag TagUnion, shardKey int32, replicaKey int32) {
	e.mu.Lock()
	defer e.mu.Unlock()

	ah, bh := e.createEstimatorsLocked(time)
	// Estimators overlap, we take linear combination to smooth value
	aWeight := 2 * float64(time%estimatorWindow) / float64(estimatorWindow)
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
		// we presume we have full view of metric, so those with "hash_by_tags" or "builtin" strategy will report wrong estimate
		key := AggKey((time/60)*60, format.BuiltinMetricIDAggHourCardinality, [format.MaxTags]int32{0, 0, 0, 0, k}, aggregatorHostTag.I, shardKey, replicaKey)
		item, _ := miMap.GetOrCreateMultiItem(key, nil, nil)
		item.Tail.AddValueCounterHost(rng, cardinality, 1, aggregatorHostTag)
	}
}

func (e *Estimator) GarbageCollect(oldestTime uint32) {
	// We repeat algorithm in createEstimatorsLocked for last timestamp we accept
	e.mu.Lock()
	defer e.mu.Unlock()

	tp := oldestTime / estimatorWindow
	stp := (oldestTime + estimatorWindow/2) / estimatorWindow
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
