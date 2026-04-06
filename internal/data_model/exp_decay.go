// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package data_model

import (
	"math"
	"sync"
	"time"
)

const ExpDecayHalfLife = 10 * time.Second

type ExpDecay struct {
	halfLife    time.Duration
	lastDecayAt time.Time
	values      map[int32]float64
	mu          sync.RWMutex
}

func NewExpDecay(halfLife time.Duration) *ExpDecay {
	return &ExpDecay{
		halfLife: halfLife,
		values:   make(map[int32]float64),
	}
}

func (d *ExpDecay) SetHalfLife(halfLife time.Duration) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.halfLife = halfLife
}

func (d *ExpDecay) Apply(now time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.values) == 0 {
		d.lastDecayAt = now
		return
	}
	if d.lastDecayAt.IsZero() {
		d.lastDecayAt = now
		return
	}
	elapsed := now.Sub(d.lastDecayAt)
	if elapsed <= 0 {
		return
	}
	factor := math.Exp2(-float64(elapsed) / float64(d.halfLife))
	for key, value := range d.values {
		value *= factor
		if value < 1 {
			delete(d.values, key)
			continue
		}
		d.values[key] = value
	}
	d.lastDecayAt = now
}

func (d *ExpDecay) Get(key int32) float64 {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.values[key]
}

func (d *ExpDecay) MergeMax(key int32, value float64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	v := max(d.values[key], value)
	if v < 1 {
		delete(d.values, key)
		return
	}
	d.values[key] = v
}
