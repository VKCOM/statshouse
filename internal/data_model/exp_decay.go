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
	values      map[int32]uint32
	mu          sync.RWMutex
}

func NewExpDecay(halfLife time.Duration) ExpDecay {
	return ExpDecay{
		halfLife:    halfLife,
		lastDecayAt: time.Now(),
		values:      make(map[int32]uint32),
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
	elapsed := now.Sub(d.lastDecayAt)
	d.lastDecayAt = now
	if elapsed <= 0 {
		return
	}
	factor := math.Exp2(-float64(elapsed) / float64(d.halfLife))
	for key, value := range d.values {
		value = uint32(math.Ceil(float64(value) * factor))
		if value < 1 {
			delete(d.values, key)
			continue
		}
		d.values[key] = value
	}
}

func (d *ExpDecay) Get(dst map[int32]uint32) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	for key, value := range d.values {
		dst[key] = value
	}
}

func (d *ExpDecay) MergeMax(f func(func(k int32, v uint32))) {
	d.mu.Lock()
	defer d.mu.Unlock()
	f(func(k int32, v uint32) {
		vmax := max(d.values[k], v)
		if vmax < 1 {
			delete(d.values, k)
			return
		}
		d.values[k] = vmax
	})
}
