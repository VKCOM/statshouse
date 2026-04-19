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

const ExpDecayMetricsHalfLife = 45 * time.Second

type ExpDecayMetrics struct {
	halfLife           time.Duration
	lastDecayAt        time.Time
	originalMetricSize map[int32]map[TagUnion]uint32
	mu                 sync.Mutex
}

func NewExpDecayMetrics(halfLife time.Duration) ExpDecayMetrics {
	return ExpDecayMetrics{
		halfLife:           halfLife,
		lastDecayAt:        time.Now(),
		originalMetricSize: make(map[int32]map[TagUnion]uint32),
	}
}

func (d *ExpDecayMetrics) SetHalfLife(halfLife time.Duration) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.halfLife = halfLife
}

func (d *ExpDecayMetrics) Apply(now time.Time, reportF func(metricID int32, host TagUnion, size uint32)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	elapsed := now.Sub(d.lastDecayAt)
	d.lastDecayAt = now
	if elapsed <= 0 {
		return
	}
	factor := math.Exp2(-float64(elapsed) / float64(d.halfLife))
	for metric, hosts := range d.originalMetricSize {
		for host, size := range hosts {
			size = uint32(math.Ceil(float64(size) * factor))
			if size == 0 {
				delete(d.originalMetricSize[metric], host)
				continue
			}
			d.originalMetricSize[metric][host] = size
			reportF(metric, host, size)
		}
	}
}

func (d *ExpDecayMetrics) MergeMaxAndGet(dst map[int32]map[TagUnion]uint32) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for metric, hosts := range dst {
		for host, size := range hosts {
			src := d.originalMetricSize[metric]
			if src == nil {
				src = map[TagUnion]uint32{}
				d.originalMetricSize[metric] = src
			}
			vmax := max(src[host], size)
			src[host] = vmax
			dst[metric][host] = vmax
		}
	}
}
