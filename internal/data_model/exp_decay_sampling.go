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

const ExpDecaySamplingHalfLife = 45 * time.Second

type ExpDecaySampling struct {
	halfLife         time.Duration
	lastDecayAt      time.Time
	sampleMetricStat map[int32]map[uint64]*MetricsBucketStat
	mu               sync.Mutex
}

func NewExpDecaySampling(halfLife time.Duration) ExpDecaySampling {
	return ExpDecaySampling{
		halfLife:         halfLife,
		lastDecayAt:      time.Now(),
		sampleMetricStat: make(map[int32]map[uint64]*MetricsBucketStat),
	}
}

func (d *ExpDecaySampling) SetHalfLife(halfLife time.Duration) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.halfLife = halfLife
}

func (d *ExpDecaySampling) Apply(now time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	elapsed := now.Sub(d.lastDecayAt)
	d.lastDecayAt = now
	if elapsed <= 0 {
		return
	}
	factor := math.Exp2(-float64(elapsed) / float64(d.halfLife))
	for metric, hosts := range d.sampleMetricStat {
		for host, stat := range hosts {
			stat.Size = uint32(math.Ceil(float64(stat.Size) * factor))
			stat.Weight = math.Ceil(stat.Weight * factor)
			if stat.Size == 0 {
				delete(d.sampleMetricStat[metric], host)
				continue
			}
		}
	}
}

func (d *ExpDecaySampling) MergeMaxAndGet(dst map[int32]map[uint64]*MetricsBucketStat) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for metric, hosts := range dst {
		for host, stat := range hosts {
			srcMetric := d.sampleMetricStat[metric]
			if srcMetric == nil {
				srcMetric = map[uint64]*MetricsBucketStat{}
				d.sampleMetricStat[metric] = srcMetric
			}
			srcStat := srcMetric[host]
			if srcStat == nil {
				srcStat = &MetricsBucketStat{}
				srcMetric[host] = srcStat
				stat.Weight *= 2 // boost new items
			}
			srcStat.Weight = max(srcStat.Weight, stat.Weight)
			srcStat.Size = max(srcStat.Size, stat.Size)
		}
	}
	for metric, hosts := range d.sampleMetricStat {
		for host, size := range hosts {
			v := dst[metric]
			if v == nil {
				v = map[uint64]*MetricsBucketStat{}
				dst[metric] = v
			}
			v[host] = size
		}
	}
}

func (d *ExpDecaySampling) Get(dst map[int32]map[uint64]*MetricsBucketStat) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for metric, hosts := range d.sampleMetricStat {
		for host, size := range hosts {
			v := dst[metric]
			if v == nil {
				v = map[uint64]*MetricsBucketStat{}
				dst[metric] = v
			}
			v[host] = size
		}
	}
}
