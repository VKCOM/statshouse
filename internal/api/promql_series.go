// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"sort"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

type promSeries struct {
	time []int64
	data []float64
	tags map[string]string
	i    int
}

func (p *promSeries) Labels() labels.Labels {
	s := make(labels.Labels, 0, len(p.tags))
	for name, value := range p.tags {
		s = append(s, labels.Label{Name: name, Value: value})
	}
	sort.Sort(s)
	return s
}

func (p *promSeries) Iterator() chunkenc.Iterator {
	return p
}

func (p *promSeries) Next() bool {
	p.i++
	return p.i < len(p.time) && p.i < len(p.data)
}

func (p *promSeries) Seek(t int64) bool {
	for i, v := range p.time {
		if t <= v {
			p.i = i
			return true
		}
	}
	return false
}

func (p *promSeries) At() (int64, float64) {
	return p.time[p.i], p.data[p.i]
}

func (p *promSeries) Err() error {
	return nil
}
