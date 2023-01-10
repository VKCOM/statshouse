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

func newPromSeries(time []int64, data []float64, tags map[string]string) *promSeries {
	return &promSeries{time, data, tags, -1}
}

func (ss *promSeries) Labels() labels.Labels {
	s := make(labels.Labels, 0, len(ss.tags))
	for name, value := range ss.tags {
		s = append(s, labels.Label{Name: name, Value: value})
	}
	sort.Sort(s)
	return s
}

func (ss *promSeries) Iterator() chunkenc.Iterator {
	return ss
}

func (ss *promSeries) Next() bool {
	ss.i++
	return ss.i < len(ss.time) && ss.i < len(ss.data)
}

func (ss *promSeries) Seek(t int64) bool {
	for i, v := range ss.time {
		if t <= v {
			ss.i = i
			return true
		}
	}
	return false
}

func (ss *promSeries) At() (int64, float64) {
	return ss.time[ss.i], ss.data[ss.i]
}

func (ss *promSeries) Err() error {
	return nil
}
