// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package promql

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash"
	"hash/fnv"
	"math"
	"sort"
	"strconv"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/vkcom/statshouse/internal/promql/parser"
)

type SeriesBag struct {
	Time  []int64
	Data  []*[]float64
	Tags  []map[string]int32
	STags []map[string]string
	Start int64
	Range int64
}

type seriesGroup struct {
	hash  uint64
	tags  map[string]int32
	stags map[string]string
	bag   SeriesBag
}

func (bag *SeriesBag) Type() parser.ValueType {
	return parser.ValueTypeMatrix
}

func (bag *SeriesBag) String() string {
	return fmt.Sprintf("%dx%d", len(bag.Data), len(bag.Time))
}

func (bag *SeriesBag) MarshalJSON() ([]byte, error) {
	type series struct {
		M map[string]string `json:"metric,omitempty"`
		V [][2]any          `json:"values,omitempty"`
	}
	s := make([]series, 0, len(bag.Data))
	for i, row := range bag.Data {
		v := make([][2]any, 0, len(bag.Time))
		for j, t := range bag.Time {
			if t < bag.Start {
				continue
			}
			if math.Float64bits((*row)[j]) != NilValueBits {
				v = append(v, [2]any{t, strconv.FormatFloat((*row)[j], 'f', -1, 64)})
			}
		}
		if len(v) != 0 {
			s = append(s, series{M: bag.STags[i], V: v})
		}
	}
	return json.Marshal(s)
}

func (bag *SeriesBag) group(tags []string, without bool) ([]seriesGroup, error) {
	if len(tags) == 0 && !without {
		return []seriesGroup{{bag: *bag}}, nil
	}
	var by map[string]bool
	if len(tags) != 0 {
		by = make(map[string]bool, len(tags))
		for _, tag := range tags {
			by[tag] = true
		}
		if without {
			notBy := make(map[string]bool)
			for i := range bag.Tags {
				for tag := range bag.Tags[i] {
					notBy[tag] = !by[tag]
				}
				for tag := range bag.STags[i] {
					notBy[tag] = !by[tag]
				}
			}
			by = notBy
		}
	}
	var (
		h      = fnv.New64()
		groups = make(map[uint64]*seriesGroup, len(bag.Tags))
	)
	for i := range bag.Tags {
		sum, s, err := bag.hashTagsAt(i, by, h)
		if err != nil {
			return nil, err
		}
		g := groups[sum]
		if g == nil {
			g = &seriesGroup{
				hash:  sum,
				tags:  make(map[string]int32),
				stags: make(map[string]string),
				bag:   SeriesBag{Time: bag.Time},
			}
			for _, tag := range s {
				if valueID, ok := bag.Tags[i][tag]; ok {
					g.tags[tag] = valueID
				}
				if value, ok := bag.STags[i][tag]; ok {
					g.stags[tag] = value
				}
			}
			groups[sum] = g
		}
		g.bag.Data = append(g.bag.Data, bag.Data[i])
		g.bag.Tags = append(g.bag.Tags, bag.Tags[i])
		g.bag.STags = append(g.bag.STags, bag.STags[i])
		h.Reset()
	}
	res := make([]seriesGroup, 0, len(groups))
	for _, g := range groups {
		res = append(res, *g)
	}
	return res, nil
}

func (bag *SeriesBag) hashTags(tags []string, out bool) (map[uint64]int, error) {
	var by map[string]bool
	if len(tags) != 0 {
		by = make(map[string]bool, len(tags))
		for _, tag := range tags {
			by[tag] = !out
		}
	}
	var (
		h   = fnv.New64()
		res = make(map[uint64]int, len(bag.Tags))
	)
	for i := range bag.Tags {
		sum, _, err := bag.hashTagsAt(i, by, h)
		if err != nil {
			return nil, err
		}
		if _, ok := res[sum]; ok {
			// should not happen
			return nil, fmt.Errorf("hash collision")
		}
		res[sum] = i
		h.Reset()
	}
	return res, nil
}

func (bag *SeriesBag) hashTagsAt(i int, by map[string]bool, h hash.Hash64) (uint64, []string, error) {
	s := make([]string, 0, len(bag.Tags[i])+len(bag.STags[i]))
	for tag := range bag.Tags[i] {
		if by == nil || by[tag] {
			s = append(s, tag)
		}
	}
	for tag := range bag.STags[i] {
		if by == nil || by[tag] {
			s = append(s, tag)
		}
	}
	sort.Strings(s)
	tagValueID := make([]byte, 4)
	for _, tag := range s {
		_, err := h.Write([]byte(tag))
		if err != nil {
			return 0, nil, err
		}
		if id, ok := bag.Tags[i][tag]; ok {
			binary.LittleEndian.PutUint32(tagValueID, uint32(id))
			_, err = h.Write(tagValueID)
		} else {
			_, err = h.Write([]byte(bag.STags[i][tag]))
		}
		if err != nil {
			return 0, nil, err
		}
	}
	return h.Sum64(), s, nil
}

func (bag *SeriesBag) dropMetricName() {
	for i := range bag.Data {
		delete(bag.STags[i], labels.MetricName)
	}
}

func (bag *SeriesBag) scalar() bool {
	if len(bag.Data) != 1 {
		return false
	}
	for _, tags := range bag.Tags {
		if len(tags) != 0 {
			return false
		}
	}
	for _, stags := range bag.STags {
		if len(stags) != 0 {
			return false
		}
	}
	return true
}
