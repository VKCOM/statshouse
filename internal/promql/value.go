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
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/promql/parser"
	"github.com/vkcom/statshouse/internal/receiver/prometheus"
)

type TagValue struct {
	ID   int32
	Meta *format.MetricMetaValue
}

type SeriesBag struct {
	Time     []int64
	Data     []*[]float64
	Tags     []map[string]TagValue
	STags    []map[string]string
	MaxHost  [][]int32
	SMaxHost [][]string
	Start    int64
	Range    int64
}

type seriesGroup struct {
	hash  uint64
	tags  map[string]TagValue
	stags map[string]string
	bag   *SeriesBag
}
type bucket struct {
	le float32
	g  seriesGroup
}

func (b *SeriesBag) append(s ...*SeriesBag) {
	for _, v := range s {
		n := len(b.Data)
		b.Data = append(b.Data, v.Data...)
		b.Tags = bagAppend(n, b.Tags, v.Tags...)
		b.STags = bagAppend(n, b.STags, v.STags...)
		b.MaxHost = bagAppend(n, b.MaxHost, v.MaxHost...)
	}
}

func (b *SeriesBag) appendX(s *SeriesBag, x ...int) {
	for _, i := range x {
		n := len(b.Data)
		b.Data = append(b.Data, s.Data[i])
		b.Tags = bagAppend(n, b.Tags, s.Tags[i])
		b.STags = bagAppend(n, b.STags, s.STags[i])
		if i < len(s.MaxHost) {
			b.MaxHost = bagAppend(n, b.MaxHost, s.MaxHost[i])
		}
	}
}

func (b *SeriesBag) appendSTagged(v *[]float64, stags map[string]string) {
	n := len(b.Data)
	b.Data = append(b.Data, v)
	b.STags = bagAppend(n, b.STags, stags)
}

func (b *SeriesBag) GetTags(i int) map[string]TagValue {
	if i < len(b.Tags) {
		return b.Tags[i]
	}
	return nil
}

func (b *SeriesBag) GetSTags(i int) map[string]string {
	if i < len(b.STags) {
		return b.STags[i]
	}
	return nil
}

func (b *SeriesBag) getTag(i int, t string) (v TagValue, ok bool) {
	if len(b.Tags) <= i {
		return TagValue{}, false
	}
	v, ok = b.Tags[i][t]
	return v, ok
}

func (b *SeriesBag) getSTag(i int, t string) (v string, ok bool) {
	if len(b.STags) <= i {
		return "", false
	}
	v, ok = b.STags[i][t]
	return v, ok
}

func (b *SeriesBag) setTag(i int, t string, v TagValue) {
	for j := len(b.Tags); j <= i; j++ {
		b.Tags = append(b.Tags, nil)
	}
	if b.Tags[i] != nil {
		b.Tags[i][t] = v
	} else {
		b.Tags[i] = map[string]TagValue{t: v}
	}
}

func (b *SeriesBag) setSTag(i int, t string, v string) {
	for j := len(b.STags); j <= i; j++ {
		b.STags = append(b.STags, nil)
	}
	if b.STags[i] != nil {
		b.STags[i][t] = v
	} else {
		b.STags[i] = map[string]string{t: v}
	}
}

func (b *SeriesBag) GetSMaxHosts(i int) []string {
	if i < len(b.SMaxHost) {
		return b.SMaxHost[i]
	}
	return nil
}

func (b *SeriesBag) Type() parser.ValueType {
	return parser.ValueTypeMatrix
}

func (b *SeriesBag) String() string {
	return fmt.Sprintf("%dx%d", len(b.Data), len(b.Time))
}

func (b *SeriesBag) MarshalJSON() ([]byte, error) { // slow but only 20 lines long
	type series struct {
		M map[string]string `json:"metric,omitempty"`
		V [][2]any          `json:"values,omitempty"`
	}
	s := make([]series, 0, len(b.Data))
	for i, row := range b.Data {
		v := make([][2]any, 0, len(b.Time))
		for j, t := range b.Time {
			if t < b.Start {
				continue
			}
			if math.Float64bits((*row)[j]) != NilValueBits {
				v = append(v, [2]any{t, strconv.FormatFloat((*row)[j], 'f', -1, 64)})
			}
		}
		if len(v) != 0 {
			s = append(s, series{M: b.GetSTags(i), V: v})
		}
	}
	return json.Marshal(s)
}

func (b *SeriesBag) histogram() ([]bucket, error) {
	s, err := b.groupBy("le")
	if err != nil || len(s) == 0 {
		return nil, err
	}
	var res []bucket
	for _, g := range s {
		res = append(res, bucket{prometheus.LexDecode(g.tags["le"].ID), g})
	}
	sort.Slice(res, func(i, j int) bool { return res[i].le < res[j].le })
	return res, nil
}

func (b *SeriesBag) groupBy(tags ...string) ([]seriesGroup, error) {
	return b.group(false, tags)
}

func (b *SeriesBag) group(without bool, tags []string) ([]seriesGroup, error) {
	if len(tags) == 0 && !without {
		return []seriesGroup{{bag: b}}, nil
	}
	var (
		h      = fnv.New64()
		by     = b.tagGroupBy(without, tags)
		groups = make(map[uint64]*seriesGroup, len(b.Tags))
	)
	for i := range b.Tags {
		sum, s, err := b.hashAt(i, by, h)
		if err != nil {
			return nil, err
		}
		var g *SeriesBag
		if v, ok := groups[sum]; !ok {
			g = &SeriesBag{Time: b.Time}
			v = &seriesGroup{
				hash:  sum,
				tags:  make(map[string]TagValue),
				stags: make(map[string]string),
				bag:   g,
			}
			for _, tag := range s {
				if t, ok2 := b.Tags[i][tag]; ok2 {
					v.tags[tag] = t
				}
				if value, ok2 := b.STags[i][tag]; ok2 {
					v.stags[tag] = value
				}
			}
			groups[sum] = v
		} else {
			g = v.bag
		}
		g.appendX(b, i)
		h.Reset()
	}
	res := make([]seriesGroup, 0, len(groups))
	for _, g := range groups {
		res = append(res, *g)
	}
	return res, nil
}

func (b *SeriesBag) hashWithout(tags ...string) (map[uint64]int, error) {
	return b.hash(true, tags)
}

func (b *SeriesBag) hash(without bool, tags []string) (map[uint64]int, error) {
	var (
		h   = fnv.New64()
		by  = b.tagGroupBy(without, tags)
		res = make(map[uint64]int, len(b.Tags))
	)
	for i := range b.Tags {
		sum, _, err := b.hashAt(i, by, h)
		if err != nil {
			return nil, err
		}
		if _, ok := res[sum]; ok {
			return nil, fmt.Errorf("label set match multiple series")
		}
		res[sum] = i
		h.Reset()
	}
	return res, nil
}

func (b *SeriesBag) hashAt(i int, by map[string]bool, h hash.Hash64) (uint64, []string, error) {
	s := make([]string, 0, len(b.Tags[i])+len(b.STags[i]))
	for name := range b.Tags[i] {
		if by == nil || by[name] {
			s = append(s, name)
		}
	}
	for name := range b.STags[i] {
		if by == nil || by[name] {
			s = append(s, name)
		}
	}
	sort.Strings(s)
	valueID := make([]byte, 4)
	for _, name := range s {
		_, err := h.Write([]byte(name))
		if err != nil {
			return 0, nil, err
		}
		if value, ok := b.Tags[i][name]; ok {
			binary.LittleEndian.PutUint32(valueID, uint32(value.ID))
			_, err = h.Write(valueID)
		} else {
			_, err = h.Write([]byte(b.STags[i][name]))
		}
		if err != nil {
			return 0, nil, err
		}
	}
	return h.Sum64(), s, nil
}

func (b *SeriesBag) tagGroupBy(without bool, tags []string) map[string]bool {
	var by map[string]bool
	if len(tags) != 0 {
		by = make(map[string]bool, len(tags))
		for _, tag := range tags {
			by[tag] = true
		}
		if without {
			notBy := make(map[string]bool)
			for i := range b.Tags {
				for tag := range b.Tags[i] {
					notBy[tag] = !by[tag]
				}
				for tag := range b.STags[i] {
					notBy[tag] = !by[tag]
				}
			}
			by = notBy
		}
	}
	return by
}

func (b *SeriesBag) dropMetricName() {
	for i := range b.STags {
		delete(b.STags[i], labels.MetricName)
	}
}

func (b *SeriesBag) scalar() bool {
	if len(b.Data) != 1 {
		return false
	}
	for _, tags := range b.Tags {
		if len(tags) != 0 {
			return false
		}
	}
	for _, stags := range b.STags {
		if len(stags) != 0 {
			return false
		}
	}
	return true
}

func (g *seriesGroup) at(i int) *SeriesBag {
	return &SeriesBag{
		Time:  g.bag.Time,
		Data:  []*[]float64{g.bag.Data[i]},
		Tags:  bagAppend(0, nil, g.tags),
		STags: bagAppend(0, nil, g.stags),
	}
}

func bagAppend[T any](n int, dst []T, src ...T) []T {
	if len(src) == 0 {
		return dst
	}
	for i := len(dst); i < n; i++ {
		var t T
		dst = append(dst, t)
	}
	return append(dst, src...)
}
