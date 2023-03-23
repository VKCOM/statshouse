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

type SeriesBag struct {
	Time    []int64
	Data    []*[]float64
	Meta    []SeriesMeta
	MaxHost [][]int32
	Range   int64
}

type SeriesMeta struct {
	Tags   map[string]int32
	STags  map[string]string
	Metric *format.MetricMetaValue
}

type seriesGroup struct {
	hash    uint64
	meta    SeriesMeta
	maxHost []int32
	bag     SeriesBag
}

type histogram struct {
	group   seriesGroup
	buckets []bucket
}

type bucket struct {
	x  int     // index in series group
	le float32 // decoded "le" tag value
}

func (b *SeriesBag) GetSMaxHosts(i int, h Handler) []string {
	if len(b.MaxHost) <= i {
		return nil
	}
	res := make([]string, len(b.MaxHost[i]))
	for j, id := range b.MaxHost[i] {
		if id != 0 {
			res[j] = h.GetTagValue(id)
		}
	}
	return res
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
			if math.Float64bits((*row)[j]) != NilValueBits {
				v = append(v, [2]any{t, strconv.FormatFloat((*row)[j], 'f', -1, 64)})
			}
		}
		if len(v) != 0 {
			s = append(s, series{M: b.getSTags(i), V: v})
		}
	}
	return json.Marshal(s)
}

func (b *SeriesBag) String() string {
	return fmt.Sprintf("%dx%d", len(b.Data), len(b.Time))
}

func (b *SeriesBag) Type() parser.ValueType {
	return parser.ValueTypeMatrix
}

func (b *SeriesBag) append(s SeriesBag) {
	n := len(b.Data)
	b.Data = append(b.Data, s.Data...)
	b.Meta = appendAt(n, b.Meta, s.Meta...)
	b.MaxHost = appendAt(n, b.MaxHost, s.MaxHost...)
}

func (b *SeriesBag) appendSTagged(v *[]float64, stags map[string]string) {
	n := len(b.Data)
	b.Data = append(b.Data, v)
	b.Meta = appendAt(n, b.Meta, SeriesMeta{STags: stags})
}

func (b *SeriesBag) appendX(s SeriesBag, x ...int) {
	for _, i := range x {
		n := len(b.Data)
		b.Data = append(b.Data, s.Data[i])
		b.Meta = appendAt(n, b.Meta, s.Meta[i])
		if i < len(s.MaxHost) {
			b.MaxHost = appendAt(n, b.MaxHost, s.MaxHost[i])
		}
	}
}

func (b *SeriesBag) at(i int) SeriesBag {
	return SeriesBag{
		Time:    b.Time,
		Data:    safeSlice(b.Data, i, i+1),
		Meta:    safeSlice(b.Meta, i, i+1),
		MaxHost: safeSlice(b.MaxHost, i, i+1),
	}
}

func (b *SeriesBag) dropMetricName() {
	for _, m := range b.Meta {
		delete(m.STags, labels.MetricName)
	}
}

func (b *SeriesBag) getSTag(i int, t string) (v string, ok bool) {
	if len(b.Meta) <= i {
		return "", false
	}
	v, ok = b.Meta[i].getSTag(t)
	return v, ok
}

func (b *SeriesBag) getSTags(i int) map[string]string {
	if i < len(b.Meta) {
		return b.Meta[i].STags
	}
	return nil
}

func (b *SeriesBag) getTag(i int, t string) (v int32, ok bool) {
	if len(b.Meta) <= i {
		return 0, false
	}
	v, ok = b.Meta[i].getTag(t)
	return v, ok
}

func (b *SeriesBag) getTags(i int) map[string]int32 {
	if i < len(b.Meta) {
		return b.Meta[i].Tags
	}
	return nil
}

func (b *SeriesBag) group(without bool, tags []string) ([]seriesGroup, error) {
	if len(tags) == 0 && !without {
		return []seriesGroup{{bag: *b, maxHost: b.groupMaxHost()}}, nil
	}
	var (
		h      = fnv.New64()
		by     = b.tagGroupBy(without, tags)
		groups = make(map[uint64]*seriesGroup, len(b.Meta))
	)
	for i := range b.Meta {
		sum, s, err := b.hashAt(i, by, h)
		if err != nil {
			return nil, err
		}
		var (
			g  *seriesGroup
			ok bool
		)
		if g, ok = groups[sum]; !ok {
			g = &seriesGroup{hash: sum, bag: SeriesBag{Time: b.Time}}
			for _, tag := range s {
				if t, ok2 := b.getTag(i, tag); ok2 {
					g.meta.SetTag(tag, t)
				}
				if value, ok2 := b.getSTag(i, tag); ok2 {
					g.meta.SetSTag(tag, value)
				}
			}
			groups[sum] = g
		}
		g.bag.appendX(*b, i)
		h.Reset()
	}
	res := make([]seriesGroup, 0, len(groups))
	for _, g := range groups {
		g.maxHost = g.bag.groupMaxHost()
		res = append(res, *g)
	}
	return res, nil
}

func (b *SeriesBag) groupMaxHost() []int32 {
	if len(b.MaxHost) == 0 {
		return nil
	}
	if len(b.MaxHost) == 1 {
		return b.MaxHost[0]
	}
	var (
		i int
		s []int32
	)
	for ; i < len(b.MaxHost); i++ {
		if len(b.MaxHost[i]) != 0 {
			s = make([]int32, 0, len(b.Time))
			break
		}
	}
	if s == nil {
		return nil
	}
	for j := 0; j < len(b.Time); j++ {
		var (
			v = b.MaxHost[i][j]
			k = i + 1
		)
		for ; k < len(b.MaxHost); k++ {
			if k < len(b.MaxHost) && b.MaxHost[k][j] != 0 && b.MaxHost[k][j] != v {
				if v == 0 {
					v = b.MaxHost[k][j]
				} else {
					v = 0
					break
				}
			}
		}
		s = append(s, v)
	}
	return s
}

func (b *SeriesBag) groupWithout(tags ...string) ([]seriesGroup, error) {
	return b.group(true, tags)
}

func (b *SeriesBag) hash(without bool, tags []string) (map[uint64]int, error) {
	var (
		h   = fnv.New64()
		by  = b.tagGroupBy(without, tags)
		res = make(map[uint64]int, len(b.Meta))
	)
	for i := range b.Meta {
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
	s := make([]string, 0, len(b.Meta[i].Tags)+len(b.Meta[i].STags))
	for name := range b.getTags(i) {
		if by == nil || by[name] {
			s = append(s, name)
		}
	}
	for name := range b.getSTags(i) {
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
		if value, ok := b.getTag(i, name); ok {
			binary.LittleEndian.PutUint32(valueID, uint32(value))
			_, err = h.Write(valueID)
		} else {
			_, err = h.Write([]byte(b.Meta[i].STags[name]))
		}
		if err != nil {
			return 0, nil, err
		}
	}
	return h.Sum64(), s, nil
}

func (b *SeriesBag) histograms() ([]histogram, error) {
	groups, err := b.groupWithout(labels.BucketLabel)
	if err != nil {
		return nil, err
	}
	var res []histogram
	for _, g := range groups {
		var s []bucket
		for i, meta := range g.bag.Meta {
			if valueID, ok := meta.getTag(labels.BucketLabel); ok {
				s = append(s, bucket{i, prometheus.LexDecode(valueID)})
			} else if value, ok2 := meta.getSTag(labels.BucketLabel); ok2 {
				var v float64
				v, err = strconv.ParseFloat(value, 32)
				if err == nil {
					s = append(s, bucket{i, float32(v)})
				}
			}
		}
		if len(s) != 0 {
			sort.Slice(s, func(i, j int) bool { return s[i].le < s[j].le })
			res = append(res, histogram{g, s})
		}
	}
	return res, nil
}

func (b *SeriesBag) scalar() bool {
	if len(b.Data) != 1 {
		return false
	}
	for _, meta := range b.Meta {
		if len(meta.Tags) != 0 {
			return false
		}
		if len(meta.STags) != 0 {
			return false
		}
	}
	return true
}

func (b *SeriesBag) setSTag(i int, t string, v string) {
	for j := len(b.Meta); j <= i; j++ {
		b.Meta = append(b.Meta, SeriesMeta{})
	}
	b.Meta[i].SetSTag(t, v)
}

func (b *SeriesBag) setTag(i int, t string, v int32) {
	for j := len(b.Meta); j <= i; j++ {
		b.Meta = append(b.Meta, SeriesMeta{})
	}
	b.Meta[i].SetTag(t, v)
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
			for _, meta := range b.Meta {
				for tag := range meta.Tags {
					notBy[tag] = !by[tag]
				}
				for tag := range meta.STags {
					notBy[tag] = !by[tag]
				}
			}
			by = notBy
		}
	}
	return by
}

func (b *SeriesBag) tagOffset(offset int64) {
	for _, meta := range b.Meta {
		meta.SetTag(labelOffset, int32(offset))
	}
}

func (b *SeriesBag) tagTotal(total int) {
	for i := range b.Meta {
		b.Meta[i].SetTag(labelTotal, int32(total))
	}
}

func (b *SeriesBag) trim(start, end int64) {
	var (
		lo int
		hi = len(b.Time)
	)
	for lo < len(b.Time) && b.Time[lo] < start {
		lo++
	}
	for lo < hi-1 && end <= b.Time[hi-1] {
		hi--
	}
	if lo != 0 || hi != len(b.Time) {
		b.Time = b.Time[lo:hi]
		for j := 0; j < len(b.Data); j++ {
			s := (*b.Data[j])[lo:hi]
			b.Data[j] = &s
		}
		for j := 0; j < len(b.MaxHost); j++ {
			b.MaxHost[j] = b.MaxHost[j][lo:hi]
		}
	}
}

func (m *SeriesMeta) DropMetricName() {
	delete(m.STags, labels.MetricName)
}

func (m *SeriesMeta) GetMetricName() string {
	if m.STags == nil {
		return ""
	}
	return m.STags[labels.MetricName]
}

func (m *SeriesMeta) GetOffset() int64 {
	if m.Tags == nil {
		return 0
	}
	return int64(m.Tags[labelOffset])
}

func (m *SeriesMeta) GetTotal() int {
	if m.Tags == nil {
		return 0
	}
	return int(m.Tags[labelTotal])
}

func (m *SeriesMeta) SetSTag(name string, value string) {
	if m.STags != nil {
		m.STags[name] = value
	} else {
		m.STags = map[string]string{name: value}
	}
}

func (m *SeriesMeta) SetTag(name string, valueID int32) {
	if m.Tags != nil {
		m.Tags[name] = valueID
	} else {
		m.Tags = map[string]int32{name: valueID}
	}
}

func (m *SeriesMeta) getSTag(name string) (v string, ok bool) {
	if m.STags == nil {
		return "", false
	}
	v, ok = m.STags[name]
	return v, ok
}

func (m *SeriesMeta) getTag(name string) (v int32, ok bool) {
	if m.Tags == nil {
		return 0, false
	}
	v, ok = m.Tags[name]
	return v, ok
}

func (g *seriesGroup) at(i int) SeriesBag {
	return SeriesBag{
		Time:    g.bag.Time,
		Data:    []*[]float64{g.bag.Data[i]},
		Meta:    appendAt(0, nil, g.meta),
		MaxHost: appendAt(0, nil, g.maxHost),
	}
}

func appendAt[T any](n int, dst []T, src ...T) []T {
	if len(src) == 0 {
		return dst
	}
	for i := len(dst); i < n; i++ {
		var t T
		dst = append(dst, t)
	}
	return append(dst, src...)
}

func safeSlice[T any](s []T, i, j int) []T {
	if s == nil {
		return nil
	}
	if i < len(s) {
		if len(s) < j {
			j = len(s)

		}
		return s[i:j]
	}
	return nil
}
