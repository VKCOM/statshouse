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

// NB! It is tempting to move MetricMetaValue into SeriesBag
// (because QuerySeries always returns series with same metadata)
// but vector selector might match several metrics and QuerySeries will
// be invoked for each of them resulting in SeriesBag having different
// series. Besides that it is convient to have "append" method defined
// on SeriesBag, so you can build it lazily starting from SeriesBag{}.
type SeriesMeta struct {
	Tags     map[string]*SeriesTag // indexed by tag ID (canonical name)
	Name2Tag map[string]*SeriesTag // indexed by tag optional Name
	Metric   *format.MetricMetaValue
}

type SeriesTag struct {
	Index     int    // shifted by one, zero means not set
	ID        string // canonical name, always set
	Name      string // optional custom name
	Value     int32
	ValueSet  bool
	SValue    string
	SValueSet bool
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

func (b *SeriesBag) GetSMaxHostsAt(i int, h Handler) []string {
	if len(b.MaxHost) <= i {
		return nil
	}
	res := make([]string, len(b.MaxHost[i]))
	for j, id := range b.MaxHost[i] {
		if id != 0 {
			res[j] = h.GetHostName(id)
		}
	}
	return res
}

// Serializes into JSON of Prometheus format. Slow but only 20 lines long.
func (b *SeriesBag) MarshalJSON() ([]byte, error) {
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
		if len(v) == 0 {
			continue
		}
		var m map[string]string
		if tags := b.getTagsAt(i); len(tags) != 0 {
			m = make(map[string]string, len(tags))
			for _, tag := range tags {
				if !tag.SValueSet || tag.SValue == format.TagValueCodeZero || tag.Name == labelWhat {
					continue
				}
				if len(tag.Name) != 0 {
					m[tag.Name] = tag.SValue
				} else {
					m[tag.ID] = tag.SValue
				}
			}
		}
		s = append(s, series{M: m, V: v})
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
	b.Meta = appendAt(len(b.Data), b.Meta, s.Meta...)
	b.MaxHost = appendAt(len(b.Data), b.MaxHost, s.MaxHost...)
	b.Data = append(b.Data, s.Data...)
}

func (b *SeriesBag) appendSTagged(row *[]float64, stags map[string]string) {
	for id, v := range stags {
		b.setSTagAt(len(b.Data), id, v)
	}
	b.Data = append(b.Data, row)
}

func (b *SeriesBag) appendX(s SeriesBag, x ...int) {
	for _, i := range x {
		b.Meta = appendAt(len(b.Data), b.Meta, s.Meta[i])
		if i < len(s.MaxHost) {
			b.MaxHost = appendAt(len(b.Data), b.MaxHost, s.MaxHost[i])
		}
		b.Data = append(b.Data, s.Data[i])
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
	b.dropTag(labels.MetricName)
}

func (b *SeriesBag) dropTag(k string) {
	for _, m := range b.Meta {
		m.dropTag(k)
	}
}

func (b *SeriesBag) getTagAt(i int, k string) (SeriesTag, bool) {
	if i < 0 || len(b.Meta) <= i {
		return SeriesTag{}, false
	}
	return b.Meta[i].getTag(k)
}

func (b *SeriesBag) getTagsAt(i int) map[string]*SeriesTag {
	if i < len(b.Meta) {
		return b.Meta[i].Tags
	}
	return nil
}

func (b *SeriesBag) getTagsAtWithout(i int, tags []string) []string {
	if i < len(b.Meta) {
		return b.Meta[i].getTagsWithout(tags)
	}
	return nil
}

func (b *SeriesBag) group(ev *evaluator, without bool, by []string) ([]seriesGroup, error) {
	if len(by) == 0 && !without {
		return []seriesGroup{{bag: *b, maxHost: b.groupMaxHost()}}, nil
	}
	var (
		h              = fnv.New64()
		groups         = make(map[uint64]*seriesGroup, len(b.Meta))
		metricMismatch bool
	)
	for i, m := range b.Meta {
		sum, tags, err := b.hashAt(i, without, by, h)
		if err != nil {
			return nil, err
		}
		var (
			g  *seriesGroup
			ok bool
		)
		if g, ok = groups[sum]; !ok {
			g = &seriesGroup{
				hash: sum,
				bag:  SeriesBag{Time: b.Time},
				meta: SeriesMeta{Metric: m.Metric},
			}
			for _, k := range tags {
				if tag, ok := b.getTagAt(i, k); ok {
					g.meta.SetTag(tag)
				}
			}
			groups[sum] = g
		}
		if metricMismatch {
			b.stringifyAt(i, ev)
		} else if g.meta.Metric != nil && g.meta.Metric != m.Metric {
			g.bag.stringify(ev)
			metricMismatch = true
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

func (b *SeriesBag) groupWithout(ev *evaluator, tags ...string) ([]seriesGroup, error) {
	return b.group(ev, true, tags)
}

func (b *SeriesBag) hash(without bool, tags []string) (map[uint64]int, error) {
	var (
		h   = fnv.New64()
		res = make(map[uint64]int, len(b.Meta))
	)
	for i := range b.Meta {
		sum, _, err := b.hashAt(i, without, tags, h)
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

func (b *SeriesBag) hashAt(i int, without bool, by []string, h hash.Hash64) (uint64, []string, error) {
	var tags []string
	if without {
		tags = b.getTagsAtWithout(i, by)
	} else {
		tags = append(tags, by...)
	}
	sort.Strings(tags)
	buf := make([]byte, 4)
	for _, k := range tags {
		t, ok := b.getTagAt(i, k)
		if !ok {
			continue
		}
		_, err := h.Write([]byte(k))
		if err != nil {
			return 0, nil, err
		}
		if t.ValueSet {
			binary.LittleEndian.PutUint32(buf, uint32(t.Value))
			_, err = h.Write(buf)
		} else if t.SValueSet {
			_, err = h.Write([]byte(t.SValue))
		} else {
			err = fmt.Errorf("%q tag value isn't set", k)
		}
		if err != nil {
			return 0, nil, err
		}
	}
	return h.Sum64(), tags, nil
}

func (b *SeriesBag) histograms(ev *evaluator) ([]histogram, error) {
	groups, err := b.groupWithout(ev, labels.BucketLabel)
	if err != nil {
		return nil, err
	}
	var res []histogram
	for _, g := range groups {
		var s []bucket
		for i, m := range g.bag.Meta {
			if t, ok := m.getTag(labels.BucketLabel); ok {
				if t.ValueSet {
					s = append(s, bucket{i, prometheus.LexDecode(t.Value)})
				} else if t.SValueSet {
					var v float64
					v, err = strconv.ParseFloat(t.SValue, 32)
					if err == nil {
						s = append(s, bucket{i, float32(v)})
					}
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
	for _, m := range b.Meta {
		if len(m.Tags) != 0 {
			return false
		}
	}
	return true
}

func (b *SeriesBag) setSTag(id, value string) {
	b.setXTag(SeriesTag{ID: id, SValue: value, SValueSet: true})
}

func (b *SeriesBag) setTag(id string, value int32) {
	b.setXTag(SeriesTag{ID: id, Value: value, ValueSet: true})
}

func (b *SeriesBag) setXTag(t SeriesTag) {
	for i := range b.Meta {
		b.Meta[i].SetTag(t)
	}
}

func (b *SeriesBag) setSTagAt(i int, id, value string) {
	b.setTagAt(i, SeriesTag{ID: id, SValue: value, SValueSet: true})
}

func (b *SeriesBag) setTagAt(i int, t SeriesTag) {
	for j := len(b.Meta); j <= i; j++ {
		b.Meta = append(b.Meta, SeriesMeta{})
	}
	b.Meta[i].SetTag(t)
}

func (b *SeriesBag) stringify(ev *evaluator) {
	for _, m := range b.Meta {
		for _, v := range m.Tags {
			v.stringify(ev, m.Metric)
		}
	}
}

func (b *SeriesBag) stringifyAt(i int, ev *evaluator) {
	if i < 0 || len(b.Meta) <= i {
		return
	}
	m := b.Meta[i].Metric
	if m == nil {
		return
	}
	for _, v := range b.Meta[i].Tags {
		v.stringify(ev, m)
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
			s := b.MaxHost[j]
			if len(s) != 0 {
				b.MaxHost[j] = s[lo:hi]
			}
		}
	}
}

func (m *SeriesMeta) DropMetricName() {
	m.dropTag(labels.MetricName)
}

func (m *SeriesMeta) DropWhat() {
	m.dropTag(labelWhat)
}

func (m *SeriesMeta) GetMetricName() string {
	t, _ := m.getTag(labels.MetricName)
	return t.SValue
}

func (m *SeriesMeta) dropTag(k string) {
	if m.Tags == nil {
		return
	}
	if t, ok := m.Tags[k]; ok {
		if 0 < t.Index && t.Index <= format.MaxTags {
			delete(m.Name2Tag, t.Name)
			delete(m.Name2Tag, format.TagID(t.Index-1))
		}
		delete(m.Tags, k)
	} else if m.Name2Tag != nil {
		if t, ok := m.Name2Tag[k]; ok {
			// k == t.Name || k == format.TagID(t.Index-1)
			delete(m.Name2Tag, t.Name)
			delete(m.Name2Tag, format.TagIDLegacy(t.Index-1))
			delete(m.Tags, format.TagID(t.Index-1))
		}
	}
}

func (m *SeriesMeta) GetOffset() int64 {
	t, _ := m.getTag(labelOffset)
	return int64(t.Value)
}

func (m *SeriesMeta) GetTotal() int {
	t, _ := m.getTag(labelTotal)
	return int(t.Value)
}

func (m *SeriesMeta) GetWhat() string {
	t, _ := m.getTag(labelWhat)
	return t.SValue
}

func (m *SeriesMeta) SetTag(t SeriesTag) {
	if m.Tags == nil {
		m.Tags = map[string]*SeriesTag{t.ID: &t}
	} else {
		m.dropTag(t.ID)
		m.Tags[t.ID] = &t
	}
	if t.Index != 0 {
		if m.Name2Tag == nil {
			m.Name2Tag = map[string]*SeriesTag{}
		}
		if len(t.Name) != 0 {
			m.Name2Tag[t.Name] = &t
		}
		m.Name2Tag[format.TagIDLegacy(t.Index-1)] = &t
	}
}

func (m *SeriesMeta) getTag(k string) (SeriesTag, bool) {
	if m.Tags == nil {
		return SeriesTag{}, false
	}
	var t *SeriesTag
	if t = m.Tags[k]; t == nil && m.Name2Tag != nil {
		t = m.Name2Tag[k]
	}
	if t == nil {
		return SeriesTag{}, false
	}
	return *t, true
}

func (m *SeriesMeta) getTagsWithout(s []string) []string {
	if m.Tags == nil {
		return nil
	}
	var res []string
	for id, t := range m.Tags {
		var found bool
		for _, k := range s { // "s" is expected to be short, no need to build a map
			if len(k) == 0 {
				continue
			}
			if k == id || k == t.Name {
				found = true
				break
			}
		}
		if !found {
			res = append(res, id)
		}
	}
	return res
}

func (tag *SeriesTag) stringify(ev *evaluator, metric *format.MetricMetaValue) {
	if tag.SValueSet {
		return
	}
	var v string
	switch tag.ID {
	case labelOffset, labelTotal: // StatsHouse specific, string isn't needed
		return
	case LabelShard:
		v = strconv.FormatUint(uint64(tag.Value), 10)
	default:
		v = ev.getTagValue(metric, tag.ID, tag.Value)
	}
	tag.SValue = v
	tag.SValueSet = true
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
