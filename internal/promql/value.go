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
	"hash/fnv"
	"math"
	"sort"
	"strconv"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/promql/parser"
	"github.com/vkcom/statshouse/internal/receiver/prometheus"
)

type TimeSeries struct {
	Time   []int64
	Series Series
}

type Series struct {
	Data []SeriesData
	Meta SeriesMeta
}

type SeriesData struct {
	Values  *[]float64
	MaxHost []int32
	Tags    SeriesTags
	Offset  int64
	What    int
}

type SeriesMeta struct {
	Metric *format.MetricMetaValue
	What   int
	Total  int
	STags  map[string]int
}

type SeriesTags struct {
	ID2Tag       map[string]*SeriesTag // indexed by tag ID (canonical name)
	Name2Tag     map[string]*SeriesTag // indexed by tag optional Name
	hashSum      uint64
	hashSumValid bool
}

type SeriesTag struct {
	Metric      *format.MetricMetaValue
	Index       int    // shifted by "SeriesTagIndexOffset", zero means not set
	ID          string // canonical name, always set
	Name        string // optional custom name
	Value       int32
	SValue      string
	stringified bool
}

const SeriesTagIndexOffset = 2

type seriesGroup struct {
	Series
	tags SeriesTags
	hash uint64
}

type histogram struct {
	group   seriesGroup
	buckets []bucket
}

type bucket struct {
	x  int     // index in series group
	le float32 // decoded "le" tag value
}

type hashOptions struct {
	on    bool
	tags  []string
	stags map[string]int
}

func (ss *Series) AddTagAt(i int, t *SeriesTag) {
	ss.Data[i].Tags.add(t, &ss.Meta)
}

func (ss *Series) removeTag(t string) {
	for _, data := range ss.Data {
		data.Tags.remove(t)
	}
}

func (ss *Series) removeMetricName() {
	ss.removeTag(labels.MetricName)
}

func (ss *Series) append(s Series) {
	ss.Meta = mergeSeriesMeta(ss.Meta, s.Meta)
	ss.Data = append(ss.Data, s.Data...)
}

func (ss *Series) appendX(s Series, x ...int) {
	ss.Meta = mergeSeriesMeta(ss.Meta, s.Meta)
	for _, i := range x {
		ss.Data = append(ss.Data, s.Data[i])
	}
}

func (ss *Series) group(ev *evaluator, opt hashOptions) ([]seriesGroup, error) {
	if len(opt.tags) == 0 && !opt.on {
		return []seriesGroup{{Series: *ss}}, nil
	}
	groups := make(map[uint64]*seriesGroup, len(ss.Data))
	for i := range ss.Data {
		sum, tags, err := ss.Data[i].Tags.hash(ev, opt, true)
		if err != nil {
			return nil, err
		}
		var (
			g  *seriesGroup
			ok bool
		)
		if g, ok = groups[sum]; !ok {
			g = &seriesGroup{
				Series: ev.newSeries(0),
				hash:   sum,
			}
			for _, v := range tags {
				if tag, ok := ss.Data[i].Tags.get(v); ok {
					g.tags.add(tag, &g.Meta)
				}
			}
			groups[sum] = g
		}
		g.appendX(*ss, i)
	}
	res := make([]seriesGroup, 0, len(groups))
	for _, g := range groups {
		res = append(res, *g)
	}
	return res, nil
}

func (ss *Series) groupMaxHost(ev *evaluator) []int32 {
	if len(ss.Data) == 0 {
		return nil
	}
	if len(ss.Data) == 1 {
		return ss.Data[0].MaxHost
	}
	var (
		i int
		s []int32
		t = ev.time()
	)
	for ; i < len(ss.Data); i++ {
		if len(ss.Data[i].MaxHost) != 0 {
			s = make([]int32, 0, len(t))
			break
		}
	}
	if s == nil {
		return nil
	}
	for j := 0; j < len(t); j++ {
		var (
			v = ss.Data[i].MaxHost[j]
			k = i + 1
		)
		for ; k < len(ss.Data); k++ {
			if k < len(ss.Data) && ss.Data[k].MaxHost[j] != 0 && ss.Data[k].MaxHost[j] != v {
				if v == 0 {
					v = ss.Data[k].MaxHost[j]
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

func (ss *Series) hash(ev *evaluator, opt hashOptions) (map[uint64]int, error) {
	res := make(map[uint64]int, len(ss.Data))
	for i := range ss.Data {
		sum, _, err := ss.Data[i].Tags.hash(ev, opt, false)
		if err != nil {
			return nil, err
		}
		if _, ok := res[sum]; ok {
			return nil, fmt.Errorf("label set match multiple series")
		}
		res[sum] = i
	}
	return res, nil
}

func (ss *Series) histograms(ev *evaluator) ([]histogram, error) {
	groups, err := ss.group(ev, hashOptions{
		tags: []string{labels.BucketLabel},
		on:   false, // group excluding BucketLabel
	})
	if err != nil {
		return nil, err
	}
	var res []histogram
	for _, g := range groups {
		var s []bucket
		for i, m := range g.Data {
			if t, ok := m.Tags.get(labels.BucketLabel); ok {
				if t.stringified {
					var v float64
					v, err = strconv.ParseFloat(t.SValue, 32)
					if err == nil {
						s = append(s, bucket{i, float32(v)})
					}
				} else {
					s = append(s, bucket{i, prometheus.LexDecode(t.Value)})
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

func (ss *Series) scalar() bool {
	if len(ss.Data) != 1 {
		return false
	}
	for _, m := range ss.Data {
		if len(m.Tags.ID2Tag) != 0 {
			return false
		}
	}
	return true
}

func (ss *Series) weight(ev *evaluator) []float64 {
	var (
		w      = make([]float64, len(ss.Data))
		nodecN int // number of non-decreasing series
	)
	for i, s := range ss.Data {
		var (
			j     int
			acc   float64
			prev  = -math.MaxFloat64
			nodec = true // non-decreasing
		)
		for _, lod := range ev.t.LODs {
			for m := 0; m < lod.Len; m++ {
				k := j + m
				if ev.t.Time[k] < ev.t.Start {
					continue // skip points before requested interval start
				}
				if ev.t.End <= ev.t.Time[k] {
					break // discard points after requested interval end
				}
				v := (*s.Values)[k]
				if !math.IsNaN(v) {
					acc += v * v * float64(lod.Step)
					if v < prev {
						nodec = false
					}
					prev = v
				}
			}
			j += lod.Len
		}
		w[i] = acc
		if nodec {
			nodecN++
		}
	}
	if nodecN == len(w) {
		// all series are non-decreasing, weight is a last value
		for i, s := range ss.Data {
			last := -math.MaxFloat64
			for i := len(*s.Values); i != 0; i-- {
				v := (*s.Values)[i-1]
				if !math.IsNaN(v) {
					last = v
					break
				}
			}
			w[i] = last
		}
	}
	return w
}

func (g *seriesGroup) at(i int) Series {
	res := Series{
		Data: g.Data[i : i+1],
		Meta: g.Meta,
	}
	res.Data[0].Tags = g.tags
	return res
}

func (t *SeriesTag) stringify(ev *evaluator) {
	if t.stringified {
		return
	}
	if len(t.SValue) != 0 {
		t.stringified = true
		return
	}
	var v string
	switch t.ID {
	case LabelShard:
		v = strconv.FormatUint(uint64(t.Value), 10)
	default:
		v = ev.h.GetTagValue(TagValueQuery{
			Version:    ev.opt.Version,
			Metric:     t.Metric,
			TagID:      t.ID,
			TagValueID: t.Value,
		})
	}
	t.SValue = v
	t.stringified = true
}

func (ts *SeriesTags) add(t *SeriesTag, m *SeriesMeta) {
	if ts.ID2Tag == nil {
		ts.ID2Tag = map[string]*SeriesTag{t.ID: t}
	} else {
		ts.remove(t.ID)
		ts.ID2Tag[t.ID] = t
	}
	if t.Index != 0 {
		if ts.Name2Tag == nil {
			ts.Name2Tag = map[string]*SeriesTag{}
		}
		if len(t.Name) != 0 {
			ts.Name2Tag[t.Name] = t
		}
		if _, id, ok := decodeTagIndexLegacy(t.Index); ok {
			ts.Name2Tag[id] = t
		}
	}
	if len(t.SValue) != 0 {
		if m.STags == nil {
			m.STags = make(map[string]int)
		}
		m.STags[t.ID]++
		t.stringified = true
	}
	ts.hashSumValid = false // tags changed, previously computed hash sum is no longer valid
}

func (ts *SeriesTags) get(t string) (*SeriesTag, bool) {
	if ts.ID2Tag == nil {
		return nil, false
	}
	var v *SeriesTag
	if v = ts.ID2Tag[t]; v == nil && ts.Name2Tag != nil {
		v = ts.Name2Tag[t]
	}
	if v == nil {
		return nil, false
	}
	return v, true
}

func (ts *SeriesTags) gets(ev *evaluator, t string) (*SeriesTag, bool) {
	if tag, ok := ts.get(t); ok {
		tag.stringify(ev)
		ts.hashSumValid = false
		return tag, true
	}
	return nil, false
}

func (ts *SeriesTags) remove(t string) {
	if ts.ID2Tag == nil {
		return
	}
	v := ts.ID2Tag[t]
	if v == nil && ts.Name2Tag != nil {
		v = ts.Name2Tag[t]
	}
	if v == nil {
		return
	}
	if _, id, ok := decodeTagIndexLegacy(v.Index); ok {
		delete(ts.Name2Tag, id)
	}
	delete(ts.Name2Tag, v.Name)
	delete(ts.ID2Tag, v.ID)
	ts.hashSumValid = false // tags changed, previously computed hash sum is no longer valid
}

func (ts *SeriesData) GetSMaxHosts(h Handler) []string {
	res := make([]string, len(ts.MaxHost))
	for j, id := range ts.MaxHost {
		if id != 0 {
			res[j] = h.GetHostName(id)
		}
	}
	return res
}

func (ts *SeriesTags) hash(ev *evaluator, opt hashOptions, listTags bool) (uint64, []string, error) {
	if ev.hh == nil {
		ev.hh = fnv.New64()
	}
	var s []string
	var cache bool
	if opt.on {
		s = append(s, opt.tags...)
	} else if len(opt.tags) == 0 {
		if listTags || !ts.hashSumValid {
			for _, v := range ts.ID2Tag {
				s = append(s, v.ID)
			}
		}
		if ts.hashSumValid {
			return ts.hashSum, s, nil
		} else {
			cache = true
		}
	} else {
		var nots []string // not "s"
		for id, tag := range ts.ID2Tag {
			var found bool
			for _, v := range opt.tags { // "tags" expected to be short, no need to build a map
				if len(v) == 0 {
					continue
				}
				if v == id || v == tag.Name {
					found = true
					break
				}
			}
			if !found {
				nots = append(nots, id)
			}
		}
		s = nots
	}
	sort.Strings(s)
	buf := make([]byte, 4)
	for _, v := range s {
		t, ok := ts.get(v)
		if !ok {
			continue
		}
		_, err := ev.hh.Write([]byte(v))
		if err != nil {
			return 0, nil, err
		}
		if opt.stags != nil && opt.stags[t.ID] != 0 {
			t.stringify(ev)
		}
		if t.stringified {
			_, err = ev.hh.Write([]byte(t.SValue))
		} else {
			binary.LittleEndian.PutUint32(buf, uint32(t.Value))
			_, err = ev.hh.Write(buf)
		}
		if err != nil {
			return 0, nil, err
		}
	}
	sum := ev.hh.Sum64()
	ev.hh.Reset()
	if cache {
		ts.hashSum = sum
		ts.hashSumValid = true
	}
	return sum, s, nil
}

// Serializes into JSON of Prometheus format. Slow but only 20 lines long.
func (ts *TimeSeries) MarshalJSON() ([]byte, error) {
	type series struct {
		M map[string]string `json:"metric,omitempty"`
		V [][2]any          `json:"values,omitempty"`
	}
	res := make([]series, 0, len(ts.Series.Data))
	for _, s := range ts.Series.Data {
		v := make([][2]any, 0, len(ts.Time))
		for j, t := range ts.Time {
			if math.Float64bits((*s.Values)[j]) != NilValueBits {
				v = append(v, [2]any{t, strconv.FormatFloat((*s.Values)[j], 'f', -1, 64)})
			}
		}
		if len(v) == 0 {
			continue
		}
		var m map[string]string
		if len(s.Tags.ID2Tag) != 0 {
			m = make(map[string]string, len(s.Tags.ID2Tag))
			for _, tag := range s.Tags.ID2Tag {
				if !tag.stringified || tag.SValue == format.TagValueCodeZero {
					continue
				}
				if len(tag.Name) != 0 {
					m[tag.Name] = tag.SValue
				} else {
					m[tag.ID] = tag.SValue
				}
			}
		}
		res = append(res, series{M: m, V: v})
	}
	return json.Marshal(res)
}

func (ts *TimeSeries) Type() parser.ValueType {
	return parser.ValueTypeMatrix
}

func (ts *TimeSeries) String() string {
	var from, to int64
	if len(ts.Time) != 0 {
		from = ts.Time[0]
		to = ts.Time[len(ts.Time)-1]
	}
	return fmt.Sprintf("series #%d, points #%d, range [%d, %d]", len(ts.Series.Data), len(ts.Time), from, to)
}

func decodeTagIndexLegacy(i int) (ix int, id string, ok bool) {
	if i <= 0 {
		return 0, "", false
	}
	ix = i - SeriesTagIndexOffset
	if 0 <= ix && ix < format.MaxTags {
		id, ok = format.TagIDLegacy(ix), true
	} else if i == format.StringTopTagIndex {
		id, ok = format.LegacyStringTopTagID, true
	}
	return ix, id, ok
}
func mergeSeriesMeta(a SeriesMeta, b SeriesMeta) SeriesMeta {
	if a.Metric != b.Metric {
		if a.Metric == nil {
			a.Metric = b.Metric
		} else {
			a.Metric = nil
		}
	}
	if a.What != b.What {
		if a.What == 0 {
			a.What = b.What
		} else {
			a.What = 0
		}
	}
	if a.Total < b.Total {
		a.Total = b.Total
	}
	if len(a.STags) == 0 {
		a.STags = b.STags
	} else if len(b.STags) != 0 {
		if len(a.STags) < len(b.STags) {
			a.STags, b.STags = b.STags, a.STags
		}
		for k, v := range b.STags {
			a.STags[k] += v
		}
	} // else both empty
	return a
}

func removeMetricName(s []Series) {
	for i := range s {
		s[i].removeMetricName()
	}
}
