// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/promql/parser"
)

const (
	SeriesTagIndexOffset = 2
	maxSeriesRows        = 10_000_000
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
	Values     *[]float64
	MinMaxHost [2][]int32 // "min" at [0], "max" at [1]
	Tags       SeriesTags
	Offset     int64
	What       int
}

type SeriesMeta struct {
	Metric *format.MetricMetaValue
	What   int
	Total  int
	STags  map[string]int
	Units  string
}

type SeriesTags struct {
	ID2Tag       map[string]*SeriesTag // indexed by tag ID (canonical name)
	Name2Tag     map[string]*SeriesTag // indexed by tag optional Name
	HashSum      uint64
	HashSumValid bool
}

type SeriesTag struct {
	Metric      *format.MetricMetaValue
	Index       int    // shifted by "SeriesTagIndexOffset", zero means not set
	ID          string // canonical name, always set
	Name        string // optional custom name
	Value       int32
	SValue      string
	Stringified bool
}

func (sr *Series) AddTagAt(x int, tg *SeriesTag) {
	sr.Data[x].Tags.Add(tg, &sr.Meta)
}

func (sr *Series) removeTag(id string) {
	for _, data := range sr.Data {
		data.Tags.Remove(id)
	}
}

func (sr *Series) RemoveMetricName() {
	sr.removeTag(labels.MetricName)
}

func (sr *Series) AppendAll(src Series) {
	sr.Data = append(sr.Data, src.Data...)
}

func (sr *Series) AppendSome(src Series, xs ...int) {
	for _, x := range xs {
		sr.Data = append(sr.Data, src.Data[x])
	}
}

func (sr *Series) DataAt(xs ...int) []SeriesData {
	res := make([]SeriesData, 0, len(xs))
	for _, x := range xs {
		res = append(res, sr.Data[x])
	}
	return res
}

func (sr *Series) Scalar() bool {
	if len(sr.Data) != 1 {
		return false
	}
	for _, m := range sr.Data {
		if len(m.Tags.ID2Tag) != 0 {
			return false
		}
	}
	return true
}

func (tg *SeriesTag) SetSValue(v string) {
	tg.SValue = v
	tg.Stringified = true
}

func (tgs *SeriesTags) Add(tg *SeriesTag, meta *SeriesMeta) {
	if len(tg.SValue) == 0 && tg.Stringified {
		// setting empty string value removes tag
		tgs.Remove(tg.ID)
		return
	}
	if tgs.ID2Tag == nil {
		tgs.ID2Tag = map[string]*SeriesTag{tg.ID: tg}
	} else {
		tgs.Remove(tg.ID)
		tgs.ID2Tag[tg.ID] = tg
	}
	if tg.Index != 0 {
		if tgs.Name2Tag == nil {
			tgs.Name2Tag = map[string]*SeriesTag{}
		}
		if len(tg.Name) != 0 {
			tgs.Name2Tag[tg.Name] = tg
		}
		if _, id, ok := decodeTagIndexLegacy(tg.Index); ok {
			tgs.Name2Tag[id] = tg
		}
	}
	if len(tg.SValue) != 0 {
		if meta.STags == nil {
			meta.STags = make(map[string]int)
		}
		meta.STags[tg.ID]++
		tg.Stringified = true
	}
	tgs.HashSumValid = false // tags changed, previously computed hash sum is no longer valid
}

func (tgs *SeriesTags) Get(id string) (*SeriesTag, bool) {
	if tgs.ID2Tag == nil {
		return nil, false
	}
	var res *SeriesTag
	if res = tgs.ID2Tag[id]; res == nil && tgs.Name2Tag != nil {
		res = tgs.Name2Tag[id]
	}
	if res == nil {
		return nil, false
	}
	return res, true
}

func (tgs *SeriesTags) Remove(id string) {
	if tgs.ID2Tag == nil {
		return
	}
	v := tgs.ID2Tag[id]
	if v == nil && tgs.Name2Tag != nil {
		v = tgs.Name2Tag[id]
	}
	if v == nil {
		return
	}
	if _, id, ok := decodeTagIndexLegacy(v.Index); ok {
		delete(tgs.Name2Tag, id)
	}
	delete(tgs.Name2Tag, v.Name)
	delete(tgs.ID2Tag, v.ID)
	if id != labels.MetricName {
		tgs.HashSumValid = false // tags changed, previously computed hash sum is no longer valid
	}
}

func (tgs *SeriesTags) clone() SeriesTags {
	res := SeriesTags{
		ID2Tag:       make(map[string]*SeriesTag, len(tgs.ID2Tag)),
		Name2Tag:     make(map[string]*SeriesTag, len(tgs.Name2Tag)),
		HashSum:      tgs.HashSum,
		HashSumValid: tgs.HashSumValid,
	}
	for id, tag := range tgs.ID2Tag {
		copy := *tag
		res.ID2Tag[id] = &copy
		if len(tag.Name) != 0 {
			res.Name2Tag[tag.Name] = &copy
		}
	}
	return res
}

func (tgs *SeriesTags) CloneSome(ids ...string) SeriesTags {
	res := SeriesTags{
		ID2Tag:   make(map[string]*SeriesTag, len(ids)),
		Name2Tag: make(map[string]*SeriesTag),
	}
	for _, id := range ids {
		if tag := tgs.ID2Tag[id]; tag != nil {
			copy := *tag
			res.ID2Tag[id] = &copy
			if len(tag.Name) != 0 {
				res.Name2Tag[tag.Name] = &copy
			}
		}
	}
	return res
}

func (d *SeriesData) GetSMaxHosts(h TagMapper) []string {
	res := make([]string, len(d.MinMaxHost[1]))
	for j, id := range d.MinMaxHost[1] {
		if id != 0 {
			res[j] = h.GetHostName(id)
		}
	}
	return res
}

func (sr *Series) LabelMinMaxHost(ev Allocator, x int, tagID string) error {
	res := make([]SeriesData, 0)
	for _, d := range sr.Data {
		tail := d.labelMinMaxHost(ev, x, tagID)
		if len(res)+len(tail) > maxSeriesRows {
			return fmt.Errorf("number of resulting series exceeds %d", maxSeriesRows)
		}
		res = append(res, tail...)
	}
	sr.Free(ev)
	sr.Data = res
	sr.Meta.Total = len(res)
	return nil
}

func (d *SeriesData) labelMinMaxHost(ev Allocator, x int, tagID string) []SeriesData {
	if len(d.MinMaxHost[x]) == 0 {
		return []SeriesData{*d}
	}
	m := map[int32]int{}
	for i, h := range d.MinMaxHost[x] {
		if !math.IsNaN((*d.Values)[i]) {
			m[h] = i
		}
	}
	res := make([]SeriesData, 0, len(m))
	for h := range m {
		data := SeriesData{
			Values: ev.Alloc(len(*d.Values)),
			Tags:   d.Tags.clone(),
			Offset: d.Offset,
			What:   d.What,
		}
		for i, v := range *d.Values {
			if d.MinMaxHost[x][i] == h {
				(*data.Values)[i] = v
			} else {
				(*data.Values)[i] = NilValue
			}
		}
		data.Tags.Add(&SeriesTag{
			ID:    tagID,
			Value: h,
		}, nil)
		res = append(res, data)
	}
	return res
}

func (sr *Series) FilterMinMaxHost(h TagMapper, x int, matchers []*labels.Matcher) {
	for i := 0; i < len(sr.Data); {
		if sr.Data[i].filterMinMaxHost(h, x, matchers) != 0 {
			i++
		} else {
			sr.Data = append(sr.Data[0:i], sr.Data[i+1:]...)
		}
	}
	sr.Meta.Total = len(sr.Data)
}

func (sr *Series) Free(ev Allocator) {
	for x := range sr.Data {
		sr.Data[x].Free(ev)
	}
}

func (sr *Series) FreeSome(ev Allocator, xs ...int) {
	for _, x := range xs {
		sr.Data[x].Free(ev)
	}
}

func (d *SeriesData) filterMinMaxHost(h TagMapper, x int, matchers []*labels.Matcher) int {
	n := 0
	for i, maxHost := range d.MinMaxHost[x] {
		discard := false
		maxHostname := h.GetHostName(maxHost)
		for _, matcher := range matchers {
			if !matcher.Matches(maxHostname) {
				discard = true
				break
			}
		}
		if discard {
			(*d.Values)[i] = NilValue
			d.MinMaxHost[x][i] = 0
		} else if !math.IsNaN((*d.Values)[i]) {
			n++
		}
	}
	return n
}

func (d *SeriesData) Free(ev Allocator) {
	ev.Free(d.Values)
	d.Values = nil
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
		for i, t := range ts.Time {
			if math.Float64bits((*s.Values)[i]) != NilValueBits {
				v = append(v, [2]any{t, strconv.FormatFloat((*s.Values)[i], 'f', -1, 64)})
			}
		}
		if len(v) == 0 {
			continue
		}
		var m map[string]string
		if len(s.Tags.ID2Tag) != 0 {
			m = make(map[string]string, len(s.Tags.ID2Tag))
			for _, tag := range s.Tags.ID2Tag {
				if !tag.Stringified || tag.SValue == format.TagValueCodeZero {
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
