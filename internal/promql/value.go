// Copyright 2025 V Kontakte LLC
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
	"github.com/vkcom/statshouse-go"
	"github.com/vkcom/statshouse/internal/chutil"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/promql/parser"
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
	MinMaxHost [2][]chutil.ArgMinMaxStringFloat32 // "min" at [0], "max" at [1]
	Tags       SeriesTags
	Offset     int64
	What       SelectorWhat
	empty      bool // used in "or" operator implementation to mark RHS series for removal
}

type SeriesMeta struct {
	Metric *format.MetricMetaValue
	Total  int
	STags  map[string]int
	Units  string
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
	Value       int64
	SValue      string
	stringified bool
}

const SeriesTagIndexOffset = 2

type histogram struct {
	*Series
	buckets []bucket
}

type bucket struct {
	x  int     // series index
	le float32 // decoded "le" tag value
}

type hashOptions struct {
	on    bool
	tags  []string
	stags map[string]int

	listUsed   bool // list tags used in hash calculation
	listUnused bool // list tags not used in hash calculation
}

type hashTags struct {
	used   []string // tags used in hash calculation
	unused []string // tags not used in hash calculation
}

type hashMeta struct {
	hashTags
	x int // series index
}

func (sr *Series) AddTagAt(x int, tg *SeriesTag) {
	sr.Data[x].Tags.add(tg, &sr.Meta)
}

func (sr *Series) removeTag(id string) {
	for _, data := range sr.Data {
		data.Tags.remove(id)
	}
}

func (sr *Series) removeMetricName() {
	sr.removeTag(labels.MetricName)
}

func (sr *Series) appendAll(src Series) {
	sr.Data = append(sr.Data, src.Data...)
}

func (sr *Series) appendSome(src Series, xs ...int) {
	for _, x := range xs {
		sr.Data = append(sr.Data, src.Data[x])
	}
}

func (ev *evaluator) groupMinMaxHost(ds []SeriesData, x int) []chutil.ArgMinMaxStringFloat32 {
	if len(ds) == 0 {
		return nil
	}
	if len(ds) == 1 {
		return ds[0].MinMaxHost[x]
	}
	var i int
	var t = ev.time()
	var res []chutil.ArgMinMaxStringFloat32
	for ; i < len(ds); i++ {
		if len(ds[i].MinMaxHost[x]) != 0 {
			res = make([]chutil.ArgMinMaxStringFloat32, 0, len(t))
			break
		}
	}
	if len(res) == 0 {
		return res
	}
	for j := 0; j < len(t); j++ {
		v := ds[i].MinMaxHost[x][j]
		k := i + 1
		for ; k < len(ds); k++ {
			if k < len(ds) {
				v.Merge(ds[k].MinMaxHost[x][j], x)
			}
		}
		res = append(res, v)
	}
	return res
}

func (sr *Series) hash(ev *evaluator, opt hashOptions) (map[uint64]hashMeta, error) {
	res := make(map[uint64]hashMeta, len(sr.Data))
	for i := range sr.Data {
		sum, tags, err := sr.Data[i].Tags.hash(ev, opt)
		if err != nil {
			return nil, err
		}
		if _, ok := res[sum]; ok {
			return nil, fmt.Errorf("label set match multiple series")
		}
		res[sum] = hashMeta{tags, i}
	}
	return res, nil
}

func (sr *Series) group(ev *evaluator, opt hashOptions) (map[uint64][]int, []hashTags, error) {
	var (
		m    = make(map[uint64][]int, len(sr.Data))
		tags []hashTags
	)
	if opt.listUsed || opt.listUnused {
		tags = make([]hashTags, len(sr.Data))
	}
	for i := range sr.Data {
		h, v, err := sr.Data[i].Tags.hash(ev, opt)
		if err != nil {
			return nil, nil, err
		}
		if tags != nil {
			tags[i] = v
		}
		m[h] = append(m[h], i)
	}
	return m, tags, nil
}

func (sr *Series) dataAt(xs ...int) []SeriesData {
	res := make([]SeriesData, 0, len(xs))
	for _, x := range xs {
		res = append(res, sr.Data[x])
	}
	return res
}

func (sr *Series) histograms(ev *evaluator) ([]histogram, error) {
	if sr.Meta.Metric == nil {
		return nil, fmt.Errorf("metric meta not found")
	}
	if len(sr.Meta.Metric.HistogramBuckets) == 0 {
		return nil, fmt.Errorf("histogram meta not found")
	}
	m, _, err := sr.group(ev, hashOptions{
		tags: []string{labels.BucketLabel},
		on:   false, // group excluding BucketLabel
	})
	if err != nil {
		return nil, err
	}
	var res []histogram
	for _, xs := range m {
		buckets := make([]bucket, 0, len(sr.Meta.Metric.HistogramBuckets))
		for _, x := range xs {
			if t, ok := sr.Data[x].Tags.Get(labels.BucketLabel); ok {
				var le float32
				if t.stringified {
					var v float64
					v, err = strconv.ParseFloat(t.SValue, 32)
					if err != nil {
						return nil, err
					}
					le = float32(v)
				} else {
					le = statshouse.LexDecode(int32(t.Value))
				}
				buckets = append(buckets, bucket{le: le, x: x})
			}
		}
		if len(buckets) != 0 {
			sort.Slice(buckets, func(i, j int) bool {
				return buckets[i].le < buckets[j].le
			})
			res = append(res, histogram{
				Series:  sr,
				buckets: buckets,
			})
		}
	}
	return res, nil
}

func (sr *Series) scalar() bool {
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

func (sr *Series) empty() bool {
	return len(sr.Data) == 0
}

func (h *histogram) seriesAt(x int) Series {
	bucket := h.buckets[x]
	data := h.Data[bucket.x : bucket.x+1]
	data[0].Tags.remove(labels.BucketLabel)
	return Series{
		Data: data,
		Meta: h.Meta,
	}
}

func (h *histogram) data() []SeriesData {
	res := make([]SeriesData, 0, len(h.buckets))
	for _, b := range h.buckets {
		res = append(res, h.Data[b.x])
	}
	return res
}

type secondsFormat struct {
	n int64  // number of seconds
	s string // corresponding format string
}

var secondsFormats []secondsFormat = []secondsFormat{
	{2678400, "%dM"}, // months
	{604800, "%dw"},  // weeks
	{86400, "%dd"},   // days
	{3600, "%dh"},    // hours
	{60, "%dm"},      // minutes
	{1, "%ds"},       // seconds
}

func (tg *SeriesTag) stringify(ev *evaluator) {
	if tg.stringified {
		return
	}
	if len(tg.SValue) != 0 {
		tg.stringified = true
		return
	}
	var v string
	switch tg.ID {
	case LabelWhat:
		v = DigestWhat(tg.Value).String()
	case LabelShard:
		v = strconv.FormatInt(tg.Value, 10)
	case LabelOffset:
		n := tg.Value // seconds
		if n < 0 {
			n = -n
		}
		for _, f := range secondsFormats {
			if n >= f.n && n%f.n == 0 {
				v = fmt.Sprintf(f.s, -tg.Value/f.n)
				break
			}
		}
	case LabelMinHost, LabelMaxHost:
		if tg.SValue != "" {
			v = tg.SValue
		} else {
			v = ev.GetHostName64(tg.Value)
		}
	default:
		if !ev.opt.RawBucketLabel && tg.Name == labels.BucketLabel {
			v = strconv.FormatFloat(float64(statshouse.LexDecode(int32(tg.Value))), 'f', -1, 32)
		} else {
			v = ev.GetTagValue(TagValueQuery{
				Version:    ev.opt.Version,
				Metric:     tg.Metric,
				TagID:      tg.ID,
				TagValueID: tg.Value,
			})
		}
	}
	tg.SetSValue(v)
}

func (tg *SeriesTag) SetSValue(v string) {
	tg.SValue = v
	tg.stringified = true
}

func (tg *SeriesTag) GetName() string {
	if tg.Name != "" {
		return tg.Name
	} else {
		return tg.ID
	}
}

func (tgs *SeriesTags) add(tg *SeriesTag, mt *SeriesMeta) {
	if len(tg.SValue) == 0 && tg.stringified {
		// setting empty string value removes tag
		tgs.remove(tg.ID)
		return
	}
	if tgs.ID2Tag == nil {
		tgs.ID2Tag = map[string]*SeriesTag{tg.ID: tg}
	} else {
		tgs.remove(tg.ID)
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
		if mt.STags == nil {
			mt.STags = make(map[string]int)
		}
		mt.STags[tg.ID]++
		tg.stringified = true
	}
	tgs.hashSumValid = false // tags changed, previously computed hash sum is no longer valid
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

func (t *SeriesTags) gets(ev *evaluator, id string) (*SeriesTag, bool) {
	if res, ok := t.Get(id); ok {
		res.stringify(ev)
		t.hashSumValid = false
		return res, true
	}
	return nil, false
}

func (tgs *SeriesTags) remove(id string) {
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
		tgs.hashSumValid = false // tags changed, previously computed hash sum is no longer valid
	}
}

func (tgs *SeriesTags) clone() SeriesTags {
	res := SeriesTags{
		ID2Tag:       make(map[string]*SeriesTag, len(tgs.ID2Tag)),
		Name2Tag:     make(map[string]*SeriesTag, len(tgs.Name2Tag)),
		hashSum:      tgs.hashSum,
		hashSumValid: tgs.hashSumValid,
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

func (tgs *SeriesTags) cloneSome(ids ...string) SeriesTags {
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

func (d *SeriesData) GetSMaxHosts(h Handler) []string {
	res := make([]string, len(d.MinMaxHost[1]))
	for j, v := range d.MinMaxHost[1] {
		res[j] = getHostName(h, v)
	}
	return res
}

func (sr *Series) labelMinMaxHost(ev *evaluator, x int, tagID string) error {
	res := make([]SeriesData, 0)
	for _, d := range sr.Data {
		tail := d.labelMinMaxHost(ev, x, tagID)
		if len(res)+len(tail) > maxSeriesRows {
			return fmt.Errorf("number of resulting series exceeds %d", maxSeriesRows)
		}
		res = append(res, tail...)
	}
	ev.freeAll(sr.Data)
	sr.Data = res
	sr.Meta.Total = len(res)
	return nil
}

func (d *SeriesData) labelMinMaxHost(ev *evaluator, x int, tagID string) []SeriesData {
	if len(d.MinMaxHost[x]) == 0 {
		return []SeriesData{*d}
	}
	m := map[chutil.ArgMinMaxStringFloat32]int{}
	for i, h := range d.MinMaxHost[x] {
		if !math.IsNaN((*d.Values)[i]) {
			m[h] = i
		}
	}
	res := make([]SeriesData, 0, len(m))
	for h := range m {
		data := SeriesData{
			Values: ev.alloc(),
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
		data.Tags.add(&SeriesTag{
			ID:     tagID,
			Value:  int64(h.AsInt32),
			SValue: h.Arg,
		}, nil)
		res = append(res, data)
	}
	return res
}

func (sr *Series) filterMinMaxHost(ev *evaluator, x int, matchers []*labels.Matcher) {
	for i := 0; i < len(sr.Data); {
		if sr.Data[i].filterMinMaxHost(ev, x, matchers) != 0 {
			i++
		} else {
			sr.Data = append(sr.Data[0:i], sr.Data[i+1:]...)
		}
	}
	sr.Meta.Total = len(sr.Data)
}

func (sr *Series) pruneMinMaxHost() {
	for i := range sr.Data {
		sr.Data[i].pruneMinMaxHost()
	}
}

func (sr *Series) free(ev *evaluator) {
	ev.freeAll(sr.Data)
}

func (sr *Series) freeSome(ev *evaluator, xs ...int) {
	ev.freeSome(sr.Data, xs...)
}

func (sr *Series) removeEmpty(ev *evaluator) {
	if sr.Meta.Total == 0 {
		sr.Meta.Total = len(sr.Data)
	}
	for i := 0; i < len(sr.Data); {
		var keep bool
		if !sr.Data[i].empty {
			for j := ev.t.ViewStartX; j < ev.t.ViewEndX; j++ {
				if !math.IsNaN((*sr.Data[i].Values)[j]) {
					keep = true
					break
				}
			}
		}
		if keep {
			i++
		} else {
			sr.Data[i].free(ev)
			sr.Data[i], sr.Data[len(sr.Data)-1] = sr.Data[len(sr.Data)-1], sr.Data[i]
			sr.Data = sr.Data[:len(sr.Data)-1]
			sr.Meta.Total--
		}
	}
}

func tagsEqual(a, b map[string]*SeriesTag) bool {
	if len(a) != len(b) {
		return false
	}
	for k, ta := range a {
		if tb, ok := b[k]; !ok ||
			ta.Metric != tb.Metric ||
			ta.ID != tb.ID ||
			ta.Value != tb.Value ||
			ta.SValue != tb.SValue {
			return false
		}
	}
	return true
}

func (d *SeriesData) filterMinMaxHost(ev *evaluator, x int, matchers []*labels.Matcher) int {
	n := 0
	for i, maxHost := range d.MinMaxHost[x] {
		discard := false
		maxHostname := getHostName(ev, maxHost)
		for _, matcher := range matchers {
			if !matcher.Matches(maxHostname) {
				discard = true
				break
			}
		}
		if discard {
			(*d.Values)[i] = NilValue
			d.MinMaxHost[x][i] = chutil.ArgMinMaxStringFloat32{}
		} else if !math.IsNaN((*d.Values)[i]) {
			n++
		}
	}
	return n
}

func (d *SeriesData) pruneMinMaxHost() {
	if len(d.MinMaxHost[0]) == 0 && len(d.MinMaxHost[1]) == 0 {
		return
	}
	for i, v := range *d.Values {
		if math.IsNaN(v) {
			if i < len(d.MinMaxHost[0]) {
				d.MinMaxHost[0][i] = chutil.ArgMinMaxStringFloat32{}

			}
			if i < len(d.MinMaxHost[1]) {
				d.MinMaxHost[1][i] = chutil.ArgMinMaxStringFloat32{}
			}
		}
	}
}

func (d *SeriesData) free(ev *evaluator) {
	ev.free(d.Values)
	d.Values = nil
}

func (tgs *SeriesTags) hash(ev *evaluator, opt hashOptions) (uint64, hashTags, error) {
	if ev.hh == nil {
		ev.hh = fnv.New64()
	}
	var ht hashTags
	var cache bool
	if opt.on {
		ht.used = append(ht.used, opt.tags...)
		if opt.listUnused {
			for id, tag := range tgs.ID2Tag {
				if id == labels.MetricName {
					continue
				}
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
					ht.unused = append(ht.unused, id)
				}
			}
		}
	} else if len(opt.tags) == 0 || (len(opt.tags) == 1 && opt.tags[0] == labels.MetricName) {
		if opt.listUsed || !tgs.hashSumValid {
			for id, tag := range tgs.ID2Tag {
				if id == labels.MetricName {
					continue
				}
				if tag.Name != "" {
					id = tag.Name
				}
				ht.used = append(ht.used, id)
			}
		}
		if tgs.hashSumValid {
			return tgs.hashSum, ht, nil
		} else {
			cache = true
		}
	} else {
		for id, tag := range tgs.ID2Tag {
			if id == labels.MetricName {
				continue
			}
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
			if tag.Name != "" {
				id = tag.Name
			}
			if !found {
				ht.used = append(ht.used, id)
			} else if opt.listUnused {
				ht.unused = append(ht.unused, id)
			}
		}
	}
	sort.Strings(ht.used)
	buf := make([]byte, 4)
	for _, v := range ht.used {
		t, ok := tgs.Get(v)
		if !ok {
			continue
		}
		_, err := ev.hh.Write([]byte(v))
		if err != nil {
			return 0, hashTags{}, err
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
			return 0, hashTags{}, err
		}
	}
	sum := ev.hh.Sum64()
	ev.hh.Reset()
	if cache {
		tgs.hashSum = sum
		tgs.hashSumValid = true
	}
	return sum, ht, nil
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
				if !tag.stringified || tag.SValue == "" {
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
	if ts == nil {
		return "<nil>"
	}
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
	} else if i == format.StringTopTagIndex || i == format.StringTopTagIndexV3 {
		id, ok = format.LegacyStringTopTagID, true
	}
	return ix, id, ok
}

func evalSeriesMeta(expr *parser.BinaryExpr, lhs SeriesMeta, rhs SeriesMeta) SeriesMeta {
	switch expr.Op {
	case parser.EQLC, parser.GTE, parser.GTR, parser.LSS, parser.LTE, parser.NEQ:
		if expr.ReturnBool {
			lhs.Units = ""
		}
	case parser.LAND, parser.LUNLESS, parser.LOR, parser.LDEFAULT:
		if len(rhs.Units) != 0 && lhs.Units != rhs.Units {
			lhs.Units = ""
		}
	case parser.ADD, parser.SUB:
		if lhs.Units != rhs.Units {
			lhs.Units = ""
		}
	default:
		lhs.Units = ""
	}
	if rhs.Metric != nil && lhs.Metric != rhs.Metric {
		lhs.Metric = nil
	}
	if lhs.Total < rhs.Total {
		lhs.Total = rhs.Total
	}
	if len(lhs.STags) == 0 {
		lhs.STags = rhs.STags
	} else if len(rhs.STags) != 0 {
		if len(lhs.STags) < len(rhs.STags) {
			lhs.STags, rhs.STags = rhs.STags, lhs.STags
		}
		for k, v := range rhs.STags {
			lhs.STags[k] += v
		}
	} // else both empty
	return lhs
}

func removeMetricName(s []Series) {
	for i := range s {
		s[i].removeMetricName()
	}
}
