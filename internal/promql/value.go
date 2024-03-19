// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package promql

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/vkcom/statshouse-go"
	"github.com/vkcom/statshouse/internal/promql/model"
	"github.com/vkcom/statshouse/internal/promql/parser"
)

type histogram struct {
	*model.Series
	buckets []bucket
}

type bucket struct {
	x  int     // series index
	le float32 // decoded "le" tag value
}

type hashOptions struct {
	on    bool
	tags  []string
	stags map[string]int // tags with non zero value will be stringified

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

func (ev *evaluator) groupMinMaxHost(ds []model.SeriesData, x int) []int32 {
	if len(ds) == 0 {
		return nil
	}
	if len(ds) == 1 {
		return ds[0].MinMaxHost[x]
	}
	var (
		i   int
		t   = ev.time()
		res []int32
	)
	for ; i < len(ds); i++ {
		if len(ds[i].MinMaxHost[x]) != 0 {
			res = make([]int32, 0, len(t))
			break
		}
	}
	if res == nil {
		return nil
	}
	for j := 0; j < len(t); j++ {
		var (
			v = ds[i].MinMaxHost[x][j]
			k = i + 1
		)
		for ; k < len(ds); k++ {
			if k < len(ds) && ds[k].MinMaxHost[x][j] != 0 && ds[k].MinMaxHost[x][j] != v {
				if v == 0 {
					v = ds[k].MinMaxHost[x][j]
				} else {
					v = 0
					break
				}
			}
		}
		res = append(res, v)
	}
	return res
}

func (ev *evaluator) hash(sr *model.Series, opt hashOptions) (map[uint64]hashMeta, error) {
	res := make(map[uint64]hashMeta, len(sr.Data))
	for i := range sr.Data {
		sum, tags, err := ev.hashT(&sr.Data[i].Tags, opt)
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

func (ev *evaluator) group(sr *model.Series, opt hashOptions) (map[uint64][]int, []hashTags, error) {
	var tags []hashTags
	if opt.listUsed || opt.listUnused {
		tags = make([]hashTags, len(sr.Data))
	}
	m := make(map[uint64][]int, len(sr.Data))
	for i := range sr.Data {
		h, v, err := ev.hashT(&sr.Data[i].Tags, opt)
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

func (ev *evaluator) histograms(sr *model.Series) ([]histogram, error) {
	m, _, err := ev.group(sr, hashOptions{
		tags: []string{labels.BucketLabel},
		on:   false, // group excluding BucketLabel
	})
	if err != nil {
		return nil, err
	}
	var res []histogram
	for _, xs := range m {
		var bs []bucket
		for _, x := range xs {
			if t, ok := sr.Data[x].Tags.Get(labels.BucketLabel); ok {
				if t.Stringified {
					var v float64
					v, err = strconv.ParseFloat(t.SValue, 32)
					if err == nil {
						bs = append(bs, bucket{x, float32(v)})
					}
				} else {
					bs = append(bs, bucket{x, statshouse.LexDecode(t.Value)})
				}
			}
		}
		if len(bs) != 0 {
			sort.Slice(bs, func(i, j int) bool { return bs[i].le < bs[j].le })
			res = append(res, histogram{sr, bs})
		}
	}
	return res, nil
}

func (h *histogram) seriesAt(x int) model.Series {
	bucket := h.buckets[x]
	data := h.Data[bucket.x : bucket.x+1]
	data[0].Tags.Remove(labels.BucketLabel)
	return model.Series{
		Data: data,
		Meta: h.Meta,
	}
}

func (h *histogram) data() []model.SeriesData {
	res := make([]model.SeriesData, 0, len(h.buckets))
	for _, b := range h.buckets {
		res = append(res, h.Data[b.x])
	}
	return res
}

type secondsFormat struct {
	n int32  // number of seconds
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

func (ev *evaluator) stringify(tg *model.SeriesTag) {
	if tg.Stringified {
		return
	}
	if len(tg.SValue) != 0 {
		tg.Stringified = true
		return
	}
	var v string
	switch tg.ID {
	case LabelWhat:
		v = model.DigestWhat(tg.Value).String()
	case LabelShard:
		v = strconv.FormatUint(uint64(tg.Value), 10)
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
		v = ev.h.GetHostName(tg.Value)
	default:
		v = ev.h.GetTagValue(model.TagValueQuery{
			Version:    ev.opt.Version,
			Metric:     tg.Metric,
			TagID:      tg.ID,
			TagValueID: tg.Value,
		})
	}
	tg.SetSValue(v)
}

func (ev *evaluator) gets(sr *model.Series, x int, id string) (*model.SeriesTag, bool) {
	tgs := &sr.Data[x].Tags
	if res, ok := tgs.Get(id); ok {
		ev.stringify(res)
		tgs.HashSumValid = false
		return res, true
	}
	return nil, false
}

func (ev *evaluator) hashT(tgs *model.SeriesTags, opt hashOptions) (uint64, hashTags, error) {
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
		if opt.listUsed || !tgs.HashSumValid {
			for id := range tgs.ID2Tag {
				if id != labels.MetricName {
					ht.used = append(ht.used, id)
				}
			}
		}
		if tgs.HashSumValid {
			return tgs.HashSum, ht, nil
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
			ev.stringify(t)
		}
		if t.Stringified {
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
		tgs.HashSum = sum
		tgs.HashSumValid = true
	}
	return sum, ht, nil
}

func evalSeriesMeta(expr *parser.BinaryExpr, lhs model.SeriesMeta, rhs model.SeriesMeta) model.SeriesMeta {
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
	if rhs.What != 0 && lhs.What != rhs.What {
		lhs.What = 0
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

func removeMetricName(s []model.Series) {
	for i := range s {
		s[i].RemoveMetricName()
	}
}
