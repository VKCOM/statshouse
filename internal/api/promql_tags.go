// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"strconv"

	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/receiver/prometheus"
)

func getGroupingTags(meta *format.MetricMetaValue, by *map[string]string) []string {
	var ret []string
	if *by != nil {
		ret = make([]string, 0, len(*by))
		for key := range *by {
			ret = append(ret, key)
		}
	} else {
		// TODO: put under command line flag --promql-test
		tags := meta.Tags[1:] // skip "env" builtin tag
		*by = map[string]string{}
		ret = make([]string, 0, len(tags))
		for _, tag := range tags {
			if len(tag.Name) != 0 {
				tagID := format.TagID(tag.Index)
				(*by)[tagID] = tag.Name
				ret = append(ret, tagID)
			}
		}
	}
	return ret
}

func (qe *promQueryable) resolveTags(t tsTags, q *metricQuery, meta *[]QuerySeriesMeta, histograms map[tsTags][]int, ixToLE map[int]float32) {
	tags := make(map[string]string, 17)
	qe.maybeAddQuerySeriesTagValue(tags, q, 0, t.Tag0)
	qe.maybeAddQuerySeriesTagValue(tags, q, 1, t.Tag1)
	qe.maybeAddQuerySeriesTagValue(tags, q, 2, t.Tag2)
	qe.maybeAddQuerySeriesTagValue(tags, q, 3, t.Tag3)
	qe.maybeAddQuerySeriesTagValue(tags, q, 4, t.Tag4)
	qe.maybeAddQuerySeriesTagValue(tags, q, 5, t.Tag5)
	qe.maybeAddQuerySeriesTagValue(tags, q, 6, t.Tag6)
	qe.maybeAddQuerySeriesTagValue(tags, q, 7, t.Tag7)
	qe.maybeAddQuerySeriesTagValue(tags, q, 8, t.Tag8)
	qe.maybeAddQuerySeriesTagValue(tags, q, 9, t.Tag9)
	qe.maybeAddQuerySeriesTagValue(tags, q, 10, t.Tag10)
	qe.maybeAddQuerySeriesTagValue(tags, q, 11, t.Tag11)
	qe.maybeAddQuerySeriesTagValue(tags, q, 12, t.Tag12)
	qe.maybeAddQuerySeriesTagValue(tags, q, 13, t.Tag13)
	qe.maybeAddQuerySeriesTagValue(tags, q, 14, t.Tag14)
	// Special case for key 15 to handle "le" tag
	tagID := format.TagID(15)
	if tagName, ok := q.by[tagID]; ok {
		if tagName == format.LETagName {
			le := prometheus.LexDecode(t.Tag15)
			tags[tagName] = strconv.FormatFloat(float64(le), 'f', -1, 32)
			ix := len(ixToLE)
			ixToLE[ix] = le
			t.Tag15 = 0 // modifying copy
			histograms[t] = append(histograms[t], ix)
		} else {
			tags[tagName] = qe.h.getRichTagValue(q.meta, Version2, tagID, t.Tag15)
		}
	}
	*meta = append(*meta, QuerySeriesMeta{Tags: tags})
}

func (qe *promQueryable) maybeAddQuerySeriesTagValue(m map[string]string, q *metricQuery, tagIx int, id int32) {
	tagID := format.TagID(tagIx)
	if tagName, ok := q.by[tagID]; ok {
		m[tagName] = qe.h.getRichTagValue(q.meta, Version2, tagID, id)
	}
}
