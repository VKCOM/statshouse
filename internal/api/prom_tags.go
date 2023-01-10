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

func (qe *promQueryable) resolveTags(t tsTags, mq *metricQuery, meta *[]QuerySeriesMeta, histograms map[tsTags][]int, ixToLE map[int]float32) {
	tags := make(map[string]string, 17)
	qe.maybeAddQuerySeriesTagValue(tags, mq, 0, t.Tag0)
	qe.maybeAddQuerySeriesTagValue(tags, mq, 1, t.Tag1)
	qe.maybeAddQuerySeriesTagValue(tags, mq, 2, t.Tag2)
	qe.maybeAddQuerySeriesTagValue(tags, mq, 3, t.Tag3)
	qe.maybeAddQuerySeriesTagValue(tags, mq, 4, t.Tag4)
	qe.maybeAddQuerySeriesTagValue(tags, mq, 5, t.Tag5)
	qe.maybeAddQuerySeriesTagValue(tags, mq, 6, t.Tag6)
	qe.maybeAddQuerySeriesTagValue(tags, mq, 7, t.Tag7)
	qe.maybeAddQuerySeriesTagValue(tags, mq, 8, t.Tag8)
	qe.maybeAddQuerySeriesTagValue(tags, mq, 9, t.Tag9)
	qe.maybeAddQuerySeriesTagValue(tags, mq, 10, t.Tag10)
	qe.maybeAddQuerySeriesTagValue(tags, mq, 11, t.Tag11)
	qe.maybeAddQuerySeriesTagValue(tags, mq, 12, t.Tag12)
	qe.maybeAddQuerySeriesTagValue(tags, mq, 13, t.Tag13)
	qe.maybeAddQuerySeriesTagValue(tags, mq, 14, t.Tag14)
	// Special case for key 15 to handle histogram LE tag
	tagID := format.TagID(15)
	if tagName, ok := mq.by[tagID]; ok {
		if tagName == format.LETagName {
			le := prometheus.LexDecode(t.Tag15)
			tags[tagName] = strconv.FormatFloat(float64(le), 'f', -1, 32)
			ix := len(ixToLE)
			ixToLE[ix] = le
			t.Tag15 = 0 // modifying a copy
			histograms[t] = append(histograms[t], ix)
		} else {
			tags[tagName] = qe.h.getRichTagValue(mq.meta, Version2, tagID, t.Tag15)
		}
	}
	*meta = append(*meta, QuerySeriesMeta{Tags: tags})
}

func (qe *promQueryable) maybeAddQuerySeriesTagValue(m map[string]string, mq *metricQuery, tagIx int, id int32) {
	tagID := format.TagID(tagIx)
	if tagName, ok := mq.by[tagID]; ok {
		m[tagName] = qe.h.getRichTagValue(mq.meta, Version2, tagID, id)
	}
}
