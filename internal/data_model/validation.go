// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package data_model

import (
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
)

func ValidateMetricData(metricBytes *tlstatshouse.MetricBytes) (ingestionStatus int32) {
	if len(metricBytes.Value)+len(metricBytes.Histogram) != 0 && len(metricBytes.Unique) != 0 {
		ingestionStatus = format.TagValueIDSrcIngestionStatusErrValueUniqueBothSet
		return
	}
	if metricBytes.Counter, ingestionStatus = format.ClampCounter(metricBytes.Counter); ingestionStatus != 0 {
		return
	}
	for i, v := range metricBytes.Value {
		if metricBytes.Value[i], ingestionStatus = format.ClampValue(v); ingestionStatus != 0 {
			return
		}
	}
	for i, v := range metricBytes.Histogram {
		if metricBytes.Histogram[i][0], ingestionStatus = format.ClampValue(v[0]); ingestionStatus != 0 {
			return
		}
		if metricBytes.Histogram[i][1], ingestionStatus = format.ClampCounter(v[1]); ingestionStatus != 0 {
			return
		}
	}
	return
}
