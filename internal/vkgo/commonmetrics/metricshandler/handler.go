// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metricshandler

import (
	"time"

	"github.com/vkcom/statshouse-go"
	"github.com/vkcom/statshouse/internal/vkgo/commonmetrics"
	"github.com/vkcom/statshouse/internal/vkgo/commonmetrics/internal/env"
)

var (
	responseTimeMetricName = env.FullMetricName("common_response_time")
	responseSizeMetricName = env.FullMetricName("common_response_size")
	requestSizeMetricName  = env.FullMetricName("common_request_size")
)

type InputRequest struct {
	commonmetrics.Method
	Protocol   string
	Status     string
	StatusCode string
}

func (r InputRequest) toRawTags() statshouse.Tags {
	tags := commonmetrics.AttachBase(statshouse.Tags{})
	tags[4] = r.Protocol
	tags[5] = r.Method.Group
	tags[6] = r.Method.Name
	tags[7] = r.Status
	tags[8] = r.StatusCode

	return tags
}

func ResponseSize(r InputRequest, value int) {
	ResponseSizeRaw(r.toRawTags(), value)
}

func RequestSize(r InputRequest, value int) {
	RequestSizeRaw(r.toRawTags(), value)
}

func ResponseTime(r InputRequest, value time.Duration) {
	ResponseTimeRaw(r.toRawTags(), value)
}

func ResponseSizeRaw(tags statshouse.Tags, value int) {
	statshouse.Value(responseSizeMetricName, tags, float64(value))
}

func RequestSizeRaw(tags statshouse.Tags, value int) {
	statshouse.Value(requestSizeMetricName, tags, float64(value))
}

func ResponseTimeRaw(tags statshouse.Tags, value time.Duration) {
	statshouse.Value(responseTimeMetricName, tags, value.Seconds())
}
