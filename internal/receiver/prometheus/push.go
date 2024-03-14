// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package prometheus

import (
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/receiver"
)

type MetricPusher interface {
	PushLocal(metric *tlstatshouse.MetricBytes, description string, scrapeInterval int)
	IsLocal() bool
}

type LocalMetricPusher struct {
	h receiver.Handler
}

func (pusher *LocalMetricPusher) PushLocal(metric *tlstatshouse.MetricBytes, description string, scrapeInterval int) {
	_, _ = pusher.h.HandleMetrics(data_model.HandlerArgs{
		MetricBytes:    metric,
		Description:    description,
		ScrapeInterval: scrapeInterval,
	})
}

func (pusher *LocalMetricPusher) IsLocal() bool {
	return true
}

func NewLocalMetricPusher(h receiver.Handler) *LocalMetricPusher {
	return &LocalMetricPusher{h: h}
}
