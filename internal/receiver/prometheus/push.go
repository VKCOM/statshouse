// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package prometheus

import (
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/receiver"
)

type MetricPusher interface {
	PushLocal(metric *tlstatshouse.MetricBytes)
	IsLocal() bool
}

type LocalMetricPusher struct {
	h receiver.Handler
}

func (pusher *LocalMetricPusher) PushLocal(metric *tlstatshouse.MetricBytes) {
	_, _ = pusher.h.HandleMetrics(metric, nil)
}

func (pusher *LocalMetricPusher) IsLocal() bool {
	return true
}

func NewLocalMetricPusher(h receiver.Handler) *LocalMetricPusher {
	return &LocalMetricPusher{h: h}
}
