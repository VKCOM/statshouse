// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metajournal

import (
	"context"

	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"

	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/VKCOM/statshouse/internal/format"
)

type MetadataLoader interface {
	// current journal, getting + editing
	LoadJournal(ctx context.Context, lastVersion int64, returnIfEmpty bool) ([]tlmetadata.Event, int64, error)
	SaveEntity(ctx context.Context, event tlmetadata.Event, create bool, del bool) (ret tlmetadata.Event, err error)

	// historic list plus old versions of entities
	GetShortHistory(ctx context.Context, id int64) (ret tlmetadata.HistoryShortResponse, err error)
	GetMetric(ctx context.Context, id int64, version int64) (ret format.MetricMetaValue, err error)
	GetDashboard(ctx context.Context, id int64, version int64) (ret format.DashboardMeta, err error)

	// mappings
	GetNewMappings(ctx context.Context, lastVersion int32, returnIfEmpty bool) (m []tlstatshouse.Mapping, curV, lastV int32, err error)
	PutTagMapping(ctx context.Context, tag string, id int32) error
	GetTagMapping(ctx context.Context, tag string, metricName string, create bool) (int32, int32, error)
	ResetFlood(ctx context.Context, metricName string, value int32) (before int32, after int32, _ error)
}
