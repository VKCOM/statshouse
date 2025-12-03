// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metajournal

import (
	"context"
	"errors"
	"strconv"

	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlmetadata"
)

var errDeadMetrics = errors.New("metrics update from storage is dead")

type AggLog func(typ string, key0 string, key1 string, key2 string, key3 string, key4 string, key5 string, message string)

type versionClient struct {
	expectedVersion int64
	ch              chan struct{}
}

type journalEventID struct {
	typ int32
	id  int64
}

type MetricsStorageLoader func(ctx context.Context, lastVersion int64, returnIfEmpty bool) ([]tlmetadata.Event, int64, error)

type ApplyEvent func(newEntries []tlmetadata.Event)

func (j *journalEventID) key() string {
	return strconv.FormatInt(int64(j.typ), 10) + "_" + strconv.FormatInt(j.id, 10)
}
