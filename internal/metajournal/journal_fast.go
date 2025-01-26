// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metajournal

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/vkcom/statshouse/internal/data_model/gen2/tlmetadata"
)

func getJournalFileLoader(fName string) (MetricsStorageLoader, error) {
	data, err := os.ReadFile(fName)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(data), "\n")
	var events []tlmetadata.Event
	for _, l := range lines {
		if l == "" {
			continue
		}
		values := strings.SplitN(l, "|", 8)
		version, err := strconv.ParseInt(values[0], 10, 64)
		if err != nil {
			return nil, err
		}
		id, err := strconv.ParseInt(values[1], 10, 64)
		if err != nil {
			return nil, err
		}
		tt, err := strconv.ParseInt(values[4], 10, 32)
		if err != nil {
			return nil, err
		}
		nID, err := strconv.ParseInt(values[6], 10, 64)
		if err != nil {
			return nil, err
		}
		event := tlmetadata.Event{
			FieldMask:   0,
			Id:          id,
			Name:        values[2],
			NamespaceId: nID,
			EventType:   int32(tt),
			Version:     version,
			Data:        values[7],
		}
		if len(events) != 0 && version <= events[len(events)-1].Version {
			fmt.Printf("hren")
		}
		events = append(events, event)
	}
	return func(ctx context.Context, lastVersion int64, returnIfEmpty bool) ([]tlmetadata.Event, int64, error) {
		var result []tlmetadata.Event
		for _, e := range events {
			if e.Version > lastVersion {
				result = append(result, e)
			}
			if len(result) >= 100 {
				break
			}
		}
		var v int64
		if len(result) > 0 {
			v = result[len(result)-1].Version
		}
		return result, v, nil
	}, nil
}

func LoadTestJournalFromFile(journal *Journal, fName string) error {
	loader, err := getJournalFileLoader("../internal/metajournal/journal.json")
	if err != nil {
		return err
	}
	journal.metaLoader = loader
	for {
		fin, err := journal.updateJournalIsFinished(nil)
		if err != nil {
			return err
		}
		if fin {
			break
		}
	}
	return nil
}
