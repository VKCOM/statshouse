// Copyright 2025 V Kontakte LLC
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

	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlmetadata"
)

//sqlite> .once journal.json
//sqlite> SELECT version, id, name, updated_at, type, deleted_at, namespace_id, REPLACE(data, CHAR(10), '\n') FROM metrics_v5 ORDER BY version asc;
//sqlite> .quit

func getJournalFileLoader(fName string) (MetricsStorageLoader, []tlmetadata.Event, error) {
	data, err := os.ReadFile(fName)
	if err != nil {
		return nil, nil, err
	}
	lines := strings.Split(string(data), "\n")
	//var newData bytes.Buffer
	var events []tlmetadata.Event
	for i, l := range lines {
		if l == "" {
			continue
		}
		values := strings.SplitN(l, "|", 8)
		version, err := strconv.ParseInt(values[0], 10, 64)
		if err != nil {
			return nil, nil, err
		}
		id, err := strconv.ParseInt(values[1], 10, 64)
		if err != nil {
			return nil, nil, err
		}
		updateTime, err := strconv.ParseUint(values[3], 10, 32)
		if err != nil {
			return nil, nil, err
		}
		tt, err := strconv.ParseInt(values[4], 10, 32)
		if err != nil {
			return nil, nil, err
		}
		_, err = strconv.ParseUint(values[5], 10, 32)
		if err != nil {
			return nil, nil, err
		}
		nID, err := strconv.ParseInt(values[6], 10, 64)
		if err != nil {
			return nil, nil, err
		}
		event := tlmetadata.Event{
			FieldMask:   0,
			Id:          id,
			Name:        values[2],
			NamespaceId: nID,
			UpdateTime:  uint32(updateTime),
			EventType:   int32(tt),
			Version:     version,
			Data:        values[7],
		}
		if len(events) != 0 && version <= events[len(events)-1].Version {
			return nil, nil, fmt.Errorf("events not sorted at %d", i)
		}
		events = append(events, event)
		//if ok, _ := compactJournalEvent(&event); ok {
		//	values[7] = event.Data
		//	newData.WriteString(strings.Join(values, "|"))
		//	newData.WriteString("\n")
		//	if event.Data != `{"visible":true}` && event.Data != `{}` && event.Data != `{"visible":true,"kind":"value_p"}` {
		//		newData.WriteString(event.Data)
		//		newData.WriteString("\n")
		//}
		//}
	}
	// _ = os.WriteFile(fName+".short", newData.Bytes(), 0666)
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
	}, events, nil
}

func LoadTestJournalFromFile(journal *JournalFast, fName string) error {
	loader, _, err := getJournalFileLoader("../internal/metajournal/journal.json")
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
