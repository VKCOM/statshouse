// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metadata

import (
	"fmt"
	"time"

	"github.com/vkcom/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/vkgo/basictl"

	binlog2 "github.com/vkcom/statshouse/internal/vkgo/binlog"
	"github.com/vkcom/statshouse/internal/vkgo/binlog/fsbinlog"

	sqlite2 "github.com/vkcom/statshouse/internal/vkgo/sqlite"
)

func applyScanEvent(scanOnly bool) func(conn sqlite2.Conn, offset int64, data []byte) (int, error) {
	return func(conn sqlite2.Conn, offset int64, data []byte) (int, error) {
		readCount := 0
		var editMetricEvent tlmetadata.EditMetricEvent
		var createMetricEvent tlmetadata.CreateMetricEvent
		var putMappingEvent tlmetadata.PutMappingEvent
		var createMappingEvent tlmetadata.CreateMappingEvent
		var editEntityEvent tlmetadata.EditEntityEvent
		var createEntityEvent tlmetadata.CreateEntityEvent
		var putBootstrapEvent tlmetadata.PutBootstrapEvent

		var tail []byte
		for len(data) > 0 {
			var tag uint32
			var err error
			tag, data, err = basictl.NatReadTag(data)
			if err != nil {
				return fsbinlog.AddPadding(readCount), err
			}

			switch tag {
			case editMetricEvent.TLTag():
				tail, err = editMetricEvent.Read(data)
				if err != nil {
					return fsbinlog.AddPadding(readCount), err
				}
				if !scanOnly {
					err = applyEditMetricEvent(conn, editMetricEvent)
					if err != nil {
						return fsbinlog.AddPadding(readCount), fmt.Errorf("can't apply binlog event MetadataEditMetricEvent correctly: %w", err)
					}
				}
			case editEntityEvent.TLTag():
				tail, err = editEntityEvent.Read(data)
				if err != nil {
					return fsbinlog.AddPadding(readCount), err
				}
				if !scanOnly {
					err = applyEditEntityEvent(conn, editEntityEvent)
					if err != nil {
						return fsbinlog.AddPadding(readCount), fmt.Errorf("can't apply binlog event MetadataEditMetricEvent correctly: %w", err)
					}
				}
			case createMetricEvent.TLTag():
				tail, err = createMetricEvent.Read(data)
				if err != nil {
					return fsbinlog.AddPadding(readCount), err
				}
				if !scanOnly {
					err = applyCreateMetricEvent(conn, createMetricEvent)
					if err != nil {
						return fsbinlog.AddPadding(readCount), fmt.Errorf("can't apply binlog event MetadataCreateMappingEvent: %w", err)
					}
				}
			case createEntityEvent.TLTag():
				tail, err = createEntityEvent.Read(data)
				if err != nil {
					return fsbinlog.AddPadding(readCount), err
				}
				if !scanOnly {
					err = applyCreateEntityEvent(conn, createEntityEvent)
					if err != nil {
						return fsbinlog.AddPadding(readCount), fmt.Errorf("can't apply binlog event MetadataCreateMappingEvent: %w", err)
					}
				}
			case putMappingEvent.TLTag():
				tail, err = putMappingEvent.Read(data)
				if err != nil {
					return fsbinlog.AddPadding(readCount), err
				}
				if !scanOnly {
					_, err = putMapping(conn, nil, putMappingEvent.Keys, putMappingEvent.Value)
					if err != nil {
						return fsbinlog.AddPadding(readCount), fmt.Errorf("can't apply binlog event MetadataPutMappingEvent: %w", err)
					}
				}
			case createMappingEvent.TLTag():
				tail, err = createMappingEvent.Read(data)
				if err != nil {
					return fsbinlog.AddPadding(readCount), err
				}
				if !scanOnly {
					err = applyCreateMappingEvent(conn, createMappingEvent)
					if err != nil {
						return fsbinlog.AddPadding(readCount), fmt.Errorf("can't apply binlog event MetadataCreateMappingEvent: %w", err)
					}
				}
			case putBootstrapEvent.TLTag():
				tail, err = putBootstrapEvent.Read(data)
				if err != nil {
					return fsbinlog.AddPadding(readCount), err
				}
				if !scanOnly {
					_, _, err := applyPutBootstrap(conn, nil, putBootstrapEvent.Mappings)
					if err != nil {
						return fsbinlog.AddPadding(readCount), fmt.Errorf("can't apply binlog event MetadataPutBootstrapEvent: %w", err)
					}
				}
			default:
				return fsbinlog.AddPadding(readCount), binlog2.ErrorUnknownMagic
			}
			readCount += fsbinlog.AddPadding(4 + len(data) - len(tail))
			data = tail
		}
		return fsbinlog.AddPadding(readCount), nil
	}
}

func applyEditMetricEvent(conn sqlite2.Conn, event tlmetadata.EditMetricEvent) error {
	return applyEditEntityEvent(conn, tlmetadata.EditEntityEvent{
		Metric: tlmetadata.Event{
			Id:         event.Metric.Id,
			Name:       event.Metric.Name,
			EventType:  event.Metric.EventType,
			Version:    event.Metric.Version,
			UpdateTime: event.Metric.UpdateTime,
			Data:       event.Metric.Data,
		},
	})
}

func applyEditEntityEvent(conn sqlite2.Conn, event tlmetadata.EditEntityEvent) error {
	deletedAt := event.Metric.Unused
	_, err := conn.Exec("UPDATE metrics_v2 SET version = $newVersion, data = json($data), updated_at = $updatedAt, deleted_at = $deletedAt WHERE version = $oldVersion AND name = $name AND id = $id;",
		sqlite2.BlobString("$data", event.Metric.Data),
		sqlite2.Int64("$updatedAt", int64(event.Metric.UpdateTime)),
		sqlite2.Int64("$oldVersion", event.OldVersion),
		sqlite2.BlobString("$name", event.Metric.Name),
		sqlite2.Int64("$id", event.Metric.Id),
		sqlite2.Int64("$newVersion", event.Metric.Version),
		sqlite2.Int64("$deletedAt", int64(deletedAt)))
	if err != nil {
		return fmt.Errorf("failed to update metric: %w", err)
	}
	return nil
}

func applyCreateMappingEvent(conn sqlite2.Conn, event tlmetadata.CreateMappingEvent) error {
	var err error
	if event.IsSetCreate() {
		_, err = conn.Exec("INSERT INTO flood_limits (last_time_update, count_free, metric_name) VALUES ($t, $c, $name)",
			sqlite2.Int64("$t", int64(event.UpdatedAt)),
			sqlite2.Int64("$c", event.Badget),
			sqlite2.BlobString("$name", event.Metric))
	} else {
		_, err = conn.Exec("UPDATE flood_limits SET last_time_update = $t, count_free = $c WHERE metric_name = $name",
			sqlite2.Int64("$t", int64(event.UpdatedAt)),
			sqlite2.Int64("$c", event.Badget),
			sqlite2.BlobString("$name", event.Metric))
	}
	if err != nil {
		return err
	}
	_, err = conn.Exec("INSERT INTO mappings (name, id) VALUES ($name, $id)",
		sqlite2.BlobString("$name", event.Key),
		sqlite2.Int64("$id", int64(event.Id)),
	)
	return err
}

func applyCreateMetricEvent(conn sqlite2.Conn, event tlmetadata.CreateMetricEvent) error {
	return applyCreateEntityEvent(conn, tlmetadata.CreateEntityEvent{
		Metric: tlmetadata.Event{
			Id:         event.Metric.Id,
			Name:       event.Metric.Name,
			EventType:  event.Metric.EventType,
			Version:    event.Metric.Version,
			UpdateTime: event.Metric.UpdateTime,
			Data:       event.Metric.Data,
			Unused:     event.Metric.Unused,
		},
	})
}

func applyCreateEntityEvent(conn sqlite2.Conn, event tlmetadata.CreateEntityEvent) error {
	_, err := conn.Exec("INSERT INTO metrics_v2 (id, version, data, name, updated_at, type, deleted_at) VALUES ($id, $version, json($data), $name, $updatedAt, $type, $deletedAt);",
		sqlite2.BlobString("$data", event.Metric.Data),
		sqlite2.BlobString("$name", event.Metric.Name),
		sqlite2.Int64("$updatedAt", int64(event.Metric.UpdateTime)),
		sqlite2.Int64("$id", event.Metric.Id),
		sqlite2.Int64("$version", event.Metric.Version),
		sqlite2.Int64("$type", int64(event.Metric.EventType)),
		sqlite2.Int64("$deletedAt", int64(event.Metric.Unused)))
	if err != nil {
		return fmt.Errorf("failed to put new metric: %w", err)
	}
	return nil
}

func getOrCreateMapping(conn sqlite2.Conn, cache []byte, metricName, key string, now time.Time, globalBudget, maxBudget, budgetBonus int64, stepSec uint32, lastCreatedID int32) (tlmetadata.GetMappingResponseUnion, []byte, error) {
	var id int32
	row := conn.Query("SELECT id FROM mappings where name = $name;", sqlite2.BlobString("$name", key))
	if row.Error() != nil {
		return tlmetadata.GetMappingResponseUnion{}, cache, row.Error()
	}
	if row.Next() {
		resp, _ := row.ColumnInt64(0)
		id := int32(resp)
		return tlmetadata.GetMappingResponse{Id: id}.AsUnion(), cache, nil
	}
	pred := roundTime(now, stepSec)
	var countToInsert = maxBudget
	var timeUpdate uint32
	var count int64
	row = conn.Query("SELECT last_time_update, count_free from flood_limits WHERE metric_name = $name",
		sqlite2.BlobString("$name", metricName))
	if row.Error() != nil {
		return tlmetadata.GetMappingResponse{Id: id}.AsUnion(), cache, row.Error()
	}
	var err error
	metricLimitIsExists := row.Next()
	skipFloodLimitModification := lastCreatedID > 0 && int64(lastCreatedID) <= globalBudget
	if metricLimitIsExists {
		lastTimeUpdate, _ := row.ColumnInt64(0)
		timeUpdate = uint32(lastTimeUpdate)
		count, _ = row.ColumnInt64(1)
		if pred < timeUpdate {
			// todo
			_ = 2
		}
		if !skipFloodLimitModification {
			countToInsert = calcBudget(count, 1, timeUpdate, pred, maxBudget, budgetBonus, stepSec)
			if countToInsert < 0 {
				return tlmetadata.GetMappingResponseFloodLimitError{}.AsUnion(), cache, nil
			}
		}
		_, err = conn.Exec("UPDATE flood_limits SET last_time_update = $t, count_free = $c WHERE metric_name = $name",
			sqlite2.Int64("$t", int64(pred)),
			sqlite2.Int64("$c", countToInsert),
			sqlite2.BlobString("$name", metricName))
		if err != nil {
			return tlmetadata.GetMappingResponseUnion{}, cache, err
		}
	} else {
		countToInsert = maxBudget - 1
		_, err = conn.Exec("INSERT INTO flood_limits (last_time_update, count_free, metric_name) VALUES ($t, $c, $name)",
			sqlite2.Int64("$t", int64(pred)),
			sqlite2.Int64("$c", countToInsert),
			sqlite2.BlobString("$name", metricName))
	}
	if err != nil {
		return tlmetadata.GetMappingResponseUnion{}, cache, fmt.Errorf("failed to update flood limits: %w", err)
	}

	idResp, err := conn.Exec("INSERT INTO mappings (name) VALUES ($name)", sqlite2.BlobString("$name", key))
	if err != nil {
		return tlmetadata.GetMappingResponseUnion{}, cache, fmt.Errorf("failed to insert mapping: %w", err)
	}
	id = int32(idResp)
	event := tlmetadata.CreateMappingEvent{
		Id:        id,
		Key:       key,
		Metric:    metricName,
		UpdatedAt: pred,
		Badget:    countToInsert,
	}
	event.SetCreate(!metricLimitIsExists)
	eventBytes, err := event.WriteBoxed(cache)
	return tlmetadata.GetMappingResponseCreated{Id: id}.AsUnion(), eventBytes, err
}

func putMapping(conn sqlite2.Conn, cache []byte, ks []string, vs []int32) ([]byte, error) {
	for i := range ks {
		_, err := conn.Exec("INSERT OR REPLACE INTO mappings(id, name) VALUES($id, $name);", sqlite2.Int64("$id", int64(vs[i])), sqlite2.BlobString("$name", ks[i]))
		if err != nil {
			return cache, err
		}
	}

	event := tlmetadata.PutMappingEvent{
		Keys:  ks,
		Value: vs,
	}
	cache, err := event.WriteBoxed(cache)
	return cache, err
}

func applyPutBootstrap(conn sqlite2.Conn, cache []byte, mappings []tlstatshouse.Mapping) (int32, []byte, error) {
	filteredMappings := make([]tlstatshouse.Mapping, 0, len(mappings))
	for _, m := range mappings {
		k, isExists, err := getMappingByID(conn, m.Value)
		if err != nil {
			return 0, cache, err
		}
		if !isExists || k != m.Str {
			continue
		}
		filteredMappings = append(filteredMappings, m)
	}
	res := tlstatshouse.GetTagMappingBootstrapResult{Mappings: filteredMappings}
	bytes, err := res.Write(nil)
	if err != nil {
		return 0, cache, err
	}
	_, err = conn.Exec("INSERT OR REPLACE INTO property (name, data) VALUES ($name, $data)",
		sqlite2.BlobString("$name", bootstrapFieldName),
		sqlite2.Blob("$data", bytes))
	if err != nil {
		return 0, cache, err
	}
	event := tlmetadata.PutBootstrapEvent{
		Mappings: filteredMappings,
	}
	cache, err = event.WriteBoxed(cache)
	return int32(len(filteredMappings)), cache, err
}

func putEntityWithFixedID(conn sqlite2.Conn, cache []byte, name string, id int64, versionToInsert int64, newJson string, updateTime uint32, typ int32) (tlmetadata.Event, []byte, error) {
	result := tlmetadata.Event{}
	_, err := conn.Exec("INSERT INTO metrics_v2 (id, version, data, name, updated_at, type, deleted_at) VALUES ($id, $version, json($data), $name, $updatedAt, $type, 0);",
		sqlite2.Int64("$id", id),
		sqlite2.Int64("$version", versionToInsert),
		sqlite2.BlobString("$data", newJson),
		sqlite2.BlobString("$name", name),
		sqlite2.Int64("$updatedAt", int64(updateTime)),
		sqlite2.Int64("$type", int64(typ)))
	if err != nil {
		return result, cache, err
	}
	result = tlmetadata.Event{
		Id:         id,
		Version:    versionToInsert,
		Data:       newJson,
		Name:       name,
		UpdateTime: updateTime,
		EventType:  typ,
	}
	tlEvent := tlmetadata.CreateEntityEvent{
		Metric: result,
	}
	cache, err = tlEvent.WriteBoxed(cache)
	return result, cache, err
}
