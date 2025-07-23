// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metadata

import (
	"fmt"
	"time"

	"github.com/VKCOM/statshouse/internal/sqlite"

	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/VKCOM/statshouse/internal/vkgo/basictl"

	binlog2 "github.com/VKCOM/statshouse/internal/vkgo/binlog"
	"github.com/VKCOM/statshouse/internal/vkgo/binlog/fsbinlog"
)

func applyScanEvent(scanOnly bool) func(conn sqlite.Conn, offset int64, data []byte) (int, error) {
	return func(conn sqlite.Conn, offset int64, data []byte) (int, error) {
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

// deprecated
func applyEditMetricEvent(conn sqlite.Conn, event tlmetadata.EditMetricEvent) error {
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

func insertHistory(conn sqlite.Conn, event tlmetadata.Event) error {
	if !event.IsSetMetadata() {
		return nil
	}
	_, err := conn.Exec("insert_entity", "INSERT INTO entity_history (entity_id, version, data, name, updated_at, type, deleted_at, namespace_id, metadata) VALUES ($entity_id, $version, $data, $name, $updatedAt, $type, $deletedAt, $namespaceId, $metadata);",
		sqlite.TextString("$data", event.Data),
		sqlite.TextString("$name", event.Name),
		sqlite.Int64("$updatedAt", int64(event.UpdateTime)),
		sqlite.Int64("$entity_id", event.Id),
		sqlite.Int64("$version", event.Version),
		sqlite.Int64("$type", int64(event.EventType)),
		sqlite.Int64("$deletedAt", int64(event.Unused)),
		sqlite.Int64("$namespaceId", event.NamespaceId),
		sqlite.TextString("$metadata", event.Metadata))
	if err != nil {
		return fmt.Errorf("failed to put history event: %w", err)
	}
	return nil
}

func applyEditEntityEvent(conn sqlite.Conn, event tlmetadata.EditEntityEvent) error {
	deletedAt := event.Metric.Unused
	_, err := conn.Exec("edit_entity", "UPDATE metrics_v5 SET version = $newVersion, data = $data, updated_at = $updatedAt, deleted_at = $deletedAt, namespace_id = $namespaceId WHERE version = $oldVersion AND name = $name AND id = $id;",
		sqlite.TextString("$data", event.Metric.Data),
		sqlite.Int64("$updatedAt", int64(event.Metric.UpdateTime)),
		sqlite.Int64("$oldVersion", event.OldVersion),
		sqlite.TextString("$name", event.Metric.Name),
		sqlite.Int64("$id", event.Metric.Id),
		sqlite.Int64("$newVersion", event.Metric.Version),
		sqlite.Int64("$deletedAt", int64(deletedAt)),
		sqlite.Int64("$namespaceId", event.Metric.NamespaceId))
	if err != nil {
		return fmt.Errorf("failed to update metric: %w", err)
	}
	err = insertHistory(conn, event.Metric)
	if err != nil {
		return fmt.Errorf("failed to update metric: %w", err)
	}
	return nil
}

// todo support metric namespacing
func applyCreateMappingEvent(conn sqlite.Conn, event tlmetadata.CreateMappingEvent) error {
	_, err := conn.Exec("insert_flood_limit", "INSERT OR REPLACE INTO flood_limits (last_time_update, count_free, metric_name) VALUES ($t, $c, $name)",
		sqlite.Int64("$t", int64(event.UpdatedAt)),
		sqlite.Int64("$c", event.Budget),
		sqlite.BlobString("$name", event.Metric))
	if err != nil {
		return err
	}
	_, err = conn.Exec("insert_mapping", "INSERT INTO mappings (name, id) VALUES ($name, $id)",
		sqlite.BlobString("$name", event.Key),
		sqlite.Int64("$id", int64(event.Id)),
	)
	return err
}

// deprecated
func applyCreateMetricEvent(conn sqlite.Conn, event tlmetadata.CreateMetricEvent) error {
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

func applyCreateEntityEvent(conn sqlite.Conn, event tlmetadata.CreateEntityEvent) error {
	_, err := conn.Exec("insert_entity", "INSERT INTO metrics_v5 (id, version, data, name, updated_at, type, deleted_at, namespace_id) VALUES ($id, $version, $data, $name, $updatedAt, $type, $deletedAt, $namespaceId);",
		sqlite.TextString("$data", event.Metric.Data),
		sqlite.TextString("$name", event.Metric.Name),
		sqlite.Int64("$updatedAt", int64(event.Metric.UpdateTime)),
		sqlite.Int64("$id", event.Metric.Id),
		sqlite.Int64("$version", event.Metric.Version),
		sqlite.Int64("$type", int64(event.Metric.EventType)),
		sqlite.Int64("$deletedAt", int64(event.Metric.Unused)),
		sqlite.Int64("$namespaceId", event.Metric.NamespaceId))
	if err != nil {
		return fmt.Errorf("failed to put new metric: %w", err)
	}
	err = insertHistory(conn, event.Metric)
	if err != nil {
		return fmt.Errorf("failed to put new metric: %w", err)
	}
	return nil
}

func getOrCreateMapping(conn sqlite.Conn, cache []byte, metricName, key string, now time.Time, globalBudget, maxBudget, budgetBonus int64, stepSec uint32, lastCreatedID int32) (tlmetadata.GetMappingResponse, []byte, error) {
	var id int32
	row := conn.Query("select_mapping", "SELECT id FROM mappings where name = $name;", sqlite.BlobString("$name", key))
	if row.Error() != nil {
		return tlmetadata.GetMappingResponse{}, cache, row.Error()
	}
	if row.Next() {
		resp, _ := row.ColumnInt64(0)
		id := int32(resp)
		return tlmetadata.GetMappingResponse0{Id: id}.AsUnion(), cache, nil
	}
	pred := roundTime(now, stepSec)
	var countToInsert = maxBudget
	var timeUpdate uint32
	var count int64
	row = conn.Query("select_flood_limit", "SELECT last_time_update, count_free from flood_limits WHERE metric_name = $name",
		sqlite.BlobString("$name", metricName))
	var err error
	metricLimitIsExists := row.Next()
	if row.Error() != nil {
		return tlmetadata.GetMappingResponse0{Id: id}.AsUnion(), cache, row.Error()
	}
	skipFloodLimitModification := lastCreatedID > 0 && int64(lastCreatedID) <= globalBudget
	if metricLimitIsExists {
		lastTimeUpdate, _ := row.ColumnInt64(0)
		timeUpdate = uint32(lastTimeUpdate)
		count, _ = row.ColumnInt64(1)
		if !skipFloodLimitModification {
			countToInsert = calcBudget(count, 1, timeUpdate, pred, maxBudget, budgetBonus, stepSec)
			if countToInsert < 0 {
				return tlmetadata.GetMappingResponseFloodLimitError{}.AsUnion(), cache, nil
			}
		}
		_, err = conn.Exec("update_flood_limit", "UPDATE flood_limits SET last_time_update = $t, count_free = $c WHERE metric_name = $name",
			sqlite.Int64("$t", int64(pred)),
			sqlite.Int64("$c", countToInsert),
			sqlite.BlobString("$name", metricName))
		if err != nil {
			return tlmetadata.GetMappingResponse{}, cache, err
		}
	} else {
		countToInsert = maxBudget - 1
		_, err = conn.Exec("insert_flood_limit", "INSERT INTO flood_limits (last_time_update, count_free, metric_name) VALUES ($t, $c, $name)",
			sqlite.Int64("$t", int64(pred)),
			sqlite.Int64("$c", countToInsert),
			sqlite.BlobString("$name", metricName))
	}
	if err != nil {
		return tlmetadata.GetMappingResponse{}, cache, fmt.Errorf("failed to update flood limits: %w", err)
	}

	idResp, err := conn.Exec("insert_mapping", "INSERT INTO mappings (name) VALUES ($name)", sqlite.BlobString("$name", key))
	if err != nil {
		return tlmetadata.GetMappingResponse{}, cache, fmt.Errorf("failed to insert mapping: %w", err)
	}
	id = int32(idResp)
	event := tlmetadata.CreateMappingEvent{
		Id:        id,
		Key:       key,
		Metric:    metricName,
		UpdatedAt: pred,
		Budget:    countToInsert,
	}
	event.SetCreate(!metricLimitIsExists)
	eventBytes := event.WriteBoxed(cache)
	return tlmetadata.GetMappingResponseCreated{Id: id}.AsUnion(), eventBytes, nil
}

func putMapping(conn sqlite.Conn, cache []byte, ks []string, vs []int32) ([]byte, error) {
	for i := range ks {
		_, err := conn.Exec("upsert_mapping", "INSERT OR REPLACE INTO mappings(id, name) VALUES($id, $name);", sqlite.Int64("$id", int64(vs[i])), sqlite.BlobString("$name", ks[i]))
		if err != nil {
			return cache, err
		}
	}

	event := tlmetadata.PutMappingEvent{
		Keys:  ks,
		Value: vs,
	}
	cache = event.WriteBoxed(cache)
	return cache, nil
}

func applyPutBootstrap(conn sqlite.Conn, cache []byte, mappings []tlstatshouse.Mapping) (int32, []byte, error) {
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
	bytes := res.Write(nil)
	_, err := conn.Exec("upsert_bootstrap", "INSERT OR REPLACE INTO property (name, data) VALUES ($name, $data)",
		sqlite.BlobString("$name", bootstrapFieldName),
		sqlite.Blob("$data", bytes))
	if err != nil {
		return 0, cache, err
	}
	event := tlmetadata.PutBootstrapEvent{
		Mappings: filteredMappings,
	}
	cache = event.WriteBoxed(cache)
	return int32(len(filteredMappings)), cache, nil
}
