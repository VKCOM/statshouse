// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metadata

import (
	"fmt"
	"time"

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/sqlite"
	"github.com/vkcom/statshouse/internal/vkgo/binlog/fsbinlog"

	"context"
)

type DBV2 struct {
	ctx    context.Context
	cancel func()
	eng    *sqlite.Engine

	metricValidationFunc func(oldJson, newJson string) error

	now func() time.Time

	lastTimeCommit     time.Time
	MustCommitEveryReq bool

	maxBudget   int64
	stepSec     uint32
	budgetBonus int64

	globalBudget          int64
	lastMappingIDToInsert int32
}

type Options struct {
	Host string

	MaxBudget    int64
	StepSec      uint32
	BudgetBonus  int64
	GlobalBudget int64

	MetricValidationFunc func(oldJson, newJson string) error
	Now                  func() time.Time
	Migration            bool
}

var scheme = `CREATE TABLE IF NOT EXISTS metrics
(
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    name    TEXT UNIQUE NOT NULL,
    version INTEGER UNIQUE NOT NULL,
    updated_at INTEGER NOT NULL,
    data    TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS metrics_id ON metrics (version);
CREATE TABLE IF NOT EXISTS mappings
(
    id   INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS flood_limits
(
    metric_name TEXT PRIMARY KEY,
    last_time_update INTEGER, -- unix ts 
    count_free integer -- доступный бюджет
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS metrics_v2
(
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    name    TEXT NOT NULL,
    version INTEGER UNIQUE NOT NULL,
    updated_at INTEGER NOT NULL,
    deleted_at INTEGER NOT NULL,
    data    TEXT NOT NULL,
    type    INTEGER NOT NULL,
    UNIQUE (type, name)
);
CREATE INDEX IF NOT EXISTS metrics_id_v2 ON metrics_v2 (version);

CREATE TABLE IF NOT EXISTS metrics_v4
(
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    name    TEXT NOT NULL,
    version INTEGER UNIQUE NOT NULL,
    updated_at INTEGER NOT NULL,
    deleted_at INTEGER NOT NULL,
    data    TEXT NOT NULL,
    type    INTEGER NOT NULL,
    UNIQUE (type, name)
) STRICT;
CREATE INDEX IF NOT EXISTS metrics_id_v3 ON metrics_v4 (version);
CREATE INDEX IF NOT EXISTS metrics_id_v3 ON metrics_v4 (type);

CREATE TABLE IF NOT EXISTS metrics_v5
(
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    name    TEXT NOT NULL,
    namespace_id INTEGER NOT NULL,
    version INTEGER UNIQUE NOT NULL,
    updated_at INTEGER NOT NULL,
    deleted_at INTEGER NOT NULL,
    data    TEXT NOT NULL,
    type    INTEGER NOT NULL,
    UNIQUE (namespace_id, type, name)
) STRICT;

CREATE INDEX IF NOT EXISTS metrics_version_v5 ON metrics_v5 (version);
CREATE INDEX IF NOT EXISTS metrics_type_v5 ON metrics_v5 (type);


CREATE TABLE IF NOT EXISTS entity_history
(
    entity_id      INTEGER NOT NULL,
    name    TEXT NOT NULL,
    namespace_id INTEGER NOT NULL,
    version INTEGER UNIQUE NOT NULL,
    updated_at INTEGER NOT NULL,
    deleted_at INTEGER NOT NULL,
    data    TEXT NOT NULL,
    type    INTEGER NOT NULL,
    metadata TEXT NOT NULL,
    UNIQUE (entity_id, version)
) STRICT;
CREATE INDEX IF NOT EXISTS entity_history_version ON entity_history (version);
CREATE INDEX IF NOT EXISTS entity_history_entity_id ON entity_history (entity_id);

CREATE TABLE IF NOT EXISTS __offset_migration
(
	offset INTEGER
);

CREATE TABLE IF NOT EXISTS property
(
    name TEXT PRIMARY KEY,
    data BLOB
);
`

const appId = 0x4d5fa5
const MaxBudget = 1000
const GlobalBudget = 1_000_000
const StepSec = 3600
const BudgetBonus = 10
const bootstrapFieldName = "bootstrap"
const metricCountReadLimit int64 = 1000
const metricBytesReadLimit int64 = 1024 * 1024
const maxResetLimit = 100_00
const entityHistoryMaxResponseSize = 1024 * 1024 * 4

func OpenDB(
	path string,
	opt Options,
	binlog fsbinlog.BinlogReadWrite) (*DBV2, error) {
	if opt.Now == nil {
		opt.Now = time.Now
	}
	if opt.MetricValidationFunc == nil {
		opt.MetricValidationFunc = func(oldJson, newJson string) error {
			return nil
		}
	}
	eng, err := sqlite.OpenEngine(sqlite.Options{
		Path:   path,
		APPID:  appId,
		Scheme: scheme,
	}, binlog, applyScanEvent(false), applyScanEvent(true))
	if err != nil {
		return nil, fmt.Errorf("failed to open engine: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	db := &DBV2{
		ctx:                  ctx,
		cancel:               cancel,
		eng:                  eng,
		metricValidationFunc: opt.MetricValidationFunc,

		budgetBonus:  opt.BudgetBonus,
		stepSec:      opt.StepSec,
		maxBudget:    opt.MaxBudget,
		globalBudget: opt.GlobalBudget,

		now:            opt.Now,
		lastTimeCommit: opt.Now(),
	}

	return db, nil
}

func loadNamespaceName(conn sqlite.Conn, id int64, version int64) (string, error) {
	rows := conn.Query("select_namespace", "SELECT name FROM metrics_v5 WHERE type = $type AND id = $id AND version = $version",
		sqlite.Int64("$type", int64(format.NamespaceEvent)),
		sqlite.Int64("$id", id),
		sqlite.Int64("$version", version),
	)
	if rows.Next() {
		name, err := rows.ColumnBlobString(0)
		if err != nil {
			return "", err
		}
		return name, nil

	}
	if rows.Error() != nil {
		return "", rows.Error()
	}

	return "", errNamespaceNotExists
}

func (db *DBV2) backup(ctx context.Context, prefix string) (string, error) {
	path, _, err := db.eng.Backup(ctx, prefix)
	return path, err
}

func (db *DBV2) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	err := db.eng.Close(ctx)
	if err != nil {
		return fmt.Errorf("failed to close db: %w", err)
	}
	db.cancel()
	return nil
}

func (db *DBV2) JournalEvents(ctx context.Context, sinceVersion int64, page int64) ([]tlmetadata.Event, error) {
	limit := metricCountReadLimit
	if page < limit {
		limit = page
	}
	result := make([]tlmetadata.Event, 0)
	var bytesRead int64
	err := db.eng.Do(ctx, "get_journal", func(conn sqlite.Conn, cache []byte) ([]byte, error) {
		rows := conn.Query("select_journal", "SELECT id, name, version, data, updated_at, type, deleted_at, namespace_id FROM metrics_v5 WHERE version > $version ORDER BY version asc;",
			sqlite.Int64("$version", sinceVersion))
		for rows.Next() {
			id, _ := rows.ColumnInt64(0)
			name, err := rows.ColumnBlobString(1)
			if err != nil {
				return cache, err
			}
			version, _ := rows.ColumnInt64(2)
			data, err := rows.ColumnBlobString(3)
			if err != nil {
				return cache, err
			}
			updatedAt, _ := rows.ColumnInt64(4)
			typ, _ := rows.ColumnInt64(5)
			deletedAt, _ := rows.ColumnInt64(6)
			namespaceID, _ := rows.ColumnInt64(7)
			bytesRead += int64(len(data)) + 20

			event := tlmetadata.Event{
				Id:         id,
				Name:       name,
				Version:    version,
				Data:       data,
				UpdateTime: uint32(updatedAt),
				EventType:  int32(typ),
				Unused:     uint32(deletedAt),
			}
			event.SetNamespaceId(namespaceID)
			result = append(result, event)
			if bytesRead > metricBytesReadLimit {
				break
			}
			if int64(len(result)) >= limit {
				break
			}
		}
		return cache, nil
	})
	return result, err
}

func (db *DBV2) GetEntityVersioned(ctx context.Context, id, version int64) (event tlmetadata.Event, err error) {
	err = db.eng.Do(ctx, "get_entity", func(conn sqlite.Conn, c []byte) ([]byte, error) {
		rows := conn.Query("get_entity", "SELECT entity_id,name, data, metadata, updated_at, namespace_id, type FROM entity_history WHERE entity_id = $id AND version = $version",
			sqlite.Int64("$id", id),
			sqlite.Int64("$version", version))

		if rows.Error() != nil {
			return c, err
		}
		if !rows.Next() {
			return c, data_model.ErrEntityNotExists
		}
		id, _ = rows.ColumnInt64(0)
		name, _ := rows.ColumnBlobString(1)
		data, _ := rows.ColumnBlobString(2)
		metadata, _ := rows.ColumnBlobString(3)
		updatedAt, _ := rows.ColumnInt64(4)
		namespaceID, _ := rows.ColumnInt64(5)
		type_, _ := rows.ColumnInt64(6)
		event = tlmetadata.Event{
			Id:         id,
			Name:       name,
			EventType:  int32(type_),
			Version:    version,
			UpdateTime: uint32(updatedAt),
			Data:       data,
		}
		event.SetNamespaceId(namespaceID)
		event.SetMetadata(metadata)
		return c, rows.Error()
	})
	return event, err
}

func (db *DBV2) SaveEntity(ctx context.Context, name string, id int64, oldVersion int64, newJson string, createMetric, deleteEntity bool, typ int32, metadata string) (tlmetadata.Event, error) {
	updatedAt := db.now().Unix()
	var result tlmetadata.Event
	createFixed := false
	err := db.eng.Do(ctx, "save_entity", func(conn sqlite.Conn, cache []byte) ([]byte, error) {
		resolvedNamespaceID, err := resolveEntity(conn, name, id, oldVersion, newJson, createMetric, deleteEntity, typ)
		if err != nil {
			return cache, err
		}
		if id < 0 {
			rows := conn.Query("select_entity", "SELECT id FROM metrics_v5 WHERE id = $id;",
				sqlite.Int64("$id", id))
			if rows.Error() != nil {
				return cache, rows.Error()
			}

			if rows.Next() {
				createMetric = false
			} else {
				createFixed = true
				createMetric = true
			}
		}
		if !createMetric {
			rows := conn.Query("select_entity", "SELECT id, version, deleted_at FROM metrics_v5 where version = $oldVersion AND id = $id;",
				sqlite.Int64("$oldVersion", oldVersion),
				sqlite.Int64("$id", id))
			if rows.Error() != nil {
				return cache, fmt.Errorf("failed to fetch old metric version: %w", rows.Error())
			}
			if !rows.Next() {
				return cache, errInvalidMetricVersion
			}
			deletedAt, _ := rows.ColumnInt64(2)
			if deleteEntity {
				deletedAt = time.Now().Unix()
			}
			_, err := conn.Exec("update_entity", "UPDATE metrics_v5 SET version = (SELECT IFNULL(MAX(version), 0) + 1 FROM metrics_v5), data = $data, updated_at = $updatedAt, name = $name, deleted_at = $deletedAt, namespace_id = $namespaceId WHERE version = $oldVersion AND id = $id;",
				sqlite.TextString("$data", newJson),
				sqlite.Int64("$updatedAt", updatedAt),
				sqlite.Int64("$oldVersion", oldVersion),
				sqlite.TextString("$name", name),
				sqlite.Int64("$id", id),
				sqlite.Int64("$deletedAt", deletedAt),
				sqlite.Int64("$namespaceId", resolvedNamespaceID))

			if err != nil {
				return cache, fmt.Errorf("failed to update metric: %d, %w", oldVersion, err)
			}
		} else {
			var err error
			if !createFixed {
				id, err = conn.Exec("insert_entity", "INSERT INTO metrics_v5 (version, data, name, updated_at, type, deleted_at, namespace_id) VALUES ( (SELECT IFNULL(MAX(version), 0) + 1 FROM metrics_v5), $data, $name, $updatedAt, $type, 0, $namespaceId);",
					sqlite.TextString("$data", newJson),
					sqlite.TextString("$name", name),
					sqlite.Int64("$updatedAt", updatedAt),
					sqlite.Int64("$type", int64(typ)),
					sqlite.Int64("$namespaceId", resolvedNamespaceID))
			} else {
				id, err = conn.Exec("insert_entity", "INSERT INTO metrics_v5 (id, version, data, name, updated_at, type, deleted_at, namespace_id) VALUES ($id, (SELECT IFNULL(MAX(version), 0) + 1 FROM metrics_v5), $data, $name, $updatedAt, $type, 0, $namespaceId);",
					sqlite.Int64("$id", id),
					sqlite.TextString("$data", newJson),
					sqlite.TextString("$name", name),
					sqlite.Int64("$updatedAt", updatedAt),
					sqlite.Int64("$type", int64(typ)),
					sqlite.Int64("$namespaceId", resolvedNamespaceID))
			}
			if err != nil {
				return cache, fmt.Errorf("failed to put new metric %s: %w", newJson, err)
			}
		}
		row := conn.Query("select_entity", "SELECT id, version, deleted_at FROM metrics_v5 where id = $id;",
			sqlite.Int64("$id", id))
		if !row.Next() {
			return cache, fmt.Errorf("can't get version of new metric(name: %s)", name)
		}
		id, _ = row.ColumnInt64(0)
		version, _ := row.ColumnInt64(1)
		if version == oldVersion {
			return cache, fmt.Errorf("can't update metric %s invalid version", name)
		}
		deletedAt, _ := row.ColumnInt64(2)

		result = tlmetadata.Event{
			Id:         id,
			Version:    version,
			Name:       name,
			Data:       newJson,
			UpdateTime: uint32(updatedAt),
			Unused:     uint32(deletedAt),
			EventType:  typ,
		}
		result.SetNamespaceId(resolvedNamespaceID)
		result.SetMetadata(metadata)
		err = insertHistory(conn, result)
		if err != nil {
			return cache, err
		}
		if createMetric {
			metadataCreatMetricEvent := tlmetadata.CreateEntityEvent{
				Metric: result,
			}
			cache = metadataCreatMetricEvent.WriteBoxed(cache)
		} else {
			metadataEditMetricEvent := tlmetadata.EditEntityEvent{
				Metric:     result,
				OldVersion: oldVersion,
			}
			cache = metadataEditMetricEvent.WriteBoxed(cache)
		}
		if err != nil {
			return cache, fmt.Errorf("can't encode binlog event: %w", err)
		}
		return cache, nil
	})
	return result, err
}

func (db *DBV2) GetHistoryShort(ctx context.Context, id int64) (resp tlmetadata.HistoryShortResponse, err error) {
	err = db.eng.Do(ctx, "get_entity", func(conn sqlite.Conn, c []byte) ([]byte, error) {
		rows := conn.Query("get_entity", "SELECT metadata, version FROM entity_history WHERE entity_id = $id ORDER BY version DESC",
			sqlite.Int64("$id", id))
		size := 0
		for rows.Next() {
			metadata, _ := rows.ColumnBlobString(0)
			version, _ := rows.ColumnInt64(1)
			resp.Events = append(resp.Events, tlmetadata.HistoryShortResponseEvent{
				Version:  version,
				Metadata: metadata,
			})
			size += len(metadata) + 8
			if size > entityHistoryMaxResponseSize {
				break
			}
		}
		return c, rows.Error()
	})
	return resp, err
}

func (db *DBV2) GetMappingByValue(ctx context.Context, value string) (int32, bool, error) {
	var res int32
	var notExists bool
	err := db.eng.Do(ctx, "get_mapping_by_value", func(conn sqlite.Conn, cache []byte) ([]byte, error) {
		row := conn.Query("select_mapping_by_name", "SELECT id FROM mappings where name = $name", sqlite.BlobString("$name", value))
		if row.Next() {
			id, _ := row.ColumnInt64(0)
			res = int32(id)
		} else {
			notExists = true
		}
		return cache, nil
	})
	return res, notExists, err
}

// TODO - remove after debug or leave for the future
func (db *DBV2) PrintAllMappings(ctx context.Context) error {
	err := db.eng.Do(ctx, "print_mappings", func(conn sqlite.Conn, cache []byte) ([]byte, error) {
		row := conn.Query("select_all_mappings", "SELECT id, name FROM mappings order by name")
		for row.Next() {
			id, _ := row.ColumnInt64(0)
			name, _ := row.ColumnBlobString(1)
			fmt.Printf("%d <-> %s\n", id, name)
		}
		return cache, nil
	})
	return err
}

func (db *DBV2) GetMappingByID(ctx context.Context, id int32) (string, bool, error) {
	var res string
	var isExists bool
	err := db.eng.Do(ctx, "get_mapping_by_key", func(conn sqlite.Conn, cache []byte) ([]byte, error) {
		var err error
		res, isExists, err = getMappingByID(conn, id)
		return cache, err
	})
	return res, isExists, err
}

func getMappingByID(conn sqlite.Conn, id int32) (k string, isExists bool, err error) {
	row := conn.Query("select_mapping_by_id", "SELECT name FROM mappings where id = $id", sqlite.Int64("$id", int64(id)))
	if row.Next() {
		k, err = row.ColumnBlobString(0)
		if err != nil {
			return "", false, err
		}
		return k, true, err
	}
	return "", false, nil
}

func (db *DBV2) ResetFlood(ctx context.Context, metric string, limit int64) (before int64, after int64, _ error) {
	err := db.eng.Do(ctx, "reset_flood", func(conn sqlite.Conn, cache []byte) ([]byte, error) {
		var err error
		before, err = db.getFreeCount(conn)
		if err != nil {
			return cache, err
		}
		if limit <= 0 {
			after = db.maxBudget
			_, err = conn.Exec("delete_flood_limit", "DELETE FROM flood_limits WHERE metric_name = $name",
				sqlite.BlobString("$name", metric))
		} else {
			after = limit
			if after > maxResetLimit {
				after = maxResetLimit
			}
			_, err = conn.Exec("insert_flood_limit", "INSERT OR REPLACE INTO flood_limits (last_time_update, count_free, metric_name) VALUES ($t, $c, $name)",
				sqlite.Int64("$t", db.now().Unix()),
				sqlite.Int64("$c", after),
				sqlite.BlobString("$name", metric))
		}
		return cache, err
	})
	return before, after, err
}

func (db *DBV2) getFreeCount(conn sqlite.Conn) (actualLimit int64, _ error) {
	rows := conn.Query("test", "SELECT count_free FROM flood_limits WHERE metric_name = $m", sqlite.BlobString("$m", "abc2"))
	if rows.Next() {
		actualLimit, _ = rows.ColumnInt64(0)
	} else {
		actualLimit = db.maxBudget
	}
	return actualLimit, rows.Error()
}

func (db *DBV2) GetOrCreateMapping(ctx context.Context, metricName, key string) (tlmetadata.GetMappingResponse, error) {
	var resp tlmetadata.GetMappingResponse
	now := db.now()
	err := db.eng.Do(ctx, "get_or_create_mapping", func(conn sqlite.Conn, cache []byte) ([]byte, error) {
		var err error
		resp, cache, err = getOrCreateMapping(conn, cache, metricName, key, now, db.globalBudget, db.maxBudget, db.budgetBonus, db.stepSec, db.lastMappingIDToInsert)
		if resp.IsCreated() {
			created, _ := resp.AsCreated()
			db.lastMappingIDToInsert = created.Id
		}
		return cache, err
	})
	if err != nil {
		return resp, fmt.Errorf("failed to create mapping: %w", err)
	}
	return resp, err
}

func (db *DBV2) PutMapping(ctx context.Context, ks []string, vs []int32) error {
	if len(ks) != len(vs) {
		return fmt.Errorf("can't match keys size and values size")
	}
	return db.eng.Do(ctx, "put_mapping", func(conn sqlite.Conn, cache []byte) ([]byte, error) {
		return putMapping(conn, cache, ks, vs)
	})
}

func (db *DBV2) GetBootstrap(ctx context.Context) (tlstatshouse.GetTagMappingBootstrapResult, error) {
	res := tlstatshouse.GetTagMappingBootstrapResult{}
	err := db.eng.Do(ctx, "get_bootstrap", func(conn sqlite.Conn, cache []byte) ([]byte, error) {
		rows := conn.Query("select_bootstrap", "SELECT data FROM property WHERE name = $name",
			sqlite.BlobString("$name", bootstrapFieldName))
		if rows.Error() != nil {
			return cache, rows.Error()
		}
		if rows.Next() {
			resBytes, err := rows.ColumnBlobRaw(0)
			if err != nil {
				return cache, err
			}
			_, err = res.Read(resBytes)
			if err != nil {
				return cache, err
			}
		}
		return cache, nil
	})
	return res, err
}

func (db *DBV2) PutBootstrap(ctx context.Context, mappings []tlstatshouse.Mapping) (int32, error) {
	var count int32
	err := db.eng.Do(ctx, "put_bootstrap", func(conn sqlite.Conn, cache []byte) ([]byte, error) {
		var err error
		count, cache, err = applyPutBootstrap(conn, cache, mappings)
		return cache, err
	})
	return count, err
}

func calcBudget(oldBudget, expense int64, lastTimeUpdate, now uint32, max, bonusToStep int64, stepSec uint32) int64 {
	if oldBudget > max {
		return oldBudget - expense
	}
	res := oldBudget - expense + int64((now-lastTimeUpdate)/stepSec)*bonusToStep
	if res >= max {
		res = max - expense
	}
	return res
}

func roundTime(now time.Time, step uint32) (pred uint32) {
	nowUnix := uint32(now.Unix())
	return nowUnix - (nowUnix % step)
}

func (db *DBV2) SaveEntityold(ctx context.Context, name string, id int64, oldVersion int64, newJson string, createMetric, deleteEntity bool, typ int32) (tlmetadata.Event, error) {
	updatedAt := db.now().Unix()
	var result tlmetadata.Event
	createFixed := false
	err := db.eng.Do(ctx, "save_entity", func(conn sqlite.Conn, cache []byte) ([]byte, error) {
		_, err := resolveEntity(conn, name, id, oldVersion, newJson, createMetric, deleteEntity, typ)
		if err != nil {
			return cache, fmt.Errorf("invalid entity: %w", err)
		}
		err = insertHistory(conn, result)
		if err != nil {
			return cache, err
		}
		if id < 0 {
			rows := conn.Query("select_entity", "SELECT id FROM metrics_v4 WHERE id = $id;",
				sqlite.Int64("$id", id))
			if rows.Error() != nil {
				return cache, rows.Error()
			}

			if rows.Next() {
				createMetric = false
			} else {
				createFixed = true
				createMetric = true
			}
		}
		if !createMetric {
			rows := conn.Query("select_entity", "SELECT id, version, deleted_at FROM metrics_v4 where version = $oldVersion AND id = $id;",
				sqlite.Int64("$oldVersion", oldVersion),
				sqlite.Int64("$id", id))
			if rows.Error() != nil {
				return cache, fmt.Errorf("failed to fetch old metric version: %w", rows.Error())
			}
			if !rows.Next() {
				return cache, errInvalidMetricVersion
			}
			deletedAt, _ := rows.ColumnInt64(2)
			if deleteEntity {
				deletedAt = time.Now().Unix()
			}
			_, err := conn.Exec("update_entity", "UPDATE metrics_v4 SET version = (SELECT IFNULL(MAX(version), 0) + 1 FROM metrics_v4), data = $data, updated_at = $updatedAt, name = $name, deleted_at = $deletedAt WHERE version = $oldVersion AND id = $id;",
				sqlite.TextString("$data", newJson),
				sqlite.Int64("$updatedAt", updatedAt),
				sqlite.Int64("$oldVersion", oldVersion),
				sqlite.TextString("$name", name),
				sqlite.Int64("$id", id),
				sqlite.Int64("$deletedAt", deletedAt))

			if err != nil {
				return cache, fmt.Errorf("failed to update metric: %d, %w", oldVersion, err)
			}
		} else {
			var err error
			if !createFixed {
				id, err = conn.Exec("insert_entity", "INSERT INTO metrics_v4 (version, data, name, updated_at, type, deleted_at) VALUES ( (SELECT IFNULL(MAX(version), 0) + 1 FROM metrics_v4), $data, $name, $updatedAt, $type, 0);",
					sqlite.TextString("$data", newJson),
					sqlite.TextString("$name", name),
					sqlite.Int64("$updatedAt", updatedAt),
					sqlite.Int64("$type", int64(typ)))
			} else {
				id, err = conn.Exec("insert_entity", "INSERT INTO metrics_v4 (id, version, data, name, updated_at, type, deleted_at) VALUES ($id, (SELECT IFNULL(MAX(version), 0) + 1 FROM metrics_v4), $data, $name, $updatedAt, $type, 0);",
					sqlite.Int64("$id", id),
					sqlite.TextString("$data", newJson),
					sqlite.TextString("$name", name),
					sqlite.Int64("$updatedAt", updatedAt),
					sqlite.Int64("$type", int64(typ)))
			}
			if err != nil {
				return cache, fmt.Errorf("failed to put new metric %s: %w", newJson, err)
			}
		}
		row := conn.Query("select_entity", "SELECT id, version, deleted_at FROM metrics_v4 where id = $id;",
			sqlite.Int64("$id", id))
		if !row.Next() {
			return cache, fmt.Errorf("can't get version of new metric(name: %s)", name)
		}
		id, _ = row.ColumnInt64(0)
		version, _ := row.ColumnInt64(1)
		if version == oldVersion {
			return cache, fmt.Errorf("can't update metric %s invalid version", name)
		}
		deletedAt, _ := row.ColumnInt64(2)

		result = tlmetadata.Event{
			Id:         id,
			Version:    version,
			Name:       name,
			Data:       newJson,
			UpdateTime: uint32(updatedAt),
			Unused:     uint32(deletedAt),
			EventType:  typ,
		}
		result.SetNamespaceId(0)
		if createMetric {
			metadataCreatMetricEvent := tlmetadata.CreateEntityEvent{
				Metric: result,
			}
			cache = metadataCreatMetricEvent.WriteBoxed(cache)
		} else {
			metadataEditMetricEvent := tlmetadata.EditEntityEvent{
				Metric:     result,
				OldVersion: oldVersion,
			}
			cache = metadataEditMetricEvent.WriteBoxed(cache)
		}
		return cache, nil
	})
	return result, err
}
