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

	binlog2 "github.com/vkcom/statshouse/internal/vkgo/binlog"

	sqlite2 "github.com/vkcom/statshouse/internal/vkgo/sqlite"

	"context"
)

type DBV2 struct {
	ctx    context.Context
	cancel func()
	eng    *sqlite2.Engine

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
const GlobalBudget = 1000000
const StepSec = 3600
const BudgetBonus = 10
const bootstrapFieldName = "bootstrap"
const metricCountReadLimit int64 = 1000
const metricBytesReadLimit int64 = 1024 * 1024

var errInvalidMetricVersion = fmt.Errorf("invalid version")

func OpenDB(
	path string,
	opt Options,
	binlog binlog2.Binlog) (*DBV2, error) {
	if opt.Now == nil {
		opt.Now = time.Now
	}
	if opt.MetricValidationFunc == nil {
		opt.MetricValidationFunc = func(oldJson, newJson string) error {
			return nil
		}
	}
	eng, err := sqlite2.OpenEngine(sqlite2.Options{
		Path:   path,
		APPID:  appId,
		Scheme: scheme,
	}, binlog, applyScanEvent(false), applyScanEvent(true))
	if err != nil {
		return nil, fmt.Errorf("failed to open engine: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	db1 := &DBV2{
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
	return db1, nil
}

func (db *DBV2) backup(ctx context.Context, prefix string) error {
	return db.eng.Backup(ctx, prefix)
}

func (db *DBV2) Close() error {
	err := db.eng.Close()
	if err != nil {
		return fmt.Errorf("failed to close db")
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
	err := db.eng.Do(ctx, func(conn sqlite2.Conn, cache []byte) ([]byte, error) {
		rows := conn.Query("SELECT id, name, version, data, updated_at, type, deleted_at FROM metrics_v2 WHERE version > $version ORDER BY version asc;",
			sqlite2.Int64("$version", sinceVersion))
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
			bytesRead += int64(len(data)) + 20
			if bytesRead > metricBytesReadLimit {
				break
			}
			if int64(len(result)) >= limit {
				break
			}
			result = append(result, tlmetadata.Event{
				Id:         id,
				Name:       name,
				Version:    version,
				Data:       data,
				UpdateTime: uint32(updatedAt),
				EventType:  int32(typ),
				Unused:     uint32(deletedAt),
			})
		}
		return cache, nil
	})
	return result, err
}

func (db *DBV2) PutOldMetric(ctx context.Context, name string, id int64, versionToInsert int64, newJson string, updateTime uint32, typ int32) (tlmetadata.Event, error) {
	metric := tlmetadata.Event{}
	err := db.eng.Do(ctx, func(conn sqlite2.Conn, cache []byte) ([]byte, error) {
		var err error
		metric, cache, err = putEntityWithFixedID(conn, cache, name, id, versionToInsert, newJson, updateTime, typ)
		return cache, err
	})
	return metric, err
}

func (db *DBV2) SaveEntity(ctx context.Context, name string, id int64, oldVersion int64, newJson string, createMetric, deleteEntity bool, typ int32) (tlmetadata.Event, error) {
	updatedAt := db.now().Unix()
	var result tlmetadata.Event
	createFixed := false
	err := db.eng.Do(ctx, func(conn sqlite2.Conn, cache []byte) ([]byte, error) {
		if id < 0 {
			rows := conn.Query("SELECT id FROM metrics_v2 WHERE id = $id;",
				sqlite2.Int64("$id", id))
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
			rows := conn.Query("SELECT id, version, deleted_at FROM metrics_v2 where version = $oldVersion AND id = $id;",
				sqlite2.Int64("$oldVersion", oldVersion),
				sqlite2.Int64("$id", id))
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
			_, err := conn.Exec("UPDATE metrics_v2 SET version = (SELECT IFNULL(MAX(version), 0) + 1 FROM metrics_v2), data = $data, updated_at = $updatedAt, name = $name, deleted_at = $deletedAt WHERE version = $oldVersion AND id = $id;",
				sqlite2.BlobString("$data", newJson),
				sqlite2.Int64("$updatedAt", updatedAt),
				sqlite2.Int64("$oldVersion", oldVersion),
				sqlite2.BlobString("$name", name),
				sqlite2.Int64("$id", id),
				sqlite2.Int64("$deletedAt", deletedAt))

			if err != nil {
				return cache, fmt.Errorf("failed to update metric: %d, %w", oldVersion, err)
			}
		} else {
			var err error
			if !createFixed {
				id, err = conn.Exec("INSERT INTO metrics_v2 (version, data, name, updated_at, type, deleted_at) VALUES ( (SELECT IFNULL(MAX(version), 0) + 1 FROM metrics_v2), $data, $name, $updatedAt, $type, 0);",
					sqlite2.BlobString("$data", newJson),
					sqlite2.BlobString("$name", name),
					sqlite2.Int64("$updatedAt", updatedAt),
					sqlite2.Int64("$type", int64(typ)))
			} else {
				id, err = conn.Exec("INSERT INTO metrics_v2 (id, version, data, name, updated_at, type, deleted_at) VALUES ($id, (SELECT IFNULL(MAX(version), 0) + 1 FROM metrics_v2), $data, $name, $updatedAt, $type, 0);",
					sqlite2.Int64("$id", id),
					sqlite2.BlobString("$data", newJson),
					sqlite2.BlobString("$name", name),
					sqlite2.Int64("$updatedAt", updatedAt),
					sqlite2.Int64("$type", int64(typ)))
			}
			if err != nil {
				return cache, fmt.Errorf("failed to put new metric %s: %w", newJson, err)
			}
		}
		row := conn.Query("SELECT id, version, deleted_at FROM metrics_v2 where id = $id;",
			sqlite2.Int64("$id", id))
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
		var err error
		if createMetric {
			metadataCreatMetricEvent := tlmetadata.CreateEntityEvent{
				Metric: result,
			}
			cache, err = metadataCreatMetricEvent.WriteBoxed(cache)
		} else {
			metadataEditMetricEvent := tlmetadata.EditEntityEvent{
				Metric:     result,
				OldVersion: oldVersion,
			}
			cache, err = metadataEditMetricEvent.WriteBoxed(cache)
		}
		if err != nil {
			return cache, fmt.Errorf("can't encode binlog event: %w", err)
		}
		return cache, nil
	})
	return result, err
}

func (db *DBV2) GetMappingByValue(ctx context.Context, key string) (int32, bool, error) {
	var res int32
	var notExists bool
	err := db.eng.Do(ctx, func(conn sqlite2.Conn, cache []byte) ([]byte, error) {
		row := conn.Query("SELECT id FROM mappings where name = $name", sqlite2.BlobString("$name", key))
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
	err := db.eng.Do(ctx, func(conn sqlite2.Conn, cache []byte) ([]byte, error) {
		row := conn.Query("SELECT id, name FROM mappings order by name")
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
	err := db.eng.Do(ctx, func(conn sqlite2.Conn, cache []byte) ([]byte, error) {
		var err error
		res, isExists, err = getMappingByID(conn, id)
		return cache, err
	})
	return res, isExists, err
}

func getMappingByID(conn sqlite2.Conn, id int32) (k string, isExists bool, err error) {
	row := conn.Query("SELECT name FROM mappings where id = $id", sqlite2.Int64("$id", int64(id)))
	if row.Next() {
		k, err = row.ColumnBlobString(0)
		if err != nil {
			return "", false, err
		}
		return k, true, err
	}
	return "", false, nil
}

func (db *DBV2) ResetFlood(ctx context.Context, metric string) error {
	return db.eng.Do(ctx, func(conn sqlite2.Conn, cache []byte) ([]byte, error) {
		_, err := conn.Exec("DELETE FROM flood_limits WHERE metric_name = $name",
			sqlite2.BlobString("$name", metric))
		return cache, err
	})
}

func (db *DBV2) GetOrCreateMapping(ctx context.Context, metricName, key string) (tlmetadata.GetMappingResponseUnion, error) {
	var resp tlmetadata.GetMappingResponseUnion
	now := db.now()
	err := db.eng.Do(ctx, func(conn sqlite2.Conn, cache []byte) ([]byte, error) {
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
	return db.eng.Do(ctx, func(conn sqlite2.Conn, cache []byte) ([]byte, error) {
		return putMapping(conn, cache, ks, vs)
	})
}

func (db *DBV2) GetBootstrap(ctx context.Context) (tlstatshouse.GetTagMappingBootstrapResult, error) {
	res := tlstatshouse.GetTagMappingBootstrapResult{}
	err := db.eng.Do(ctx, func(conn sqlite2.Conn, cache []byte) ([]byte, error) {
		rows := conn.Query("SELECT data FROM property WHERE name = $name",
			sqlite2.BlobString("$name", bootstrapFieldName))
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
	err := db.eng.Do(ctx, func(conn sqlite2.Conn, cache []byte) ([]byte, error) {
		var err error
		count, cache, err = applyPutBootstrap(conn, cache, mappings)
		return cache, err
	})
	return count, err
}

func calcBudget(oldBudget, expense int64, lastTimeUpdate, now uint32, max, bonusToStep int64, stepSec uint32) int64 {
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
