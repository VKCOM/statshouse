// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package mapping

import (
	"fmt"
	"time"

	"context"

	"github.com/VKCOM/statshouse/internal/vkgo/easydb"
)

type DB struct {
	db *easydb.DB
}

const schema = `CREATE TABLE IF NOT EXISTS mappings
(
    id   INTEGER PRIMARY KEY,
    name TEXT UNIQUE
);
`

const (
	dbOpenTimeout = 40 * time.Second
	dbBusyTimeout = 20 * time.Second
)

func OpenDB(path string) (*DB, error) {
	ctx, cancel := context.WithTimeout(context.Background(), dbOpenTimeout)
	defer cancel()

	db, err := easydb.Open(ctx, path, dbBusyTimeout, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	return &DB{
		db: db,
	}, nil
}

func (db *DB) Close() error {
	closeErr := db.db.Close()
	return closeErr
}

func (db *DB) GetMappingByValue(ctx context.Context, value string) (int32, bool, error) {
	var id int32
	ok, err := db.db.Get(ctx, &id, "SELECT id FROM mappings where name = $name", value)
	return id, ok, err
}

func (db *DB) CreateMapping(ctx context.Context, id int32, value string) error {
	return db.db.Exec(ctx, "INSERT OR REPLACE INTO mappings (id, name) VALUES ($id, $name)", id, value)
}

func (db *DB) GetMaxMappingID(ctx context.Context) (int32, error) {
	var id int32
	_, err := db.db.Get(ctx, &id, "SELECT COALESCE(MAX(id), 0) FROM mappings")
	return id, err
}
