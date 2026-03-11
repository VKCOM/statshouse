// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metarqlite

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// we must ensure business logic:
// namespace (prefix of metric and group name before ":") exists
// namespace never renamed
// when editing metric or group, namespace ID does not change.

var scheme = `[
["CREATE TABLE IF NOT EXISTS property (
    name TEXT PRIMARY KEY,
    data BLOB
)"],

["CREATE TABLE IF NOT EXISTS mappings (
    id   INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE
)"],

["CREATE TABLE IF NOT EXISTS flood_limits (
    metric_name TEXT PRIMARY KEY,
    last_time_update INTEGER,
    count_free integer
) WITHOUT ROWID"],

["CREATE TABLE IF NOT EXISTS metrics_v5 (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    name    TEXT NOT NULL,
    namespace_id INTEGER NOT NULL,
    version INTEGER UNIQUE NOT NULL,
    updated_at INTEGER NOT NULL,
    deleted_at INTEGER NOT NULL,
    data    TEXT NOT NULL,
    type    INTEGER NOT NULL,
    UNIQUE (namespace_id, type, name)
) STRICT"],
["CREATE INDEX IF NOT EXISTS metrics_version_v5 ON metrics_v5 (version)"],
["CREATE UNIQUE INDEX metrics_v5_type_name ON metrics_v5 (type, name)"],

["CREATE TABLE IF NOT EXISTS entity_history (
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
) STRICT"],
["CREATE INDEX IF NOT EXISTS entity_history_version ON entity_history (version)"],
["CREATE INDEX IF NOT EXISTS entity_history_entity_id ON entity_history (entity_id)"]
]
`

func (l *RQLiteLoader) CreateSchema(ctx context.Context) error {
	address, _, _ := l.getConfig()
	if address == "" {
		return errors.New("cannot create schema because rqlite addesses not configured")
	}
	// rqlite does not allow newlines in statements
	reqBody := strings.ReplaceAll(scheme, "\n", " ")
	_, err := l.sendRequest(address, "request", true, reqBody)
	if err != nil {
		return fmt.Errorf("failed to create schema: %w", err)
	}
	return nil
}
