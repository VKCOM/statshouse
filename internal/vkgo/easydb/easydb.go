// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package easydb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3" // SQLite driver for database/sql

	"github.com/VKCOM/statshouse/internal/vkgo/semaphore"
)

const (
	sqliteDriver = "sqlite3"
)

type DB struct {
	sem  *semaphore.Weighted
	dbx  *sqlx.DB
	stmt map[string]*sqlx.Stmt
}

type Tx struct {
	ctx  context.Context
	txx  *sqlx.Tx
	stmt map[string]*sqlx.Stmt
}

func Open(ctx context.Context, path string, busyTimeout time.Duration, schema string) (*DB, error) {
	// Escape the path to avoid callers specifying DSN explicitly. UTF-8 encoding is default.
	dsn := fmt.Sprintf("file:%v?_busy_timeout=%v&_synchronous=FULL", url.QueryEscape(path), int64(busyTimeout/time.Millisecond))

	dbx, err := sqlx.Open(sqliteDriver, dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open DB %q: %w", path, err)
	}

	dbx.SetMaxOpenConns(1) // additionally, we use a mutex for all DB access

	err = dbx.PingContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to ping DB %q: %w", path, err)
	}

	_, err = dbx.ExecContext(ctx, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to apply schema to DB %q: %w", path, err)
	}

	return &DB{
		sem:  semaphore.NewWeighted(1),
		dbx:  dbx,
		stmt: map[string]*sqlx.Stmt{},
	}, nil
}

func (db *DB) Close() error {
	_ = db.sem.Acquire(context.Background(), 1)
	defer db.sem.Release(1)

	for _, stmt := range db.stmt {
		_ = stmt.Close()
	}

	return db.dbx.Close()
}

// Check verifies database integrity. Check runs in O(N log N) time, where N is the database size.
func (db *DB) Check(ctx context.Context) error {
	if err := db.sem.Acquire(ctx, 1); err != nil {
		return err
	}
	defer db.sem.Release(1)

	_, err := db.dbx.ExecContext(ctx, "PRAGMA integrity_check")
	return err
}

// Get executes a query that is expected to return at most one row.
func (db *DB) Get(ctx context.Context, dest interface{}, query string, args ...interface{}) (bool, error) {
	if err := db.sem.Acquire(ctx, 1); err != nil {
		return false, err
	}
	defer db.sem.Release(1)

	stmt, err := prepareLocked(ctx, db.dbx, db.stmt, query)
	if err != nil {
		return false, err
	}

	err = stmt.GetContext(ctx, dest, args...)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// Select executes a query that is expected to return any number of rows.
func (db *DB) Select(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	if err := db.sem.Acquire(ctx, 1); err != nil {
		return err
	}
	defer db.sem.Release(1)

	stmt, err := prepareLocked(ctx, db.dbx, db.stmt, query)
	if err != nil {
		return err
	}

	return stmt.SelectContext(ctx, dest, args...)
}

// Exec executes a query without returning any rows.
func (db *DB) Exec(ctx context.Context, query string, args ...interface{}) error {
	if err := db.sem.Acquire(ctx, 1); err != nil {
		return err
	}
	defer db.sem.Release(1)

	stmt, err := prepareLocked(ctx, db.dbx, db.stmt, query)
	if err != nil {
		return err
	}

	_, err = stmt.ExecContext(ctx, args...)
	return err
}

// Tx executes fn in a transaction.
func (db *DB) Tx(ctx context.Context, fn func(*Tx) error) error {
	if err := db.sem.Acquire(ctx, 1); err != nil {
		return err
	}
	defer db.sem.Release(1)

	txx, err := db.dbx.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = txx.Rollback() }()

	tx := &Tx{
		ctx:  ctx,
		txx:  txx,
		stmt: map[string]*sqlx.Stmt{},
	}
	defer func() {
		for _, stmt := range tx.stmt {
			_ = stmt.Close()
		}
	}()

	err = fn(tx)
	if err != nil {
		return err
	}

	return txx.Commit()
}

// Get executes a query that is expected to return at most one row.
func (tx *Tx) Get(dest interface{}, query string, args ...interface{}) (bool, error) {
	stmt, err := prepareLocked(tx.ctx, tx.txx, tx.stmt, query)
	if err != nil {
		return false, err
	}

	err = stmt.GetContext(tx.ctx, dest, args...)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// Select executes a query that is expected to return any number of rows.
func (tx *Tx) Select(dest interface{}, query string, args ...interface{}) error {
	stmt, err := prepareLocked(tx.ctx, tx.txx, tx.stmt, query)
	if err != nil {
		return err
	}

	return stmt.SelectContext(tx.ctx, dest, args...)
}

// Exec executes a query without returning any rows.
func (tx *Tx) Exec(query string, args ...interface{}) error {
	stmt, err := prepareLocked(tx.ctx, tx.txx, tx.stmt, query)
	if err != nil {
		return err
	}

	_, err = stmt.ExecContext(tx.ctx, args...)
	return err
}

type preparer interface {
	PreparexContext(ctx context.Context, query string) (*sqlx.Stmt, error)
}

func prepareLocked(ctx context.Context, p preparer, cache map[string]*sqlx.Stmt, query string) (*sqlx.Stmt, error) {
	stmt, ok := cache[query]
	if ok {
		return stmt, nil
	}

	stmt, err := p.PreparexContext(ctx, query)
	if err != nil {
		return nil, err
	}
	cache[query] = stmt

	return stmt, nil
}
