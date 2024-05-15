// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlite

import (
	"fmt"

	"go.uber.org/multierr"

	"github.com/vkcom/statshouse/internal/sqlite/sqlite0"
)

func SetLogf(fn func(code int, msg string)) {
	sqlite0.SetLogf(fn)
}

func Version() string {
	return sqlite0.Version()
}

func doSingleROToWALQuery(path string, pageSize int, f func(conn *sqliteConn) error) (err error) {
	ro, err := openROWAL(path, pageSize)
	if err != nil {
		return err
	}
	conn := newSqliteConn(ro, 10)
	defer func() {
		err = multierr.Append(err, conn.Close())
	}()
	return f(conn)
}

func doSingleROQuery(path string, f func(*Engine) error) error {
	conn, err := sqlite0.Open(path, sqlite0.OpenReadonly)
	if err != nil {
		return err
	}
	err = conn.SetBusyTimeout(busyTimeout)
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("failed to set DB busy timeout to %v: %w", busyTimeout, err)
	}
	e := &Engine{
		opt: Options{Path: path, StatsOptions: StatsOptions{}},
		rw:  newSqliteConn(conn, 10),
	}
	err = f(e)
	e.rw.cache.close(&err)
	closeErr := conn.Close()
	if err != nil {
		return err
	}
	return closeErr
}
