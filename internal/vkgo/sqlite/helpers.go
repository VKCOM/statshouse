// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlite

import (
	"fmt"

	"github.com/vkcom/statshouse/internal/vkgo/sqlite/internal/sqlite0"
)

const (
	argByte      = 1
	argByteConst = 2
	argString    = 3
	argInt64     = 4
	argText      = 5
)

type Arg struct {
	name string
	typ  int
	b    []byte
	s    string
	n    int64
}

func Blob(name string, b []byte) Arg {
	return Arg{
		name: name,
		typ:  argByte,
		b:    b,
	}
}

func BlobConst(name string, b []byte) Arg {
	return Arg{
		name: name,
		typ:  argByteConst,
		b:    b,
	}
}

func BlobString(name string, s string) Arg {
	return Arg{
		name: name,
		typ:  argString,
		s:    s,
	}
}

func BlobText(name string, s string) Arg {
	return Arg{
		name: name,
		typ:  argText,
		s:    s,
	}
}

func Int64(name string, n int64) Arg {
	return Arg{
		name: name,
		typ:  argInt64,
		n:    n,
	}
}

func SetLogf(fn func(code int, msg string)) {
	sqlite0.SetLogf(fn)
}

func Version() string {
	return sqlite0.Version()
}

func doSingleROToWALQuery(path string, f func(*Engine) error) error {
	ro, err := openROWAL(path)
	if err != nil {
		return err
	}

	e := &Engine{
		opt: Options{Path: path},
		rw:  newSqliteConn(ro),
	}
	err = f(e)
	for _, si := range e.rw.prep {
		_ = si.stmt.Close()
	}

	closeErr := ro.Close()
	if err != nil {
		return err
	}
	return closeErr
}

func doSingleROQuery(path string, f func(*Engine) error) error {
	conn, err := sqlite0.Open(path, sqlite0.OpenReadonly|sqlite0.OpenNoMutex|sqlite0.OpenPrivateCache)
	if err != nil {
		return err
	}
	err = conn.SetBusyTimeout(busyTimeout)
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("failed to set DB busy timeout to %v: %w", busyTimeout, err)
	}
	e := &Engine{
		opt: Options{Path: path},
		rw:  newSqliteConn(conn),
	}
	err = f(e)
	for _, si := range e.rw.prep {
		_ = si.stmt.Close()
	}

	closeErr := conn.Close()
	if err != nil {
		return err
	}
	return closeErr
}
