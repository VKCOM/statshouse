// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlite0

/*
#cgo CFLAGS: -std=gnu99
#cgo CFLAGS: -Os

#cgo CFLAGS: -DSQLITE_THREADSAFE=2
#cgo CFLAGS: -DSQLITE_POWERSAFE_OVERWRITE=1
#cgo CFLAGS: -DSQLITE_DIRECT_OVERFLOW_READ
#cgo CFLAGS: -DSQLITE_DEFAULT_WAL_SYNCHRONOUS=1
#cgo CFLAGS: -DSQLITE_USE_ALLOCA
#cgo CFLAGS: -DHAVE_MALLOC_USABLE_SIZE=1
#cgo CFLAGS: -DHAVE_FDATASYNC=1
#cgo CFLAGS: -DHAVE_USLEEP=1
#cgo CFLAGS: -DSQLITE_DQS=0
#cgo CFLAGS: -DSQLITE_DEFAULT_MEMSTATUS=0
#cgo CFLAGS: -DSQLITE_LIKE_DOESNT_MATCH_BLOBS
#cgo CFLAGS: -DSQLITE_MAX_EXPR_DEPTH=0
#cgo CFLAGS: -DSQLITE_ENABLE_API_ARMOR
#cgo CFLAGS: -DSQLITE_ENABLE_FTS5
#cgo CFLAGS: -DSQLITE_ENABLE_JSON1
#cgo CFLAGS: -DSQLITE_ENABLE_NORMALIZE
#cgo CFLAGS: -DSQLITE_ENABLE_PREUPDATE_HOOK
#cgo CFLAGS: -DSQLITE_ENABLE_RTREE
#cgo CFLAGS: -DSQLITE_ENABLE_SETLK_TIMEOUT
#cgo CFLAGS: -DSQLITE_ENABLE_SNAPSHOT
#cgo CFLAGS: -DSQLITE_ENABLE_UNLOCK_NOTIFY
#cgo CFLAGS: -DSQLITE_OMIT_AUTOINIT
#cgo CFLAGS: -DSQLITE_OMIT_DECLTYPE
#cgo CFLAGS: -DSQLITE_OMIT_DEPRECATED
#cgo CFLAGS: -DSQLITE_OMIT_LOAD_EXTENSION
#cgo CFLAGS: -DSQLITE_OMIT_PROGRESS_CALLBACK
#cgo CFLAGS: -DSQLITE_OMIT_SHARED_CACHE
#cgo CFLAGS: -DSQLITE_OMIT_UTF16
#cgo CFLAGS: -DSQLITE_MAX_MMAP_SIZE=35184372088832ll
#cgo CFLAGS: -DSQLITE_ENABLE_UPDATE_DELETE_LIMIT
#cgo CFLAGS: -DSQLITE_DEFAULT_WAL_AUTOCHECKPOINT=0

#cgo unsafe CFLAGS: -DSQLITE_MMAP_READWRITE

#cgo debug CFLAGS: -DSQLITE_DEBUG
#cgo debug CFLAGS: -DSQLITE_ENABLE_EXPENSIVE_ASSERT
#cgo debug CFLAGS: -DSQLITE_DEBUG_OS_TRACE=1
#cgo debug CFLAGS: -DSQLITE_FORCE_OS_TRACE

#cgo linux CFLAGS: -DHAVE_PREAD64=1 -DHAVE_PWRITE64=1
#cgo darwin CFLAGS: -DHAVE_PREAD=1 -DHAVE_PWRITE=1
#cgo linux LDFLAGS: -lm

#include "sqlite-helpers.h"
*/
import "C"
import (
	"runtime"
	"time"
	"unsafe"
)

var (
	initErr error
)

func init() {
	rc := C._sqlite_enable_logging()
	if rc != ok {
		initErr = sqliteErr(rc, nil, "_sqlite_enable_logging")
	}
	rc = C.sqlite3_initialize()
	if rc != ok {
		initErr = sqliteErr(rc, nil, "sqlite3_initialize")
	}
}

func SetLogf(fn LogFunc) {
	logFuncMu.Lock()
	defer logFuncMu.Unlock()

	logFunc = fn
}

func Version() string {
	if initErr != nil {
		return ""
	}
	return C.GoString(C.sqlite3_libversion())
}

type Conn struct {
	conn        *C.sqlite3
	unlock      *C.unlock
	id          int64
	walSwitchCB func(int, uint)
}

func Open(path string, flags int) (*Conn, error) {
	if initErr != nil {
		return nil, initErr
	}

	cFlags := openNoMutex | openPrivateCache
	for _, f := range []int{OpenReadonly, OpenReadWrite, OpenCreate, OpenMemory} {
		if flags&f != 0 {
			cFlags |= f
		}
	}

	var cConn *C.sqlite3
	path = EnsureZeroTermStr(path)
	rc := C.sqlite3_open_v2(unsafeStringCPtr(path), &cConn, C.int(flags), nil) //nolint:gocritic // nonsense
	runtime.KeepAlive(path)
	if rc != ok {
		err := sqliteErr(rc, cConn, "sqlite3_open_v2")
		C.sqlite3_close_v2(cConn)
		return nil, err
	}

	C.sqlite3_extended_result_codes(cConn, 1)

	rc = C._sqlite_config_defensive(cConn)
	if rc != ok {
		err := sqliteErr(rc, cConn, "_sqlite_config_defensive")
		C.sqlite3_close_v2(cConn)
		return nil, err
	}

	rc = C._sqlite_config_untrusted_schema(cConn)
	if rc != ok {
		err := sqliteErr(rc, cConn, "_sqlite_config_untrusted_schema")
		C.sqlite3_close_v2(cConn)
		return nil, err
	}

	c := &Conn{
		conn:   cConn,
		unlock: C.unlock_alloc(),
	}

	connMu.Lock()
	defer connMu.Unlock()
	c.id = connMaxID
	connMaxID++
	connMap[c.id] = c
	if rc != ok {
		err := sqliteErr(rc, cConn, "_sqlite_set_wal_switch_callback")
		delete(connMap, c.id)
		C.sqlite3_close_v2(cConn)
		return nil, err
	}
	return c, nil
}

func (c *Conn) Close() error {
	var err error
	if c.conn != nil {
		rc := C.sqlite3_close(c.conn)
		if rc != ok {
			err = sqliteErr(rc, nil, "sqlite3_close")
			if rc == busy {
				C.sqlite3_close_v2(c.conn)
			}
		}
		c.conn = nil
	}
	if c.unlock != nil {
		C.unlock_free(c.unlock)
		c.unlock = nil
	}
	connMu.Lock()
	defer connMu.Unlock()
	delete(connMap, c.id)
	return err
}

func (c *Conn) EnableWALSwitchCallback() error {
	p := unsafe.Pointer(&c.id)
	rc := C._sqlite_set_wal_switch_callback(c.conn, p)
	return sqliteErr(rc, c.conn, "_sqlite_set_wal_switch_callback")
}

func (c *Conn) RegisterWALSwitchCallback(walSwitchCB func(int, uint)) {
	c.walSwitchCB = walSwitchCB
}

func (c *Conn) AutoCommit() bool {
	return C.sqlite3_get_autocommit(c.conn) != 0
}

func (c *Conn) SetAutoCheckpoint(n int) error {
	rc := C.sqlite3_wal_autocheckpoint(c.conn, C.int(n))
	return sqliteErr(rc, c.conn, "sqlite3_wal_autocheckpoint")
}

// TODO падать если ошибка отличается от db is locked
func (c *Conn) Checkpoint() error {
	rc := C.sqlite3_wal_checkpoint_v2(c.conn, nil, C.SQLITE_CHECKPOINT_PASSIVE, nil, nil)
	return sqliteErr(rc, c.conn, "sqlite3_wal_checkpoint_v2")
}

func (c *Conn) SetBusyTimeout(dt time.Duration) error {
	rc := C.sqlite3_busy_timeout(c.conn, C.int(dt/time.Millisecond))
	return sqliteErr(rc, c.conn, "sqlite3_busy_timeout")
}

func (c *Conn) Exec(sql string) error {
	sql = EnsureZeroTermStr(sql)
	rc := C.sqlite3_exec(c.conn, unsafeStringCPtr(sql), nil, nil, nil)
	runtime.KeepAlive(sql)
	return sqliteErr(rc, c.conn, "sqlite3_exec")
}

func (c *Conn) LastInsertRowID() int64 {
	id := C.sqlite3_last_insert_rowid(c.conn)
	return int64(id)
}

func (c *Conn) RowsAffected() int64 {
	id := C.sqlite3_changes(c.conn)
	return int64(id)
}

type Stmt struct {
	conn             *Conn
	stmt             *C.sqlite3_stmt
	keepAliveStrings []string
	keepAliveBytes   [][]byte
	params           map[string]int
	n                int
}

func (c *Conn) Prepare(sql []byte) (*Stmt, []byte, error) {
	var cStmt *C.sqlite3_stmt
	var cTail *C.char
	sql = ensureZeroTerm(sql)
	cSQL := unsafeSliceCPtr(sql)
	rc := C._sqlite3_blocking_prepare_v3(c.conn, c.unlock, cSQL, C.int(len(sql)), 0, &cStmt, &cTail) //nolint:gocritic // nonsense
	runtime.KeepAlive(sql)
	if rc != ok {
		return nil, nil, sqliteErr(rc, c.conn, "_sqlite3_blocking_prepare_v3")
	}
	if cStmt == nil {
		return nil, nil, nil
	}

	var tail []byte
	if cTail != nil {
		tailOffset := int(C.str_offset(cSQL, cTail))
		if tailOffset >= 0 && tailOffset < len(sql) {
			tail = sql[tailOffset:]
		}
	}

	n := int(C.sqlite3_bind_parameter_count(cStmt))
	var params map[string]int
	if n > 0 {
		params = make(map[string]int, n)
		for i := 0; i < n; i++ {
			name := C.sqlite3_bind_parameter_name(cStmt, C.int(i+1))
			if name != nil {
				params[C.GoString(name)] = i + 1
			}
		}
	}
	return &Stmt{
		conn:   c,
		stmt:   cStmt,
		params: params,
		n:      n,
	}, tail, nil
}

func (s *Stmt) Close() error {
	rc := C.sqlite3_finalize(s.stmt)
	s.stmt = nil
	return sqliteErr(rc, s.conn.conn, "sqlite3_finalize")
}

func (s *Stmt) SQL() string {
	return C.GoString(C.sqlite3_sql(s.stmt))
}

func (s *Stmt) NormalizedSQL() string {
	return C.GoString(C.sqlite3_normalized_sql(s.stmt))
}

func (s *Stmt) ExpandedSQL() string {
	cStr := C.sqlite3_expanded_sql(s.stmt)
	if cStr == nil {
		return ""
	}
	defer C.sqlite3_free(unsafe.Pointer(cStr))

	return C.GoString(cStr)
}

func (s *Stmt) Reset() error {
	rc := C.sqlite3_reset(s.stmt)
	return sqliteErr(rc, s.conn.conn, "sqlite3_reset")
}

func (s *Stmt) ClearBindings() error { // TODO: use
	rc := C.sqlite3_clear_bindings(s.stmt)
	for i := range s.keepAliveStrings {
		s.keepAliveStrings[i] = ""
	}
	for i := range s.keepAliveBytes {
		s.keepAliveBytes[i] = nil
	}
	return sqliteErr(rc, s.conn.conn, "sqlite3_clear_bindings")
}

func (s *Stmt) Param(name string) int {
	return s.params[name]
}

func (s *Stmt) ParamBytes(name []byte) int {
	return s.params[string(name)]
}

func (s *Stmt) BindNull(param int) error {
	rc := C.sqlite3_bind_null(s.stmt, C.int(param))
	return sqliteErr(rc, s.conn.conn, "sqlite3_bind_null")
}

func (s *Stmt) BindZeroBlob(param int, n int) error {
	rc := C.sqlite3_bind_zeroblob(s.stmt, C.int(param), C.int(n))
	return sqliteErr(rc, s.conn.conn, "sqlite3_bind_zeroblob")
}

func (s *Stmt) BindBlob(param int, v []byte) error {
	if len(v) == 0 {
		return s.BindZeroBlob(param, 0) // micro-optimization
	}
	rc := C._sqlite3_bind_blob(s.stmt, C.int(param), unsafeSlicePtr(v), C.int(len(v)), 1)
	return sqliteErr(rc, s.conn.conn, "_sqlite3_bind_blob")
}

// BindBlobUnsafe caller must ensure that v is immutable during query execution.
func (s *Stmt) BindBlobUnsafe(param int, v []byte) error {
	if len(v) == 0 {
		return s.BindZeroBlob(param, 0) // micro-optimization
	}
	if s.keepAliveBytes == nil {
		s.keepAliveBytes = make([][]byte, s.n)
	}
	s.keepAliveBytes[param-1] = v
	rc := C._sqlite3_bind_blob(s.stmt, C.int(param), unsafeSlicePtr(v), C.int(len(v)), 0)
	return sqliteErr(rc, s.conn.conn, "_sqlite3_bind_blob")
}

func (s *Stmt) BindBlobString(param int, v string) error {
	if len(v) == 0 {
		return s.BindZeroBlob(param, 0) // micro-optimization
	}
	if s.keepAliveStrings == nil {
		s.keepAliveStrings = make([]string, s.n)
	}
	s.keepAliveStrings[param-1] = v
	rc := C._sqlite3_bind_blob(s.stmt, C.int(param), unsafeStringPtr(v), C.int(len(v)), 0)
	return sqliteErr(rc, s.conn.conn, "_sqlite3_bind_blob")
}

func (s *Stmt) BindText(param int, v []byte) error {
	rc := C._sqlite3_bind_text(s.stmt, C.int(param), unsafeSliceCPtr(v), C.int(len(v)), 1)
	return sqliteErr(rc, s.conn.conn, "_sqlite3_bind_text")
}

// BindTextUnsafe caller must ensure that v is immutable during query execution.
func (s *Stmt) BindTextUnsafe(param int, v []byte) error {
	if s.keepAliveBytes == nil {
		s.keepAliveBytes = make([][]byte, s.n)
	}
	s.keepAliveBytes[param-1] = v
	rc := C._sqlite3_bind_text(s.stmt, C.int(param), unsafeSliceCPtr(v), C.int(len(v)), 0)
	return sqliteErr(rc, s.conn.conn, "_sqlite3_bind_text")
}

func (s *Stmt) BindTextString(param int, v string) error {
	if s.keepAliveStrings == nil {
		s.keepAliveStrings = make([]string, s.n)
	}
	s.keepAliveStrings[param-1] = v
	rc := C._sqlite3_bind_text(s.stmt, C.int(param), unsafeStringCPtr(v), C.int(len(v)), 0)
	return sqliteErr(rc, s.conn.conn, "_sqlite3_bind_text")
}

func (s *Stmt) BindInt64(param int, v int64) error {
	rc := C.sqlite3_bind_int64(s.stmt, C.int(param), C.longlong(v))
	return sqliteErr(rc, s.conn.conn, "sqlite3_bind_int64")
}

func (s *Stmt) BindFloat64(param int, v float64) error {
	rc := C.sqlite3_bind_double(s.stmt, C.int(param), C.double(v))
	return sqliteErr(rc, s.conn.conn, "sqlite3_bind_double")
}

func (s *Stmt) Step() (bool, error) {
	rc := C._sqlite3_blocking_step(s.conn.unlock, s.stmt)
	switch rc {
	case row:
		return true, nil
	case done:
		return false, nil
	default:
		return false, sqliteErr(rc, s.conn.conn, "_sqlite3_blocking_step")
	}
}

// ColumnBlobUnsafe can return nil slice both for zero-length BLOB and SQL NULL.
func (s *Stmt) ColumnBlobUnsafe(i int) ([]byte, error) {
	p := C.sqlite3_column_blob(s.stmt, C.int(i))
	if p == nil {
		rc := C.sqlite3_errcode(s.conn.conn)
		if rc != ok && rc != row {
			return nil, sqliteErr(rc, s.conn.conn, "sqlite3_column_blob") // out-of-memory during format conversion
		}
		return nil, nil // zero-length BLOB or SQLStr NULL
	}
	n := C.sqlite3_column_bytes(s.stmt, C.int(i))
	if n == 0 {
		return nil, nil
	}
	return unsafePtrToSlice(p, int(n)), nil
}

func (s *Stmt) ColumnBlob(i int, buf []byte) ([]byte, error) {
	b, err := s.ColumnBlobUnsafe(i)
	return append(buf, b...), err
}

func (s *Stmt) ColumnBlobString(i int) (string, error) {
	b, err := s.ColumnBlobUnsafe(i)
	return string(b), err
}

func (s *Stmt) ColumnTextString(i int) (string, error) {
	b, err := s.ColumnBlobUnsafe(i)
	return string(b), err
}

func (s *Stmt) ColumnBlobUnsafeString(i int) (string, error) {
	b, err := s.ColumnBlobUnsafe(i)
	return unsafeSliceToString(b), err
}

func (s *Stmt) ColumnInt64(i int) int64 {
	value := C.sqlite3_column_int64(s.stmt, C.int(i))
	return int64(value)
}

func (s *Stmt) ColumnFloat64(i int) float64 {
	value := C.sqlite3_column_double(s.stmt, C.int(i))
	return float64(value)
}

func (s *Stmt) ColumnNull(i int) bool {
	typ := C.sqlite3_column_type(s.stmt, C.int(i))
	return typ == C.SQLITE_NULL
}
