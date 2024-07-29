// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlite0

/*
#include "sqlite3.h"
*/
import "C"
import (
	"fmt"
	"sync"
	"unsafe"
)

type LogFunc func(code int, msg string)

var (
	emptyBytes = make([]byte, 0)

	logFunc   LogFunc = func(code int, msg string) {}
	logFuncMu sync.Mutex
)

//export _sqliteLogFunc
func _sqliteLogFunc(_ unsafe.Pointer, cCode C.int, cMsg *C.char) {
	msg := ""
	if cMsg != nil {
		msg = C.GoString(cMsg)
	}

	logFuncMu.Lock()
	defer logFuncMu.Unlock()

	logFunc(int(cCode), msg)
}

type Error struct {
	rc   int
	from string
	msg  string
}

func (err Error) Code() int {
	return err.rc
}

func (err Error) Error() string {
	return fmt.Sprintf("%s: %s [%d]", err.from, err.msg, err.rc)
}

// sqliteErr must be called immediately after the failed operation, without releasing any locks
func sqliteErr(rc C.int, conn *C.sqlite3, from string) error {
	switch {
	case rc == ok:
		return nil
	case conn != nil && rc == C.sqlite3_errcode(conn):
		return Error{int(rc), from, C.GoString(C.sqlite3_errmsg(conn))}
	default:
		return Error{int(rc), from, C.GoString(C.sqlite3_errstr(rc))}
	}
}

func ensureZeroTerm(s []byte) []byte {
	if len(s) == 0 || s[len(s)-1] != 0 {
		return append(s, '\x00')
	}
	return s
}
func ensureZeroTermStr(s string) string {
	if len(s) == 0 || s[len(s)-1] != 0 {
		s += "\x00"
	}
	return s
}

// unsafeSlicePtr always returns a non-nil pointer.
func unsafeSlicePtr(b []byte) unsafe.Pointer {
	if b == nil {
		b = emptyBytes
	}
	return unsafe.Pointer(unsafe.SliceData(b))
}

// unsafeStringPtr always returns a non-nil pointer.
func unsafeStringPtr(s string) unsafe.Pointer {
	if s == "" {
		return unsafeSlicePtr(nil)
	}
	return unsafe.Pointer(unsafe.StringData(s))
}

// unsafeSliceCPtr always returns a non-nil pointer.
func unsafeSliceCPtr(s []byte) *C.char {
	return (*C.char)(unsafeSlicePtr(s))
}

// unsafeStringCPtr always returns a non-nil pointer.
func unsafeStringCPtr(s string) *C.char {
	return (*C.char)(unsafeStringPtr(s))
}

func unsafePtrToSlice(p unsafe.Pointer, n int) (b []byte) {
	if n > 0 {
		b = unsafe.Slice((*byte)(p), n)
	}
	return
}

func unsafeSliceToString(b []byte) (s string) {
	if len(b) > 0 {
		s = unsafe.String(unsafe.SliceData(b), len(b))
	}
	return
}
