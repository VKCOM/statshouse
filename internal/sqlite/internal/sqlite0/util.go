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
	"reflect"
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

func ensureZeroTerm(s string) string {
	if len(s) == 0 || s[len(s)-1] != 0 {
		s += "\x00"
	}
	return s
}

func unsafeStringPtr(s string) unsafe.Pointer {
	return unsafe.Pointer((*reflect.StringHeader)(unsafe.Pointer(&s)).Data)
}

func unsafeStringCPtr(s string) *C.char {
	return (*C.char)(unsafeStringPtr(s))
}

func unsafeSlicePtr(b []byte) unsafe.Pointer {
	if b == nil {
		b = emptyBytes
	}
	return unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&b)).Data)
}

func unsafeSlice(p unsafe.Pointer, n int) (b []byte) {
	if n > 0 {
		h := (*reflect.SliceHeader)(unsafe.Pointer(&b))
		h.Data = uintptr(p)
		h.Len = n
		h.Cap = n
	}
	return
}

func unsafeToString(b []byte) (s string) {
	if len(b) > 0 {
		h := (*reflect.StringHeader)(unsafe.Pointer(&s))
		h.Data = (*reflect.SliceHeader)(unsafe.Pointer(&b)).Data
		h.Len = len(b)
	}
	return
}
