// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlite0

/*
#include "sqlite3.h"
*/
import "C"

const (
	// we don't really expect end-users to encounter BUSY, as we should encapsulate all concurrent access properly
	// see also: https://www.sqlite.org/wal.html#sometimes_queries_return_sqlite_busy_in_wal_mode
	busy = C.SQLITE_BUSY

	ok   = C.SQLITE_OK
	row  = C.SQLITE_ROW
	done = C.SQLITE_DONE
)

// https://www.sqlite.org/c3ref/open.html
const (
	OpenReadonly  = C.SQLITE_OPEN_READONLY
	OpenReadWrite = C.SQLITE_OPEN_READWRITE
	OpenCreate    = C.SQLITE_OPEN_CREATE
	OpenMemory    = C.SQLITE_OPEN_MEMORY

	openNoMutex      = C.SQLITE_OPEN_NOMUTEX
	openPrivateCache = C.SQLITE_OPEN_PRIVATECACHE
)
