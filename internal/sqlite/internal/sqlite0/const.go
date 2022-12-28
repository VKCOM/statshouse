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

const (
	Busy = C.SQLITE_BUSY
	ok   = C.SQLITE_OK
	row  = C.SQLITE_ROW
	done = C.SQLITE_DONE

	preparePersistent = C.SQLITE_PREPARE_PERSISTENT
)

// https://www.sqlite.org/c3ref/open.html
const (
	OpenReadonly     = C.SQLITE_OPEN_READONLY
	OpenReadWrite    = C.SQLITE_OPEN_READWRITE
	OpenCreate       = C.SQLITE_OPEN_CREATE
	OpenURI          = C.SQLITE_OPEN_URI
	OpenMemory       = C.SQLITE_OPEN_MEMORY
	OpenNoMutex      = C.SQLITE_OPEN_NOMUTEX
	OpenFullMutex    = C.SQLITE_OPEN_FULLMUTEX
	OpenSharedCache  = C.SQLITE_OPEN_SHAREDCACHE
	OpenPrivateCache = C.SQLITE_OPEN_PRIVATECACHE
)
