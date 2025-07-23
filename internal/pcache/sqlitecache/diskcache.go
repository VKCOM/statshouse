// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlitecache

import (
	"context"
	"errors"
	"os"
	"time"

	"go.uber.org/multierr"

	"github.com/VKCOM/statshouse/internal/pcache"
	"github.com/VKCOM/statshouse/internal/vkgo/easydb"
)

// TODO: faster channel-free implementation once easydb exposes more explicit tx control

const (
	DefaultTxDuration = 60 * time.Second

	cacheOpenTimeout = 40 * time.Second
	cacheBusyTimeout = cacheOpenTimeout / 2

	diskCacheSchema = /* language=SQLite */ `
DROP TABLE IF EXISTS cache_kv;
CREATE TABLE IF NOT EXISTS cache_kv_v2 (
    namespace   BLOB NOT NULL,
    key         BLOB NOT NULL,
    value       BLOB NOT NULL,
    update_time DATETIME NOT NULL,
    ttl         INTEGER NOT NULL,
    PRIMARY KEY (namespace, key)
) WITHOUT ROWID;
`
	selectQuery = /* language=SQLite */ `
SELECT value, update_time, ttl FROM cache_kv_v2 WHERE namespace=? AND key=?
`
	listQuery = /* language=SQLite */ `
SELECT value, update_time, ttl FROM cache_kv_v2 WHERE namespace=?
`
	listKeysQuery = /* language=SQLite */ `
SELECT key FROM cache_kv_v2 WHERE namespace=?
LIMIT ?
OFFSET ?
`
	countQuery = /* language=SQLite */ `
SELECT count(*) as value FROM cache_kv_v2 WHERE namespace=?
`
	updateQuery = /* language=SQLite */ `
REPLACE INTO cache_kv_v2 (namespace, key, value, update_time, ttl) VALUES (?, ?, ?, ?, ?)
`
	eraseQuery = /* language=SQLite */ `
DELETE FROM cache_kv_v2 WHERE namespace=? AND key=?
`
	eraseNamespaceQuery = /* language=SQLite */ `
DELETE FROM cache_kv_v2 WHERE namespace=?
`
)

var (
	errDiskCacheClosed = errors.New("disk cache closed")
)

type diskRead struct {
	ns  string
	key string
	ret chan pcache.ReadResult
}

type diskWrite struct {
	ns              string
	key             string
	val             []byte
	erase           bool
	erase_namespace bool
	update          time.Time
	ttl             time.Duration
	ret             chan error
}

// Rename local ListResult to _listResult and ListKeysResult to _listKeysResult
type _listResult struct {
	Value []pcache.ListResult
	err   error
}

type diskList struct {
	ns  string
	ret chan _listResult
}

type _listKeysResult struct {
	Value []pcache.ListKeysResult
	err   error
}

type diskListKeys struct {
	ns     string
	limit  int
	offset int
	ret    chan _listKeysResult
}

type countResult struct {
	Value int
	err   error
}

type diskCount struct {
	ns  string
	ret chan countResult
}

// SqliteDiskCache implements DiskCache using SQLite as backend.
type SqliteDiskCache struct {
	db         *easydb.DB
	filePath   string
	txDuration time.Duration
	r          chan diskRead
	w          chan diskWrite
	l          chan diskList
	lk         chan diskListKeys
	c          chan diskCount
	closed     chan struct{}
	runErr     chan error
}

// OpenSqliteDiskCache creates a new SqliteDiskCache instance.
func OpenSqliteDiskCache(cacheFilename string, txDuration time.Duration) (*SqliteDiskCache, error) {
	ctx, cancel := context.WithTimeout(context.Background(), cacheOpenTimeout)
	defer cancel()

	db, err := easydb.Open(ctx, cacheFilename, cacheBusyTimeout, diskCacheSchema)
	if err != nil {
		return nil, err
	}

	dc := &SqliteDiskCache{
		db:         db,
		filePath:   cacheFilename,
		txDuration: txDuration,
		r:          make(chan diskRead),
		w:          make(chan diskWrite),
		l:          make(chan diskList),
		lk:         make(chan diskListKeys),
		c:          make(chan diskCount),
		closed:     make(chan struct{}),
		runErr:     make(chan error),
	}

	go func() {
		dc.runErr <- dc.run()
	}()

	return dc, nil
}

func (dc *SqliteDiskCache) DiskSizeBytes() (int64, error) {
	fi, err := os.Stat(dc.filePath)
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

func (dc *SqliteDiskCache) Close() error {
	close(dc.closed)
	closeErr := dc.db.Close()
	runErr := <-dc.runErr
	return multierr.Append(closeErr, runErr)
}

func (dc *SqliteDiskCache) Get(ns string, key string) ([]byte, time.Time, time.Duration, error, bool) {
	ch := make(chan pcache.ReadResult)
	select {
	case dc.r <- diskRead{ns: ns, key: key, ret: ch}:
		ret := <-ch
		return ret.Value, ret.Update, ret.TTL, ret.Err, ret.Found
	case <-dc.closed:
		return nil, time.Time{}, 0, errDiskCacheClosed, false
	}
}

func (dc *SqliteDiskCache) Set(ns string, key string, val []byte, update time.Time, ttl time.Duration) error {
	ch := make(chan error)
	select {
	case dc.w <- diskWrite{ns: ns, key: key, val: val, erase: false, update: update, ttl: ttl, ret: ch}:
		return <-ch
	case <-dc.closed:
		return errDiskCacheClosed
	}
}

func (dc *SqliteDiskCache) List(ns string) ([]pcache.ListResult, error) {
	ch := make(chan _listResult)
	select {
	case dc.l <- diskList{ns: ns, ret: ch}:
		ret := <-ch
		return ret.Value, ret.err
	case <-dc.closed:
		return nil, errDiskCacheClosed
	}
}

func (dc *SqliteDiskCache) ListKeys(ns string, limit, offset int) ([]pcache.ListKeysResult, error) {
	ch := make(chan _listKeysResult)
	select {
	case dc.lk <- diskListKeys{ns: ns, limit: limit, offset: offset, ret: ch}:
		ret := <-ch
		return ret.Value, ret.err
	case <-dc.closed:
		return nil, errDiskCacheClosed
	}
}

func (dc *SqliteDiskCache) VerySlowCountDoNotUse(ns string) (int, error) {
	ch := make(chan countResult)
	select {
	case dc.c <- diskCount{ns: ns, ret: ch}:
		ret := <-ch
		return ret.Value, ret.err
	case <-dc.closed:
		return 0, errDiskCacheClosed
	}
}

func (dc *SqliteDiskCache) Erase(ns string, key string) error {
	ch := make(chan error)
	select {
	case dc.w <- diskWrite{ns: ns, key: key, val: nil, erase: true, ret: ch}:
		return <-ch
	case <-dc.closed:
		return errDiskCacheClosed
	}
}

func (dc *SqliteDiskCache) EraseNamespace(ns string) error {
	ch := make(chan error)
	select {
	case dc.w <- diskWrite{ns: ns, erase_namespace: true, ret: ch}:
		return <-ch
	case <-dc.closed:
		return errDiskCacheClosed
	}
}

func (dc *SqliteDiskCache) run() error {
	for {
		err := dc.tx()
		if err != nil {
			// After tx error, reply to all channels with errors for some duration
			// So that callers are not deadlocked
			dc.noTx(err)
		}
		select {
		case <-dc.closed:
			return err
		default:
		}
	}
}

func (dc *SqliteDiskCache) noTx(lastError error) {
	t := time.NewTimer(dc.txDuration) // Keep it simple, cool down equals to normal transaction duration
	defer t.Stop()
	for {
		select {
		case <-t.C:
			return
		case <-dc.closed:
			return
		case r := <-dc.r:
			r.ret <- pcache.ReadResult{Err: lastError}
		case w := <-dc.w:
			w.ret <- lastError
		case l := <-dc.l:
			l.ret <- _listResult{err: lastError}
		case lk := <-dc.lk:
			lk.ret <- _listKeysResult{err: lastError}
		case c := <-dc.c:
			c.ret <- countResult{err: lastError}
		}
	}
}

func (dc *SqliteDiskCache) tx() error {
	t := time.NewTimer(dc.txDuration)
	defer t.Stop()

	return dc.db.Tx(context.Background(), // we don't want to interrupt DB operations
		func(tx *easydb.Tx) error {
			emptySlice := make([]byte, 0)
			for {
				select {
				case <-t.C:
					return nil
				case <-dc.closed:
					return nil
				case r := <-dc.r:
					var v pcache.ReadResult
					v.Found, v.Err = tx.Get(&v, selectQuery, r.ns, r.key)
					r.ret <- v
					if v.Err != nil {
						return v.Err
					}
				case w := <-dc.w:
					val := w.val
					if val == nil {
						val = emptySlice // avoid triggering NOT NULL constraint
					}
					var err error
					switch {
					case w.erase_namespace:
						err = tx.Exec(eraseNamespaceQuery, w.ns)
					case w.erase:
						err = tx.Exec(eraseQuery, w.ns, w.key)
					default:
						err = tx.Exec(updateQuery, w.ns, w.key, val, w.update, w.ttl)
					}
					w.ret <- err
					if err != nil {
						return err
					}
				case l := <-dc.l:
					var v _listResult
					v.err = tx.Select(&v.Value, listQuery, l.ns)
					l.ret <- v
					if v.err != nil {
						return v.err
					}
				case lk := <-dc.lk:
					var v _listKeysResult
					v.err = tx.Select(&v.Value, listKeysQuery, lk.ns, lk.limit, lk.offset)
					lk.ret <- v
					if v.err != nil {
						return v.err
					}
				case c := <-dc.c:
					var v countResult
					_, v.err = tx.Get(&v.Value, countQuery, c.ns)
					c.ret <- v
					if v.err != nil {
						return v.err
					}
				}
			}
		})
}
