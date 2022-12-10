// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlite

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	binlog2 "github.com/vkcom/statshouse/internal/vkgo/binlog"
	"github.com/vkcom/statshouse/internal/vkgo/binlog/fsbinlog"

	"go.uber.org/atomic"

	"github.com/vkcom/statshouse/internal/vkgo/sqlite/internal/sqlite0"

	"go.uber.org/multierr"
	"pgregory.net/rand"
)

// TODO: use build of sqlite with custom WAL magic to prevent accidental checkpointing by command-line tools
// TODO: check the app ID at startup
// TODO: check the results of PRAGMA statements
// TODO: use mmap in all connections?
// TODO: handle sqlite0.Busy
// TODO: integrity check
// TODO: built-in simple migrator
// TODO: auto-rollback savepoint in case of any SQL-related errors
// TODO: consider madvise in unixRemapfile() for mmap

// how it should work:
// - if no write tx, open tx, create snapshot, upgrade to write tx
// - every 1s, COMMIT write tx
// - disable auto-checkpointing, and never checkpoint past the last snapshot before gms commit
// - checkpoint every so often, try to minimize fsync load (async checkpointing?)
//   - easy implementation: have a read tx open that will prevent the checkpoint from advancing
//   - hard mode: have a special lock in place?
//   - checkpoint in a separate thread: need non-exclusive access!
//
// on revert:
// - close write tx
// - sqlite_snapshot_revert
// - continue

// - memset(&pWal->hdr, 0, sizeof(WalIndexHdr))
//   - only done in wal-index recovery (start of read tx, ) or after a checkpoint:
//         If a new wal-index header was loaded before the checkpoint was
//         performed, then the pager-cache associated with pWal is now
//         out of date. So zero the cached wal-index header to ensure that
//         next time the pager opens a snapshot on this database it knows that
//         the cache needs to be reset.
//   - make snapshot_get() return error?
//   - or, some other memcmp(&pWal->hdr
//
// - if we don't start a read tx, our hdr can be changed. can this be a problem?
// - should we hold a b-tree checkpoint or no?
// - sqlite3WalUndo calls callbacks to do something with the removed pages, should we too?
//   - dirty pages only exist inside tx, we are not inside a tx
//
// - look at snapshot logic in sqlite3WalBeginReadTransaction for "safety" conditions

const (
	busyTimeout = 5 * time.Second
	cacheKB     = 65536                         // 64MB
	mmapSize    = 8 * 1024 * 1024 * 1024 * 1024 // 8TB
	commitEvery = 60 * time.Second

	beginStmt  = "BEGIN IMMEDIATE" // TODO: not immediate? then SQLITE_BUSY_SNAPSHOT from upgrade is possible
	commitStmt = "COMMIT"
	normSelect = "SELECT"
	normVacuum = "VACUUM INTO"

	initOffsetTable   = "CREATE TABLE IF NOT EXISTS __binlog_offset (offset INTEGER);"
	snapshotMetaTable = "CREATE TABLE IF NOT EXISTS __snapshot_meta (meta BLOB);"
)

var (
	errAlreadyClosed = errors.New("sqlite-engine: already closed")
	errUnsafe        = errors.New("sqlite-engine: unsafe SQL")
	safeStatements   = []string{"SELECT", "INSERT", "UPDATE", "DELETE", "REPLACE", "UPSERT"}
)

type Engine struct {
	opt Options
	rw  *sqlite0.Conn
	//	chk *sqlite0.Conn
	//	ro  *sqlite0.Conn

	rwMu           sync.Mutex
	prep           map[string]stmtInfo
	used           map[*sqlite0.Stmt]struct{}
	err            error
	spIn           bool
	spOk           bool
	spBeginStmt    string
	spCommitStmt   string
	spRollbackStmt string
	binlog         binlog2.Binlog

	waitQMx       sync.Mutex
	waitQ         []waitCommitInfo
	committedInfo *atomic.Value
	dbOffset      int64

	apply                ApplyEventFunction
	waitUntilBinlogReady chan struct{}
	readyNotify          sync.Once
	commitCh             chan struct{}
}

type Options struct {
	Path   string
	APPID  int32
	Scheme string
}

type binlogEngineImpl struct {
	e *Engine
}

type waitCommitInfo struct {
	offset int64
	waitCh chan struct{}
}

type committedInfo struct {
	meta   []byte
	offset int64
}

type stmtInfo struct {
	stmt         *sqlite0.Stmt
	isSafe       bool
	isSelect     bool
	isVacuumInto bool
}

type ApplyEventFunction func(conn Conn, offset int64, cache []byte) (int, error)

func OpenEngine(
	opt Options,
	binlog binlog2.Binlog,
	apply ApplyEventFunction) (*Engine, error) {
	rw, err := openRW(opt.Path, opt.APPID, opt.Scheme, initOffsetTable, snapshotMetaTable)
	if err != nil {
		return nil, fmt.Errorf("failed to open RW connection: %w", err)
	}
	// chk, err := openChk(path)
	// if err != nil {
	//	_ = rw.Close()
	//	return nil, fmt.Errorf("failed to open CHK connection: %w", err)
	//}
	//
	// ro, err := openROWAL(path)
	// if err != nil {
	//	_ = rw.Close()
	//	_ = chk.Close()
	//	return nil, fmt.Errorf("failed to open RO connection: %w", err)
	// }

	var spID [16]byte
	_, _ = rand.New().Read(spID[:])
	e := &Engine{
		rw: rw,
		//	chk:                     chk,
		//	ro:                      ro,
		opt:                  opt,
		prep:                 map[string]stmtInfo{},
		used:                 map[*sqlite0.Stmt]struct{}{},
		spBeginStmt:          fmt.Sprintf("SAVEPOINT __sqlite_engine_auto_%x", spID[:]),
		spCommitStmt:         fmt.Sprintf("RELEASE __sqlite_engine_auto_%x", spID[:]),
		spRollbackStmt:       fmt.Sprintf("ROLLBACK TO __sqlite_engine_auto_%x", spID[:]),
		apply:                apply,
		binlog:               binlog,
		committedInfo:        &atomic.Value{},
		dbOffset:             0,
		readyNotify:          sync.Once{},
		waitUntilBinlogReady: make(chan struct{}),
		commitCh:             make(chan struct{}, 1),
	}

	e.txLoopIter(false, false)
	if err := e.err; err != nil {
		_ = e.close(false)
		return nil, fmt.Errorf("failed to start write transaction: %w", err)
	}
	if binlog != nil {
		binlogEngineImpl := &binlogEngineImpl{e: e}
		e.committedInfo.Store(&committedInfo{})
		offset, err := e.binlogLoadOrCreatePosition()
		if err != nil {
			_ = e.close(false)
			return nil, fmt.Errorf("failed to load binlog position: %w", err)
		}
		e.dbOffset = offset
		meta, err := e.binlogLoadOrCreateMeta()
		if err != nil {
			_ = e.close(false)
			return nil, fmt.Errorf("failed to load snapshot meta: %w", err)
		}

		errCh := make(chan error)
		go func() {
			err = binlog.Start(offset, meta, binlogEngineImpl)
			if err != nil {
				fmt.Println(err.Error())
				errCh <- err
			}
		}()
		select {
		case <-e.waitUntilBinlogReady:
		case err := <-errCh:
			_ = e.close(false)
			return nil, fmt.Errorf("failed to start %w", err)
		}
		offset, err = e.binlogLoadOrCreatePosition()
		if err != nil {
			_ = e.close(false)
			return nil, fmt.Errorf("failed to load binlog position: %w", err)
		}
		if e.dbOffset != offset {
			return nil, fmt.Errorf("lev offsets are different: e.dbOffset=%d, offset=%d", e.dbOffset, offset)
		}
		info, _ := e.committedInfo.Load().(*committedInfo)
		if e.dbOffset > info.offset {
			return nil, fmt.Errorf("db offset greater than binlog offset after binlog reread")
		}
		e.txLoopIter(true, false)
		if err := e.err; err != nil {
			_ = e.close(false)
			return nil, fmt.Errorf("failed to commit binlog offset: %w", err)
		}
	}
	go e.txLoop()

	return e, nil
}

func openWAL(path string, flags int) (*sqlite0.Conn, error) {
	conn, err := sqlite0.Open(path, flags)
	if err != nil {
		return nil, err
	}

	// todo make checkpoint manually
	if false {
		err = conn.SetAutoCheckpoint(0)
		if err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("failed to disable DB auto-checkpoints: %w", err)
		}
	}

	err = conn.SetBusyTimeout(busyTimeout)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to set DB busy timeout to %v: %w", busyTimeout, err)
	}

	err = conn.Exec("PRAGMA journal_mode=WAL2")
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to enable DB WAL mode: %w", err)
	}

	return conn, nil
}

func openRW(path string, appID int32, schemas ...string) (*sqlite0.Conn, error) {
	conn, err := openWAL(path, sqlite0.OpenReadWrite|sqlite0.OpenCreate|sqlite0.OpenNoMutex|sqlite0.OpenPrivateCache)
	if err != nil {
		return nil, err
	}

	err = conn.Exec(fmt.Sprintf("PRAGMA cache_size=-%d", cacheKB))
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to change DB cache size to %dKB: %w", cacheKB, err)
	}

	if false {
		err = conn.Exec(fmt.Sprintf("PRAGMA mmap_size=%d", mmapSize))
		if err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("failed to set DB mmap size to %d: %w", mmapSize, err)
		}
	}

	err = conn.Exec(fmt.Sprintf("PRAGMA application_id=%d", appID)) // make DB ready to use snapshots
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to set DB app ID %d: %w", appID, err)
	}

	for _, schema := range schemas {
		err = conn.Exec(schema)
		if err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("failed to setup DB schema: %w", err)
		}
	}

	return conn, nil
}

//	func openChk(path string) (*sqlite0.Conn, error) {
//		conn, err := openWAL(path, sqlite0.OpenReadWrite|sqlite0.OpenNoMutex|sqlite0.OpenPrivateCache)
//		if err != nil {
//			return nil, err
//		}
//
//		return conn, nil
//	}
func openROWAL(path string) (*sqlite0.Conn, error) {
	conn, err := openWAL(path, sqlite0.OpenReadonly|sqlite0.OpenNoMutex|sqlite0.OpenPrivateCache)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func (e *Engine) binlogLoadOrCreatePosition() (int64, error) {
	var offset int64
	err := e.do(func(conn Conn) error {
		var isExists bool
		var err error
		offset, isExists, err = binlogLoadPosition(conn)
		if err != nil {
			return err
		}
		if isExists {
			return nil
		}
		_, err = conn.Exec("INSERT INTO __binlog_offset(offset) VALUES(0)")
		return err
	})
	return offset, err
}

func (e *Engine) binlogLoadOrCreateMeta() ([]byte, error) {
	var meta []byte
	err := e.do(func(conn Conn) error {
		rows := conn.Query("SELECT meta from __snapshot_meta")
		if rows.err != nil {
			return rows.err
		}
		if rows.Next() {
			meta, _ = rows.ColumnBlob(0, meta)
			return nil
		}
		_, err := conn.Exec("INSERT INTO __snapshot_meta(meta) VALUES($meta)", Blob("$meta", meta))
		return err
	})
	return meta, err
}

func (e *Engine) binlogUpdateMeta(conn Conn, meta []byte) error {
	_, err := conn.Exec("UPDATE __snapshot_meta SET meta = $meta;", Blob("$meta", meta))
	return err
}

func binlogLoadPosition(conn Conn) (offset int64, isExists bool, err error) {
	rows := conn.Query("SELECT offset from __binlog_offset")
	if rows.err != nil {
		return 0, false, rows.err
	}
	if rows.Next() {
		offset, _ := rows.ColumnInt64(0)
		return offset, true, nil
	}
	return 0, false, nil
}

func (e *Engine) Close() error {
	return e.close(true)
}

func (e *Engine) close(waitCommit bool) error {
	if waitCommit {
		e.txLoopIter(true, true)
	}
	e.rwMu.Lock()
	defer e.rwMu.Unlock()

	err := e.err
	e.err = errAlreadyClosed
	for _, si := range e.prep {
		multierr.AppendInto(&err, si.stmt.Close())
	}
	multierr.AppendInto(&err, e.rw.Close())
	// multierr.AppendInto(&err, e.chk.Close())
	// multierr.AppendInto(&err, e.ro.Close()) // close RO one last to prevent checkpoint-on-close logic in other connections

	return err
}

func (e *Engine) txLoop() {
	t := time.NewTicker(commitEvery)
	defer t.Stop()

	for range t.C {
		e.txLoopIter(true, true)
	}
}

// Apply применяется при перечитывании или при работе реплики
func (impl *binlogEngineImpl) Apply(payload []byte) (newOffset int64, err error) {
	e := impl.e
	var errToReturn error
	var n int
	offset := e.dbOffset
	errFromTx := e.do(func(conn Conn) error {
		// mustn't change any state in this function
		dbOffset, _, err := binlogLoadPosition(conn)
		if err != nil {
			return err
		}
		newOffset = offset
		var shouldSkipLen = 0
		if dbOffset > offset {
			oldLen := len(payload)
			shouldSkipLen = int(dbOffset - offset)
			if shouldSkipLen > oldLen {
				shouldSkipLen = oldLen
			}
			payload = payload[shouldSkipLen:]
			if len(payload) == 0 {
				n = oldLen
				return nil
			}
		}
		n, err = e.apply(conn, offset+int64(shouldSkipLen), payload)
		n += shouldSkipLen
		newOffset = offset + int64(n)
		err1 := binlogUpdateOffset(conn, newOffset)
		if err != nil {
			errToReturn = err
			if !errors.Is(err, binlog2.ErrorUnknownMagic) && !errors.Is(err, binlog2.ErrorNotEnoughData) {
				return err
			}
		}
		return err1
	})
	if errFromTx == nil {
		e.txLoopIter(true, false)
		e.dbOffset = newOffset
	}
	return e.dbOffset, errToReturn
}

func (impl *binlogEngineImpl) Commit(offset int64, snapshotMeta []byte, safeSnapshotOffset int64) {
	e := impl.e
	old := e.committedInfo.Load()
	if old != nil {
		ci := old.(*committedInfo)
		if ci.offset > offset {
			return
		}
	}
	e.committedInfo.Store(&committedInfo{
		meta:   snapshotMeta,
		offset: offset,
	})
	select {
	case e.commitCh <- struct{}{}:
	default:
	}
	e.binlogNotifyWaited(offset)
}

func (impl *binlogEngineImpl) Revert(toOffset int64) bool {
	return true
}

func (impl *binlogEngineImpl) ChangeRole(info binlog2.ChangeRoleInfo) {
	e := impl.e
	if info.IsReady {
		e.readyNotify.Do(func() {
			close(e.waitUntilBinlogReady)
		})
	}
}

func (impl *binlogEngineImpl) Skip(skipLen int64) int64 {
	impl.e.dbOffset += skipLen
	err := impl.e.do(func(conn Conn) error {
		return binlogUpdateOffset(conn, impl.e.dbOffset)
	})
	if err != nil {
		log.Panicf("error in skip handler: %s", err)
	}
	return impl.e.dbOffset
}

func (e *Engine) binlogWaitDBSync(conn Conn) *committedInfo {
	info, _ := e.committedInfo.Load().(*committedInfo)
	for info.offset < e.dbOffset {
		<-e.commitCh
		info, _ = e.committedInfo.Load().(*committedInfo)
	}
	return info
}

func (e *Engine) txLoopIter(commit, waitBinlogCommit bool) {
	c := e.start(false)
	defer c.close()
	var info *committedInfo
	if waitBinlogCommit && e.binlog != nil {
		info = e.binlogWaitDBSync(c)
	}
	if commit {
		if e.binlog != nil && info != nil && len(info.meta) > 0 {
			err := e.binlogUpdateMeta(c, info.meta)
			if err != nil {
				e.err = err
			}
		}
		if e.err == nil {
			_, err := c.exec(true, commitStmt)
			if err != nil {
				e.err = fmt.Errorf("periodic tx commit failed: %w", err)
			}
		}
	}

	if e.err == nil {
		_, err := c.exec(true, beginStmt)
		if err != nil {
			e.err = fmt.Errorf("periodic tx begin failed: %w", err)
		}
	}
}

func backupToTemp(e *Engine, prefix string) (string, error) {
	c := e.start(false)
	defer c.close()
	path := prefix + "." + strconv.FormatUint(rand.Uint64(), 10) + ".tmp"
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		_, err := c.exec(true, "VACUUM INTO $to", BlobText("$to", path))
		e.err = err
	}
	return path, e.err
}

func getBackupPath(e *Engine, prefix string) (string, error) {
	c := e.start(false)
	defer c.close()
	offs, _, err := binlogLoadPosition(c)
	path := prefix + "." + strconv.FormatInt(offs, 10)
	return path, err
}

func (e *Engine) binlogNotifyWaited(committedOffset int64) {
	e.waitQMx.Lock()
	defer e.waitQMx.Unlock()
	i := 0
	for i = 0; i < len(e.waitQ); i++ {
		if e.waitQ[i].offset > committedOffset {
			break
		}
		close(e.waitQ[i].waitCh)
	}
	e.waitQ = e.waitQ[i:]
}

func (e *Engine) do(fn func(Conn) error) error {
	c := e.start(true)
	defer c.close()
	err := fn(c)
	if err != nil {
		e.spOk = false
		return err
	}
	e.spOk = true
	return err
}

func (e *Engine) Backup(prefix string) error {
	var path string
	err := doSingleROToWALQuery(e.opt.Path, func(e *Engine) error {
		var err error
		path, err = backupToTemp(e, prefix)
		return err
	})
	defer func() {
		_ = os.Remove(path)
	}()
	if err != nil {
		return err
	}
	var backupExpectedPath string
	err = doSingleROQuery(path, func(e *Engine) error {
		backupExpectedPath, err = getBackupPath(e, prefix)
		return err
	})
	if err != nil {
		return err
	}
	if _, err := os.Stat(backupExpectedPath); os.IsNotExist(err) {
		return os.Rename(path, backupExpectedPath)
	}
	return fmt.Errorf("snapshot %s already exists", backupExpectedPath)
}

func (e *Engine) Do(fn func(Conn, []byte) ([]byte, error),
	waitCommit bool) error {
	c := e.start(true)

	var err error
	buffer, err := fn(c, nil)
	if err != nil {
		e.spOk = false
		c.close()
		return err
	}
	shouldWriteBinlog := len(buffer) > 0
	var ch chan struct{}
	if shouldWriteBinlog && e.binlog != nil {
		offsetBeforeWrite := e.dbOffset
		offsetAfterWritePredicted := offsetBeforeWrite + int64(fsbinlog.AddPadding(len(buffer)))

		err = binlogUpdateOffset(c, offsetAfterWritePredicted)
		if err != nil {
			e.spOk = false
			c.close()
			return err
		}

		var offsetAfterWrite int64
		if waitCommit {
			offsetAfterWrite, err = e.binlog.AppendASAP(e.dbOffset, buffer)
		} else {
			offsetAfterWrite, err = e.binlog.Append(e.dbOffset, buffer)
		}
		if err != nil {
			e.spOk = false
			c.close()
			return err
		}
		info, _ := e.committedInfo.Load().(*committedInfo)
		if waitCommit && info.offset < offsetAfterWrite {
			ch = make(chan struct{})
			e.waitQMx.Lock()
			e.waitQ = append(e.waitQ, waitCommitInfo{
				offset: offsetAfterWritePredicted,
				waitCh: ch,
			})
			e.waitQMx.Unlock()
		} else {
			waitCommit = false
		}
		// after this line we can't roll back tx
		e.dbOffset = offsetAfterWrite
	}
	e.spOk = true
	c.close()
	if waitCommit && shouldWriteBinlog && e.binlog != nil {
		<-ch
	}
	return nil
}

func binlogUpdateOffset(c Conn, offset int64) error {
	_, err := c.Exec("UPDATE __binlog_offset set offset = $offset;", Int64("$offset", offset))
	return err
}

func (e *Engine) start(autoSavepoint bool) Conn {
	e.rwMu.Lock()
	return Conn{e, autoSavepoint}
}

type Conn struct {
	e             *Engine
	autoSavepoint bool
}

func (c Conn) close() {
	c.execEndSavepoint()
	for stmt := range c.e.used {
		_ = stmt.Reset()
		delete(c.e.used, stmt)
	}
	c.e.rwMu.Unlock()
}

func (c Conn) execBeginSavepoint() error {
	c.e.spIn = true
	c.e.spOk = false
	_, err := c.exec(true, c.e.spBeginStmt)
	return err
}

func (c Conn) execEndSavepoint() {
	if c.e.spIn {
		if c.e.spOk {
			_, _ = c.exec(true, c.e.spCommitStmt)
		} else {
			_, _ = c.exec(true, c.e.spRollbackStmt)
		}
		c.e.spIn = false
	}
}

func (c Conn) Query(sql string, args ...Arg) Rows {
	return c.query(false, sql, args...)
}

func (c Conn) Exec(sql string, args ...Arg) (int64, error) {
	return c.exec(false, sql, args...)
}

func (c Conn) ExecUnsafe(sql string, args ...Arg) (int64, error) {
	return c.exec(true, sql, args...)
}

func (c Conn) query(allowUnsafe bool, sql string, args ...Arg) Rows {
	s, err := c.doQuery(allowUnsafe, sql, args...)
	return Rows{c.e, s, err, false}
}

func (c Conn) exec(allowUnsafe bool, sql string, args ...Arg) (int64, error) {
	rows := c.query(allowUnsafe, sql, args...)
	for rows.Next() {
	}
	return c.LastInsertRowID(), rows.Error()
}

type Rows struct {
	e    *Engine
	s    *sqlite0.Stmt
	err  error
	used bool
}

func (r *Rows) Error() error {
	return r.err
}

func (r *Rows) next(setUsed bool) bool {
	if r.err != nil {
		return false
	}

	if setUsed && !r.used {
		r.e.used[r.s] = struct{}{}
		r.used = true
	}
	row, err := r.s.Step()
	if err != nil {
		r.err = err
	}
	return row
}

func (r *Rows) Next() bool {
	return r.next(true)
}

func (r *Rows) ColumnBlob(i int, buf []byte) ([]byte, error) {
	return r.s.ColumnBlob(i, buf)
}

func (r *Rows) ColumnBlobRaw(i int) ([]byte, error) {
	return r.s.ColumnBlobRaw(i)
}

func (r *Rows) ColumnBlobRawString(i int) (string, error) {
	return r.s.ColumnBlobRawString(i)
}

func (r *Rows) ColumnBlobString(i int) (string, error) {
	return r.s.ColumnBlobString(i)
}

func (r *Rows) ColumnIsNull(i int) bool {
	return r.s.ColumnIsNull(i)
}

func (r *Rows) ColumnInt64(i int) (int64, error) {
	return r.s.ColumnInt64(i)
}

func (c Conn) LastInsertRowID() int64 {
	return c.e.rw.LastInsertRowID()
}

func (c Conn) doQuery(allowUnsafe bool, sql string, args ...Arg) (*sqlite0.Stmt, error) {
	if c.e.err != nil {
		return nil, c.e.err
	}

	si, ok := c.e.prep[sql]
	var err error
	if !ok {
		si, err = prepare(c.e.rw, sql)
		if err != nil {
			return nil, err
		}
		c.e.prep[sql] = si
	}

	if !allowUnsafe && !si.isSafe {
		return nil, errUnsafe
	}
	if c.autoSavepoint && (!si.isSelect && !si.isVacuumInto) && !c.e.spIn {
		err := c.execBeginSavepoint()
		if err != nil {
			return nil, err
		}
	}

	_, used := c.e.used[si.stmt]
	if used {
		err := si.stmt.Reset()
		if err != nil {
			return nil, err
		}
	}

	return doStmt(si, args...)
}

func doStmt(si stmtInfo, args ...Arg) (*sqlite0.Stmt, error) {
	for _, arg := range args {
		var err error
		p := si.stmt.Param(arg.name)
		switch arg.typ {
		case argByte:
			err = si.stmt.BindBlob(p, arg.b)
		case argByteConst:
			err = si.stmt.BindBlobConst(p, arg.b)
		case argString:
			err = si.stmt.BindBlobString(p, arg.s)
		case argInt64:
			err = si.stmt.BindInt64(p, arg.n)
		case argText:
			err = si.stmt.BindBlobText(p, arg.s)
		default:
			err = fmt.Errorf("unknown arg type for %q: %v", arg.name, arg.typ)
		}
		if err != nil {
			return nil, err
		}
	}
	return si.stmt, nil
}

func prepare(c *sqlite0.Conn, sql string) (stmtInfo, error) {
	s, _, err := c.Prepare(sql, true)
	if err != nil {
		return stmtInfo{}, err
	}
	norm := s.NormalizedSQL()
	return stmtInfo{
		stmt:         s,
		isSafe:       isSafe(norm),
		isSelect:     isSelect(norm),
		isVacuumInto: isVacuumInto(norm),
	}, nil
}

func isSelect(normSQL string) bool {
	return strings.HasPrefix(normSQL, normSelect+" ")
}

func isVacuumInto(normSQL string) bool {
	return strings.HasPrefix(normSQL, normVacuum+" ")
}

func isSafe(normSQL string) bool {
	for _, s := range safeStatements {
		if strings.HasPrefix(normSQL, s+" ") {
			return true
		}
	}
	return false
}
