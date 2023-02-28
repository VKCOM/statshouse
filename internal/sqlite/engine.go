// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlite

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/multierr"

	binlog2 "github.com/vkcom/statshouse/internal/vkgo/binlog"
	"github.com/vkcom/statshouse/internal/vkgo/binlog/fsbinlog"

	"go.uber.org/atomic"

	"github.com/vkcom/statshouse/internal/sqlite/internal/sqlite0"

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
	busyTimeout        = 5 * time.Second
	cacheKB            = 65536                         // 64MB
	mmapSize           = 8 * 1024 * 1024 * 1024 * 1024 // 8TB
	commitEveryDefault = 1 * time.Second
	commitTXTimeout    = 10 * time.Second

	beginStmt  = "BEGIN IMMEDIATE" // TODO: not immediate? then SQLITE_BUSY_SNAPSHOT from upgrade is possible
	commitStmt = "COMMIT"
	normSelect = "SELECT"
	normVacuum = "VACUUM INTO"

	initOffsetTable     = "CREATE TABLE IF NOT EXISTS __binlog_offset (offset INTEGER);"
	snapshotMetaTable   = "CREATE TABLE IF NOT EXISTS __snapshot_meta (meta BLOB);"
	internalQueryPrefix = "__"
)

var (
	errAlreadyClosed = errors.New("sqlite-engine: already closed")
	errUnsafe        = errors.New("sqlite-engine: unsafe SQL")
	safeStatements   = []string{"SELECT", "INSERT", "UPDATE", "DELETE", "REPLACE", "UPSERT"}
)

type Engine struct {
	opt  Options
	ctx  context.Context
	stop func()
	rw   *sqliteConn
	//	chk *sqlite0.Conn
	roMx          sync.Mutex
	roFree        []*sqliteConn
	roFreeShared  []*sqliteConn
	roCond        *sync.Cond
	roCount       int
	roCountShared int

	mode engineMode

	binlog         binlog2.Binlog
	dbOffset       int64
	lastCommitTime time.Time

	waitQMx       sync.Mutex
	waitQ         []waitCommitInfo
	committedInfo *atomic.Value

	apply                ApplyEventFunction
	scan                 ApplyEventFunction
	waitUntilBinlogReady chan struct{}
	readyNotify          sync.Once
	commitCh             chan struct{}

	isTest            bool
	mustCommitNowFlag bool
	mustWaitCommit    bool
}

type Options struct {
	Path              string
	APPID             int32
	StatsOptions      StatsOptions
	Scheme            string
	Replica           bool
	CommitEvery       time.Duration
	DurabilityMode    DurabilityMode
	ReadAndExit       bool
	CommitOnEachWrite bool // use only to test. If true break binlog + sqlite consistency
	ProfileCallback   ProfileCallback
	MaxROConn         int
}

type waitCommitInfo struct {
	offset   int64
	readWait bool
	waitCh   chan struct{}
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

type ProfileCallback func(sql, expandedSQL string, duration time.Duration)
type ApplyEventFunction func(conn Conn, offset int64, cache []byte) (int, error)

type engineMode int
type DurabilityMode int

const (
	master  engineMode = iota // commit sql tx by timer, to synchronize sqlite and binlog
	replica engineMode = iota // queue data in apply and commit sql tx after got commit from binlog
)

const (
	WaitCommit   DurabilityMode = iota // Wait for commit to finish before returning from Do()
	NoWaitCommit DurabilityMode = iota // Do not wait for commit to finish before returning from Do()
	NoBinlog     DurabilityMode = iota // Do not use binlog, just commit to sqlite
)

func OpenEngine(
	opt Options,
	binlog binlog2.Binlog,
	apply ApplyEventFunction,
	scan ApplyEventFunction, // this helps to work in replica mode with old binlog
) (*Engine, error) {
	e, err := openDB(opt, binlog, apply, scan)
	if err != nil {
		return nil, err
	}
	err = e.runBinlogAndWaitReady()
	if err != nil {
		e.close(false, false)
		return nil, err
	}
	e.mode = master
	if opt.Replica {
		e.mode = replica
	}
	if e.opt.DurabilityMode == WaitCommit || e.opt.DurabilityMode == NoBinlog {
		go e.txLoop()
	}
	return e, nil
}

func openDB(opt Options,
	binlog binlog2.Binlog,
	apply ApplyEventFunction,
	scan ApplyEventFunction) (*Engine, error) {
	if opt.CommitEvery == 0 {
		opt.CommitEvery = commitEveryDefault
	}
	if opt.MaxROConn == 0 {
		opt.MaxROConn = 100
	}
	rw, err := openRW(openWAL, opt.Path, opt.APPID, opt.ProfileCallback, opt.Scheme, initOffsetTable, snapshotMetaTable)
	if err != nil {
		return nil, fmt.Errorf("failed to open RW connection: %w", err)
	}

	ctx, stop := context.WithCancel(context.Background())
	e := &Engine{
		rw:   newSqliteConn(rw),
		ctx:  ctx,
		stop: stop,
		//	chk:                     chk,
		opt: opt,

		apply:                apply,
		scan:                 scan,
		binlog:               binlog,
		committedInfo:        &atomic.Value{},
		dbOffset:             0,
		readyNotify:          sync.Once{},
		waitUntilBinlogReady: make(chan struct{}),
		commitCh:             make(chan struct{}, 1),
		mode:                 replica,
	}
	e.roCond = sync.NewCond(&e.roMx)
	if opt.ReadAndExit {
		e.opt.DurabilityMode = NoBinlog
	}
	e.commitTXAndStartNew(false, false)
	if err := e.rw.err; err != nil {
		_ = e.close(false, false)
		return nil, fmt.Errorf("failed to start write transaction: %w", err)
	}
	return e, nil
}

func (e *Engine) runBinlogAndWaitReady() error {
	if e.binlog != nil {
		impl, err := e.binlogRun()
		if err != nil {
			return err
		}
		return e.binlogWaitReady(impl)
	}
	return nil
}
func (e *Engine) binlogRun() (*binlogEngineImpl, error) {
	impl := &binlogEngineImpl{e: e}
	e.committedInfo.Store(&committedInfo{})
	offset, err := e.binlogLoadOrCreatePosition()
	if err != nil {
		_ = e.close(false, false)
		return nil, fmt.Errorf("failed to load binlog position: %w", err)
	}
	e.dbOffset = offset
	meta, err := e.binlogLoadOrCreateMeta()
	if err != nil {
		_ = e.close(false, false)
		return nil, fmt.Errorf("failed to load snapshot meta: %w", err)
	}
	if !e.isTest {
		go func() {
			err = e.binlog.Run(offset, meta, impl)
			if err != nil {
				e.rw.mu.Lock()
				e.rw.err = err
				e.rw.mu.Unlock()
			}
			if e.opt.ReadAndExit {
				close(e.waitUntilBinlogReady)
			}

		}()
	}
	return impl, nil
}

func (e *Engine) binlogWaitReady(impl *binlogEngineImpl) error {
	startRereadingTime := time.Now()
	<-e.waitUntilBinlogReady
	e.opt.StatsOptions.measureActionDurationSince("binlog_reread", startRereadingTime)
	// in master mode we need to apply all queued events, before handling queries
	// in replica mode it's not needed, because we are getting apply events from binlog
	if !e.opt.Replica && impl.state == waitToCommit {
		err := impl.applyQueue.applyAllChanges(impl.apply, impl.skip)
		if err != nil {
			return fmt.Errorf("failed to apply queued events: %w", err)
		}
		impl.state = none
	}
	if e.opt.ReadAndExit {
		e.binlog = nil
	}
	return nil
}

func openWAL(path string, flags int, callback ProfileCallback) (*sqlite0.Conn, error) {
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

	if callback != nil {
		conn.RegisterCallback(sqlite0.ProfileCallback(callback))
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

func openRW(open func(path string, flags int, callback ProfileCallback) (*sqlite0.Conn, error), path string, appID int32, callback ProfileCallback, schemas ...string) (*sqlite0.Conn, error) {
	conn, err := open(path, sqlite0.OpenReadWrite|sqlite0.OpenCreate|sqlite0.OpenNoMutex|sqlite0.OpenSharedCache, callback)
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
func openROWAL(path string, shared bool, callback ProfileCallback) (*sqlite0.Conn, error) {
	flags := sqlite0.OpenPrivateCache
	if shared {
		flags = sqlite0.OpenSharedCache
	}
	conn, err := openWAL(path, flags|sqlite0.OpenReadonly|sqlite0.OpenNoMutex, callback)
	if err != nil {
		return nil, err
	}
	if shared {
		err = conn.Exec("PRAGMA read_uncommitted = true;")
		if err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("failed to enable read uncommitted: %w", err)
		}
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
		_, err = conn.Exec("__insert_binlog_pos", "INSERT INTO __binlog_offset(offset) VALUES(0)")
		return err
	})
	return offset, err
}

func (e *Engine) binlogLoadOrCreateMeta() ([]byte, error) {
	var meta []byte
	err := e.do(func(conn Conn) error {
		rows := conn.Query("__select_meta", "SELECT meta from __snapshot_meta")
		if rows.err != nil {
			return rows.err
		}
		if rows.Next() {
			meta, _ = rows.ColumnBlob(0, meta)
			return nil
		}
		_, err := conn.Exec("__insert_meta", "INSERT INTO __snapshot_meta(meta) VALUES($meta)", Blob("$meta", meta))
		return err
	})
	return meta, err
}

func (e *Engine) binlogUpdateMeta(conn Conn, meta []byte) error {
	_, err := conn.Exec("__update_meta", "UPDATE __snapshot_meta SET meta = $meta;", Blob("$meta", meta))
	return err
}

func binlogLoadPosition(conn Conn) (offset int64, isExists bool, err error) {
	rows := conn.Query("__select_binlog_pos", "SELECT offset from __binlog_offset")
	if rows.err != nil {
		return 0, false, rows.err
	}
	if rows.Next() {
		offset, _ := rows.ColumnInt64(0)
		return offset, true, nil
	}
	return 0, false, nil
}

func (e *Engine) Close(ctx context.Context) error {
	ch := make(chan error, 1)
	defer close(ch)
	go func() {
		err := e.close(e.opt.DurabilityMode == WaitCommit || e.opt.DurabilityMode == NoBinlog, e.opt.DurabilityMode == WaitCommit)
		select {
		case ch <- err:
		default:
		}
	}()
	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *Engine) close(shouldCommit, waitCommitBinlog bool) error {
	if shouldCommit {
		e.commitTXAndStartNew(true, waitCommitBinlog)
	}
	err := e.rw.Close()
	for _, conn := range e.roFree {
		multierr.AppendInto(&err, conn.Close())
	}
	// multierr.AppendInto(&err, e.chk.Close())
	// multierr.AppendInto(&err, e.ro.Close()) // close RO one last to prevent checkpoint-on-close logic in other connections
	return err
}

func (e *Engine) txLoop() {
	for {
		select {
		case <-time.After(e.opt.CommitEvery):
		case <-e.ctx.Done():
			return
		}

		// TODO replace with runtime mode change
		if e.mode == master {
			e.commitTXAndStartNew(true, e.opt.DurabilityMode == WaitCommit)
		}
	}
}

func (e *Engine) binlogWaitDBSync(conn Conn) *committedInfo {
	defer e.opt.StatsOptions.measureWaitDurationSince(waitBinlogSync, time.Now())
	info, _ := e.committedInfo.Load().(*committedInfo)
	for info.offset < e.dbOffset {
		<-e.commitCh
		info, _ = e.committedInfo.Load().(*committedInfo)
	}
	return info
}

func (e *Engine) commitTXAndStartNew(commit, waitBinlogCommit bool) {
	c := e.start(context.Background(), false)
	defer c.close()
	_ = e.commitTXAndStartNewLocked(c, commit, waitBinlogCommit, false)

}

func (e *Engine) commitTXAndStartNewLocked(c Conn, commit, waitBinlogCommit, skipUpdateMeta bool) error {
	var info *committedInfo
	if waitBinlogCommit && e.binlog != nil {
		info = e.binlogWaitDBSync(c)
	}
	if commit {
		startCommit := time.Now()
		if !skipUpdateMeta && e.binlog != nil && info != nil && len(info.meta) > 0 {
			err := e.binlogUpdateMeta(c, info.meta)
			if err != nil {
				e.rw.err = err
			}
		}
		if e.rw.err == nil {
			_, err := c.exec(true, "__commit", commitStmt)
			if err != nil {
				e.rw.err = fmt.Errorf("periodic tx commit failed: %w", err)
			}
		}
		e.opt.StatsOptions.measureActionDurationSince("commit_tx", startCommit)
	}

	if e.rw.err == nil {
		_, err := c.exec(true, "__begin_tx", beginStmt)
		if err != nil {
			e.rw.err = fmt.Errorf("periodic tx begin failed: %w", err)
		}
	}
	e.lastCommitTime = time.Now()
	return e.rw.err
}

func backupToTemp(ctx context.Context, e *Engine, prefix string) (string, error) {
	c := e.start(ctx, false)
	defer c.close()
	path := prefix + "." + strconv.FormatUint(rand.Uint64(), 10) + ".tmp"
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		_, err := c.ExecUnsafe("__vacuum", "VACUUM INTO $to", BlobText("$to", path))
		e.rw.err = err
	}
	return path, e.rw.err
}

func getBackupPath(e *Engine, prefix string) (string, error) {
	c := e.start(context.Background(), false)
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
		if !e.waitQ[i].readWait && e.waitQ[i].offset > committedOffset {
			break
		}
		close(e.waitQ[i].waitCh)
	}
	e.waitQ = e.waitQ[i:]
}

func (e *Engine) do(fn func(Conn) error) error {
	c := e.start(context.Background(), true)
	defer c.close()
	err := fn(c)
	if err != nil {
		e.rw.spOk = false
		return err
	}
	e.rw.spOk = true
	return err
}

func (e *Engine) Backup(ctx context.Context, prefix string) error {
	defer e.opt.StatsOptions.measureActionDurationSince("backup", time.Now())
	var path string
	err := doSingleROToWALQuery(e.opt.Path, func(e *Engine) error {
		var err error
		path, err = backupToTemp(ctx, e, prefix)
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

// ViewCommitted - can view only committed to sqlite data
// It depends on e.opt.CommitEvery
func (e *Engine) ViewCommitted(ctx context.Context, queryName string, fn func(Conn) error) error {
	return e.view(ctx, queryName, fn, false, &e.roFree, &e.roCount)
}

// TODO support wait for WaitCommit mode
func (e *Engine) ViewUncommitted(ctx context.Context, queryName string, fn func(Conn) error) error {
	return e.view(ctx, queryName, fn, true, &e.roFreeShared, &e.roCountShared)
}

func (e *Engine) view(ctx context.Context, queryName string, fn func(Conn) error, shared bool, roFree *[]*sqliteConn, roCount *int) error {
	if err := checkQueryName(queryName); err != nil {
		return err
	}
	startTimeBeforeLock := time.Now()
	e.roMx.Lock()
	var conn *sqliteConn
	for len(*roFree) == 0 && *roCount >= e.opt.MaxROConn {
		e.roCond.Wait()
	}
	if len(*roFree) == 0 {
		ro, err := openROWAL(e.opt.Path, shared, e.opt.ProfileCallback)
		if err != nil {
			e.roMx.Unlock()
			return fmt.Errorf("failed to open RO connection: %w", err)
		}
		conn = newSqliteConn(ro)
		*roCount++
	} else {
		conn = (*roFree)[0]
		*roFree = (*roFree)[1:]
	}
	e.roMx.Unlock()
	defer func() {
		e.roMx.Lock()
		*roFree = append(*roFree, conn)
		e.roMx.Unlock()
		e.roCond.Signal()
	}()
	conn.mu.Lock()
	e.opt.StatsOptions.measureWaitDurationSince(waitView, startTimeBeforeLock)
	c := Conn{conn, false, ctx, &e.opt.StatsOptions}
	defer c.close()
	defer e.opt.StatsOptions.measureSqliteTxDurationSince(txView, queryName, time.Now())
	err := fn(c)

	return err
}

func (e *Engine) mustCommitNow(waitCommitMode, isReadOp bool) bool {
	if !e.isTest {
		return time.Since(e.lastCommitTime) >= e.opt.CommitEvery && !waitCommitMode && !isReadOp
	}
	return e.mustCommitNowFlag
}

func checkQueryName(qn string) error {
	if strings.HasPrefix(qn, internalQueryPrefix) {
		return fmt.Errorf("query prefix %q is reserved", internalQueryPrefix)
	}
	return nil
}

func (e *Engine) doWithoutWait(ctx context.Context, queryName string, fn func(Conn, []byte) ([]byte, error)) (chan struct{}, error) {
	if err := checkQueryName(queryName); err != nil {
		return nil, err
	}
	startTimeBeforeLock := time.Now()
	c := e.start(ctx, true)
	defer c.close()
	e.opt.StatsOptions.measureWaitDurationSince(waitDo, startTimeBeforeLock)
	defer e.opt.StatsOptions.measureSqliteTxDurationSince(txDo, queryName, time.Now())
	e.rw.spOk = false
	var err error
	buffer, err := fn(c, nil)
	if err != nil {
		return nil, err
	}
	if e.opt.DurabilityMode == NoBinlog {
		e.rw.spOk = true
		return nil, nil
	}
	shouldWriteBinlog := len(buffer) > 0
	isReadOp := !shouldWriteBinlog
	if shouldWriteBinlog && e.mode == replica {
		return nil, fmt.Errorf("failed to write binlog in replica mode") // TODO replace with GMS error
	}
	var ch chan struct{}
	waitCommitMode := e.opt.DurabilityMode == WaitCommit
	mustCommitNow := e.mustCommitNow(waitCommitMode, isReadOp)
	var offsetAfterWritePredicted int64
	if shouldWriteBinlog {
		offsetBeforeWrite := e.dbOffset
		offsetAfterWritePredicted = offsetBeforeWrite + int64(fsbinlog.AddPadding(len(buffer)))

		err = binlogUpdateOffset(c, offsetAfterWritePredicted)
		if err != nil {
			return nil, err
		}

		var offsetAfterWrite int64
		if waitCommitMode || mustCommitNow {
			offsetAfterWrite, err = e.binlog.AppendASAP(e.dbOffset, buffer)
		} else {
			offsetAfterWrite, err = e.binlog.Append(e.dbOffset, buffer)
		}
		if err != nil {
			return nil, err
		}
		// after this line we can't roll back savepoint
		e.dbOffset = offsetAfterWrite
	}
	if e.opt.CommitOnEachWrite {
		_ = e.commitTXAndStartNewLocked(c, true, false, true)
		mustCommitNow = false
	}
	if waitCommitMode || mustCommitNow {
		e.waitQMx.Lock()
		info, _ := e.committedInfo.Load().(*committedInfo)
		// check if commit was already done
		alreadyCommitted := offsetAfterWritePredicted <= info.offset
		uncommittedWriteExists := len(e.waitQ) > 0
		if (waitCommitMode && isReadOp && uncommittedWriteExists) ||
			(waitCommitMode && !isReadOp && !alreadyCommitted) ||
			(!waitCommitMode && mustCommitNow && !alreadyCommitted) {
			ch = make(chan struct{})
			e.waitQ = append(e.waitQ, waitCommitInfo{
				offset:   offsetAfterWritePredicted,
				waitCh:   ch,
				readWait: isReadOp,
			})
		}
		e.waitQMx.Unlock()
	}
	if mustCommitNow && ch != nil {
		<-ch
		ch = nil
		_ = e.commitTXAndStartNewLocked(c, true, false, false)
	}
	e.rw.spOk = true
	return ch, nil
}

func (e *Engine) Do(ctx context.Context, queryName string, fn func(Conn, []byte) ([]byte, error)) error {
	ch, err := e.doWithoutWait(ctx, queryName, fn)
	if err != nil {
		return err
	}
	if ch != nil {
		<-ch
	}
	return nil
}

func binlogUpdateOffset(c Conn, offset int64) error {
	_, err := c.Exec("__update_binlog_pos", "UPDATE __binlog_offset set offset = $offset;", Int64("$offset", offset))
	return err
}

func (e *Engine) start(ctx context.Context, autoSavepoint bool) Conn {
	e.rw.mu.Lock()
	return Conn{e.rw, autoSavepoint, ctx, &e.opt.StatsOptions}
}
