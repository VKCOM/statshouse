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
	maxROConn          = 128

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
	opt  Options
	ctx  context.Context
	stop func()
	rw   *sqliteConn
	//	chk *sqlite0.Conn
	roMx    sync.Mutex
	roFree  []*sqliteConn
	roCount int
	roCond  *sync.Cond

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
}

type Options struct {
	Path           string
	APPID          int32
	Scheme         string
	Replica        bool
	CommitEvery    time.Duration
	DurabilityMode DurabilityMode
	ReadAndExit    bool
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

type ApplyEventFunction func(conn Conn, offset int64, cache []byte) (int, error)

type engineMode int
type DurabilityMode int

const (
	master  = iota // commit sql tx by timer, to synchronize sqlite and binlog
	replica = iota // queue data in apply and commit sql tx after got commit from binlog
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
	if opt.CommitEvery == 0 {
		opt.CommitEvery = commitEveryDefault
	}
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
	if opt.ReadAndExit {
		e.opt.DurabilityMode = NoBinlog
	}
	e.roCond = sync.NewCond(&e.roMx)

	e.commitTXAndStartNew(false, false)
	if err := e.rw.err; err != nil {
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

		go func() {
			err = binlog.Run(offset, meta, binlogEngineImpl)
			if err != nil {
				e.rw.mu.Lock()
				e.rw.err = err
				e.rw.mu.Unlock()
			}
			if opt.ReadAndExit {
				close(e.waitUntilBinlogReady)
			}

		}()
		<-e.waitUntilBinlogReady
		// in master mode we need to apply all queued events, before handling queries
		// in replica mode it's not needed, because we are getting apply events from binlog
		if !opt.Replica && binlogEngineImpl.state == waitToCommit {
			err := binlogEngineImpl.applyQueue.applyAllChanges(binlogEngineImpl.apply, binlogEngineImpl.skip)
			if err != nil {
				return nil, fmt.Errorf("failed to apply queued events: %w", err)
			}
			binlogEngineImpl.state = none
		}
		if opt.ReadAndExit {
			e.binlog = nil
		}
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

func (e *Engine) Close(ctx context.Context) error {
	ch := make(chan error, 1)
	defer close(ch)
	go func() {
		select {
		case ch <- e.close(e.opt.DurabilityMode == WaitCommit):
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

func (e *Engine) close(waitCommit bool) error {
	if waitCommit {
		e.commitTXAndStartNew(true, true)
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
	_ = e.commitTXAndStartNewLocked(c, commit, waitBinlogCommit)

}

func (e *Engine) commitTXAndStartNewLocked(c Conn, commit, waitBinlogCommit bool) error {
	var info *committedInfo
	if waitBinlogCommit && e.binlog != nil {
		info = e.binlogWaitDBSync(c)
	}
	if commit {
		if e.binlog != nil && info != nil && len(info.meta) > 0 {
			err := e.binlogUpdateMeta(c, info.meta)
			if err != nil {
				e.rw.err = err
			}
		}
		if e.rw.err == nil {
			_, err := c.exec(true, commitStmt)
			if err != nil {
				e.rw.err = fmt.Errorf("periodic tx commit failed: %w", err)
			}
		}
	}

	if e.rw.err == nil {
		_, err := c.exec(true, beginStmt)
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
		_, err := c.exec(true, "VACUUM INTO $to", BlobText("$to", path))
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
// TODO: research shm to see uncommitted data
func (e *Engine) ViewCommitted(ctx context.Context, fn func(Conn) error) error {
	e.roMx.Lock()
	var conn *sqliteConn
	for len(e.roFree) == 0 && e.roCount >= maxROConn {
		e.roCond.Wait()
	}
	if len(e.roFree) == 0 {
		ro, err := openROWAL(e.opt.Path)
		if err != nil {
			e.roMx.Unlock()
			return fmt.Errorf("failed to open RO connection: %w", err)
		}
		conn = newSqliteConn(ro)
		e.roCount++
	} else {
		conn = e.roFree[0]
		e.roFree = e.roFree[1:]
	}
	e.roMx.Unlock()
	defer func() {
		e.roMx.Lock()
		e.roFree = append(e.roFree, conn)
		e.roMx.Unlock()
		e.roCond.Signal()
	}()
	conn.mu.Lock()
	c := Conn{conn, false, ctx}
	err := fn(c)
	c.close()
	return err
}

func (e *Engine) Do(ctx context.Context, fn func(Conn, []byte) ([]byte, error)) error {
	c := e.start(ctx, true)
	e.rw.spOk = false
	var err error
	buffer, err := fn(c, nil)
	if err != nil {
		c.close()
		return err
	}
	if e.opt.DurabilityMode == NoBinlog {
		e.rw.spOk = true
		c.close()
		return nil
	}
	shouldWriteBinlog := len(buffer) > 0
	isReadOp := !shouldWriteBinlog
	if shouldWriteBinlog && e.mode == replica {
		c.close()
		return fmt.Errorf("failed to write binlog in replica mode") // TODO replace with GMS error
	}
	var ch chan struct{}
	waitCommit := e.opt.DurabilityMode == WaitCommit
	mustCommitNow := time.Since(e.lastCommitTime) >= e.opt.CommitEvery && !waitCommit && !isReadOp
	var offsetAfterWritePredicted int64
	if shouldWriteBinlog {
		offsetBeforeWrite := e.dbOffset
		offsetAfterWritePredicted = offsetBeforeWrite + int64(fsbinlog.AddPadding(len(buffer)))

		err = binlogUpdateOffset(c, offsetAfterWritePredicted)
		if err != nil {
			c.close()
			return err
		}

		var offsetAfterWrite int64
		if waitCommit || mustCommitNow {
			offsetAfterWrite, err = e.binlog.AppendASAP(e.dbOffset, buffer)
		} else {
			offsetAfterWrite, err = e.binlog.Append(e.dbOffset, buffer)
		}
		if err != nil {
			c.close()
			return err
		}
		// after this line we can't roll back savepoint
		e.dbOffset = offsetAfterWrite
	}
	if waitCommit || mustCommitNow {
		e.waitQMx.Lock()
		info, _ := e.committedInfo.Load().(*committedInfo)
		// check if commit was already done
		alreadyCommitted := offsetAfterWritePredicted <= info.offset
		uncommittedWriteExists := len(e.waitQ) > 0
		if (isReadOp && uncommittedWriteExists) || (!isReadOp && !alreadyCommitted) || mustCommitNow {
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
		_ = e.commitTXAndStartNewLocked(c, true, false)
	}
	e.rw.spOk = true
	c.close()
	if ch != nil {
		<-ch
	}
	return nil
}

func binlogUpdateOffset(c Conn, offset int64) error {
	_, err := c.Exec("UPDATE __binlog_offset set offset = $offset;", Int64("$offset", offset))
	return err
}

func (e *Engine) start(ctx context.Context, autoSavepoint bool) Conn {
	e.rw.mu.Lock()
	return Conn{e.rw, autoSavepoint, ctx}
}
