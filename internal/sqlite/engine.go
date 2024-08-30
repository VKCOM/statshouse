// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlite

import (
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"context"
	"errors"
	"strconv"

	"github.com/vkcom/statshouse/internal/sqlite/sqlite0"
	binlog2 "github.com/vkcom/statshouse/internal/vkgo/binlog"
	"github.com/vkcom/statshouse/internal/vkgo/binlog/fsbinlog"
	"go.uber.org/atomic"
	"go.uber.org/multierr"
	"pgregory.net/rand"
)

// TODO: explicit blocking Engine.Run to run binlog
// TODO: use build of sqlite with custom WAL magic to prevent accidental checkpointing by command-line tools
// TODO: check the app ID at startup
// TODO: check the results of PRAGMA statements
// TODO: use mmap in all connections?
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
	defaultPageSize    = 4 * 1024 // 4KB

	beginStmt  = "BEGIN IMMEDIATE" // make sure we don't get SQLITE_BUSY in the middle of transaction
	commitStmt = "COMMIT"
	normSelect = "SELECT"
	normVacuum = "VACUUM INTO"

	initOffsetTable     = "CREATE TABLE IF NOT EXISTS __binlog_offset (offset INTEGER);"
	snapshotMetaTable   = "CREATE TABLE IF NOT EXISTS __snapshot_meta (meta BLOB);"
	internalQueryPrefix = "__"
)

var (
	safeStatements = []string{"SELECT", "INSERT", "UPDATE", "DELETE", "REPLACE", "UPSERT"}
)

type (
	Engine struct {
		opt  Options
		ctx  context.Context
		stop func()
		rw   *sqliteConn
		//	chk *sqlite0.Conn
		roMx    sync.Mutex
		roFree  []*sqliteConn
		roCond  *sync.Cond
		roCount int

		mode engineMode

		binlog         binlog2.Binlog
		dbOffset       int64
		commitOffset   atomic.Int64 // debug only
		lastCommitTime time.Time

		waitQMx       sync.Mutex
		waitQ         []waitCommitInfo
		committedInfo *atomic.Value

		apply                ApplyEventFunction
		scan                 ApplyEventFunction
		waitUntilBinlogReady chan error
		readyNotify          sync.Once
		commitCh             chan struct{}

		binlogEnd chan struct{}

		isTest            bool
		mustCommitNowFlag bool
		mustWaitCommit    bool
		readOnlyEngine    bool

		logMx       sync.Mutex
		nextLogTime time.Time
	}

	Options struct {
		Path                  string
		APPID                 uint32
		StatsOptions          StatsOptions
		Scheme                string
		Replica               bool
		CommitEvery           time.Duration
		DurabilityMode        DurabilityMode
		ReadAndExit           bool
		CommitOnEachWrite     bool // use only to test. If true break binlog + sqlite consistency
		WaitBinlogCommitDebug bool // ...

		MaxROConn              int
		CacheMaxSizePerConnect int
		PageSize               int
	}

	waitCommitInfo struct {
		offset   int64
		readWait bool
		waitCh   chan struct{}
	}

	committedInfo struct {
		meta   []byte
		offset int64
	}

	stmtInfo struct {
		stmt         *sqlite0.Stmt
		isSafe       bool
		isSelect     bool
		isVacuumInto bool
	}

	ApplyEventFunction func(conn Conn, offset int64, cache []byte) (int, error)

	engineMode     int
	DurabilityMode int
)

const (
	master  engineMode = iota // commit sql tx by timer, to synchronize sqlite and binlog
	replica engineMode = iota // queue data in apply and commit sql tx after got commit from binlog
)

const (
	WaitCommit   DurabilityMode = iota // Wait for commit to finish before returning from Do()
	NoWaitCommit DurabilityMode = iota // Do not wait for commit to finish before returning from Do()
	NoBinlog     DurabilityMode = iota // Do not use binlog, just commit to sqlite
)

func OpenRO(opt Options) (*Engine, error) {
	rawConn, err := sqlite0.Open(opt.Path, sqlite0.OpenReadonly)
	if err != nil {
		return nil, err
	}
	conn := newSqliteConn(rawConn, 1)

	ctx, stop := context.WithCancel(context.Background())
	e := &Engine{
		rw:             nil,
		ctx:            ctx,
		stop:           stop,
		opt:            opt,
		roFree:         []*sqliteConn{conn},
		roCount:        1,
		mode:           replica,
		readOnlyEngine: true,
	}
	e.roCond = sync.NewCond(&e.roMx)
	return e, nil
}

func OpenROWal(opt Options) (*Engine, error) {
	ro, err := openROWAL(opt.Path, opt.PageSize)
	if err != nil {
		return nil, err
	}
	conn := newSqliteConn(ro, opt.CacheMaxSizePerConnect)

	ctx, stop := context.WithCancel(context.Background())
	e := &Engine{
		rw:             nil,
		ctx:            ctx,
		stop:           stop,
		opt:            opt,
		roFree:         []*sqliteConn{conn},
		roCount:        1,
		mode:           replica,
		readOnlyEngine: true,
	}
	e.roCond = sync.NewCond(&e.roMx)
	return e, nil
}

func checkWals(dbPath string) (wal1NotExists bool, wal2NotExists bool, err error) {
	wal1Path := dbPath + "-wal"
	wal2Path := dbPath + "-wal2"
	_, err = os.Stat(wal1Path)
	if os.IsNotExist(err) {
		wal1NotExists = true
		err = nil
	}
	if err != nil {
		return false, false, err
	}
	_, err = os.Stat(wal2Path)
	if os.IsNotExist(err) {
		wal2NotExists = true
		err = nil
	}
	if err != nil {
		return false, false, err
	}
	return
}

func OpenEngine(
	opt Options,
	binlog binlog2.Binlog, // binlog - will be closed during Close execution
	apply ApplyEventFunction,
	scan ApplyEventFunction, // this helps to work in replica mode with old binlog
) (*Engine, error) {
	wal1NotExists, wal2NotExists, err := checkWals(opt.Path)
	if err != nil {
		return nil, fmt.Errorf("faield to check wal existence: %w", err)
	}
	if wal1NotExists {
		log.Println("wal1 is not exist")
	} else {
		log.Println("wal1 is exist")
	}
	if wal2NotExists {
		log.Println("wal2 is not exist")
	} else {
		log.Println("wal2 is exist")
	}
	e, err := openDB(opt, binlog, apply, scan)
	if err != nil {
		return nil, err
	}
	err = e.runBinlogAndWaitReady()
	if err != nil {
		e.close(false, false)
		return nil, err
	}
	if e.opt.DurabilityMode == WaitCommit {
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
	rw, err := openRW(openWAL, opt.Path, opt.APPID, opt.PageSize, opt.Scheme, initOffsetTable, snapshotMetaTable)
	if err != nil {
		return nil, fmt.Errorf("failed to open RW connection: %w", err)
	}

	ctx, stop := context.WithCancel(context.Background())
	e := &Engine{
		rw:   newSqliteConn(rw, opt.CacheMaxSizePerConnect),
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
		waitUntilBinlogReady: make(chan error, 1),
		commitCh:             make(chan struct{}, 1),
		mode:                 replica,
		binlogEnd:            make(chan struct{}),
	}
	if !opt.Replica {
		e.mode = master
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
func (e *Engine) binlogRun() (*binlogEngineReplicaImpl, error) {
	impl := &binlogEngineReplicaImpl{e: e}
	e.committedInfo.Store(&committedInfo{})
	offset, err := e.binlogLoadOrCreatePosition()
	if err != nil {
		_ = e.close(false, false)
		return nil, fmt.Errorf("failed to load binlog position: %w", err)
	}
	e.dbOffset = offset
	log.Println("[sqlite] read from db binlog position: ", e.dbOffset)
	meta, err := e.binlogLoadOrCreateMeta()
	if err != nil {
		_ = e.close(false, false)
		return nil, fmt.Errorf("failed to load snapshot meta: %w", err)
	}
	if !e.isTest {
		log.Println("[sqlite] starting binlog")
		go func() {
			var err error
			defer func() {
				close(e.binlogEnd)
			}()
			defer func() {
				if e.opt.ReadAndExit || err != nil {
					e.readyNotify.Do(func() {
						e.waitUntilBinlogReady <- err
						close(e.waitUntilBinlogReady)
					})

				}
			}()
			err = e.binlog.Run(offset, meta, nil, impl)
			if err != nil {
				e.rw.mu.Lock()
				e.rw.err = err
				e.rw.mu.Unlock()
			}

		}()
	}
	return impl, nil
}

func (e *Engine) binlogWaitReady(impl *binlogEngineReplicaImpl) error {
	startRereadingTime := time.Now()
	err := <-e.waitUntilBinlogReady
	if err != nil {
		return err
	}
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

func openWAL(path string, flags int, pageSize int) (*sqlite0.Conn, error) {
	conn, err := sqlite0.Open(path, flags)
	if err != nil {
		return nil, err
	}

	err = conn.SetBusyTimeout(busyTimeout)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to set DB busy timeout to %v: %w", busyTimeout, err)
	}

	if pageSize <= 0 {
		pageSize = defaultPageSize
	}

	err = conn.Exec(fmt.Sprintf("PRAGMA page_size=%d", pageSize))
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to set page_size: %w", err)
	}

	err = conn.Exec("PRAGMA journal_mode=WAL2")
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to enable DB WAL mode: %w", err)
	}
	return conn, nil
}

func openRW(open func(path string, flags int, pageSize int) (*sqlite0.Conn, error), path string, appID uint32, pageSize int, schemas ...string) (*sqlite0.Conn, error) {
	conn, err := open(path, sqlite0.OpenReadWrite|sqlite0.OpenCreate, pageSize)
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

//	func openChk(pathDB string) (*sqlite0.Conn, error) {
//		conn, err := openWAL(pathDB, sqlite0.OpenReadWrite|sqlite0.OpenNoMutex|sqlite0.OpenPrivateCache)
//		if err != nil {
//			return nil, err
//		}
//
//		return conn, nil
//	}

func openROWAL(path string, pageSize int) (*sqlite0.Conn, error) {
	return openWAL(path, sqlite0.OpenReadonly, pageSize)
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
	go func() {
		err := e.close(true, e.opt.DurabilityMode != NoBinlog)
		select {
		case ch <- err:
			close(ch)
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
	var error error
	if e.opt.DurabilityMode != NoBinlog && e.binlog != nil {
		e.binlog.RequestShutdown()
	}
	for {
		/*
			активное ожидаение чтобы не усложнять код либы, требуется для более безопасного переезда на вторую версию
		*/
		e.roMx.Lock()
		if len(e.roFree) != e.roCount {
			log.Println("[sqlite] don't use RO connections when close engine")
			e.roMx.Unlock()
			time.Sleep(time.Millisecond * 10)
		} else {
			break
		}
	}
	defer e.roMx.Unlock()
	for _, conn := range e.roFree {
		err := conn.Close()
		if err != nil {
			multierr.AppendInto(&error, fmt.Errorf("failed to close RO connection: %w", err))
		}
	}
	if !e.readOnlyEngine {
		if shouldCommit && error == nil {
			err := e.commitTXAndStartNew(true, waitCommitBinlog)
			if err != nil {
				multierr.AppendInto(&error, fmt.Errorf("failed to commit before close: %w", err))
			}
		}
		err := e.rw.Close()
		if err != nil {
			multierr.AppendInto(&error, fmt.Errorf("failed to close RW connection: %w", err))
		}
	}

	// multierr.AppendInto(&err, e.chk.Close())
	// multierr.AppendInto(&err, e.ro.Close()) // close RO one last to prevent checkpoint-on-close logic in other connections
	return error
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

func (e *Engine) binlogWaitDBSync(conn Conn) (*committedInfo, error) {
	defer e.opt.StatsOptions.measureWaitDurationSince(waitBinlogSync, time.Now())
	info, _ := e.committedInfo.Load().(*committedInfo)
	binlogFinished := false
	for info.offset < e.dbOffset {
		if binlogFinished {
			return nil, fmt.Errorf("binlog closed")
		}
		select {
		case <-e.commitCh:
		case <-e.binlogEnd:
			binlogFinished = true
		}
		info, _ = e.committedInfo.Load().(*committedInfo)
	}
	return info, nil
}

func (e *Engine) commitTXAndStartNew(commit, waitBinlogCommit bool) error {
	c := e.rw.startNewConn(false, context.Background(), &e.opt.StatsOptions)
	defer c.close(nil)
	return e.commitRWTXAndStartNewLocked(c, commit, waitBinlogCommit, false)

}

func (e *Engine) commitRWTXAndStartNewLocked(c Conn, commit, waitBinlogCommit, skipUpdateMeta bool) (err error) {
	var info *committedInfo
	c = c.withoutTimeout()
	defer func() {
		c.c.spIn = false
	}()
	if waitBinlogCommit && e.binlog != nil {
		info, err = e.binlogWaitDBSync(c)
		if err != nil {
			return err
		}
	}
	if commit {
		startCommit := time.Now()
		if !skipUpdateMeta && e.binlog != nil && info != nil && len(info.meta) > 0 {
			err = e.binlogUpdateMeta(c, info.meta)
			if err != nil {
				e.rw.err = err
			}
		}
		if e.rw.err == nil {
			_, err := c.exec(true, "__commit", nil, commitStmt)
			if err != nil {
				e.rw.err = fmt.Errorf("periodic tx commit failed: %w", err)
			}
		}
		e.opt.StatsOptions.measureActionDurationSince("commit_tx", startCommit)
	}

	if e.rw.err == nil {
		_, err := c.exec(true, "__begin_tx", nil, beginStmt)
		if err != nil {
			e.rw.err = fmt.Errorf("periodic tx begin failed: %w", err)
		}
	}
	e.lastCommitTime = time.Now()
	return e.rw.err
}

func backupToTemp(ctx context.Context, conn *sqliteConn, prefix string, stats *StatsOptions) (string, error) {
	c := conn.startNewConn(false, ctx, stats)
	defer c.close(nil)
	path := prefix + "." + strconv.FormatUint(rand.Uint64(), 10) + ".tmp"
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		_, err := c.ExecUnsafe("__vacuum", "VACUUM INTO $to", TextString("$to", path))
		conn.err = err
	}
	return path, conn.err
}

func getBackupPath(e *Engine, prefix string) (string, int64, error) {
	c := e.rw.startNewConn(false, context.Background(), &e.opt.StatsOptions)
	defer c.close(nil)
	pos, _, err := binlogLoadPosition(c)
	if err != nil {
		return "", pos, err
	}

	copyPos := pos
	numLen := -4
	for copyPos > 0 {
		numLen++
		copyPos /= 10
	}
	if numLen < 0 {
		numLen = 0
	}

	posStr := fmt.Sprintf(`%04d`, pos)
	prefix = fmt.Sprintf(`%s.%02d`, prefix, numLen)

	for l := 4; l <= len(posStr); l++ {
		filename := prefix + posStr[:l]
		if _, err = os.Stat(filename); os.IsNotExist(err) {
			return filename, pos, nil
		}
	}

	return "", pos, fmt.Errorf("can not create backup with pos=%d, probably backup already exist", pos)
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
	c := e.rw.startNewConn(true, context.Background(), &e.opt.StatsOptions)
	defer c.close(nil)
	err := fn(c)
	if err != nil {
		e.rw.spOk = false
		return err
	}
	e.rw.spOk = true
	return err
}

func (e *Engine) Backup(ctx context.Context, prefix string) (string, int64, error) {
	defer e.opt.StatsOptions.measureActionDurationSince("backup", time.Now())
	var path string
	err := doSingleROToWALQuery(e.opt.Path, e.opt.PageSize, func(conn *sqliteConn) error {
		var err error
		path, err = backupToTemp(ctx, conn, prefix, &e.opt.StatsOptions)
		return err
	})
	defer func() {
		_ = os.Remove(path)
	}()
	if err != nil {
		return "", 0, err
	}
	var backupExpectedPath string
	var snapshotPos int64
	err = doSingleROQuery(path, func(e *Engine) error {
		backupExpectedPath, snapshotPos, err = getBackupPath(e, prefix)
		return err
	})
	if err != nil {
		return "", snapshotPos, err
	}
	return backupExpectedPath, snapshotPos, os.Rename(path, backupExpectedPath)
}

func (e *Engine) View(ctx context.Context, queryName string, fn func(Conn) error) (err error) {
	if err = checkQueryName(queryName); err != nil {
		return err
	}
	startTimeBeforeLock := time.Now()
	e.roMx.Lock()
	var conn *sqliteConn
	for len(e.roFree) == 0 && e.roCount >= e.opt.MaxROConn {
		e.roCond.Wait()
	}
	if len(e.roFree) == 0 {
		ro, err := openROWAL(e.opt.Path, e.opt.PageSize)
		if err != nil {
			e.roMx.Unlock()
			return fmt.Errorf("failed to open RO connection: %w", err)
		}
		conn = newSqliteConn(ro, e.opt.CacheMaxSizePerConnect)
		e.roCount++
	} else {
		conn = (e.roFree)[0]
		e.roFree = (e.roFree)[1:]
	}
	e.roMx.Unlock()
	defer func() {
		e.roMx.Lock()
		e.roFree = append(e.roFree, conn)
		e.roMx.Unlock()
		e.roCond.Signal()
	}()
	e.opt.StatsOptions.measureWaitDurationSince(waitView, startTimeBeforeLock)
	c, err := conn.startNewROConn(ctx, &e.opt.StatsOptions)
	if err != nil {
		return multierr.Append(errEngineBroken, err)
	}
	defer func() {
		err = multierr.Append(err, c.closeRO())
	}()
	defer e.opt.StatsOptions.measureSqliteTxDurationSince(txView, queryName, time.Now())
	err = fn(c)

	return err
}

func (e *Engine) mustCommitNow(waitCommitMode, isReadOp bool) bool {
	if !e.isTest {
		return ((e.opt.WaitBinlogCommitDebug && e.opt.CommitOnEachWrite) || (time.Since(e.lastCommitTime) >= e.opt.CommitEvery && !waitCommitMode)) && !isReadOp
	}
	return e.mustCommitNowFlag
}

func checkQueryName(qn string) error {
	if strings.HasPrefix(qn, internalQueryPrefix) {
		return fmt.Errorf("query prefix %q is reserved", internalQueryPrefix)
	}
	return nil
}

func (e *Engine) doWithoutWait(ctx context.Context, queryName string, fn func(Conn, []byte) ([]byte, error)) (_ chan struct{}, dbOffset int64, commitOffset int64, err error) {
	if err := checkQueryName(queryName); err != nil {
		return nil, 0, 0, err
	}
	startTimeBeforeLock := time.Now()
	var commit func(c Conn) error = nil
	c, err := e.rw.startNewRWConn(true, ctx, &e.opt.StatsOptions, e)
	if err != nil {
		return nil, 0, 0, multierr.Append(errEngineBroken, err)
	}
	offsetBeforeWrite := e.dbOffset
	defer func() {
		if err != nil {
			log.Println("[sqlite] return err to user", err.Error(), "offsetBeforeWrite:", offsetBeforeWrite, "offsetAfterWrite:", e.dbOffset)
			e.dbOffset = offsetBeforeWrite
		}
		err1 := c.close(commit)
		if err == nil {
			if err1 != nil {
				log.Println("[sqlite] got error during to close: ", err1.Error())
			}
			err = multierr.Append(err, err1)
		}
	}()
	e.opt.StatsOptions.measureWaitDurationSince(waitDo, startTimeBeforeLock)
	defer e.opt.StatsOptions.measureSqliteTxDurationSince(txDo, queryName, time.Now())
	buffer, err := fn(c, nil)
	if err != nil {
		return nil, e.dbOffset, e.commitOffset.Load(), err
	}
	if e.opt.DurabilityMode == NoBinlog {
		e.rw.spOk = err == nil
		commit = func(c Conn) error {
			return e.commitRWTXAndStartNewLocked(c, true, false, true)
		}
		return nil, e.dbOffset, e.commitOffset.Load(), err
	}
	shouldWriteBinlog := len(buffer) > 0
	isReadOp := !shouldWriteBinlog
	if shouldWriteBinlog && e.mode == replica {
		return nil, e.dbOffset, e.commitOffset.Load(), fmt.Errorf("failed to write binlog in replica mode") // TODO replace with GMS error
	}
	var ch chan struct{}
	waitCommitMode := e.opt.DurabilityMode == WaitCommit
	mustCommitNow := e.mustCommitNow(waitCommitMode, isReadOp)
	var offsetAfterWritePredicted int64
	if shouldWriteBinlog {
		offsetAfterWritePredicted = offsetBeforeWrite + int64(fsbinlog.AddPadding(len(buffer)))
		err = binlogUpdateOffset(c, offsetAfterWritePredicted)
		if err != nil {
			log.Println("[sqlite] failed to update binlog position:", err.Error())
			return nil, e.dbOffset, e.commitOffset.Load(), err
		}

		var offsetAfterWrite int64
		if waitCommitMode || mustCommitNow {
			offsetAfterWrite, err = e.binlog.AppendASAP(e.dbOffset, buffer)
		} else {
			offsetAfterWrite, err = e.binlog.Append(e.dbOffset, buffer)
		}
		if err != nil {
			log.Println("[sqlite] got error from binlog:", err.Error())
			return nil, e.dbOffset, e.commitOffset.Load(), err
		}
		// after this line can't rollback tx!!!!!!
		e.dbOffset = offsetAfterWrite
	}
	if e.opt.CommitOnEachWrite {
		commit = func(c Conn) error {
			return e.commitRWTXAndStartNewLocked(c, true, e.opt.WaitBinlogCommitDebug, !e.opt.WaitBinlogCommitDebug)
		}
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
		commit = func(c Conn) error {
			return e.commitRWTXAndStartNewLocked(c, true, false, false)
		}
	}
	e.rw.spOk = err == nil
	return ch, e.dbOffset, e.commitOffset.Load(), err
}

func (e *Engine) DoWithOffset(ctx context.Context, queryName string, fn func(Conn, []byte) ([]byte, error)) (dbOffset int64, commitOffset int64, err error) {
	if e.readOnlyEngine {
		return 0, 0, errReadOnly
	}
	ch, dbOffset, commitedOffset, err := e.doWithoutWait(ctx, queryName, fn)
	if err != nil {
		return dbOffset, commitedOffset, err
	}
	if ch != nil {
		<-ch
	}
	return dbOffset, commitedOffset, nil
}

// Do require handle of errEngineBroken
func (e *Engine) Do(ctx context.Context, queryName string, fn func(Conn, []byte) ([]byte, error)) error {
	_, _, err := e.DoWithOffset(ctx, queryName, fn)
	return err
}

func (e *Engine) rareLog(format string, v ...any) {
	e.logMx.Lock()
	defer e.logMx.Unlock()
	now := time.Now()
	if now.After(e.nextLogTime) {
		log.Printf(format, v...)
		e.nextLogTime = now.Add(time.Second * 10)
	}
}

func binlogUpdateOffset(c Conn, offset int64) error {
	_, err := c.Exec("__update_binlog_pos", "UPDATE __binlog_offset set offset = $offset;", Int64("$offset", offset))
	return err
}
