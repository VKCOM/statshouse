package sqlitev2

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"

	"github.com/vkcom/statshouse/internal/vkgo/binlog"
	"go.uber.org/multierr"
	"pgregory.net/rand"
)

type (
	Engine struct {
		opt               Options
		rw                *sqliteConn
		binlog            binlog.Binlog
		binlogEngine      *binlogEngine
		finishBinlogRunCh chan struct{}
		readyCh           chan error
		readOnly          bool

		roConnPool  *connPool
		readyNotify sync.Once
	}

	Options struct {
		Path                         string
		APPID                        int32
		Scheme                       string
		ReadOnly                     bool
		MaxROConn                    int
		CacheApproxMaxSizePerConnect int
		ShowLastInsertID             bool
		BinlogOptions
	}
	BinlogOptions struct {
		Replica     bool
		ReadAndExit bool
	}
	ApplyEventFunction func(conn Conn, payload []byte) (int, error)
)

const (
	initOffsetTable     = "CREATE TABLE IF NOT EXISTS __binlog_offset (offset INTEGER);"
	snapshotMetaTable   = "CREATE TABLE IF NOT EXISTS __snapshot_meta (meta BLOB);"
	internalQueryPrefix = "__"
)

func openRO(opt Options) (*Engine, error) {
	e := &Engine{
		opt:      opt,
		readOnly: true,
		roConnPool: newConnPool(opt.MaxROConn, func() (*sqliteConn, error) {
			return newSqliteROWALConn(opt.Path, opt.CacheApproxMaxSizePerConnect)
		}),
	}
	return e, nil
}

/*
engine := OpenEngine(...)
can use engine as sqlite wrapper

go engine.Run(...)
can't use engine

engine.WaitReady()
can use engine as sqlite + binlog wrapper
*/
func OpenEngine(opt Options) (*Engine, error) {
	if opt.ReadOnly {
		return openRO(opt)
	}
	rw, err := newSqliteRWWALConn(opt.Path, opt.APPID, opt.ShowLastInsertID, opt.CacheApproxMaxSizePerConnect)
	if err != nil {
		return nil, err
	}
	err = rw.applyScheme(initOffsetTable, snapshotMetaTable, opt.Scheme)
	if err != nil {
		errClose := rw.Close()
		return nil, fmt.Errorf("failed to apply acheme: %w", multierr.Append(err, errClose))
	}
	e := &Engine{
		opt:               opt,
		rw:                rw,
		finishBinlogRunCh: make(chan struct{}),
		readyCh:           make(chan error, 1),
		roConnPool: newConnPool(opt.MaxROConn, func() (*sqliteConn, error) {
			return newSqliteROWALConn(opt.Path, opt.CacheApproxMaxSizePerConnect)
		}),
	}
	return e, nil
}

/*
binlog - will be closed during to Engine.Close
*/
func (e *Engine) Run(binlog binlog.Binlog, applyEventFunction ApplyEventFunction) (err error) {
	e.binlog = binlog
	defer func() { close(e.finishBinlogRunCh) }()
	e.rw.dbOffset, err = e.binlogLoadOrCreatePosition()
	if err != nil {
		err = fmt.Errorf("failed to load binlog position during to start run: %w", err)
		e.readyNotify.Do(func() {
			e.readyCh <- err
			close(e.readyCh)
		})
		return err
	}
	meta, err := e.binlogLoadOrCreateMeta()
	if err != nil {
		err = fmt.Errorf("failed to load binlog meta durint to start run: %w", err)
		e.readyNotify.Do(func() {
			e.readyCh <- err
			close(e.readyCh)
		})
		return err
	}
	e.binlogEngine = newBinlogEngine(e, applyEventFunction)
	err = e.rw.setError(e.binlog.Run(e.rw.dbOffset, meta, e.binlogEngine))
	// TODO race?
	e.rw.mu.Lock()
	defer e.rw.mu.Unlock()
	e.binlog = nil
	if err != nil {
		e.readyNotify.Do(func() {
			e.readyCh <- err
			close(e.readyCh)
		})
		return err
	}
	e.readyNotify.Do(func() {
		close(e.readyCh)
	})
	return nil
}

func (e *Engine) WaitReady() error {
	return <-e.readyCh
}

func (e *Engine) Backup(ctx context.Context, prefix string) (string, error) {
	if prefix == "" {
		return "", fmt.Errorf("backup prefix is empty")
	}
	conn, err := newSqliteROWALConn(e.opt.Path, 1)
	if err != nil {
		return "", fmt.Errorf("failed to open RO connection to backup: %w", err)
	}
	defer func() {
		_ = conn.Close()
	}()
	c := newUserConn(conn, ctx)
	path := prefix + "." + strconv.FormatUint(rand.Uint64(), 10) + ".tmp"
	defer func() {
		_ = os.Remove(path)
	}()
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			_, err := c.Exec("__vacuum", "VACUUM INTO $to", TextString("$to", path))
			if err != nil {
				return path, err
			}
		} else {
			return path, fmt.Errorf("os.Stats failed: %w", err)
		}
	}

	conn1, err := newSqliteROConn(path)
	if err != nil {
		return "", fmt.Errorf("failed to open RO connection to rename backup: %w", err)
	}
	defer func() {
		_ = conn1.Close()
	}()
	c1 := newUserConn(conn1, ctx)
	expectedPath, err := getBackupPath(c1, prefix)
	if err != nil {
		return "", fmt.Errorf("failed to get backup path: %w", err)
	}
	return expectedPath, os.Rename(path, expectedPath)
}

func getBackupPath(conn internalConn, prefix string) (string, error) {
	pos, isExists, err := binlogLoadPosition(conn)
	if err != nil {
		return "", fmt.Errorf("failed to load binlog position from backup: %w", err)
	}
	if !isExists {
		return "", fmt.Errorf("failed to load binlog position: db is empty")
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
			return filename, nil
		}
	}

	return "", fmt.Errorf("can not create backup with pos=%d, probably backup already exist", pos)
}

func (e *Engine) View(ctx context.Context, queryName string, fn func(Conn) error) (err error) {
	if err = checkUserQueryName(queryName); err != nil {
		return err
	}

	conn, err := e.roConnPool.get()
	if err != nil {
		return fmt.Errorf("faield to get RO conn: %w", err)
	}
	defer e.roConnPool.put(conn)

	err = conn.beginTxLocked()
	if err != nil {
		return fmt.Errorf("failed to begon RO tx: %w", err)
	}
	defer func() {
		errRollback := conn.rollbackLocked()
		if errRollback != nil {
			err = multierr.Append(err, errRollback)
		}
	}()
	c := newUserConn(conn, ctx)
	err = fn(c)
	if err != nil {
		return fmt.Errorf("user error: %w", err)
	}
	err = conn.nonBinlogCommitTxLocked()
	if err != nil {
		return fmt.Errorf("failed to commit RO tx: %w", err)
	}
	return err
}

func (e *Engine) Do(ctx context.Context, queryName string, do func(c Conn, cache []byte) ([]byte, error)) (err error) {
	if err := checkUserQueryName(queryName); err != nil {
		return err
	}
	// todo calc wait lock duration
	e.rw.mu.Lock()
	defer e.rw.mu.Unlock()
	if e.readOnly {
		return ErrReadOnly
	}
	err = e.rw.beginTxLocked()
	if err != nil {
		return fmt.Errorf("failed to begin tx: %w", err)
	}
	defer func() {
		errRollback := e.rw.rollbackLocked()
		if errRollback != nil {
			err = multierr.Append(err, errRollback)
		}
	}()
	conn := newUserConn(e.rw, ctx)
	bytes, err := do(conn, e.rw.binlogCache[:0])
	if err != nil {
		return fmt.Errorf("user error: %w", err)
	}
	if len(bytes) == 0 {
		return e.rw.nonBinlogCommitTxLocked()
	}
	if e.binlog == nil {
		return fmt.Errorf("can't write binlog event: binlog is nil")
	}
	offsetAfterWrite, err := e.binlog.Append(e.rw.dbOffset, bytes)
	if err != nil {
		return fmt.Errorf("binlog Append return error: %w", err)
	}
	return e.rw.binlogCommitTxLocked(offsetAfterWrite)
}

// В случае возникновения ошибки двиожк считается сломаным
func (e *Engine) internalDo(name string, do func(c internalConn) error) error {
	if err := checkInternalQueryName(name); err != nil {
		return err
	}
	e.rw.mu.Lock()
	defer e.rw.mu.Unlock()
	err := e.internalDoLocked(name, do)
	return e.rw.setErrorLocked(err)
}
func (e *Engine) internalDoLocked(name string, do func(c internalConn) error) (err error) {
	err = e.rw.beginTxLocked()
	if err != nil {
		return fmt.Errorf("failed to begin tx: %w", err)
	}
	defer func() {
		errRollback := e.rw.rollbackLocked()
		if errRollback != nil {
			err = multierr.Append(err, errRollback)
		}
	}()
	conn := newUserConn(e.rw, context.Background()) // внутренние запросы не таймаутят
	err = do(conn)
	if err != nil {
		return fmt.Errorf("user logic error: %w", err)
	}
	return e.rw.nonBinlogCommitTxLocked()
}

func (e *Engine) binlogLoadOrCreateMeta() ([]byte, error) {
	var meta []byte
	err := e.internalDo("__load_binlog", func(conn Conn) error {
		rows := conn.Query("__select_meta", "SELECT meta from __snapshot_meta")
		if rows.err != nil {
			return rows.err
		}
		for rows.Next() {
			meta, _ = rows.ColumnBlob(0, meta)
			return nil
		}
		_, err := conn.Exec("__insert_meta", "INSERT INTO __snapshot_meta(meta) VALUES($meta)", Blob("$meta", meta))
		return err
	})
	return meta, err
}

func (e *Engine) binlogLoadOrCreatePosition() (int64, error) {
	var offset int64
	err := e.internalDo("__load_binlog", func(conn Conn) error {
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

func (e *Engine) Close() error {
	return e.close(e.binlog != nil && !e.opt.ReadAndExit)
}

func (e *Engine) close(waitCommitBinlog bool) error {
	if !e.readOnly {
		e.rw.mu.Lock()
		e.readOnly = true
		e.rw.mu.Unlock()
	}
	var error error
	if waitCommitBinlog {
		err := e.binlog.Shutdown()
		if err != nil {
			multierr.AppendInto(&error, err)
		}
		<-e.finishBinlogRunCh
	}
	if !e.readOnly {
		err := e.rw.Close()
		if err != nil {
			multierr.AppendInto(&error, fmt.Errorf("failed to close RW connection: %w", err))
		}
	}
	//todo wait when read is finish?
	e.roConnPool.close(&error)
	return error
}

func binlogLoadPosition(conn internalConn) (offset int64, isExists bool, err error) {
	rows := conn.Query("__select_binlog_pos", "SELECT offset from __binlog_offset")
	if rows.err != nil {
		return 0, false, rows.err
	}
	for rows.Next() {
		offset := rows.ColumnInt64(0)
		return offset, true, nil
	}
	return 0, false, nil
}

func checkUserQueryName(qn string) error {
	if len(qn) > 2 && qn[0:2] == internalQueryPrefix {
		return fmt.Errorf("query prefix %q is reserved, got: %s", internalQueryPrefix, qn)
	}
	return nil
}

func checkInternalQueryName(qn string) error {
	if len(qn) > 2 && qn[0:2] == internalQueryPrefix {
		return nil
	}
	return fmt.Errorf("use prefix %s for internal query, got: %s", internalQueryPrefix, qn)
}
