package sqlitev2

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/vkcom/statshouse/internal/vkgo/binlog"
	"go.uber.org/multierr"
	"pgregory.net/rand"
)

type (
	Engine struct {
		opt               Options
		rw                *sqliteRWConn
		binlog            binlog.Binlog
		binlogEngine      *binlogEngineReplicaImpl
		finishBinlogRunCh chan struct{}
		readyCh           chan error
		readOnly          bool

		roMx    sync.Mutex
		roFree  []*sqliteRWConn
		roCond  *sync.Cond
		roCount int

		readyNotify sync.Once
	}

	Options struct {
		Path                   string
		APPID                  int32
		Scheme                 string
		Replica                bool
		ReadAndExit            bool
		MaxROConn              int
		CacheMaxSizePerConnect int
		ShowLastInsertID       bool
	}
	ApplyEventFunction func(conn Conn, cache []byte) (int, error)
)

const (
	initOffsetTable     = "CREATE TABLE IF NOT EXISTS __binlog_offset (offset INTEGER);"
	snapshotMetaTable   = "CREATE TABLE IF NOT EXISTS __snapshot_meta (meta BLOB);"
	internalQueryPrefix = "__"
)

func OpenRO(opt Options) (*Engine, error) {
	if opt.MaxROConn <= 0 {
		opt.MaxROConn = 128
	}
	e := &Engine{
		opt:      opt,
		readOnly: true,
	}
	e.roCond = sync.NewCond(&e.roMx)
	return e, nil
}

func OpenEngine(opt Options, binlog binlog.Binlog) (*Engine, error) {
	if opt.MaxROConn <= 0 {
		opt.MaxROConn = 128
	}
	rw, err := newSqliteRWWALConn(opt.Path, opt.APPID, opt.ShowLastInsertID, opt.CacheMaxSizePerConnect, initOffsetTable, snapshotMetaTable, opt.Scheme)
	if err != nil {
		return nil, err
	}
	e := &Engine{
		opt:               opt,
		rw:                rw,
		finishBinlogRunCh: make(chan struct{}),
		readyCh:           make(chan error, 1),
		binlog:            binlog,
	}
	e.roCond = sync.NewCond(&e.roMx)
	return e, nil
}

/*
binlog - будет закрыт внутри Engine при вызове Close
*/
// todo вернуть бинлог в параметр
func (e *Engine) Run(applyEventFunction ApplyEventFunction) (err error) {
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
	if err != nil {
		err = fmt.Errorf("failed run binlog: %w", err)
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
	if e.binlog == nil {
		return nil
	}
	return <-e.readyCh
}

func (e *Engine) Backup(ctx context.Context, prefix string) (string, error) {
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

	conn1, err := newSqliteROWALConn(path, 1)
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
	_ = conn1.Close()
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
	if err = checkQueryName(queryName); err != nil {
		return err
	}
	e.roMx.Lock()
	var conn *sqliteRWConn
	for len(e.roFree) == 0 && e.roCount >= e.opt.MaxROConn {
		e.roCond.Wait()
	}
	if len(e.roFree) == 0 {
		conn, err = newSqliteROWALConn(e.opt.Path, e.opt.CacheMaxSizePerConnect)
		if err != nil {
			e.roMx.Unlock()
			return fmt.Errorf("failed to open RO connection: %w", err)
		}
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
	conn.mu.Lock()
	defer conn.mu.Unlock()
	txID, err := conn.beginTxLocked()
	if err != nil {
		return fmt.Errorf("failed to begon RO tx: %w", err)
	}
	defer conn.rollbackLocked(txID)
	c := newUserConn(conn, ctx)
	err = fn(c)
	if err != nil {
		return fmt.Errorf("user error: %w", err)
	}
	err = conn.nonBinlogCommitTxLocked(txID)
	if err != nil {
		return fmt.Errorf("failed to commit RO tx: %w", err)
	}
	return err
}

func (e *Engine) Do(ctx context.Context, queryName string, do func(c Conn, cache []byte) ([]byte, error)) error {
	if err := checkQueryName(queryName); err != nil {
		return err
	}
	// todo calc wait lock duration
	e.rw.mu.Lock()
	defer e.rw.mu.Unlock()
	txId, err := e.rw.beginTxLocked()
	if err != nil {
		return fmt.Errorf("failed to begin tx: %w", err)
	}
	defer func() { _ = e.rw.rollbackLocked(txId) }()
	conn := newUserConn(e.rw, ctx)
	bytes, err := do(conn, e.rw.binlogCache[:0])
	if err != nil {
		return fmt.Errorf("user error: %w", err)
	}
	if len(bytes) == 0 {
		return e.rw.nonBinlogCommitTxLocked(txId)
	}
	offsetAfterWrite, err := e.binlog.Append(e.rw.dbOffset, bytes)
	if err != nil {
		return fmt.Errorf("binlog Apprent return error: %w", err)
	}
	return e.rw.binlogCommitTxLocked(txId, offsetAfterWrite)
}

// В случае возникновения ошибки двиожк считается сломаным
func (e *Engine) internalDo(name string, do func(c internalConn) error) error {
	e.rw.mu.Lock()
	defer e.rw.mu.Unlock()
	err := e.internalDoLocked(name, do)
	return e.rw.setErrorLocked(err)
}
func (e *Engine) internalDoLocked(name string, do func(c internalConn) error) error {
	txId, err := e.rw.beginTxLocked()
	if err != nil {
		return fmt.Errorf("failed to begin tx: %w", err)
	}
	defer func() { _ = e.rw.rollbackLocked(txId) }()
	conn := newUserConn(e.rw, context.Background()) // внутренние запросы не таймаутят
	err = do(conn)
	if err != nil {
		return fmt.Errorf("user logic error: %w", err)
	}
	return e.rw.nonBinlogCommitTxLocked(txId)
}

func (e *Engine) binlogLoadOrCreateMeta() ([]byte, error) {
	var meta []byte
	err := e.internalDo("load-binlog", func(conn Conn) error {
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
	err := e.internalDo("load", func(conn Conn) error {
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

func (e *Engine) Close(ctx context.Context) error {
	return e.close(ctx, e.binlog != nil)
}

func (e *Engine) close(ctx context.Context, waitCommitBinlog bool) error {
	var error error
	if waitCommitBinlog {
		err := e.binlog.Shutdown()
		if err != nil {
			multierr.AppendInto(&error, err)
		}
		select {
		case <-e.finishBinlogRunCh:
		case <-ctx.Done():
		}
	}
	if !e.readOnly {
		e.rw.cache.close(&error)
		err := e.rw.Close()
		if err != nil {
			multierr.AppendInto(&error, fmt.Errorf("failed to close RW connection: %w", err))
		}
	}
	e.roMx.Lock()
	defer e.roMx.Unlock()
	if len(e.roFree) != e.roCount {
		log.Println("[sqlite] don't use RO connections when close engine")
	}
	for _, conn := range e.roFree {
		conn.cache.close(&error)
		err := conn.Close()
		if err != nil {
			multierr.AppendInto(&error, fmt.Errorf("failed to close RO connection: %w", err))
		}
	}
	// multierr.AppendInto(&err, e.chk.Close())
	// multierr.AppendInto(&err, e.ro.Close()) // close RO one last to prevent checkpoint-on-close logic in other connections
	return error
}

func binlogLoadPosition(conn internalConn) (offset int64, isExists bool, err error) {
	rows := conn.Query("__select_binlog_pos", "SELECT offset from __binlog_offset")
	if rows.err != nil {
		return 0, false, rows.err
	}
	for rows.Next() {
		offset, _ := rows.ColumnInt64(0)
		return offset, true, nil
	}
	return 0, false, nil
}

func checkQueryName(qn string) error {
	if strings.HasPrefix(qn, internalQueryPrefix) {
		return fmt.Errorf("query prefix %q is reserved", internalQueryPrefix)
	}
	return nil
}
