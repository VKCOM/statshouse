package sqlitev2

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/vkcom/statshouse/internal/sqlite/sqlite0"
	"go.uber.org/multierr"
)

type (
	sqliteConn struct {
		conn             *sqlite0.Conn
		mu               sync.Mutex
		dbOffset         int64
		committedOffset  int64
		binlogCache      []byte
		cache            *queryCachev2
		queryBuffer      []byte
		connError        error
		showLastInsertID bool
		beginStmt        string
		committed        bool
		stats            StatsOptions
	}
)

var (
	beginImmediateStmt = sqlite0.EnsureZeroTermStr("BEGIN IMMEDIATE") // make sure we don't get SQLITE_BUSY in the middle of transaction
	beginDeferredStmt  = sqlite0.EnsureZeroTermStr("BEGIN")
	commitStmt         = sqlite0.EnsureZeroTermStr("COMMIT")
	rollbackStmt       = sqlite0.EnsureZeroTermStr("ROLLBACK")
)

var innerCtx = context.Background()

// use only for snapshots
func newSqliteROConn(path string, logger *log.Logger) (*sqliteConn, error) {
	conn, err := open(path, sqlite0.OpenReadonly)
	if err != nil {
		return nil, fmt.Errorf("failed to openRO conn: %w", err)
	}
	return &sqliteConn{
		conn:      conn,
		mu:        sync.Mutex{},
		cache:     newQueryCachev2(10, logger),
		beginStmt: beginDeferredStmt,
	}, nil
}

func newSqliteROWALConn(path string, cacheSize int, logger *log.Logger) (*sqliteConn, error) {
	conn, err := openROWAL(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open RO connL %w", err)
	}
	return &sqliteConn{
		conn:      conn,
		mu:        sync.Mutex{},
		connError: nil,
		cache:     newQueryCachev2(cacheSize, logger),
		beginStmt: beginDeferredStmt,
	}, nil
}
func newSqliteRWWALConn(path string, appid int32, showLastInsertID bool, cacheSize int, logger *log.Logger) (*sqliteConn, error) {
	conn, err := openRW(openWAL, path, appid)
	if err != nil {
		return nil, fmt.Errorf("failed to open RW conn: %w", err)
	}
	return &sqliteConn{
		conn:             conn,
		mu:               sync.Mutex{},
		dbOffset:         0,
		binlogCache:      make([]byte, 0),
		connError:        nil,
		showLastInsertID: showLastInsertID,
		cache:            newQueryCachev2(cacheSize, logger),
		beginStmt:        beginImmediateStmt,
	}, nil
}

func (c *sqliteConn) applyScheme(schemas ...string) error {
	for _, schema := range schemas {
		err := c.execLocked(schema)
		if err != nil {
			return fmt.Errorf("failed to setup DB schema: %w", err)
		}
	}
	return nil
}

func (c *sqliteConn) setError(err error) error {
	if err == nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.setErrorLocked(err)
}

func (c *sqliteConn) setErrorLocked(err error) error {
	if c.connError != nil {
		return err
	}
	c.connError = err
	return err
}

func (c *sqliteConn) execLocked(sql string) error {
	err := c.conn.Exec(sql)
	return err
}

func (c *sqliteConn) nonBinlogCommitTxLocked() error {
	if c.committed {
		return nil
	}
	c.committed = true
	c.cache.closeTx()
	err := c.execLocked(commitStmt)
	if err != nil {
		c.connError = err
	}
	return err
}

func (c *sqliteConn) beginTxLocked() error {
	if c.connError != nil {
		return c.connError
	}
	c.committed = false
	return c.execLocked(c.beginStmt)
}

// если не смогли закомитить, движок находится в неконсистентном состоянии. Запрещаем запись
func (c *sqliteConn) binlogCommitTxLocked(newOffset int64) (int64, error) {
	if c.committed {
		return c.dbOffset, nil
	}
	c.committed = true
	err := c.saveBinlogOffsetLocked(newOffset)
	if err != nil {
		c.connError = err
		return c.dbOffset, fmt.Errorf("failed to save binlog offset: %w", err)
	}
	c.cache.closeTx()
	err = c.execLocked(commitStmt)
	if err != nil {
		c.connError = err
		return c.dbOffset, fmt.Errorf("failed to commit TX: %w", err)
	} else {
		c.dbOffset = newOffset
	}
	return c.dbOffset, nil
}

// если не смогли откатиться, движок находится в неконсистентном состоянии. Запрещаем запись
func (c *sqliteConn) rollbackLocked() error {
	if c.committed {
		return nil
	}
	c.committed = true
	c.cache.closeTx()
	err := c.execLocked(rollbackStmt)
	if err != nil {
		c.connError = err
	}
	return err
}

// if sqlString == "", sqlBytes could be copied to store as map key
func (c *sqliteConn) initStmt(sqlBytes []byte, sqlString string, args ...Arg) (*sqlite0.Stmt, error) {
	sqlIsRawBytes := len(sqlBytes) > 0
	if sqlIsRawBytes {
		c.queryBuffer = append(c.queryBuffer[:0], sqlBytes...)
	} else {
		c.queryBuffer = append(c.queryBuffer[:0], sqlString...)
	}
	var si *sqlite0.Stmt
	var err error
	var ok bool
	key := calcHashBytes(c.queryBuffer)
	si, ok = c.cache.get(key, time.Now())
	if !ok {
		si, err = prepare(c.conn, c.queryBuffer)
		if err != nil {
			return nil, fmt.Errorf("failed to prepare stmt: %w", err)
		}
		c.cache.put(key, time.Now(), si)
	} else {
		err := si.Reset()
		if err != nil {
			return nil, fmt.Errorf("failed to reset stmt")
		}
	}

	stmt, err := bindParam(si, args...)
	return stmt, err
}

func (c *sqliteConn) execLockedArgs(ctx context.Context, name string, sql []byte, sqlStr string, args ...Arg) (int64, error) {
	rows := c.queryLocked(ctx, exec, name, sql, sqlStr, args...)
	for rows.Next() {
	}
	var id int64
	if c.showLastInsertID {
		id = c.LastInsertRowID()
	}
	return id, rows.Error()
}

func (c *sqliteConn) queryLocked(ctx context.Context, type_, name string, sql []byte, sqlStr string, args ...Arg) Rows {
	start := time.Now()
	s, err := c.initStmt(sql, sqlStr, args...)
	return Rows{ctx, c, s, err, name, start, type_}
}

func prepare(c *sqlite0.Conn, sql []byte) (*sqlite0.Stmt, error) {
	s, _, err := c.Prepare(sql)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (c *sqliteConn) saveBinlogOffsetLocked(newOffset int64) error {
	_, err := c.execLockedArgs(innerCtx, "__update_binlog_pos", nil, "UPDATE __binlog_offset set offset = $offset;", Int64("$offset", newOffset))
	return err
}

func (c *sqliteConn) saveBinlogMetaLocked(meta []byte) error {
	_, err := c.execLockedArgs(innerCtx, "__update_meta", nil, "UPDATE __snapshot_meta SET meta = $meta;", Blob("$meta", meta))
	return err
}

func (c *sqliteConn) LastInsertRowID() int64 {
	return c.conn.LastInsertRowID()
}

func (c *sqliteConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	err := c.connError
	c.connError = errAlreadyClosed
	c.cache.close(&err)
	multierr.AppendInto(&err, c.conn.Close())
	return err
}

func openROWAL(path string) (*sqlite0.Conn, error) {
	return openWAL(path, sqlite0.OpenReadonly)
}
