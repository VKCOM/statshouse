package sqlitev2

import (
	"context"
	"fmt"
	"log"
	"runtime/debug"
	"time"

	"github.com/VKCOM/statshouse/internal/sqlitev2/cache"
	"github.com/VKCOM/statshouse/internal/sqlitev2/sqlite0"
	"go.uber.org/multierr"
	"go4.org/mem"
)

type (
	sqliteConn struct {
		conn      *sqlite0.Conn
		cache     *cache.QueryCache
		builder   *queryBuilder
		connError error
		beginStmt string
		committed bool

		stats  StatsOptions
		logger *log.Logger
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
func newSqliteROConn(path string, stats StatsOptions, logger *log.Logger) (*sqliteConn, error) {
	conn, err := open(path, sqlite0.OpenReadonly)
	if err != nil {
		return nil, fmt.Errorf("failed to openRO conn: %w", err)
	}
	return &sqliteConn{
		conn:      conn,
		builder:   &queryBuilder{},
		cache:     cache.NewQueryCache(10, logger),
		beginStmt: beginDeferredStmt,
		stats:     stats,
		logger:    logger,
	}, nil
}

func newSqliteROWALConn(path string, cacheSize int, stats StatsOptions, logger *log.Logger) (*sqliteConn, error) {
	conn, err := openROWAL(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open RO connL %w", err)
	}
	return &sqliteConn{
		conn:      conn,
		builder:   &queryBuilder{},
		connError: nil,
		cache:     cache.NewQueryCache(cacheSize, logger),
		beginStmt: beginDeferredStmt,
		stats:     stats,
		logger:    logger,
	}, nil
}
func newSqliteRWWALConn(path string, appid uint32, cacheSize int, pageSize int, stats StatsOptions, logger *log.Logger) (*sqliteConn, error) {
	conn, err := openRW(openWAL, path, appid, pageSize)
	if err != nil {
		return nil, fmt.Errorf("failed to open RW conn: %w", err)
	}
	return &sqliteConn{
		conn:      conn,
		builder:   &queryBuilder{},
		connError: nil,
		cache:     cache.NewQueryCache(cacheSize, logger),
		beginStmt: beginImmediateStmt,
		stats:     stats,
		logger:    logger,
	}, nil
}

func (c *sqliteConn) integrityCheck() error {
	return c.execLocked("PRAGMA integrity_check;")
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

func (c *sqliteConn) setErrorLocked(err error) error {
	if err == nil {
		return err
	}
	if c.connError != nil {
		return err
	}
	c.logger.Println("engine is broken")
	c.logger.Println(string(debug.Stack()))
	c.connError = err
	return err
}

func (c *sqliteConn) execLocked(sql string) error {
	err := c.conn.Exec(sql)
	return err
}

func (c *sqliteConn) commitTxLocked() error {
	if c.committed {
		return nil
	}
	c.committed = true
	c.cache.FinishTX()
	err := c.execLocked(commitStmt) // Во время выполнения этой строки, может произойти смена вала
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

// если не смогли откатиться, движок находится в неконсистентном состоянии. Запрещаем запись
func (c *sqliteConn) rollbackLocked() error {
	if c.committed {
		return nil
	}
	c.committed = true
	c.cache.FinishTX()
	err := c.execLocked(rollbackStmt)
	if err != nil {
		c.connError = err
	}
	return err
}

// if sqlString == "", sqlBytes could be copied to store as map key
func (c *sqliteConn) initStmt(sqlBytes []byte, sqlString string, args ...Arg) (*sqlite0.Stmt, error) {
	sqlIsRawBytes := len(sqlBytes) > 0
	var q mem.RO
	if sqlIsRawBytes {
		q = mem.B(sqlBytes)
	} else {
		q = mem.S(sqlString)
	}

	query, err := c.builder.BuildQuery(q, args...)

	var si *sqlite0.Stmt
	var ok bool
	if err != nil {
		return nil, err
	}
	key := cache.CalcHashBytes(query)
	si, ok = c.cache.Get(key, time.Now())
	if !ok {
		si, err = prepare(c.conn, query)
		if err != nil {
			return nil, fmt.Errorf("failed to prepare stmt: %w", err)
		}
		c.cache.Put(key, time.Now(), si)
	}

	stmt, err := bindParam(si, c.builder, args...)
	return stmt, err
}

func (c *sqliteConn) execLockedArgs(ctx context.Context, name string, sql []byte, sqlStr string, args ...Arg) error {
	rows := c.queryLocked(ctx, exec, name, sql, sqlStr, args...)
	for rows.Next() {
	}
	return rows.Error()
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
func (c *sqliteConn) LastInsertRowID() int64 {
	return c.conn.LastInsertRowID()
}

func (c *sqliteConn) RowsAffected() int64 {
	return c.conn.RowsAffected()
}

func (c *sqliteConn) Close() error {
	err := c.connError
	c.connError = ErrAlreadyClosed
	c.cache.Close(&err)
	multierr.AppendInto(&err, c.conn.Close())
	return err
}
