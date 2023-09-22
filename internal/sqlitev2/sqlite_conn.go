package sqlitev2

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/vkcom/statshouse/internal/sqlite/internal/sqlite0"
)

type sqliteRWConn struct {
	conn        *sqlite0.Conn
	mu          sync.Mutex
	dbOffset    int64
	binlogCache []byte
	//cache          *queryCache
	connError error
	currentTx int64
}

const (
	beginStmt    = "BEGIN IMMEDIATE" // make sure we don't get SQLITE_BUSY in the middle of transaction
	commitStmt   = "COMMIT"
	rollbackStmt = "ROLLBACK"
)

var innerCtx = context.Background()

func newSqliteRWConn(conn *sqlite0.Conn) *sqliteRWConn {
	return &sqliteRWConn{
		conn:        conn,
		mu:          sync.Mutex{},
		dbOffset:    0,
		binlogCache: make([]byte, 0),
		connError:   nil,
		currentTx:   0,
	}
}

func (c *sqliteRWConn) setError(err error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.setErrorLocked(err)
}

func (c *sqliteRWConn) setErrorLocked(err error) error {
	if c.connError != nil {
		return err
	}
	c.connError = err
	return err
}

func (c *sqliteRWConn) execLocked(sql string) error {
	return c.conn.Exec(sql)
}

func (c *sqliteRWConn) nonBinlogCommitTxLocked(txID int64) error {
	if c.currentTx != txID {
		return nil
	}
	err := c.execLocked(commitStmt)
	if err != nil {
		c.connError = err
	}
	return err
}

func (c *sqliteRWConn) beginTxLocked() (txID int64, _ error) {
	if c.connError != nil {
		return 0, c.connError
	}
	return c.currentTx, c.execLocked(beginStmt)
}

// если не смогли закомитить, движок находится в неконсистентном состоянии. Запрещаем запись
func (c *sqliteRWConn) binlogCommitTxLocked(txID, newOffset int64) error {
	if c.currentTx != txID {
		return nil
	}
	c.currentTx++
	err := c.saveBinlogOffsetLocked(newOffset)
	if err != nil {
		c.connError = err
		return fmt.Errorf("failed to save binlog offset: %w", err)
	}
	err = c.execLocked(commitStmt)
	if err != nil {
		c.connError = err
	} else {
		c.dbOffset = newOffset
	}
	return err
}

// если не смогли откатиться, движок находится в неконсистентном состоянии. Запрещаем запись
func (c *sqliteRWConn) rollbackLocked(txID int64) error {
	if c.currentTx != txID {
		return nil
	}
	c.currentTx++
	err := c.execLocked(rollbackStmt)
	if err != nil {
		c.connError = err
	}
	return err
}

// if sqlString == "", sqlBytes could be copied to store as map key
func (c *sqliteRWConn) initStmt(sqlBytes []byte, sqlString string, args ...Arg) (*sqlite0.Stmt, error) {
	if len(sqlBytes) == 0 {
		sqlBytes = append(sqlBytes, sqlString...)
	}
	//var sqlRO mem.RO
	//if sqlIsRawBytes {
	//	sqlRO = mem.B(sqlBytes)
	//} else {
	//	sqlRO = mem.S(sqlString)
	//}
	//si, ok := c.c.cache.get(sqlString, sqlBytes)
	var si *sqlite0.Stmt
	var err error
	_, ok := 0, false
	if !ok {
		si, err = prepare(c.conn, sqlBytes)
		if err != nil {
			return nil, err
		}
		//if sqlIsRawBytes {
		//	c.c.cache.put(string(sqlBytes), si)
		//} else {
		//	c.c.cache.put(sqlString, si)
		//}
	}

	stmt, err := bindParam(si, args...)
	return stmt, err
}

func (c *sqliteRWConn) execLockedArgs(ctx context.Context, name string, sql []byte, sqlStr string, args ...Arg) error {
	rows := c.queryLocked(ctx, name, sql, sqlStr, args...)
	for rows.Next() {
	}
	return rows.Error()

}

func (c *sqliteRWConn) queryLocked(ctx context.Context, name string, sql []byte, sqlStr string, args ...Arg) Rows {
	start := time.Now()
	s, err := c.initStmt(sql, sqlStr, args...)
	return Rows{ctx, c, s, err, name, start}
}

func prepare(c *sqlite0.Conn, sql []byte) (*sqlite0.Stmt, error) {
	s, _, err := c.Prepare(sql)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (c *sqliteRWConn) saveBinlogOffsetLocked(newOffset int64) error {
	err := c.execLockedArgs(innerCtx, "__update_binlog_pos", nil, "UPDATE __binlog_offset set offset = $offset;", Int64("$offset", newOffset))
	return err
}

func (c *sqliteRWConn) saveBinlogMetaLocked(meta []byte) error {
	err := c.execLockedArgs(innerCtx, "__update_meta", nil, "UPDATE __snapshot_meta SET meta = $meta;", Blob("$meta", meta))
	return err
}
