package sqlitev2

import (
	"context"
	"time"
)
import "github.com/vkcom/statshouse/internal/sqlite/internal/sqlite0"

type Conn = userConn
type internalConn = userConn
type userConn struct {
	ctx  context.Context
	conn *sqliteRWConn
}

type Rows struct {
	ctx   context.Context
	c     *sqliteRWConn
	s     *sqlite0.Stmt
	err   error
	name  string
	start time.Time
}

func (r *Rows) ColumnBlob(i int, buf []byte) ([]byte, error) {
	return r.s.ColumnBlob(i, buf)
}

func (r *Rows) ColumnBlobRaw(i int) ([]byte, error) {
	return r.s.ColumnBlobUnsafe(i)
}

func (r *Rows) ColumnBlobRawString(i int) (string, error) {
	return r.s.ColumnBlobUnsafeString(i)
}

func (r *Rows) ColumnBlobString(i int) (string, error) {
	return r.s.ColumnBlobString(i)
}

func (r *Rows) ColumnIsNull(i int) bool {
	return r.s.ColumnNull(i)
}

func (r *Rows) ColumnInt64(i int) (int64, error) {
	return r.s.ColumnInt64(i)
}

func (r *Rows) ColumnFloat64(i int) (float64, error) {
	return r.s.ColumnFloat64(i)
}

func (c Conn) LastInsertRowID() int64 {
	return c.conn.LastInsertRowID()
}

func (c Conn) CacheSize() int {
	return c.cache.size()
}

func (r *Rows) Error() error {
	return r.err
}

func (r *Rows) Next() bool {
	if r.err != nil {
		return false
	}
	if r.ctx != nil {
		if err := r.ctx.Err(); err != nil {
			r.err = err
			return false
		}
	}
	row, err := r.s.Step()
	if err != nil {
		r.err = err
	}
	if !row {
		// todo retrun to cache
	}
	return row
}

func newUserConn(sqliteConn *sqliteRWConn, ctx context.Context) Conn {
	return Conn{
		ctx:  ctx,
		conn: sqliteConn,
	}
}

func (c Conn) Query(name, sql string, args ...Arg) Rows {
	return c.query(true, false, query, name, nil, sql, args...)
}

func (c Conn) QueryBytes(name string, sql []byte, args ...Arg) Rows {
	return c.query(true, false, query, name, sql, "", args...)
}

func (c Conn) Exec(name, sql string, args ...Arg) (int64, error) {
	return c.exec(false, name, nil, sql, args...)
}

func (c Conn) ExecBytes(name string, sql []byte, args ...Arg) (int64, error) {
	return c.exec(false, name, sql, "", args...)
}
