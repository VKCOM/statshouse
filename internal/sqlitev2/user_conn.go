package sqlitev2

import "C"
import (
	"context"
	"time"

	"github.com/vkcom/statshouse/internal/sqlitev2/sqlite0"
)

type internalConn struct {
	Conn
}

type Conn struct {
	ctx  context.Context
	conn *sqliteConn
}

type Rows struct {
	ctx   context.Context
	conn  *sqliteConn
	s     *sqlite0.Stmt
	err   error
	name  string
	start time.Time
	type_ string
}

func (r *Rows) ColumnIsNull(i int) bool {
	return r.s.ColumnNull(i)
}

func (r *Rows) ColumnBlob(i int, buf []byte) ([]byte, error) {
	return r.s.ColumnBlob(i, buf)
}

func (r *Rows) ColumnBlobUnsafe(i int) ([]byte, error) {
	if AvoidUnsafe {
		return r.s.ColumnBlob(i, nil)
	}
	return r.s.ColumnBlobUnsafe(i)
}

func (r *Rows) ColumnBlobString(i int) (string, error) {
	return r.s.ColumnBlobString(i)
}

func (r *Rows) ColumnTextString(i int) (string, error) {
	return r.s.ColumnTextString(i)
}

func (r *Rows) ColumnInteger(i int) int64 {
	return r.s.ColumnInt64(i)
}

func (r *Rows) ColumnReal(i int) float64 {
	return r.s.ColumnFloat64(i)
}

func (c Conn) LastInsertRowID() int64 {
	return c.conn.LastInsertRowID()
}

func (c Conn) RowsAffected() int64 {
	return c.conn.RowsAffected()
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
			r.conn.stats.measureSqliteQueryDurationSince(r.type_, r.name, "timeout", r.start)
			return false
		}
	}
	row, err := r.s.Step()
	if err != nil {
		r.err = err
	}
	if !row {
		status := "ok"
		if err != nil {
			status = "error"
		}
		r.conn.stats.measureSqliteQueryDurationSince(r.type_, r.name, status, r.start)
	}
	return row
}

func newUserConn(sqliteConn *sqliteConn, ctx context.Context) Conn {
	return Conn{
		ctx:  ctx,
		conn: sqliteConn,
	}
}

func internalFromUser(c Conn) internalConn {
	return newInternalConn(c.conn)
}

func newInternalConn(sqliteConn *sqliteConn) internalConn {
	return internalConn{
		Conn{
			ctx:  context.Background(),
			conn: sqliteConn,
		},
	}
}

func (c Conn) Query(name, sql string, args ...Arg) Rows {
	return c.conn.queryLocked(c.ctx, query, name, nil, sql, args...)
}

func (c Conn) QueryBytes(name string, sql []byte, args ...Arg) Rows {
	return c.conn.queryLocked(c.ctx, query, name, sql, "", args...)
}

func (c Conn) Exec(name, sql string, args ...Arg) error {
	return c.conn.execLockedArgs(c.ctx, name, nil, sql, args...)
}

func (c Conn) ExecBytes(name string, sql []byte, args ...Arg) error {
	return c.conn.execLockedArgs(c.ctx, name, sql, "", args...)
}
