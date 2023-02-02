package sqlite

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.uber.org/multierr"

	"github.com/vkcom/statshouse/internal/sqlite/internal/sqlite0"

	"pgregory.net/rand"
)

type sqliteConn struct {
	rw             *sqlite0.Conn
	mu             sync.Mutex
	prep           map[string]stmtInfo
	used           map[*sqlite0.Stmt]struct{}
	err            error
	spIn           bool
	spOk           bool
	spBeginStmt    string
	spCommitStmt   string
	spRollbackStmt string
}

type Conn struct {
	c             *sqliteConn
	autoSavepoint bool
	ctx           context.Context
	stats         *StatsOptions
}

func newSqliteConn(rw *sqlite0.Conn) *sqliteConn {
	var spID [16]byte
	_, _ = rand.New().Read(spID[:])
	return &sqliteConn{
		rw:             rw,
		prep:           make(map[string]stmtInfo),
		used:           make(map[*sqlite0.Stmt]struct{}),
		spBeginStmt:    fmt.Sprintf("SAVEPOINT __sqlite_engine_auto_%x", spID[:]),
		spCommitStmt:   fmt.Sprintf("RELEASE __sqlite_engine_auto_%x", spID[:]),
		spRollbackStmt: fmt.Sprintf("ROLLBACK TO __sqlite_engine_auto_%x", spID[:]),
	}
}

func (c *sqliteConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	err := c.err
	c.err = errAlreadyClosed
	for _, si := range c.prep {
		multierr.AppendInto(&err, si.stmt.Close())
	}
	multierr.AppendInto(&err, c.rw.Close())
	return err
}

func (c Conn) close() {
	c.execEndSavepoint()
	for stmt := range c.c.used {
		_ = stmt.Reset()
		delete(c.c.used, stmt)
	}
	c.c.mu.Unlock()
}

func (c Conn) execBeginSavepoint() error {
	c.c.spIn = true
	c.c.spOk = false
	_, err := c.exec(true, "__begin_savepoint", c.c.spBeginStmt)
	return err
}

func (c Conn) execEndSavepoint() {
	if c.c.spIn {
		if c.c.spOk {
			_, _ = c.exec(true, "__commit_savepoint", c.c.spCommitStmt)
		} else {
			_, _ = c.exec(true, "__rollback_savepoint", c.c.spRollbackStmt)
		}
		c.c.spIn = false
	}
}

func (c Conn) Query(name, sql string, args ...Arg) Rows {
	return c.query(false, query, name, sql, args...)
}

func (c Conn) Exec(name, sql string, args ...Arg) (int64, error) {
	return c.exec(false, name, sql, args...)
}

func (c Conn) ExecUnsafe(name, sql string, args ...Arg) (int64, error) {
	return c.exec(true, name, sql, args...)
}

func (c Conn) query(allowUnsafe bool, type_, name, sql string, args ...Arg) Rows {
	start := time.Now()
	s, err := c.doQuery(allowUnsafe, sql, args...)
	return Rows{c.c, s, err, false, c.ctx, name, start, c.stats, type_}
}

func (c Conn) exec(allowUnsafe bool, name, sql string, args ...Arg) (int64, error) {
	rows := c.query(allowUnsafe, exec, name, sql, args...)
	for rows.Next() {
	}
	return c.LastInsertRowID(), rows.Error()
}

type Rows struct {
	c     *sqliteConn
	s     *sqlite0.Stmt
	err   error
	used  bool
	ctx   context.Context
	name  string
	start time.Time
	stats *StatsOptions
	type_ string
}

func (r *Rows) Error() error {
	return r.err
}

func (r *Rows) next(setUsed bool) bool {
	if r.err != nil {
		return false
	}
	if r.ctx != nil {
		if err := r.ctx.Err(); err != nil {
			r.err = err
			return false
		}
	}

	if setUsed && !r.used {
		r.c.used[r.s] = struct{}{}
		r.used = true
	}
	row, err := r.s.Step()
	if err != nil {
		r.err = err
	}
	if !row {
		r.stats.measureSqliteQueryDurationSince(r.type_, r.name, r.start)
	}
	return row
}

func (r *Rows) Next() bool {
	return r.next(true)
}

func (r *Rows) ColumnBlob(i int, buf []byte) ([]byte, error) {
	return r.s.ColumnBlob(i, buf)
}

func (r *Rows) ColumnBlobRaw(i int) ([]byte, error) {
	return r.s.ColumnBlobRaw(i)
}

func (r *Rows) ColumnBlobRawString(i int) (string, error) {
	return r.s.ColumnBlobRawString(i)
}

func (r *Rows) ColumnBlobString(i int) (string, error) {
	return r.s.ColumnBlobString(i)
}

func (r *Rows) ColumnIsNull(i int) bool {
	return r.s.ColumnIsNull(i)
}

func (r *Rows) ColumnInt64(i int) (int64, error) {
	return r.s.ColumnInt64(i)
}

func (c Conn) LastInsertRowID() int64 {
	return c.c.rw.LastInsertRowID()
}

func (c Conn) doQuery(allowUnsafe bool, sql string, args ...Arg) (*sqlite0.Stmt, error) {
	if c.c.err != nil {
		return nil, c.c.err
	}

	si, ok := c.c.prep[sql]
	var err error
	if !ok {
		si, err = prepare(c.c.rw, sql)
		if err != nil {
			return nil, err
		}
		c.c.prep[sql] = si
	}

	if !allowUnsafe && !si.isSafe {
		return nil, errUnsafe
	}
	if c.autoSavepoint && (!si.isSelect && !si.isVacuumInto) && !c.c.spIn {
		err := c.execBeginSavepoint()
		if err != nil {
			return nil, err
		}
	}

	_, used := c.c.used[si.stmt]
	if used {
		err := si.stmt.Reset()
		if err != nil {
			return nil, err
		}
	}

	return doStmt(si, args...)
}

func doStmt(si stmtInfo, args ...Arg) (*sqlite0.Stmt, error) {
	for _, arg := range args {
		var err error
		p := si.stmt.Param(arg.name)
		switch arg.typ {
		case argByte:
			err = si.stmt.BindBlob(p, arg.b)
		case argByteConst:
			err = si.stmt.BindBlobConst(p, arg.b)
		case argString:
			err = si.stmt.BindBlobString(p, arg.s)
		case argInt64:
			err = si.stmt.BindInt64(p, arg.n)
		case argText:
			err = si.stmt.BindBlobText(p, arg.s)
		default:
			err = fmt.Errorf("unknown arg type for %q: %v", arg.name, arg.typ)
		}
		if err != nil {
			return nil, err
		}
	}
	return si.stmt, nil
}

func prepare(c *sqlite0.Conn, sql string) (stmtInfo, error) {
	s, _, err := c.Prepare(sql, true)
	if err != nil {
		return stmtInfo{}, err
	}
	norm := s.NormalizedSQL()
	return stmtInfo{
		stmt:         s,
		isSafe:       isSafe(norm),
		isSelect:     isSelect(norm),
		isVacuumInto: isVacuumInto(norm),
	}, nil
}

func isSelect(normSQL string) bool {
	return strings.HasPrefix(normSQL, normSelect+" ")
}

func isVacuumInto(normSQL string) bool {
	return strings.HasPrefix(normSQL, normVacuum+" ")
}

func isSafe(normSQL string) bool {
	for _, s := range safeStatements {
		if strings.HasPrefix(normSQL, s+" ") {
			return true
		}
	}
	return false
}
