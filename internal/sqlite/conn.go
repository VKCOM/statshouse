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
	used           map[*sqlite0.Stmt]bool
	err            error
	spIn           bool
	spOk           bool
	spBeginStmt    string
	spCommitStmt   string
	spRollbackStmt string
	numParams      *queryBuilder
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
		used:           make(map[*sqlite0.Stmt]bool),
		spBeginStmt:    fmt.Sprintf("SAVEPOINT __sqlite_engine_auto_%x", spID[:]),
		spCommitStmt:   fmt.Sprintf("RELEASE __sqlite_engine_auto_%x", spID[:]),
		spRollbackStmt: fmt.Sprintf("ROLLBACK TO __sqlite_engine_auto_%x", spID[:]),
		numParams:      &queryBuilder{},
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
	for stmt, skipCache := range c.c.used {
		if skipCache {
			_ = stmt.Close()
		} else {
			_ = stmt.Reset()
		}
		delete(c.c.used, stmt)
	}
	c.c.mu.Unlock()
}

func (c Conn) execBeginSavepoint() error {
	c.c.spIn = true
	c.c.spOk = false
	_, err := c.ExecUnsafe("__begin_savepoint", c.c.spBeginStmt)
	return err
}

func (c Conn) execEndSavepoint() {
	if c.c.spIn {
		if c.c.spOk {
			_, _ = c.ExecUnsafe("__commit_savepoint", c.c.spCommitStmt)
		} else {
			_, _ = c.ExecUnsafe("__rollback_savepoint", c.c.spRollbackStmt)
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
	s, skipCache, err := c.doQuery(allowUnsafe, sql, args...)
	return Rows{c.c, s, err, false, c.ctx, name, start, c.stats, type_, skipCache}
}

func (c Conn) exec(allowUnsafe bool, name, sql string, args ...Arg) (int64, error) {
	rows := c.query(allowUnsafe, exec, name, sql, args...)
	for rows.Next() {
	}
	return c.LastInsertRowID(), rows.Error()
}

type Rows struct {
	c                   *sqliteConn
	s                   *sqlite0.Stmt
	err                 error
	used                bool
	ctx                 context.Context
	name                string
	start               time.Time
	stats               *StatsOptions
	type_               string
	mustCloseConnection bool
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
		r.c.used[r.s] = r.mustCloseConnection
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

func (c Conn) PrepMapLength() int {
	return len(c.c.prep)
}

func checkSliceParamName(s string) bool {
	return strings.HasPrefix(s, "$") && strings.HasSuffix(s, "$")
}

func (c Conn) doQuery(allowUnsafe bool, sql string, args ...Arg) (*sqlite0.Stmt, bool, error) {
	if c.c.err != nil {
		return nil, false, c.c.err
	}
	skipCache := false
	c.c.numParams.reset(sql)
	for _, arg := range args {
		if strings.HasPrefix(arg.name, "$internal") {
			return nil, false, fmt.Errorf("prefix $internal is reserved")
		}
		if arg.isSliceArg() {
			if !checkSliceParamName(arg.name) {
				return nil, false, fmt.Errorf("invalid list arg name %s", arg.name)
			}
			skipCache = true
			c.c.numParams.addSliceParam(arg)
		}
	}
	sqlBytes, err := c.c.numParams.buildQueryLocked()
	if err != nil {
		return nil, false, err
	}
	var si stmtInfo
	var ok bool
	if !skipCache {
		si, ok = c.c.prep[sql]
	}
	if !ok {
		start := time.Now()
		si, err = prepare(c.c.rw, sqlBytes)
		c.stats.measureActionDurationSince("sqlite_prepare", start)
		if err != nil {
			return nil, skipCache, err
		}
		if !skipCache {
			c.c.prep[sql] = si
		}
	}

	if !allowUnsafe && !si.isSafe {
		return nil, skipCache, errUnsafe
	}
	if c.autoSavepoint && (!si.isSelect && !si.isVacuumInto) && !c.c.spIn {
		err := c.execBeginSavepoint()
		if err != nil {
			return nil, skipCache, err
		}
	}

	_, used := c.c.used[si.stmt]
	if used {
		err := si.stmt.Reset()
		if err != nil {
			return nil, skipCache, err
		}
	}

	stmt, err := c.doStmt(si, args...)
	return stmt, skipCache, err
}

func (c Conn) doStmt(si stmtInfo, args ...Arg) (*sqlite0.Stmt, error) {
	start := 0
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
		case argInt64Slice:
			for _, n := range arg.ns {
				p := si.stmt.ParamBytes(c.c.numParams.nameLocked(start))
				err = si.stmt.BindInt64(p, n)
				if err != nil {
					return nil, err
				}
				start++
			}
		case argTextSlice:
			for _, n := range arg.ss {
				p := si.stmt.ParamBytes(c.c.numParams.nameLocked(start))
				err = si.stmt.BindBlobText(p, n)
				if err != nil {
					return nil, err
				}
				start++
			}
		default:
			err = fmt.Errorf("unknown arg type for %q: %v", arg.name, arg.typ)
		}
		if err != nil {
			return nil, err
		}
	}
	return si.stmt, nil
}

func prepare(c *sqlite0.Conn, sql []byte) (stmtInfo, error) {
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
