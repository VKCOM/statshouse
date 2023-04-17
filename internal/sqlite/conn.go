package sqlite

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.uber.org/multierr"
	"go4.org/mem"

	"github.com/vkcom/statshouse/internal/sqlite/internal/sqlite0"

	"pgregory.net/rand"
)

type sqliteConn struct {
	rw             *sqlite0.Conn
	mu             sync.Mutex
	cache          *queryCache
	used           map[*sqlite0.Stmt]struct{}
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

func newSqliteConn(rw *sqlite0.Conn, cacheMaxSize int) *sqliteConn {
	var spID [16]byte
	_, _ = rand.New().Read(spID[:])
	return &sqliteConn{
		rw:             rw,
		cache:          newQueryCache(cacheMaxSize),
		used:           make(map[*sqlite0.Stmt]struct{}),
		spBeginStmt:    fmt.Sprintf("SAVEPOINT __sqlite_engine_auto_%x", spID[:]),
		spCommitStmt:   fmt.Sprintf("RELEASE __sqlite_engine_auto_%x", spID[:]),
		spRollbackStmt: fmt.Sprintf("ROLLBACK TO __sqlite_engine_auto_%x", spID[:]),
		numParams:      &queryBuilder{},
	}
}

func (c *sqliteConn) startNewConn(autoSavepoint bool, ctx context.Context, stats *StatsOptions) Conn {
	c.mu.Lock()
	return Conn{c, autoSavepoint, ctx, stats}
}

func (c *sqliteConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	err := c.err
	c.err = errAlreadyClosed
	c.cache.close(&err)
	multierr.AppendInto(&err, c.rw.Close())
	return err
}

func (c Conn) close() {
	c.execEndSavepoint()
	for stmt := range c.c.used {
		_ = stmt.Reset()
		delete(c.c.used, stmt)
	}
	c.c.cache.closeTx()
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
	return c.query(false, query, name, nil, sql, args...)
}

func (c Conn) QueryBytes(name string, sql []byte, args ...Arg) Rows {
	return c.query(false, query, name, sql, "", args...)
}

func (c Conn) Exec(name, sql string, args ...Arg) (int64, error) {
	return c.exec(false, name, nil, sql, args...)
}

func (c Conn) ExecBytes(name string, sql []byte, args ...Arg) (int64, error) {
	return c.exec(false, name, sql, "", args...)
}

func (c Conn) ExecUnsafe(name, sql string, args ...Arg) (int64, error) {
	return c.exec(true, name, nil, sql, args...)
}

func (c Conn) query(allowUnsafe bool, type_, name string, sql []byte, sqlStr string, args ...Arg) Rows {
	start := time.Now()
	s, err := c.doQuery(allowUnsafe, sql, sqlStr, args...)
	return Rows{c.c, s, err, false, c.ctx, name, start, c.stats, type_}
}

func (c Conn) exec(allowUnsafe bool, name string, sql []byte, sqlStr string, args ...Arg) (int64, error) {
	rows := c.query(allowUnsafe, exec, name, sql, sqlStr, args...)
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

func (c Conn) CacheSize() int {
	return c.c.cache.size()
}

func checkSliceParamName(s string) bool {
	return strings.HasPrefix(s, "$") && strings.HasSuffix(s, "$")
}

// if sqlString == "", sqlBytes could be copied to store as map key
func (c Conn) doQuery(allowUnsafe bool, sqlBytes []byte, sqlString string, args ...Arg) (*sqlite0.Stmt, error) {
	if c.c.err != nil {
		return nil, c.c.err
	}
	sqlIsRawBytes := true
	if len(sqlBytes) == 0 {
		sqlIsRawBytes = false
	}
	var sqlRO mem.RO
	if sqlIsRawBytes {
		sqlRO = mem.B(sqlBytes)
	} else {
		sqlRO = mem.S(sqlString)
	}
	c.c.numParams.reset(sqlRO)
	for _, arg := range args {
		if strings.HasPrefix(arg.name, "$internal") {
			return nil, fmt.Errorf("prefix $internal is reserved")
		}
		if arg.isSliceArg() {
			if !checkSliceParamName(arg.name) {
				return nil, fmt.Errorf("invalid list arg name %s", arg.name)
			}
			c.c.numParams.addSliceParam(arg)
		}
	}
	sqlBytes, err := c.c.numParams.buildQueryLocked()
	if err != nil {
		return nil, err
	}
	si, ok := c.c.cache.get(sqlString, sqlBytes)
	if !ok {
		start := time.Now()
		si, err = prepare(c.c.rw, sqlBytes, true)
		c.stats.measureActionDurationSince("sqlite_prepare", start)
		if err != nil {
			return nil, err
		}
		if sqlIsRawBytes {
			c.c.cache.put(string(sqlBytes), si)
		} else {
			c.c.cache.put(sqlString, si)
		}
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

	stmt, err := c.doStmt(si, args...)
	return stmt, err
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
			err = si.stmt.BindBlobConstUnsafe(p, arg.b)
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

func prepare(c *sqlite0.Conn, sql []byte, skipCache bool) (stmtInfo, error) {
	s, _, err := c.Prepare(sql, !skipCache)
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
