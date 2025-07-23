package sqlite

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.uber.org/multierr"
	"go4.org/mem"
	"pgregory.net/rand"

	"github.com/VKCOM/statshouse/internal/sqlite/sqlite0"
)

type sqliteConn struct {
	rw             *sqlite0.Conn
	mu             sync.Mutex
	cache          *queryCache
	used           map[*sqlite0.Stmt]struct{} // TODO move to query cache
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
	engine        *Engine // not nil for RW conn
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

func (c Conn) withoutTimeout() Conn {
	c.ctx = context.Background()
	return c
}

func (c *sqliteConn) startNewConn(autoSavepoint bool, ctx context.Context, stats *StatsOptions) Conn {
	c.mu.Lock()
	return Conn{c, autoSavepoint, ctx, stats, nil}
}

func (c *sqliteConn) startNewROConn(ctx context.Context, stats *StatsOptions) (Conn, error) {
	c.mu.Lock()
	if c.err != nil {
		c.mu.Unlock()
		return Conn{}, c.err
	}
	err := c.rw.Exec("BEGIN")
	if err != nil {
		c.mu.Unlock()
		return Conn{}, err
	}
	return Conn{c, false, ctx, stats, nil}, nil
}

func (c *sqliteConn) startNewRWConn(autoSavepoint bool, ctx context.Context, stats *StatsOptions, engine *Engine) (_ Conn, err error) {
	c.mu.Lock()
	if c.err != nil {
		err = c.err
		c.mu.Unlock()
	}
	return Conn{c, autoSavepoint, ctx, stats, engine}, err
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

func (c Conn) close(commit func(c Conn) error) error {
	defer c.c.mu.Unlock()
	c = c.withoutTimeout()
	canCommit := c.execEndSavepoint()
	for stmt := range c.c.used {
		_ = stmt.Reset()
		delete(c.c.used, stmt)
	}
	c.c.cache.closeTx()
	if commit != nil && canCommit && c.engine != nil {
		return commit(c)
	}
	return c.c.err
}

func (c Conn) closeRO() error {
	defer c.c.mu.Unlock()
	for stmt := range c.c.used {
		_ = stmt.Reset()
		delete(c.c.used, stmt)
	}
	c.c.cache.closeTx()
	err := c.c.rw.Exec(commitStmt)
	return multierr.Append(c.c.err, err)
}

func (c Conn) execBeginSavepoint() error {
	c.c.spIn = true
	c.c.spOk = false
	_, err := c.ExecUnsafe("__begin_savepoint", c.c.spBeginStmt)
	c.c.spIn = err == nil
	return err
}

func (c Conn) execEndSavepoint() (canCommit bool) {
	if c.c.spIn {
		ok := c.c.spOk && c.c.err == nil
		var err error
		if ok {
			_, err = c.ExecUnsafe("__commit_savepoint", c.c.spCommitStmt)
		} else {
			_, err = c.ExecUnsafe("__rollback_savepoint", c.c.spRollbackStmt)
		}
		if err != nil {
			c.c.err = err // if fail db is readonly
		}
		c.c.spIn = false
		return ok && c.c.err == nil
	}
	return false
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

func (c Conn) ExecUnsafe(name, sql string, args ...Arg) (int64, error) {
	return c.exec(true, name, nil, sql, args...)
}

func (c Conn) query(isRO, allowUnsafe bool, type_, name string, sql []byte, sqlStr string, args ...Arg) Rows {
	start := time.Now()
	s, err := c.doQuery(isRO, allowUnsafe, sql, sqlStr, args...)
	return Rows{c.c, s, err, false, c.ctx, name, start, c.stats, type_}
}

func (c Conn) exec(allowUnsafe bool, name string, sql []byte, sqlStr string, args ...Arg) (int64, error) {
	rows := c.query(false, allowUnsafe, exec, name, sql, sqlStr, args...)
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
	if !r.used {
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
	return c.c.rw.LastInsertRowID()
}

func (c Conn) CacheSize() int {
	return c.c.cache.size()
}

func checkSliceParamName(s string) bool {
	return strings.HasPrefix(s, "$") && strings.HasSuffix(s, "$")
}

// if sqlString == "", sqlBytes could be copied to store as map key
func (c Conn) doQuery(isRO, allowUnsafe bool, sqlBytes []byte, sqlString string, args ...Arg) (*sqlite0.Stmt, error) {
	if c.c.err != nil && !isRO {
		return nil, c.c.err
	}
	useSQLRawBytes := len(sqlBytes) > 0
	var sqlRO mem.RO
	if useSQLRawBytes {
		sqlRO = mem.B(sqlBytes)
	} else {
		sqlRO = mem.S(sqlString)
	}
	c.c.numParams.reset(sqlRO)
	hasSliceParam := false
	for _, arg := range args {
		if strings.HasPrefix(arg.name, "$internal") {
			return nil, fmt.Errorf("prefix $internal is reserved")
		}
		if arg.slice {
			hasSliceParam = true
			if !checkSliceParamName(arg.name) {
				return nil, fmt.Errorf("invalid list arg name %s", arg.name)
			}
			c.c.numParams.addSliceParam(arg)
		}
	}
	sqlBytes, err := c.c.numParams.buildQueryLocked()
	if hasSliceParam {
		useSQLRawBytes = true // can't use sqlString as a cache key
	}
	if err != nil {
		return nil, err
	}
	si, ok := c.c.cache.get(sqlBytes)
	if !ok {
		start := time.Now()
		si, err = prepare(c.c.rw, sqlBytes)
		c.stats.measureActionDurationSince("sqlite_prepare", start)
		if err != nil {
			return nil, err
		}
		if useSQLRawBytes {
			c.c.cache.put(string(sqlBytes), si)
		} else {
			c.c.cache.put(sqlString, si) // use user string to avoid alloc
		}
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
	// TODO add used immediately

	stmt, err := c.doStmt(si, args...)
	return stmt, err
}

func (c Conn) doStmt(si stmtInfo, args ...Arg) (*sqlite0.Stmt, error) {
	start := 0
	for _, arg := range args {
		if arg.slice {
			for i := 0; i < arg.length; i++ {
				p := si.stmt.ParamBytes(c.c.numParams.nameLocked(start))
				var err error
				switch arg.typ {
				case argBlob:
					err = si.stmt.BindBlob(p, arg.bs[i])
				case argBlobUnsafe:
					err = si.stmt.BindBlobUnsafe(p, arg.bs[i])
				case argBlobString:
					err = si.stmt.BindBlobString(p, arg.ss[i])
				case argText:
					err = si.stmt.BindText(p, arg.bs[i])
				case argTextUnsafe:
					err = si.stmt.BindTextUnsafe(p, arg.bs[i])
				case argTextString:
					err = si.stmt.BindTextString(p, arg.ss[i])
				case argInt64:
					err = si.stmt.BindInt64(p, arg.ns[i])
				case argFloat64:
					err = si.stmt.BindFloat64(p, arg.fs[i])
				default:
					err = fmt.Errorf("unsupported slice arg type for %q: %v", arg.name, arg.typ)
				}
				if err != nil {
					return nil, err
				}
				start++
			}
		} else {
			p := si.stmt.Param(arg.name)
			var err error
			switch arg.typ {
			case argNull:
				err = si.stmt.BindNull(p)
			case argZeroBlob:
				err = si.stmt.BindZeroBlob(p, int(arg.n))
			case argBlob:
				err = si.stmt.BindBlob(p, arg.b)
			case argBlobUnsafe:
				err = si.stmt.BindBlobUnsafe(p, arg.b)
			case argBlobString:
				err = si.stmt.BindBlobString(p, arg.s)
			case argText:
				err = si.stmt.BindText(p, arg.b)
			case argTextUnsafe:
				err = si.stmt.BindTextUnsafe(p, arg.b)
			case argTextString:
				err = si.stmt.BindTextString(p, arg.s)
			case argInt64:
				err = si.stmt.BindInt64(p, arg.n)
			case argFloat64:
				err = si.stmt.BindFloat64(p, arg.f)
			default:
				err = fmt.Errorf("unsupported arg type for %q: %v", arg.name, arg.typ)
			}
			if err != nil {
				return nil, err
			}
		}
	}
	return si.stmt, nil
}

func prepare(c *sqlite0.Conn, sql []byte) (stmtInfo, error) {
	s, _, err := c.Prepare(sql)
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
