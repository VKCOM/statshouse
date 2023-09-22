package sqlitev2

import (
	"context"
	"fmt"

	"github.com/vkcom/statshouse/internal/vkgo/binlog"
)

type (
	Engine struct {
		rw           *sqliteRWConn
		binlog       binlog.Binlog
		binlogEngine *binlogEngineReplicaImpl
		finishCh     chan struct{}
		readyCh      chan error
	}

	Options struct {
		Path                   string
		APPID                  int32
		Scheme                 string
		Replica                bool
		ReadAndExit            bool
		MaxROConn              int
		CacheMaxSizePerConnect int
	}
	ApplyEventFunction func(conn Conn, cache []byte) (int, error)
)

/*
	binlog - будет закрыт внутри Engine при вызове Close
*/
func NewEngine(opt Options) (*Engine, error) {
	conn, err := openRW(openWAL, opt.Path, opt.APPID, opt.Scheme)
	if err != nil {
		return nil, err
	}
	rw := newSqliteRWConn(conn)
	e := &Engine{
		rw:       rw,
		finishCh: make(chan struct{}),
		readyCh:  make(chan error, 1),
	}
	return e, nil
}

func (e *Engine) Run(applyEventFunction ApplyEventFunction) (err error) {
	if e.binlog != nil {
		offset, err := e.binlogLoadOrCreatePosition()
		if err != nil {
			err = fmt.Errorf("failed to load binlog position during to start run: %w", err)
			e.readyCh <- err
			return err
		}
		meta, err := e.binlogLoadOrCreateMeta()
		if err != nil {
			err = fmt.Errorf("failed to load binlog meta durint to start run: %w", err)
			e.readyCh <- err
			return err
		}
		e.binlogEngine = newBinlogEngine(e, applyEventFunction)
		err = e.rw.setError(e.binlog.Run(offset, meta, e.binlogEngine))
		if err != nil {
			err = fmt.Errorf("failed run binlog: %w", err)
			e.readyCh <- err
			return err
		}
	}

	<-e.finishCh
	return nil
}

func (e *Engine) WaitReady() error {
	if e.binlog == nil {
		return nil
	}
	return <-e.readyCh

}

func (e *Engine) Do(name string, ctx context.Context, do func(c Conn, cache []byte) ([]byte, error)) error {
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
		if rows.Next() {
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

func binlogLoadPosition(conn internalConn) (offset int64, isExists bool, err error) {
	rows := conn.Query("__select_binlog_pos", "SELECT offset from __binlog_offset")
	if rows.err != nil {
		return 0, false, rows.err
	}
	if rows.Next() {
		offset, _ := rows.ColumnInt64(0)
		return offset, true, nil
	}
	return 0, false, nil
}
