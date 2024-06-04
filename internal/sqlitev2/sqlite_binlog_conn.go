package sqlitev2

import (
	"fmt"
	"log"
	"sync"

	"github.com/vkcom/statshouse/internal/sqlitev2/waitpool"
	"go.uber.org/multierr"
)

type (
	sqliteBinlogConn struct {
		mu               sync.Mutex
		conn             *sqliteConn
		isReplica        bool
		dbOffset         int64
		binlogCache      []byte
		committed        bool
		waitDbOffsetPool *waitpool.WaitPool
	}
)

func newSqliteBinlogConn(path string, appid uint32, cacheSize int, pageSize int, isReplica bool, stats StatsOptions, waitDbOffsetPool *waitpool.WaitPool, logger *log.Logger) (*sqliteBinlogConn, error) {
	conn, err := newSqliteRWWALConn(path, appid, cacheSize, pageSize, stats, logger)
	if err != nil {
		return nil, err
	}
	return &sqliteBinlogConn{
		mu:               sync.Mutex{},
		conn:             conn,
		dbOffset:         0,
		waitDbOffsetPool: waitDbOffsetPool,
		isReplica:        isReplica,
	}, nil
}

func (c *sqliteBinlogConn) setReplica(isReplica bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.isReplica = isReplica
}

func (c *sqliteBinlogConn) getDBOffsetLocked() int64 {
	return c.dbOffset
}

func (c *sqliteBinlogConn) setDBOffsetLocked(offset int64) {
	c.dbOffset = offset
}

func (c *sqliteBinlogConn) setError(err error) error {
	if err == nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.setErrorLocked(err)
}

func (c *sqliteBinlogConn) setErrorLocked(err error) error {
	if err == nil {
		return err
	}
	return c.conn.setErrorLocked(err)
}

func (c *sqliteBinlogConn) nonBinlogCommitTxLocked() error {
	return c.conn.commitTxLocked()
}

func (c *sqliteBinlogConn) beginTxLocked() error {
	err := c.conn.beginTxLocked()
	c.committed = err != nil // if err == nil, wait commit
	return err
}

// если не смогли закомитить, движок находится в неконсистентном состоянии. Запрещаем запись
func (c *sqliteBinlogConn) binlogCommitTxLocked(newOffset int64) error {
	if c.committed {
		return nil
	}
	c.committed = true
	err := c.saveBinlogOffsetLocked(newOffset)
	if err != nil {
		c.conn.connError = err
		return fmt.Errorf("failed to save binlog offset: %w", err)
	}

	err = c.conn.commitTxLocked()
	if err != nil {
		return fmt.Errorf("failed to commit TX: %w", err)
	}
	c.setDBOffsetLocked(newOffset)
	c.waitDbOffsetPool.Notify(newOffset)
	return nil
}

func (c *sqliteBinlogConn) rollbackLocked() error {
	if c.committed {
		return nil
	}
	c.committed = true
	return c.conn.rollbackLocked()
}

func (c *sqliteBinlogConn) saveCommitInfo(snapshotMeta []byte, offset int64) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	err = c.beginTxLocked()
	if err != nil {
		return err
	}
	defer func() {
		errRollback := c.rollbackLocked()
		if errRollback != nil {
			err = multierr.Append(err, errRollback)
		}
	}()
	err = c.saveBinlogMetaLocked(snapshotMeta)
	if err != nil {
		c.conn.connError = err
		return fmt.Errorf("failed to save binlog meta: %w", err)
	}
	err = c.saveBinlogCommittedOffsetLocked(offset)
	if err != nil {
		c.conn.connError = err
		return fmt.Errorf("failed to save binlog committed offset: %w", err)
	}

	return c.nonBinlogCommitTxLocked()
}

func (c *sqliteBinlogConn) saveBinlogOffsetLocked(newOffset int64) error {
	err := c.conn.execLockedArgs(innerCtx, "__update_binlog_pos", nil, "UPDATE __binlog_offset set offset = $offset;", Integer("$offset", newOffset))
	return err
}

func (c *sqliteBinlogConn) saveBinlogCommittedOffsetLocked(newOffset int64) error {
	err := c.conn.execLockedArgs(innerCtx, "__update_binlog_commited_pos", nil, "UPDATE __binlog_commit_offset set offset = $offset;", Integer("$offset", newOffset))
	return err
}

func (c *sqliteBinlogConn) saveBinlogMetaLocked(meta []byte) error {
	err := c.conn.execLockedArgs(innerCtx, "__update_meta", nil, "UPDATE __snapshot_meta SET meta = $meta;", Blob("$meta", meta))
	return err
}

func (c *sqliteBinlogConn) enableWALSwitchCallbackLocked() error {
	return c.conn.conn.EnableWALSwitchCallback()
}

func (c *sqliteBinlogConn) registerWALSwitchCallbackLocked(walSwitchCB func(int, uint)) {
	c.conn.conn.RegisterWALSwitchCallback(walSwitchCB)
}

func (c *sqliteBinlogConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn.Close()
}
