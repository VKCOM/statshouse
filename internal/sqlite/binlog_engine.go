package sqlite

import (
	"context"
	"errors"
	"time"

	binlog2 "github.com/vkcom/statshouse/internal/vkgo/binlog"
)

type binlogEngineImpl struct {
	e              *Engine
	lastCommitTime time.Time
	applyQueue     *applyQueue
	state          commitState
}

type commitState int

const (
	none                 commitState = iota
	waitToCommit         commitState = iota
	maxReplicaQueueBytes             = 1 << 30 // 1GB
)

// Apply is used when re-reading or when working as a replica
func (impl *binlogEngineImpl) Apply(payload []byte) (newOffset int64, err error) {
	defer impl.e.opt.StatsOptions.measureActionDurationSince("engine_apply", time.Now())
	e := impl.e
	if e.opt.ReadAndExit {
		offs, err := impl.apply(payload)
		e.commitTXAndStartNew(true, false)
		return offs, err
	}
	if (e.isTest && e.mustWaitCommit) ||
		(!e.isTest && (time.Since(impl.lastCommitTime) > e.opt.CommitEvery || impl.state == waitToCommit)) {
		committedInfo := e.committedInfo.Load().(*committedInfo)
		if committedInfo != nil && e.dbOffset > committedInfo.offset {
			if impl.state == none {
				impl.applyQueue = newApplyQueue(impl.applyQueue, e.dbOffset, maxReplicaQueueBytes, &impl.e.opt.StatsOptions)
			}
			impl.state = waitToCommit
			n, err := e.scan(Conn{}, e.dbOffset, payload)
			newOffset, err = impl.applyQueue.addNewBody(payload[:n], err)
			return newOffset, err
		}
	}
	return impl.apply(payload)
}

func (impl *binlogEngineImpl) apply(payload []byte) (newOffset int64, err error) {
	e := impl.e
	var errToReturn error
	var n int
	offset := e.dbOffset
	errFromTx := e.do(func(conn Conn) error {
		// mustn't change any in memory state in this function or to be ready to rollback it
		dbOffset, _, err := binlogLoadPosition(conn)
		if err != nil {
			return err
		}
		newOffset = offset
		var shouldSkipLen = 0
		if dbOffset > offset {
			oldLen := len(payload)
			shouldSkipLen = int(dbOffset - offset)
			if shouldSkipLen > oldLen {
				shouldSkipLen = oldLen
			}
			payload = payload[shouldSkipLen:]
			if len(payload) == 0 {
				n = oldLen
				return nil
			}
		}
		n, err = e.apply(conn, offset+int64(shouldSkipLen), payload)
		n += shouldSkipLen
		newOffset = offset + int64(n)
		err1 := binlogUpdateOffset(conn, newOffset)
		if err != nil {
			errToReturn = err
			if !errors.Is(err, binlog2.ErrorUnknownMagic) && !errors.Is(err, binlog2.ErrorNotEnoughData) {
				return err
			}
		}
		return err1
	})
	if errFromTx == nil {
		e.dbOffset = newOffset
	}
	return e.dbOffset, errToReturn
}

func (impl *binlogEngineImpl) Commit(offset int64, snapshotMeta []byte, safeSnapshotOffset int64) (err error) {
	defer impl.e.opt.StatsOptions.measureActionDurationSince("engine_commit", time.Now())
	e := impl.e
	old := e.committedInfo.Load()
	if old != nil {
		ci := old.(*committedInfo)
		if ci.offset > offset {
			return
		}
	}
	e.committedInfo.Store(&committedInfo{
		meta:   snapshotMeta,
		offset: offset,
	})

	select {
	case e.commitCh <- struct{}{}:
	default:
	}
	e.binlogNotifyWaited(offset)
	waitCommit := impl.state == waitToCommit && offset >= e.dbOffset
	// TODO: replace with runtime mode change
	if waitCommit {
		ctx, cancel := context.WithTimeout(context.Background(), commitTXTimeout)
		c := e.start(ctx, false)
		err := e.commitTXAndStartNewLocked(c, true, false, false)
		impl.lastCommitTime = time.Now()
		c.close()
		cancel()
		if err != nil {
			return err
		}
		err = impl.applyQueue.applyAllChanges(impl.apply, impl.skip)
		if err != nil {
			return err
		}
		impl.state = none
	}
	return nil
}

func (impl *binlogEngineImpl) Revert(toOffset int64) (bool, error) {
	return false, nil
}

func (impl *binlogEngineImpl) ChangeRole(info binlog2.ChangeRoleInfo) error {
	e := impl.e
	if info.IsReady {
		e.readyNotify.Do(func() {
			close(e.waitUntilBinlogReady)
		})
	}
	return nil
}

func (impl *binlogEngineImpl) Skip(skipLen int64) (int64, error) {
	defer impl.e.opt.StatsOptions.measureActionDurationSince("engine_skip", time.Now())
	if impl.state == waitToCommit {
		return impl.applyQueue.addNewSkip(skipLen), nil
	}
	return impl.skip(skipLen)
}

func (impl *binlogEngineImpl) skip(skipLen int64) (int64, error) {
	var offset int64
	err := impl.e.do(func(conn Conn) error {
		impl.e.dbOffset += skipLen
		offset = impl.e.dbOffset
		return binlogUpdateOffset(conn, impl.e.dbOffset)
	})
	return offset, err
}
