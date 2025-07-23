package sqlite

import (
	"errors"
	"io"
	"time"

	binlog2 "github.com/VKCOM/statshouse/internal/vkgo/binlog"
)

type (
	binlogEngineReplicaImpl struct {
		e              *Engine
		lastCommitTime time.Time
		applyQueue     *applyQueue
		state          commitState
	}

	commitState int
)

const (
	none                 commitState = iota
	waitToCommit         commitState = iota
	maxReplicaQueueBytes             = 1 << 30 // 1GB
)

func isEOFErr(err error) bool {
	return errors.Is(err, binlog2.ErrorNotEnoughData) ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF)
}

func isExpectedError(err error) bool {
	return errors.Is(err, binlog2.ErrorUnknownMagic) || isEOFErr(err)
}

// Apply is used when re-reading or when working as a replica
func (impl *binlogEngineReplicaImpl) Apply(payload []byte) (newOffset int64, err error) {
	impl.e.rareLog("[sqlite] apply payload (len: %d)", len(payload))
	defer impl.e.opt.StatsOptions.measureActionDurationSince("engine_apply", time.Now())
	e := impl.e
	if e.opt.ReadAndExit || e.opt.CommitOnEachWrite {
		offs, err := impl.apply(payload)
		if err != nil {
			return offs, err
		}
		impl.e.rareLog("[sqlite] commit applied payload (new offset: %d)", offs)
		err = e.commitTXAndStartNew(true, false)
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

func (impl *binlogEngineReplicaImpl) apply(payload []byte) (newOffset int64, err error) {
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
			if !isExpectedError(err) {
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

func (impl *binlogEngineReplicaImpl) Commit(offset int64, snapshotMeta []byte, safeSnapshotOffset int64) (err error) {
	defer impl.e.opt.StatsOptions.measureActionDurationSince("engine_commit", time.Now())
	e := impl.e
	e.commitOffset.Store(offset)
	old := e.committedInfo.Load()
	if old != nil {
		ci := old.(*committedInfo)
		if ci.offset > offset {
			return
		}
	}
	snapshotMetaCpy := make([]byte, len(snapshotMeta))
	copy(snapshotMetaCpy, snapshotMeta)
	e.committedInfo.Store(&committedInfo{
		meta:   snapshotMetaCpy,
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
		start := time.Now()
		err := e.commitTXAndStartNew(true, false)
		impl.lastCommitTime = time.Now()
		impl.e.opt.StatsOptions.measureActionDurationSince("engine_commit_delayed_tx", start)
		if err != nil {
			return err
		}
		start = time.Now()
		defer impl.e.opt.StatsOptions.measureActionDurationSince("engine_apply_delayed", start)
		err = impl.applyQueue.applyAllChanges(impl.apply, impl.skip)
		if err != nil {
			return err
		}
		impl.state = none
	}
	return nil
}

func (impl *binlogEngineReplicaImpl) Revert(toOffset int64) (bool, error) {
	return false, nil
}

func (impl *binlogEngineReplicaImpl) StartReindex(operator binlog2.ReindexOperator) {
}

func (impl *binlogEngineReplicaImpl) ChangeRole(info binlog2.ChangeRoleInfo) error {
	e := impl.e
	if info.IsReady {
		e.readyNotify.Do(func() {
			close(e.waitUntilBinlogReady)
		})
	}
	return nil
}

func (impl *binlogEngineReplicaImpl) Skip(skipLen int64) (int64, error) {
	defer impl.e.opt.StatsOptions.measureActionDurationSince("engine_skip", time.Now())
	if impl.state == waitToCommit {
		return impl.applyQueue.addNewSkip(skipLen), nil
	}
	return impl.skip(skipLen)
}

func (impl *binlogEngineReplicaImpl) skip(skipLen int64) (int64, error) {
	var offset int64
	err := impl.e.do(func(conn Conn) error {
		impl.e.dbOffset += skipLen
		offset = impl.e.dbOffset
		return binlogUpdateOffset(conn, impl.e.dbOffset)
	})
	impl.e.rareLog("[sqlite] skip offset (new offset: %d)", offset)
	return offset, err
}

func (impl *binlogEngineReplicaImpl) Shutdown() {
	// nop
}

func (impl *binlogEngineReplicaImpl) Split(offset int64, toShardID string) bool {
	return false
}
