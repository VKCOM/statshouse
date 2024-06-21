package sqlitev2

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/vkcom/statshouse/internal/vkgo/binlog"
)

type binlogEngine struct {
	e             *Engine
	applyFunction ApplyEventFunction

	waitQMx            sync.Mutex
	waitQ              []waitCommitInfo
	waitQBuffer        []waitCommitInfo
	committedOffset    int64
	safeSnapshotOffset int64
}

func (b *binlogEngine) Shutdown() {
	b.e.userEngine.Shutdown()
}

type waitCommitInfo struct {
	offset                 int64
	waitSafeSnapshotOffset bool // если true ждем safeSnapshotOffset, в противном случае смотрим на commit offset
	waitCh                 chan struct{}
}

func isEOFErr(err error) bool {
	return errors.Is(err, binlog.ErrorNotEnoughData) ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF)
}

func isExpectedError(err error) bool {
	return errors.Is(err, binlog.ErrorUnknownMagic) || isEOFErr(err)
}

func newBinlogEngine(e *Engine, applyFunction ApplyEventFunction) *binlogEngine {
	return &binlogEngine{
		e:             e,
		applyFunction: applyFunction,
	}
}

func (b *binlogEngine) Apply(payload []byte) (newOffset int64, errToReturn error) {
	b.e.rareLog("apply payload (len: %d)", len(payload))
	defer b.e.opt.StatsOptions.measureActionDurationSince("engine_apply", time.Now())
	err := b.e.internalDoBinlog("__apply_binlog", func(c internalConn) (int64, error) {
		readLen, err := b.applyFunction(c.Conn, payload)
		newOffset = b.e.rw.getDBOffsetLocked() + int64(readLen)
		if err != nil && isExpectedError(err) {
			errToReturn = err
			return newOffset, nil
		}
		return newOffset, err
	})
	if err != nil {
		return newOffset, b.e.rw.setError(err)
	}
	return newOffset, errToReturn
}

func (b *binlogEngine) Skip(skipLen int64) (newOffset int64, err error) {
	defer b.e.opt.StatsOptions.measureActionDurationSince("engine_skip", time.Now())
	err = b.e.internalDoBinlog("__skip_binlog", func(c internalConn) (int64, error) {
		newOffset = b.e.rw.getDBOffsetLocked() + skipLen
		return newOffset, nil
	})
	b.e.rareLog("[sqlite] skip offset (new offset: %d)", newOffset)
	return newOffset, b.e.rw.setError(err)
}

// База может обгонять бинлог. Никак не учитываем toOffset
func (b *binlogEngine) Commit(toOffset int64, snapshotMeta []byte, safeSnapshotOffset int64) (err error) {
	defer b.e.opt.StatsOptions.measureActionDurationSince("engine_commit", time.Now())
	b.e.rareLog("commit toOffset: %d, safeSnapshotOffset: %d", toOffset, safeSnapshotOffset)
	b.binlogNotifyWaited(toOffset, safeSnapshotOffset)
	err = b.e.rw.saveCommitInfo(snapshotMeta, toOffset) // унести в другое место чтобы не лочить в бинлог горутине?
	if err != nil {
		return err
	}
	b.e.checkpointer.notifyCommit(toOffset)
	return nil

}

func (b *binlogEngine) Revert(toOffset int64) (bool, error) {
	b.e.userEngine.Revert(toOffset)
	return false, nil
}

func (b *binlogEngine) ChangeRole(info binlog.ChangeRoleInfo) error {
	b.e.logger.Printf("change role: %+v", info)
	if info.IsReady {
		b.e.readyNotify.Do(func() {
			b.e.rw.mu.Lock()
			defer b.e.rw.mu.Unlock()
			b.e.logger.Printf("engine is ready, currentDbOffset: %d", b.e.rw.getDBOffsetLocked())
			close(b.e.readyCh)
		})
	}
	// TODO add notify user
	b.e.rw.setReplica(!info.IsMaster)
	b.e.userEngine.ChangeRole(info)
	return nil
}

func (b *binlogEngine) StartReindex() error {
	return fmt.Errorf("implement")
}

func (b *binlogEngine) binlogWait(offset int64, waitSafeSnapshotOffset bool) {
	b.waitQMx.Lock()
	if (!waitSafeSnapshotOffset && offset <= b.committedOffset) || waitSafeSnapshotOffset && offset <= b.safeSnapshotOffset {
		b.waitQMx.Unlock()
		return
	}
	ch := make(chan struct{}, 1)
	b.waitQ = append(b.waitQ, waitCommitInfo{
		offset:                 offset,
		waitCh:                 ch,
		waitSafeSnapshotOffset: waitSafeSnapshotOffset,
	})
	b.waitQMx.Unlock()
	<-ch
}

func (b *binlogEngine) binlogNotifyWaited(committedOffset int64, safeSnapshotOffset int64) {
	b.waitQMx.Lock()
	defer b.waitQMx.Unlock()
	b.committedOffset = committedOffset
	b.safeSnapshotOffset = safeSnapshotOffset
	b.waitQBuffer = b.waitQBuffer[:0]
	for _, wi := range b.waitQ {
		if (!wi.waitSafeSnapshotOffset && wi.offset > committedOffset) ||
			(wi.waitSafeSnapshotOffset && wi.offset > safeSnapshotOffset) {
			b.waitQBuffer = append(b.waitQBuffer, wi)
			continue
		}
		wi.waitCh <- struct{}{}
		close(wi.waitCh)
	}
	t := b.waitQ
	b.waitQ = b.waitQBuffer
	b.waitQBuffer = t
}
