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

	checkpointer *checkpointer
}

func (b *binlogEngine) Shutdown() {
	//TODO implement me
	panic("implement me")
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
		checkpointer:  newCkeckpointer(e),
		applyFunction: applyFunction,
	}
}

func (b *binlogEngine) RunCheckpointer() {
	b.checkpointer.goCheckpoint()
}

func (b *binlogEngine) StopCheckpointer() {
	b.checkpointer.stop()
}

func (b *binlogEngine) Apply(payload []byte) (newOffset int64, errToReturn error) {
	b.e.rareLog("apply payload (len: %d)", len(payload))
	defer b.e.opt.StatsOptions.measureActionDurationSince("engine_apply", time.Now())
	err := b.e.internalDo("__apply_binlog", func(c internalConn) error {
		readLen, err := b.applyFunction(c.Conn, payload)
		newOffset = b.e.rw.dbOffset + int64(readLen)
		if opErr := b.e.rw.saveBinlogOffsetLocked(newOffset); opErr != nil {
			return b.e.rw.setErrorLocked(opErr)
		}
		if err != nil && isExpectedError(err) {
			errToReturn = err
			return nil
		}
		return err
	})
	if err != nil {
		return newOffset, b.e.rw.setError(err)
	}
	b.e.rw.dbOffset = newOffset
	return newOffset, errToReturn
}

func (b *binlogEngine) Skip(skipLen int64) (newOffset int64, err error) {
	defer b.e.opt.StatsOptions.measureActionDurationSince("engine_skip", time.Now())
	err = b.e.internalDo("__skip_binlog", func(c internalConn) error {
		newOffset = b.e.rw.dbOffset + skipLen
		return b.e.rw.saveBinlogOffsetLocked(newOffset)
	})
	if err == nil {
		b.e.rw.dbOffset = newOffset
	}
	b.e.rareLog("[sqlite] skip offset (new offset: %d)", newOffset)
	return newOffset, b.e.rw.setError(err)
}

// База может обгонять бинлог. Никак не учитываем toOffset
func (b *binlogEngine) Commit(toOffset int64, snapshotMeta []byte, safeSnapshotOffset int64) (err error) {
	//if b.e.testOptions != nil {
	//	b.e.testOptions.sleep()
	//}
	defer b.e.opt.StatsOptions.measureActionDurationSince("engine_commit", time.Now())
	b.e.rareLog("commit toOffset: %d, safeSnapshotOffset: %d", toOffset, safeSnapshotOffset)
	b.binlogNotifyWaited(toOffset, safeSnapshotOffset)
	err = b.e.rw.saveCommitInfo(snapshotMeta, toOffset) // унести в другое место чтобы не лочить в бинлог горутине?
	if err != nil {
		return err
	}
	b.checkpointer.notifyCommit(toOffset)
	return nil

}

func (b *binlogEngine) Revert(toOffset int64) (bool, error) {
	return false, nil
}

func (b *binlogEngine) ChangeRole(info binlog.ChangeRoleInfo) error {
	b.e.logger.Printf("change role: %+v", info)
	if info.IsReady {
		b.e.readyNotify.Do(func() {
			b.e.rw.mu.Lock()
			defer b.e.rw.mu.Unlock()
			b.e.logger.Printf("engine is ready, currentDbOffset: %d", b.e.rw.dbOffset)
			close(b.e.readyCh)
		})
	}
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
