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
	lastSnapshotMeta   []byte
	committedOffset    int64
	safeSnapshotOffset int64

	checkpointMx sync.Mutex

	waitCheckpointOffset int64
	waitCheckpoint       bool
}

type waitCommitInfo struct {
	offset                 int64
	waitSafeSnapshotOffset bool
	waitCh                 chan []byte
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
	if b.e.testOptions != nil {
		b.e.testOptions.sleep()
	}
	defer b.e.opt.StatsOptions.measureActionDurationSince("engine_commit", time.Now())
	b.e.rareLog("commit toOffset: %d, safeSnapshotOffset: %d", toOffset, safeSnapshotOffset)
	b.binlogNotifyWaited(toOffset, snapshotMeta, safeSnapshotOffset)
	err = b.e.rw.saveCommitInfo(snapshotMeta, toOffset)
	if err != nil {
		return err
	}
	b.doCkeckpointIfCan(toOffset)
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

func (b *binlogEngine) binlogWait(offset int64, waitSafeSnapshotOffset bool) []byte {
	b.waitQMx.Lock()
	if (!waitSafeSnapshotOffset && offset <= b.committedOffset) || waitSafeSnapshotOffset && offset <= b.safeSnapshotOffset {
		meta := b.lastSnapshotMeta
		b.waitQMx.Unlock()
		return meta
	}
	ch := make(chan []byte, 1)
	b.waitQ = append(b.waitQ, waitCommitInfo{
		offset:                 offset,
		waitCh:                 ch,
		waitSafeSnapshotOffset: waitSafeSnapshotOffset,
	})
	b.waitQMx.Unlock()
	meta := <-ch
	return meta
}

func (b *binlogEngine) binlogNotifyWaited(committedOffset int64, snapshotMeta []byte, safeSnapshotOffset int64) {
	b.waitQMx.Lock()
	defer b.waitQMx.Unlock()
	b.lastSnapshotMeta = make([]byte, len(snapshotMeta))
	copy(b.lastSnapshotMeta, snapshotMeta)
	b.committedOffset = committedOffset
	b.safeSnapshotOffset = safeSnapshotOffset
	b.waitQBuffer = b.waitQBuffer[:0]
	for _, wi := range b.waitQ {
		if (!wi.waitSafeSnapshotOffset && wi.offset > committedOffset) ||
			(wi.waitSafeSnapshotOffset && wi.offset > safeSnapshotOffset) {
			b.waitQBuffer = append(b.waitQBuffer, wi)
			continue
		}
		wi.waitCh <- b.lastSnapshotMeta
		close(wi.waitCh)
	}
	t := b.waitQ
	b.waitQ = b.waitQBuffer
	b.waitQBuffer = t
}

func (b *binlogEngine) setWaitCheckpointOffset() {
	b.checkpointMx.Lock()
	defer b.checkpointMx.Unlock()
	b.waitCheckpointOffset = b.e.rw.dbOffset // TODO можно ли обращаться без синхронизации?
	b.waitCheckpoint = true
}

func (b *binlogEngine) doCkeckpointIfCan(commitOffset int64) {
	b.e.rw.mu.Lock()
	defer b.e.rw.mu.Unlock()
	b.checkpointMx.Lock()
	defer b.checkpointMx.Unlock() // TODO не обязательно брать лок на все время чекпоинта
	if b.waitCheckpoint && b.waitCheckpointOffset <= commitOffset {
		err := b.e.rw.conn.Checkpoint()
		if err != nil {
			fmt.Println(fmt.Errorf("CHECKPOINT ERROR: %w", err).Error())
			return
		}
		fmt.Println("CHECKPOINT OK: %w")
		// TODO если ошибка то пытается еще раз через время
		b.waitCheckpoint = false
	}
}
