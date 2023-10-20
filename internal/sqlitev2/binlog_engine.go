package sqlitev2

import (
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/vkcom/statshouse/internal/vkgo/binlog"
)

type binlogEngine struct {
	e             *Engine
	applyFunction ApplyEventFunction
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

func (b binlogEngine) Apply(payload []byte) (newOffset int64, errToReturn error) {
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

func (b binlogEngine) Skip(skipLen int64) (newOffset int64, err error) {
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
func (b binlogEngine) Commit(toOffset int64, snapshotMeta []byte, safeSnapshotOffset int64) (err error) {
	if b.e.testOptions != nil {
		b.e.testOptions.sleep()
	}
	defer b.e.opt.StatsOptions.measureActionDurationSince("engine_commit", time.Now())
	b.e.rareLog("commit toOffset: %d, safeSnapshotOffset: %d", toOffset, safeSnapshotOffset)
	return b.e.internalDo("__commit_save_meta", func(conn internalConn) error {
		return b.e.rw.saveBinlogMetaLocked(snapshotMeta)
	})
}

func (b binlogEngine) Revert(toOffset int64) (bool, error) {
	return false, nil
}

func (b binlogEngine) ChangeRole(info binlog.ChangeRoleInfo) error {
	b.e.logger.Printf("change role: %+v", info)
	if info.IsReady {
		b.e.readyNotify.Do(func() {
			close(b.e.readyCh)
		})
	}
	return nil
}

func (b binlogEngine) StartReindex() error {
	return fmt.Errorf("implement")
}
