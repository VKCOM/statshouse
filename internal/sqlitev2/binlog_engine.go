package sqlitev2

import (
	"errors"
	"fmt"
	"io"

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
	err := b.e.internalDo("apply", func(c Conn) error {
		readLen, err := b.applyFunction(c, payload)
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
	err = b.e.internalDo("skip", func(c Conn) error {
		newOffset = b.e.rw.dbOffset + skipLen
		return b.e.rw.saveBinlogOffsetLocked(newOffset)
	})
	if err == nil {
		b.e.rw.dbOffset = newOffset
	}
	return newOffset, b.e.rw.setError(err)
}

// База может обгонять бинлог. Никак не учитываем toOffset
func (b binlogEngine) Commit(toOffset int64, snapshotMeta []byte, safeSnapshotOffset int64) (err error) {
	return b.e.internalDo("save-meta", func(conn internalConn) error {
		return b.e.rw.saveBinlogMetaLocked(snapshotMeta)
	})
}

func (b binlogEngine) Revert(toOffset int64) (bool, error) {
	return false, nil
}

func (b binlogEngine) ChangeRole(info binlog.ChangeRoleInfo) error {
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
