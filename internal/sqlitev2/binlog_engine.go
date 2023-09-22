package sqlitev2

import (
	"errors"
	"fmt"
	"io"

	"github.com/vkcom/statshouse/internal/vkgo/binlog"
)

type binlogEngineReplicaImpl struct {
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

func newBinlogEngine(e *Engine, applyFunction ApplyEventFunction) *binlogEngineReplicaImpl {
	return &binlogEngineReplicaImpl{
		e:             e,
		applyFunction: applyFunction,
	}
}

func (b binlogEngineReplicaImpl) Apply(payload []byte) (newOffset int64, errToReturn error) {
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
	} else {
		b.e.rw.dbOffset = newOffset
	}
	return newOffset, errToReturn
}

func (b binlogEngineReplicaImpl) Skip(skipLen int64) (newOffset int64, err error) {
	err = b.e.internalDo("skip", func(c Conn) error {
		b.e.rw.dbOffset += skipLen
		newOffset = b.e.rw.dbOffset
		return b.e.rw.saveBinlogOffsetLocked(newOffset)
	})
	return newOffset, b.e.rw.setError(err)
}

// База может обгонять бинлог. Никак не учитываем toOffset
func (b binlogEngineReplicaImpl) Commit(toOffset int64, snapshotMeta []byte, safeSnapshotOffset int64) (err error) {
	return b.e.internalDo("save-meta", func(conn internalConn) error {
		return b.e.rw.saveBinlogMetaLocked(snapshotMeta)
	})
}

func (b binlogEngineReplicaImpl) Revert(toOffset int64) (bool, error) {
	return false, nil
}

func (b binlogEngineReplicaImpl) ChangeRole(info binlog.ChangeRoleInfo) error {
	//TODO implement me
	panic("implement me")
}

func (b binlogEngineReplicaImpl) StartReindex() error {
	return fmt.Errorf("implement")
}
