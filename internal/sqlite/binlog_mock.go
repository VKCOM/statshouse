package sqlite

import (
	"encoding/binary"

	"github.com/stretchr/testify/require"
	"github.com/vkcom/statshouse/internal/vkgo/binlog"
)

type binlogMock struct {
	bytes      []byte
	engine     binlog.Engine
	t          require.TestingT
	lastCommit int64
	skipL      int64
}

func NewBinlogMock(engine binlog.Engine, t require.TestingT) binlog.Binlog {
	return &binlogMock{engine: engine, t: t}
}

func (b *binlogMock) Run(offset int64, snapshotMeta []byte, engine binlog.Engine) error {
	b.engine = engine
	//bytes := b.bytes
	//if len(bytes) > 0 {
	//	max := 4
	//	for len(bytes) > 0 {
	//		offs, err := b.engine.Apply(b.bytes[:max])
	//		bytes = b.bytes[:offs]
	//		if errors.Is(err, binlog.ErrorNotEnoughData) {
	//			max *= 2
	//		} else {
	//			max = 4
	//			require.NoError(b.t, err)
	//		}
	//	}
	//	require.NoError(b.t, engine.ChangeRole(binlog.ChangeRoleInfo{
	//		IsMaster:   false,
	//		IsReady:    true,
	//		ViewNumber: 0,
	//	}))
	//}
	return nil
}

func (b *binlogMock) Restart() {
}

func (b *binlogMock) Append(onOffset int64, payload []byte) (nextLevOffset int64, err error) {
	for len(payload)%4 != 0 {
		payload = append(payload, 0)
	}
	b.bytes = append(b.bytes, payload...)
	return int64(len(b.bytes)), nil
}

func (b *binlogMock) AppendASAP(onOffset int64, payload []byte) (nextLevOffset int64, err error) {
	offs, err := b.Append(onOffset, payload)
	b.MockCommit(b.t)
	return offs, err
}

func (b *binlogMock) AddStats(stats map[string]string) {

}

func (b *binlogMock) Shutdown() error {
	return nil
}

func (b *binlogMock) EngineStatus(status binlog.EngineStatus) {

}

func (b *binlogMock) MockCommit(t require.TestingT) bool {
	offs := int64(len(b.bytes)) + b.skipL
	if b.lastCommit == offs {
		return false
	}
	err := b.engine.Commit(int64(offs), binary.AppendVarint(nil, int64(offs)), int64(offs))
	require.NoError(t, err)
	b.lastCommit = offs
	return true
}

func (b *binlogMock) MockApply(t require.TestingT, bytes []byte) int64 {
	newOffset, err := b.engine.Apply(bytes)
	require.NoError(t, err)
	b.bytes = append(b.bytes, bytes...)
	return newOffset
}

//func (b *binlogMock) MockCommitReread(t require.TestingT, commitOffset int64) bool {
//	if b.lastCommit == commitOffset {
//		return false
//	}
//	err := b.engine.Commit(commitOffset, binary.AppendVarint(nil, commitOffset), commitOffset)
//	require.NoError(t, err)
//	b.lastCommit = commitOffset
//	return true
//}
//
//func (b *binlogMock) MockApplyReread(t require.TestingT, offset, duration int64) (newOffset int64, isFinish bool, err error) {
//	bytes := b.bytes[offset:][:duration]
//	if len(bytes) == 0 {
//		return 0, true, nil
//	}
//	max := 4
//	for {
//		readBytes, err := b.engine.Apply(b.bytes[:max])
//		bytes = b.bytes[:readBytes]
//		if errors.Is(err, binlog.ErrorNotEnoughData) {
//			max *= 2
//			continue
//		}
//		if err != nil {
//			return 0, false, err
//		}
//		return offset + readBytes, false, nil
//
//	}
//}

func (b *binlogMock) MockSkip(t require.TestingT, skip int64) int64 {
	newOffset, err := b.engine.Skip(skip)
	b.skipL += skip
	require.NoError(t, err)
	return newOffset
}
