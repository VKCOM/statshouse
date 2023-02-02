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

func (b *binlogMock) MockSkip(t require.TestingT, skip int64) int64 {
	newOffset, err := b.engine.Skip(skip)
	b.skipL += skip
	require.NoError(t, err)
	return newOffset
}
