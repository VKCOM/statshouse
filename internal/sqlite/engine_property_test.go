package sqlite

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vkcom/statshouse/internal/vkgo/basictl"
	"go.uber.org/atomic"
	"pgregory.net/rapid"

	"github.com/vkcom/statshouse/internal/sqlite/internal/sqlite0"
	"github.com/vkcom/statshouse/internal/vkgo/binlog"
)

const path = "test.db"
const appID = 123

func increment(r *rapid.T, e *Engine, v int64) {
	err := e.Do(context.Background(), "test", func(conn Conn, bytes []byte) ([]byte, error) {
		_, err := conn.Exec("test", "UPDATE test SET value = value + $v", Int64("$v", v))
		return binlogEvent(v), err
	})
	require.NoError(r, err)
}

func checkActual(r *rapid.T, e *Engine, actual int64) {
	err := e.do(func(conn Conn) error {
		row := conn.Query("test", "SELECT value FROM test")
		require.NoError(r, row.Error())
		require.True(r, row.Next())
		v, _ := row.ColumnInt64(0)
		require.Equal(r, actual, v)
		return nil
	})
	require.NoError(r, err)
}

var magicTest = []byte{0x14, 0x29, 0x31, 0x46}
var (
	_ *engineMachineWaitCommitMode   // for staticcheck: type engineMachineWaitCommitMode is unused (U1000)
	_ *engineMachineNoWaitCommitMode // for staticcheck: type engineMachineNoWaitCommitMode is unused (U1000)
	_ *engineMachineBinlogRun        // for staticcheck: type engineMachineBinlogRun is unused (U1000)
)

func binlogEvent(v int64) []byte {
	buffer := make([]byte, 4)
	copy(buffer, magicTest)
	return basictl.LongWrite(buffer, v)
}

func readBinlogEvent(b []byte) ([]byte, int64, error) {
	if len(b) < 4 {
		return b, 0, binlog.ErrorNotEnoughData
	}
	if b[0] != magicTest[0] || b[1] != magicTest[1] || b[2] != magicTest[2] || b[3] != magicTest[3] {
		return b, 0, binlog.ErrorUnknownMagic
	}
	b = b[4:]
	var v int64
	b, err := basictl.LongRead(b, &v)
	return b, v, err
}

func applyFunc(scan bool) func(conn Conn, offset int64, bytes []byte) (int, error) {
	return func(conn Conn, offset int64, bytes []byte) (int, error) {
		read := len(bytes)
		for len(bytes) > 0 {
			var v int64
			var err error
			bytes, v, err = readBinlogEvent(bytes)
			if err != nil {
				return 0, err
			}
			if !scan {
				_, err = conn.Exec("test", "UPDATE test SET value = value + $v", Int64("$v", v))
				if err != nil {
					return 0, err
				}
			}
		}
		return read, nil
	}
}

type engineMachineWaitCommitMode struct {
	engine    *Engine
	actual    int64
	committed int64
}

func (m *engineMachineWaitCommitMode) Init(t *rapid.T) {
	e, err := newEngine(t, WaitCommit, "CREATE TABLE IF NOT EXISTS test (value INTEGER)")
	require.NoError(t, err)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	m.engine = e
	err = e.Do(context.Background(), "test", func(conn Conn, bytes []byte) ([]byte, error) {
		_, err := conn.Exec("test", "INSERT INTO test (value) VALUES (0)")
		return nil, err
	})
	require.NoError(t, err)
	e.commitTXAndStartNew(true, false)
}

func (m *engineMachineWaitCommitMode) DoWrite(r *rapid.T) {
	v := rapid.Int32().Draw(r, "append value")
	m.actual += int64(v)
	increment(r, m.engine, int64(v))
}

func (m *engineMachineWaitCommitMode) DoTx(r *rapid.T) {
	m.engine.commitTXAndStartNew(true, true)
	m.committed = m.actual
}

func (m *engineMachineWaitCommitMode) Check(t *rapid.T) {
	checkActual(t, m.engine, m.actual)
}

func TestWaitCommit(t *testing.T) {
	rapid.Check(t, rapid.Run[*engineMachineWaitCommitMode]())
}

type engineMachineNoWaitCommitMode struct {
	engine    *Engine
	actual    int64
	committed int64
}

func (m *engineMachineNoWaitCommitMode) Init(t *rapid.T) {
	//todo unify
	e, err := newEngine(t, NoWaitCommit, "CREATE TABLE IF NOT EXISTS test (value INTEGER)")
	e.isTest = true
	require.NoError(t, err)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	m.engine = e
	err = e.Do(context.Background(), "test", func(conn Conn, bytes []byte) ([]byte, error) {
		_, err := conn.Exec("test", "INSERT INTO test (value) VALUES (0)")
		return nil, err
	})
	require.NoError(t, err)
	e.commitTXAndStartNew(true, false)
}

func (m *engineMachineNoWaitCommitMode) DoWrite(r *rapid.T) {
	v := rapid.Int32().Draw(r, "append value")
	m.actual += int64(v)
	increment(r, m.engine, int64(v))
}

func (m *engineMachineNoWaitCommitMode) DoBinlogCommit(r *rapid.T) {
	m.engine.binlog.(*binlogMock).MockCommit(r)
}

func (m *engineMachineNoWaitCommitMode) DoWriteTx(r *rapid.T) {
	m.engine.mustCommitNowFlag = true
	v := rapid.Int32().Draw(r, "append value")
	m.actual += int64(v)
	increment(r, m.engine, int64(v))
	m.committed = m.actual
	m.engine.mustCommitNowFlag = false
}

func (m *engineMachineNoWaitCommitMode) Check(t *rapid.T) {
	checkActual(t, m.engine, m.actual)
}

func TestNoWaitCommit(t *testing.T) {
	rapid.Check(t, rapid.Run[*engineMachineNoWaitCommitMode]())
}

type engineMachineBinlogRun struct {
	bl            *binlogMock
	engine        *Engine
	actualValue   int64
	currentOffset int64
	commitOffset  int64
	wasCommit     bool

	lastActualValueBeforeWait   int64
	lastCurrentOffsetBeforeWait int64
}

func (m *engineMachineBinlogRun) Init(t *rapid.T) {
	e, err := newEngine(t, NoWaitCommit, "CREATE TABLE IF NOT EXISTS test (value INTEGER)")
	e.isTest = true
	require.NoError(t, err)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	m.engine = e
	m.bl = e.binlog.(*binlogMock)
	err = e.Do(context.Background(), "test", func(conn Conn, bytes []byte) ([]byte, error) {
		_, err := conn.Exec("test", "INSERT INTO test (value) VALUES (0)")
		return nil, err
	})
	require.NoError(t, err)
	e.commitTXAndStartNew(true, false)
}

func (m *engineMachineBinlogRun) ApplyBinlog(t *rapid.T) {
	if !m.engine.mustWaitCommit && m.wasCommit && m.currentOffset > m.commitOffset {
		if rapid.Bool().Draw(t, "should start wait commit") {
			m.engine.mustWaitCommit = true
			m.lastActualValueBeforeWait = m.actualValue
			m.lastCurrentOffsetBeforeWait = m.currentOffset
		}
	}
	n := rapid.IntRange(1, 100).Draw(t, "binlog events size")
	events := []byte{}
	for i := 0; i < n; i++ {
		x := rapid.Int32().Draw(t, "binlog event value")
		m.actualValue += int64(x)
		events = append(events, binlogEvent(int64(x))...)
	}
	m.bl.MockApply(t, events)
	m.currentOffset += int64(len(events))
}

func (m *engineMachineBinlogRun) CommitBinlog(t *rapid.T) {
	m.commitOffset = m.currentOffset
	if m.bl.MockCommit(t) {
		m.wasCommit = true
		m.engine.mustWaitCommit = false
	}
}

func (m *engineMachineBinlogRun) SkipBinlog(t *rapid.T) {
	n := rapid.IntRange(4, 120).Draw(t, "binlog size")
	n = n - n%4
	m.bl.MockSkip(t, int64(n))
	m.currentOffset += int64(n)
}

func (m *engineMachineBinlogRun) Check(t *rapid.T) {
	currentOffset := m.currentOffset
	actualValue := m.actualValue
	if m.engine.mustWaitCommit {
		currentOffset = m.lastCurrentOffsetBeforeWait
		actualValue = m.lastActualValueBeforeWait
	}
	checkActual(t, m.engine, actualValue)
	require.Equal(t, currentOffset, m.engine.dbOffset)
	if m.wasCommit {
		committedInfo, ok := m.engine.committedInfo.Load().(*committedInfo)
		require.True(t, ok)
		actualCommittedOffset := committedInfo.offset
		require.Equal(t, m.commitOffset, actualCommittedOffset)
		actualCommittedOffset, err := binary.ReadVarint(bytes.NewReader(committedInfo.meta))
		require.NoError(t, err)
		require.Equal(t, m.commitOffset, actualCommittedOffset)
	}
}

func TestApplyCommit(t *testing.T) {
	rapid.Check(t, rapid.Run[*engineMachineBinlogRun]())
}

func openInMemory(path string, flags int, cb ProfileCallback) (*sqlite0.Conn, error) {
	conn, err := sqlite0.Open(path, flags|sqlite0.OpenMemory)
	if err != nil {
		return nil, err
	}

	// todo make checkpoint manually
	if false {
		err = conn.SetAutoCheckpoint(0)
		if err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("failed to disable DB auto-checkpoints: %w", err)
		}
	}

	err = conn.SetBusyTimeout(busyTimeout)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to set DB busy timeout to %v: %w", busyTimeout, err)
	}

	return conn, nil
}

func newEngine(t require.TestingT, mode DurabilityMode, scheme string) (*Engine, error) {
	rw, err := openRW(openInMemory, path, appID, nil, initOffsetTable, snapshotMetaTable, scheme)
	if err != nil {
		return nil, fmt.Errorf("failed to open RW connection: %w", err)
	}

	ctx, stop := context.WithCancel(context.Background())
	e := &Engine{
		rw:   newSqliteConn(rw),
		ctx:  ctx,
		stop: stop,
		//	chk:                     chk,
		opt: Options{
			Path:           path,
			APPID:          appID,
			Scheme:         "",
			Replica:        false,
			DurabilityMode: mode,
			StatsOptions:   StatsOptions{},
		},

		apply:                applyFunc(false),
		scan:                 applyFunc(true),
		committedInfo:        &atomic.Value{},
		dbOffset:             0,
		readyNotify:          sync.Once{},
		waitUntilBinlogReady: make(chan struct{}),
		commitCh:             make(chan struct{}, 1),
		mode:                 master,
	}
	e.isTest = true
	e.roCond = sync.NewCond(&e.roMx)
	e.commitTXAndStartNew(false, false)
	if err := e.rw.err; err != nil {
		_ = e.close(false, false)
		return nil, fmt.Errorf("failed to start write transaction: %w", err)
	}
	binlogEngineImpl := &binlogEngineImpl{e: e}
	e.committedInfo.Store(&committedInfo{})
	offset, err := e.binlogLoadOrCreatePosition()
	if err != nil {
		_ = e.close(false, false)
		return nil, fmt.Errorf("failed to load binlog position: %w", err)
	}
	e.dbOffset = offset
	_, err = e.binlogLoadOrCreateMeta()
	if err != nil {
		_ = e.close(false, false)
		return nil, fmt.Errorf("failed to load snapshot meta: %w", err)
	}
	e.binlog = NewBinlogMock(binlogEngineImpl, t)
	e.commitTXAndStartNew(true, false)
	if err := e.rw.err; err != nil {
		_ = e.close(false, false)
		return nil, fmt.Errorf("failed to start write transaction: %w", err)
	}
	return e, nil
}
