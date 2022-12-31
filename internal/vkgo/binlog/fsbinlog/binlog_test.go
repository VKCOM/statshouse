// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package fsbinlog

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/vkcom/statshouse/internal/vkgo/basictl"
	"github.com/vkcom/statshouse/internal/vkgo/binlog"
	"github.com/vkcom/statshouse/internal/vkgo/binlog/fsbinlog/internal/gen/tlfsbinlog"
)

const testMagic = uint32(12345)

func TestMain(m *testing.M) {
	testEnvTurnOffFSync = true
	code := m.Run()
	os.Exit(code)
}

type LoggerStdout struct{}

func (*LoggerStdout) Tracef(format string, args ...interface{}) {
	log.Printf("Trace: "+format+"\n", args...)
}
func (*LoggerStdout) Debugf(format string, args ...interface{}) {
	log.Printf("Debug: "+format+"\n", args...)
}
func (*LoggerStdout) Infof(format string, args ...interface{}) {
	log.Printf("Info: "+format+"\n", args...)
}
func (*LoggerStdout) Warnf(format string, args ...interface{}) {
	log.Printf("Warn: "+format+"\n", args...)
}
func (*LoggerStdout) Errorf(format string, args ...interface{}) {
	log.Printf("Error: "+format+"\n", args...)
}

func serialize(s string) []byte {
	out := make([]byte, 8)
	binary.LittleEndian.PutUint32(out, testMagic)
	binary.LittleEndian.PutUint32(out[4:], uint32(len(s)))
	out = append(out, []byte(s)...)
	return out
}

func deserialize(expectedMagic uint32, payload []byte) (string, int64, error) {
	magic, payload, err := basictl.NatReadTag(payload)
	if err != nil {
		return "", 0, err
	}

	if magic != expectedMagic {
		return "", 0, binlog.ErrorUnknownMagic
	}

	valSize, payload, err := basictl.NatReadTag(payload)
	if err != nil {
		return "", 0, err
	}

	if len(payload) < int(valSize) {
		return "", 0, binlog.ErrorNotEnoughData
	}

	value := make([]byte, valSize)
	copy(value, payload[:valSize])
	return string(value), int64(AddPadding(int(8 + valSize))), nil
}

var genStrCount byte // for easier debugging

func genStr(n int) string {
	return string(genBytes(n))
}

func genBytes(n int) []byte {
	s := make([]byte, n)
	genBytesIntoSlice(s)
	return s
}

func genBytesIntoSlice(buff []byte) {
	for i := 0; i < len(buff); i++ {
		buff[i] = byte('a') + genStrCount
	}
	genStrCount = (genStrCount + 1) % 26
}

func InitBinlogWithLevs(t *testing.T, options binlog.Options, engine TestEngine, levs []string) binlog.Binlog {
	_, err := CreateEmptyFsBinlog(options)
	assert.Nil(t, err)

	bl := OpenBinlogAndWriteLevs(t, options, engine, levs)
	return bl
}

type TestEngine interface {
	binlog.Engine
	WaitForReadyFlag()
	WaitUntilCommit(offset int64) int64

	GetCurrentOffset() int64
	SetCurrentOffset(offset int64)
	AddOffset(offset int64)
}

type TestEngineImpl struct {
	ready          atomic.Bool
	kindaReady     atomic.Bool
	currentOffset  atomic.Int64
	commitPosition atomic.Int64
	commitCrc      atomic.Uint32
	commitMu       sync.Mutex
	commitMeta     []byte
	commitCV       *sync.Cond

	applyCb func(payload []byte) (newOffset int64, err error)
}

func (e *TestEngineImpl) Apply(payload []byte) (int64, error) {
	if e.applyCb == nil {
		log.Fatalf("Error: applyCb is not set")
	}
	return e.applyCb(payload)
}
func (e *TestEngineImpl) Commit(offset int64, snapshotMeta []byte, safeSnapshotOffset int64) {
	e.commitMu.Lock()

	e.commitPosition.Store(offset)
	e.commitMeta = snapshotMeta
	meta := tlfsbinlog.SnapshotMeta{}
	_, err := meta.ReadBoxed(snapshotMeta)
	if err != nil {
		log.Fatalf("cannot read snap meta: %s", err)
	}
	e.commitCrc.Store(meta.CommitCrc)

	e.commitMu.Unlock()
	e.commitCV.Signal()
}

func (e *TestEngineImpl) Skip(skipLen int64) int64 {
	e.currentOffset.Add(skipLen)
	return e.currentOffset.Load()
}

func (e *TestEngineImpl) Revert(toOffset int64) bool {
	panic("not implemented")
}

func (e *TestEngineImpl) ChangeRole(info binlog.ChangeRoleInfo) {
	if info.IsReady {
		e.ready.Store(true)
	}
	e.kindaReady.Store(true)
}

func (e *TestEngineImpl) WaitForReadyFlag() {
	for !e.ready.Load() {
	}
}

func (e *TestEngineImpl) WaitForKindaReadyFlag() {
	for !e.kindaReady.Load() {
	}
}

func (e *TestEngineImpl) WaitUntilCommit(offset int64) int64 {
	e.commitMu.Lock()
	defer e.commitMu.Unlock()

	for {
		curPos := e.commitPosition.Load()
		if curPos >= offset {
			log.Printf("Finish wait for commit on position %d", offset)
			return curPos
		}
		log.Printf("Wait for commit on position %d, but stil got %d. Continue to wait", offset, e.commitPosition.Load())
		e.commitCV.Wait()
	}
}

func (e *TestEngineImpl) GetCommitedmeta() []byte {
	e.commitMu.Lock()
	defer e.commitMu.Unlock()
	return e.commitMeta
}

func (e *TestEngineImpl) GetCurrentOffset() int64 {
	return e.currentOffset.Load()
}
func (e *TestEngineImpl) SetCurrentOffset(offset int64) {
	e.currentOffset.Store(offset)
}
func (e *TestEngineImpl) AddOffset(offset int64) {
	e.currentOffset.Add(offset)
}

func NewTestEngine(startPosition int64) *TestEngineImpl {
	out := &TestEngineImpl{}
	out.commitCV = sync.NewCond(&out.commitMu)
	out.SetCurrentOffset(startPosition)
	return out
}

func OpenBinlogAndWriteLevs(t *testing.T, options binlog.Options, engine TestEngine, levs []string) binlog.Binlog {
	bl, err := NewFsBinlog(&LoggerStdout{}, options)
	assert.Nil(t, err)

	go func() {
		err = bl.Run(0, []byte{}, engine)
		assert.Nil(t, err)
	}()

	engine.WaitForReadyFlag()

	offset := WriteLevsToBinlog(t, bl, engine, levs)
	engine.WaitUntilCommit(offset)
	return bl
}

func WriteLevsToBinlog(t *testing.T, bl binlog.Binlog, eng TestEngine, levs []string) int64 {
	for _, lev := range levs[:len(levs)-1] {
		offset, err := bl.Append(eng.GetCurrentOffset(), serialize(lev))
		eng.SetCurrentOffset(offset)
		assert.Nil(t, err)
	}

	offset, err := bl.AppendASAP(eng.GetCurrentOffset(), serialize(levs[len(levs)-1]))
	assert.Nil(t, err)
	eng.SetCurrentOffset(offset)

	return offset
}

func TestBinlogSetGet(t *testing.T) {
	dir := t.TempDir()

	testLevs := []string{"hello world", "hello world 2", genStr(50)}

	options := binlog.Options{
		ReplicaMode: false,
		PrefixPath:  dir + "/test_pref",
		Magic:       testMagic,
	}
	{
		bl := InitBinlogWithLevs(t, options, NewTestEngine(0), testLevs)
		assert.Nil(t, bl.Shutdown())
	}

	count := 0
	engine := NewTestEngine(0)
	engine.applyCb = func(payload []byte) (int64, error) {
		value, n, err := deserialize(testMagic, payload)
		if err != nil {
			return 0, err
		}
		assert.Equal(t, testLevs[count], value)
		count++
		engine.AddOffset(n)

		return engine.GetCurrentOffset(), nil
	}

	bl, err := NewFsBinlog(&LoggerStdout{}, options)
	assert.Nil(t, err)

	stop := make(chan struct{})
	go func() {
		assert.Nil(t, bl.Run(0, []byte{}, engine))
		stop <- struct{}{}
	}()

	engine.WaitForReadyFlag()

	assert.Equal(t, len(testLevs), count)
	assert.Nil(t, bl.Shutdown())
	<-stop
}

func TestUnknownMagic(t *testing.T) {
	dir := t.TempDir()

	testLevs := []string{"hello world", "hello world 2"}

	options := binlog.Options{
		ReplicaMode: false,
		PrefixPath:  dir + "/test_pref",
		Magic:       testMagic,
	}
	{
		engine := NewTestEngine(0)
		bl := InitBinlogWithLevs(t, options, engine, testLevs)

		// add lev with unknown magic
		lev := "wrong hello"
		out := make([]byte, 8)
		binary.LittleEndian.PutUint32(out, testMagic+1)
		binary.LittleEndian.PutUint32(out[4:], uint32(len(lev)))
		out = append(out, []byte(lev)...)
		offset, err := bl.AppendASAP(engine.GetCurrentOffset(), out)
		assert.NoError(t, err)
		engine.SetCurrentOffset(offset)
		engine.WaitUntilCommit(offset)

		assert.Nil(t, bl.Shutdown())
	}

	count := 0
	engine := NewTestEngine(0)
	engine.applyCb = func(payload []byte) (int64, error) {
		value, n, err := deserialize(testMagic, payload)
		engine.AddOffset(n)
		if count != 2 {
			assert.NoError(t, err)
		} else {
			assert.Error(t, err)
			return engine.GetCurrentOffset(), err
		}
		assert.Equal(t, testLevs[count], value)
		count++
		return engine.GetCurrentOffset(), nil
	}

	bl, err := NewFsBinlog(&LoggerStdout{}, options)
	assert.Nil(t, err)

	require.Error(t, bl.Run(0, []byte{}, engine))

	assert.Equal(t, len(testLevs), count)
	assert.NoError(t, bl.Shutdown())
}

func TestBinlogReadFromPosition(t *testing.T) {
	dir := t.TempDir()
	testLevs := []string{"hello world", "hello world 2", "hello world3", "hello world4"}

	options := binlog.Options{
		PrefixPath: dir + "/test_pref",
		Magic:      testMagic,
	}
	snapPos := int64(0)
	{
		var err error
		engine := NewTestEngine(0)
		bl := InitBinlogWithLevs(t, options, engine, testLevs[:1])

		snapPos, err = bl.Append(engine.GetCurrentOffset(), serialize(testLevs[1])) // wrote 2 event
		assert.NoError(t, err)
		engine.SetCurrentOffset(snapPos)

		offset := WriteLevsToBinlog(t, bl, engine, testLevs[2:])
		engine.WaitUntilCommit(offset)
		assert.Nil(t, bl.Shutdown())
	}

	engine := NewTestEngine(snapPos)
	count := 2
	engine.applyCb = func(payload []byte) (int64, error) {
		value, n, err := deserialize(testMagic, payload)
		assert.Nil(t, err)
		assert.Equal(t, testLevs[count], value)
		count++
		engine.AddOffset(n)
		return engine.GetCurrentOffset(), nil
	}

	bl, err := NewFsBinlog(&LoggerStdout{}, options)
	assert.Nil(t, err)

	stop := make(chan struct{})
	go func() {
		assert.Nil(t, bl.Run(snapPos, []byte{}, engine))
		stop <- struct{}{}
	}()

	engine.WaitForReadyFlag()

	assert.Equal(t, len(testLevs), count)

	require.NoError(t, bl.Shutdown())
	<-stop
}

func TestBinlogReadFromPositionRotateFile(t *testing.T) {
	dir := t.TempDir()
	testLevs := []string{genStr(1024), genStr(700), genStr(512), genStr(256), genStr(512)}

	options := binlog.Options{
		PrefixPath:   dir + "/test_pref",
		MaxChunkSize: 1024,
		Magic:        testMagic,
	}
	snapPos := int64(0)
	{
		var err error
		engine := NewTestEngine(0)
		bl := InitBinlogWithLevs(t, options, engine, testLevs[:1])

		snapPos, err = bl.Append(engine.GetCurrentOffset(), serialize(testLevs[1]))
		assert.NoError(t, err)
		engine.SetCurrentOffset(snapPos)

		offset := WriteLevsToBinlog(t, bl, engine, testLevs[2:])
		engine.WaitUntilCommit(offset)
		assert.Nil(t, bl.Shutdown())
	}

	engine := NewTestEngine(snapPos)
	count := 2
	engine.applyCb = func(payload []byte) (int64, error) {
		value, n, err := deserialize(testMagic, payload)
		assert.Nil(t, err)
		assert.Equal(t, testLevs[count], value)
		count++
		engine.AddOffset(n)
		return engine.GetCurrentOffset(), nil
	}

	bl, err := NewFsBinlog(&LoggerStdout{}, options)
	assert.Nil(t, err)

	stop := make(chan struct{})
	go func() {
		assert.Nil(t, bl.Run(snapPos, []byte{}, engine))
		stop <- struct{}{}
	}()

	engine.WaitForReadyFlag()

	assert.Equal(t, len(testLevs), count)

	require.NoError(t, bl.Shutdown())
	<-stop
}

func TestBinlogWriteWithSeveralFiles(t *testing.T) {
	dir := t.TempDir()

	testLevs := []string{genStr(1024), genStr(512), genStr(512), genStr(512), genStr(512), genStr(512)}

	options := binlog.Options{
		PrefixPath:   dir + "/test_pref",
		MaxChunkSize: 1024,
		Magic:        testMagic,
	}
	snapPos := int64(0)

	// Write first part
	{
		engine := NewTestEngine(0)
		bl := InitBinlogWithLevs(t, options, engine, testLevs[:2])
		snapPos = engine.WaitUntilCommit(1500)

		offset := WriteLevsToBinlog(t, bl, engine, testLevs[2:4])
		engine.WaitUntilCommit(offset)
		assert.Nil(t, bl.Shutdown())
	}

	// Open new binlog and continue to write second part
	{
		engine := NewTestEngine(0)
		engine.applyCb = func(payload []byte) (int64, error) {
			_, n, err := deserialize(testMagic, payload)
			engine.AddOffset(n)
			return engine.GetCurrentOffset(), err
		}
		bl := OpenBinlogAndWriteLevs(t, options, engine, testLevs[4:])
		assert.Nil(t, bl.Shutdown())
	}

	// Now read all events
	engine := NewTestEngine(snapPos)
	count := 2
	engine.applyCb = func(payload []byte) (int64, error) {
		value, n, err := deserialize(testMagic, payload)
		assert.Nil(t, err)
		assert.Equal(t, testLevs[count], value)
		count++
		engine.AddOffset(n)
		return engine.GetCurrentOffset(), nil
	}

	bl, err := NewFsBinlog(&LoggerStdout{}, options)
	assert.Nil(t, err)

	stop := make(chan struct{})
	go func() {
		assert.Nil(t, bl.Run(snapPos, []byte{}, engine))
		stop <- struct{}{}
	}()

	engine.WaitForReadyFlag()

	assert.Equal(t, len(testLevs), count)
	require.NoError(t, bl.Shutdown())
	<-stop
}

func TestBinlogSimulateMasterChangeWithPartialEvent(t *testing.T) {
	dir := t.TempDir()

	testLevs := []string{genStr(1024), genStr(1024), genStr(1024), genStr(1024)}

	options := binlog.Options{
		PrefixPath: dir + "/test_pref",
		Magic:      testMagic,
	}

	{
		engine := NewTestEngine(0)
		bl := InitBinlogWithLevs(t, options, engine, testLevs)
		require.NoError(t, bl.Shutdown())
	}

	splitPos := 1520
	filePath := dir + "/test_pref.000000.bin"
	leftover, err := os.ReadFile(filePath)
	require.Nil(t, err)
	leftover = leftover[splitPos:]

	err = os.Truncate(filePath, int64(splitPos))
	require.Nil(t, err)

	s, _ := os.Stat(filePath)
	fmt.Printf("size %d\n", s.Size())

	engine := NewTestEngine(0)
	count := 0
	engine.applyCb = func(payload []byte) (int64, error) {
		value, n, err := deserialize(testMagic, payload)
		engine.AddOffset(n)
		if err != nil {
			return engine.GetCurrentOffset(), err
		}
		assert.Equal(t, testLevs[count], value)
		count++
		return engine.GetCurrentOffset(), nil
	}

	bl, pidChange, err := NewFsBinlogMasterChange(&LoggerStdout{}, options)
	assert.Nil(t, err)

	stop := make(chan struct{})
	go func() {
		assert.NoError(t, bl.Run(0, []byte{}, engine))
		stop <- struct{}{}
	}()

	engine.WaitForKindaReadyFlag()
	assert.Equal(t, 1, count)

	{
		f, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY, 0666)
		assert.Nil(t, err)
		_, err = f.Write(leftover)
		assert.Nil(t, err)
		_ = f.Close()
	}

	// Ok, read tail
	*pidChange <- struct{}{}

	engine.WaitForReadyFlag()
	assert.Equal(t, len(testLevs), count)

	require.NoError(t, bl.Shutdown())
	<-stop
}

func TestBinlogBigSetGetRandom(t *testing.T) {
	dir := t.TempDir()

	seed := time.Now().UnixNano()
	genStrCount = 0
	fmt.Printf("Test rand seed: %d\n", seed)
	seededRand := rand.New(rand.NewSource(seed))

	N := 10000
	type tCase struct {
		lev    string
		offset int64
	}
	testLevs := make([]tCase, N)
	for i := 0; i < N; i++ {
		testLevs[i] = tCase{
			lev: genStr(seededRand.Intn(1024 * 10)),
		}
	}

	options := binlog.Options{
		ReplicaMode:  false,
		PrefixPath:   dir + "/test_pref",
		Magic:        testMagic,
		MaxChunkSize: 2 * 1024 * 1024,
	}
	{
		engine := NewTestEngine(0)
		_, err := CreateEmptyFsBinlog(options)
		assert.Nil(t, err)

		bl, err := NewFsBinlog(&LoggerStdout{}, options)
		assert.Nil(t, err)

		stop := make(chan struct{})
		go func() {
			assert.NoError(t, bl.Run(0, []byte{}, engine))
			stop <- struct{}{}
		}()

		engine.WaitForReadyFlag()

		offset := engine.GetCurrentOffset()
		require.Equal(t, int64(44), offset) // only start levs
		for i, l := range testLevs {
			testLevs[i].offset = engine.GetCurrentOffset()
			require.Nil(t, err)
			if i == len(testLevs)-1 {
				offset, err = bl.AppendASAP(engine.GetCurrentOffset(), serialize(l.lev))
			} else {
				offset, err = bl.Append(engine.GetCurrentOffset(), serialize(l.lev))
			}

			engine.SetCurrentOffset(offset)
			require.Nil(t, err)
		}

		engine.WaitUntilCommit(offset)
		assert.Nil(t, bl.Shutdown())
		<-stop
	}

	count := 0
	readEngine := NewTestEngine(0)
	readEngine.applyCb = func(payload []byte) (int64, error) {
		for {
			if len(payload) == 0 {
				return readEngine.GetCurrentOffset(), nil
			}
			value, n, err := deserialize(testMagic, payload)
			if err != nil {
				return readEngine.GetCurrentOffset(), err
			}
			require.Equal(t, testLevs[count].offset, readEngine.GetCurrentOffset())
			require.Equal(t, testLevs[count].lev, value)

			readEngine.AddOffset(n)
			payload = payload[n:]
			count++
		}
	}

	bl, err := NewFsBinlog(&LoggerStdout{}, options)
	assert.Nil(t, err)
	stop := make(chan struct{})
	go func() {
		assert.NoError(t, bl.Run(0, []byte{}, readEngine))
		stop <- struct{}{}
	}()

	readEngine.WaitForReadyFlag()

	assert.Equal(t, len(testLevs), count)
	assert.Nil(t, bl.Shutdown())
	<-stop
}

func TestBigLev(t *testing.T) {
	dir := t.TempDir()

	testLevs := []string{genStr(1024 * 1024)}

	options := binlog.Options{
		PrefixPath: dir + "/test_pref",
		Magic:      testMagic,
	}
	{
		bl := InitBinlogWithLevs(t, options, NewTestEngine(0), testLevs)
		assert.Nil(t, bl.Shutdown())
	}

	count := 0
	engine := NewTestEngine(0)
	engine.applyCb = func(payload []byte) (int64, error) {
		value, n, err := deserialize(testMagic, payload)
		engine.AddOffset(n)
		if err != nil {
			return engine.GetCurrentOffset(), err
		}
		assert.Equal(t, testLevs[count], value)
		count++

		return engine.GetCurrentOffset(), nil
	}

	bl, err := NewFsBinlog(nil, options)
	assert.Nil(t, err)

	stop := make(chan struct{})
	go func() {
		assert.Nil(t, bl.Run(0, []byte{}, engine))
		stop <- struct{}{}
	}()

	engine.WaitForReadyFlag()

	assert.Equal(t, len(testLevs), count)

	assert.Nil(t, bl.Shutdown())
	<-stop
}

func TestSnapshotMeta(t *testing.T) {
	dir := t.TempDir()

	testLevs := []string{"hello world", "hello world 2", genStr(50)}

	options := binlog.Options{
		ReplicaMode: false,
		PrefixPath:  dir + "/test_pref",
		Magic:       testMagic,
	}
	var snapPos int64
	var snapMeta []byte
	{
		engine := NewTestEngine(0)
		bl := InitBinlogWithLevs(t, options, engine, testLevs[:2])
		snapPos = engine.commitPosition.Load()
		snapMeta = engine.GetCommitedmeta()

		offset := WriteLevsToBinlog(t, bl, engine, testLevs[2:])
		engine.WaitUntilCommit(offset)
		assert.Nil(t, bl.Shutdown())
	}

	count := 2
	engine := NewTestEngine(snapPos)
	engine.applyCb = func(payload []byte) (int64, error) {
		value, n, err := deserialize(testMagic, payload)
		engine.AddOffset(n)
		assert.NoError(t, err)
		assert.Equal(t, testLevs[count], value)
		count++

		return engine.GetCurrentOffset(), nil
	}

	bl, err := NewFsBinlog(&LoggerStdout{}, options)
	assert.Nil(t, err)

	stop := make(chan struct{})
	go func() {
		require.NoError(t, bl.Run(snapPos, snapMeta, engine))
		stop <- struct{}{}
	}()

	engine.WaitForReadyFlag()
	assert.Equal(t, len(testLevs), count)
	assert.Nil(t, bl.Shutdown())
	<-stop
}

func TestSnapshotMetaCommitInDifferentFile(t *testing.T) {
	dir := t.TempDir()

	testLevs := []string{genStr(256), genStr(256), genStr(500), genStr(500), genStr(500), genStr(500)}

	options := binlog.Options{
		MaxChunkSize: 1024,
		PrefixPath:   dir + "/test_pref",
		Magic:        testMagic,
	}
	var snapPos int64
	var snapMeta []byte
	{
		engine := NewTestEngine(0)
		bl := InitBinlogWithLevs(t, options, engine, testLevs[:2])
		snapMeta = engine.GetCommitedmeta() // meta in first file

		snapPos = WriteLevsToBinlog(t, bl, engine, testLevs[2:4]) // rotate, pos will be in new file

		offset := WriteLevsToBinlog(t, bl, engine, testLevs[4:])
		engine.WaitUntilCommit(offset)
		assert.Nil(t, bl.Shutdown())
	}

	count := 4
	engine := NewTestEngine(snapPos)
	engine.applyCb = func(payload []byte) (int64, error) {
		value, n, err := deserialize(testMagic, payload)
		engine.AddOffset(n)
		assert.NoError(t, err)
		assert.Equal(t, testLevs[count], value)
		count++

		return engine.GetCurrentOffset(), nil
	}

	bl, err := NewFsBinlog(&LoggerStdout{}, options)
	assert.Nil(t, err)

	stop := make(chan struct{})
	go func() {
		require.NoError(t, bl.Run(snapPos, snapMeta, engine))
		stop <- struct{}{}
	}()

	engine.WaitForReadyFlag()
	assert.Equal(t, len(testLevs), count)
	assert.Nil(t, bl.Shutdown())
	<-stop
}

func BenchmarkWrite(b *testing.B) {
	dir := b.TempDir()

	options := binlog.Options{
		PrefixPath: dir + "/test_pref",
	}

	levSize := 1024

	_, _ = CreateEmptyFsBinlog(options)
	bl, _ := NewFsBinlog(nil, options)
	engine := NewTestEngine(0)

	go func() {
		if err := bl.Run(0, []byte{}, engine); err != nil {
			log.Fatalln(err)
		}
	}()
	engine.WaitForReadyFlag()

	var testLevs [][]byte
	for i := 0; i < 26; i++ {
		testLevs = append(testLevs, genBytes(levSize))
	}

	count := byte(0)
	getLev := func() []byte {
		count = (count + 1) % 26
		return testLevs[count]
	}

	// Для того, чтобы сравнить время тупой записи в файл
	fd, err := os.OpenFile(filepath.Join(dir, "just.a.file"), os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Fatalln(err)
	}
	separateFile := bufio.NewWriter(fd)
	b.Run("trivial file write", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			testLev := getLev()
			if _, err := separateFile.Write(testLev); err != nil {
				log.Panicln(err)
			}
			b.SetBytes(int64(len(testLev)))
		}
	})

	// wait until disk buffers cool down
	_ = fd.Sync()
	_ = os.Remove(filepath.Join(dir, "just.a.file"))
	time.Sleep(2 * time.Second)

	offset := int64(0)
	b.Run("simple append", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			testLev := getLev()
			offset, _ = bl.Append(engine.GetCurrentOffset(), testLev)
			engine.SetCurrentOffset(offset)
			b.SetBytes(int64(len(testLev)))
		}
	})
	engine.WaitUntilCommit(offset)
	time.Sleep(2 * time.Second)

	b.Run("append asap", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			testLev := getLev()
			offset, _ = bl.AppendASAP(engine.GetCurrentOffset(), testLev)
			engine.SetCurrentOffset(offset)
			b.SetBytes(int64(len(testLev)))
		}
	})
	engine.WaitUntilCommit(offset)
	time.Sleep(2 * time.Second)

	b.Run("append asap and wait", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			testLev := getLev()
			offset, _ = bl.AppendASAP(engine.GetCurrentOffset(), testLev)
			engine.SetCurrentOffset(offset)
			engine.WaitUntilCommit(offset)

			b.SetBytes(int64(len(testLev)))
		}
	})
}

func TestReplicaWithRotate(t *testing.T) {
	dir := t.TempDir()
	testLevs := []string{genStr(1024), genStr(700), genStr(512), genStr(512), genStr(512), genStr(512)}

	masterOptions := binlog.Options{
		PrefixPath:   dir + "/test_pref",
		MaxChunkSize: 1024,
		Magic:        testMagic,
	}
	snapPos := int64(0)
	masterEngine := NewTestEngine(0)
	masterBl := InitBinlogWithLevs(t, masterOptions, masterEngine, testLevs[:1])

	replicaOptions := binlog.Options{
		PrefixPath:   dir + "/test_pref",
		MaxChunkSize: 1024,
		Magic:        testMagic,
		ReplicaMode:  true,
	}
	replicaEngine := NewTestEngine(snapPos)
	count := atomic.Int64{}
	replicaEngine.applyCb = func(payload []byte) (int64, error) {
		value, n, err := deserialize(testMagic, payload)
		replicaEngine.AddOffset(n)
		assert.Nil(t, err)
		assert.Equal(t, testLevs[count.Load()], value)
		count.Add(1)
		return replicaEngine.GetCurrentOffset(), nil
	}

	replicaBl, err := NewFsBinlog(&LoggerStdout{}, replicaOptions)
	assert.Nil(t, err)

	stopChan := make(chan struct{})
	go func() {
		assert.Nil(t, replicaBl.Run(snapPos, []byte{}, replicaEngine))
		stopChan <- struct{}{}
	}()

	for replicaEngine.GetCurrentOffset() < 1000 {
		time.Sleep(10 * time.Millisecond)
	}

	WriteLevsToBinlog(t, masterBl, masterEngine, testLevs[1:])

	for count.Load() != int64(len(testLevs)) {
		time.Sleep(10 * time.Millisecond)
	}

	assert.Nil(t, masterBl.Shutdown())
	assert.Nil(t, replicaBl.Shutdown())
	<-stopChan
}

func TestBinlogReadAndExit(t *testing.T) {
	dir := t.TempDir()

	testLevs := []string{"hello world", "hello world 2", genStr(50)}

	options := binlog.Options{
		PrefixPath: dir + "/test_pref",
		Magic:      testMagic,
	}
	{
		bl := InitBinlogWithLevs(t, options, NewTestEngine(0), testLevs)
		assert.Nil(t, bl.Shutdown())
	}

	options.ReadAndExit = true

	count := 0
	engine := NewTestEngine(0)
	engine.applyCb = func(payload []byte) (int64, error) {
		value, n, err := deserialize(testMagic, payload)
		if err != nil {
			return 0, err
		}
		assert.Equal(t, testLevs[count], value)
		count++
		engine.AddOffset(n)

		return engine.GetCurrentOffset(), nil
	}

	bl, err := NewFsBinlog(&LoggerStdout{}, options)
	assert.Nil(t, err)

	assert.Nil(t, bl.Run(0, []byte{}, engine))

	assert.Equal(t, len(testLevs), count)
}
