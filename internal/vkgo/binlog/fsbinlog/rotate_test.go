// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package fsbinlog

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/vkcom/statshouse/internal/vkgo/binlog"
)

func TestBinlogRotate(t *testing.T) {
	dir := t.TempDir()

	testLevs := []string{genStr(256), genStr(700), genStr(512), genStr(256)}

	options := binlog.Options{
		PrefixPath:   dir + "/test_pref",
		MaxChunkSize: 1024,
		Magic:        testMagic,
	}
	{
		bl := InitBinlogWithLevs(t, options, NewTestEngine(0), testLevs)
		assert.Nil(t, bl.Shutdown())
	}

	files, err := os.ReadDir(dir)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(files))

	count := 0
	engine := NewTestEngine(0)
	engine.applyCb = func(payload []byte) (int64, error) {
		value, n, err := deserialize(testMagic, payload)
		engine.AddOffset(n)
		assert.Nil(t, err)
		assert.Equal(t, testLevs[count], value)
		count++
		return engine.GetCurrentOffset(), err
	}

	bl, err := NewFsBinlog(&LoggerStdout{}, options)
	assert.Nil(t, err)

	stop := make(chan struct{})
	go func() {
		assert.Nil(t, bl.Start(0, []byte{}, engine))
		stop <- struct{}{}
	}()

	engine.WaitForReadyFlag()
	assert.Equal(t, len(testLevs), count)

	assert.Nil(t, bl.Shutdown())
	<-stop
}

func TestRotateLevsCorrect(t *testing.T) {
	dir := t.TempDir()
	seed := time.Now().UnixNano()
	genStrCount = 0
	fmt.Printf("Test rand seed: %d\n", seed)
	seededRand := rand.New(rand.NewSource(seed))

	N := 5000

	testLevs := make([]string, N)
	for i := 0; i < N; i++ {
		testLevs[i] = genStr(seededRand.Intn(1024 * 10))
	}

	options := binlog.Options{
		PrefixPath:   dir + "/test_pref",
		Magic:        testMagic,
		MaxChunkSize: 1024 * 1024,
	}

	engine := NewTestEngine(0)
	_, err := CreateEmptyFsBinlog(options)
	assert.Nil(t, err)

	bl, err := NewFsBinlog(&LoggerStdout{}, options)
	assert.Nil(t, err)

	stop := make(chan struct{})
	go func() {
		assert.NoError(t, bl.Start(0, []byte{}, engine))
		stop <- struct{}{}
	}()

	engine.WaitForReadyFlag()

	offset := int64(0)
	for _, l := range testLevs {
		offset, err = bl.AppendASAP(engine.GetCurrentOffset(), serialize(l))
		engine.SetCurrentOffset(offset)
		require.Nil(t, err)
	}

	engine.WaitUntilCommit(offset)
	assert.Nil(t, bl.Shutdown())
	<-stop

	testForCrcCorrectness(t, dir)
}

func TestRotateLevsCorrectFromMiddle(t *testing.T) {
	dir := t.TempDir()
	seed := time.Now().UnixNano()
	genStrCount = 0
	fmt.Printf("Test rand seed: %d\n", seed)
	seededRand := rand.New(rand.NewSource(seed))

	N := 1000

	testLevs := make([]string, N)
	for i := 0; i < N; i++ {
		testLevs[i] = genStr(seededRand.Intn(1024 * 10))
	}

	options := binlog.Options{
		PrefixPath:   dir + "/test_pref",
		Magic:        testMagic,
		MaxChunkSize: 1024 * 1024 / 2,
	}

	engine := NewTestEngine(0)
	_, err := CreateEmptyFsBinlog(options)
	assert.Nil(t, err)

	bl, err := NewFsBinlog(&LoggerStdout{}, options)
	assert.Nil(t, err)

	stop := make(chan struct{})
	go func() {
		assert.NoError(t, bl.Start(0, []byte{}, engine))
		stop <- struct{}{}
	}()

	engine.WaitForReadyFlag()

	offset := int64(0)
	for _, l := range testLevs[:N/2] {
		offset, err = bl.AppendASAP(engine.GetCurrentOffset(), serialize(l))
		engine.SetCurrentOffset(offset)
		require.Nil(t, err)
	}

	engine.WaitUntilCommit(offset)
	assert.Nil(t, bl.Shutdown())
	<-stop

	// Now write second parts
	bl, err = NewFsBinlog(&LoggerStdout{}, options)
	assert.Nil(t, err)

	engine = NewTestEngine(0)
	engine.applyCb = func(payload []byte) (int64, error) {
		_, n, err := deserialize(testMagic, payload)
		engine.AddOffset(n)
		return engine.GetCurrentOffset(), err
	}

	stop = make(chan struct{})
	go func() {
		assert.NoError(t, bl.Start(0, []byte{}, engine))
		stop <- struct{}{}
	}()

	engine.WaitForReadyFlag()

	offset = int64(0)
	for _, l := range testLevs[N/2:] {
		offset, err = bl.AppendASAP(engine.GetCurrentOffset(), serialize(l))
		engine.SetCurrentOffset(offset)
		require.Nil(t, err)
	}

	engine.WaitUntilCommit(offset)
	assert.Nil(t, bl.Shutdown())
	<-stop

	testForCrcCorrectness(t, dir)
}

func testForCrcCorrectness(t *testing.T, dir string) {
	files, err := os.ReadDir(dir)
	assert.Nil(t, err)

	fileOffset := int64(0)
	prevHash := uint64(0)
	curHash := uint64(0)
	crc := uint32(0)

	for i, fi := range files {
		data, err := os.ReadFile(filepath.Join(dir, fi.Name()))
		require.NoError(t, err)

		if i == 0 {
			curHash = calcFirstBinlogFileLogHash(data)
		}

		fp, err := os.Open(filepath.Join(dir, fi.Name()))
		require.NoError(t, err)

		if i != 0 {
			var rotateFrom levRotateFrom
			var buff [levRotateSize]byte

			_, err = fp.ReadAt(buff[:], 0)
			require.NoError(t, err)

			_, err = readLevRotateFrom(&rotateFrom, buff[:])
			require.NoError(t, err)

			assert.Equal(t, fileOffset, rotateFrom.CurLogPos)
			assert.Equal(t, prevHash, rotateFrom.PrevLogHash)
			assert.Equal(t, curHash, rotateFrom.CurLogHash)
		}

		fileInfo, err := fi.Info()
		require.NoError(t, err)

		fileOffset += fileInfo.Size()
		crc = updateCrc(crc, data[:len(data)-levRotateSize])
		prevHash = curHash
		curHash = calcNextLogHash(prevHash, fileOffset, crc)

		if i != len(files)-1 {
			var rotateTo levRotateTo
			var buff [levRotateSize]byte

			_, err = fp.ReadAt(buff[:], fileInfo.Size()-levRotateSize)
			require.NoError(t, err)

			_, err = readLevRotateTo(&rotateTo, buff[:])
			require.NoError(t, err)

			assert.Equal(t, fileOffset, rotateTo.NextLogPos)
			assert.Equal(t, prevHash, rotateTo.CurLogHash)
			assert.Equal(t, curHash, rotateTo.NextLogHash)
		}

		crc = updateCrc(crc, data[len(data)-levRotateSize:])
	}
}

func calcFirstBinlogFileLogHash(data []byte) uint64 {
	const (
		halfSize = 16 * 1024
		fullSize = 2 * halfSize
	)

	hasher := md5.New()

	switch {
	case len(data) <= fullSize:
		hasher.Write(data)

	case len(data) >= halfSize:
		hasher.Write(data[:halfSize])
		hasher.Write(data[len(data)-halfSize : len(data)-16])
		hasher.Write(make([]byte, 16)) // hash calc with rotate to last 16 byte is zero
	}

	fileHash := hasher.Sum(nil)
	return binary.LittleEndian.Uint64(fileHash)
}
