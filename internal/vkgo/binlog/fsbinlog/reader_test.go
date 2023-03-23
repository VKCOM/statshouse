// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package fsbinlog

import (
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func GetPosAndCrc(t *testing.T, dir string) (int64, uint32) {
	files, err := os.ReadDir(dir)
	assert.Nil(t, err)

	fileNames := make([]string, len(files))
	for i, f := range files {
		fileNames[i] = f.Name()
	}

	sort.Slice(fileNames, func(i, j int) bool {
		return fileNames[i] < fileNames[j]
	})

	crc := uint32(0)
	pos := int64(0)
	for _, name := range fileNames {
		fileData, err := os.ReadFile(filepath.Join(dir, name))
		require.NoError(t, err)

		crc = crc32.Update(crc, crc32.IEEETable, fileData)
		pos += int64(len(fileData))
	}

	return pos, crc
}

func copyFolder(t *testing.T, src, dst string) {
	files, err := os.ReadDir(src)
	assert.Nil(t, err)

	for _, f := range files {
		data, err := os.ReadFile(filepath.Join(src, f.Name()))
		assert.Nil(t, err)
		err = os.WriteFile(filepath.Join(dst, f.Name()), data, 0666)
		assert.Nil(t, err)
	}
}

func TestRead(t *testing.T) {
	_, filename, _, _ := runtime.Caller(0)
	testDataDir := filepath.Join(filepath.Dir(filename), "test_data/test_log")

	PrefixPath := filepath.Join(testDataDir, "test_log")

	stop := make(chan struct{})
	reader, err := newBinlogReader(nil, time.Second, nil, &stat{}, false, &stop)
	require.NoError(t, err)

	engine := NewTestEngine(0)
	count := 0
	engine.applyCb = func(payload []byte) (int64, error) {
		value, n, err := deserialize(12345, payload)
		engine.AddOffset(n)
		if err != nil {
			return engine.GetCurrentOffset(), err
		}
		require.Equal(t, strconv.Itoa(count), value)
		count++
		return engine.GetCurrentOffset(), nil
	}

	pos, crc, err := reader.readAllFromPosition(0, PrefixPath, testMagic, engine, nil, false)
	require.NoError(t, err)

	expectedPos, expectedCrc := GetPosAndCrc(t, testDataDir)

	require.Equal(t, expectedPos, pos)
	require.Equal(t, expectedCrc, crc)

	close(stop)
}

func TestReadIncorrectMagic(t *testing.T) {
	_, filename, _, _ := runtime.Caller(0)
	testDataDir := filepath.Join(filepath.Dir(filename), "test_data/test_log")
	PrefixPath := filepath.Join(testDataDir, "test_log")

	stop := make(chan struct{})
	reader, err := newBinlogReader(nil, time.Second, nil, &stat{}, false, &stop)
	require.NoError(t, err)

	_, _, err = reader.readAllFromPosition(0, PrefixPath, testMagic+1, NewTestEngine(0), nil, false)
	require.Error(t, err)

	close(stop)
}

func TestReadFromMiddle(t *testing.T) {
	_, filename, _, _ := runtime.Caller(0)
	testDataDir := filepath.Join(filepath.Dir(filename), "test_data/test_log")

	PrefixPath := filepath.Join(testDataDir, "test_log")
	stop := make(chan struct{})
	reader, err := newBinlogReader(nil, time.Second, nil, &stat{}, false, &stop)
	require.NoError(t, err)

	count := 20
	startPosition := int64(356) // this is for count == 20
	engine := NewTestEngine(startPosition)
	engine.applyCb = func(payload []byte) (n int64, err error) {
		value, n, err := deserialize(12345, payload)
		engine.AddOffset(n)
		if err != nil {
			return engine.GetCurrentOffset(), err
		}
		require.Equal(t, strconv.Itoa(count), value)
		count++
		return engine.GetCurrentOffset(), nil
	}

	pos, crc, err := reader.readAllFromPosition(startPosition, PrefixPath, 0, engine, nil, false)
	require.NoError(t, err)

	expectedPos, expectedCrc := GetPosAndCrc(t, testDataDir)

	require.Equal(t, expectedPos, pos)
	require.Equal(t, expectedCrc, crc)

	close(stop)
}

func TestReadReplicaMode(t *testing.T) {
	tmpDir := t.TempDir()

	_, filename, _, _ := runtime.Caller(0)
	testDataDir := filepath.Join(filepath.Dir(filename), "test_data/test_log")
	copyFolder(t, testDataDir, tmpDir)
	PrefixPath := filepath.Join(tmpDir, "test_log")

	stopCh := make(chan struct{})
	reader, err := newBinlogReader(nil, time.Second, nil, &stat{}, false, &stopCh)
	require.NoError(t, err)

	engine := NewTestEngine(0)
	count := atomic.NewInt32(0)
	engine.applyCb = func(payload []byte) (int64, error) {
		value, n, err := deserialize(12345, payload)
		engine.AddOffset(n)
		if err != nil {
			return engine.GetCurrentOffset(), err
		}
		require.Equal(t, strconv.Itoa(int(count.Load())), value)
		count.Add(1)
		return engine.GetCurrentOffset(), nil
	}

	quit := make(chan struct{})
	go func() {
		_, _, err := reader.readAllFromPosition(0, PrefixPath, 0, engine, nil, true)
		if err != nil && !errors.Is(err, errStopped) {
			require.NoError(t, err)
		}
		quit <- struct{}{}
	}()

	engine.WaitForReadyFlag()

	lastBinlogFile := ""
	{
		files, err := os.ReadDir(tmpDir)
		assert.Nil(t, err)

		fileNames := make([]string, len(files))
		for i, f := range files {
			fileNames[i] = f.Name()
		}

		sort.Slice(fileNames, func(i, j int) bool {
			return fileNames[i] < fileNames[j]
		})

		lastBinlogFile = filepath.Join(tmpDir, fileNames[len(fileNames)-1])
	}

	f, err := os.OpenFile(lastBinlogFile, os.O_APPEND|os.O_WRONLY, 0600)
	assert.Nil(t, err)

	addPadding := func(body []byte) []byte {
		// All levs in binlog should be aligned by 4 bytes.
		// We should add it since we write manually to file
		if padding := len(body) % 4; padding != 0 {
			var zero [4]byte
			body = append(body, zero[:4-padding]...)
		}
		return body
	}

	_, err = f.Write(addPadding(serialize("50")))
	assert.Nil(t, err)
	_, err = f.Write(addPadding(serialize("51")))
	assert.Nil(t, err)

	err = f.Close()
	assert.Nil(t, err)

	for count.Load() != 52 {
		time.Sleep(time.Millisecond * 10)
	}

	// expectedPos, expectedCrc := GetPosAndCrc(t, testDataDir)
	// require.Equal(t, expectedPos, pos)
	// require.Equal(t, expectedCrc, crc)

	close(stopCh)
	<-quit
}
