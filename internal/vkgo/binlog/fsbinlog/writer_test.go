// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package fsbinlog

import (
	"hash/crc32"
	"path/filepath"
	"sort"
	"sync"
	"testing"

	"github.com/myxo/gofs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const beginSize = 24 + 20 // start lev + tag lev

func TestSimpleWrite(t *testing.T) {
	fs := gofs.NewMemoryFs()
	dir := fs.TempDir()

	options := Options{
		PrefixPath:   filepath.Join(dir, "test_log"),
		MaxChunkSize: 100000,
		Fs:           fs,
	}
	file, err := CreateEmptyFsBinlog(options)
	require.NoError(t, err)

	fileData, err := fs.ReadFile(file)
	require.NoError(t, err)
	crcAfterCreate := crc32.Update(0, crc32.IEEETable, fileData)

	fh := FileHeader{FileName: file}
	err = readBinlogHeaderFile(fs, &fh, 0)
	require.NoError(t, err)

	buff := newBuffEx(crcAfterCreate, int64(len(fileData)), int64(len(fileData)), int64(options.MaxChunkSize))

	engine := NewTestEngine(int64(len(fileData)))
	stop := make(chan struct{})
	bw, err := newBinlogWriter(fs, &LoggerStdout{}, engine, options, int64(len(fileData)), &fh, buff, &stat{}, stop, nil, 0)
	require.NoError(t, err)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err = bw.loop(0)
		assert.NoError(t, err)
	}()

	buff.mu.Lock()
	lev1 := []byte("hello123") // remember to add padding by 4 bytes
	lev2 := []byte("world   ")
	buff.appendLevUnsafe(lev1)
	buff.appendLevUnsafe(lev2)
	buff.rd.commitASAP = true
	buff.mu.Unlock()
	bw.dataCh <- struct{}{}

	engine.WaitUntilCommit(int64(beginSize + len(lev1) + len(lev2)))

	fileData, err = fs.ReadFile(file)
	require.NoError(t, err)

	crcFinal := crc32.Update(0, crc32.IEEETable, fileData)

	lev1Pos := beginSize
	lev2Pos := beginSize + len(lev1)
	require.Equal(t, lev1, fileData[lev1Pos:lev2Pos])
	require.Equal(t, lev2, fileData[lev2Pos:])

	require.Equal(t, engine.commitPosition.Load(), int64(lev2Pos+len(lev2)))
	require.Equal(t, engine.commitCrc.Load(), crcFinal)

	close(stop)
	wg.Wait()
}

func TestRotate(t *testing.T) {
	fs := gofs.NewMemoryFs()
	dir := fs.TempDir()

	options := Options{
		PrefixPath:   filepath.Join(dir, "test_log"),
		MaxChunkSize: 1024,
		Fs:           fs,
	}
	file, err := CreateEmptyFsBinlog(options)
	require.NoError(t, err)

	fileData, err := fs.ReadFile(file)
	require.NoError(t, err)
	crcAfterCreate := crc32.Update(0, crc32.IEEETable, fileData)

	fh := FileHeader{FileName: file}
	err = readBinlogHeaderFile(fs, &fh, 0)
	require.NoError(t, err)

	buff := newBuffEx(crcAfterCreate, int64(len(fileData)), int64(len(fileData)), int64(options.MaxChunkSize))
	engine := NewTestEngine(int64(len(fileData)))
	stop := make(chan struct{})
	bw, err := newBinlogWriter(fs, &LoggerStdout{}, engine, options, int64(len(fileData)), &fh, buff, &stat{}, stop, nil, 0)
	require.NoError(t, err)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err = bw.loop(0)
		assert.NoError(t, err)
	}()

	// 0 and 1 should be in first file, 2 in second
	buff.mu.Lock()
	levs := []string{genStr(512), genStr(512), genStr(512)}
	for _, lev := range levs[:2] {
		buff.appendLevUnsafe([]byte(lev))
	}
	buff.rotateFile()
	buff.appendLevUnsafe([]byte(levs[2]))
	buff.rd.commitASAP = true
	buff.mu.Unlock()
	bw.dataCh <- struct{}{}

	engine.WaitUntilCommit(int64(beginSize + 512*3))

	files, err := fs.ReadDir(dir)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(files))

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
		fileData, err = fs.ReadFile(filepath.Join(dir, name))
		require.NoError(t, err)

		crc = crc32.Update(crc, crc32.IEEETable, fileData)
		pos += int64(len(fileData))
	}

	require.Equal(t, []byte(levs[len(levs)-1]), fileData)

	require.Equal(t, engine.commitPosition.Load(), pos)
	require.Equal(t, engine.commitCrc.Load(), crc)

	close(stop)
	wg.Wait()
}
