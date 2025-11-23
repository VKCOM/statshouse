// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package data_model

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/zeebo/xxh3"

	"github.com/VKCOM/statshouse/internal/vkgo/basictl"
)

// this is deprecated and will be removed soon

type ChunkedStorageSaver struct {
	magic        uint32
	scratch      []byte
	offset       int64
	maxChunkSize int // limit for tests
	WriteAt      func(offset int64, data []byte) error
	Truncate     func(offset int64) error
}

type ChunkedStorageLoader struct {
	magic    uint32
	scratch  []byte
	offset   int64
	fileSize int64
	ReadAt   func(b []byte, offset int64) error
}

// ChunkedStorageShared wraps saver and loader with shared offset
type ChunkedStorageShared struct {
	offset int64 // shared offset between saver and loader
	saver  *ChunkedStorageSaver
	loader *ChunkedStorageLoader
}

func NewChunkedStorageShared(writeAt func(offset int64, data []byte) error, truncate func(offset int64) error, readAt func(b []byte, offset int64) error) *ChunkedStorageShared {
	return &ChunkedStorageShared{
		saver: &ChunkedStorageSaver{
			WriteAt:  writeAt,
			Truncate: truncate,
		},
		loader: &ChunkedStorageLoader{
			ReadAt: readAt,
		},
	}
}

func (c *ChunkedStorageShared) StartWriteWithOffset(magic uint32, maxChunkSize int) []byte {
	// will reset offset
	c.saver.offset = c.offset
	res := c.saver.StartWriteWithOffset(magic, maxChunkSize, c.offset)
	c.offset = c.saver.offset
	return res
}

func (c *ChunkedStorageShared) FinishItem(chunk []byte) ([]byte, error) {
	c.saver.offset = c.offset
	chunk, err := c.saver.FinishItem(chunk)
	if err != nil {
		c.saver.offset = c.offset
		return nil, err
	}
	c.offset = c.saver.offset
	return chunk, nil
}

func (c *ChunkedStorageShared) FinishWrite(chunk []byte) error {
	c.saver.offset = c.offset
	err := c.saver.FinishWrite(chunk)
	if err != nil {
		c.saver.offset = c.offset
		return err
	}
	c.offset = c.saver.offset
	return nil
}

func (c *ChunkedStorageShared) StartRead(fileSize int64, magic uint32) {
	// will reset offset
	c.loader.StartRead(fileSize, magic)
	c.offset = c.loader.offset
}

func (c *ChunkedStorageShared) ReadNext() (chunk []byte, first bool, err error) {
	c.loader.offset = c.offset
	chunk, first, err = c.loader.ReadNext()
	if err != nil {
		c.loader.offset = c.offset
		return nil, false, err
	}
	return chunk, first, err
}

func (c *ChunkedStorageShared) FinishReadChunk() {
	c.offset = c.loader.offset
}

func ChunkedStorageFile(fp *os.File) (writeAt func(offset int64, data []byte) error, truncate func(offset int64) error, readAt func(b []byte, offset int64) error, fileSize int64) {
	if fp == nil {
		return func(offset int64, data []byte) error { return nil }, func(offset int64) error { return nil }, func(b []byte, offset int64) error { return nil }, 0
	}
	writeAt = func(offset int64, data []byte) error {
		_, err := fp.WriteAt(data, offset)
		return err
	}
	truncate = func(offset int64) error {
		return fp.Truncate(offset)
	}
	fileSize, _ = fp.Seek(0, 2) // should be never, but we do not care
	readAt = func(b []byte, offset int64) error {
		_, err := fp.ReadAt(b, offset)
		return err
	}
	return
}

func ChunkedStorageSlice(fp *[]byte) (writeAt func(offset int64, data []byte) error, truncate func(offset int64) error, readAt func(b []byte, offset int64) error, fileSize int64) {
	writeAt = func(offset int64, data []byte) error {
		if len(*fp) < int(offset)+len(data) {
			*fp = append(*fp, make([]byte, int(offset)+len(data)-len(*fp))...)
		}
		copy((*fp)[offset:], data)
		return nil
	}
	truncate = func(offset int64) error {
		*fp = (*fp)[:offset]
		return nil
	}
	fileSize = int64(len(*fp))
	readAt = func(data []byte, offset int64) error {
		copy(data, (*fp)[offset:])
		return nil
	}
	return
}

// pass maxChunkSize 0 for default
func (c *ChunkedStorageSaver) StartWrite(magic uint32, maxChunkSize int) []byte {
	return c.StartWriteWithOffset(magic, maxChunkSize, 0)
}

func (c *ChunkedStorageSaver) StartWriteWithOffset(magic uint32, maxChunkSize int, offset int64) []byte {
	if maxChunkSize <= 0 || maxChunkSize > ChunkSize {
		maxChunkSize = ChunkSize
	}
	if cap(c.scratch) < chunkHeaderSize+ChunkSize+chunkHashSize {
		c.scratch = make([]byte, 0, chunkHeaderSize+ChunkSize+chunkHashSize)
	}
	c.magic = magic
	c.offset = offset
	c.maxChunkSize = maxChunkSize
	return c.startChunk(c.scratch)
}

func (c *ChunkedStorageSaver) startChunk(chunk []byte) []byte {
	chunk = basictl.NatWrite(chunk[:0], c.magic)
	chunk = basictl.NatWrite(chunk, 0) // placeholder for body size
	return chunk
}

func (c *ChunkedStorageSaver) finishChunk(chunk []byte) ([]byte, error) {
	if len(chunk) == chunkHeaderSize {
		return chunk, nil
	}
	binary.LittleEndian.PutUint32(chunk[4:8], uint32(len(chunk)-chunkHeaderSize))
	h := xxh3.Hash128(chunk)
	chunk = binary.BigEndian.AppendUint64(chunk, h.Hi)
	chunk = binary.BigEndian.AppendUint64(chunk, h.Lo)
	if err := c.WriteAt(c.offset, chunk); err != nil {
		return chunk, err
	}
	c.offset += int64(len(chunk))
	return c.startChunk(chunk), nil
}

func (c *ChunkedStorageSaver) FinishItem(chunk []byte) ([]byte, error) {
	if len(chunk) < chunkHeaderSize {
		panic("saver invariant violated, caller must only append to chunk")
	}
	if len(chunk) < chunkHeaderSize+ChunkSize/2 { // write after half space used
		return chunk, nil
	}
	if len(chunk) > chunkHeaderSize+ChunkSize {
		return chunk, fmt.Errorf("too big item(s) - %d bytes", len(chunk))
	}
	return c.finishChunk(chunk)
}

// returns scratch for reuse in StartWrite, if you wish
func (c *ChunkedStorageSaver) FinishWrite(chunk []byte) error {
	chunk, err := c.finishChunk(chunk)
	c.scratch = chunk // reuse
	if err != nil {
		return err
	}
	return c.Truncate(c.offset)
}

func (c *ChunkedStorageLoader) StartRead(fileSize int64, magic uint32) {
	if len(c.scratch) < chunkHeaderSize+ChunkSize+chunkHashSize {
		c.scratch = make([]byte, chunkHeaderSize+ChunkSize+chunkHashSize)
	}
	c.magic = magic
	c.offset = 0
	c.fileSize = fileSize
}

func (c *ChunkedStorageLoader) ReadNext() (chunk []byte, first bool, err error) {
	first = c.offset == 0
	if c.offset == c.fileSize {
		return nil, first, nil
	}
	if c.offset+chunkHeaderSize+chunkHashSize > c.fileSize {
		return nil, first, fmt.Errorf("chunk at %d header overflows file size %d", c.offset, c.fileSize)
	}
	if err := c.ReadAt(c.scratch[:chunkHeaderSize], c.offset); err != nil {
		return nil, first, err
	}
	if m := binary.LittleEndian.Uint32(c.scratch[:]); m != c.magic {
		return nil, first, fmt.Errorf("chunk at %d invalid magic 0x%x", c.offset, m)
	}
	s := int64(binary.LittleEndian.Uint32(c.scratch[4:]))
	if s > ChunkSize {
		return nil, first, fmt.Errorf("chunk at %d body size %d overflows hard limit %d", c.offset, s, ChunkSize)
	}
	nextChunkOffset := c.offset + chunkHeaderSize + s + chunkHashSize
	if nextChunkOffset > c.fileSize {
		return nil, first, fmt.Errorf("chunk at %d body size %d overflows file size %d", c.offset, s, c.fileSize)
	}
	if err := c.ReadAt(c.scratch[:chunkHeaderSize+s+chunkHashSize], c.offset); err != nil { // read header again for simplicity
		return nil, first, err
	}
	h := xxh3.Hash128(c.scratch[:chunkHeaderSize+s])
	if h.Hi != binary.BigEndian.Uint64(c.scratch[chunkHeaderSize+s:]) ||
		h.Lo != binary.BigEndian.Uint64(c.scratch[chunkHeaderSize+s+8:]) {
		return nil, first, fmt.Errorf("chunk at %d has wrong xxhash", c.offset)
	}
	c.offset = nextChunkOffset
	return c.scratch[chunkHeaderSize : chunkHeaderSize+s], first, nil
}
