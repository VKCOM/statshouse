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

	"github.com/VKCOM/statshouse/internal/vkgo/basictl"
	"github.com/zeebo/xxh3"
)

// this is file format used for all metadata
// it consists of byte chunks protected by magic and xxh3
// files are saved at the exit (or sometimes periodically) and fsync is never called

// each individual item must be < chunkSize/2

// File format is chunk-based so that reading and writing requires not so much memory.
// Also, we can start saving changes incrementally later.
// file: [chunk]...
// chunk: [magic] [body size] [body] [xxhash of all previous bytes]
// body:  [element]...

const ChunkedMagicMappings = 0x83a28d18
const ChunkedMagicJournal = 0x83a28d1f
const ChunkedMagicConfig = 0x83a28d1a

const chunkSize = 1024 * 1024 // Never decrease it, otherwise reading will break.
const chunkHeaderSize = 4 + 4 // magic + body size
const chunkHashSize = 16

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
	if maxChunkSize <= 0 || maxChunkSize > chunkSize {
		maxChunkSize = chunkSize
	}
	if cap(c.scratch) < chunkHeaderSize+chunkSize+chunkHashSize {
		c.scratch = make([]byte, 0, chunkHeaderSize+chunkSize+chunkHashSize)
	}
	c.magic = magic
	c.offset = 0
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
	if len(chunk) < chunkHeaderSize+chunkSize/2 { // write after half space used
		return chunk, nil
	}
	if len(chunk) > chunkHeaderSize+chunkSize {
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
	if len(c.scratch) < chunkHeaderSize+chunkSize+chunkHashSize {
		c.scratch = make([]byte, chunkHeaderSize+chunkSize+chunkHashSize)
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
	if s > chunkSize {
		return nil, first, fmt.Errorf("chunk at %d body size %d overflows hard limit %d", c.offset, s, chunkSize)
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
