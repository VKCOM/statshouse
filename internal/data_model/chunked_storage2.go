package data_model

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/zeebo/xxh3"
)

// this is file format used for all metadata
// it consists of byte chunks protected by magic and xxh3
// files are saved at the exit (or sometimes periodically) and fsync is never called

// each individual item must be < chunkSize/2

// File format is chunk-based so that reading and writing requires not so much memory.
// Also, we can start saving changes incrementally later.
// file: [chunk]...
// chunk: [magic] [body size] [body] [xxhash]
// body:  [element]...

// xxhash is calculated over all chunk bytes plus xxhash of previous chunk
// ... [xxhash] [magic] [body size] [body] [xxhash]
//              <--------------------------------->  // this chunk
//     <--------------------------------->  --^ // hash of these bytes
// for the first chunk we use [00..00] instead of hash of previous chunk

const ChunkedMagicMappings = 0x83a28d18
const ChunkedMagicJournal = 0x83a28d1f
const ChunkedMagicConfig = 0x83a28d1a

const ChunkSize = 1024 * 1024 // Never decrease it, otherwise reading will break.
const chunkHeaderSize = 4 + 4 // magic + body size
const chunkHashSize = 16

type ChunkedStorage2 struct {
	magic        uint32
	maxChunkSize int // limit for tests
	scratch      []byte
	offset       int64
	hash         xxh3.Uint128
	WriteAt      func(offset int64, data []byte) error
	Truncate     func(offset int64) error

	ReadAt          func(b []byte, offset int64) error // we use it as flag that reading complete
	nextOffset      int64
	nextHash        xxh3.Uint128
	initialFileSize int64
}

// after creating ChunkedStorage2, you must first read everything, calling ReadNext() until it
// either returns error or empty chunk

// then, you can write chunk at any moment you like by calling
//

func NewChunkedStorage2File(magic uint32, maxChunkSize int, fp *os.File) *ChunkedStorage2 {
	if maxChunkSize <= 0 || maxChunkSize > ChunkSize {
		maxChunkSize = ChunkSize
	}
	result := &ChunkedStorage2{
		magic:        magic,
		maxChunkSize: maxChunkSize,
		scratch:      make([]byte, chunkHashSize+chunkHeaderSize+ChunkSize+chunkHashSize),
	}
	if fp == nil {
		result.WriteAt = func(offset int64, data []byte) error { return nil }
		result.Truncate = func(offset int64) error { return nil }
		result.ReadAt = func(b []byte, offset int64) error { return nil }
		return result
	}
	result.WriteAt = func(offset int64, data []byte) error {
		_, err := fp.WriteAt(data, offset)
		return err
	}
	result.Truncate = func(offset int64) error {
		return fp.Truncate(offset)
	}
	result.ReadAt = func(b []byte, offset int64) error {
		_, err := fp.ReadAt(b, offset)
		return err
	}
	result.initialFileSize, _ = fp.Seek(0, 2) // should be never, but we do not care
	return result
}

func ChunkedStorage2Slice(magic uint32, maxChunkSize int, fp *[]byte) *ChunkedStorage2 {
	if maxChunkSize <= 0 || maxChunkSize > ChunkSize {
		maxChunkSize = ChunkSize
	}
	return &ChunkedStorage2{
		magic:        magic,
		maxChunkSize: maxChunkSize,
		scratch:      make([]byte, chunkHashSize+chunkHeaderSize+ChunkSize+chunkHashSize),
		WriteAt: func(offset int64, data []byte) error {
			if len(*fp) < int(offset)+len(data) {
				*fp = append(*fp, make([]byte, int(offset)+len(data)-len(*fp))...)
			}
			copy((*fp)[offset:], data)
			return nil
		},
		Truncate: func(offset int64) error {
			*fp = (*fp)[:offset]
			return nil
		},
		ReadAt: func(data []byte, offset int64) error {
			copy(data, (*fp)[offset:])
			return nil
		},
		initialFileSize: int64(len(*fp)),
	}
}

func (c *ChunkedStorage2) IsFirst() bool {
	return c.offset == 0
}

func (c *ChunkedStorage2) ReadNext() (chunk []byte, err error) {
	if c.ReadAt == nil { // reading finished, we do not interference with writing
		return nil, nil
	}
	// we change offset only on the next call to ReadNext, so if during chunk
	// parsing there's error and ReadNext is not called, offset will not be updated
	c.offset = c.nextOffset
	c.hash = c.nextHash
	if c.offset == c.initialFileSize {
		c.ReadAt = nil
		return nil, nil
	}
	if c.offset+chunkHeaderSize+chunkHashSize > c.initialFileSize {
		return nil, fmt.Errorf("chunk at %d header overflows file size %d", c.offset, c.initialFileSize)
	}
	putHash(c.scratch, c.hash)
	currentChunk := c.scratch[chunkHashSize:]
	if err := c.ReadAt(currentChunk[:chunkHeaderSize], c.offset); err != nil {
		return nil, err
	}
	if m := binary.LittleEndian.Uint32(currentChunk); m != c.magic {
		return nil, fmt.Errorf("chunk at %d invalid magic 0x%x", c.offset, m)
	}
	s := int64(binary.LittleEndian.Uint32(currentChunk[4:]))
	if s > ChunkSize {
		return nil, fmt.Errorf("chunk at %d body size %d overflows hard limit %d", c.offset, s, ChunkSize)
	}
	nextChunkOffset := c.offset + chunkHeaderSize + s + chunkHashSize
	if nextChunkOffset > c.initialFileSize {
		return nil, fmt.Errorf("chunk at %d body size %d overflows file size %d", c.offset, s, c.initialFileSize)
	}
	if err := c.ReadAt(currentChunk[:chunkHeaderSize+s+chunkHashSize], c.offset); err != nil { // read header again for simplicity
		return nil, err
	}
	actualHash := getHash(currentChunk[chunkHeaderSize+s:])
	h := xxh3.Hash128(c.scratch[:chunkHashSize+chunkHeaderSize+s])
	if h != actualHash {
		return nil, fmt.Errorf("chunk at %d has wrong xxhash", c.offset)
	}
	c.nextOffset = nextChunkOffset
	c.nextHash = actualHash
	return currentChunk[chunkHeaderSize : chunkHeaderSize+s], nil
}

func (c *ChunkedStorage2) StartWriteChunk() []byte {
	putHash(c.scratch, c.hash)
	binary.LittleEndian.PutUint32(c.scratch[chunkHashSize:], c.magic)
	binary.LittleEndian.PutUint32(c.scratch[chunkHashSize+4:], 0) // body size
	return c.scratch[:chunkHashSize+chunkHeaderSize]
}

func (c *ChunkedStorage2) finishChunk(chunk []byte) ([]byte, error) {
	if len(chunk) < chunkHashSize+chunkHeaderSize {
		panic("saver invariant violated, caller must only append to chunk")
	}
	if len(chunk) == chunkHashSize+chunkHeaderSize {
		return chunk, nil
	}
	binary.LittleEndian.PutUint32(chunk[chunkHashSize+4:], uint32(len(chunk)-chunkHashSize-chunkHeaderSize))
	h := xxh3.Hash128(chunk)
	chunk = binary.BigEndian.AppendUint64(chunk, h.Hi)
	chunk = binary.BigEndian.AppendUint64(chunk, h.Lo)
	if c.WriteAt == nil {
		return c.StartWriteChunk(), fmt.Errorf("chunk storage discarded chunk due to previous write error")
	}
	if err := c.WriteAt(c.offset, chunk[chunkHashSize:]); err != nil {
		// we must discard chunk, otherwise it grows beyond panic
		// after that file is forever broken, so we simply stop writing until restart
		c.WriteAt = nil
		return c.StartWriteChunk(), err
	}
	c.hash = h
	c.offset += int64(len(chunk) - chunkHashSize)
	return c.StartWriteChunk(), nil
}

func (c *ChunkedStorage2) FinishItem(chunk []byte) ([]byte, error) {
	if len(chunk) < chunkHashSize+chunkHeaderSize {
		panic("saver invariant violated, caller must only append to chunk")
	}
	if len(chunk) < chunkHashSize+chunkHeaderSize+ChunkSize/2 { // write after half space used
		return chunk, nil
	}
	if len(chunk) > chunkHashSize+chunkHeaderSize+ChunkSize {
		return chunk, fmt.Errorf("too big item(s) - %d bytes", len(chunk))
	}
	return c.finishChunk(chunk)
}

func (c *ChunkedStorage2) FinishWriteChunk(chunk []byte) error {
	_, err := c.finishChunk(chunk)
	if err != nil {
		return err
	}
	return c.Truncate(c.offset)
}

func putHash(b []byte, h xxh3.Uint128) {
	binary.BigEndian.PutUint64(b, h.Hi)
	binary.BigEndian.PutUint64(b[8:], h.Lo)
}

func getHash(b []byte) xxh3.Uint128 {
	return xxh3.Uint128{
		Hi: binary.BigEndian.Uint64(b),
		Lo: binary.BigEndian.Uint64(b[8:]),
	}
}
