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
	scratch []byte // first bytes are reserved for efficient hash calculation

	// common part, writer start at the point reader finished
	offset int64
	hash   xxh3.Uint128

	// reading part
	ReadAt          func(b []byte, offset int64) error // we use it as flag that reading complete
	nextOffset      int64
	nextHash        xxh3.Uint128
	initialFileSize int64

	// writer part
	magic        uint32
	maxChunkSize int // limit for tests
	WriteAt      func(offset int64, data []byte) error
	Truncate     func(offset int64) error
	writeErr     error // if we add chunks and get error, adding chunks is NOP until reset to start of file
}

// after creating ChunkedStorage2, you must first read everything, calling ReadNext() until it
// either returns error or empty chunk

// then, you can write chunk at any moment you like by calling
//

func NewChunkedStorageNop() *ChunkedStorage2 {
	return &ChunkedStorage2{
		scratch:  make([]byte, chunkHashSize+chunkHeaderSize+ChunkSize+chunkHashSize),
		WriteAt:  func(offset int64, data []byte) error { return nil },
		Truncate: func(offset int64) error { return nil },
		ReadAt:   func(b []byte, offset int64) error { return nil },
	}
}

func NewChunkedStorage2File(fp *os.File) *ChunkedStorage2 {
	if fp == nil {
		return NewChunkedStorageNop()
	}
	initialFileSize, _ := fp.Seek(0, 2) // if this fails, we do not care
	return &ChunkedStorage2{
		scratch: make([]byte, chunkHashSize+chunkHeaderSize+ChunkSize+chunkHashSize),
		WriteAt: func(offset int64, data []byte) error {
			_, err := fp.WriteAt(data, offset)
			return err
		},
		Truncate: func(offset int64) error {
			return fp.Truncate(offset)
		},
		ReadAt: func(b []byte, offset int64) error {
			_, err := fp.ReadAt(b, offset)
			return err
		},
		initialFileSize: initialFileSize,
	}
}

func NewChunkedStorage2Slice(fp *[]byte) *ChunkedStorage2 {
	if fp == nil {
		return NewChunkedStorageNop()
	}
	return &ChunkedStorage2{
		scratch: make([]byte, chunkHashSize+chunkHeaderSize+ChunkSize+chunkHashSize),
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

func (c *ChunkedStorage2) ReadNext(magic uint32) (chunk []byte, err error) {
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
	if m := binary.LittleEndian.Uint32(currentChunk); m != magic {
		return nil, fmt.Errorf("chunk at %d invalid magic 0x%x, expected 0x%x", c.offset, m, magic)
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

func (c *ChunkedStorage2) ResetToStartOfFile() {
	c.hash = xxh3.Uint128{}
	c.offset = 0
	c.writeErr = nil
}

func (c *ChunkedStorage2) StartWriteChunk(magic uint32, maxChunkSize int) []byte {
	if maxChunkSize <= 0 || maxChunkSize > ChunkSize {
		maxChunkSize = ChunkSize
	}
	c.ReadAt = nil // prevent subsequent reading, if ReadNext was not called to the end
	c.magic = magic
	c.maxChunkSize = maxChunkSize
	return c.startChunk()
}

func (c *ChunkedStorage2) startChunk() []byte {
	copy(c.scratch, make([]byte, chunkHashSize+chunkHeaderSize)) // keep it tidy
	return c.scratch[:chunkHashSize+chunkHeaderSize]
}

func (c *ChunkedStorage2) finishChunk(chunk []byte) ([]byte, error) {
	if len(chunk) < chunkHashSize+chunkHeaderSize {
		panic("saver invariant violated, caller must only append to chunk")
	}
	if len(chunk) == chunkHashSize+chunkHeaderSize {
		return chunk, nil
	}
	putHash(chunk, c.hash)
	binary.LittleEndian.PutUint32(chunk[chunkHashSize:], c.magic)
	binary.LittleEndian.PutUint32(chunk[chunkHashSize+4:], uint32(len(chunk)-chunkHashSize-chunkHeaderSize))
	h := xxh3.Hash128(chunk)
	chunk = binary.BigEndian.AppendUint64(chunk, h.Hi)
	chunk = binary.BigEndian.AppendUint64(chunk, h.Lo)
	if c.writeErr != nil {
		return c.startChunk(), fmt.Errorf("chunk storage discarded chunk due to previous write error: %v", c.writeErr)
	}
	if err := c.WriteAt(c.offset, chunk[chunkHashSize:]); err != nil {
		// we must discard chunk, otherwise it grows beyond panic
		// after that file is forever broken, so we simply stop writing until restart
		c.writeErr = err
		return c.startChunk(), fmt.Errorf("chunk storage write error: %v", c.writeErr)
	}
	c.hash = h
	c.offset += int64(len(chunk) - chunkHashSize)
	return c.startChunk(), nil
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
